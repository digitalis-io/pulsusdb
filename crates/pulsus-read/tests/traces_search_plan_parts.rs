//! Issue #492 part 3: the compiled plan's part list, checked against the
//! statements each committed golden actually renders.
//!
//! **The expected side is the byte-frozen golden file, not a constant in
//! this crate.** Every case's query comes from the golden's own `-- q: `
//! header and its statement sequence from the golden's `== … ==` section
//! headings, so the plan is compared against an artifact produced by the
//! SQL builders. `traces_search_sql.rs::CASES` is the input those goldens
//! were generated from; comparing against it would have compared the
//! planner with itself.
//!
//! The mapping from a section heading to the source the plan names is the
//! table below, and it is the only place the two vocabularies meet.

use std::collections::BTreeMap;

use pulsus_read::compile::plan::{PartShape, PlanShape};
use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, plan_search};
use pulsus_read::{SearchPlan, SpanFilterCtx};

/// The same fixed window the golden suite plans against.
const PARAMS: SearchParams = SearchParams {
    start_ns: 1_700_000_000_000_000_000,
    end_ns: 1_700_010_800_000_000_000,
    limit: 20,
    spss: 3,
};

const MAX_CANDIDATES: u64 = 100_000;

/// The three cases whose plan names ONE generator part where the golden
/// renders TWO statements — a frozen, named exception.
///
/// Both of each case's phase-1 generators read the SAME table, so no
/// `Cut` in the closed set of four explains the second statement:
/// `Cut::DisjointSources` fires on an `OR` whose sides resolve against
/// DIFFERENT sources, and these do not. The shipped planner sends two
/// anyway, because `filter::collect`'s rule is about COMPLETENESS (`a || b`
/// needs both sides' sets) and not about sources — one `WHERE` could hold
/// both.
///
/// Whether one `WHERE` over an `OR` prunes as well as two ranked reads is
/// a pushdown measurement nobody has taken, and taking it inside the part
/// whose contract is "no SQL moves" is the confusion that part exists to
/// prevent. The measurement is owed by the part that compiles a
/// generator; `docs/query-lowering.md` §2.7.4 and §2.7.9 carry the same
/// exception with the same reason.
///
/// The gate below asserts this list EQUALS the measured set, so it cannot
/// grow silently.
const SAME_SOURCE_GENERATOR_FAN_OUT: [&str; 3] = [
    "nested_boolean",
    "structural_descendant",
    "structural_sibling",
];

/// One golden file, parsed.
struct Golden {
    case: String,
    query: String,
    /// Every `== … ==` heading, in file order.
    sections: Vec<String>,
    /// The table each `phase1 generator[i]` section reads, in order.
    generator_tables: Vec<String>,
    distributed: bool,
}

fn golden_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_search")
}

fn goldens() -> Vec<Golden> {
    let mut out = Vec::new();
    let mut paths: Vec<_> = std::fs::read_dir(golden_dir())
        .expect("the golden directory is readable")
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    paths.sort();
    for path in paths {
        let text = std::fs::read_to_string(&path).expect("golden is readable");
        let mut case = None;
        let mut query = None;
        let mut sections = Vec::new();
        let mut generator_tables = Vec::new();
        let mut pending_generator = false;
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("-- case: ") {
                case = Some(rest.to_string());
            } else if let Some(rest) = line.strip_prefix("-- q: ") {
                query = Some(rest.to_string());
            } else if let Some(inner) = line.strip_prefix("== ").and_then(|l| l.strip_suffix(" =="))
            {
                sections.push(inner.to_string());
                pending_generator = inner.starts_with("phase1 generator[");
            } else if pending_generator && let Some(table) = line.strip_prefix("FROM ") {
                generator_tables.push(table.to_string());
                pending_generator = false;
            }
        }
        let case = case.expect("every golden names its case");
        let query = query.expect("every golden carries its query");
        assert_eq!(
            generator_tables.len(),
            sections
                .iter()
                .filter(|s| s.starts_with("phase1 generator["))
                .count(),
            "{case}: every generator section must name the table it reads"
        );
        let distributed = text.contains("_dist");
        out.push(Golden {
            case,
            query,
            sections,
            generator_tables,
            distributed,
        });
    }
    assert_eq!(out.len(), 56, "the committed search corpus");
    out
}

fn plan_query(q: &str, distributed: bool) -> SearchPlan {
    let (spans, attrs) = if distributed {
        ("trace_spans_dist", "trace_attrs_idx_dist")
    } else {
        ("trace_spans", "trace_attrs_idx")
    };
    let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("{q}: {e}"));
    plan_search(
        &query,
        &PARAMS,
        &SearchCtx {
            filter: SpanFilterCtx {
                spans_table: spans,
                attrs_table: attrs,
            },
            max_candidates: MAX_CANDIDATES,
            max_series: 1_000,
            distributed,
        },
    )
    .unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

/// The source the plan names for one golden section, or `None` for a
/// section that is deliberately not a chain link.
///
/// The `by()` cardinality pre-flight probe is the only `None`: it is an
/// admission check that runs before phase 1 and answers `422` without
/// reading a result row, so it produces no candidate and consumes none.
fn section_source(
    section: &str,
    generator_tables: &mut std::vec::IntoIter<String>,
) -> Option<String> {
    if section.starts_with("phase1 generator[") {
        let table = generator_tables
            .next()
            .expect("one FROM per generator section");
        return Some(table.trim_end_matches("_dist").to_string());
    }
    if section.starts_with("phase2 membership[") {
        return Some("trace_attrs_idx:membership".to_string());
    }
    if section.starts_with("phase2 aggregate values[")
        || section.starts_with("phase2 select values[")
    {
        return Some("trace_attrs_idx:values".to_string());
    }
    if section.starts_with("phase2 event set[") {
        return Some("trace_attrs_idx:event_sets".to_string());
    }
    match section {
        "phase2 hydration (sample batch)" => Some("trace_spans:hydration".to_string()),
        "phase2 trace context (sample batch)" => Some("trace_spans:trace_ctx".to_string()),
        "phase2 child counts (sample batch)" => Some("trace_spans:child_count".to_string()),
        "root hydration (sample winners)" => Some("trace_spans:root".to_string()),
        "by() cardinality probe" => None,
        other => panic!("unmapped golden section {other:?}"),
    }
}

/// The SQL parts' source names, in plan order.
fn sql_part_sources(shape: &PlanShape) -> Vec<String> {
    shape
        .parts
        .iter()
        .filter_map(|p| match p {
            PartShape::Sql(s) => Some(s.name.clone()),
            PartShape::Engine(_) => None,
        })
        .collect()
}

/// The SQL parts, in plan order — the shape, not just the name.
fn sql_parts(shape: &PlanShape) -> Vec<&pulsus_read::compile::plan::SqlPartShape> {
    shape
        .parts
        .iter()
        .filter_map(|p| match p {
            PartShape::Sql(s) => Some(s.as_ref()),
            PartShape::Engine(_) => None,
        })
        .collect()
}

/// Criterion 7: for every committed case, the sequence of sources the
/// plan says it reads equals the sequence the golden renders.
#[test]
fn the_plan_sql_parts_match_the_sections_each_golden_case_renders() {
    for g in goldens() {
        let plan = plan_query(&g.query, g.distributed);
        let shape = plan.plan_shape();
        let mut tables = g.generator_tables.clone().into_iter();
        let mut golden_seq: Vec<String> = g
            .sections
            .iter()
            .filter_map(|s| section_source(s, &mut tables))
            .collect();
        if SAME_SOURCE_GENERATOR_FAN_OUT.contains(&g.case.as_str()) {
            // The frozen exception: two statements against one table,
            // which no `Cut` explains, so the plan names one part. Drop
            // the SECOND generator entry — the first is the one both
            // statements read.
            let second = golden_seq
                .iter()
                .position(|s| !s.contains(':'))
                .and_then(|first| {
                    golden_seq[first + 1..]
                        .iter()
                        .position(|s| !s.contains(':'))
                        .map(|i| first + 1 + i)
                })
                .unwrap_or_else(|| panic!("{}: the exception needs two generators", g.case));
            golden_seq.remove(second);
        }
        let plan_seq = sql_part_sources(&shape);
        assert_eq!(
            plan_seq, golden_seq,
            "{}: the plan sends {plan_seq:?} and the golden renders {golden_seq:?}",
            g.case
        );
    }
}

/// Criterion 8: the same-source generator fan-out exception is exactly
/// those three cases — asserted as an EQUALITY against the measured set,
/// so a fourth case cannot join it silently.
#[test]
fn the_generator_fan_out_exception_is_exactly_these_three() {
    let mut measured: Vec<String> = Vec::new();
    for g in goldens() {
        let plan = plan_query(&g.query, g.distributed);
        let shape = plan.plan_shape();
        // A generator part is an SQL part with no seed: it opens the
        // plan rather than reading what an earlier statement returned.
        let generator_parts = sql_parts(&shape)
            .iter()
            .filter(|s| s.seed.is_none())
            .count();
        if generator_parts != plan.generator_sqls.len() {
            measured.push(g.case.clone());
        }
    }
    measured.sort();
    let frozen: Vec<String> = SAME_SOURCE_GENERATOR_FAN_OUT
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        measured, frozen,
        "the same-source generator fan-out exception moved: expected {frozen:?} got {measured:?}"
    );
}

/// The six queries the issue names, with the part counts it states.
const NAMED_QUERIES: [(&str, usize); 6] = [
    (r#"{ span.http.method = "GET" }"#, 4),
    (r#"{ span.http.method = "GET" } | max(duration) > 1s"#, 4),
    (
        r#"{ resource.service.name = "grp" } | by(name) | count() > 2"#,
        3,
    ),
    (
        r#"{ resource.service.name = "grp" } | count() > 2 | by(name)"#,
        3,
    ),
    (
        r#"{ resource.service.name = "checkout" } | select(span.http.method)"#,
        4,
    ),
    (
        r#"{ resource.service.name = "checkout" || span.http.method = "GET" }"#,
        5,
    ),
];

/// Criterion 9: the six named queries plan as the issue states.
#[test]
fn the_six_named_queries_plan_as_the_issue_states() {
    for (q, want) in NAMED_QUERIES {
        let plan = plan_query(q, false);
        let shape = plan.plan_shape();
        let parts = sql_parts(&shape);
        let names: Vec<&str> = parts.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            parts.len(),
            want,
            "{q}: expected {want} SQL parts, got {}: {names:?}",
            parts.len()
        );
        // Every case's LAST SQL statement is the winners' root read, and
        // it is a source handoff keyed on the trace id.
        let last = parts.last().expect("a plan is never empty");
        let cut = last
            .cut
            .as_ref()
            .unwrap_or_else(|| panic!("{q}: the root read must carry a cut"));
        assert_eq!(cut.why, "source_handoff", "{q}");
        assert_eq!(cut.source.as_deref(), Some("trace_spans:root"), "{q}");
        assert_eq!(cut.key.as_deref(), Some("trace_id"), "{q}");
    }

    // The disjunction across two tables: its SECOND generator part is the
    // one that cannot share the first's `WHERE`.
    let plan = plan_query(NAMED_QUERIES[5].0, false);
    let shape = plan.plan_shape();
    let parts = sql_parts(&shape);
    let cut = parts[1]
        .cut
        .as_ref()
        .expect("the second generator carries a cut");
    assert_eq!(cut.why, "disjoint_sources");
    assert_eq!(cut.sources, vec!["trace_spans", "trace_attrs_idx"]);
    assert_eq!(parts[0].name, "trace_spans");
    assert_eq!(parts[1].name, "trace_attrs_idx");
}

/// Criterion 10: the two pipeline orderings send the same statements and
/// differ only in what our own process does with them.
///
/// This is the narrowest difference in the set: identical SQL, identical
/// part lists, told apart only by the link list.
#[test]
fn the_two_orderings_share_a_part_list_and_differ_in_the_link_list() {
    let by_then_count = plan_query(
        r#"{ resource.service.name = "grp" } | by(name) | count() > 2"#,
        false,
    );
    let count_then_by = plan_query(
        r#"{ resource.service.name = "grp" } | count() > 2 | by(name)"#,
        false,
    );
    let a = by_then_count.plan_shape();
    let b = count_then_by.plan_shape();
    let (pa, pb) = (sql_part_sources(&a), sql_part_sources(&b));
    let stages =
        |s: &PlanShape| -> Vec<String> { s.links.iter().map(|l| l.stage.clone()).collect() };
    let (sa, sb) = (stages(&a), stages(&b));
    assert!(
        pa == pb && sa != sb,
        "the two orderings must differ in links and agree in parts: parts {pa:?} vs {pb:?}, \
         links {sa:?} vs {sb:?}"
    );
}

/// Criterion 11: the chain length is an identity of the plan's own
/// counters — the scale-invariant form of "this adds no per-row work".
///
/// One link per statement the request may send and one per stage our own
/// process runs; nothing here counts rows, candidates or spans.
#[test]
fn the_chain_length_is_an_identity_of_the_plans_own_counters() {
    for g in goldens() {
        let plan = plan_query(&g.query, g.distributed);
        let shape = plan.plan_shape();
        let pipeline = pulsus_traceql::parse(&g.query)
            .expect("the golden's query parses")
            .pipeline
            .len();
        let identity = 1                                        // Source
            + usize::from(plan.has_structural_relation())
            + usize::from(plan.needs_nested_set())
            + plan.bool_truth_leaves()
            + pipeline
            + 1                                                 // Hydrate
            + plan.probes_len()
            + plan.agg_fields_len()
            + plan.select_attrs_len()
            + plan.event_sets_len()
            + usize::from(plan.needs_trace_ctx())
            + usize::from(plan.needs_child_counts())
            + 3; // Order, Limit, Emit
        let n = shape.links.len();
        assert_eq!(
            n, identity,
            "{}: chain length {n} != the identity's {identity}",
            g.case
        );
    }
}

/// Criterion 14: no part claims a keyset page loop, and none carries the
/// inexact-limit cut.
///
/// A TraceQL search pages over CANDIDATE BATCHES — a chunk driver on the
/// phase-2 reads — and the winners' root read is issued once, after the
/// limit is already satisfied. A page loop attached to it describes a
/// loop that does not exist.
#[test]
fn no_part_carries_the_keyset_driver_or_the_inexact_limit_cut() {
    for g in goldens() {
        let plan = plan_query(&g.query, g.distributed);
        let shape = plan.plan_shape();
        for (i, part) in sql_parts(&shape).iter().enumerate() {
            assert_ne!(
                part.issue, "per_seed:keyset",
                "{}: part {i} claims a keyset page loop; a TraceQL search pages over candidate \
                 batches, and the root read is issued once",
                g.case
            );
            assert_ne!(
                part.cut.as_ref().map(|c| c.why),
                Some("inexact_limit"),
                "{}: part {i} claims a keyset page loop; a TraceQL search pages over candidate \
                 batches, and the root read is issued once",
                g.case
            );
        }
    }
}

/// A validity gate for the six above: the corpus this target reads is the
/// committed one, it is not empty, and the section vocabulary it maps is
/// the whole vocabulary the goldens use.
///
/// Without it, a `goldens()` that silently returned nothing would make
/// four of the six tests pass over an empty loop.
#[test]
fn the_corpus_this_target_reads_is_the_committed_one() {
    let gs = goldens();
    assert_eq!(gs.len(), 56);
    let mut kinds: BTreeMap<String, usize> = BTreeMap::new();
    let mut total = 0usize;
    for g in &gs {
        for s in &g.sections {
            total += 1;
            let kind = match s.split_once('[') {
                Some((head, _)) => format!("{head}[i]"),
                None => s.clone(),
            };
            *kinds.entry(kind).or_insert(0) += 1;
        }
    }
    assert_eq!(total, 239, "the committed corpus renders 239 statements");
    assert_eq!(
        kinds.get("by() cardinality probe").copied().unwrap_or(0),
        1,
        "exactly one case carries the by() pre-flight probe, and it is the one statement the \
         chain deliberately does not name"
    );
    assert_eq!(
        kinds.len(),
        10,
        "the section vocabulary this target maps: {kinds:?}"
    );
}

/// The seed of the first statement after the generators names EVERY
/// generator it merges, not just the last one.
///
/// **The executor merges the generators' candidate lists and hydrates the
/// merged set** — `merge_candidates` in `traces::exec` — so a plan
/// crediting one of two generators describes a dependency the request
/// does not have. It is D1's family: a part naming something other than
/// what it reads.
///
/// The corpus makes the claim non-vacuous rather than an assumption: the
/// count of two-generator cases is asserted, so a corpus that lost them
/// would fail here instead of passing over cases that cannot discriminate.
#[test]
fn the_first_seeded_part_names_every_generator_it_merges() {
    let mut multi_generator_cases = 0usize;
    for g in goldens() {
        let plan = plan_query(&g.query, g.distributed);
        let shape = plan.plan_shape();
        // Generator parts open the plan: SQL parts with no seed.
        let generators: Vec<usize> = shape
            .parts
            .iter()
            .enumerate()
            .filter_map(|(i, p)| match p {
                PartShape::Sql(s) if s.seed.is_none() => Some(i),
                _ => None,
            })
            .collect();
        assert!(
            !generators.is_empty(),
            "{}: a plan always opens with at least one statement",
            g.case
        );
        if generators.len() > 1 {
            multi_generator_cases += 1;
        }
        let first_seeded = shape
            .parts
            .iter()
            .find_map(|p| match p {
                PartShape::Sql(s) if s.seed.is_some() => Some(s.as_ref()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{}: every search hydrates", g.case));
        let from = &first_seeded
            .seed
            .as_ref()
            .expect("filtered on Some above")
            .from;
        assert_eq!(
            *from, generators,
            "{}: the first seeded part draws from {generators:?} and the plan says {from:?}",
            g.case
        );
    }
    assert_eq!(
        multi_generator_cases,
        11 - SAME_SOURCE_GENERATOR_FAN_OUT.len(),
        "eight committed cases plan TWO generator parts — the eleven that send two generator \
         statements, less the three same-source ones the frozen exception collapses to one part. \
         Without them this gate could not tell a merged seed from a single one"
    );
}

/// The two shape invariants the design record states about `seed` and
/// `cut`, pinned so a third round does not find them stale.
///
/// They are NOT the same invariant, and the difference is the whole
/// point: **only the first part has no cut**, because the second and
/// later branches of a disjunction each carry `Cut::DisjointSources` —
/// while **several parts may have no seed**, because each of those
/// branches is a source statement that consumes nothing. Writing the
/// second by symmetry with the first is what put a false sentence in
/// `docs/query-lowering.md` (issue #492 part 3, code review round 2).
///
/// The unseeded parts are additionally asserted to be the LEADING run,
/// which is what "opens the plan" means: a plan that acquired an
/// unseeded statement in the middle would be describing a read with no
/// input and no predicate.
#[test]
fn only_the_first_part_has_no_cut_and_the_unseeded_parts_open_the_plan() {
    let mut cases_with_two_unseeded = 0usize;
    for g in goldens() {
        let plan = plan_query(&g.query, g.distributed);
        let shape = plan.plan_shape();
        let cutless: Vec<usize> = shape
            .parts
            .iter()
            .enumerate()
            .filter_map(|(i, p)| match p {
                PartShape::Sql(s) if s.cut.is_none() => Some(i),
                _ => None,
            })
            .collect();
        assert_eq!(
            cutless,
            vec![0],
            "{}: exactly one part may have no cut and it is the first; got {cutless:?}",
            g.case
        );
        let unseeded: Vec<usize> = shape
            .parts
            .iter()
            .enumerate()
            .filter_map(|(i, p)| match p {
                PartShape::Sql(s) if s.seed.is_none() => Some(i),
                _ => None,
            })
            .collect();
        assert_eq!(
            unseeded,
            (0..unseeded.len()).collect::<Vec<_>>(),
            "{}: the parts with no seed must be the leading run that OPENS the plan; got \
             {unseeded:?}",
            g.case
        );
        if unseeded.len() > 1 {
            cases_with_two_unseeded += 1;
        }
    }
    assert_eq!(
        cases_with_two_unseeded, 8,
        "eight committed cases open the plan with more than one statement; without them this \
         gate could not tell the seed rule from the cut rule"
    );
}
