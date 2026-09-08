//! Issue #492 part 7: **`select()` is refused, and these are the two
//! sentences of the record that can stop being true.**
//!
//! Part 7 measured whether a `select()` projection can be compiled into
//! the statements a TraceQL search already sends. It cannot, for the
//! query the scope enumeration names, and part 7 changes no production
//! line — the outcome is a record, `docs/query-lowering.md` §9.8. Two of
//! that record's sentences are about what the planner does today, and
//! this file is what makes them fail if the planner stops doing it.
//!
//! # Claim 1 — the named query sends exactly one attribute-index read
//!
//! `{ resource.service.name = "checkout" } | select(span.http.method)`
//! plans four SQL parts, and only one of them reads `trace_attrs_idx`:
//! the `select()` value read itself. The other three read `trace_spans`
//! — the generator, the per-batch hydration, and the winners' root read.
//!
//! That single read is the whole refusal. A projection has to put an
//! attribute value beside a span, and the three `trace_spans` statements
//! have no attribute value in them; putting one there means reading a
//! second table inside one statement, which is a join, and ADR 0008
//! names no join clause. There is no second attribute-index read to
//! merge this one into, because there is only one.
//!
//! [`the_named_select_query_reads_the_attribute_index_exactly_once`] is
//! that sentence as a check. It reddens if a later change gives the
//! query a second attribute-index read — at which point the merge §9.8
//! measures becomes available and the refusal has to be revisited.
//!
//! # Claim 2 — it is the only committed `select()` case in that position
//!
//! Of the seven committed `traces_search` goldens that render a
//! `phase2 select values[…]` section, six also render at least one other
//! per-batch `trace_attrs_idx` read — a membership read, an aggregate
//! value read or an event set read — and exactly one does not.
//! [`exactly_one_committed_select_case_has_no_merge_partner`] asserts
//! both lists by name, so the claim fails whichever way it stops being
//! true: a seventh case losing its partner, or the one case gaining one.
//!
//! # Where this stops
//!
//! Both tests read the compiled plan and the committed goldens. They say
//! what the planner emits; they say nothing about what any of it costs.
//! Every figure in §9.8 was taken on a corpus built from the recipe
//! printed there, on a container that no longer exists, and none of it is
//! checkable in CI. The recipe is what makes those figures re-takeable;
//! this file is not.

use std::collections::BTreeMap;
use std::fs;

use pulsus_read::compile::plan::{PartShape, PlanShape};
use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, plan_search};
use pulsus_read::{SearchPlan, SpanFilterCtx};

/// The query the scope enumeration names, and the one §9.8's refusal is
/// about.
const NAMED_QUERY: &str = r#"{ resource.service.name = "checkout" } | select(span.http.method)"#;

/// The same fixed window the golden suite plans against, so a part list
/// taken here is the part list the goldens froze.
const PARAMS: SearchParams = SearchParams {
    start_ns: 1_700_000_000_000_000_000,
    end_ns: 1_700_010_800_000_000_000,
    limit: 20,
    spss: 3,
};

const MAX_CANDIDATES: u64 = 100_000;

fn plan_query(q: &str) -> SearchPlan {
    let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("{q}: {e}"));
    plan_search(
        &query,
        &PARAMS,
        &SearchCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
            max_candidates: MAX_CANDIDATES,
            max_series: 1_000,
            distributed: false,
        },
    )
    .unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

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

/// The four statements the named query sends, in plan order, and the one
/// of them that reads the attribute index.
///
/// The whole list is asserted rather than the count alone: a plan that
/// dropped the hydration read and grew a second attribute read would
/// keep the count at four and would be a different query.
#[test]
fn the_named_select_query_reads_the_attribute_index_exactly_once() {
    let plan = plan_query(NAMED_QUERY);
    let sources = sql_part_sources(&plan.plan_shape());
    assert_eq!(
        sources,
        vec![
            "trace_spans".to_string(),
            "trace_spans:hydration".to_string(),
            "trace_attrs_idx:values".to_string(),
            "trace_spans:root".to_string(),
        ],
        "{NAMED_QUERY}: the four statements this query sends"
    );
    let attrs_reads: Vec<&String> = sources
        .iter()
        .filter(|s| s.starts_with("trace_attrs_idx"))
        .collect();
    assert_eq!(
        attrs_reads.len(),
        1,
        "{NAMED_QUERY}: the select() value read is the query's ONLY attribute-index read, so \
         there is nothing to merge it with and the projection cannot compile without a join. \
         Reads: {attrs_reads:?}"
    );
}

/// The per-batch `trace_attrs_idx` sections a committed `traces_search`
/// golden can render. A `select()` case whose only entry here is its own
/// value read has no statement to merge into.
const ATTRS_READ_SECTIONS: [&str; 4] = [
    "== phase2 select values[",
    "== phase2 aggregate values[",
    "== phase2 membership[",
    "== phase2 event set[",
];

/// Exactly one committed `select()` case sends its value read alone.
///
/// Both name lists are asserted, and so is the number of goldens walked,
/// so a walk that silently covered a subset cannot pass.
#[test]
fn exactly_one_committed_select_case_has_no_merge_partner() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_search");
    let mut paths: Vec<std::path::PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
        .map(|e| e.expect("dir entry").path())
        .collect();
    paths.sort();

    let mut scanned = 0usize;
    // Case name -> how many per-batch attribute-index reads it renders,
    // for the cases that render a `select()` value read at all.
    let mut classification: BTreeMap<String, usize> = BTreeMap::new();
    for path in paths {
        scanned += 1;
        let name = path
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or_else(|| panic!("non-UTF-8 golden name: {}", path.display()))
            .to_string();
        let text =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let mut reads = 0usize;
        let mut has_select = false;
        for line in text.lines() {
            for section in ATTRS_READ_SECTIONS {
                if line.starts_with(section) {
                    reads += 1;
                    if section == ATTRS_READ_SECTIONS[0] {
                        has_select = true;
                    }
                }
            }
        }
        if has_select {
            classification.insert(name, reads);
        }
    }

    let alone: Vec<&String> = classification
        .iter()
        .filter(|(_, n)| **n == 1)
        .map(|(k, _)| k)
        .collect();
    let partnered: Vec<&String> = classification
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|(k, _)| k)
        .collect();

    assert_eq!(
        alone,
        vec!["issue492_select_span_attr.sql"],
        "exactly one committed select() case sends its value read alone, and it is the query the \
         scope enumeration names. Classification: {classification:?}"
    );
    assert_eq!(
        partnered,
        vec![
            "agg_and_select.sql",
            "event_name_vs_attr.sql",
            "event_time_since_start_vs_attr.sql",
            "issue492_by_attr_then_count.sql",
            "rhs_attr.sql",
            "spanset_by_attr.sql",
        ],
        "every other committed select() case already sends a second attribute-index read, which \
         is what the join-free merge §9.8 measures would share. Classification: {classification:?}"
    );
    assert_eq!(
        scanned, 72,
        "every committed traces_search golden is walked; a subset walk cannot pass"
    );
}
