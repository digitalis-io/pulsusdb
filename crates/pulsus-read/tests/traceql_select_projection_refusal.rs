//! Issue #492 part 7 recorded that a `select()` projection could not be
//! compiled into the statements a TraceQL search already sends, and
//! `docs/query-lowering.md` §9.8 is that record. **Issue #558 removed the
//! premise.** This file is what the two claims of that record became once
//! they stopped being true.
//!
//! # What the refusal rested on, and what happened to it
//!
//! Part 7's argument was: a projection has to put an attribute value
//! beside a span; the three `trace_spans` statements have no attribute
//! value in them; putting one there means reading a SECOND TABLE inside
//! one statement, which is a join, and ADR 0008 names no join clause.
//!
//! The value is no longer in a second table. Since issue #557 the span
//! row's own `attr_key`/`attr_scope`/`attr_val`/`attr_type` arrays are
//! what an attribute condition reads, and issue #558 reads a projected
//! field's value, its numeric reading and its stored kind from the same
//! arrays, at one located element. No table was joined: the arrays were
//! already on the row the hydration statement fetches.
//!
//! # Claim 1 — the named query now sends three statements and no
//! attribute-index read at all
//!
//! `{ resource.service.name = "checkout" } | select(span.http.method)`
//! planned four SQL parts, of which one read `trace_attrs_idx`: the
//! `select()` value read. It plans three now — the generator, the
//! per-batch hydration and the winners' root read — and every one of them
//! reads `trace_spans`.
//! [`the_named_select_query_reads_the_attribute_index_exactly_once`]
//! asserts the whole list, so it reddens whichever way the statement set
//! moves.
//!
//! # Claim 2 — no committed golden renders a value-read section
//!
//! The old claim counted the committed `traces_search` goldens that
//! render a `phase2 select values[…]` section and paired them with the
//! ones that also render an aggregate or event-set read, because a case
//! with no partner had nothing to merge into. There is no such section
//! left to count.
//! [`exactly_one_committed_select_case_has_no_merge_partner`] is now the
//! assertion that **no** committed golden renders
//! `== phase2 select values[` or `== phase2 aggregate values[`, over all
//! 75 files, with the count of files walked asserted so a subset walk
//! cannot pass.
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
            recent_table: "trace_recent",
            errors_table: "trace_error_spans",
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

/// The three statements the named query sends, in plan order, none of
/// which reads the attribute index (issue #558).
///
/// The whole list is asserted rather than the count alone: a plan that
/// dropped the hydration read and grew an attribute read would keep the
/// count at three and would be a different query.
#[test]
fn the_named_select_query_reads_the_attribute_index_exactly_once() {
    let plan = plan_query(NAMED_QUERY);
    let sources = sql_part_sources(&plan.plan_shape());
    assert_eq!(
        sources,
        vec![
            "trace_spans".to_string(),
            "trace_spans:hydration".to_string(),
            "trace_spans:root".to_string(),
        ],
        "{NAMED_QUERY}: the three statements this query sends"
    );
    let attrs_reads: Vec<&String> = sources
        .iter()
        .filter(|s| s.starts_with("trace_attrs_idx"))
        .collect();
    assert!(
        attrs_reads.is_empty(),
        "{NAMED_QUERY}: the select() value is a projected slot on the hydration statement, so \
         this query sends no attribute-index read at all and §9.8's premise — that the value \
         lives in a second table — no longer holds. Reads: {attrs_reads:?}"
    );
    // The field is still PLANNED as its own slot; what is gone is its
    // statement. Without this a planner that dropped the `select()`
    // entirely would satisfy the list above.
    assert_eq!(
        plan.select_attrs_len(),
        1,
        "{NAMED_QUERY}: the select() field is still planned"
    );
}

/// The two value-read sections issue #558 removed. No committed
/// `traces_search` golden renders either one.
const VALUE_READ_SECTIONS: [&str; 2] = ["== phase2 select values[", "== phase2 aggregate values["];

/// No committed golden renders a `select()` or aggregate value-read
/// section (issue #558).
///
/// The number of goldens walked is asserted too, so a walk that silently
/// covered a subset — or none — cannot pass.
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
    // Case name -> the value-read sections it renders. Empty for every
    // case, which is the claim.
    let mut offenders: BTreeMap<String, Vec<String>> = BTreeMap::new();
    // The control: the ONE per-batch attribute section a golden can still
    // render, counted so a scanner that read nothing cannot pass.
    let mut event_set_sections = 0usize;
    for path in paths {
        scanned += 1;
        let name = path
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or_else(|| panic!("non-UTF-8 golden name: {}", path.display()))
            .to_string();
        let text =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        for line in text.lines() {
            if line.starts_with("== phase2 event set[") {
                event_set_sections += 1;
            }
            for section in VALUE_READ_SECTIONS {
                if line.starts_with(section) {
                    offenders
                        .entry(name.clone())
                        .or_default()
                        .push(line.to_string());
                }
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "issue #558 removed both value-read statements, so no committed golden renders either \
         section. Found: {offenders:?}"
    );
    assert_eq!(
        event_set_sections, 3,
        "the three event-set sections are the control: a scanner that read nothing would \
         satisfy the emptiness above"
    );
    assert_eq!(
        scanned, 75,
        "every committed traces_search golden is walked; a subset walk cannot pass"
    );
}
