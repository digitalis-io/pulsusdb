//! Issue #335 Stage D0: the served spanset-`by()` key set, plan-frozen
//! before the Stage D2 grammar change that rewrites the production.
//!
//! **Why this exists, and what it replaces.** Stage D2 narrows the spanset
//! `by()` to ONE operand and widens that operand from a bare `Field` to a
//! whole `FieldExpr`, matching `groupOperation` (`expr.y:177-179` @ Tempo
//! v3.0.2). The claim that comes with it is "no served query moved". The
//! evidence originally offered for that claim was `PINNED_SQL_CORPUS` in
//! `golden_sql_freeze.rs` being unchanged — and it is unchanged, but it is
//! **corroboration over three of the nineteen served by-key kinds**, not
//! proof: exactly three of its 69 frozen goldens exercise the search-side
//! `by()` (`spanset_by_service.sql`, `spanset_by_attr.sql`,
//! `spanset_by_root_service.sql`; the two `*_by_service` metrics goldens
//! are the `attributeList` production, which D2 does not touch).
//!
//! This is the gate that covers the whole served set. `docs/api.md` §4.2
//! enumerates nineteen by-key kinds PulsusDB groups by — five physical
//! columns, three nested-set intrinsics, ten trace-level intrinsics and
//! attributes — and every one of them gets its `SearchPlan` rendered and
//! byte-pinned here.
//!
//! **The pins were generated BEFORE Stage D2**, under `By { fields }`, so
//! the stage that changes the AST cannot generate the artefact it is
//! measured against. After D2 the same test re-derives them under
//! `By { key }`: a bare-field key must plan byte-identically, and if it
//! does not, this is red in D2's own diff.
//!
//! Hermetic — planning only, no ClickHouse. Regenerate deliberately with
//! `PULSUS_REGEN_BY_KEY_PLANS=1 cargo test -p pulsus-read --test
//! traces_by_key_plan_freeze` and review the diff.

use std::collections::BTreeMap;
use std::path::PathBuf;

use pulsus_read::SpanFilterCtx;
use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, plan_search};

/// Every by-key kind PulsusDB serves, exactly as `docs/api.md` §4.2
/// enumerates them: the five physical columns, the three nested-set
/// intrinsics, the ten trace-level intrinsics, and an attribute.
///
/// The count is in the type. `docs/api.md` also names the four EXCLUDED
/// keys — the span-event / span-link intrinsics, which are a clean `400`
/// because a span carries a *collection* of events and links and there is
/// no single scalar group value — and they are deliberately absent here:
/// this freezes what is SERVED.
const SERVED_BY_KEYS: [&str; 19] = [
    // physical columns
    "name",
    "resource.service.name",
    "duration",
    "status",
    "kind",
    // nested-set intrinsics
    "nestedSetParent",
    "nestedSetLeft",
    "nestedSetRight",
    // trace-level intrinsics
    "traceDuration",
    "rootName",
    "rootServiceName",
    "span:childCount",
    "span:id",
    "span:parentID",
    "trace:id",
    "statusMessage",
    "instrumentation:name",
    "instrumentation:version",
    // attributes
    ".a",
];

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join("traces_by_key_plans.json")
}

/// The same fixed, deterministic planner inputs the golden SQL suites use,
/// so only the by-key varies between rows.
fn render_plan(key: &str) -> String {
    let text = format!("{{ .x = 1 }} | by({key})");
    let query = pulsus_traceql::parse(&text)
        .unwrap_or_else(|e| panic!("{text:?} must parse — it is a served by-key: {e}"));
    pulsus_traceql::validate(&query).unwrap_or_else(|e| panic!("{text:?} must validate: {e}"));
    let plan = plan_search(
        &query,
        &SearchParams {
            start_ns: 1_700_000_000_000_000_000,
            end_ns: 1_700_010_800_000_000_000,
            limit: 20,
            spss: 3,
        },
        &SearchCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
            max_candidates: 100_000,
            max_series: 1_000,
            distributed: false,
        },
    )
    .unwrap_or_else(|e| panic!("{text:?} must plan — it is a SERVED by-key: {e}"));
    format!("{plan:#?}")
}

/// Every served by-key plans exactly as it did before the `by()`
/// production was rewritten.
///
/// *RED when:* any served by-key plans differently — which is what a
/// grammar change reaching the read path looks like. It is not a count to
/// refresh: regenerate only with the behaviour change written down.
#[test]
fn every_served_by_key_plans_as_it_did_before_the_grammar_change() {
    let mut rendered: BTreeMap<String, String> = BTreeMap::new();
    for key in SERVED_BY_KEYS {
        assert!(
            rendered.insert(key.to_string(), render_plan(key)).is_none(),
            "{key:?} is listed twice in SERVED_BY_KEYS — a duplicate silently shrinks the set \
             this freeze covers"
        );
    }

    let path = fixture_path();
    if std::env::var("PULSUS_REGEN_BY_KEY_PLANS").as_deref() == Ok("1") {
        std::fs::create_dir_all(path.parent().expect("golden dir")).expect("create golden dir");
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&rendered).expect("serialise") + "\n",
        )
        .expect("write fixture");
        eprintln!("regenerated {} ({} keys)", path.display(), rendered.len());
        return;
    }

    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "read {}: {e} — regenerate with PULSUS_REGEN_BY_KEY_PLANS=1 and review the diff",
            path.display()
        )
    });
    let pinned: BTreeMap<String, String> =
        serde_json::from_str(&raw).expect("traces_by_key_plans.json must parse");

    assert_eq!(
        pinned.keys().collect::<Vec<_>>(),
        rendered.keys().collect::<Vec<_>>(),
        "the pinned by-key set is not the served by-key set — a key added to SERVED_BY_KEYS \
         without its pin, or a pin left behind by a key that stopped being served"
    );
    let mut moved = Vec::new();
    for (key, plan) in &rendered {
        if pinned.get(key) != Some(plan) {
            moved.push(key.as_str());
        }
    }
    assert!(
        moved.is_empty(),
        "{} served by-key(s) now plan differently: {:?}. This is the read path moving under a \
         grammar change. Regenerate with PULSUS_REGEN_BY_KEY_PLANS=1 only once the behaviour \
         change is deliberate and written down",
        moved.len(),
        moved
    );
}

/// The freeze covers the set `docs/api.md` promises, not a sample of it.
/// A gate whose domain silently narrows is the defect this issue exists
/// to remove, so the arity is asserted rather than assumed.
#[test]
fn the_freeze_covers_all_nineteen_served_by_key_kinds() {
    assert_eq!(
        SERVED_BY_KEYS.len(),
        19,
        "docs/api.md §4.2 enumerates nineteen served by-key kinds; this freeze must cover every \
         one of them"
    );
}
