//! Issue #458 AC 9: the two ledger entries and the accept fixture must
//! describe the SAME divergences, and each entry must carry the facts it
//! claims to.
//!
//! # Why this is `assert_eq!` on two sets, not `contains`
//!
//! The existing ledger tests in this repo are one-way `contains` checks,
//! and that pattern was **measured** during planning to accept an extra
//! ledger entry with no corresponding fixture case — it reported
//! `2 passed, 0 failed`. A one-way check answers "is this named
//! somewhere", which is not the claim. The claim is a RELATION: the class
//! ids named in `traceql-metrics-filter-residual-refusals` are exactly
//! the divergence classes the fixture records. So this builds two
//! `BTreeSet`s and compares them, and prints the symmetric difference in
//! both directions when they disagree.
//!
//! Hermetic: reads two committed files, runs no query and needs no
//! container.

use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    probes: Vec<Probe>,
}

#[derive(Deserialize)]
struct Probe {
    query: String,
    ours: Side,
    divergence: Option<String>,
}

#[derive(Deserialize)]
struct Side {
    verdict: String,
    #[allow(dead_code)]
    body: Option<String>,
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn ledger() -> String {
    let path = workspace_root().join("docs/benchmarks/traces-differential-ledger.md");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

fn fixture() -> Fixture {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("metrics_filter_accept.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

/// Collapses every run of whitespace to one space. The ledger is
/// hard-wrapped prose, so a sentence a test needs to find is split across
/// lines at a column nobody chose deliberately; squashing makes the needle
/// a property of the sentence rather than of the wrap.
fn squash(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The body of one `### \`<id>\`` section, up to the next `### `.
fn entry_body<'a>(ledger: &'a str, id: &str) -> &'a str {
    let marker = format!("### `{id}`");
    let start = ledger
        .find(&marker)
        .unwrap_or_else(|| panic!("docs/benchmarks/traces-differential-ledger.md has no {marker}"));
    let rest = &ledger[start + marker.len()..];
    match rest.find("\n### ") {
        Some(end) => &rest[..end],
        None => rest,
    }
}

#[test]
fn the_residual_refusal_entry_names_exactly_the_fixtures_divergence_classes() {
    let ledger = ledger();
    let raw_body = entry_body(&ledger, "traceql-metrics-filter-residual-refusals");
    let body = squash(raw_body);
    let f = fixture();

    // From the fixture: the classes on probes we reject and that carry a
    // divergence id (the reference accepts them — the accept suite proves
    // that half separately, from the live oracle).
    let from_fixture: BTreeSet<String> = f
        .probes
        .iter()
        .filter(|p| p.ours.verdict == "reject")
        .filter_map(|p| p.divergence.clone())
        .collect();
    assert!(
        !from_fixture.is_empty(),
        "the fixture must record at least one divergence class"
    );

    // From the ledger: every `metrics-filter-…` class id the entry names.
    // Scanned out of the entry body rather than hand-listed, so the two
    // sides are derived from their own files and can genuinely disagree.
    let mut from_ledger: BTreeSet<String> = BTreeSet::new();
    for chunk in raw_body.split('`') {
        if chunk.starts_with("metrics-filter-")
            && chunk
                .chars()
                .all(|c| c.is_ascii_lowercase() || c == '-' || c.is_ascii_digit())
        {
            from_ledger.insert(chunk.to_string());
        }
    }

    let only_ledger: Vec<&String> = from_ledger.difference(&from_fixture).collect();
    let only_fixture: Vec<&String> = from_fixture.difference(&from_ledger).collect();
    assert_eq!(
        from_ledger, from_fixture,
        "the ledger entry and the accept fixture must name the SAME divergence classes.\n  \
         in the ledger only: {only_ledger:?}\n  in the fixture only: {only_fixture:?}"
    );

    // Every class's exact 400 body appears in the entry, so the table
    // cannot name a class while quoting a message the code stopped
    // producing.
    for probe in f.probes.iter().filter(|p| p.divergence.is_some()) {
        let body_text =
            probe.ours.body.as_deref().unwrap_or_else(|| {
                panic!("{:?}: a rejecting probe must carry its body", probe.query)
            });
        assert!(
            body.contains(body_text),
            "the residual entry must quote the exact 400 body for {:?}:\n  {body_text}",
            probe.query
        );
    }
}

/// The five facts the window entry must carry, each asserted
/// individually. An entry that merely EXISTS satisfies nothing here.
#[test]
fn the_root_window_entry_carries_all_five_of_its_required_facts() {
    let ledger = ledger();
    let body = squash(entry_body(
        &ledger,
        "traceql-metrics-nestedsetparent-root-window",
    ));

    // 1. The measurement, with route AND window.
    for needle in [
        "GET /api/search?q={nestedSetParent<0 && resource.service.name=\"orphan\"}\
         &start=<T+100>&end=<T+400>&limit=10",
        "0300000000000002",
        "grafana/tempo@sha256:aa8df8d0",
    ] {
        assert!(
            body.contains(needle),
            "fact 1 (the measurement, with route and window) must state {needle:?}"
        );
    }
    // 2. Which side is right, with the source rule.
    assert!(
        body.contains("nested_set_model.go:11-12,57")
            && body.contains("Which side is right: the reference"),
        "fact 2 must name the reference as right and cite the rule"
    );
    // 3. Which of OUR surfaces is right.
    assert!(
        body.contains("Which of our surfaces is right: the metrics route")
            && body.contains("parent_id = <all-zero>"),
        "fact 3 must name the metrics route as the correct one, with its lowering"
    );
    // 4. The open class this belongs to, named.
    assert!(
        body.contains("nestedset_value_differential.rs"),
        "fact 4 must name the open window-clipping class by its suite"
    );
    // 5. That the split is temporary and closes on the SEARCH side.
    assert!(
        body.contains("closes on the SEARCH side")
            && body.contains("never** by the metrics route regressing"),
        "fact 5 must say the split is temporary and which side closes it"
    );
}

/// The residual entry is a GAP record and says so — the distinction the
/// task-manager's ruling turns on (the reference has no metrics-specific
/// filter guard, so these are things we do not do yet, not judgements we
/// made differently).
#[test]
fn the_residual_entry_states_that_it_records_gaps_rather_than_divergences_of_judgement() {
    let ledger = ledger();
    let body = squash(entry_body(
        &ledger,
        "traceql-metrics-filter-residual-refusals",
    ));
    assert!(
        body.contains("pkg/traceql/engine.go:31-48"),
        "the entry must cite why the reference serves all of them"
    );
    assert!(
        squash(&ledger).contains(
            "### `traceql-metrics-filter-residual-refusals` (issue #458, wave 1) — \
             **a GAP record, not a divergence**"
        ),
        "the entry heading must say it is a gap record"
    );
}

/// AC11 (issue #477): the five bucket-geometry/exemplar ledger entries
/// exist, each names the endpoint it is about, and each carries the fact
/// its own disposition rests on.
///
/// The endpoint is asserted because a ledger row without a route goes
/// stale invisibly: the reference answers the same query differently on
/// the range and the instant routes, so "the reference does X" is not a
/// claim until it says where.
#[test]
fn the_five_metrics_geometry_ledger_entries_each_name_their_endpoint() {
    let ledger = ledger();
    // (id, a needle from the entry's own measured content)
    const ENTRIES: [(&str, &str); 5] = [
        (
            "traceql-metrics-fractional-ms-step-rejected",
            // The counterexample that says the bound is "not a whole
            // millisecond" rather than "sub-millisecond".
            "`100.25ms` is far above one millisecond",
        ),
        (
            "traceql-metrics-end-cutoff-unadopted",
            // The transition is not a fixed point, and nothing reads it.
            "not a fixed point",
        ),
        (
            "traceql-metrics-density-by-function",
            // Recorded as MATCHED so nobody densifies the aggregations.
            "so nobody \"fixes\" the value aggregations into density",
        ),
        (
            "traceql-metrics-zero-fill-without-a-block",
            // Worded as introduced BY this change, not as pre-existing.
            "introduced deliberately by issue #477",
        ),
        (
            "traceql-metrics-exemplar-count-not-a-parity-surface",
            // The count is a sampler's output and is not gated.
            "would make a **correct** implementation fail",
        ),
    ];
    for (id, needle) in ENTRIES {
        let body = squash(entry_body(&ledger, id));
        assert!(
            body.contains("/api/traces/v1/metrics/query_range"),
            "{id}: a ledger row must name the endpoint it is about"
        );
        assert!(
            body.contains(&squash(needle)),
            "{id}: the entry does not carry {needle:?}"
        );
    }
    // The hint's unit change is ledgered separately (ruling 1 on issue
    // #477): it is a behaviour change for existing users, not a
    // divergence from the reference.
    let unit = squash(entry_body(&ledger, "traceql-metrics-exemplars-total-budget"));
    assert!(unit.contains("/api/traces/v1/metrics/query_range"));
    assert!(
        unit.contains("used to mean N exemplars **per bucket**"),
        "the entry must state what changed, not only what is true now"
    );
}
