//! Issue #335 Stage B0: the WIRE-axis baseline for the accept-surface
//! audit, frozen before any grammar change, and re-derived from the tree
//! under test so it cannot have been measured against any other tree.
//!
//! **What the wire disposition is.** `accept_surface.rs` scores our side
//! as `parse ∘ validate` — the reference capture's own composition — but
//! the ruling on this issue is that the scoreboard must not report a win
//! the live system would not deliver: a query that parses and validates
//! can still be a planner 400 on the wire while the reference answers
//! 2xx. The wire disposition is therefore the full route's 400-surface,
//! `parse → validate → plan`, exactly as the server handlers compose it
//! (`traces_api/search.rs`: parse, `querytext::validate_semantics`,
//! `pulsus_read::plan_search`; `traces_api/metrics.rs`: the same shape
//! into `plan_trace_metrics`), with a probe routed to the planner of the
//! endpoint its reference verdict was captured against. Execution is not
//! part of the accept surface — a plan that renders is an accept.
//!
//! **Why this test exists — the B0 freeze, and the enumeration it closes
//! (plan v6–v10).** `wire_baseline.json` holds the per-probe disposition
//! plus `WIRE_AGREE_BASELINE`/`WIRE_DIVERGE_BASELINE`, captured BEFORE
//! any #335 grammar work, so later stages are measured against a fixed
//! number rather than one that moves with them. A source-free commit
//! proves only the commit's contents, not the tree the numbers came from
//! — the revert trick (edit, measure, revert, commit the numbers)
//! survives it. This test closes that: every disposition is RE-DERIVED
//! through the live route from the tree under test, per probe, and the
//! two constants are asserted against the totals of the DERIVED
//! dispositions joined with `matrix.json`'s reference verdicts — never
//! by counting the committed values against themselves. Numbers taken
//! against any other tree fail here in B0's own PR. The remaining
//! ordering questions (who may create the file, whether it can move
//! later, and that no grammar work lands first) are the CI freeze gates
//! in the `wire-baseline-freeze` job — the four checks are one
//! enumeration, not four precautions.
//!
//! **The baseline is directional, not immutable** (issue #335 Stage C).
//! It may move only toward FEWER divergences: per probe, nothing that
//! agreed on `origin/main` may diverge here, and neither the divergence
//! count nor the probe set may grow. That is what a legitimate
//! improvement looks like — Stage C's own re-pin is one, `avg((.a))`
//! reject→accept — and a decrease cannot express the thing the freeze
//! exists to stop, which is numbers measured against already-changed
//! code so broken code agrees with itself. This test is the half that
//! makes the committed values the TREE's values; the CI job is the half
//! that judges the direction they moved.
//!
//! **Why it lives in `pulsus-read`:** `parse → validate → plan` is only
//! reachable here (`pulsus-traceql` cannot depend on the planners), so
//! the fixtures are read across crates from
//! `pulsus-traceql/tests/accept_surface/`. The planner inputs are the
//! fixed deterministic shapes the golden suites pin
//! (`traces_search_sql.rs` / `traces_metrics_sql.rs`), so the derivation
//! is hermetic and reproducible; only the query varies per probe.

use std::path::PathBuf;

use pulsus_read::SpanFilterCtx;
use pulsus_read::traces::metrics_plan::{MetricsCtx, MetricsParams, plan_trace_metrics};
use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, plan_search};
use serde::Deserialize;

/// The frozen baseline: the two constants and the per-probe dispositions,
/// and NOTHING else — `deny_unknown_fields` at both levels makes the
/// "nothing else in that file" requirement a parse failure rather than a
/// review habit.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
struct WireBaseline {
    WIRE_AGREE_BASELINE: usize,
    WIRE_DIVERGE_BASELINE: usize,
    wire_baseline: Vec<WireProbe>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireProbe {
    query: String,
    /// `accept` | `reject` — the full route's verdict for this probe.
    pulsus_wire: String,
}

/// The reference half of the audit matrix — only the fields this test
/// joins on; the rest belong to `accept_surface.rs`.
#[derive(Deserialize)]
struct Matrix {
    accept_surface_probes: Vec<MatrixProbe>,
}

#[derive(Deserialize)]
struct MatrixProbe {
    query: String,
    reference: String,
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../pulsus-traceql/tests/accept_surface")
}

fn read_json<T: serde::de::DeserializeOwned>(name: &str) -> T {
    let path = fixture_dir().join(name);
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("{name} must parse: {e}"))
}

/// Mirrors `accept_surface.rs::is_metrics` — the endpoint the probe's
/// reference verdict was captured against, and therefore the planner its
/// wire disposition must route through. Duplicated across the crate
/// boundary because neither test crate can depend on the other; the
/// oracle leg over there and the totals here would both drift loudly if
/// the two copies diverged.
fn is_metrics(q: &str) -> bool {
    ["rate(", "_over_time(", "compare(", "topk(", "bottomk("]
        .iter()
        .any(|m| q.contains(m))
}

/// The full route, hermetically: `parse → validate → plan`, with the
/// golden suites' fixed deterministic planner inputs. Everything here is
/// the tree under test — nothing is read from the baseline being
/// checked.
fn wire_accepts(q: &str) -> bool {
    let Ok(query) = pulsus_traceql::parse(q) else {
        return false;
    };
    if pulsus_traceql::validate(&query).is_err() {
        return false;
    }
    let filter = SpanFilterCtx {
        spans_table: "trace_spans",
        attrs_table: "trace_attrs_idx",
    };
    if is_metrics(q) {
        plan_trace_metrics(
            &query,
            &MetricsParams {
                start_ns: 1_700_000_000_000_000_000,
                end_ns: 1_700_010_800_000_000_000,
                step_s: 60,
            },
            &MetricsCtx {
                filter,
                scan_budget_rows: 50_000_000,
                max_series: 1_000,
                distributed: false,
                skip_unavailable_shards: false,
            },
        )
        .is_ok()
    } else {
        plan_search(
            &query,
            &SearchParams {
                start_ns: 1_700_000_000_000_000_000,
                end_ns: 1_700_010_800_000_000_000,
                limit: 20,
                spss: 3,
            },
            &SearchCtx {
                filter,
                max_candidates: 100_000,
                max_series: 1_000,
                distributed: false,
            },
        )
        .is_ok()
    }
}

/// The B0 mechanism (plan v6): the committed baseline must REPRODUCE
/// from the tree under test — per probe, and the constants against the
/// derived totals — so a baseline measured against any other tree fails
/// in its own PR.
#[test]
fn every_committed_wire_verdict_is_reproduced_by_the_planner() {
    let baseline: WireBaseline = read_json("wire_baseline.json");
    let matrix: Matrix = read_json("matrix.json");

    // Coverage first: one baseline entry per matrix probe, in matrix
    // order, no extras and no omissions — a baseline that quietly covers
    // a different probe set is not a baseline for this audit.
    assert_eq!(
        baseline.wire_baseline.len(),
        matrix.accept_surface_probes.len(),
        "the baseline must cover every accept-surface probe"
    );
    for (b, m) in baseline
        .wire_baseline
        .iter()
        .zip(&matrix.accept_surface_probes)
    {
        assert_eq!(
            b.query, m.query,
            "baseline order must be the matrix's probe order"
        );
    }

    // Per probe: the committed disposition must be what the live route
    // derives today. Aggregates are NOT the assertion — two errors that
    // cancel in the totals still fail here, probe by probe.
    let mut drift = Vec::new();
    let mut derived_agree = 0usize;
    let mut derived_diverge = 0usize;
    for (b, m) in baseline
        .wire_baseline
        .iter()
        .zip(&matrix.accept_surface_probes)
    {
        let derived = if wire_accepts(&b.query) {
            "accept"
        } else {
            "reject"
        };
        if derived == m.reference {
            derived_agree += 1;
        } else {
            derived_diverge += 1;
        }
        if derived != b.pulsus_wire {
            drift.push(format!(
                "{:?}: committed pulsus_wire={} but the route derives {derived}",
                b.query, b.pulsus_wire
            ));
        }
    }
    assert!(
        drift.is_empty(),
        "{} probe(s) do not reproduce from this tree — the committed baseline was measured \
         against a different tree, or the route changed under it. A deliberate re-pin \
         re-records these values in the same change, and the CI freeze gate then decides \
         whether the move was toward FEWER divergences (allowed) or more (never):\n{}",
        drift.len(),
        drift.join("\n")
    );

    // The constants, against the DERIVED totals — never against counts
    // of the committed values, which would compare the file to itself.
    assert_eq!(
        derived_agree, baseline.WIRE_AGREE_BASELINE,
        "WIRE_AGREE_BASELINE does not reproduce from this tree"
    );
    assert_eq!(
        derived_diverge, baseline.WIRE_DIVERGE_BASELINE,
        "WIRE_DIVERGE_BASELINE does not reproduce from this tree"
    );
    assert_eq!(
        baseline.WIRE_AGREE_BASELINE + baseline.WIRE_DIVERGE_BASELINE,
        baseline.wire_baseline.len(),
        "the two constants must partition the probe set"
    );
}
