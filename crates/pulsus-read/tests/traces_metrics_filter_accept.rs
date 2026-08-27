//! Issue #458 AC 7: the metrics-filter accept surface, pinned on **both**
//! sides.
//!
//! One entry per probe in `fixtures/metrics_filter_accept.json` carrying
//! the query, our verdict (with the exact 400 body when we reject), the
//! reference's verdict (same), and a `divergence` class id or `null`.
//! Two tests:
//!
//! * **hermetic** — re-derives `ours` from the tree under test through
//!   `parse → validate → plan_trace_metrics`, exactly as
//!   `traces_api/metrics.rs` composes it, and asserts it equals the
//!   committed value for every probe. A committed verdict measured
//!   against any other tree fails in its own PR
//!   (`accept_surface_wire.rs`'s mechanism).
//! * **oracle** — gated on `PULSUSDB_TEMPO_DIFF_URL`, re-derives
//!   `reference` live from the digest-pinned container and asserts it
//!   equals the committed value. It is FAIL-CLOSED via
//!   `require_live_gate`: in a live CI job with the `env:` block dropped
//!   it panics rather than skipping green (issue #320).
//!
//! # What this suite cannot see, said plainly
//!
//! **A refusal turned into "accept everything" keeps its `accept`
//! disposition and passes here unchanged.** That is the break that
//! motivated issue #458 — `LeafEval::NestedSet { .. } => Ok("1")` — and
//! no accept-surface fixture can catch it, because the accept surface
//! does not move. `traces_metrics_nested_set_live.rs` (AC 3b, the answer
//! identity) and `traces_metrics_sql.rs` (AC 5, the golden SQL bytes) are
//! the criteria that can. A gate that pretended otherwise would be worse
//! than an honest limit.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use pulsus_read::SpanFilterCtx;
use pulsus_read::traces::metrics_plan::{MetricsCtx, MetricsParams, plan_trace_metrics};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
struct Fixture {
    /// The number of probes where we reject and the reference accepts.
    /// Committed so the gap can only shrink deliberately.
    DIVERGENCE_COUNT: usize,
    capture: Capture,
    limits: BTreeMap<String, String>,
    probes: Vec<Probe>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Capture {
    reference_image: String,
    reference_config: String,
    route: String,
    window: String,
    note: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Probe {
    query: String,
    ours: Side,
    reference: Side,
    divergence: Option<String>,
    why: String,
}

#[derive(Deserialize, PartialEq, Eq, Debug)]
#[serde(deny_unknown_fields)]
struct Side {
    verdict: String,
    body: Option<String>,
}

fn fixture() -> Fixture {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("metrics_filter_accept.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

/// The full metrics route, hermetically: `parse → validate →
/// plan_trace_metrics`, with the golden suite's fixed deterministic
/// planner inputs. Nothing is read from the fixture being checked.
fn derive_ours(q: &str) -> Side {
    let reject = |body: String| Side {
        verdict: "reject".to_string(),
        body: Some(body),
    };
    let query = match pulsus_traceql::parse(q) {
        Ok(v) => v,
        Err(e) => return reject(e.to_string()),
    };
    if let Err(e) = pulsus_traceql::validate(&query) {
        return reject(e.to_string());
    }
    match plan_trace_metrics(
        &query,
        &MetricsParams {
            start_ns: 1_700_000_000_000_000_000,
            end_ns: 1_700_010_800_000_000_000,
            step_s: 60,
        },
        &MetricsCtx {
            filter: SpanFilterCtx {
                spans_table: "trace_spans",
                attrs_table: "trace_attrs_idx",
            },
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            distributed: false,
            skip_unavailable_shards: false,
        },
    ) {
        Ok(_) => Side {
            verdict: "accept".to_string(),
            body: None,
        },
        Err(e) => reject(e.to_string()),
    }
}

#[test]
fn every_committed_verdict_of_ours_is_reproduced_by_the_planner() {
    let f = fixture();
    assert!(!f.probes.is_empty(), "the fixture must carry probes");
    let mut drifted = Vec::new();
    let mut seen = BTreeSet::new();
    for probe in &f.probes {
        assert!(
            seen.insert(probe.query.clone()),
            "duplicate probe {:?}",
            probe.query
        );
        assert!(
            !probe.why.trim().is_empty(),
            "{:?}: every probe states why it is in the set",
            probe.query
        );
        let derived = derive_ours(&probe.query);
        if derived != probe.ours {
            drifted.push(format!(
                "{:?}\n  committed: {:?}\n  derived:   {derived:?}",
                probe.query, probe.ours
            ));
        }
    }
    assert!(
        drifted.is_empty(),
        "{} of {} committed verdicts do not reproduce from this tree:\n{}",
        drifted.len(),
        f.probes.len(),
        drifted.join("\n")
    );

    // The divergence count is asserted against the DERIVED join, never
    // against the committed values counted among themselves.
    let derived_divergences: BTreeSet<&str> = f
        .probes
        .iter()
        .filter(|p| derive_ours(&p.query).verdict == "reject" && p.reference.verdict == "accept")
        .map(|p| p.query.as_str())
        .collect();
    assert_eq!(
        derived_divergences.len(),
        f.DIVERGENCE_COUNT,
        "DIVERGENCE_COUNT is {} but the derived join has {}: {derived_divergences:?}",
        f.DIVERGENCE_COUNT,
        derived_divergences.len()
    );

    // `divergence` is a class id exactly on the divergent probes — not a
    // free-text annotation someone can leave stale on either side.
    let mut label_errors = Vec::new();
    for probe in &f.probes {
        let is_divergent = derived_divergences.contains(probe.query.as_str());
        match (&probe.divergence, is_divergent) {
            (Some(_), true) | (None, false) => {}
            (Some(c), false) => label_errors.push(format!(
                "{:?} carries divergence {c:?} but the two sides agree",
                probe.query
            )),
            (None, true) => label_errors.push(format!(
                "{:?} diverges (we reject, the reference accepts) with no class id",
                probe.query
            )),
        }
    }
    assert!(label_errors.is_empty(), "{}", label_errors.join("\n"));

    // The capture provenance is present — a fixture whose reference column
    // has no route and no window is not checkable (a routeless row cost
    // three review rounds on #294).
    for (name, value) in [
        ("reference_image", &f.capture.reference_image),
        ("reference_config", &f.capture.reference_config),
        ("route", &f.capture.route),
        ("window", &f.capture.window),
        ("note", &f.capture.note),
    ] {
        assert!(!value.trim().is_empty(), "capture.{name} must be stated");
    }
    assert!(
        f.capture.reference_image.contains("sha256:"),
        "the reference must be pinned by digest, not by tag"
    );
    assert!(
        f.limits.contains_key("what_this_cannot_see"),
        "the fixture states its own blind spot"
    );
}

/// Which endpoint a probe was captured against. Every probe in this
/// fixture is a metrics-form query, which the assertion below makes a
/// failure rather than an assumption.
fn is_metrics(q: &str) -> bool {
    ["rate(", "_over_time(", "compare(", "topk(", "bottomk("]
        .iter()
        .any(|m| q.contains(m))
}

/// Replays one query against the pinned container on the route and window
/// the fixture records. Anything that is not a conclusive 2xx/400 fails
/// loudly as inconclusive — never silently counted as a rejection.
fn reference_side(base: &str, query: &str) -> Side {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let start = now.saturating_sub(3600).to_string();
    let end = now.to_string();
    let url = format!("{}/api/metrics/query_range", base.trim_end_matches('/'));
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "-w", "\n%{http_code}", "-G", "--max-time", "20"]);
    cmd.args(["--data-urlencode", &format!("q={query}")]);
    for (k, v) in [
        ("start", start.as_str()),
        ("end", end.as_str()),
        ("step", "60s"),
    ] {
        cmd.args(["--data-urlencode", &format!("{k}={v}")]);
    }
    cmd.arg(&url);
    let out = cmd.output().expect("curl must be on PATH");
    let text = String::from_utf8_lossy(&out.stdout);
    let (body, code) = text
        .rsplit_once('\n')
        .unwrap_or_else(|| panic!("unparseable curl output for {query:?}: {text:?}"));
    match code.trim().parse::<u32>().unwrap_or(0) {
        200..=299 => Side {
            verdict: "accept".to_string(),
            body: None,
        },
        400 => Side {
            verdict: "reject".to_string(),
            body: Some(body.trim().to_string()),
        },
        other => panic!(
            "inconclusive: the reference returned {other} for {query:?} \
             (only 2xx=accept / 400=reject are conclusive); body {body:?}"
        ),
    }
}

#[test]
fn every_committed_reference_verdict_still_holds_against_the_pinned_oracle() {
    // Fail-closed: in a live CI job with the env block dropped this
    // panics rather than skipping green (issue #320).
    pulsus_testkit::require_live_gate("PULSUSDB_TEMPO_DIFF_URL");
    let Ok(base) = std::env::var("PULSUSDB_TEMPO_DIFF_URL") else {
        eprintln!("PULSUSDB_TEMPO_DIFF_URL unset; skipping the metrics-filter oracle leg");
        return;
    };
    let f = fixture();
    let mut drifted = Vec::new();
    for probe in &f.probes {
        assert!(
            is_metrics(&probe.query),
            "{:?} is not a metrics-form query but the capture route is {:?}",
            probe.query,
            f.capture.route
        );
        let live = reference_side(&base, &probe.query);
        if live.verdict != probe.reference.verdict {
            drifted.push(format!(
                "{:?}: committed reference {:?}, live {:?}",
                probe.query, probe.reference.verdict, live.verdict
            ));
            continue;
        }
        // A rejecting body is pinned on both sides; an accepting one is
        // not (the 200 envelope carries job counts that are not a
        // property of the query — see `capture.note`).
        if live.verdict == "reject" && live.body != probe.reference.body {
            drifted.push(format!(
                "{:?}: committed reference body {:?}, live {:?}",
                probe.query, probe.reference.body, live.body
            ));
        }
    }
    assert!(
        drifted.is_empty(),
        "{} committed reference verdict(s) no longer hold against \
         {}:\n{}",
        drifted.len(),
        f.capture.reference_image,
        drifted.join("\n")
    );
}
