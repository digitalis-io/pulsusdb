//! Env-gated black-box differential leg (issue #191, M8-LQ0).
//!
//! Replays every registry construct's probe against an **unmodified**,
//! digest-pinned v3.7.4 LogQL reference container and observes only the HTTP
//! status. No upstream source is read — this is pure runtime use of the
//! reference image as a language oracle.
//!
//! The gate is disposition-driven, not an ad-hoc allowlist: every construct
//! records the oracle's verdict in `dispositions.json` (`oracle`:
//! `accept`/`reject`), and this leg asserts the LIVE oracle still matches that
//! recorded verdict. Every construct is therefore exactly one of:
//!   * an **agreement** — `supported` ∧ the reference accepts (HTTP 2xx), or
//!     interim ∧ the reference rejects (both reject the probe, HTTP 400), or
//!   * a **tracked interim gap** — interim ∧ the reference accepts: a real
//!     compatibility gap, visible in the registry/dispositions with a
//!     public-doc citation and an owning issue.
//!
//! Contract:
//!   * A `supported` construct ⇒ the reference accepts (2xx). A rejection is
//!     an unescalated divergence ⇒ RED.
//!   * An interim construct ⇒ the reference returns exactly its recorded
//!     verdict.
//!   * Any other status (401/404/429/5xx/connection failure) ⇒ fail loudly as
//!     *inconclusive* — never silently counted as a rejection.
//!
//! Gate: skips cleanly unless `PULSUSDB_LOGQL_DIFF_URL` is set (e.g.
//! `http://localhost:13100`). The reference and PulsusDB both serve the
//! `/loki/api/v1/query_range` compat alias (docs/api.md §8.1). A construct
//! whose registry entry declares `endpoint: instant` probes the
//! `/loki/api/v1/query` alias instead (issue #221: `approx_topk` is
//! instant-only in the reference, so only the instant endpoint yields a
//! conclusive 2xx/400 syntax verdict; the oracle container config must
//! enable it — see `ci/logql/config.yaml`).

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Verdict {
    Accept,
    Reject,
}

/// The disposition-driven classification of a construct whose live oracle
/// verdict already matches its recorded `oracle` (the recorded-verdict guard
/// runs first, in the loop). Factored out so the RED arms are provable by a
/// hermetic unit test, not only by the live leg.
#[derive(Debug)]
enum Outcome {
    Agreement,
    TrackedInterim,
    Mismatch(String),
}

fn classify_verdict(id: &str, status: &str, live: Verdict) -> Outcome {
    match (status, live) {
        ("supported", Verdict::Accept) => Outcome::Agreement,
        ("supported", Verdict::Reject) => Outcome::Mismatch(format!(
            "{id}: supported but the reference rejects — an unescalated divergence"
        )),
        // Reject-parity (#203): we reject AND the reference must reject. A live
        // Accept is an unescalated divergence in the other direction (we
        // reject, the reference does not) — a loud mismatch, never silently
        // folded into the tracked-interim bucket by the wildcard below.
        ("reject-parity", Verdict::Reject) => Outcome::Agreement,
        ("reject-parity", Verdict::Accept) => Outcome::Mismatch(format!(
            "{id}: reject-parity but the reference now accepts — an unescalated divergence"
        )),
        (_, Verdict::Reject) => Outcome::Agreement, // interim ∧ both reject
        (_, Verdict::Accept) => Outcome::TrackedInterim, // interim ∧ reference accepts
    }
}

fn conf_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("conformance")
}

fn read(path: PathBuf) -> String {
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[derive(Deserialize)]
struct Registry {
    constructs: Vec<Construct>,
}
#[derive(Deserialize)]
struct Construct {
    id: String,
    probe: String,
    /// Optional probe endpoint override (issue #221). `"instant"` routes
    /// the probe to `/loki/api/v1/query`; absent = the default
    /// `query_range`. Needed for constructs that are INSTANT-ONLY in the
    /// reference: `approx_topk` returns 500 on `query_range` in every
    /// configuration (bare: `approx_topk is not enabled`; enabled:
    /// `count min sketches are only supported on instant queries` — both
    /// probed against the pinned digest), so a range probe can never be
    /// conclusive for it. This is registry METADATA driving the probe
    /// shape, not an id allowlist — the verdict contract is unchanged.
    #[serde(default)]
    endpoint: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum ProbeEndpoint {
    Range,
    Instant,
}

/// Resolves a construct's probe endpoint, failing LOUDLY on an unknown
/// value (a typo must never silently fall back to the range endpoint).
fn probe_endpoint(c: &Construct) -> ProbeEndpoint {
    match c.endpoint.as_deref() {
        None => ProbeEndpoint::Range,
        Some("instant") => ProbeEndpoint::Instant,
        Some(other) => panic!(
            "{}: unknown probe endpoint {other:?} (only \"instant\" is recognized)",
            c.id
        ),
    }
}
#[derive(Deserialize)]
struct Dispositions {
    entries: Vec<Disposition>,
}
#[derive(Deserialize)]
struct Disposition {
    construct: String,
    status: String,
    oracle: String,
}

/// GETs a query at the construct's compat-alias endpoint (`query_range` by
/// default; `query` for the instant-only constructs, issue #221) and maps
/// the HTTP status to a verdict. 2xx is Accept, exactly 400 is Reject;
/// anything else (0 = connection failure, 401/404/429/5xx, …) is
/// inconclusive and fails the test loudly.
fn oracle_verdict(base: &str, query: &str, endpoint: ProbeEndpoint) -> Verdict {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let mut cmd = Command::new("curl");
    cmd.args([
        "-s",
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        "-G",
        "--max-time",
        "20",
    ]);
    cmd.args(["--data-urlencode", &format!("query={query}")]);
    match endpoint {
        ProbeEndpoint::Range => {
            let start = now.saturating_sub(3600).to_string();
            let end = now.to_string();
            cmd.args(["--data-urlencode", &format!("start={start}")]);
            cmd.args(["--data-urlencode", &format!("end={end}")]);
            cmd.args(["--data-urlencode", "step=60s"]);
            cmd.args(["--data-urlencode", "limit=1"]);
            cmd.arg(format!(
                "{}/loki/api/v1/query_range",
                base.trim_end_matches('/')
            ));
        }
        ProbeEndpoint::Instant => {
            cmd.args(["--data-urlencode", &format!("time={now}000000000")]);
            cmd.args(["--data-urlencode", "limit=1"]);
            cmd.arg(format!("{}/loki/api/v1/query", base.trim_end_matches('/')));
        }
    }
    let out = cmd.output().expect("curl must be on PATH");
    let code: u32 = String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0);
    match code {
        200..=299 => Verdict::Accept,
        400 => Verdict::Reject,
        other => panic!(
            "inconclusive: reference returned {other} for {query:?} \
             (only 2xx=accept / 400=reject are conclusive)"
        ),
    }
}

#[test]
fn registry_probes_match_the_recorded_oracle_verdict() {
    let Some(base) = pulsus_testkit::live_endpoint("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset; skipping the LogQL differential leg");
        return;
    };

    let registry: Registry =
        serde_json::from_str(&read(conf_dir().join("registry-logql-v3.7.4.json"))).unwrap();
    let disp: Dispositions =
        serde_json::from_str(&read(conf_dir().join("dispositions.json"))).unwrap();

    let recorded: BTreeMap<&str, (&str, Verdict)> = disp
        .entries
        .iter()
        .map(|d| {
            let v = match d.oracle.as_str() {
                "accept" => Verdict::Accept,
                "reject" => Verdict::Reject,
                other => panic!("{}: bad recorded oracle {other:?}", d.construct),
            };
            (d.construct.as_str(), (d.status.as_str(), v))
        })
        .collect();

    let mut agreements = 0usize;
    let mut tracked_interim = 0usize;
    let mut mismatches: Vec<String> = Vec::new();

    for c in &registry.constructs {
        let (status, want) = recorded
            .get(c.id.as_str())
            .unwrap_or_else(|| panic!("{}: no disposition", c.id));
        let live = oracle_verdict(&base, &c.probe, probe_endpoint(c));
        if live != *want {
            mismatches.push(format!(
                "{}: recorded oracle={want:?} but live reference {live:?} for {:?}",
                c.id, c.probe
            ));
            continue;
        }
        match classify_verdict(c.id.as_str(), status, live) {
            Outcome::Agreement => agreements += 1,
            Outcome::TrackedInterim => tracked_interim += 1,
            Outcome::Mismatch(m) => mismatches.push(m),
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} construct(s) disagreed with the recorded oracle verdict — re-record the `oracle` \
         field (a construct that flips is a real oracle change, never an allowlist bypass):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
    eprintln!(
        "LogQL differential: {} constructs, {agreements} agreements, {tracked_interim} tracked \
         interim gaps (all visible in the registry with an owning issue)",
        registry.constructs.len()
    );
}

// Hermetic guards for the probe-endpoint metadata (issue #221): the leg's
// live run only exercises the values actually present in the registry, so
// these pin the resolution rules without a container.
#[test]
fn probe_endpoint_metadata_is_validated_and_instant_only_constructs_carry_it() {
    let registry: Registry =
        serde_json::from_str(&read(conf_dir().join("registry-logql-v3.7.4.json"))).unwrap();
    for c in &registry.constructs {
        // Loud on typos — the resolver panics on any unknown value.
        let endpoint = probe_endpoint(c);
        if c.id == "agg.approx_topk" {
            // approx_topk is instant-only in the reference (query_range is
            // a 500 in EVERY configuration), so its probe MUST route to
            // the instant endpoint or the leg is structurally
            // inconclusive.
            assert!(
                matches!(endpoint, ProbeEndpoint::Instant),
                "agg.approx_topk must declare `endpoint: instant`"
            );
        }
    }
}

#[test]
#[should_panic(expected = "unknown probe endpoint")]
fn an_unknown_probe_endpoint_fails_loudly() {
    probe_endpoint(&Construct {
        id: "x".to_string(),
        probe: "x".to_string(),
        endpoint: Some("websocket".to_string()),
    });
}

// Hermetic RED-path proof (#203 plan-review TEST-GAP): the pinned v3.7.4
// reference only exercises the reject-parity ∧ Reject agreement arm, so a
// reference flip to Accept is never covered live. This proves the
// `("reject-parity", Verdict::Accept)` arm records a loud mismatch rather than
// silently folding it into the tracked-interim bucket via the wildcard.
#[test]
fn reject_parity_reference_accept_is_a_mismatch() {
    match classify_verdict("stage.distinct", "reject-parity", Verdict::Accept) {
        Outcome::Mismatch(m) => assert!(
            m.contains("reject-parity") && m.contains("now accepts"),
            "unexpected mismatch message: {m:?}"
        ),
        other => panic!("expected a mismatch for a reject-parity oracle flip, got {other:?}"),
    }
    // The both-reject agreement stays an agreement.
    assert!(matches!(
        classify_verdict("stage.distinct", "reject-parity", Verdict::Reject),
        Outcome::Agreement
    ));
}
