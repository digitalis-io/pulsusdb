//! Env-gated black-box differential leg (issue #191, M8-LQ0).
//!
//! Replays every registry construct's probe against an **unmodified**,
//! digest-pinned v3.7.3 LogQL reference container and observes only the HTTP
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
//! `/loki/api/v1/query_range` compat alias (docs/api.md §8.1).

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

/// GETs a query at the `/loki/api/v1/query_range` compat alias and maps the
/// HTTP status to a verdict. 2xx is Accept, exactly 400 is Reject; anything
/// else (0 = connection failure, 401/404/429/5xx, …) is inconclusive and
/// fails the test loudly.
fn oracle_verdict(base: &str, query: &str) -> Verdict {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let start = now.saturating_sub(3600).to_string();
    let end = now.to_string();
    let url = format!("{}/loki/api/v1/query_range", base.trim_end_matches('/'));
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
    cmd.args(["--data-urlencode", &format!("start={start}")]);
    cmd.args(["--data-urlencode", &format!("end={end}")]);
    cmd.args(["--data-urlencode", "step=60s"]);
    cmd.args(["--data-urlencode", "limit=1"]);
    cmd.arg(&url);
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
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset; skipping the LogQL differential leg");
        return;
    };

    let registry: Registry =
        serde_json::from_str(&read(conf_dir().join("registry-logql-v3.7.3.json"))).unwrap();
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
        let live = oracle_verdict(&base, &c.probe);
        if live != *want {
            mismatches.push(format!(
                "{}: recorded oracle={want:?} but live reference {live:?} for {:?}",
                c.id, c.probe
            ));
            continue;
        }
        match (*status, live) {
            ("supported", Verdict::Accept) => agreements += 1,
            ("supported", Verdict::Reject) => mismatches.push(format!(
                "{}: supported but the reference rejects — an unescalated divergence",
                c.id
            )),
            (_, Verdict::Reject) => agreements += 1, // interim ∧ both reject
            (_, Verdict::Accept) => tracked_interim += 1, // interim ∧ reference accepts
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
