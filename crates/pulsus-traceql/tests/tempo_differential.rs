//! Env-gated black-box differential leg (issue #180, T1 firm deliverable).
//!
//! Replays every registry construct's probe against an **unmodified**
//! `grafana/tempo:v3.0.2` container and observes only the HTTP status. No
//! Tempo source is read — this is pure runtime use of the upstream image as
//! a language oracle.
//!
//! The gate is disposition-driven, not an ad-hoc allowlist: every construct
//! records the oracle's verdict in `dispositions.json` (`tempo`:
//! `accept`/`reject`), and this leg asserts the LIVE oracle still matches
//! that recorded verdict. Every disagreement is therefore either
//!   * an **agreement** — `supported` ∧ Tempo accepts, or interim ∧ Tempo
//!     rejects (both reject the probe), or
//!   * a **tracked interim gap** — interim ∧ Tempo accepts: a real
//!     compatibility gap, visible in the registry/dispositions with a
//!     public-doc citation and an owning sub-issue (#181–#184).
//!
//! There is no unauthorized exemption path.
//!
//! Contract (Plan v3 harness test 7, tightened by the round-2 review):
//!   * A `supported` construct ⇒ Tempo accepts (HTTP 2xx).
//!   * An interim construct ⇒ Tempo returns exactly its recorded verdict
//!     (2xx for a tracked gap, HTTP **400** for a both-reject agreement).
//!   * Any other status (401/404/429/5xx/connection failure) ⇒ fail loudly
//!     as *inconclusive* — never silently counted as a rejection.
//!
//! Endpoint routing: metrics-form queries (a `rate()`/`*_over_time()`/
//! `compare()`/`topk()`/`bottomk()` stage) go to `/api/metrics/query_range`,
//! everything else to `/api/search`.
//!
//! Gate: skips cleanly unless `PULSUSDB_TEMPO_DIFF_URL` is set (e.g.
//! `http://localhost:13200`).

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
    tempo: String,
}

fn is_metrics(q: &str) -> bool {
    ["rate(", "_over_time(", "compare(", "topk(", "bottomk("]
        .iter()
        .any(|m| q.contains(m))
}

/// GETs a query and maps the HTTP status to a verdict. A 2xx is Accept, a
/// 400 is Reject; anything else (0 = connection failure, 401/404/429/5xx,
/// …) is inconclusive and fails the test loudly.
fn tempo_verdict(base: &str, query: &str) -> Verdict {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let start = now.saturating_sub(3600).to_string();
    let end = now.to_string();
    let (path, extra): (&str, Vec<(&str, &str)>) = if is_metrics(query) {
        (
            "/api/metrics/query_range",
            vec![("start", &start), ("end", &end), ("step", "60s")],
        )
    } else {
        (
            "/api/search",
            vec![("start", &start), ("end", &end), ("limit", "1")],
        )
    };
    let url = format!("{}{}", base.trim_end_matches('/'), path);
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
    cmd.args(["--data-urlencode", &format!("q={query}")]);
    for (k, v) in extra {
        cmd.args(["--data-urlencode", &format!("{k}={v}")]);
    }
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
            "inconclusive: Tempo returned {other} for {query:?} \
             (only 2xx=accept / 400=reject are conclusive)"
        ),
    }
}

#[test]
fn registry_probes_match_the_recorded_tempo_verdict() {
    let Ok(base) = std::env::var("PULSUSDB_TEMPO_DIFF_URL") else {
        eprintln!("PULSUSDB_TEMPO_DIFF_URL unset; skipping the Tempo differential leg");
        return;
    };

    let registry: Registry =
        serde_json::from_str(&read(conf_dir().join("registry-traceql-v3.0.2.json"))).unwrap();
    let disp: Dispositions =
        serde_json::from_str(&read(conf_dir().join("dispositions.json"))).unwrap();

    use std::collections::BTreeMap;
    let recorded: BTreeMap<&str, (&str, Verdict)> = disp
        .entries
        .iter()
        .map(|d| {
            let v = match d.tempo.as_str() {
                "accept" => Verdict::Accept,
                "reject" => Verdict::Reject,
                other => panic!("{}: bad recorded tempo {other:?}", d.construct),
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
        let live = tempo_verdict(&base, &c.probe);
        if live != *want {
            mismatches.push(format!(
                "{}: recorded tempo={want:?} but live Tempo {live:?} for {:?}",
                c.id, c.probe
            ));
            continue;
        }
        match (*status, live) {
            ("supported", Verdict::Accept) => agreements += 1,
            ("supported", Verdict::Reject) => mismatches.push(format!(
                "{}: supported but Tempo rejects — an unescalated divergence",
                c.id
            )),
            (_, Verdict::Reject) => agreements += 1, // interim ∧ both reject
            (_, Verdict::Accept) => tracked_interim += 1, // interim ∧ Tempo accepts
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} construct(s) disagreed with the recorded Tempo verdict — re-record the `tempo` field \
         (a construct that flips is a real oracle change, never an allowlist bypass):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
    eprintln!(
        "Tempo differential: {} constructs, {agreements} agreements, {tracked_interim} tracked \
         interim gaps (all visible in the registry with an owning issue)",
        registry.constructs.len()
    );
}
