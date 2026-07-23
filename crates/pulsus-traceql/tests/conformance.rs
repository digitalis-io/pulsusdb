//! The TraceQL conformance foundation (issue #180, M7-TQ1).
//!
//! A committed, clean-room enumeration of the entire *documented* TraceQL
//! surface for the pinned language target (TraceQL as shipped in **Tempo
//! v3.0.2**, grounded in the published grafana.com TraceQL documentation)
//! and a disposition manifest giving every construct exactly one
//! machine-checked home. No Tempo/Grafana/Loki source, grammar, enum, or
//! test corpus is copied, fetched, or vendored — see
//! `tests/conformance/PROVENANCE.md`.
//!
//! Guarantees (each a `#[test]`, all hermetic, riding the `ci` job):
//!   1. `registry_matches_its_integrity_manifest` — SHA-256 + per-category
//!      counts + non-empty public-doc citation per entry.
//!   2. `every_registry_construct_has_exactly_one_disposition` — bijection.
//!      An un-dispositioned construct is a RED test (surfacing coverage
//!      gaps as failures, never silent skips).
//!   3. `disposition_probes_match_their_status` — each probe's ACTUAL parse
//!      outcome must match its status; a named boundary must be
//!      `NotYetSupported` naming the construct, never a bare generic error.
//!   4. evidence pointers resolve against the corpus MANIFEST and reproduce
//!      the claimed class.
//!   5. `interim_disposition_count_is_pinned` — exact-equality pin.
//!   6. `divergence_count_is_zero_at_t1` — no divergence entries yet.
//!   7. `grafana_cases_are_ok_named_or_ledgered` — every observed-query
//!      case is `Ok`, a named boundary, or ledgered; every ledger entry
//!      must still reproduce its generic failure (monotone shrink).
//!
//! Pure check functions back the file tests so the RED paths are proven by
//! committed negative-fixture unit tests.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use pulsus_traceql::{TraceQlError, parse};

// #192 owns the 9 schema-blocked event/link/instrumentation-scope
// constructs (the residual interim set after the M7-TQ6 closeout, issue
// #185). Every interim disposition must name it; a construct owned by a
// now-closed sub-issue (179/181–184) is RED. The closeout gate
// (`interim_entries_are_allowlisted`) is the forcing function that drives
// the interim set to zero once #192 lands.
const VALID_ISSUES: [u64; 1] = [192];
// The committed open-issue allowlist the strict closeout gate enforces:
// every interim entry's owning_issue must be in it (auto-tightening to
// `interim_count == 0` when #192 flips its 9 to `supported`).
const CLOSEOUT_INTERIM_ALLOWLIST: &[u64] = &[192];
const DOCS_PREFIX: &str = "https://grafana.com/docs/tempo/";
const REPO_PREFIX: &str = "https://github.com/digitalis-io/pulsusdb/";

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Registry {
    #[allow(dead_code)]
    language: String,
    #[allow(dead_code)]
    target: String,
    constructs: Vec<Construct>,
}

#[derive(Debug, Deserialize)]
struct Construct {
    id: String,
    category: String,
    #[allow(dead_code)]
    syntax: String,
    doc: String,
    probe: String,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    sha256: String,
    construct_count: usize,
    category_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Deserialize)]
struct Dispositions {
    interim_count_pin: usize,
    entries: Vec<Disposition>,
}

#[derive(Debug, Clone, Deserialize)]
struct Disposition {
    construct: String,
    status: Status,
    // The oracle-recorded verdict of Tempo v3.0.2 for this construct's probe
    // (measured black-box, tests/conformance/PROVENANCE.md). It makes the
    // differential disposition-driven: an interim construct Tempo *accepts*
    // is a tracked compat gap; one Tempo *rejects* is a both-reject
    // agreement — no construct is silently exempted.
    tempo: Tempo,
    #[serde(default)]
    error_construct: Option<String>,
    #[serde(default)]
    evidence: Vec<String>,
    #[serde(default)]
    owning_issue: Option<u64>,
    // Divergence-only (all REQUIRED when status == Divergence): the
    // owner-escalated, oracle-cited record of an intentional deviation.
    #[serde(default)]
    justification: Option<String>,
    #[serde(default)]
    oracle_citation: Option<String>,
    #[serde(default)]
    owner_escalation: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Status {
    Supported,
    InterimNamed,
    InterimGeneric,
    /// We reject the construct AND the pinned reference rejects it — parity,
    /// not a compatibility gap (issue #185). Requires the probe to error
    /// (Named|Generic); the live differential separately confirms the
    /// reference also rejects. An oracle flip to Accept turns it into an
    /// unescalated divergence ⇒ RED.
    RejectParity,
    Divergence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Tempo {
    Accept,
    Reject,
}

#[derive(Debug, Deserialize)]
struct ReplayLedger {
    entries: Vec<LedgerEntry>,
}

#[derive(Debug, Deserialize)]
struct LedgerEntry {
    case: String,
    #[allow(dead_code)]
    first_blocking_construct: String,
    owning_issues: Vec<u64>,
}

/// A probe's actual parse outcome, reduced to the class the disposition
/// harness reasons about.
#[derive(Debug, PartialEq, Eq)]
enum ProbeClass {
    Parses,
    Named(String),
    Generic,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

fn conf_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("conformance")
}

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("corpus")
}

fn read(path: &PathBuf) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

fn registry_bytes() -> Vec<u8> {
    let path = conf_dir().join("registry-traceql-v3.0.2.json");
    fs::read(&path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

fn load_registry() -> Registry {
    serde_json::from_slice(&registry_bytes()).expect("registry JSON parses")
}

fn load_manifest() -> Manifest {
    serde_json::from_str(&read(&conf_dir().join("registry-manifest.json")))
        .expect("manifest parses")
}

fn load_dispositions() -> Dispositions {
    serde_json::from_str(&read(&conf_dir().join("dispositions.json"))).expect("dispositions parse")
}

fn load_ledger() -> ReplayLedger {
    serde_json::from_str(&read(&conf_dir().join("replay-ledger.json"))).expect("ledger parses")
}

/// Every `<class>/<stem>` declared by the byte-frozen corpus MANIFEST.
fn corpus_manifest() -> BTreeSet<String> {
    read(&corpus_dir().join("MANIFEST"))
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// The query text of a corpus case (single trailing newline stripped, as
/// the corpus file format mandates).
fn corpus_input(case: &str) -> String {
    let raw = read(&corpus_dir().join(format!("{case}.traceql")));
    raw.strip_suffix('\n').unwrap_or(&raw).to_string()
}

// ---------------------------------------------------------------------------
// Pure checks (shared by the file tests and the negative-fixture unit tests)
// ---------------------------------------------------------------------------

fn classify(probe: &str) -> ProbeClass {
    match parse(probe) {
        Ok(_) => ProbeClass::Parses,
        Err(TraceQlError::NotYetSupported { construct, .. }) => ProbeClass::Named(construct),
        Err(_) => ProbeClass::Generic,
    }
}

/// Registry ids and disposition constructs must be a bijection; an
/// un-dispositioned construct (or an orphan disposition) is an error.
fn check_bijection(construct_ids: &[String], disp_constructs: &[String]) -> Result<(), String> {
    let reg: BTreeSet<&str> = construct_ids.iter().map(String::as_str).collect();
    if reg.len() != construct_ids.len() {
        return Err("registry has duplicate construct ids".to_string());
    }
    let disp: BTreeSet<&str> = disp_constructs.iter().map(String::as_str).collect();
    if disp.len() != disp_constructs.len() {
        return Err("dispositions have duplicate constructs".to_string());
    }
    let undispositioned: Vec<_> = reg.difference(&disp).collect();
    let orphan: Vec<_> = disp.difference(&reg).collect();
    if !undispositioned.is_empty() || !orphan.is_empty() {
        return Err(format!(
            "disposition bijection broken: constructs with no disposition {undispositioned:?}; \
             dispositions with no construct {orphan:?}"
        ));
    }
    Ok(())
}

/// A disposition's status must match its probe's actual parse outcome (and
/// carry the status-specific required fields). `divergence` is metadata-only
/// (no probe constraint) and is validated by [`check_divergence`].
fn check_status(d: &Disposition, probe: &str) -> Result<(), String> {
    let class = classify(probe);
    match d.status {
        Status::Supported => match class {
            ProbeClass::Parses => Ok(()),
            other => Err(format!(
                "{}: status `supported` but probe {probe:?} gave {other:?}",
                d.construct
            )),
        },
        Status::InterimNamed => {
            let want = d.error_construct.as_deref().ok_or_else(|| {
                format!("{}: interim-named requires `error_construct`", d.construct)
            })?;
            check_interim_issue(d)?;
            match &class {
                ProbeClass::Named(got) if got == want => {
                    // The Display must name the construct — never a bare
                    // generic error for a real construct.
                    let err = parse(probe).expect_err("interim-named probe must error");
                    if err.to_string().contains(want) {
                        Ok(())
                    } else {
                        Err(format!(
                            "{}: NotYetSupported Display {:?} does not name {want:?}",
                            d.construct,
                            err.to_string()
                        ))
                    }
                }
                ProbeClass::Named(got) => Err(format!(
                    "{}: interim-named error_construct {want:?} but probe named {got:?}",
                    d.construct
                )),
                other => Err(format!(
                    "{}: interim-named but probe {probe:?} gave {other:?} (expected NotYetSupported)",
                    d.construct
                )),
            }
        }
        Status::InterimGeneric => {
            check_interim_issue(d)?;
            match class {
                ProbeClass::Generic => Ok(()),
                other => Err(format!(
                    "{}: interim-generic but probe {probe:?} gave {other:?} \
                     (a construct that now parses or names a boundary must have its \
                     disposition flipped)",
                    d.construct
                )),
            }
        }
        // Reject-parity: we reject the probe (a named boundary or a generic
        // error) AND the reference rejects it. It is NOT a compat gap, so it
        // needs no owning issue; the live differential enforces that the
        // reference still rejects.
        Status::RejectParity => match class {
            ProbeClass::Named(_) | ProbeClass::Generic => Ok(()),
            ProbeClass::Parses => Err(format!(
                "{}: reject-parity but probe {probe:?} now parses (we no longer reject it)",
                d.construct
            )),
        },
        Status::Divergence => check_divergence(d),
    }
}

fn check_interim_issue(d: &Disposition) -> Result<(), String> {
    match d.owning_issue {
        Some(i) if VALID_ISSUES.contains(&i) => Ok(()),
        Some(i) => Err(format!(
            "{}: owning_issue {i} not in {VALID_ISSUES:?}",
            d.construct
        )),
        None => Err(format!(
            "{}: interim-* requires an owning_issue",
            d.construct
        )),
    }
}

/// A `divergence` entry must carry a non-empty justification plus a
/// published-doc oracle citation and an owner-escalation URL of the right
/// shapes, plus an owning issue (Plan v2 Δ1 / AC 6).
fn check_divergence(d: &Disposition) -> Result<(), String> {
    let justification = d.justification.as_deref().unwrap_or("");
    if justification.trim().is_empty() {
        return Err(format!(
            "{}: divergence requires a non-empty justification",
            d.construct
        ));
    }
    match d.oracle_citation.as_deref() {
        Some(u) if u.starts_with(DOCS_PREFIX) => {}
        _ => {
            return Err(format!(
                "{}: divergence requires an oracle_citation starting {DOCS_PREFIX}",
                d.construct
            ));
        }
    }
    match d.owner_escalation.as_deref() {
        Some(u) if u.starts_with(REPO_PREFIX) => {}
        _ => {
            return Err(format!(
                "{}: divergence requires an owner_escalation starting {REPO_PREFIX}",
                d.construct
            ));
        }
    }
    if d.owning_issue.is_none() {
        return Err(format!(
            "{}: divergence requires an owning_issue",
            d.construct
        ));
    }
    Ok(())
}

/// Number of interim (named + generic) dispositions — the pinned quantity
/// that every T2–T5 PR lowers and #185 drives to 0.
fn interim_count(entries: &[Disposition]) -> usize {
    entries
        .iter()
        .filter(|d| matches!(d.status, Status::InterimNamed | Status::InterimGeneric))
        .count()
}

// ---------------------------------------------------------------------------
// File tests
// ---------------------------------------------------------------------------

#[test]
fn registry_matches_its_integrity_manifest() {
    let registry = load_registry();
    let manifest = load_manifest();

    let sha: String = Sha256::digest(registry_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        sha, manifest.sha256,
        "registry SHA-256 drift — an edit to registry-traceql-v3.0.2.json must be deliberate \
         and re-pin registry-manifest.json"
    );

    assert_eq!(
        registry.constructs.len(),
        manifest.construct_count,
        "construct_count pin mismatch"
    );

    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for c in &registry.constructs {
        *counts.entry(c.category.clone()).or_default() += 1;
    }
    assert_eq!(
        counts, manifest.category_counts,
        "per-category count pin mismatch"
    );

    // AC 3: every entry carries a non-empty public-doc citation.
    let ids: BTreeSet<&str> = registry.constructs.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(
        ids.len(),
        registry.constructs.len(),
        "duplicate construct ids in the registry"
    );
    for c in &registry.constructs {
        assert!(
            c.doc.starts_with(DOCS_PREFIX),
            "{}: doc citation {:?} must begin {DOCS_PREFIX}",
            c.id,
            c.doc
        );
        assert!(!c.probe.trim().is_empty(), "{}: empty probe", c.id);
    }
}

#[test]
fn every_registry_construct_has_exactly_one_disposition() {
    let registry = load_registry();
    let disp = load_dispositions();
    let ids: Vec<String> = registry.constructs.iter().map(|c| c.id.clone()).collect();
    let constructs: Vec<String> = disp.entries.iter().map(|d| d.construct.clone()).collect();
    check_bijection(&ids, &constructs).expect("registry <-> disposition bijection");
}

#[test]
fn disposition_probes_match_their_status() {
    let registry = load_registry();
    let disp = load_dispositions();
    let probes: BTreeMap<&str, &str> = registry
        .constructs
        .iter()
        .map(|c| (c.id.as_str(), c.probe.as_str()))
        .collect();
    for d in &disp.entries {
        let probe = probes
            .get(d.construct.as_str())
            .unwrap_or_else(|| panic!("{}: no registry probe", d.construct));
        check_status(d, probe).expect("disposition status matches its probe");
    }
}

#[test]
fn evidence_pointers_resolve_and_reproduce_their_class() {
    let manifest = corpus_manifest();
    let disp = load_dispositions();
    for d in &disp.entries {
        for ev in &d.evidence {
            assert!(
                manifest.contains(ev),
                "{}: evidence {ev:?} is not a corpus MANIFEST case",
                d.construct
            );
            let outcome = parse(&corpus_input(ev));
            match d.status {
                Status::Supported => assert!(
                    ev.starts_with("accept/") && outcome.is_ok(),
                    "{}: supported evidence {ev:?} must be an accept/ case that parses, got {outcome:?}",
                    d.construct
                ),
                Status::InterimNamed => assert!(
                    ev.starts_with("unsupported/")
                        && matches!(outcome, Err(TraceQlError::NotYetSupported { .. })),
                    "{}: interim-named evidence {ev:?} must be an unsupported/ NotYetSupported case, got {outcome:?}",
                    d.construct
                ),
                Status::InterimGeneric | Status::RejectParity | Status::Divergence => {}
            }
        }
    }
}

#[test]
fn interim_disposition_count_is_pinned() {
    let disp = load_dispositions();
    assert_eq!(
        interim_count(&disp.entries),
        disp.interim_count_pin,
        "interim_count_pin drift — a status flip must re-pin it (#185 lowers it to 9; #192 -> 0)"
    );
}

/// The strict closeout gate (issue #185): every interim entry must be owned
/// by an OPEN issue in the committed allowlist. An interim owned by a
/// now-closed sub-issue (179/181–184) is RED — the forcing function that
/// made #185 finish the 19 language-complete constructs. When #192 flips
/// its 9 to `supported`, the interim set empties and this passes at
/// `interim_count == 0` with no #185-side edit.
#[test]
fn interim_entries_are_allowlisted() {
    let disp = load_dispositions();
    let stray: Vec<String> = disp
        .entries
        .iter()
        .filter(|d| matches!(d.status, Status::InterimNamed | Status::InterimGeneric))
        .filter(|d| {
            d.owning_issue
                .map(|i| !CLOSEOUT_INTERIM_ALLOWLIST.contains(&i))
                .unwrap_or(true)
        })
        .map(|d| format!("{} (owning_issue {:?})", d.construct, d.owning_issue))
        .collect();
    assert!(
        stray.is_empty(),
        "interim dispositions not owned by an allowlisted open issue \
         {CLOSEOUT_INTERIM_ALLOWLIST:?}: {stray:?}"
    );
}

#[test]
fn differential_categories_are_pinned() {
    let disp = load_dispositions();
    let mut supported = 0usize;
    let mut tracked_interim = 0usize; // interim ∧ Tempo accepts (a real gap)
    let mut both_reject = 0usize; // interim ∧ Tempo rejects (agreement)
    let mut reject_parity = 0usize; // reject-parity ∧ Tempo rejects (agreement)
    let mut unescalated_divergence = Vec::new(); // supported ∧ Tempo rejects
    let mut oracle_flipped_reject_parity = Vec::new(); // reject-parity ∧ Tempo accepts
    for d in &disp.entries {
        match (d.status, d.tempo) {
            (Status::Supported, Tempo::Accept) => supported += 1,
            (Status::Supported, Tempo::Reject) => unescalated_divergence.push(&d.construct),
            (Status::RejectParity, Tempo::Reject) => reject_parity += 1,
            (Status::RejectParity, Tempo::Accept) => {
                oracle_flipped_reject_parity.push(&d.construct)
            }
            (Status::Divergence, _) => {}
            (_, Tempo::Accept) => tracked_interim += 1,
            (_, Tempo::Reject) => both_reject += 1,
        }
    }
    // A `supported` construct Tempo rejects would be an unrecorded
    // divergence (we are more permissive than the oracle) — never allowed
    // to slip in silently.
    assert!(
        unescalated_divergence.is_empty(),
        "supported constructs Tempo rejects (unescalated divergences): {unescalated_divergence:?}"
    );
    // A reject-parity construct the oracle now ACCEPTS is an unescalated
    // divergence in the other direction (we reject, the reference does not).
    assert!(
        oracle_flipped_reject_parity.is_empty(),
        "reject-parity constructs the reference now accepts (unescalated divergences): \
         {oracle_flipped_reject_parity:?}"
    );
    // Exact pins — the M7-TQ6 closeout (issue #185) flips 15 tracked gaps
    // to `supported` (88 → 103), moves the 4 both-reject constructs into
    // the new reject-parity bucket (both_reject 4 → 0, reject_parity = 4),
    // and leaves the 9 schema-blocked Cat-C constructs as the residual
    // tracked interim (24 → 9).
    assert_eq!(
        supported, 103,
        "supported (both-accept agreement) count pin"
    );
    assert_eq!(
        tracked_interim, 9,
        "tracked interim gap count pin (interim ∧ Tempo accepts, each with an owning issue)"
    );
    assert_eq!(
        both_reject, 0,
        "both-reject agreement count pin (interim ∧ Tempo rejects)"
    );
    assert_eq!(
        reject_parity, 4,
        "reject-parity count pin (reject-parity ∧ Tempo rejects)"
    );

    // Every tracked interim gap must name an owning sub-issue and cite a
    // public doc (the registry `doc`, checked in the integrity test).
    for d in &disp.entries {
        if d.status != Status::Supported && d.tempo == Tempo::Accept {
            assert!(
                d.owning_issue.is_some(),
                "{}: a tracked interim gap must name an owning issue",
                d.construct
            );
        }
    }
}

#[test]
fn divergence_count_is_zero_at_t1() {
    let disp = load_dispositions();
    let n = disp
        .entries
        .iter()
        .filter(|d| d.status == Status::Divergence)
        .count();
    assert_eq!(n, 0, "AC 6: zero divergence entries at T1");
    // The schema still carries + validates the divergence fields (see the
    // negative-fixture unit tests below) so later escalations are enforced.
}

#[test]
fn grafana_cases_are_ok_named_or_ledgered() {
    let manifest = corpus_manifest();
    let ledger = load_ledger();
    let ledgered: BTreeSet<&str> = ledger.entries.iter().map(|e| e.case.as_str()).collect();

    let grafana: Vec<&String> = manifest
        .iter()
        .filter(|c| c.starts_with("grafana/"))
        .collect();
    assert!(
        !grafana.is_empty(),
        "the grafana/ seed corpus must not be empty"
    );

    for case in &grafana {
        let outcome = parse(&corpus_input(case));
        let ok_or_named =
            outcome.is_ok() || matches!(&outcome, Err(TraceQlError::NotYetSupported { .. }));
        assert!(
            ok_or_named || ledgered.contains(case.as_str()),
            "{case}: a generic failure must be recorded in replay-ledger.json, got {outcome:?}"
        );
    }

    // Monotone shrink: a ledger entry that no longer reproduces its generic
    // failure (now parses `Ok` or reaches a named boundary) is stale.
    for entry in &ledger.entries {
        assert!(
            manifest.contains(&entry.case),
            "ledger case {:?} is not a corpus MANIFEST case",
            entry.case
        );
        let class = classify(&corpus_input(&entry.case));
        assert_eq!(
            class,
            ProbeClass::Generic,
            "stale ledger entry {:?}: it now resolves to {class:?}; drop it (the ledger only shrinks)",
            entry.case
        );
        for i in &entry.owning_issues {
            assert!(
                VALID_ISSUES.contains(i),
                "ledger case {:?}: owning issue {i} not in {VALID_ISSUES:?}",
                entry.case
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Negative-fixture unit tests — prove every RED path
// ---------------------------------------------------------------------------

fn divergence_fixture() -> Disposition {
    Disposition {
        construct: "fixture.divergence".to_string(),
        status: Status::Divergence,
        tempo: Tempo::Reject,
        error_construct: None,
        evidence: vec![],
        owning_issue: Some(183),
        justification: Some("parity genuinely infeasible for reason X".to_string()),
        oracle_citation: Some(format!("{DOCS_PREFIX}latest/traceql/#x")),
        owner_escalation: Some(format!("{REPO_PREFIX}issues/180#issuecomment-1")),
    }
}

#[test]
fn undispositioned_construct_is_red() {
    let ids = vec!["a".to_string(), "b".to_string()];
    let disp = vec!["a".to_string()]; // b has no disposition
    assert!(check_bijection(&ids, &disp).is_err());
}

#[test]
fn orphan_disposition_is_red() {
    let ids = vec!["a".to_string()];
    let disp = vec!["a".to_string(), "ghost".to_string()];
    assert!(check_bijection(&ids, &disp).is_err());
}

#[test]
fn a_well_formed_divergence_fixture_validates() {
    check_divergence(&divergence_fixture()).expect("the reference fixture is valid");
}

#[test]
fn divergence_without_owner_escalation_is_red() {
    let mut d = divergence_fixture();
    d.owner_escalation = None;
    assert!(check_divergence(&d).is_err());
}

#[test]
fn divergence_with_non_repo_owner_escalation_is_red() {
    let mut d = divergence_fixture();
    d.owner_escalation = Some("https://example.com/not-a-ruling".to_string());
    assert!(check_divergence(&d).is_err());
}

#[test]
fn divergence_without_oracle_citation_is_red() {
    let mut d = divergence_fixture();
    d.oracle_citation = None;
    assert!(check_divergence(&d).is_err());
}

#[test]
fn divergence_with_non_docs_oracle_citation_is_red() {
    let mut d = divergence_fixture();
    d.oracle_citation = Some("https://example.com/spec".to_string());
    assert!(check_divergence(&d).is_err());
}

#[test]
fn divergence_with_empty_justification_is_red() {
    let mut d = divergence_fixture();
    d.justification = Some("   ".to_string());
    assert!(check_divergence(&d).is_err());
}

#[test]
fn interim_generic_without_owning_issue_is_red() {
    let d = Disposition {
        construct: "fixture.generic".to_string(),
        status: Status::InterimGeneric,
        tempo: Tempo::Accept,
        error_construct: None,
        evidence: vec![],
        owning_issue: None,
        justification: None,
        oracle_citation: None,
        owner_escalation: None,
    };
    // A probe that genuinely produces a generic error, so only the missing
    // owning_issue can fail it. (`{ nestedSetParent < 0 }` now parses since
    // issue #181, so it is no longer a generic-error probe — use a bare
    // unknown word, which is a plain positioned syntax error.)
    assert!(check_status(&d, "{ zzz = 1 }").is_err());
}

#[test]
fn interim_named_mislabelled_as_generic_is_red() {
    // A probe that names a boundary cannot be dispositioned interim-generic.
    // `{ .foo }` now parses (existence, issue #185), so the fixture probes a
    // still-named boundary (`parent scope`) and is owned by the allowlisted
    // #192.
    let d = Disposition {
        construct: "fixture.named-as-generic".to_string(),
        status: Status::InterimGeneric,
        tempo: Tempo::Accept,
        error_construct: None,
        evidence: vec![],
        owning_issue: Some(192),
        justification: None,
        oracle_citation: None,
        owner_escalation: None,
    };
    assert!(check_status(&d, r#"{ parent.foo = "x" }"#).is_err());
}

#[test]
fn stale_ledger_entry_that_now_parses_is_red() {
    // The monotone-shrink guard: a case that resolves to `Ok`/`Named` is no
    // longer a generic failure and must be dropped from the ledger.
    assert_eq!(classify("{ .a = 1 }"), ProbeClass::Parses);
    assert_ne!(classify("{ .a = 1 }"), ProbeClass::Generic);
}
