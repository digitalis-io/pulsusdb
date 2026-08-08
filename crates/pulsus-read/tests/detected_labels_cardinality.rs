//! Issue #261. The `/detected_labels` half of the registered cardinality
//! divergence: PulsusDB reports the EXACT distinct-value count where the
//! reference reports a p14 HyperLogLog estimate.
//!
//! Nothing here changes behaviour, and nothing here proves the endpoint
//! works — the live gates in `crates/pulsus-server/tests/logs_detected_live.rs`
//! and `crates/pulsus-read/tests/query_log_gates.rs` do that. Neither of
//! those discriminates this change: #261 edited no `src/`, so the code
//! they exercise IS the pre-#261 code and they pass against it by
//! construction. What this file gates is the pair of DOCUMENTED claims
//! #261 corrected:
//!
//! 1. the retracted universal agreement threshold ("the estimate equals
//!    the exact count up to 5327") must not come back, in the ledger or
//!    on the public API surface; and
//! 2. the capture that replaced it must keep discriminating — every row
//!    of the artifact disagrees with us, and the artifact contains the
//!    two facts that make the universal reading impossible (a family
//!    that diverges BELOW 5328, and one `N` at which three families give
//!    three different reference answers).
//!
//! Hermetic: reads committed bytes and renders production SQL. No
//! container, no network.

use std::collections::{HashMap, HashSet};

use pulsus_read::logql::sql;

/// The `/detected_labels` capture. Deliberately a NEW artifact rather
/// than a column added to `golden/detected_cardinality/reference_divergence.tsv`,
/// whose 5-column shape is frozen by `detected_fields_witness.rs`'s AC 19.
const TSV: &str = include_str!("golden/detected_labels_cardinality/reference_divergence.tsv");

fn repo_file(rel: &str) -> String {
    let path = format!("{}/../../{rel}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn crate_file(rel: &str) -> String {
    let path = format!("{}/{rel}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

/// One row of the capture.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    family: String,
    n_distinct: u64,
    reference_value: u64,
    pulsus_exact: u64,
    observed_by: String,
    ledger_id: String,
    note: String,
}

/// The `#`-comment header, verbatim, minus the leading `# `.
fn header(tsv: &str) -> String {
    tsv.lines()
        .take_while(|l| l.starts_with('#'))
        .map(|l| l.trim_start_matches('#').trim_start())
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse_rows(tsv: &str) -> Vec<Row> {
    let mut rows = Vec::new();
    for line in tsv.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        assert_eq!(cols.len(), 7, "row must have 7 columns: {line:?}");
        for (i, c) in cols.iter().enumerate() {
            assert!(!c.trim().is_empty(), "column {i} is empty in {line:?}");
        }
        rows.push(Row {
            family: cols[0].to_string(),
            n_distinct: cols[1].parse().expect("n_distinct"),
            reference_value: cols[2].parse().expect("reference_value"),
            pulsus_exact: cols[3].parse().expect("pulsus_exact"),
            observed_by: cols[4].to_string(),
            ledger_id: cols[5].to_string(),
            note: cols[6].to_string(),
        });
    }
    rows
}

/// Every column of every row carries a rule — including `note` and
/// `observed_by`, which look like free text and are the two places a
/// silently-unprovenanced row would hide:
///
/// (a) our side is EXACT (`pulsus_exact == n_distinct`);
/// (b) the row DISAGREES (`reference_value != pulsus_exact`) — an
///     agreeing point discriminates nothing and belongs in the header as
///     a last-agreeing witness, not in the body;
/// (c) `observed_by` is one of the two accepted instruments;
/// (d) `ledger_id` occurs verbatim in the ledger, which names #261;
/// (e) `note` is present and `(family, n_distinct)` is unique;
/// (f) the two mandatory CONTAINER rows are present with their measured
///     numbers;
/// (g) a mandatory row sits BELOW 5328 — the standing guard against the
///     retracted universal threshold being re-derived from this file;
/// (h) at least two families share one `n_distinct` and answer
///     DIFFERENTLY — the artifact's own proof that the threshold is not
///     a function of `N`;
/// (i) the header pins the capture conditions (image tag + reference
///     SHA) and records the last-agreeing witnesses, which must NOT also
///     appear as rows.
#[test]
fn detected_labels_divergence_rows_hold_and_the_ledger_names_them() {
    let rows = parse_rows(TSV);
    let ledger = repo_file("docs/benchmarks/logs-differential-ledger.md");
    assert!(ledger.contains("#261"), "the ledger must name issue #261");

    let mut seen: HashSet<(String, u64)> = HashSet::new();
    let mut by_n: HashMap<u64, HashSet<u64>> = HashMap::new();
    for r in &rows {
        assert_eq!(r.pulsus_exact, r.n_distinct, "(a) we are exact: {r:?}");
        assert_ne!(
            r.reference_value, r.pulsus_exact,
            "(b) a kept row must DISAGREE: {r:?}"
        );
        assert!(
            r.observed_by == "container" || r.observed_by == "library",
            "(c) observed_by must be `container` or `library`: {r:?}"
        );
        assert!(
            ledger.contains(&r.ledger_id),
            "(d) the ledger is missing {:?}",
            r.ledger_id
        );
        assert!(!r.note.trim().is_empty(), "(e) every row carries a note");
        assert!(
            seen.insert((r.family.clone(), r.n_distinct)),
            "(e) duplicate (family, n_distinct): {r:?}"
        );
        by_n.entry(r.n_distinct)
            .or_default()
            .insert(r.reference_value);
    }

    // (f) + (g): the two container-observed points, one of them below
    // the retracted 5328.
    for (family, n, reference) in [("svc-", 4533u64, 4532u64), ("pod-", 7708, 7640)] {
        let row = rows
            .iter()
            .find(|r| r.family == family && r.n_distinct == n)
            .unwrap_or_else(|| panic!("(f) the mandatory {family}{n} row is missing"));
        assert_eq!(row.reference_value, reference, "(f) {family}{n}");
        assert_eq!(
            row.observed_by, "container",
            "(f) {family}{n} must be container-observed, not derived"
        );
    }
    assert!(
        rows.iter()
            .any(|r| r.observed_by == "container" && r.n_distinct < 5328),
        "(g) a container-observed divergence BELOW 5328 must stay in the artifact — \
         it is the counterexample to the retracted universal threshold"
    );

    // (h) one N, more than one reference answer.
    assert!(
        by_n.values().any(|answers| answers.len() > 1),
        "(h) the artifact must keep at least one `n_distinct` at which two \
         families give DIFFERENT reference answers; without it the file can be \
         re-read as a table of thresholds indexed by N"
    );

    // (i) capture conditions and the agreeing witnesses.
    let head = header(TSV);
    for needle in [
        "grafana/loki:3.7.4",
        "b318f2829f0ae2094ab3a1e90780450e9e4b03be",
        "hyperloglog v0.2.6",
    ] {
        assert!(head.contains(needle), "(i) the header must pin {needle:?}");
    }
    for (family, n) in [("pod-", 7707u64), ("svc-", 4532)] {
        assert!(
            head.contains(&format!("n = {n} -> {n}")),
            "(i) the header must record the last-agreeing witness {family}{n}"
        );
        assert!(
            !rows.iter().any(|r| r.family == family && r.n_distinct == n),
            "(i) an agreeing witness must not also be a row: {family}{n}"
        );
    }
}

/// Normalizes prose for phrase matching: lowercase, markdown emphasis
/// and code ticks removed, `≤`/dashes folded, whitespace (including line
/// wraps) collapsed. Without the wrap-collapsing, a banned sentence
/// re-enters simply by falling across two lines.
fn normalize(text: &str) -> String {
    let lowered = text
        .to_lowercase()
        .replace('≤', "<=")
        .replace(['—', '–'], "-");
    let mut out = String::with_capacity(lowered.len());
    let mut last_space = false;
    for ch in lowered.chars() {
        match ch {
            '`' | '*' | '_' | '"' => {}
            c if c.is_whitespace() => {
                if !last_space {
                    out.push(' ');
                    last_space = true;
                }
            }
            c => {
                out.push(c);
                last_space = false;
            }
        }
    }
    out
}

/// The claim that #261 exists to retract must not return to the ledger
/// or to the public API surface, and the statement that replaced it must
/// still be there.
///
/// **What this gate reaches, and where it stops.** It sweeps EVERY
/// git-TRACKED file in the repository — not a hand-listed set, because a
/// hand-listed set cannot see the fifth copy nobody remembered; the
/// claim lived in four separate files before #261 and each was found by
/// grep, not by recollection. Line wraps are collapsed first, so the
/// retracted sentence cannot come back by being re-flowed or
/// re-emphasised. What it CANNOT do is recognise a NEW sentence
/// asserting some other number as a universal threshold; no textual
/// rule can. That case is covered instead by the positive half below:
/// the ledger has to keep saying what the threshold actually depends
/// on, and docs/api.md has to keep saying that no such distinct-value
/// count exists, so a rewrite that quietly reintroduces a bound has to
/// delete a sentence this test requires. That is where the instrument
/// stops.
///
/// `crates/pulsus-read/tests/golden/detected_cardinality/reference_divergence.tsv`
/// is deliberately NOT in the file list. Its header carries the same
/// wording, scoped by the sentence immediately before it (`"v0"`…
/// `"v{n-1}"`), and for that family it is true — re-verified 2026-08-08
/// by scanning n = 1…5400 with a fresh sketch per n against the vendored
/// v0.2.6 library, whose first `Estimate() != n` is n = 5328. It is also
/// a frozen #244 artifact. Excluding it is a decision with a reason, not
/// an oversight.
#[test]
fn the_universal_agreement_threshold_claim_stays_retracted() {
    // The `v{i}`-scoped #244 capture, excluded with its reason in this
    // test's doc comment.
    const FROZEN_244_ARTIFACT: &str = "golden/detected_cardinality/reference_divergence.tsv";
    // Every one of these is a form that WAS committed somewhere in the
    // tree before #261, or the phrase that presupposed it. Each is
    // assembled from halves so the literal never appears in any file —
    // which lets the sweep below include its OWN definition site rather
    // than carve out an exemption for it. The one exemption in this test
    // is the frozen #244 artifact, and it has a reason.
    let banned: Vec<String> = [
        ["for every ", "distinct-value count"],
        ["for every ", "n <= 5327"],
        ["first divergence ", "at n = 5328"],
        ["first divergence ", "is n = 5328"],
        ["far inside ", "the agreeing range"],
    ]
    .iter()
    .map(|halves| halves.concat())
    .collect();
    // Every TRACKED file, from git rather than from a directory walk: a
    // walk would also read a developer's untracked scratch files, and a
    // gate that fails on those is a gate people learn to ignore. A
    // failure to list is a hard failure, never a quiet empty sweep.
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let listing = std::process::Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args(["ls-files", "-z"])
        .output()
        .expect("git ls-files must run: this gate's domain is the tracked tree");
    assert!(
        listing.status.success(),
        "git ls-files failed: {}",
        String::from_utf8_lossy(&listing.stderr)
    );
    // Cheap pre-filter so the sweep does not normalize every megabyte of
    // committed benchmark JSON. Each token is a single unsplittable word
    // or number, so no line wrap can hide it — and the loop below ASSERTS
    // that every banned phrase contains at least one of them, which is
    // what makes skipping a file that contains none provably safe rather
    // than probably safe.
    const PREFILTER: &[&str] = &["5327", "5328", "distinct-value", "agreeing"];
    for phrase in &banned {
        assert!(
            PREFILTER.iter().any(|t| phrase.contains(t)),
            "banned phrase {phrase:?} has no PREFILTER token, so the sweep would \
             skip files containing it"
        );
    }
    let mut scanned = 0usize;
    let mut inspected = 0usize;
    for rel in String::from_utf8_lossy(&listing.stdout).split('\0') {
        if rel.is_empty() || rel.ends_with(FROZEN_244_ARTIFACT) {
            continue; // the one deliberate exclusion, reasoned above
        }
        let path = repo.join(rel);
        let Ok(bytes) = std::fs::read(&path) else {
            continue; // a listed-but-absent path (submodule, sparse checkout)
        };
        scanned += 1;
        let raw = String::from_utf8_lossy(&bytes).to_lowercase();
        if !PREFILTER.iter().any(|t| raw.contains(t)) {
            continue;
        }
        let norm = normalize(&raw);
        inspected += 1;
        for banned in &banned {
            assert!(
                !norm.contains(banned),
                "{rel} states the retracted universal agreement threshold: {banned:?}. \
                 The threshold is a property of the VALUE STRINGS; say which family \
                 a number was measured on, or do not give a number (issue #261)."
            );
        }
    }
    assert!(
        scanned > 500,
        "the sweep must actually have covered the tracked tree (scanned {scanned} files); \
         a sweep that silently covers nothing passes every ban"
    );
    assert!(
        inspected > 0,
        "the pre-filter matched nothing at all, which would make the ban vacuous — \
         the ledger alone should have carried a matching token"
    );

    // The positive half — the sentences that must survive.
    let ledger = normalize(&repo_file("docs/benchmarks/logs-differential-ledger.md"));
    assert!(
        ledger.contains("the agreement threshold is a property of the value strings"),
        "the ledger must keep saying what the threshold depends on"
    );
    assert!(
        ledger.contains("4533"),
        "the ledger must keep the svc-{{i}} counterexample cardinality 4533, which is \
         BELOW the number the retracted claim named"
    );
    assert!(
        ledger.contains("one n, three reference answers"),
        "the ledger must keep the one-N-three-answers observation, which is what makes \
         a threshold indexed by N impossible to write"
    );
    let api = normalize(&repo_file("docs/api.md"));
    assert!(
        api.contains(
            "there is no distinct-value count below which the two are guaranteed to agree"
        ),
        "docs/api.md §2.6.3 must keep the negative statement that replaced the bound"
    );
}

/// `docs/api.md` §2.6.2 must not list the absent `sketch` key as a
/// divergence again. It is parity: the reference strips the field on the
/// HTTP path in every deployment shape, and the section has to carry the
/// two citations that let a reader re-check that without re-measuring.
///
/// A false divergence entry is worse than a missing one, because the
/// next reader defends it.
#[test]
fn the_absent_sketch_key_is_documented_as_parity_not_as_a_divergence() {
    let api = repo_file("docs/api.md");
    let norm = normalize(&api);
    assert!(
        !norm.contains("no sketch key is emitted"),
        "docs/api.md §2.6.2 lists the absent `sketch` key as a deliberate divergence \
         again; the reference never emits it on the HTTP surface (issue #261)"
    );
    assert!(
        norm.contains("parity, not a divergence - no sketch key"),
        "docs/api.md §2.6.2 must state the absent `sketch` key as parity"
    );
    for citation in [
        "newdetectedlabelscardinalityfilter",
        "roundtrip.go:347-370",
        "modules.go:1368",
    ] {
        assert!(
            norm.contains(citation),
            "the parity note must keep its source citation {citation:?}"
        );
    }
}

/// The committed capture transcript in PROVENANCE.md and the committed
/// artifact must not drift apart: every disagreeing transcript row is a
/// TSV row with the same numbers and the same instrument, and every
/// agreeing transcript row is a header witness rather than a row.
///
/// Without this, the transcript is prose — the class of claim that never
/// fails on its own.
#[test]
fn the_provenance_transcript_and_the_artifact_agree() {
    let provenance = crate_file("tests/logqltest/PROVENANCE.md");
    let start = provenance
        .find("```pulsus-261-detected-labels-capture")
        .expect("the #261 capture block must exist in PROVENANCE.md");
    let body = &provenance[start..];
    let end = body[3..].find("```").expect("closing fence") + 3;
    let block = &body[..end];

    let rows = parse_rows(TSV);
    let head = header(TSV);
    let mut transcribed = 0usize;
    for line in block.lines().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        assert!(cols.len() >= 4, "malformed transcript row: {line:?}");
        let family = cols[0];
        let n: u64 = cols[1].parse().expect("transcript n");
        let value: u64 = cols[2].parse().expect("transcript cardinality");
        let observed_by = cols[3];
        transcribed += 1;
        if value == n {
            assert!(
                !rows.iter().any(|r| r.family == family && r.n_distinct == n),
                "{family}{n} agrees, so it must not be an artifact row"
            );
            assert!(
                head.contains(&format!("n = {n} -> {n}")),
                "the agreeing transcript row {family}{n} must be a header witness"
            );
        } else {
            let row = rows
                .iter()
                .find(|r| r.family == family && r.n_distinct == n)
                .unwrap_or_else(|| panic!("transcript row {family}{n} is missing from the TSV"));
            assert_eq!(row.reference_value, value, "{family}{n} value drift");
            assert_eq!(row.observed_by, observed_by, "{family}{n} instrument drift");
        }
    }
    assert_eq!(
        transcribed,
        rows.len() + 2,
        "the transcript must carry every artifact row plus the two last-agreeing witnesses"
    );
}

/// The production aggregate still computes the EXACT count.
///
/// Weak on its own, and said so: it reads rendered SQL text, so it only
/// stops a silent estimator swap in a text-level refactor. The value
/// claim is carried by
/// `detected_labels_cardinality_is_exact_at_the_reference_divergence_points`
/// (`crates/pulsus-server/tests/logs_detected_live.rs`), which asserts
/// the number on the HTTP response body.
#[test]
fn the_detected_labels_aggregate_is_still_an_exact_count() {
    for fingerprints in [None, Some(&[7u64, 9][..])] {
        let rendered = sql::detected_labels(
            "log_streams_idx",
            &["'2026-08-01'".to_string()],
            fingerprints,
        );
        assert!(
            rendered.contains("uniqExact(val) AS cardinality"),
            "the exact aggregate is the contract: {rendered}"
        );
        // Every ClickHouse approximate distinct-count entry point, not
        // just the one a refactor would most likely reach for.
        for estimator in [
            "uniq(",
            "uniqCombined",
            "uniqCombined64",
            "uniqHLL12",
            "uniqTheta",
            "uniqUpTo",
            "uniqExactIf",
        ] {
            assert!(
                !rendered.contains(estimator),
                "an approximate distinct-count ({estimator}) reached the \
                 /detected_labels aggregate: {rendered}"
            );
        }
    }
}
