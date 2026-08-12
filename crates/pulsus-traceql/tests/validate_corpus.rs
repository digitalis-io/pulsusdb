//! Issue #328: the hermetic half of the semantic validator's evidence.
//!
//! `tests/conformance/validate-vectors.json` records, for every vector,
//! the verdict of the PINNED reference container (its `captured` header
//! names the image digest and date; the capture route is the search
//! shadow `query` parameter — parse + `traceql.Validate` only, no
//! execution, the purest observation of the route-independent
//! validator). This suite replays every vector through our
//! `parse` + `validate` and asserts:
//!
//! * the recorded `pulsus` verdict is reproduced, variant for variant;
//! * `pulsus == tempo` unless the row names a `divergence` — and every
//!   named divergence id exists in the traces differential ledger, and
//!   every `traceql-validate-*` ledger row id is named by some vector
//!   (the two-way fixture↔ledger link, D9);
//! * coverage cannot silently shrink: every implemented check has ≥1
//!   accept AND ≥1 reject vector, every `ValidateError` variant is
//!   witnessed, and every measured `Re2Verdict::Unknown` residual class
//!   appears;
//! * the whole-corpus half (AC 8): every registry probe and every
//!   committed corpus accept case that parses also VALIDATES — the
//!   standing guard against a planner-only rule creeping into the
//!   validator.
//!
//! The live half — re-issuing every vector against the pinned container
//! and asserting the recorded status — is
//! `tempo_differential.rs::validate_vectors_match_the_live_reference`,
//! gated fail-closed on `PULSUSDB_TEMPO_VECTORS`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use serde::Deserialize;

use pulsus_traceql::{VALIDATE_RULES, ValidateError, parse, validate};

fn conf_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("conformance")
}

fn read(path: PathBuf) -> String {
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[derive(Debug, Deserialize)]
struct Vectors {
    captured: Captured,
    vectors: Vec<Vector>,
}

#[derive(Debug, Deserialize)]
struct Captured {
    image: String,
    #[allow(dead_code)]
    date: String,
    #[allow(dead_code)]
    route_note: String,
}

#[derive(Debug, Deserialize)]
struct Vector {
    id: String,
    check: String,
    query: String,
    route: String,
    tempo_status: u16,
    tempo: String,
    pulsus: String,
    #[serde(default)]
    error_variant: Option<String>,
    #[serde(default)]
    unknown_class: Option<String>,
    #[serde(default)]
    divergence: Option<String>,
}

fn load() -> Vectors {
    serde_json::from_str(&read(conf_dir().join("validate-vectors.json"))).expect("vectors JSON")
}

/// The implemented checks — must mirror `VALIDATE_RULES`' ids (asserted
/// below), so the coverage loop and the rule table cannot drift apart.
fn implemented_checks() -> Vec<&'static str> {
    VALIDATE_RULES.iter().map(|(id, _)| *id).collect()
}

/// The `Re2Verdict::Unknown` classes, DERIVED FROM THE CODE rather than
/// from probing (issue #328 fix round 1): one class family per `Unknown`
/// return site in `pulsus-re2`, mirroring the per-site census
/// `pulsus_re2::…::every_unknown_return_site_has_a_named_class_representative`.
/// The mechanical halves of the link: every class here must carry a
/// vector (below), and every vector's `unknown_class` must appear here
/// (below). Stated at its true strength (fix round 2): a BRAND-NEW
/// `Unknown` return site in `pulsus-re2` is caught by these tests only
/// if it moves a verdict in the frozen `screen_verdicts.txt` corpus —
/// otherwise review of that crate ALONE is the guard, census included
/// (a new site has no row until someone adds one). The mechanism that
/// closes this is #336's RE2-syntax parser, deliberately not built
/// here. Which members the reference accepts or rejects is measured per
/// class into the vectors and ledgered
/// (`traceql-validate-re2-unknown-residual`, owner #336).
const UNKNOWN_CLASSES: &[&str] = &[
    // scan: the escape check, bare or inside a class. **`rust-only-escape`
    // retired with #400 Stage 2** — rule (d) refuses every `\u`/`\U`
    // spelling, so the class has no `Unknown` member left.
    // `unicode-property` survives on the half of it that is IN
    // `unicodeTable`.
    "unicode-property",
    "boundary-escape",
    "trailing-backslash",
    // scan: non-portable `(?…` group heads
    "lookaround",
    "named-group",
    "nonportable-group-head",
    // compile: Rust rejects inside RE2's accept set, or its own budget
    "literal-quoting",
    "octal-escape",
    "compiled-too-big",
];

/// **The ten vectors #400 Stage 2 moved `pulsus: accept -> reject`**, and
/// the direction assertion that makes a move the OTHER way red rather
/// than merely reviewable.
///
/// `pulsus_traceql::validate`'s `check_regex` consumes
/// `pulsus_re2::re2_verdict`, which now consults
/// `re2_definitely_rejects` first. Every vector below already recorded
/// `tempo: reject, tempo_status: 400`, so every move is toward parity —
/// provable from committed data, with no container. A row that moved
/// while Tempo SERVES it would be a regression, and this list plus the
/// assertion in [`every_vector_reproduces_its_recorded_pulsus_verdict`]
/// is what says so.
///
/// Three `UNKNOWN_CLASSES` retired with them, because every member moved:
/// `repetition-of-repetition` (rx-u6, rx-u21, rx-u22), `over-max-repeat`
/// (rx-u5) and `rust-only-escape` (rx-u12, rx-u13). `unicode-property`
/// survives on rx-u17 (`\p{L}` — the name IS in `unicodeTable`, so both
/// engines serve it) and `nonportable-group-head` on rx-u20 (`(?#c)a` —
/// `#` is not one of the `{u, x, R}` flags rule (c) claims).
const MOVED_BY_400_STAGE2: &[&str] = &[
    "rx-u4", "rx-u5", "rx-u6", "rx-u12", "rx-u13", "rx-u16", "rx-u18", "rx-u19", "rx-u21", "rx-u22",
];

#[test]
fn every_vector_reproduces_its_recorded_pulsus_verdict() {
    let doc = load();
    assert!(
        doc.captured.image.starts_with("grafana/tempo@sha256:"),
        "the capture header must pin the digest, got {:?}",
        doc.captured.image
    );
    for v in &doc.vectors {
        assert_eq!(v.route, "search-query", "{}: unexpected route", v.id);
        let query = parse(&v.query)
            .unwrap_or_else(|e| panic!("{}: {:?} must parse here: {e}", v.id, v.query));
        let verdict = validate(&query);
        match v.pulsus.as_str() {
            "accept" => assert_eq!(
                verdict,
                Ok(()),
                "{}: {:?} must validate Ok (recorded pulsus=accept)",
                v.id,
                v.query
            ),
            "reject" => {
                let err = verdict.expect_err(&format!(
                    "{}: {:?} must be rejected (recorded pulsus=reject)",
                    v.id, v.query
                ));
                let want = v
                    .error_variant
                    .as_deref()
                    .unwrap_or_else(|| panic!("{}: a reject row must name error_variant", v.id));
                assert_eq!(
                    err.rule_id(),
                    want,
                    "{}: {:?} rejected under the wrong rule ({err})",
                    v.id,
                    v.query
                );
            }
            other => panic!("{}: bad pulsus verdict {other:?}", v.id),
        }
        // The status column and the verdict column must agree with each
        // other — a fabricated row cannot carry a 200 reject.
        match (v.tempo.as_str(), v.tempo_status) {
            ("accept", 200..=299) | ("reject", 400) => {}
            (t, s) => panic!("{}: tempo={t:?} disagrees with tempo_status={s}", v.id),
        }
    }
    // Issue #400 Stage 2, criterion 17: the DIRECTION of the ten moved
    // rows. Every one must be a rejection that Tempo also rejects — a
    // vector that moved to `reject` while Tempo serves it is a
    // regression wearing the same shape as a fix.
    for id in MOVED_BY_400_STAGE2 {
        let v = doc
            .vectors
            .iter()
            .find(|v| v.id == *id)
            .unwrap_or_else(|| panic!("{id}: moved vector is gone from the corpus"));
        assert_eq!(v.pulsus, "reject", "{id}: must be a rejection here");
        assert_eq!(
            v.tempo, "reject",
            "{id}: PulsusDB refuses this and Tempo SERVES it — the move was away from parity"
        );
        assert_eq!(v.tempo_status, 400, "{id}");
        assert_eq!(
            v.error_variant.as_deref(),
            Some("invalid-regex"),
            "{id}: the rejection must come from the regex check, not another rule"
        );
        assert!(
            v.divergence.is_none() && v.unknown_class.is_none(),
            "{id}: both sides reject it now, so it is neither a divergence nor an `Unknown`              residual — leaving either field would keep a retired class alive"
        );
    }
}

#[test]
fn every_divergence_is_ledgered_and_every_ledger_row_is_witnessed() {
    let doc = load();
    let ledger = read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/benchmarks/traces-differential-ledger.md"),
    );
    let mut named: BTreeSet<&str> = BTreeSet::new();
    for v in &doc.vectors {
        match &v.divergence {
            Some(row) => {
                named.insert(row.as_str());
                assert!(
                    ledger.contains(&format!("### `{row}`")),
                    "{}: divergence {row:?} has no ledger row",
                    v.id
                );
            }
            None => assert_eq!(
                v.pulsus, v.tempo,
                "{}: pulsus and tempo disagree with no divergence row named",
                v.id
            ),
        }
    }
    // The reverse direction: every LIVE `traceql-validate-*` ledger row id
    // is witnessed by at least one vector, so neither side can be retired
    // alone.
    //
    // A row whose heading is marked `RETIRED` is held to the OPPOSITE
    // requirement: it must be witnessed by NO vector. A retired row
    // describes a divergence that no longer exists, so a surviving witness
    // means the retirement is false. That inversion is what stops
    // "RETIRED" being a way to silence this gate — the word does not
    // exempt the row, it swaps which assertion it must satisfy.
    for line in ledger.lines() {
        let Some(rest) = line.strip_prefix("### `traceql-validate-") else {
            continue;
        };
        let id = format!(
            "traceql-validate-{}",
            rest.split('`').next().unwrap_or_default()
        );
        if line.contains("RETIRED") {
            assert!(
                !named.contains(id.as_str()),
                "ledger row {id:?} is marked RETIRED but a vector still names it as a divergence"
            );
        } else {
            assert!(
                named.contains(id.as_str()),
                "ledger row {id:?} is witnessed by no vector"
            );
        }
    }
}

#[test]
fn coverage_cannot_silently_shrink() {
    let doc = load();
    let mut accept_per_check: BTreeMap<&str, usize> = BTreeMap::new();
    let mut reject_per_check: BTreeMap<&str, usize> = BTreeMap::new();
    let mut variants: BTreeSet<&str> = BTreeSet::new();
    let mut classes: BTreeSet<&str> = BTreeSet::new();
    let mut ids: BTreeSet<&str> = BTreeSet::new();
    for v in &doc.vectors {
        assert!(ids.insert(&v.id), "duplicate vector id {:?}", v.id);
        match v.pulsus.as_str() {
            "accept" => *accept_per_check.entry(v.check.as_str()).or_default() += 1,
            _ => *reject_per_check.entry(v.check.as_str()).or_default() += 1,
        }
        if let Some(variant) = &v.error_variant {
            variants.insert(variant.as_str());
        }
        if let Some(class) = &v.unknown_class {
            classes.insert(class.as_str());
        }
    }
    for check in implemented_checks() {
        assert!(
            accept_per_check.get(check).copied().unwrap_or(0) >= 1,
            "check {check:?} has no accept vector"
        );
        assert!(
            reject_per_check.get(check).copied().unwrap_or(0) >= 1,
            "check {check:?} has no reject vector"
        );
    }
    for variant in implemented_checks() {
        assert!(
            variants.contains(variant),
            "ValidateError variant {variant:?} is witnessed by no reject vector"
        );
    }
    for class in UNKNOWN_CLASSES {
        assert!(
            classes.contains(class),
            "Unknown residual class {class:?} has no vector"
        );
    }
    for class in &classes {
        assert!(
            UNKNOWN_CLASSES.contains(class),
            "vector unknown_class {class:?} is not in the enumerated class list — \
             extend UNKNOWN_CLASSES and the ledger row together"
        );
    }
}

/// AC 8's whole-corpus half: every registry probe and every committed
/// accept-corpus case that PARSES also VALIDATES. This is what stands
/// against a planner-only rule quietly re-implemented in the validator:
/// the corpus is the accepted language, and the validator must accept
/// all of it that the recorded Tempo verdict accepts.
#[test]
fn every_parsing_registry_probe_and_corpus_case_validates() {
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
        tempo: String,
    }
    let registry: Registry =
        serde_json::from_str(&read(conf_dir().join("registry-traceql-v3.0.2.json"))).unwrap();
    let disp: Dispositions =
        serde_json::from_str(&read(conf_dir().join("dispositions.json"))).unwrap();
    let tempo_accepts: BTreeMap<&str, bool> = disp
        .entries
        .iter()
        .map(|d| (d.construct.as_str(), d.tempo == "accept"))
        .collect();
    let mut checked = 0usize;
    for c in &registry.constructs {
        let Ok(query) = parse(&c.probe) else { continue };
        if tempo_accepts.get(c.id.as_str()).copied().unwrap_or(false) {
            assert_eq!(
                validate(&query),
                Ok(()),
                "registry probe {} ({:?}) parses and Tempo accepts it — the validator must too",
                c.id,
                c.probe
            );
            checked += 1;
        }
    }
    assert!(checked > 80, "only {checked} registry probes checked");

    // The committed accept corpora.
    for dir in ["accept", "grafana"] {
        let corpus_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("corpus")
            .join(dir);
        let mut cases = 0usize;
        for entry in fs::read_dir(&corpus_dir).expect("corpus dir") {
            let path = entry.expect("dir entry").path();
            if path.extension().is_none_or(|e| e != "traceql") {
                continue;
            }
            let text = read(path.clone());
            let expr = text.trim_end_matches('\n');
            let Ok(query) = parse(expr) else {
                panic!("accept corpus case {path:?} no longer parses")
            };
            assert_eq!(
                validate(&query),
                Ok(()),
                "accept corpus case {path:?} ({expr:?}) must validate Ok"
            );
            cases += 1;
        }
        assert!(cases > 0, "no cases under {corpus_dir:?}");
    }
}

/// The vector file's queries must PARSE here — a vector that stops
/// parsing would silently drop out of the semantic evidence (the parse
/// evidence lives in the #326 suites).
#[test]
fn every_vector_query_parses() {
    for v in load().vectors {
        assert!(
            parse(&v.query).is_ok(),
            "{}: {:?} no longer parses",
            v.id,
            v.query
        );
    }
}

/// `implemented_checks` mirrors the rule table, and the misc
/// route-independence vectors are the only rows outside it.
#[test]
fn vector_checks_are_rule_ids_or_the_route_independence_group() {
    let known: BTreeSet<&str> = implemented_checks().into_iter().collect();
    for v in load().vectors {
        assert!(
            known.contains(v.check.as_str()) || v.check == "route-independence",
            "{}: unknown check {:?}",
            v.id,
            v.check
        );
    }
}

/// The error variants named by the vectors are real rule ids (a typo'd
/// variant would otherwise vacuously satisfy nothing).
#[test]
fn vector_error_variants_are_real_rule_ids() {
    let known: BTreeSet<&str> = implemented_checks().into_iter().collect();
    for v in load().vectors {
        if let Some(variant) = &v.error_variant {
            assert!(
                known.contains(variant.as_str()),
                "{}: unknown error_variant {:?}",
                v.id,
                variant
            );
        }
    }
    // And the enum side cannot grow silently either: an instance of each
    // variant maps onto the table (the exhaustive-match guarantee, made
    // observable here).
    let witnessed: BTreeSet<&str> = [
        ValidateError::TypeMismatch {
            expr: String::new(),
        }
        .rule_id(),
        ValidateError::IllegalOperator {
            expr: String::new(),
        }
        .rule_id(),
        ValidateError::InvalidRegex {
            value: String::new(),
            reason: String::new(),
        }
        .rule_id(),
        ValidateError::InvalidRegexOperand {
            operand: String::new(),
        }
        .rule_id(),
        ValidateError::SpansetFilterNotBoolean {
            expr: String::new(),
        }
        .rule_id(),
        ValidateError::IllegalUnaryOperator {
            expr: String::new(),
        }
        .rule_id(),
        ValidateError::AggregateNotNumeric {
            expr: String::new(),
        }
        .rule_id(),
        ValidateError::AggregateNotSpanReferencing {
            expr: String::new(),
        }
        .rule_id(),
        ValidateError::IntrinsicNotNil {
            field: String::new(),
        }
        .rule_id(),
        ValidateError::QuantileOutOfRange {
            value: String::new(),
        }
        .rule_id(),
        ValidateError::TooManyGroupBys { got: 0, max: 0 }.rule_id(),
        ValidateError::NonPositiveLimit {
            expr: String::new(),
        }
        .rule_id(),
        ValidateError::CompareWithSecondStage.rule_id(),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        witnessed, known,
        "the ValidateError variants and VALIDATE_RULES ids must be the same set"
    );
}
