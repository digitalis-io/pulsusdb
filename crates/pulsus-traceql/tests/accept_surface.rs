//! Parser accept-surface audit against the reference grammar (issue #335).
//!
//! Two accept-surface gaps were found *sideways* in one week — a colon-scoped
//! intrinsic rejected as a comparison right-hand side, and unary `!` binding
//! at the wrong level. Nobody had gone looking, and neither is visible to the
//! construct registry (`tests/conformance/`), which enumerates *documented
//! constructs* and so cannot see a precedence level we placed wrongly or an
//! operand position we never wired up.
//!
//! This suite is the systematic comparison that registry cannot do. Every
//! probe in `accept_surface/matrix.json` records:
//!   * `reference` — the black-box verdict of an unmodified
//!     `grafana/tempo:3.0.2` container (2xx = accept, 400 = reject), and
//!   * `pulsus` — what this parser does today, agreeing or not.
//!
//! A probe that *diverges* carries a `class` naming the structural reason
//! (see `divergence_classes` in the matrix). The counts are pinned, so a
//! divergence can only be added or removed deliberately; #335's fixes lower
//! `diverge` and raise `agree`.
//!
//! **`meaning_probes` are the dangerous half.** Both implementations accept
//! them and neither errors — they simply mean different things, so the same
//! query returns different spans with nothing to signal it. The reference's
//! own parse is recorded in `reference_parse`, captured from the fully
//! parenthesised expression the reference echoes in its type-error messages
//! (and, for the spanset level where no message exists, from a result
//! differential). No Tempo source is read: this is runtime use of the
//! upstream image as an oracle, the same posture as `tempo_differential.rs`.
//!
//! Gate: the live leg skips cleanly unless `PULSUSDB_TEMPO_DIFF_URL` is set.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;

#[derive(Deserialize)]
struct Matrix {
    divergence_classes: Vec<DivergenceClass>,
    accept_surface_probes: Vec<Probe>,
    meaning_probes: Vec<MeaningProbe>,
    closed_meaning_probes: Vec<ClosedMeaningProbe>,
}

#[derive(Deserialize)]
struct DivergenceClass {
    id: String,
    title: String,
    reference: String,
    pulsus: String,
    impact: String,
    /// `open` — still diverging, with probes to prove it. `closed` — fixed,
    /// and required to have NO diverging probe left, which is what turns
    /// "we closed it" into an assertion rather than a claim.
    status: String,
}

#[derive(Deserialize)]
struct Probe {
    query: String,
    reference: String,
    pulsus: String,
    verdict: String,
    #[serde(default)]
    class: Option<String>,
    /// Set when a probe that used to diverge was brought into agreement,
    /// naming the issue that did it.
    #[serde(default)]
    closed_by: Option<u32>,
    /// The divergence class the probe belonged to before it was closed —
    /// an agreement may not carry `class`, so the closure records it here
    /// (added with Stage A; the wave-1 D2 closures predate the field).
    #[serde(default)]
    closed_class: Option<String>,
}

/// A probe both implementations accept and parse *differently*. `pulsus_parse`
/// is this parser's fully parenthesising `Display`; `reference_parse` is the
/// reference's own rendering of its parse, and `evidence` names how it was
/// observed.
#[derive(Deserialize)]
struct MeaningProbe {
    query: String,
    class: String,
    reference_parse: String,
    pulsus_parse: String,
    evidence: String,
}

/// A meaning divergence that has been FIXED. `reference_grouping` is the
/// reference's reading written with explicit parentheses, so closure is
/// machine-checked by parsing both and comparing the ASTs — not asserted
/// by eye against the reference's own rendering, which spells the same
/// tree differently.
#[derive(Deserialize)]
struct ClosedMeaningProbe {
    query: String,
    class: String,
    reference_grouping: String,
    pulsus_parse: String,
    closed_by: u32,
}

/// Agreements / divergences pinned exactly. These only move with a
/// deliberate grammar change: a fix lowers `DIVERGE` and raises `AGREE` by
/// the same amount, and a regression fails here first.
///
/// Audit capture (#335): 221 probes, 176 agree, 45 diverge, 7 meaning
/// divergences. First fix wave (#335, classes D2/D8/D9/D10/D11): D2's
/// twenty probes flipped reject→accept, so `AGREE` 176 + 20 = 196 and
/// `DIVERGE` 45 − 20 = 25; no agreement became a divergence. D8/D9/D10/D11
/// are meaning-only and carry no accept-surface probe, so they move
/// `MEANING` 7 − 4 = 3 and leave the probe counts alone.
///
/// Stage A (#335): [`accepts`] became `parse ∘ validate` to match what the
/// reference verdict measures, flipping exactly two D1 probes
/// (`{ !name + 1 = 2 }`, `{ !name * 2 = 3 }`) diverge→agree — our
/// validator already rejects them; only the scoreboard scored parse alone
/// — so `AGREE` 196 + 2 = 198 and `DIVERGE` 25 − 2 = 23, with no probe
/// moving agree→diverge
/// ([`stage_a_flipped_exactly_the_two_recorded_d1_probes`] shows the flip
/// rather than inferring it from these counts). The remaining 23 are
/// D1 (5), D3 (7), D4 (4), D5 (2), D6 (1), D7 (4) — the field-expression
/// regrammar.
/// Stage B capture (#335 AC 4, binding ruling): the `!`/absence
/// de-conflation may not begin until the reference behaviour of every
/// spelling our tree conflates onto existence is measured. The
/// mechanism class is "spellings that parse to `Exists`/`Not(Exists)`
/// here" — bare, `!`, `= nil`, `!= nil`, at both scopes — and the
/// result differential found the conflation WIDER than the `!`/nil
/// pair the plan named: the reference reads a bare attribute as
/// truthiness while `!= nil` is presence, two spellings that are one
/// AST here. Class D12 records all of it; `MEANING` 3 + 6 = 9.
const TOTAL: usize = 221;
const AGREE: usize = 198;
const DIVERGE: usize = 23;
const MEANING: usize = 9;
const CLOSED_MEANING: usize = 4;

fn matrix() -> Matrix {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("accept_surface")
        .join("matrix.json");
    let raw = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("matrix.json must parse")
}

/// Our side of the scoreboard: `parse ∘ validate` (Stage A of #335). The
/// `reference` verdict this is scored against is an HTTP status from a
/// route that runs the reference's parse AND its semantic validation, so
/// scoring our side as `parse` alone compared two different measurements
/// — a query our validator already rejects was counted as an accept.
fn accepts(query: &str) -> bool {
    match pulsus_traceql::parse(query) {
        Ok(ast) => pulsus_traceql::validate(&ast).is_ok(),
        Err(_) => false,
    }
}

#[test]
fn every_probe_still_matches_its_recorded_pulsus_disposition() {
    let m = matrix();
    let mut drift = Vec::new();
    for p in &m.accept_surface_probes {
        let want = match p.pulsus.as_str() {
            "accept" => true,
            "reject" => false,
            other => panic!("{}: bad recorded pulsus verdict {other:?}", p.query),
        };
        if accepts(&p.query) != want {
            drift.push(format!(
                "{:?}: recorded pulsus={} but the parser now {}s it",
                p.query,
                p.pulsus,
                if want { "reject" } else { "accept" }
            ));
        }
    }
    assert!(
        drift.is_empty(),
        "{} probe(s) drifted — an accept-surface change must be deliberate: re-record the \
         `pulsus` field (and the pinned counts) in the same change:\n{}",
        drift.len(),
        drift.join("\n")
    );
}

#[test]
fn recorded_verdicts_agree_with_the_two_recorded_sides() {
    let m = matrix();
    for p in &m.accept_surface_probes {
        let want = if p.reference == p.pulsus {
            "agree"
        } else {
            "diverge"
        };
        assert_eq!(p.verdict, want, "{:?}: verdict does not follow", p.query);
    }
}

#[test]
fn every_divergence_carries_a_class_and_every_class_is_used() {
    let m = matrix();
    let declared: Vec<&str> = m.divergence_classes.iter().map(|c| c.id.as_str()).collect();
    for p in &m.accept_surface_probes {
        match (p.verdict.as_str(), &p.class) {
            ("diverge", None) => panic!("{:?}: a divergence needs a class", p.query),
            ("diverge", Some(c)) => assert!(
                declared.contains(&c.as_str()),
                "{:?}: undeclared class {c}",
                p.query
            ),
            ("agree", Some(c)) => panic!("{:?}: an agreement must not carry class {c}", p.query),
            _ => {}
        }
        if let Some(cc) = &p.closed_class {
            assert!(
                declared.contains(&cc.as_str()),
                "{:?}: undeclared closed_class {cc}",
                p.query
            );
            assert!(
                p.closed_by.is_some() && p.verdict == "agree",
                "{:?}: closed_class requires a closure on an agreeing probe",
                p.query
            );
        }
    }
    for mp in &m.meaning_probes {
        assert!(
            declared.contains(&mp.class.as_str()),
            "{:?}: undeclared class {}",
            mp.query,
            mp.class
        );
    }
    for cp in &m.closed_meaning_probes {
        assert!(
            declared.contains(&cp.class.as_str()),
            "{:?}: undeclared class {}",
            cp.query,
            cp.class
        );
    }
    for c in &m.divergence_classes {
        let open_probes = m
            .accept_surface_probes
            .iter()
            .filter(|p| p.class.as_deref() == Some(c.id.as_str()))
            .count()
            + m.meaning_probes.iter().filter(|p| p.class == c.id).count();
        match c.status.as_str() {
            "open" => assert!(
                open_probes > 0,
                "class {} is declared open but no probe still diverges — close it",
                c.id
            ),
            // The teeth: a class cannot be recorded closed while a probe
            // still shows the divergence.
            "closed" => {
                assert_eq!(
                    open_probes, 0,
                    "class {} is recorded closed but {open_probes} probe(s) still diverge",
                    c.id
                );
                assert!(
                    m.closed_meaning_probes.iter().any(|p| p.class == c.id)
                        || m.accept_surface_probes
                            .iter()
                            .any(|p| p.closed_by.is_some() && p.verdict == "agree"),
                    "class {} is recorded closed with nothing recording the closure",
                    c.id
                );
            }
            other => panic!("class {}: bad status {other:?}", c.id),
        }
        for (field, value) in [
            ("title", &c.title),
            ("reference", &c.reference),
            ("pulsus", &c.pulsus),
        ] {
            assert!(!value.trim().is_empty(), "class {}: empty {field}", c.id);
        }
        assert!(
            matches!(
                c.impact.as_str(),
                "accept-surface" | "quiet meaning" | "accept-surface AND quiet meaning"
            ),
            "class {}: bad impact {:?}",
            c.id,
            c.impact
        );
    }
}

#[test]
fn agreement_and_divergence_counts_are_pinned() {
    let m = matrix();
    let total = m.accept_surface_probes.len();
    let diverge = m
        .accept_surface_probes
        .iter()
        .filter(|p| p.verdict == "diverge")
        .count();
    assert_eq!(total, TOTAL, "probe count moved");
    assert_eq!(total - diverge, AGREE, "agreement count moved");
    assert_eq!(diverge, DIVERGE, "divergence count moved");
    assert_eq!(m.meaning_probes.len(), MEANING, "meaning-probe count moved");
    assert_eq!(
        m.closed_meaning_probes.len(),
        CLOSED_MEANING,
        "closed-meaning-probe count moved"
    );
    // The arithmetic in the constants' doc comment, asserted rather than
    // narrated: nothing may leave `agree` on the way to a lower `diverge`.
    let closed = m
        .accept_surface_probes
        .iter()
        .filter(|p| p.closed_by.is_some())
        .count();
    assert!(
        m.accept_surface_probes
            .iter()
            .all(|p| p.closed_by.is_none() || p.verdict == "agree"),
        "a probe recorded as closed must now agree"
    );
    assert_eq!(
        closed + DIVERGE,
        45,
        "closed + still-diverging must equal the audit capture's 45 divergences"
    );
}

/// Stage B of #335, AC 4 (binding): the `!`/absence collapse cannot
/// begin until these spellings' reference behaviour is captured — this
/// test is what makes that an ordering fact rather than an intention,
/// together with the capture landing in its own commit before any
/// grammar change. The four ruled queries plus the two bare spellings
/// the capture found conflated the same way must be present by exact
/// string, as D12 meaning probes.
#[test]
fn stage_b_not_absence_meaning_probes_are_captured() {
    let m = matrix();
    for q in [
        // The four the ruling names:
        "{ !.a }",
        "{ !span.a }",
        "{ .a = nil }",
        "{ span.a = nil }",
        // The two the capture's own sweep added (bare = truthiness in
        // the reference, presence here):
        "{ .a }",
        "{ span.a }",
    ] {
        let p = m
            .meaning_probes
            .iter()
            .find(|p| p.query == q)
            .unwrap_or_else(|| panic!("AC4: {q:?} must be captured before the collapse"));
        assert_eq!(p.class, "D12", "{q:?}");
        assert!(
            p.evidence.contains("result differential"),
            "{q:?}: the capture must be container-measured, not asserted"
        );
    }
}

/// Stage A of #335, SHOWN rather than inferred from the counts: the old
/// verdict (parse alone accepts) and the new one (parse ∘ validate
/// rejects) are both asserted for the two flipped probes, and the flip
/// set is pinned exactly — over the whole matrix, `parse` and
/// `parse ∘ validate` disagree on precisely these two class-D1 probes,
/// so nothing else moved in either direction, per probe rather than in
/// aggregate.
#[test]
fn stage_a_flipped_exactly_the_two_recorded_d1_probes() {
    const FLIPPED: [&str; 2] = ["{ !name + 1 = 2 }", "{ !name * 2 = 3 }"];
    for q in FLIPPED {
        // The OLD verdict: parse alone accepts — this is what the
        // scoreboard used to score, and why these diverged.
        let ast = pulsus_traceql::parse(q).unwrap_or_else(|e| {
            panic!("{q:?}: the pre-Stage-A verdict (parse accepts) broke: {e}")
        });
        // The NEW verdict: validation rejects, agreeing with the
        // reference's recorded reject.
        assert!(
            pulsus_traceql::validate(&ast).is_err(),
            "{q:?}: the post-Stage-A verdict (validate rejects) broke"
        );
    }
    let m = matrix();
    let flipped_now: Vec<&str> = m
        .accept_surface_probes
        .iter()
        .filter(|p| pulsus_traceql::parse(&p.query).is_ok() != accepts(&p.query))
        .map(|p| p.query.as_str())
        .collect();
    assert_eq!(
        flipped_now, FLIPPED,
        "the parse-vs-validate flip set moved — Stage A's claim was exactly these two"
    );
    for q in FLIPPED {
        let p = m
            .accept_surface_probes
            .iter()
            .find(|p| p.query == q)
            .expect("flipped probe present");
        assert_eq!(p.reference, "reject", "{q:?}");
        assert_eq!(
            p.pulsus, "reject",
            "{q:?}: recorded verdict must be the new one"
        );
        assert_eq!(p.verdict, "agree", "{q:?}: must not still diverge");
        assert_eq!(p.closed_by, Some(335), "{q:?}: closure must name the issue");
        assert_eq!(
            p.closed_class.as_deref(),
            Some("D1"),
            "{q:?}: closure must name the class it closed"
        );
    }
}

/// Closure is proved, not asserted: our parse of the bare query must equal
/// our parse of the reference's grouping written with explicit parentheses.
/// Before the fix each of these compared unequal.
#[test]
fn closed_meaning_probes_now_group_like_the_reference() {
    let m = matrix();
    for p in &m.closed_meaning_probes {
        assert_eq!(p.closed_by, 335, "{:?}: unexpected closing issue", p.query);
        let bare = pulsus_traceql::parse(&p.query)
            .unwrap_or_else(|e| panic!("{:?} must parse: {e}", p.query));
        let explicit = pulsus_traceql::parse(&p.reference_grouping).unwrap_or_else(|e| {
            panic!(
                "{:?}: the recorded reference grouping must parse: {e}",
                p.reference_grouping
            )
        });
        assert_eq!(
            bare, explicit,
            "{:?} no longer groups like the reference's {:?}",
            p.query, p.reference_grouping
        );
        assert_eq!(
            bare.to_string(),
            p.pulsus_parse,
            "{:?}: rendering drifted",
            p.query
        );
    }
}

/// The quiet half: both sides accept, so only the *rendering* can catch a
/// drift. Pinning this parser's `Display` means a grammar fix flips these
/// RED and has to update `pulsus_parse` to the reference's reading —
/// the mechanism by which a meaning divergence gets closed on purpose.
#[test]
fn meaning_probes_still_parse_the_way_this_parser_recorded() {
    let m = matrix();
    let mut drift = Vec::new();
    for p in &m.meaning_probes {
        assert!(
            !p.evidence.trim().is_empty(),
            "{:?}: a meaning probe needs its observation evidence",
            p.query
        );
        assert_ne!(
            p.reference_parse, p.pulsus_parse,
            "{:?}: recorded as a meaning divergence but both parses are identical",
            p.query
        );
        match pulsus_traceql::parse(&p.query) {
            Ok(parsed) => {
                let rendered = parsed.to_string();
                if rendered != p.pulsus_parse {
                    drift.push(format!(
                        "{:?}: recorded {:?} but now renders {rendered:?}",
                        p.query, p.pulsus_parse
                    ));
                }
            }
            Err(e) => drift.push(format!("{:?}: no longer parses ({e})", p.query)),
        }
    }
    assert!(
        drift.is_empty(),
        "{} meaning probe(s) drifted — closing one is a deliberate grammar change:\n{}",
        drift.len(),
        drift.join("\n")
    );
}

fn is_metrics(q: &str) -> bool {
    ["rate(", "_over_time(", "compare(", "topk(", "bottomk("]
        .iter()
        .any(|m| q.contains(m))
}

/// GETs a query and maps the HTTP status to accept/reject. Anything other
/// than 2xx/400 is inconclusive and fails loudly — never silently counted.
fn reference_accepts(base: &str, query: &str) -> bool {
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
        200..=299 => true,
        400 => false,
        other => panic!(
            "inconclusive: the reference returned {other} for {query:?} \
             (only 2xx=accept / 400=reject are conclusive)"
        ),
    }
}

#[test]
fn recorded_reference_verdicts_still_hold() {
    let Ok(base) = std::env::var("PULSUSDB_TEMPO_DIFF_URL") else {
        eprintln!("PULSUSDB_TEMPO_DIFF_URL unset; skipping the accept-surface oracle leg");
        return;
    };
    let m = matrix();
    let mut mismatches = Vec::new();
    for p in &m.accept_surface_probes {
        let want = p.reference == "accept";
        if reference_accepts(&base, &p.query) != want {
            mismatches.push(format!(
                "{:?}: recorded reference={} but the live oracle disagrees",
                p.query, p.reference
            ));
        }
    }
    // Every meaning probe (open or closed) is accepted by both sides by
    // construction; assert that much (the grouping itself is pinned
    // hermetically above).
    for p in m
        .meaning_probes
        .iter()
        .map(|p| &p.query)
        .chain(m.closed_meaning_probes.iter().map(|p| &p.query))
    {
        if !reference_accepts(&base, p) {
            mismatches.push(format!(
                "{p:?}: recorded as accepted-by-both but the live oracle rejects it"
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} probe(s) disagreed with the recorded reference verdict — an oracle change is real, \
         re-record it deliberately:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
    eprintln!(
        "accept-surface audit: {} probes + {} open and {} closed meaning probes replayed \
         against the oracle",
        m.accept_surface_probes.len(),
        m.meaning_probes.len(),
        m.closed_meaning_probes.len()
    );
}
