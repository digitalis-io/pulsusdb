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

use std::collections::{BTreeMap, BTreeSet};
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
    ///
    /// **This is the PARSE axis** (`parse ∘ validate`, what
    /// [`accepts`] measures). A class closed here may still be open on
    /// the wire — see [`DivergenceClass::wire_status`].
    status: String,
    /// The WIRE axis (`parse → validate → plan`), present only on a
    /// class whose two axes disagree (issue #335 Stage C: D7). Held to
    /// the same teeth as `status`, one axis over:
    /// [`a_class_open_on_the_wire_has_a_probe_still_diverging_there`]
    /// joins this against the committed `wire_baseline.json` column, so
    /// the field cannot become a comfortable sentence.
    #[serde(default)]
    wire_status: Option<String>,
    /// Why the two axes disagree, in the class row rather than only in
    /// PROVENANCE.md — a reader meeting `status: "closed"` must not have
    /// to go looking. Required (and non-empty) whenever `wire_status` is.
    #[serde(default)]
    wire_note: Option<String>,
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
    /// The issue that owns the gap a probe diverging on the WIRE names
    /// (#351's convention, made checkable here). Required exactly when a
    /// probe agrees on the parse axis and diverges on the wire — the
    /// case that has no `class` to be owned through — and forbidden once
    /// it agrees on the wire, so a closed gap cannot leave a stale
    /// pointer behind. See
    /// [`a_wire_divergence_the_parse_axis_cannot_see_names_its_owning_issue`].
    #[serde(default)]
    owning_issue: Option<u32>,
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
///
/// Stage B (#335), the grammar collapse: one precedence-climbing routine
/// replaces the layered field-expression parser, so every operand
/// position takes the same grammar and `!` becomes a field-level prefix
/// operator. That closes five classes outright — **D1** (5 probes),
/// **D3** (7), **D4** (4), **D5** (2), **D6** (1) — moving 19 probes
/// diverge→agree and none the other way, so `AGREE` 198 + 19 = 217 and
/// `DIVERGE` 23 − 19 = 4. Fifteen were reject→accept (the uniform operand
/// grammar now accepts what the reference accepts) and four were
/// accept→reject (`{ !name = "foo" }` and friends: `!` binds tighter than
/// `=`, so its operand is the intrinsic, and the reference rejects `!` on
/// a non-boolean with the same message we now produce).
///
/// The 4 residuals are all **D7** — `avg(<field expr>)` still takes a
/// restricted argument grammar. That is Stage C, not a surprise.
///
/// The direction claim is checked in the DIFF, not inferred here: the
/// re-pin commit changes 19 `"verdict"` lines, every one `diverge` →
/// `agree`.
///
/// Stage C (#335), the aggregate argument: `parse_aggregate` takes a
/// full field expression and `validate` gained rule 11 (numeric-or-
/// attribute AND references the span), so **D7**'s 4 probes flip
/// diverge→agree — `AGREE` 217 + 4 = 221 and `DIVERGE` 4 − 0 = 0 — and
/// two both-reject probes (`avg(1)`, `avg("x")`) move their rejection
/// from the parser to the validator without changing verdict.
///
/// **`DIVERGE == 0` is a statement about the PARSE axis and nothing
/// more.** D7 is not closed for a user: on the wire
/// (`parse → validate → plan`, `pulsus-read/tests/accept_surface_wire.rs`)
/// three of its four probes are still planner 400s against a reference
/// 2xx — `avg(span:childCount)` and `avg(trace:duration)` have no
/// numeric aggregation path and `avg(.a + 1)` is a composite source. The
/// class row carries `wire_status: "open"` saying so. Read this constant
/// as "the grammar agrees", never as "these queries work".
const TOTAL: usize = 221;
const AGREE: usize = 221;
const DIVERGE: usize = 0;
const MEANING: usize = 6;
const CLOSED_MEANING: usize = 7;

/// The audit matrix, **validated at load** on the two properties every
/// gate in this file reads through:
///
/// * **Query uniqueness.** The query is a probe's identity here, and
///   several gates look one up with `find`, which takes the first match
///   and says nothing about a second.
/// * **The `reference` domain.** `accept` | `reject` and nothing else,
///   because every scoring comparison in this file is a string equality
///   against that column: an `""` or `"accept "` would not fail as
///   malformed, it would quietly score as the opposite verdict.
///
/// **Why the digest is not enough** (review round, and my earlier
/// reasoning was wrong here):
/// [`the_reference_column_is_frozen_against_silent_re_pinning`] digests
/// `(query, reference)` in order, which makes it a CHANGE TRIPWIRE, not
/// a uniqueness assertion. Re-pinning is a sanctioned operation, and a
/// re-pin that duplicates a query moves the digest with it — consistent,
/// and still two probes scoring off one baseline row.
fn matrix() -> Matrix {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("accept_surface")
        .join("matrix.json");
    let raw = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let m: Matrix = serde_json::from_str(&raw).expect("matrix.json must parse");

    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut duplicated = Vec::new();
    let mut malformed = Vec::new();
    for p in &m.accept_surface_probes {
        if !seen.insert(p.query.as_str()) {
            duplicated.push(p.query.clone());
        }
        if !matches!(p.reference.as_str(), "accept" | "reject") {
            malformed.push(format!("{:?} -> reference {:?}", p.query, p.reference));
        }
    }
    assert!(
        duplicated.is_empty(),
        "{} duplicate probe key(s) in matrix.json — a probe's query IS its identity, and a \
         duplicate makes two probes score off one baseline row while the reference digest \
         stays consistent with itself:\n{}",
        duplicated.len(),
        duplicated.join("\n")
    );
    assert!(
        malformed.is_empty(),
        "{} probe(s) carry a reference verdict outside {{accept, reject}} — every gate here \
         compares against that column by string equality, so a malformed value scores as the \
         opposite verdict instead of failing as bad data:\n{}",
        malformed.len(),
        malformed.join("\n")
    );
    m
}

/// The committed WIRE-axis column. Only the two fields the joins below
/// need; the file's own shape is held by
/// `pulsus-read/tests/accept_surface_wire.rs`, which is also what
/// re-derives `pulsus_wire` from the tree under test.
#[derive(Deserialize)]
struct WireBaseline {
    wire_baseline: Vec<WireProbe>,
}

#[derive(Deserialize)]
struct WireProbe {
    query: String,
    pulsus_wire: String,
}

/// The wire column as a join map, **validated before anything is scored
/// through it**: the query is the probe's identity here, so the join has
/// to be total and one-to-one or a verdict read through it means
/// nothing.
///
/// The earlier cut looked a probe up with `.iter().find(...)`, which
/// takes the FIRST match and says nothing about a second — the Rust
/// spelling of the weakness `wire-baseline-freeze` rejects in every file
/// it builds a join from (`jq`'s `from_entries` silently keeps a winner;
/// `find` silently keeps the other one). A duplicated key whose earlier
/// copy reads `accept` would make a diverging probe score as agreeing,
/// and [`a_wire_divergence_the_parse_axis_cannot_see_names_its_owning_issue`]
/// would then stop requiring an owner for it — the exact blindness these
/// gates exist to remove. So:
///
/// * **Uniqueness** — a repeated `query` fails, naming it.
/// * **Reverse membership** — every baseline entry names a matrix probe,
///   so an entry that scores nothing cannot sit there unnoticed. The
///   forward half (every probe has an entry) is [`diverges_on_wire`]'s
///   panic.
/// * **The disposition domain** — `accept` | `reject` and nothing else.
///   The scoring comparison is `disposition != probe.reference`, a
///   string equality, so `""` or `"accept "` would not fail as
///   malformed: it would score as a divergence, and on a probe that
///   really agrees that is a wrong verdict arriving as a plausible one
///   (review round). Bad data has to fail as bad data.
///
/// Cross-crate, `accept_surface_wire.rs`'s
/// `every_committed_wire_verdict_is_reproduced_by_the_planner` already
/// asserts the stronger POSITIONAL bijection (equal lengths, equal
/// queries index by index, which a duplicate breaks on length). This is
/// deliberately not a substitute: it is a different crate's suite, and
/// this file's gates must not depend on it to know that their own join
/// is sound.
///
/// The matrix side of the join is held to the same two properties by
/// [`matrix`] itself, at load.
fn wire_dispositions(m: &Matrix) -> BTreeMap<String, String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("accept_surface")
        .join("wire_baseline.json");
    let raw = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let wire: WireBaseline = serde_json::from_str(&raw).expect("wire_baseline.json must parse");

    let mut map: BTreeMap<String, String> = BTreeMap::new();
    let mut duplicated = Vec::new();
    let mut unmatched = Vec::new();
    let mut malformed = Vec::new();
    for w in &wire.wire_baseline {
        if map.insert(w.query.clone(), w.pulsus_wire.clone()).is_some() {
            duplicated.push(w.query.clone());
        }
        if !m.accept_surface_probes.iter().any(|p| p.query == w.query) {
            unmatched.push(w.query.clone());
        }
        if !matches!(w.pulsus_wire.as_str(), "accept" | "reject") {
            malformed.push(format!("{:?} -> pulsus_wire {:?}", w.query, w.pulsus_wire));
        }
    }
    assert!(
        malformed.is_empty(),
        "{} wire baseline entry(ies) carry a disposition outside {{accept, reject}} — the score \
         is a string equality against the reference column, so a malformed value reads as a \
         divergence instead of failing as bad data:\n{}",
        malformed.len(),
        malformed.join("\n")
    );
    assert!(
        duplicated.is_empty(),
        "{} duplicate probe key(s) in wire_baseline.json — a probe's query IS its identity on \
         this axis, and a duplicate decides silently which row a join sees:\n{}",
        duplicated.len(),
        duplicated.join("\n")
    );
    assert!(
        unmatched.is_empty(),
        "{} wire baseline entry(ies) name no probe in matrix.json — an entry that scores nothing \
         is not a baseline for this audit:\n{}",
        unmatched.len(),
        unmatched.join("\n")
    );
    map
}

/// Whether a probe's committed wire disposition disagrees with the
/// reference verdict it was captured against. Panics rather than
/// defaulting when the wire side is missing: an unjoinable probe is an
/// unscored probe, which is the failure mode both wire gates exist to
/// deny. This is the forward half of the join's totality; the other two
/// halves are [`wire_dispositions`]'s.
fn diverges_on_wire(wire: &BTreeMap<String, String>, probe: &Probe) -> bool {
    wire.get(&probe.query)
        .map(|disposition| *disposition != probe.reference)
        .unwrap_or_else(|| panic!("{:?} has no wire baseline entry", probe.query))
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

/// The wire axis's half of the class-status teeth (issue #335 Stage C,
/// plan v3 AC 2: *no class may be `wire_status: "closed"` while any
/// probe diverges on that axis*).
///
/// **Why it is needed at all.** Stage C closes D7 on the parse axis and
/// leaves it open on the wire, so `matrix.json` gained a `wire_status`
/// field — and a status field with no assertion behind it is the exact
/// shape this issue has paid for repeatedly. This is the assertion.
///
/// **What it can and cannot see.** It joins against the COMMITTED
/// `pulsus_wire` column in `wire_baseline.json`, because
/// `parse → validate → plan` is unreachable from this crate (the cargo
/// edge runs the other way). That column is itself re-derived from the
/// tree under test, per probe, by
/// `pulsus-read/tests/accept_surface_wire.rs` — so the two together
/// bind the status to the planner's real behaviour, and neither alone
/// does. Stated rather than assumed: if that suite is deleted, this one
/// degrades to checking a file against a file.
///
/// **The bound on its rule, because it was once read wider than it is
/// (issue #335 follow-up).** This quantifies over CLASSES that declare a
/// `wire_status`, and reaches a probe only through `class` /
/// `closed_class`. A probe that diverges on the wire while AGREEING on
/// the parse axis has neither field by construction — an agreement may
/// not carry `class` — so it is invisible here however many of them
/// there are. Ten such probes were passing this gate while naming
/// nobody. That half is
/// [`a_wire_divergence_the_parse_axis_cannot_see_names_its_owning_issue`];
/// this one says nothing about it.
#[test]
fn a_class_open_on_the_wire_has_a_probe_still_diverging_there() {
    let m = matrix();
    let wire = wire_dispositions(&m);

    let query_diverges_on_wire = |query: &str| -> bool {
        let probe = m
            .accept_surface_probes
            .iter()
            .find(|p| p.query == query)
            .unwrap_or_else(|| panic!("{query:?} is in the wire baseline but not the matrix"));
        diverges_on_wire(&wire, probe)
    };

    for c in &m.divergence_classes {
        let Some(wire_status) = &c.wire_status else {
            continue;
        };
        assert!(
            c.wire_note.as_ref().is_some_and(|n| !n.trim().is_empty()),
            "class {}: wire_status needs its wire_note",
            c.id
        );
        // A probe belongs to the class through `class` (still diverging
        // on the parse axis) or `closed_class` (closed there) — the
        // wire question is asked of both.
        let members: Vec<&str> = m
            .accept_surface_probes
            .iter()
            .filter(|p| {
                p.class.as_deref() == Some(c.id.as_str())
                    || p.closed_class.as_deref() == Some(c.id.as_str())
            })
            .map(|p| p.query.as_str())
            .collect();
        let diverging = members
            .iter()
            .filter(|q| query_diverges_on_wire(q))
            .collect::<Vec<_>>();
        match wire_status.as_str() {
            "open" => assert!(
                !diverging.is_empty(),
                "class {} is recorded wire-open but every one of its {} probes now agrees on \
                 the wire — close it, and say so in the re-pin",
                c.id,
                members.len()
            ),
            "closed" => assert!(
                diverging.is_empty(),
                "class {} is recorded wire-closed but {} probe(s) still diverge there: {:?}",
                c.id,
                diverging.len(),
                diverging
            ),
            other => panic!("class {}: bad wire_status {other:?}", c.id),
        }
    }
}

/// The other half of the wire-axis teeth (issue #335 follow-up): **a
/// probe that diverges on the wire while agreeing on the parse axis must
/// name the issue that owns the gap.**
///
/// **Why this is a separate rule from the class statuses.** The parse
/// axis owns its divergences by construction: a diverging probe must
/// carry a `class`, every class is declared in this matrix, and the
/// matrix's own `owning_issue` is the audit issue those classes belong
/// to. A probe that only diverges on the WIRE has no class at all — an
/// agreement may not carry one — and the gap is not the audit's: it is
/// whichever planner refuses the query. So the pointer has to be on the
/// probe, and nothing but this test requires it.
///
/// **The absence it was written to see.** Ten probes agreed on parse and
/// were planner 400s against a reference 2xx while
/// [`a_class_open_on_the_wire_has_a_probe_still_diverging_there`] stayed
/// green — three from #335 Stage C's aggregate-argument grammar
/// (`avg(span:childCount)`, `avg(trace:duration)`, `avg(.a + 1)`), seven
/// from #182's deferred `by()`/`_over_time()` follow-ups. The seven were
/// owned in prose only; the three were owned by nobody. A registry that
/// cannot see an absence has nothing to report, so a gap stays quiet
/// until somebody happens to look.
///
/// **Both directions, so the field cannot rot.** An owner is REQUIRED
/// while the probe diverges on the wire and FORBIDDEN once it agrees —
/// closing a gap therefore deletes its pointer in the same change that
/// re-pins the baseline, exactly as `closed_by` works on the parse axis.
#[test]
fn a_wire_divergence_the_parse_axis_cannot_see_names_its_owning_issue() {
    let m = matrix();
    let wire = wire_dispositions(&m);

    let mut unowned = Vec::new();
    let mut stale = Vec::new();
    for p in &m.accept_surface_probes {
        if let Some(issue) = p.owning_issue {
            assert!(
                issue > 0,
                "{:?}: owning_issue must be an issue number",
                p.query
            );
        }
        let diverges = diverges_on_wire(&wire, p);
        match (diverges, p.verdict.as_str(), p.owning_issue) {
            (true, "agree", None) => unowned.push(p.query.clone()),
            (false, _, Some(issue)) => stale.push(format!("{:?} -> #{issue}", p.query)),
            _ => {}
        }
    }

    assert!(
        unowned.is_empty(),
        "{} probe(s) are refused on the wire while agreeing on the parse axis and name no \
         owning_issue — a planner gap tracked by nothing. Add `owning_issue` naming the issue \
         that owns the refusal (the planner's own 400 usually names it), or close the gap:\n{}",
        unowned.len(),
        unowned.join("\n")
    );
    assert!(
        stale.is_empty(),
        "{} probe(s) carry an owning_issue but now AGREE on the wire — the gap is closed, so \
         the pointer goes with it in the same change that re-pins the baseline:\n{}",
        stale.len(),
        stale.join("\n")
    );
}

/// **The oracle column is frozen independently of the file that holds
/// it.** Every other gate here compares our verdicts against
/// `matrix.json`'s `reference` column, so all of them pass if that column
/// is edited to match us — a re-pin marking its own homework, and nothing
/// else in the apparatus would notice.
///
/// The digest covers `(query, reference)` for all 221 probes, in order.
/// It lives in this SOURCE file rather than beside the data, so a
/// data-only edit fails; moving it is a deliberate, reviewable act that
/// means one thing: **the reference container was re-measured.** Never
/// update it to make a run go green.
///
/// **What the digest function actually is: an FNV-1a-SHAPED rolling
/// multiplicative digest, and NOT FNV-1a.** It is written inline rather
/// than pulled from a hash crate — no new dependency for a
/// change-detector, and the value is regenerated from the assertion
/// message — but the multiplier below is `0x1000_0000_01b3`, one hex
/// digit longer than the FNV-1a 64-bit prime `0x100000001b3`. The offset
/// basis and the xor-then-multiply order are FNV-1a's; the prime is not.
///
/// **Deliberately not corrected** (review round, #335): this is a change
/// DETECTOR, and any odd multiplier over `u64` is one — no verdict,
/// count or comparison anywhere in this suite depends on the value being
/// a particular hash. `REFERENCE_DIGEST` is pinned to THIS function, and
/// a constant whose entire purpose is not to move casually should not be
/// moved to make a name accurate. So the label is corrected instead of
/// the arithmetic.
///
/// Do not "fix" the multiplier: it would move `REFERENCE_DIGEST` for a
/// cosmetic reason, which is exactly the move this test exists to make
/// suspicious. If it is ever changed, that is a re-pin like any other
/// and must be reviewed as one.
///
/// (Found by recomputing the digest independently while building a
/// mutant: the low 32 bits matched and the high bits did not, which is
/// what an over-long multiplier looks like.)
///
/// **Where else this multiplier appears, swept rather than assumed.**
/// `git grep -nIE '[Ff][Nn][Vv]|01b3|01B3|0100_0193|cbf2_9ce4|cbf29ce4|811c_9dc5|811c9dc5'`
/// — the NAME plus both offset bases and both prime tails in every
/// `_`-separated spelling, so a site that uses the constants without
/// saying "FNV" is still found. Five implementations. The other
/// change-detector, `pulsus-read/tests/golden_sql_freeze.rs`, is
/// deliberately this same function and says so. The three that feed an
/// IDENTITY all use the real primes and must:
/// `logql/labels.rs::fnv1a64` (label-set fingerprint),
/// `logql/cms.rs` (the byte-exact `hashn` sketch port), and
/// `promql/eval/quote.rs` (a checksum compared against a Go snippet
/// that multiplies by `0x100000001b3`). So the over-long multiplier is
/// confined to the two change-detectors, where it is immaterial.
#[test]
fn the_reference_column_is_frozen_against_silent_re_pinning() {
    const REFERENCE_DIGEST: u64 = 0x95bd_d72a_11e5_6743;
    let m = matrix();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut feed = |b: &[u8]| {
        for byte in b {
            h ^= u64::from(*byte);
            // NOT the FNV-1a prime (`0x100000001b3`) — one hex digit
            // longer, and left that way on purpose. See the doc comment:
            // changing it moves REFERENCE_DIGEST for a cosmetic reason.
            h = h.wrapping_mul(0x1000_0000_01b3);
        }
    };
    for p in &m.accept_surface_probes {
        feed(p.query.as_bytes());
        feed(&[0x01]);
        feed(p.reference.as_bytes());
        feed(&[0x00]);
    }
    assert_eq!(
        h, REFERENCE_DIGEST,
        "the reference (ORACLE) column changed. This is not a count to \
         refresh: it means the probe set or the reference container's \
         answers moved. Re-measure against the pinned digest, record what \
         changed and why, and only then update REFERENCE_DIGEST to {h:#x}"
    );
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

/// Every probe on which `parse` alone and `parse ∘ validate` disagree —
/// i.e. every rejection that the grammar collapse moved OUT of the parser
/// — must be attributable to a rule on the parse→validate class list kept
/// in `validate_field_expr`'s doc comment.
///
/// This replaces Stage A's `stage_a_flipped_exactly_the_two_recorded_d1_probes`,
/// which pinned the flip set as exactly two queries by name. That was the
/// right gate while the layered parser made two rejections movable; the
/// Stage B collapse legitimately moves 70, so naming them individually
/// would be a list nobody could check. The class list is the checkable
/// form of the same claim: a flip whose rule is NOT on the list is an
/// unexplained parse-axis move and fails here.
///
/// The list, and what each row catches (counts at the Stage B re-pin):
///
/// | rule | flips |
/// |---|---|
/// | `type-mismatch` (operand types must match, every binary class) | 34 |
/// | `spanset-filter-not-boolean` (a filter body must resolve to a boolean) | 16 |
/// | `illegal-unary-operator` (`!` takes a boolean, `-` a number) | 12 |
/// | `illegal-operator` (operator legal for both operand types) | 7 |
/// | `invalid-regex-operand` (`=~`/`!~` needs a string literal) | 1 |
/// | `aggregate-not-numeric` (rule 11's type half — Stage C) | 1 |
/// | `aggregate-not-span-referencing` (rule 11's span half — Stage C) | 1 |
///
/// Counts are asserted as a TOTAL, not per rule: which rule catches a
/// given query is an implementation detail that may legitimately shift
/// (two rules can both hold and the reference reports one), but the flip
/// set as a whole may not grow a member no rule on the list explains.
#[test]
fn every_parse_axis_flip_is_explained_by_the_class_list() {
    /// The parse→validate class list, by `ValidateError::rule_id`.
    const MOVED_TO_VALIDATE: [&str; 7] = [
        "type-mismatch",
        "illegal-operator",
        "invalid-regex-operand",
        "spanset-filter-not-boolean",
        "illegal-unary-operator",
        // Stage C: `parse_aggregate` no longer screens its argument
        // against an aggregatable-intrinsic allowlist, so `avg(1)` and
        // `avg("x")` are rule-11 rejections instead of parse errors.
        "aggregate-not-numeric",
        "aggregate-not-span-referencing",
    ];
    let m = matrix();
    let mut flips = 0usize;
    let mut unexplained = Vec::new();
    for p in &m.accept_surface_probes {
        let Ok(ast) = pulsus_traceql::parse(&p.query) else {
            continue; // the parser still rejects it: nothing moved
        };
        let Err(err) = pulsus_traceql::validate(&ast) else {
            continue; // accepted by both: nothing moved
        };
        flips += 1;
        if !MOVED_TO_VALIDATE.contains(&err.rule_id()) {
            unexplained.push(format!("{:?} -> {}", p.query, err.rule_id()));
        }
    }
    assert!(
        unexplained.is_empty(),
        "{} parse-axis flip(s) are on no class-list row — a rejection that moved out \
         of the parser without a rule to land on is unexplained, not a count to update:\n{}",
        unexplained.len(),
        unexplained.join("\n")
    );
    assert_eq!(flips, 72, "the parse-axis flip set moved");
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
