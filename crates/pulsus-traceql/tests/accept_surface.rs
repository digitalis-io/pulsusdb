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
    /// How this class's construct is IDENTIFIED, and therefore what may
    /// discharge a reachability claim about it (issue #335 Stage D0).
    ///
    /// * `lexical` — the construct is a TOKEN. An absence sweep can mean
    ///   something, and the probe carries a `subject` drawn from
    ///   [`DivergenceClass::subject_atoms`].
    /// * `positional` — the construct is a PLACE in the grammar. There is
    ///   nothing to sweep, and sweeping something adjacent is exactly the
    ///   defect this field exists to make unrepresentable: `.a = 1` really
    ///   does sweep to zero hits, so a record built that way is honest and
    ///   meaningless. A positional class must discharge r4 with a
    ///   citation.
    ///
    /// Only the pre-Stage-D0 classes may omit it — they predate the field
    /// and carry no reachability records.
    #[serde(default)]
    subject_kind: Option<String>,
    /// The closed set of tokens a `lexical` class's probes may name as
    /// their `subject`, so a probe cannot invent one. Empty for a
    /// `positional` class.
    #[serde(default)]
    subject_atoms: Vec<String>,
    /// What this class commits to: `D1` | `D2` | `held`. Each value has a
    /// mechanical consequence, checked here or named as a convention —
    /// see [`every_divergence_carries_a_class_and_every_class_is_used`].
    /// `held` means MEASURED, OWNED, UNSCHEDULED. It never means "won't
    /// fix": a held class stays `status: "open"`, its probes keep
    /// diverging, and every one of them names its owning issue.
    #[serde(default)]
    stage: Option<String>,
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
    ///
    /// **Widened at Stage D0 to carry the `held` ownership too.** A class
    /// staged `held` is measured and unscheduled, and the plan's rule is
    /// that every one of its probes names the issue that owns it. Those
    /// two populations are disjoint by construction — a wire-only
    /// divergence has no `class`, a held-class member always has one — so
    /// one field carries both without ambiguity, and the "forbidden once
    /// it agrees" half still stops a pointer rotting in place.
    #[serde(default)]
    owning_issue: Option<u32>,
    /// Which client path can reach this probe's construct — one of the
    /// four tiers in [`REACHABILITY_TIERS`] (issue #335 Stage D0).
    /// Required exactly when the probe DIVERGES and forbidden otherwise:
    /// an agreement has no divergence to justify a reachability claim
    /// about, which is the `class` field's posture one column over.
    #[serde(default)]
    reachability: Option<String>,
    /// The token under classification, for a probe in a `lexical` class.
    /// Three things are compared, and each one closes a forgery that was
    /// actually built against an earlier cut of this schema: it must be
    /// one of its class's declared `subject_atoms`; it must occur in this
    /// probe's own query; and an absence sweep's `token` must EQUAL it,
    /// not merely contain it.
    #[serde(default)]
    subject: Option<String>,
    /// How the reachability claim is discharged. Every field in it is
    /// compared against something, or it is not a field — see
    /// [`every_probe_records_its_reachability`].
    #[serde(default)]
    reachability_evidence: Option<ReachabilityEvidence>,
}

/// A reachability record. It carries no `path`, no `lines`, no `request`
/// and no `observed_field`: those belong to a declared anchor or capture
/// in `reachability.json` and are named by ID, so there is one place to
/// review them rather than one per probe.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReachabilityEvidence {
    /// `capture` (r1) | `insertion` (r2) | `highlight-only` (r3) |
    /// `absence-sweep` (r4, lexical) | `citation` (r4, positional).
    kind: String,
    /// r1: the capture id.
    #[serde(default)]
    capture: Option<String>,
    /// r1: the value observed in the capture's `observed_field`. Must
    /// EQUAL the probe's `subject` — the captured value IS the construct.
    #[serde(default)]
    observed_value: Option<String>,
    /// r2 / r4-positional: the anchor id.
    #[serde(default)]
    anchor: Option<String>,
    /// r3: two DISTINCT anchor ids — the token list and the omission.
    /// "Highlighted but not offered" is a two-part claim and may not be
    /// made with one citation.
    #[serde(default)]
    anchors: Vec<String>,
    /// r4-lexical: the sweep, every field of it compared.
    #[serde(default)]
    tool: Option<String>,
    #[serde(default)]
    flags: Vec<String>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    revision: Option<String>,
    #[serde(default)]
    scope: Vec<String>,
    #[serde(default)]
    hits: Option<usize>,
}

/// The declared citation targets — nine anchors and one capture, read
/// once and joined by id. See `reachability.json`'s `what_this_is`.
#[derive(Deserialize)]
struct Reachability {
    datasource_revision: String,
    schema_header: String,
    declared_paths: Vec<String>,
    declared_scopes: BTreeMap<String, String>,
    anchors: BTreeMap<String, Anchor>,
    captures: BTreeMap<String, Capture>,
}

#[derive(Deserialize)]
struct Anchor {
    revision: String,
    path: String,
    lines: String,
    shows: String,
    #[serde(default)]
    insert_text: Option<String>,
}

#[derive(Deserialize)]
struct Capture {
    endpoint: String,
    request: String,
    observed_field: String,
    #[allow(dead_code)]
    observed_body: String,
    emitters: Vec<String>,
    #[allow(dead_code)]
    why_this_is_r1: String,
}

/// One row per production in the reference grammar's rule section.
#[derive(Deserialize)]
struct GrammarSlots {
    slots: Vec<GrammarSlot>,
}

#[derive(Deserialize)]
struct GrammarSlot {
    production: String,
    ref_lines: String,
    disposition: String,
    #[serde(default)]
    probes: Vec<String>,
    why: String,
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
/// **Stage D0 (#335, 2026-08-12) raised `TOTAL` 221 → 306 and `DIVERGE`
/// 0 → 56. NOTHING REGRESSED.** The first enumeration was built from the
/// reference's precedence table and its *field-expression* operand
/// positions — **24 of the grammar's 33 productions**. Stage D0
/// enumerated all 33 (`accept_surface/grammar_slots.json`, with the
/// command that produced the list). The nine that had never been probed —
/// `root`, `spansetPipelineExpression`, `spansetPipeline`,
/// `coalesceOperation`, `spansetExpression`, `scalarFilterOperation`,
/// `scalarPipelineExpression`, `scalarPipeline`, `metricsFilterOperation`
/// — carry two divergence classes on their own
/// (D19, D24), and the tokens `unspecified`, `minInt`, `maxInt`, `nil`,
/// `topk`, `bottomk`, `compare(` and `with(` appeared in **zero** of the
/// old 221 probes. `DIVERGE` rose because the surface became MEASURED,
/// not because anything decayed. The 85 new probes are 29 agreeing and 56
/// diverging across classes D13–D24; every one was replayed twice against
/// the digest-pinned oracle with identical verdicts and no inconclusive
/// (non-200/400) answer. See `accept_surface/PROVENANCE.md` §Stage D0.
///
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
/// moving agree→diverge. The remaining 23 are D1 (5), D3 (7), D4 (4),
/// D5 (2), D6 (1), D7 (4) — the field-expression regrammar.
///
/// Stage B capture (#335 AC 4, binding ruling): the `!`/absence
/// de-conflation may not begin until the reference behaviour of every
/// spelling our tree conflates onto existence is measured. The
/// mechanism class is "spellings that parse to `Exists`/`Not(Exists)`
/// here" — bare, `!`, `= nil`, `!= nil`, at both scopes — and the
/// result differential found the conflation WIDER than the `!`/nil
/// pair the plan named. Class D12 records all of it; `MEANING` 3 + 6 = 9.
///
/// Stage B (#335), the grammar collapse: one precedence-climbing routine
/// replaces the layered field-expression parser, closing **D1** (5),
/// **D3** (7), **D4** (4), **D5** (2) and **D6** (1) — 19 probes
/// diverge→agree and none the other way, so `AGREE` 198 + 19 = 217 and
/// `DIVERGE` 23 − 19 = 4. The 4 residuals are all **D7**.
///
/// Stage C (#335), the aggregate argument: `parse_aggregate` takes a
/// full field expression and `validate` gained rule 11, so **D7**'s 4
/// probes flip diverge→agree — `AGREE` 217 + 4 = 221 and `DIVERGE` 4 − 0
/// = 0.
///
/// **`DIVERGE` is a statement about the PARSE axis and nothing more.** D7
/// is not closed for a user: on the wire
/// (`parse → validate → plan`, `pulsus-read/tests/accept_surface_wire.rs`)
/// three of its four probes are still planner 400s against a reference
/// 2xx. The class row carries `wire_status: "open"` saying so. Read these
/// constants as "the grammar agrees", never as "these queries work".
///
/// Issue #460 closes **D17** — `compare()`'s two other reference
/// productions (`expr.y:325-326`). Its two probes flip diverge→agree, so
/// `AGREE` 266 + 2 = 268 and `DIVERGE` 40 − 2 = 38; `TOTAL` is unchanged
/// at 306 and no probe moved the other way. Unlike D7, D17 closes on the
/// WIRE too — both probes plan (`wire_baseline.json`, `pulsus_wire`
/// reject→accept) — and the engine honours `topN` and the
/// `(start, end]` selection window rather than merely parsing them, so
/// this closure is not a scoreboard move.
const TOTAL: usize = 306;
const AGREE: usize = 268;
const DIVERGE: usize = 38;
const MEANING: usize = 6;
const CLOSED_MEANING: usize = 7;

/// The four reachability tiers, closed. Each says what the Grafana
/// TraceQL client DOES with the construct, not how likely a divergence
/// feels — a warrant wider than the command behind it is what round 3 of
/// this issue shipped and round 4 caught.
///
/// * `r1-client-emitted` — the datasource writes the exact query text.
/// * `r2-skeleton-inserted` — a completion puts the construct on screen
///   with an empty slot; the divergent ARGUMENT form is typed by hand.
/// * `r3-highlighted-only` — the token is marked wherever it appears; no
///   completion path reaches the form.
/// * `r4-no-traceql-surface-path` — nothing on the TraceQL surface
///   emits, inserts or highlights it.
const REACHABILITY_TIERS: [&str; 4] = [
    "r1-client-emitted",
    "r2-skeleton-inserted",
    "r3-highlighted-only",
    "r4-no-traceql-surface-path",
];

/// Every production in the reference grammar's rule section, in file
/// order. The LENGTH is in the type, so deleting a row from
/// `grammar_slots.json` does not compile — the `MOVED_TO_VALIDATE`
/// posture this file already uses, one artefact over.
const GRAMMAR_SLOTS: [&str; 33] = [
    "root",
    "spansetPipelineExpression",
    "wrappedSpansetPipeline",
    "spansetPipeline",
    "groupOperation",
    "coalesceOperation",
    "selectOperation",
    "attribute",
    "attributeList",
    "numericList",
    "spansetExpression",
    "spansetFilter",
    "scalarFilter",
    "scalarFilterOperation",
    "scalarPipelineExpressionFilter",
    "scalarPipelineExpression",
    "wrappedScalarPipeline",
    "scalarPipeline",
    "scalarExpression",
    "aggregate",
    "metricsAggregation",
    "metricsSecondStage",
    "metricsFilterOperation",
    "metricsFilter",
    "metricsSecondStagePipeline",
    "hint",
    "hints",
    "hintList",
    "fieldExpression",
    "static",
    "intrinsicField",
    "scopedIntrinsicField",
    "attributeField",
];

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

fn fixture<T: serde::de::DeserializeOwned>(name: &str) -> T {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("accept_surface")
        .join(name);
    let raw = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("{name} must parse: {e}"))
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

        // Stage D0's three class fields. They are optional in the schema
        // only so the pre-Stage-D0 classes, which predate them and carry
        // no reachability records, keep parsing; a class that declares
        // one must declare all three coherently.
        let Some(stage) = c.stage.as_deref() else {
            assert!(
                c.subject_kind.is_none() && c.subject_atoms.is_empty(),
                "class {}: subject_kind/subject_atoms without a stage",
                c.id
            );
            continue;
        };
        assert!(
            matches!(stage, "D1" | "D2" | "held"),
            "class {}: bad stage {stage:?} (D1 | D2 | held)",
            c.id
        );
        let subject_kind = c
            .subject_kind
            .as_deref()
            .unwrap_or_else(|| panic!("class {}: a staged class needs a subject_kind", c.id));
        match subject_kind {
            "lexical" => assert!(
                !c.subject_atoms.is_empty(),
                "class {}: a lexical class must declare the atoms its probes may name",
                c.id
            ),
            "positional" => assert!(
                c.subject_atoms.is_empty(),
                "class {}: a positional class has no token to name — its construct is a PLACE in \
                 the grammar, and sweeping something adjacent is the defect this split exists to \
                 make unrepresentable",
                c.id
            ),
            other => panic!("class {}: bad subject_kind {other:?}", c.id),
        }
        // `held` means MEASURED, OWNED, UNSCHEDULED — never "won't fix".
        // The enforceable half: the class stays open (which the teeth
        // above then back with a still-diverging probe), and every one of
        // its probes names the issue that owns it. The unenforceable half
        // is a stated CONVENTION and is labelled one: no artefact carries
        // "a held class may not be described in prose as closed" in a
        // form a test can read, so it is enforced at review.
        if stage == "held" {
            assert_eq!(
                c.status, "open",
                "class {} is staged `held` — measured, owned and unscheduled — but recorded \
                 {:?}. A held class that is really closed is mis-staged; a held class nobody \
                 will act on is a ledgered divergence, not a row left to age",
                c.id, c.status
            );
            let unowned: Vec<&str> = m
                .accept_surface_probes
                .iter()
                .filter(|p| p.class.as_deref() == Some(c.id.as_str()))
                .filter(|p| p.owning_issue.is_none())
                .map(|p| p.query.as_str())
                .collect();
            assert!(
                unowned.is_empty(),
                "class {} is staged `held` but {} of its probes name no owning_issue — an \
                 unscheduled gap tracked by nothing is the shape `held` exists to deny: {:?}",
                c.id,
                unowned.len(),
                unowned
            );
        }
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

    // Stage D0 widened the field: a probe of a `held` class also names
    // its owner (the plan's "measured, owned, unscheduled"), and those
    // probes are not wire-only divergences. The two populations are
    // disjoint — a wire-only divergence has no `class` by construction —
    // so the "forbidden once it agrees" half is narrowed by exactly that
    // set and by nothing else.
    let held_classes: BTreeSet<&str> = m
        .divergence_classes
        .iter()
        .filter(|c| c.stage.as_deref() == Some("held"))
        .map(|c| c.id.as_str())
        .collect();
    let is_held_member = |p: &Probe| p.class.as_deref().is_some_and(|c| held_classes.contains(c));

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
            (false, _, Some(issue)) if !is_held_member(p) => {
                stale.push(format!("{:?} -> #{issue}", p.query))
            }
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
/// it.**
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
/// **Where else this multiplier appears: run the sweep. There is no
/// list here on purpose.**
///
/// ```text
/// git grep -nIE '[Ff][Nn][Vv]|01b3|01B3|0100_0193|cbf2_9ce4|cbf29ce4|811c_9dc5|811c9dc5'
/// ```
///
/// Read its
/// output; do not trust a summary of it, including a past one. Three
/// hand-written enumerations of that output were attempted on #335 and
/// each was false in a new way — a count, then a classification, then
/// the argument bounding what the pattern could not see. A list in a
/// comment is a snapshot nothing re-checks; the command is the answer.
#[test]
fn the_reference_column_is_frozen_against_silent_re_pinning() {
    const REFERENCE_DIGEST: u64 = 0xd3fa_a287_0705_cfe4;
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
    // Every divergence this audit has ever recorded is either closed or
    // still open — nothing may leave the accounting. 45 came from the
    // original capture and 56 more from Stage D0's grammar-slot
    // enumeration, so the invariant is 101 and each fix moves a probe
    // from the right-hand term to the left, never off the ledger.
    assert_eq!(
        closed + DIVERGE,
        101,
        "closed + still-diverging must equal every divergence this audit has recorded: the \
         capture's 45 plus Stage D0's 56"
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
    // 72 through Stage C; Stage D0 added one — `{ .a = 1 } | count() =~ 1`
    // parses and is refused by `illegal-operator`, which is already on the
    // list above. A flip whose rule is NOT on the list still fails here.
    assert_eq!(flips, 73, "the parse-axis flip set moved");
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

// ---------------------------------------------------------------------------
// Stage D0 (#335): the enumeration, its reachability ranking, and the
// two constants that stop either being edited quietly.
// ---------------------------------------------------------------------------

/// The accept-surface audit's SUBJECT is the reference grammar's whole
/// production set. That claim used to be prose, and it was false: the
/// first enumeration covered 24 of 33 productions, and the nine it
/// omitted carried two divergence classes nothing could see.
///
/// This is the checkable half. `GRAMMAR_SLOTS` is a fixed-size array, so
/// deleting a row does not compile; every row must carry a disposition
/// from the closed set, a non-empty reason, and probe queries that
/// actually exist in `matrix.json`.
///
/// **What it does NOT check, stated rather than implied:** that the list
/// is 33 rows and not 34. The reference grammar is AGPL and deliberately
/// not vendored, and CI has no Tempo checkout, so nothing here can
/// compare the manifest against `expr.y`. The list is documentation with
/// a recorded reproduction command in the file's own header — the honest
/// form. A hand-maintained enumeration claiming completeness is exactly
/// what failed on this issue before.
#[test]
fn every_grammar_production_is_enumerated_and_dispositioned() {
    let slots: GrammarSlots = fixture("grammar_slots.json");
    let m = matrix();
    let probes: BTreeSet<&str> = m
        .accept_surface_probes
        .iter()
        .map(|p| p.query.as_str())
        .collect();

    assert_eq!(
        slots.slots.len(),
        GRAMMAR_SLOTS.len(),
        "grammar_slots.json must carry one row per production in the reference grammar's rule \
         section; the count is pinned in the type"
    );
    let named: Vec<&str> = slots.slots.iter().map(|s| s.production.as_str()).collect();
    assert_eq!(
        named,
        GRAMMAR_SLOTS.to_vec(),
        "grammar_slots.json's productions must be exactly GRAMMAR_SLOTS, in grammar file order"
    );

    let mut faults = Vec::new();
    for s in &slots.slots {
        if !matches!(
            s.disposition.as_str(),
            "probed" | "covered-by" | "no-operand-slot"
        ) {
            faults.push(format!(
                "{}: bad disposition {:?}",
                s.production, s.disposition
            ));
        }
        if s.why.trim().is_empty() {
            faults.push(format!("{}: empty `why`", s.production));
        }
        if !s.ref_lines.starts_with("expr.y:") {
            faults.push(format!(
                "{}: ref_lines {:?} must cite the grammar file and its line range",
                s.production, s.ref_lines
            ));
        }
        if s.disposition == "probed" && s.probes.is_empty() {
            faults.push(format!(
                "{}: dispositioned `probed` with no probe — say `covered-by` and name what covers \
                 it, or probe it",
                s.production
            ));
        }
        for q in &s.probes {
            if !probes.contains(q.as_str()) {
                faults.push(format!(
                    "{}: cites probe {q:?}, which is in no matrix row — a manifest that names a \
                     query nobody measured is the enumeration failing one layer down",
                    s.production
                ));
            }
        }
    }
    assert!(
        faults.is_empty(),
        "{} grammar-slot row fault(s):\n{}",
        faults.len(),
        faults.join("\n")
    );
}

/// The 85 Stage D0 probes must be present by exact string, mirroring
/// [`stage_b_not_absence_meaning_probes_are_captured`] one stage over:
/// the enumeration's value is the SET, and a set is only committed if
/// editing a member fails.
///
/// One probe per divergence class is named here plus the two boundary
/// agreements that bound D19 and D24 — the negative results that stop a
/// later fix over-widening. *RED when:* any named probe string is edited
/// or dropped.
#[test]
fn stage_d0_probes_are_captured() {
    let m = matrix();
    let by_query: BTreeMap<&str, &Probe> = m
        .accept_surface_probes
        .iter()
        .map(|p| (p.query.as_str(), p))
        .collect();

    for (q, class) in [
        ("{ kind = unspecified }", Some("D13")),
        ("{ .a = minInt }", Some("D14")),
        ("{ nil = nil }", Some("D15")),
        ("{ .a = 1 } | by(.b, .c)", Some("D16")),
        ("{ .a = 1 } | compare({ .b = 2 }, 10)", Some("D17")),
        ("{ .a = 1 } | rate() > -1", Some("D18")),
        ("({ .a = 1 } | count() > 1)", Some("D19")),
        ("{ .a = 1 } | (count()) > 4", Some("D20")),
        ("{ .a = 1 } | rate() | topk(5) > 1", Some("D21")),
        ("{ .a = 1 } | topk(10)", Some("D22")),
        ("{ .a = 1 } with(a=1) with(b=2)", Some("D23")),
        ("by(.a)", Some("D24")),
        // The two boundaries. Both sides REJECT these, and that is the
        // point: they pin where D19's and D24's fixes must stop.
        ("({ .a = 1 } | count() > 1) && { .b = 2 }", None),
        ("by(.a) && { .b = 1 }", None),
    ] {
        let p = by_query
            .get(q)
            .unwrap_or_else(|| panic!("Stage D0: {q:?} must be in the probe matrix"));
        match class {
            Some(c) => assert!(
                p.class.as_deref() == Some(c) || p.closed_class.as_deref() == Some(c),
                "{q:?}: expected class {c}, got class={:?} closed_class={:?}",
                p.class,
                p.closed_class
            ),
            None => assert_eq!(
                p.verdict, "agree",
                "{q:?} is a class BOUNDARY and must agree — both sides reject it"
            ),
        }
    }
}

/// Every diverging probe records WHICH CLIENT PATH reaches its
/// construct, and the record's every field is compared against something
/// — or it is not a field.
///
/// **Why the form is this strict.** Two earlier cuts of this schema were
/// forged in review. A free-text evidence string took no scope and proved
/// no execution; a `token` merely *occurring in* the probe's query let
/// `.a = 1` discharge an absence claim, which is honest and meaningless.
/// So: the tier comes from a closed set; the subject comes from its
/// class's declared atoms and must occur in the query; an absence sweep's
/// token must EQUAL the subject, its scope must be a declared pathspec,
/// its flags must include `-i` (a case-sensitive sweep is what produced a
/// false zero here), its revision must be the pinned one, and its hits
/// must be zero. A POSITIONAL class may not sweep at all: its construct
/// is a place in the grammar, and there is nothing to sweep.
///
/// **Residual — what this cannot see**, reproduced from
/// `reachability.json`'s `schema_header` so a reader of the test meets it
/// too: nothing here executes or reads anything outside this repository,
/// so a recorded sweep that was never run — or was run against a
/// different tree than the one it names — passes, and so does a citation
/// whose line range nobody read. CI has no Tempo checkout and no
/// Grafana-datasource checkout; the anchors and commands are reproducible
/// by hand at the named revision, and that reproduction, by a human at
/// review time, is the whole of the guarantee. **This instrument stops
/// here on purpose.**
#[test]
fn every_probe_records_its_reachability() {
    let m = matrix();
    let r: Reachability = fixture("reachability.json");
    let classes: BTreeMap<&str, &DivergenceClass> = m
        .divergence_classes
        .iter()
        .map(|c| (c.id.as_str(), c))
        .collect();
    let declared_paths: BTreeSet<&str> = r.declared_paths.iter().map(String::as_str).collect();

    let mut faults = Vec::new();
    let mut fault = |q: &str, msg: String| faults.push(format!("{q:?}: {msg}"));

    for p in &m.accept_surface_probes {
        let diverges = p.verdict == "diverge";
        // Present iff diverging — the `class` posture. An agreement has
        // no divergence to justify a reachability claim about.
        if !diverges {
            if p.reachability.is_some() || p.reachability_evidence.is_some() || p.subject.is_some()
            {
                fault(
                    &p.query,
                    "agrees, so it may not carry a reachability record".to_string(),
                );
            }
            continue;
        }
        let Some(class) = p.class.as_deref().and_then(|c| classes.get(c)) else {
            continue; // an undeclared class is the other test's failure
        };
        // Pre-Stage-D0 classes carry no reachability records.
        if class.stage.is_none() {
            continue;
        }
        let Some(tier) = p.reachability.as_deref() else {
            fault(&p.query, "diverges but records no reachability tier".into());
            continue;
        };
        if !REACHABILITY_TIERS.contains(&tier) {
            fault(&p.query, format!("tier {tier:?} is outside the closed set"));
            continue;
        }
        let lexical = class.subject_kind.as_deref() == Some("lexical");
        match (&p.subject, lexical) {
            (Some(s), true) => {
                if !class.subject_atoms.iter().any(|a| a == s) {
                    fault(
                        &p.query,
                        format!(
                            "subject {s:?} is not a declared atom of {} {:?}",
                            class.id, class.subject_atoms
                        ),
                    );
                }
                if !p.query.contains(s.as_str()) {
                    fault(
                        &p.query,
                        format!("subject {s:?} does not occur in the probe it is the subject of"),
                    );
                }
            }
            (None, true) => fault(
                &p.query,
                "a lexical class's probe must name its subject".into(),
            ),
            (Some(s), false) => fault(
                &p.query,
                format!(
                    "{} is positional, so probe subject {s:?} is meaningless",
                    class.id
                ),
            ),
            (None, false) => {}
        }
        let Some(ev) = &p.reachability_evidence else {
            fault(&p.query, "records a tier with no evidence".into());
            continue;
        };
        let want_kind = match tier {
            "r1-client-emitted" => "capture",
            "r2-skeleton-inserted" => "insertion",
            "r3-highlighted-only" => "highlight-only",
            _ if lexical => "absence-sweep",
            _ => "citation",
        };
        if ev.kind != want_kind {
            fault(
                &p.query,
                format!(
                    "tier {tier} must be discharged by {want_kind:?}, got {:?}",
                    ev.kind
                ),
            );
            continue;
        }
        match ev.kind.as_str() {
            "capture" => {
                let Some(id) = ev.capture.as_deref() else {
                    fault(&p.query, "r1 must name a capture".into());
                    continue;
                };
                let Some(cap) = r.captures.get(id) else {
                    fault(&p.query, format!("capture {id:?} is not declared"));
                    continue;
                };
                if ev.observed_value.as_deref() != p.subject.as_deref() {
                    fault(
                        &p.query,
                        format!(
                            "observed_value {:?} != subject {:?} — the captured value IS the \
                             construct",
                            ev.observed_value, p.subject
                        ),
                    );
                }
                for e in &cap.emitters {
                    if !r.anchors.contains_key(e) {
                        fault(
                            &p.query,
                            format!("capture emitter {e:?} is not a declared anchor"),
                        );
                    }
                }
            }
            "insertion" => {
                let Some(id) = ev.anchor.as_deref() else {
                    fault(&p.query, "r2 must name an anchor".into());
                    continue;
                };
                let Some(a) = r.anchors.get(id) else {
                    fault(&p.query, format!("anchor {id:?} is not declared"));
                    continue;
                };
                let Some(text) = a.insert_text.as_deref() else {
                    fault(
                        &p.query,
                        format!(
                            "anchor {id:?} declares no insert_text, so it cannot discharge r2 — \
                             r2 means the SKELETON is inserted"
                        ),
                    );
                    continue;
                };
                // r2 means the completion puts the construct on screen
                // with an EMPTY slot and the argument is hand-typed. An
                // inserted text carrying a comma or an arithmetic
                // operator would be claiming the client emits the
                // divergent form itself, which is r1.
                if text.contains(',') || text.contains(['+', '-', '*', '/', '%', '^', '!', '=']) {
                    fault(
                        &p.query,
                        format!(
                            "anchor {id:?} inserts {text:?}, which carries an argument — that is \
                             r1's claim, not r2's"
                        ),
                    );
                }
            }
            "highlight-only" => {
                if ev.anchors.len() != 2 || ev.anchors[0] == ev.anchors[1] {
                    fault(
                        &p.query,
                        format!(
                            "r3 is a two-part claim — the token list AND the omission — so it \
                             needs two DISTINCT anchors, got {:?}",
                            ev.anchors
                        ),
                    );
                }
                for id in &ev.anchors {
                    if !r.anchors.contains_key(id) {
                        fault(&p.query, format!("anchor {id:?} is not declared"));
                    }
                }
            }
            "absence-sweep" => {
                if ev.token.as_deref() != p.subject.as_deref() {
                    fault(
                        &p.query,
                        format!(
                            "sweep token {:?} != subject {:?} — a sweep must be OF the construct \
                             under classification",
                            ev.token, p.subject
                        ),
                    );
                }
                if ev.revision.as_deref() != Some(r.datasource_revision.as_str()) {
                    fault(
                        &p.query,
                        format!(
                            "sweep revision {:?} is not the pinned {:?}",
                            ev.revision, r.datasource_revision
                        ),
                    );
                }
                if !ev.flags.iter().any(|f| f == "-i") {
                    fault(
                        &p.query,
                        "a sweep must be case-insensitive (-i); a case-sensitive sweep is what \
                         produced a false zero on this issue"
                            .into(),
                    );
                }
                if ev.scope.is_empty() {
                    fault(&p.query, "a sweep must declare its scope".into());
                }
                for s in &ev.scope {
                    if !r.declared_scopes.contains_key(s) {
                        fault(
                            &p.query,
                            format!("sweep scope {s:?} is not a declared pathspec"),
                        );
                    }
                }
                if ev.hits != Some(0) {
                    fault(
                        &p.query,
                        format!("claims no client path but records {:?} hits", ev.hits),
                    );
                }
                if ev.tool.as_deref() != Some("git grep") {
                    fault(&p.query, format!("unexpected sweep tool {:?}", ev.tool));
                }
            }
            "citation" => {
                let Some(id) = ev.anchor.as_deref() else {
                    fault(&p.query, "a citation must name an anchor".into());
                    continue;
                };
                if !r.anchors.contains_key(id) {
                    fault(&p.query, format!("anchor {id:?} is not declared"));
                }
            }
            other => fault(&p.query, format!("unknown evidence kind {other:?}")),
        }
    }

    // The declared tables themselves: every anchor's revision is the
    // pinned one and its path is on the declared list, so an anchor
    // cannot quietly cite somewhere else.
    for (id, a) in &r.anchors {
        assert_eq!(
            a.revision, r.datasource_revision,
            "anchor {id}: revision {:?} is not the pinned {:?}",
            a.revision, r.datasource_revision
        );
        assert!(
            declared_paths.contains(a.path.as_str()),
            "anchor {id}: path {:?} is not on the declared file list",
            a.path
        );
        assert!(!a.lines.trim().is_empty(), "anchor {id}: empty line range");
        assert!(!a.shows.trim().is_empty(), "anchor {id}: empty `shows`");
    }
    for (id, c) in &r.captures {
        assert!(
            c.endpoint.starts_with('/') && !c.request.trim().is_empty(),
            "capture {id}: an endpoint and the request that produced it are required"
        );
        assert!(
            !c.observed_field.trim().is_empty(),
            "capture {id}: name the field the value was read out of"
        );
    }

    assert!(
        faults.is_empty(),
        "{} reachability record fault(s):\n{}",
        faults.len(),
        faults.join("\n")
    );
}

/// The residual paragraph is part of the artefact, not part of a review
/// conversation. A gate whose boundary is only in a plan comment has no
/// boundary six months later.
#[test]
fn the_reachability_schema_header_records_its_residual() {
    let r: Reachability = fixture("reachability.json");
    for phrase in [
        // The two surviving shapes.
        "never run",
        "different tree",
        "never read",
        // The shapes that are NOT residual, named beside them so a later
        // reader does not assume they were overlooked.
        "not a declared pathspec",
        "not the construct under classification",
        "per-record line range",
        "inserts an argument",
        "r3 with one citation",
        // The stopping rule, in applicable form.
        "stops here on purpose",
    ] {
        assert!(
            r.schema_header.contains(phrase),
            "reachability.json's schema_header no longer records {phrase:?} — the residual is the \
             deliverable, and deleting a line of it makes the gate claim more than it checks"
        );
    }
}

/// The tier totals, DERIVED from the probe rows rather than read back
/// from a committed summary. *RED when:* any probe changes tier in a way
/// that moves a total.
const TIER_HISTOGRAM: [(&str, usize); 4] = [
    // Stage D1 closed D13 (all 5 r1 probes), D14 (4 of r4) and D23 (1 of
    // r3), and a closed probe drops its reachability record — the field is
    // present exactly while the probe diverges. r1 is 0 because the ONE
    // class a client actually emits is the one that got fixed first, which
    // is the ranking doing its job rather than an empty bucket.
    ("r1-client-emitted", 0),
    // Stage D2 closed D16, r2's other six; issue #460 closed D17, r2's
    // last two — a closed probe drops its reachability record, so the
    // tier empties. r2 at 0 is the ranking doing its job: the tiers that
    // say a client can reach the construct are the ones that got fixed.
    ("r2-skeleton-inserted", 0),
    ("r3-highlighted-only", 17),
    ("r4-no-traceql-surface-path", 21),
];

#[test]
fn the_reachability_tier_histogram_is_pinned() {
    let m = matrix();
    let mut counts: BTreeMap<&str, usize> = REACHABILITY_TIERS.iter().map(|t| (*t, 0)).collect();
    for p in &m.accept_surface_probes {
        if let Some(t) = p.reachability.as_deref() {
            *counts.get_mut(t).expect("tier is in the closed set") += 1;
        }
    }
    let want: BTreeMap<&str, usize> = TIER_HISTOGRAM.iter().copied().collect();
    assert_eq!(
        counts, want,
        "the reachability tier totals moved. That is a re-classification, which decides which \
         divergences get fixed first — re-pin TIER_HISTOGRAM deliberately, in the same change"
    );
}

/// **The histogram alone cannot see a swap.** Two probes exchanging
/// tiers preserves every total, and an unexamined row hiding behind an
/// unchanged number is precisely the failure this audit keeps paying
/// for. So the classification carries its own digest.
///
/// **The feed is CONTENT-INCLUSIVE, and that is the load-bearing word.**
/// An anchor's `lines` is fed, so editing `294-299` to `1` moves the
/// digest and forces a source-line re-pin in the same diff. An id-only
/// join would accept that edit silently. **Do not "simplify" this to an
/// id join** — the same shape as the `git log` simplification the B0 gate
/// warns about.
///
/// Encoding, injective by construction: one record per item, tagged with
/// a kind byte; every field as an 8-byte big-endian length followed by
/// its bytes; every LIST field as an 8-byte big-endian element count
/// followed by each element in that same length-prefixed form. No two
/// states produce the same stream, whatever bytes appear inside a query,
/// a path or a `shows` string.
///
/// | kind | per | fields fed, in this order |
/// |---|---|---|
/// | `0x01` | diverging probe, in `matrix.json` order | `query`, `class`, `reachability`, `subject` (empty for a positional class), `discharge_ref` |
/// | `0x02` | divergence class, by id | `id`, `subject_kind`, `subject_atoms`, `stage`, `status` |
/// | `0x03` | anchor, by id | `id`, `revision`, `path`, `lines`, `shows`, `insert_text` (empty when absent) |
/// | `0x04` | capture, by id | `id`, `endpoint`, `request`, `observed_field`, `emitters` |
///
/// `discharge_ref` is the list identifying HOW the probe is discharged:
/// the capture id (r1), the anchor id (r2 and r4-positional), the two
/// anchor ids in order (r3), or the sweep token (r4-lexical).
///
/// The digest function is the same FNV-1a-SHAPED rolling multiplicative
/// digest [`the_reference_column_is_frozen_against_silent_re_pinning`]
/// uses, and the same caveat applies: the multiplier is deliberately not
/// the FNV-1a prime and must not be "fixed".
#[test]
fn the_reachability_classification_is_frozen_against_a_silent_swap() {
    // Issue #460 re-pins this: closing D17 drops its two probes'
    // reachability records from the 0x01 feed and moves its class row's
    // `stage`/`status` in the 0x02 feed. No anchor, capture or other
    // class moved, and no probe changed TIER — the two simply stopped
    // being classified, because a probe carries a reachability record
    // exactly while it diverges.
    const REACHABILITY_DIGEST: u64 = 0xa0f6_b493_ab42_d8d9;
    let m = matrix();
    let r: Reachability = fixture("reachability.json");

    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let byte = |b: u8, h: &mut u64| {
        *h ^= u64::from(b);
        *h = h.wrapping_mul(0x1000_0000_01b3);
    };
    let field = |s: &str, h: &mut u64| {
        for b in (s.len() as u64).to_be_bytes() {
            byte(b, h);
        }
        for b in s.as_bytes() {
            byte(*b, h);
        }
    };
    let list = |items: &[&str], h: &mut u64| {
        for b in (items.len() as u64).to_be_bytes() {
            byte(b, h);
        }
        for it in items {
            field(it, h);
        }
    };

    for p in &m.accept_surface_probes {
        let Some(tier) = p.reachability.as_deref() else {
            continue;
        };
        byte(0x01, &mut h);
        field(&p.query, &mut h);
        field(p.class.as_deref().unwrap_or(""), &mut h);
        field(tier, &mut h);
        field(p.subject.as_deref().unwrap_or(""), &mut h);
        let ev = p
            .reachability_evidence
            .as_ref()
            .expect("a classified probe carries its evidence");
        let refs: Vec<&str> = match ev.kind.as_str() {
            "capture" => vec![ev.capture.as_deref().unwrap_or("")],
            "insertion" | "citation" => vec![ev.anchor.as_deref().unwrap_or("")],
            "highlight-only" => ev.anchors.iter().map(String::as_str).collect(),
            "absence-sweep" => vec![ev.token.as_deref().unwrap_or("")],
            _ => vec![],
        };
        list(&refs, &mut h);
    }

    let mut classes: Vec<&DivergenceClass> = m.divergence_classes.iter().collect();
    classes.sort_by(|a, b| a.id.cmp(&b.id));
    for c in classes {
        byte(0x02, &mut h);
        field(&c.id, &mut h);
        field(c.subject_kind.as_deref().unwrap_or(""), &mut h);
        let atoms: Vec<&str> = c.subject_atoms.iter().map(String::as_str).collect();
        list(&atoms, &mut h);
        field(c.stage.as_deref().unwrap_or(""), &mut h);
        field(&c.status, &mut h);
    }
    for (id, a) in &r.anchors {
        byte(0x03, &mut h);
        field(id, &mut h);
        field(&a.revision, &mut h);
        field(&a.path, &mut h);
        field(&a.lines, &mut h);
        field(&a.shows, &mut h);
        field(a.insert_text.as_deref().unwrap_or(""), &mut h);
    }
    for (id, c) in &r.captures {
        byte(0x04, &mut h);
        field(id, &mut h);
        field(&c.endpoint, &mut h);
        field(&c.request, &mut h);
        field(&c.observed_field, &mut h);
        let emitters: Vec<&str> = c.emitters.iter().map(String::as_str).collect();
        list(&emitters, &mut h);
    }

    assert_eq!(
        h, REACHABILITY_DIGEST,
        "the reachability classification moved. A compensating swap keeps every total, so this \
         is the constant that sees it — and an anchor's line range is fed too, so editing one \
         lands here. Re-pin to {h:#x} only with the reclassification written down"
    );
}

/// PROVENANCE.md's operand-position table used to carry two rows about
/// `by(...)` that were false **in both directions**: they said the
/// reference rejects arithmetic/unary/parenthesised keys and accepts a
/// comma list, and the opposite is true. They described the METRICS
/// `by(...)` — `attributeList` — while claiming to describe the spanset
/// one, and all thirteen backing probes were metrics probes.
///
/// A row citing a query was not enough to catch that; the row's CLAIM has
/// to be compared with the measurement. Each row now carries a trailer
/// the test parses — `<!-- production: X | probe: `Q` -->` — and this
/// asserts the named production is one of the 33, the probe exists, and
/// **the row's "reference accepts?" column equals that probe's recorded
/// `reference` verdict**.
///
/// *RED when:* a row says `yes` and its probe records `reject` — which is
/// exactly the old unqualified `by(...)` row.
#[test]
fn every_operand_position_row_agrees_with_its_probe() {
    let m = matrix();
    let by_query: BTreeMap<&str, &Probe> = m
        .accept_surface_probes
        .iter()
        .map(|p| (p.query.as_str(), p))
        .collect();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("accept_surface")
        .join("PROVENANCE.md");
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

    let mut rows = 0usize;
    let mut faults = Vec::new();
    for line in text.lines() {
        let Some((row, trailer)) = line.split_once("<!-- production:") else {
            continue;
        };
        rows += 1;
        let trailer = trailer.trim_end().trim_end_matches("-->").trim();
        let Some((production, probe)) = trailer.split_once('|') else {
            faults.push(format!("malformed trailer: {trailer:?}"));
            continue;
        };
        let production = production.trim();
        let probe = probe
            .trim()
            .strip_prefix("probe:")
            .unwrap_or(probe)
            .trim()
            .trim_matches('`');
        if !GRAMMAR_SLOTS.contains(&production) {
            faults.push(format!("{production:?} is not one of the 33 productions"));
        }
        let Some(p) = by_query.get(probe) else {
            faults.push(format!("{production}: probe {probe:?} is in no matrix row"));
            continue;
        };
        // The row's own claim: the "reference accepts?" column.
        let claim = if row.contains("| yes ") {
            "accept"
        } else if row.contains("| no ") {
            "reject"
        } else {
            faults.push(format!(
                "{production}: row does not state a `yes`/`no` reference verdict: {row:?}"
            ));
            continue;
        };
        if p.reference != claim {
            faults.push(format!(
                "{production}: the row claims the reference {claim}s {probe:?}, but the measured \
                 verdict is {}",
                p.reference
            ));
        }
    }
    assert!(
        rows >= 4,
        "PROVENANCE.md's operand-position table must carry its `by(...)` rows with production \
         trailers — found {rows}"
    );
    assert!(
        faults.is_empty(),
        "{} operand-position row fault(s):\n{}",
        faults.len(),
        faults.join("\n")
    );
}

/// The Stage D0 section of PROVENANCE.md must exist and name the
/// productions whose omission is the whole finding — deleting one from
/// the section is how the record quietly narrows back to the subset it
/// started as.
#[test]
fn the_provenance_records_the_grammar_slot_enumeration() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("accept_surface")
        .join("PROVENANCE.md");
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let start = text
        .find("## Stage D0")
        .expect("PROVENANCE.md needs its `## Stage D0` section");
    let section = &text[start..];
    for production in [
        "root",
        "spansetPipeline",
        "wrappedSpansetPipeline",
        "spansetPipelineExpression",
        "spansetExpression",
        "coalesceOperation",
        "scalarFilterOperation",
        "scalarPipelineExpression",
        "scalarPipeline",
        "metricsFilterOperation",
        "groupOperation",
        "metricsSecondStagePipeline",
        "metricsFilter",
        "static",
        "hints",
    ] {
        assert!(
            section.contains(production),
            "PROVENANCE.md §Stage D0 no longer names the production {production:?}"
        );
    }
    for blind_spot in [
        "the lexer decides",
        "after a successful parse",
        "Evaluation semantics",
        "Reachability",
    ] {
        assert!(
            section.contains(blind_spot),
            "PROVENANCE.md §Stage D0 must keep stating what the enumeration CANNOT see \
             ({blind_spot:?}) — an enumeration with unstated blind spots is the defect, not the \
             fix"
        );
    }
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
