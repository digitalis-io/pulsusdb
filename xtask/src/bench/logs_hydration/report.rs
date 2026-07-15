//! Evidence schema, markdown rendering, and the materiality verdict
//! predicate for `cargo xtask bench logs-hydration` (issue #35). Mirrors
//! `metrics_labels::report`'s split (issue #34 precedent): `Serialize +
//! Deserialize` evidence structs, `render_markdown`, and a
//! `consistency_tests` submodule that loads the **committed** full-tier
//! JSON and recomputes its verdict from the pinned formulas below —
//! asserting `recorded_verdict == recomputed_verdict`, never requiring a
//! particular verdict class (architect plan v5 [R2]/[R3]: all three
//! verdict classes are internally consistent and committable; only
//! `material`/`not_material` may close the issue, per the separate,
//! non-test close-gate).

use crate::bench::Profile;

/// The three hydration paths this scenario benchmarks (architect plan
/// "Interfaces & contracts"): `Eager` runs the current product shape
/// (stage 2 hydrates every selector-matched stream before stage 3's
/// `LIMIT`); `LateIdx`/`LateProj` are the two bench-local late-hydration
/// prototypes, differing only in how they derive the cheap pre-`LIMIT`
/// `service` set (`log_streams_idx` `DISTINCT val` vs. a narrow
/// `log_streams` `DISTINCT service` projection).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum Variant {
    Eager,
    LateIdx,
    LateProj,
}

impl Variant {
    pub const ALL: [Variant; 3] = [Variant::Eager, Variant::LateIdx, Variant::LateProj];

    pub fn name(self) -> &'static str {
        match self {
            Variant::Eager => "eager",
            Variant::LateIdx => "late_idx",
            Variant::LateProj => "late_proj",
        }
    }

    /// Parses `--rss-variant`'s value (the hidden RSS-probe child mode —
    /// architect plan v5 [R1]).
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "eager" => Ok(Variant::Eager),
            "late_idx" => Ok(Variant::LateIdx),
            "late_proj" => Ok(Variant::LateProj),
            other => anyhow::bail!(
                "--rss-variant: unknown variant {other:?} (expected \"eager\", \"late_idx\", or \
                 \"late_proj\")"
            ),
        }
    }
}

/// A repeated measurement's summary statistics — `values.len()` is the
/// cardinality (architect plan [R5]: **6** for every wall-clock/`system.
/// query_log` metric — one per measured Latin-square round — **3** for
/// `client_rss_delta_kib`/`client_rss_child_hwm_delta_kib` — one per fresh
/// RSS-probe child). `median` is `(sorted[n/2-1] + sorted[n/2]) / 2` for
/// even `n` (architect plan v3 edge case 4 — pinned once here so the
/// runtime verdict assignment and `consistency_tests`' recompute agree
/// bit-for-bit), or the middle element for odd `n`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Dist {
    pub values: Vec<f64>,
    pub median: f64,
    pub min: f64,
    pub max: f64,
}

impl Dist {
    /// Builds a [`Dist`] from `values` (must be non-empty — every caller in
    /// this scenario supplies a fixed, known-non-empty rep count, so this
    /// is a documented infallible-invariant panic, not a production error
    /// path — same convention as `queries.rs`'s `RunOnce::terminal`).
    pub fn from_values(mut values: Vec<f64>) -> Self {
        assert!(
            !values.is_empty(),
            "Dist::from_values requires at least one sample"
        );
        values.sort_by(|a, b| a.partial_cmp(b).expect("bench measurements are finite"));
        let n = values.len();
        let median = if n.is_multiple_of(2) {
            (values[n / 2 - 1] + values[n / 2]) / 2.0
        } else {
            values[n / 2]
        };
        let min = values[0];
        let max = values[n - 1];
        Dist {
            values,
            median,
            min,
            max,
        }
    }
}

/// One stage's per-round measurements (architect plan [R4] evidence
/// schema): `stage` is one of `resolution` | `service_idx` | `service_proj`
/// | `samples` | `hydration_full` | `hydration_late`. `cpu_micros` sources
/// `ProfileEvents['OSCPUVirtualTimeMicroseconds']`, falling back to
/// `UserTimeMicroseconds + SystemTimeMicroseconds` when the former is
/// unavailable (recorded once, scenario-wide, as
/// [`super::super::LogsHydrationReport`]'s... see `paths.rs`'s
/// `CPU_METRIC_SOURCE` — edge case #3 of the architect plan v1).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StageDist {
    pub stage: String,
    pub read_rows: Dist,
    pub read_bytes: Dist,
    pub selected_marks: Dist,
    pub memory_usage: Dist,
    pub query_duration_ms: Dist,
    pub cpu_micros: Dist,
    /// `cpu_micros / (query_duration_ms * 1000)` — ~1.0 indicates one core
    /// saturated for the stage's whole wall duration (the single-threaded
    /// JSON-parse signature the issue's Context describes).
    pub cpu_wall_ratio: Dist,
}

/// One `(path, breadth)` cell's full evidence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PathEvidence {
    /// `"eager"` | `"late_idx"` | `"late_proj"`.
    pub path: String,
    pub breadth: u32,
    pub service: String,
    pub resolved_fps: u64,
    pub returned_rows: u64,
    pub result_fps: u64,
    pub stages: Vec<StageDist>,
    /// Per-round path peak (`max` over that round's stages'
    /// `memory_usage`), then collected across the 6 measured rounds into a
    /// `Dist` (architect plan [R5]: "the median of per-repetition path
    /// peaks", never a max-of-medians).
    pub server_peak_memory_usage: Dist,
    /// Sum, across stages, of each stage's median `read_bytes`/`read_rows`
    /// — additive totals, never a sum of `memory_usage` (architect plan
    /// [F2]).
    pub total_read_bytes: u64,
    pub total_read_rows: u64,
    /// The isolated hydration stage's median `read_bytes`/`cpu_micros` —
    /// `hydration_full` for `eager`, `hydration_late` for the two late
    /// variants — the per-breadth hydration-cost figures the report doc's
    /// comparative-result table quotes (reported diagnostics, not
    /// verdict-predicate inputs — see [`ValidityGates`]/[`evaluate_verdict`]
    /// for what the v7 predicate actually gates on).
    pub hydration_read_bytes_median: u64,
    pub hydration_cpu_micros_median: u64,
    /// In-process wall-clock: stream + decode into the production row
    /// types (`pulsus_read::logql::rows::{StreamMetaRow,SampleRow}`) +
    /// envelope assembly, across every stage the path executes.
    pub client_wall_ms: Dist,
    /// Parent-sampled `rss_peak - rss_at_ready` over the query window
    /// (architect plan v5 [R1]) — the primary RSS attribution.
    pub client_rss_delta_kib: Dist,
    /// The child's own `VmHWM(exit) - VmHWM(ready)` — demoted to a
    /// corroborating lower-bound diagnostic only (v5 [R1]: a monotonic
    /// high-water-mark delta censors to 0 whenever the startup peak
    /// exceeds the query-time peak).
    pub client_rss_child_hwm_delta_kib: Dist,
    /// Set when `client_rss_delta_kib`'s median falls outside
    /// `[0.25x, 4x]` of the decoded envelope's payload size (architect plan
    /// [R6] sane-band) — marks the RSS-backed corroboration claims
    /// `inconclusive` for this cell without affecting the wall/server-
    /// derived verdict.
    pub rss_suspect: bool,
}

impl PathEvidence {
    pub fn stage(&self, name: &str) -> Option<&StageDist> {
        self.stages.iter().find(|s| s.stage == name)
    }
}

/// One breadth's full evidence: the shared stage-1 resolution outcome plus
/// every variant's [`PathEvidence`] (`eager`, `late_idx`, `late_proj`, in
/// that order).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BreadthReport {
    pub breadth: u32,
    pub service: String,
    pub resolved_fps: u64,
    pub paths: Vec<PathEvidence>,
}

impl BreadthReport {
    pub fn path(&self, name: &str) -> Option<&PathEvidence> {
        self.paths.iter().find(|p| p.path == name)
    }
}

/// The recorded decision (architect plan [R2]/[R3]/[R4], v5 total-precedence
/// rule). All three classes are internally consistent and committable;
/// only `Material`/`NotMaterial` may close issue #35 (the separate,
/// non-test close-gate — see this module's doc comment).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Verdict {
    Material,
    NotMaterial,
    Inconclusive,
}

/// The client-wall materiality threshold (architect plan materiality
/// criterion, unchanged since v4): A's median `client_wall_ms` at the high
/// breadth must be at least this many times a B variant's, for that B to
/// count as a material win.
pub const CLIENT_WIN_THRESHOLD: f64 = 2.0;
/// Validity gate (b)'s dispersion bound (architect plan v7): a
/// decision-feeding `client_wall_ms` `Dist`'s `max/median` must not exceed
/// this, at the high breadth, for A and for each B variant. Chosen a
/// priori from observed rep noise (the worst measured client-wall
/// dispersion across the runs that informed this plan was `1.42`) as a
/// trustworthiness floor, not tuned to any particular verdict.
pub const REP_STABILITY_MAX_OVER_MEDIAN_THRESHOLD: f64 = 2.0;
/// The verdict predicate's low/high breadth anchors (architect plan [R4]:
/// evaluated on the full-tier `[1k,10k,50k]` sweep at its two endpoints).
pub const LOW_BREADTH: u32 = 1_000;
pub const HIGH_BREADTH: u32 = 50_000;

/// Provenance for the three v7 **validity gates** — direction-neutral
/// measurement-trustworthiness checks that replaced v6's growth-curve
/// shape gates (architect plan v7: those gates encoded a-priori guesses
/// about eager's/late's `cpu_micros` growth curves that the measured data
/// contradicted **in both directions for the same structural cause** —
/// `log_streams`' sparse index not pruning granules at these breadths —
/// making further threshold tuning verdict-shopping). None of the three
/// gates below can be satisfied or failed by *which* variant wins; they
/// only certify the measurement itself is trustworthy enough for the
/// unchanged 2.0x client-wall decision gate to be believed.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ValidityGates {
    /// (a) Correctness/identity. Always `true` for any breadth present in
    /// committed evidence: `paths::correctness_gate` is mandatory and
    /// aborts the *entire* run before any evidence is recorded if the
    /// returned fingerprint/envelope set (every path, every breadth) ever
    /// diverges from the corpus generator's fixed expected result set
    /// (architect plan F5). This field is provenance/auditability, not a
    /// value that can be `false` in committed data — see
    /// `paths::assert_result_set_identity`.
    pub identity_ok: bool,
    /// (b) Rep-stability: `max/median <= REP_STABILITY_MAX_OVER_MEDIAN_THRESHOLD`
    /// for the decision-feeding `client_wall_ms` `Dist` of `eager`,
    /// `late_idx`, and `late_proj`, all at [`HIGH_BREADTH`].
    pub rep_stability_ok: bool,
    /// The three measured `max/median` ratios gate (b) checked, keyed by
    /// path name (`"eager"` | `"late_idx"` | `"late_proj"`) — recorded
    /// even when the gate passes, so the artifact shows *how much*
    /// headroom there was, not just a boolean.
    pub rep_stability_max_over_median: std::collections::BTreeMap<String, f64>,
    /// (c) Cross-path storage-equality, checked at **every** breadth in
    /// the sweep: `resolution`/`samples` stage `read_bytes`/
    /// `selected_marks` are byte-identical across `eager`/`late_idx`/
    /// `late_proj`, and `eager.hydration_full.{read_bytes,selected_marks}`
    /// equals each late variant's `hydration_late.{read_bytes,
    /// selected_marks}`. Proves storage I/O is strategy-invariant — the
    /// storage layer is provably blameless — so any wall/CPU delta
    /// isolates the hydration *strategy*, not I/O (the late paths' own
    /// service-derivation stage bytes are the reported *cost* of that
    /// strategy, not part of this equality).
    pub storage_equality_ok: bool,
}

/// The recorded materiality decision plus every input the predicate
/// consumed — carried on [`LogsHydrationReport`] and recomputed bit-for-bit
/// by `consistency_tests` from the same formulas.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerdictOutcome {
    pub verdict: Verdict,
    pub validity_gates: ValidityGates,
    /// `Some(_)` once the validity gates all pass (v7: no per-B shape
    /// filtering — both variants are always evaluated once the
    /// measurement is trustworthy).
    pub win_late_idx: Option<bool>,
    pub win_late_proj: Option<bool>,
    pub recommended_b_variant: Option<String>,
    pub rebenchmark_note: Option<String>,
}

fn max_over_median(d: &Dist) -> f64 {
    if d.median != 0.0 {
        d.max / d.median
    } else if d.max == 0.0 {
        1.0
    } else {
        f64::INFINITY
    }
}

/// Compares the **complete six-value per-repetition distributions**
/// (`Dist::values`), not just the median (code review round-2 [medium]:
/// equal medians can conceal per-repetition `read_bytes`/`selected_marks`
/// divergence — e.g. `[1,1,1,1,1,1]` and `[0,0,0,0,0,6]` share a median of
/// `1` but are not byte-exact). `Dist::values` is the sorted rep sequence
/// (`Dist::from_values`), so this is a sorted-multiset comparison — exact
/// for the deterministic storage counters this gate checks (a
/// non-deterministic counter would already violate v7's byte-exact
/// premise regardless of round-to-round pairing).
fn storage_equal(a: &StageDist, b: &StageDist) -> bool {
    a.read_bytes.values == b.read_bytes.values && a.selected_marks.values == b.selected_marks.values
}

/// Validity gate (c): cross-path storage-equality, checked at every
/// breadth present in `breadths` (not only the two anchors — storage
/// invariance is a per-breadth property of the query engine, not a
/// breadth-sweep growth curve, so checking the whole sweep is strictly
/// more thorough). `None` if any path/stage the check needs is missing.
fn storage_equality_holds(breadths: &[BreadthReport]) -> Option<bool> {
    for br in breadths {
        let eager = br.path("eager")?;
        let late_idx = br.path("late_idx")?;
        let late_proj = br.path("late_proj")?;

        for stage_name in ["resolution", "samples"] {
            let e = eager.stage(stage_name)?;
            let i = late_idx.stage(stage_name)?;
            let p = late_proj.stage(stage_name)?;
            if !storage_equal(e, i) || !storage_equal(e, p) {
                return Some(false);
            }
        }

        let eager_hyd = eager.stage("hydration_full")?;
        let idx_hyd = late_idx.stage("hydration_late")?;
        let proj_hyd = late_proj.stage("hydration_late")?;
        if !storage_equal(eager_hyd, idx_hyd) || !storage_equal(eager_hyd, proj_hyd) {
            return Some(false);
        }
    }
    Some(true)
}

/// Evaluates the architect plan v7 validity-gate-redesigned verdict rule
/// over `breadths` (`None` if [`LOW_BREADTH`]/[`HIGH_BREADTH`]'s evidence,
/// or any path/stage a gate needs, is absent — e.g. the CI-tier `[1k,10k]`
/// sweep, which never reaches [`HIGH_BREADTH`] and therefore carries no
/// verdict by design).
///
/// **Precedence (v7, total):**
/// 1. Any of the three [`ValidityGates`] fails → `Inconclusive` + a
///    rebenchmark note naming which gate(s) failed — never `NotMaterial`
///    (an untrustworthy measurement is not evidence of "no win").
/// 2. All validity gates pass, **any** B variant passes the unchanged
///    `CLIENT_WIN_THRESHOLD` decision gate → `Material` (both B variants
///    are always evaluated — v7 removed the v5/v6 per-B shape-validity
///    filtering; if both pass, name — never gate — the one with the lower
///    median service-derivation-stage `cpu_micros` at the high breadth,
///    tiebreak `client_wall_ms`).
/// 3. All validity gates pass, no B variant passes the decision gate →
///    `NotMaterial`.
pub fn evaluate_verdict(breadths: &[BreadthReport]) -> Option<VerdictOutcome> {
    let low_present = breadths.iter().any(|b| b.breadth == LOW_BREADTH);
    let high_present = breadths.iter().any(|b| b.breadth == HIGH_BREADTH);
    if !low_present || !high_present {
        return None;
    }
    let high = breadths.iter().find(|b| b.breadth == HIGH_BREADTH)?;

    let a_high = high.path("eager")?;
    let b_idx_high = high.path("late_idx")?;
    let b_proj_high = high.path("late_proj")?;

    // (a) Correctness/identity — see ValidityGates::identity_ok's doc
    // comment: always true for committed data.
    let identity_ok = true;

    // (b) Rep-stability, at the high breadth, on the three decision-feeding
    // client_wall_ms Dists.
    let mut rep_stability_max_over_median = std::collections::BTreeMap::new();
    rep_stability_max_over_median
        .insert("eager".to_string(), max_over_median(&a_high.client_wall_ms));
    rep_stability_max_over_median.insert(
        "late_idx".to_string(),
        max_over_median(&b_idx_high.client_wall_ms),
    );
    rep_stability_max_over_median.insert(
        "late_proj".to_string(),
        max_over_median(&b_proj_high.client_wall_ms),
    );
    let rep_stability_ok = rep_stability_max_over_median
        .values()
        .all(|&r| r <= REP_STABILITY_MAX_OVER_MEDIAN_THRESHOLD);

    // (c) Cross-path storage-equality, over the whole sweep.
    let storage_equality_ok = storage_equality_holds(breadths)?;

    let validity_gates = ValidityGates {
        identity_ok,
        rep_stability_ok,
        rep_stability_max_over_median,
        storage_equality_ok,
    };

    if !identity_ok || !rep_stability_ok || !storage_equality_ok {
        let mut reasons = Vec::new();
        if !identity_ok {
            reasons.push("correctness/identity gate failed".to_string());
        }
        if !rep_stability_ok {
            reasons.push(format!(
                "rep-stability gate failed (client_wall_ms max/median exceeded \
                 {REP_STABILITY_MAX_OVER_MEDIAN_THRESHOLD}x for at least one path at breadth \
                 {HIGH_BREADTH} — measurement not trustworthy, excessive host/scheduling noise)"
            ));
        }
        if !storage_equality_ok {
            reasons.push(
                "cross-path storage-equality gate failed (resolution/samples/hydration \
                 read_bytes or selected_marks diverged across paths — storage I/O was not \
                 isolated, so a wall/CPU delta cannot be attributed to hydration strategy alone)"
                    .to_string(),
            );
        }
        let note = format!(
            "validity gate(s) failed: {} — measurement not trustworthy; revise and rerun \
             --profile full",
            reasons.join("; ")
        );
        return Some(VerdictOutcome {
            verdict: Verdict::Inconclusive,
            validity_gates,
            win_late_idx: None,
            win_late_proj: None,
            recommended_b_variant: None,
            rebenchmark_note: Some(note),
        });
    }

    let a_wall_high = a_high.client_wall_ms.median;
    let win_late_idx = Some(a_wall_high >= CLIENT_WIN_THRESHOLD * b_idx_high.client_wall_ms.median);
    let win_late_proj =
        Some(a_wall_high >= CLIENT_WIN_THRESHOLD * b_proj_high.client_wall_ms.median);

    let passing: Vec<&str> = [("late_idx", win_late_idx), ("late_proj", win_late_proj)]
        .into_iter()
        .filter(|(_, w)| w.unwrap_or(false))
        .map(|(name, _)| name)
        .collect();

    if passing.is_empty() {
        return Some(VerdictOutcome {
            verdict: Verdict::NotMaterial,
            validity_gates,
            win_late_idx,
            win_late_proj,
            recommended_b_variant: None,
            rebenchmark_note: None,
        });
    }

    let recommended = if passing.len() == 1 {
        passing[0].to_string()
    } else {
        // Both passed: name (never gate) the cheaper service-derivation
        // stage at the high breadth — lower median cpu_micros (v7: not
        // read_bytes, which the storage-equality gate already proved
        // identical across derivation strategies at the samples/
        // resolution level, though `service_idx`/`service_proj` are the
        // one stage pair that genuinely differs — see `paths.rs`),
        // tiebreak `client_wall_ms`.
        let idx_stage = high.path("late_idx")?.stage("service_idx")?;
        let proj_stage = high.path("late_proj")?.stage("service_proj")?;
        if idx_stage.cpu_micros.median < proj_stage.cpu_micros.median {
            "late_idx".to_string()
        } else if proj_stage.cpu_micros.median < idx_stage.cpu_micros.median {
            "late_proj".to_string()
        } else if b_idx_high.client_wall_ms.median <= b_proj_high.client_wall_ms.median {
            "late_idx".to_string()
        } else {
            "late_proj".to_string()
        }
    };

    Some(VerdictOutcome {
        verdict: Verdict::Material,
        validity_gates,
        win_late_idx,
        win_late_proj,
        recommended_b_variant: Some(recommended),
        rebenchmark_note: None,
    })
}

/// Closeout artifact fields (architect plan [R5]/AC3) — filled by the
/// task-manager/human decision-comment step, never by this scenario's code
/// (out of scope: "do NOT auto-file the follow-up issue from code").
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CloseoutRef {
    pub comment_url: Option<String>,
    pub followup_issue: Option<String>,
    pub no_change_rationale: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogsHydrationReport {
    pub profile: Profile,
    pub seed: u64,
    pub breadths: Vec<u32>,
    /// Which `ProfileEvents` key fired for `cpu_micros`
    /// (`"OSCPUVirtualTimeMicroseconds"` or the
    /// `"UserTimeMicroseconds+SystemTimeMicroseconds"` fallback) — recorded
    /// once, scenario-wide (architect plan edge case #2 / open question
    /// #3).
    pub cpu_metric_source: String,
    pub breadth_reports: Vec<BreadthReport>,
    /// `None` unless the sweep reaches [`HIGH_BREADTH`] (the CI-tier
    /// `[1k,10k]` artifact is record-only, no verdict).
    pub verdict: Option<VerdictOutcome>,
    pub closeout: CloseoutRef,
}

fn fmt_dist(d: &Dist) -> String {
    format!("{:.1} [{:.1},{:.1}]", d.median, d.min, d.max)
}

/// Renders the per-path/per-breadth/per-stage evidence table plus the
/// verdict summary. Every row is rendered — curation for
/// docs/benchmarks/m1-logs-late-hydration.md happens in the committed
/// report, not here (same division of labour as `report.rs::render_markdown`
/// / `metrics_labels::report::render_markdown`).
pub fn render_markdown(report: &LogsHydrationReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "<!-- generated by `cargo xtask bench logs-hydration` (profile={:?}, seed={}, \
         cpu_metric_source={}) -->\n",
        report.profile, report.seed, report.cpu_metric_source
    ));
    out.push_str(&format!("Breadths: {:?}.\n\n", report.breadths));

    out.push_str(
        "| breadth | path | stage | read_rows | read_bytes | selected_marks | memory_usage | \
         query_duration_ms | cpu_micros | cpu_wall_ratio |\n",
    );
    out.push_str("|---|---|---|---|---|---|---|---|---|---|\n");
    for b in &report.breadth_reports {
        for p in &b.paths {
            for s in &p.stages {
                out.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                    b.breadth,
                    p.path,
                    s.stage,
                    fmt_dist(&s.read_rows),
                    fmt_dist(&s.read_bytes),
                    fmt_dist(&s.selected_marks),
                    fmt_dist(&s.memory_usage),
                    fmt_dist(&s.query_duration_ms),
                    fmt_dist(&s.cpu_micros),
                    fmt_dist(&s.cpu_wall_ratio),
                ));
            }
        }
    }

    out.push_str(
        "\n| breadth | path | resolved_fps | result_fps | client_wall_ms | \
         server_peak_memory_usage | client_rss_delta_kib | client_rss_child_hwm_delta_kib | \
         rss_claim |\n",
    );
    out.push_str("|---|---|---|---|---|---|---|---|---|\n");
    for b in &report.breadth_reports {
        for p in &b.paths {
            // `rss_claim` renders as `inconclusive` — never as corroborating
            // evidence — whenever the [R6] sane band flagged the sample
            // `rss_suspect` (architect plan v6 [A2] / code review [medium]:
            // RSS never enters the verdict, and a suspect cell must not be
            // presented as a supporting reduction). `reported (in-band)` is
            // the only state that would ever describe RSS as corroborating,
            // and even then only as a non-gating diagnostic.
            let rss_claim = if p.rss_suspect {
                "inconclusive (suspect measurement)"
            } else {
                "reported (in-band, non-gating)"
            };
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                b.breadth,
                p.path,
                p.resolved_fps,
                p.result_fps,
                fmt_dist(&p.client_wall_ms),
                fmt_dist(&p.server_peak_memory_usage),
                fmt_dist(&p.client_rss_delta_kib),
                fmt_dist(&p.client_rss_child_hwm_delta_kib),
                rss_claim,
            ));
        }
    }

    out.push_str("\n## Verdict\n\n");
    match &report.verdict {
        Some(v) => {
            out.push_str(&format!(
                "verdict={:?} identity_ok={} rep_stability_ok={} rep_stability_max_over_median={:?} \
                 storage_equality_ok={} win_late_idx={:?} win_late_proj={:?} \
                 recommended_b_variant={:?}\n\n",
                v.verdict,
                v.validity_gates.identity_ok,
                v.validity_gates.rep_stability_ok,
                v.validity_gates.rep_stability_max_over_median,
                v.validity_gates.storage_equality_ok,
                v.win_late_idx,
                v.win_late_proj,
                v.recommended_b_variant,
            ));
            if let Some(note) = &v.rebenchmark_note {
                out.push_str(&format!("Rebenchmark note: {note}\n"));
            }
        }
        None => out.push_str(
            "No verdict recorded — this sweep does not reach the high breadth \
             (50,000) the materiality predicate is evaluated at (CI-tier record-only \
             artifact).\n",
        ),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dist(values: &[f64]) -> Dist {
        Dist::from_values(values.to_vec())
    }

    fn flat_dist(v: f64) -> Dist {
        dist(&[v; 6])
    }

    fn stage_with(name: &str, read_bytes: f64, selected_marks: f64, cpu_micros: f64) -> StageDist {
        StageDist {
            stage: name.to_string(),
            read_rows: flat_dist(1.0),
            read_bytes: flat_dist(read_bytes),
            selected_marks: flat_dist(selected_marks),
            memory_usage: flat_dist(1.0),
            query_duration_ms: flat_dist(1.0),
            cpu_micros: flat_dist(cpu_micros),
            cpu_wall_ratio: flat_dist(1.0),
        }
    }

    /// Builds one path's evidence with independent control over every
    /// stage's `(read_bytes, selected_marks)` pair (so tests can construct
    /// a cross-path-consistent baseline, then perturb exactly one figure to
    /// exercise validity gate (c)) and the raw `client_wall_ms` rep values
    /// (so tests can construct a stable or a dispersed Dist directly, for
    /// validity gate (b)).
    #[allow(clippy::too_many_arguments)]
    fn path_evidence(
        name: &str,
        hydration_stage: &str,
        resolution: (f64, f64),
        samples: (f64, f64),
        hydration: (f64, f64),
        derive_cpu_micros: f64,
        wall_values: &[f64],
    ) -> PathEvidence {
        let mut stages = vec![stage_with("resolution", resolution.0, resolution.1, 1.0)];
        if name != "eager" {
            let derive_stage = if name == "late_idx" {
                "service_idx"
            } else {
                "service_proj"
            };
            stages.push(stage_with(derive_stage, 10.0, 1.0, derive_cpu_micros));
        }
        stages.push(stage_with("samples", samples.0, samples.1, 1.0));
        stages.push(stage_with(hydration_stage, hydration.0, hydration.1, 1.0));
        PathEvidence {
            path: name.to_string(),
            breadth: 0,
            service: "svc-broad".to_string(),
            resolved_fps: 1,
            returned_rows: 100,
            result_fps: 100,
            stages,
            server_peak_memory_usage: flat_dist(1.0),
            total_read_bytes: hydration.0 as u64,
            total_read_rows: 1,
            hydration_read_bytes_median: hydration.0 as u64,
            hydration_cpu_micros_median: 1,
            client_wall_ms: dist(wall_values),
            client_rss_delta_kib: flat_dist(1.0),
            client_rss_child_hwm_delta_kib: flat_dist(1.0),
            rss_suspect: false,
        }
    }

    /// A cross-path-consistent breadth (validity gates (b)/(c) both pass by
    /// construction): `resolution`/`samples`/hydration `read_bytes`/
    /// `selected_marks` are identical across `eager`/`late_idx`/`late_proj`
    /// (satisfying gate (c)), and every `client_wall_ms` Dist is flat
    /// (`max/median == 1.0`, satisfying gate (b)). `a_wall`/`b_wall` are
    /// each repeated 6x into a flat Dist — tests that need to control
    /// dispersion or storage divergence directly mutate the returned
    /// `BreadthReport`.
    fn breadth(breadth: u32, a_wall: f64, b_wall: f64) -> BreadthReport {
        let resolution = (1_000.0, 4.0);
        let samples = (2_000.0, 8.0);
        let hydration = (500.0, 2.0);
        let mut eager = path_evidence(
            "eager",
            "hydration_full",
            resolution,
            samples,
            hydration,
            0.0,
            &[a_wall; 6],
        );
        eager.breadth = breadth;
        let mut late_idx = path_evidence(
            "late_idx",
            "hydration_late",
            resolution,
            samples,
            hydration,
            5.0,
            &[b_wall; 6],
        );
        late_idx.breadth = breadth;
        let mut late_proj = path_evidence(
            "late_proj",
            "hydration_late",
            resolution,
            samples,
            hydration,
            5.0,
            &[b_wall; 6],
        );
        late_proj.breadth = breadth;
        BreadthReport {
            breadth,
            service: "svc-broad".to_string(),
            resolved_fps: u64::from(breadth),
            paths: vec![eager, late_idx, late_proj],
        }
    }

    fn stage_mut<'a>(path: &'a mut PathEvidence, name: &str) -> &'a mut StageDist {
        path.stages
            .iter_mut()
            .find(|s| s.stage == name)
            .unwrap_or_else(|| panic!("stage {name} missing"))
    }

    #[test]
    fn dist_from_values_computes_the_pinned_even_cardinality_median() {
        let d = Dist::from_values(vec![5.0, 1.0, 3.0, 2.0, 4.0, 6.0]);
        assert_eq!(d.median, 3.5);
        assert_eq!(d.min, 1.0);
        assert_eq!(d.max, 6.0);
    }

    /// The exact regression code review round-2 [medium] caught: equal
    /// medians (both `1.0`) but a diverging per-repetition distribution —
    /// a median-only comparison would have wrongly reported storage
    /// equality here.
    #[test]
    fn storage_equal_requires_full_distribution_equality_not_just_median() {
        let a = stage_with("resolution", 1.0, 1.0, 1.0);
        let mut b = stage_with("resolution", 1.0, 1.0, 1.0);
        // Median of [0,0,0,0,0,6] is (0+0)/2 = 0.0... use a distribution
        // whose median genuinely agrees with `a`'s flat 1.0 (six reps) but
        // whose values differ per-repetition: [0,1,1,1,1,3] -> sorted
        // median (1+1)/2 = 1.0, matching `a`'s flat-1.0 median exactly.
        b.read_bytes = Dist::from_values(vec![0.0, 1.0, 1.0, 1.0, 1.0, 3.0]);
        assert_eq!(
            a.read_bytes.median, b.read_bytes.median,
            "medians must agree for this to be the regression the median-only check missed"
        );
        assert_ne!(a.read_bytes.values, b.read_bytes.values);
        assert!(!storage_equal(&a, &b));
    }

    #[test]
    fn storage_equal_passes_when_full_distributions_match() {
        let a = stage_with("resolution", 100.0, 5.0, 1.0);
        let b = stage_with("resolution", 100.0, 5.0, 999.0);
        // cpu_micros deliberately differs — storage_equal only compares
        // read_bytes/selected_marks, never cpu_micros (a diagnostic, not a
        // storage-I/O figure).
        assert!(storage_equal(&a, &b));
    }

    /// Code review round-2 [low]: a recorded `VerdictOutcome` that agrees
    /// on the top-level `Verdict` enum but drifts on validity-gate
    /// provenance must be detected as unequal by the same `PartialEq`
    /// `consistency_tests::recorded_verdict_matches_the_recomputed_verdict`
    /// relies on — demonstrated directly here without a live artifact.
    #[test]
    fn verdict_outcome_partial_eq_detects_provenance_drift_behind_an_agreeing_verdict() {
        let recorded = VerdictOutcome {
            verdict: Verdict::NotMaterial,
            validity_gates: ValidityGates {
                identity_ok: true,
                rep_stability_ok: true,
                rep_stability_max_over_median: std::collections::BTreeMap::new(),
                storage_equality_ok: true,
            },
            win_late_idx: Some(false),
            win_late_proj: Some(false),
            recommended_b_variant: None,
            rebenchmark_note: None,
        };
        let mut recomputed = recorded.clone();
        // Same top-level Verdict, but the recomputed win flag disagrees —
        // a class-only comparison would miss this.
        recomputed.win_late_proj = Some(true);
        assert_eq!(recorded.verdict, recomputed.verdict);
        assert_ne!(recorded, recomputed);
    }

    #[test]
    fn evaluate_verdict_is_none_without_both_anchor_breadths() {
        let breadths = vec![breadth(1_000, 10.0, 10.0)];
        assert!(evaluate_verdict(&breadths).is_none());
    }

    #[test]
    fn evaluate_verdict_is_material_when_validity_holds_and_client_wins() {
        // Client wall: A 200ms vs B 50ms at 50k (4x, >= 2.0x).
        let breadths = vec![breadth(1_000, 10.0, 10.0), breadth(50_000, 200.0, 50.0)];
        let outcome = evaluate_verdict(&breadths).expect("both anchors present");
        assert_eq!(outcome.verdict, Verdict::Material);
        assert!(outcome.validity_gates.identity_ok);
        assert!(outcome.validity_gates.rep_stability_ok);
        assert!(outcome.validity_gates.storage_equality_ok);
        assert_eq!(outcome.win_late_idx, Some(true));
        assert_eq!(outcome.win_late_proj, Some(true));
        // Both B variants win; late_idx's service-derivation cpu_micros
        // (5.0, set by the `breadth` helper for both) ties, so the wall
        // tiebreak decides — both walls are equal too (`b_wall` shared), so
        // `<=` picks late_idx deterministically.
        assert_eq!(outcome.recommended_b_variant, Some("late_idx".to_string()));
    }

    #[test]
    fn evaluate_verdict_is_not_material_when_validity_holds_but_client_does_not_win() {
        // Client wall: A 60ms vs B 50ms at 50k (1.2x, < 2.0x).
        let breadths = vec![breadth(1_000, 10.0, 10.0), breadth(50_000, 60.0, 50.0)];
        let outcome = evaluate_verdict(&breadths).expect("both anchors present");
        assert_eq!(outcome.verdict, Verdict::NotMaterial);
        assert!(outcome.validity_gates.rep_stability_ok);
        assert!(outcome.validity_gates.storage_equality_ok);
        assert_eq!(outcome.win_late_idx, Some(false));
        assert_eq!(outcome.win_late_proj, Some(false));
        assert_eq!(outcome.recommended_b_variant, None);
    }

    #[test]
    fn evaluate_verdict_is_inconclusive_when_rep_stability_fails() {
        // A's client_wall_ms at 50k: [10,10,10,10,10,50] -> median 10, max
        // 50, ratio 5.0 > 2.0 — excessive dispersion, untrustworthy.
        let mut breadths = vec![breadth(1_000, 10.0, 10.0), breadth(50_000, 200.0, 50.0)];
        let high = breadths.iter_mut().find(|b| b.breadth == 50_000).unwrap();
        let eager = high.paths.iter_mut().find(|p| p.path == "eager").unwrap();
        eager.client_wall_ms = dist(&[10.0, 10.0, 10.0, 10.0, 10.0, 50.0]);
        let outcome = evaluate_verdict(&breadths).expect("both anchors present");
        assert_eq!(outcome.verdict, Verdict::Inconclusive);
        assert!(!outcome.validity_gates.rep_stability_ok);
        assert!(outcome.validity_gates.storage_equality_ok);
        assert!(
            outcome
                .rebenchmark_note
                .as_ref()
                .unwrap()
                .contains("rep-stability")
        );
        assert_eq!(outcome.win_late_idx, None);
    }

    #[test]
    fn evaluate_verdict_is_inconclusive_when_storage_equality_fails() {
        // Diverge late_idx's hydration_late.read_bytes from eager's
        // hydration_full.read_bytes at the high breadth — storage I/O is
        // no longer strategy-invariant, so the measurement can't isolate
        // hydration strategy.
        let mut breadths = vec![breadth(1_000, 10.0, 10.0), breadth(50_000, 200.0, 50.0)];
        let high = breadths.iter_mut().find(|b| b.breadth == 50_000).unwrap();
        let late_idx = high
            .paths
            .iter_mut()
            .find(|p| p.path == "late_idx")
            .unwrap();
        stage_mut(late_idx, "hydration_late").read_bytes = flat_dist(999_999.0);
        let outcome = evaluate_verdict(&breadths).expect("both anchors present");
        assert_eq!(outcome.verdict, Verdict::Inconclusive);
        assert!(!outcome.validity_gates.storage_equality_ok);
        assert!(outcome.validity_gates.rep_stability_ok);
        assert!(
            outcome
                .rebenchmark_note
                .as_ref()
                .unwrap()
                .contains("storage-equality")
        );
    }

    #[test]
    fn evaluate_verdict_recommends_the_cheaper_derivation_when_both_b_win() {
        // Both B variants win (shared b_wall); late_proj's service-proj
        // stage gets a lower cpu_micros than late_idx's service_idx, so
        // late_proj should be named.
        let mut breadths = vec![breadth(1_000, 10.0, 10.0), breadth(50_000, 200.0, 50.0)];
        let high = breadths.iter_mut().find(|b| b.breadth == 50_000).unwrap();
        let late_proj = high
            .paths
            .iter_mut()
            .find(|p| p.path == "late_proj")
            .unwrap();
        stage_mut(late_proj, "service_proj").cpu_micros = flat_dist(1.0);
        let outcome = evaluate_verdict(&breadths).expect("both anchors present");
        assert_eq!(outcome.verdict, Verdict::Material);
        assert_eq!(outcome.recommended_b_variant, Some("late_proj".to_string()));
    }

    #[test]
    fn logs_hydration_report_round_trips_through_json() {
        let report = LogsHydrationReport {
            profile: Profile::Ci,
            seed: 1,
            breadths: vec![1_000, 10_000],
            cpu_metric_source: "OSCPUVirtualTimeMicroseconds".to_string(),
            breadth_reports: vec![breadth(1_000, 10.0, 10.0)],
            verdict: None,
            closeout: CloseoutRef::default(),
        };
        let json = serde_json::to_string(&report).expect("serializes");
        let back: LogsHydrationReport = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back.breadths, vec![1_000, 10_000]);
    }

    #[test]
    fn render_markdown_includes_every_breadth_and_the_verdict_section() {
        let report = LogsHydrationReport {
            profile: Profile::Ci,
            seed: 1,
            breadths: vec![1_000],
            cpu_metric_source: "OSCPUVirtualTimeMicroseconds".to_string(),
            breadth_reports: vec![breadth(1_000, 10.0, 10.0)],
            verdict: None,
            closeout: CloseoutRef::default(),
        };
        let md = render_markdown(&report);
        assert!(md.contains("late_idx"));
        assert!(md.contains("## Verdict"));
        assert!(md.contains("No verdict recorded"));
    }
}

/// Mechanical consistency test (architect plan v5 [R2]/[R3]): loads the
/// **committed** `docs/benchmarks/data/logs-hydration-full.json` and
/// recomputes its verdict from [`evaluate_verdict`], asserting the
/// **complete recorded `VerdictOutcome` equals the complete recomputed
/// one** — every validity-gate boolean and measured ratio, both `win_*`
/// flags, `recommended_b_variant`, and `rebenchmark_note` — not just the
/// top-level `Verdict` enum (code review round-2 [low]: comparing only the
/// enum would let recorded provenance drift from what the pinned formulas
/// actually produce while the test stayed green). Green for **any** of the
/// three verdict classes (an `Inconclusive` artifact is internally
/// consistent and may be committed as history) — this test never requires
/// a particular verdict. It reads a real file, not a live database, so it
/// runs in the ordinary `cargo test -p xtask` pass.
#[cfg(test)]
mod consistency_tests {
    use super::*;

    fn load_committed_report() -> LogsHydrationReport {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../docs/benchmarks/data/logs-hydration-full.json"
        );
        let json = std::fs::read_to_string(path).unwrap_or_else(|e| {
            panic!(
                "failed to read the committed evidence artifact at {path}: {e} — this test \
                 recomputes the logs-hydration verdict from it; regenerate via `cargo xtask \
                 bench logs-hydration --profile full` if it is genuinely missing"
            )
        });
        serde_json::from_str(&json).unwrap_or_else(|e| {
            panic!("failed to deserialize the committed evidence artifact at {path}: {e}")
        })
    }

    #[test]
    fn recorded_verdict_matches_the_recomputed_verdict() {
        let report = load_committed_report();
        let recomputed = evaluate_verdict(&report.breadth_reports);
        assert_eq!(
            report.verdict, recomputed,
            "the committed logs-hydration-full.json's recorded VerdictOutcome does not match \
             what evaluate_verdict recomputes from its own breadth_reports (validity-gate \
             booleans/ratios, win flags, recommended variant, or rebenchmark note have drifted \
             from the pinned formulas, even if the top-level Verdict enum still happens to \
             agree) — recorded={:?} recomputed={:?}",
            report.verdict, recomputed
        );
    }

    /// AC3's hard close-gate (architect plan v5 [R3], separate from the
    /// always-green consistency assertion above): issue #35 may not close
    /// on an `Inconclusive` artifact. This test documents, rather than
    /// enforces at `cargo test` time (an `Inconclusive` artifact is valid,
    /// committable history mid-rebenchmark-iteration — enforcing it here
    /// would make every intermediate commit a broken build), the
    /// obligation: a `Material`/`NotMaterial` verdict's decision comment
    /// (with its follow-up-issue link / no-change rationale) must be
    /// posted on issue #35 before close.
    #[test]
    fn material_or_not_material_verdicts_carry_a_closeout_obligation_note() {
        let report = load_committed_report();
        if let Some(v) = &report.verdict {
            match v.verdict {
                Verdict::Material => assert!(
                    report.closeout.followup_issue.is_some()
                        || report.closeout.comment_url.is_some(),
                    "a material verdict requires either a recorded follow-up-issue link or at \
                     least a decision-comment URL before issue #35 may close"
                ),
                Verdict::NotMaterial => assert!(
                    report.closeout.no_change_rationale.is_some()
                        || report.closeout.comment_url.is_some(),
                    "a not_material verdict requires either a recorded no-change rationale or at \
                     least a decision-comment URL before issue #35 may close"
                ),
                Verdict::Inconclusive => {
                    assert!(
                        v.rebenchmark_note.is_some(),
                        "an inconclusive verdict must carry a rebenchmark note"
                    );
                }
            }
        }
    }
}
