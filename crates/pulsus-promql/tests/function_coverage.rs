//! Issue #64 (M6-01): the machine-checked function-coverage drift test —
//! the single authority for "what PromQL surface PulsusDB implements".
//!
//! Gates, in order (plan v2):
//! 1. registry integrity (SHA-256 + counts) before any check (#29 F1);
//! 2. coverage identity: manifest names == registry names, both
//!    directions, for functions (89) and aggregation operators (14);
//! 3. experimental parity with the registry (17 experimental functions;
//!    `limitk`/`limit_ratio` experimental operators);
//! 4. probe classification: `status == "implemented"` ⟺ a typed synthetic
//!    probe runs `parse -> plan -> evaluate` to `Ok` (Δ3: never plan()-Ok
//!    alone), and scheduled/deferred probes error;
//! 5. every `implemented` entry's `witness` corpus case exists, is not
//!    skip-listed or ledger-allowlisted, and passes when replayed through
//!    the same runner the corpus test uses;
//! 6. the closed language-feature set matches by exact identity, and all
//!    11 `Expr` discriminants are mapped (compile-time exhaustive match —
//!    a 12th variant in a parser bump fails compilation here).

#[path = "promqltest/mod.rs"]
mod driver;

use std::collections::{BTreeMap, BTreeSet};

use pulsus_promql::parser::Expr;
use pulsus_promql::{FetchedSeries, Labels, PlanParams, Sample, SeriesData, plan};

use driver::runner::collect_constructs;
use driver::{CoverageManifest, RegistryFunction, Status, Witness, load_registry_verified};

// ---------------------------------------------------------------------------
// 1–3: integrity + identity + experimental parity
// ---------------------------------------------------------------------------

/// The pinned v3.13.0 registry dimensions: 89 functions (the
/// features.md §7 count — function-only, per the #64 Q1 adjudication), of
/// which 17 are experimental, plus 14 aggregation-operator keywords
/// tracked as a separate dimension.
#[test]
fn registry_pins_the_v3_13_dimensions() {
    let registry = load_registry_verified();
    assert_eq!(registry.prometheus_tag, "v3.13.0");
    assert_eq!(
        registry.functions.len(),
        89,
        "v3.13.0 registers 89 functions"
    );
    assert_eq!(
        registry.functions.iter().filter(|f| f.experimental).count(),
        17,
        "v3.13.0 marks 17 functions experimental"
    );
    assert_eq!(
        registry.aggregation_operators.len(),
        14,
        "v3.13.0 has 14 aggregation-operator keywords"
    );
    assert_eq!(
        registry
            .aggregation_operators
            .iter()
            .filter(|o| o.experimental)
            .map(|o| o.name.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["limit_ratio", "limitk"]),
        "limitk/limit_ratio are the experimental aggregators \
         (lex.go IsExperimentalAggregator)"
    );
}

#[test]
fn coverage_identity_matches_the_registry_both_directions() {
    let registry = load_registry_verified();
    let manifest = CoverageManifest::load();
    assert_eq!(manifest.prometheus_tag, registry.prometheus_tag);

    let registry_fns: BTreeSet<&str> = registry.functions.iter().map(|f| f.name.as_str()).collect();
    let manifest_fns: BTreeSet<&str> = manifest.functions.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        manifest_fns.len(),
        manifest.functions.len(),
        "duplicate function names in function-coverage.json"
    );
    let phantom: Vec<_> = manifest_fns.difference(&registry_fns).collect();
    let missing: Vec<_> = registry_fns.difference(&manifest_fns).collect();
    assert!(
        phantom.is_empty() && missing.is_empty(),
        "function-coverage.json != registry: phantom={phantom:?} missing={missing:?}"
    );

    let registry_ops: BTreeSet<&str> = registry
        .aggregation_operators
        .iter()
        .map(|o| o.name.as_str())
        .collect();
    let manifest_ops: BTreeSet<&str> = manifest
        .aggregation_operators
        .iter()
        .map(|o| o.name.as_str())
        .collect();
    assert_eq!(
        manifest_ops.len(),
        manifest.aggregation_operators.len(),
        "duplicate operator names in function-coverage.json"
    );
    assert_eq!(
        manifest_ops, registry_ops,
        "aggregation operators in function-coverage.json != registry"
    );
}

#[test]
fn experimental_flags_match_the_registry() {
    let registry = load_registry_verified();
    let manifest = CoverageManifest::load();
    let registry_by_name: BTreeMap<&str, bool> = registry
        .functions
        .iter()
        .map(|f| (f.name.as_str(), f.experimental))
        .collect();
    for f in &manifest.functions {
        assert_eq!(
            Some(&f.experimental),
            registry_by_name.get(f.name.as_str()),
            "experimental flag for function {:?} disagrees with the registry",
            f.name
        );
    }
    let ops_by_name: BTreeMap<&str, bool> = registry
        .aggregation_operators
        .iter()
        .map(|o| (o.name.as_str(), o.experimental))
        .collect();
    for o in &manifest.aggregation_operators {
        assert_eq!(
            Some(&o.experimental),
            ops_by_name.get(o.name.as_str()),
            "experimental flag for operator {:?} disagrees with the registry",
            o.name
        );
    }
}

// ---------------------------------------------------------------------------
// Structural rules (AC5)
// ---------------------------------------------------------------------------

fn check_structural(
    kind: &str,
    name: &str,
    status: Status,
    issue: &Option<String>,
    rationale: &Option<String>,
    witness: &Option<Witness>,
    problems: &mut Vec<String>,
) {
    match status {
        Status::Scheduled => {
            if issue.as_deref().unwrap_or("").trim().is_empty() {
                problems.push(format!("{kind} {name}: scheduled without an issue"));
            }
        }
        Status::Deferred => {
            if rationale.as_deref().unwrap_or("").trim().is_empty() {
                problems.push(format!("{kind} {name}: deferred without a rationale"));
            }
        }
        Status::Implemented => {
            if witness.is_none() {
                problems.push(format!(
                    "{kind} {name}: implemented without a witness corpus case (Δ3)"
                ));
            }
        }
    }
}

#[test]
fn every_entry_has_a_home_and_implemented_entries_have_witnesses() {
    let manifest = CoverageManifest::load();
    let mut problems = Vec::new();
    for f in &manifest.functions {
        check_structural(
            "function",
            &f.name,
            f.status,
            &f.issue,
            &f.rationale,
            &f.witness,
            &mut problems,
        );
    }
    for o in &manifest.aggregation_operators {
        check_structural(
            "operator",
            &o.name,
            o.status,
            &o.issue,
            &o.rationale,
            &o.witness,
            &mut problems,
        );
    }
    for l in &manifest.language_features {
        check_structural(
            "language feature",
            &l.name,
            l.status,
            &l.issue,
            &l.rationale,
            &l.witness,
            &mut problems,
        );
        // A probe is mandatory unless the feature's surface is a `.test`
        // directive rather than a PromQL expression — then it must say so
        // and cannot claim `implemented` (nothing would prove it).
        if l.probe.is_none() {
            if l.probe_rationale.as_deref().unwrap_or("").trim().is_empty() {
                problems.push(format!(
                    "language feature {}: no probe and no probe_rationale",
                    l.name
                ));
            }
            if l.status == Status::Implemented {
                problems.push(format!(
                    "language feature {}: implemented but probe-less — unprovable",
                    l.name
                ));
            }
        }
    }
    assert!(problems.is_empty(), "\n{}", problems.join("\n"));
}

// ---------------------------------------------------------------------------
// 4: probe classification (typed probe through parse -> plan -> evaluate)
// ---------------------------------------------------------------------------

/// Builds the auto-probe for a registry function from its `arg_types`
/// (plan interfaces): `vector -> m`, `matrix -> m[5m]`, `scalar -> 1`,
/// `string -> "s"`, with the minimum required argument count.
fn build_function_probe(f: &RegistryFunction) -> String {
    let required = if f.variadic < 0 {
        f.arg_types.len().saturating_sub(1)
    } else {
        f.arg_types.len().saturating_sub(f.variadic as usize)
    };
    let args: Vec<&str> = f.arg_types[..required]
        .iter()
        .map(|t| match t.as_str() {
            "vector" => "m",
            "matrix" => "m[5m]",
            "scalar" => "1",
            "string" => "\"s\"",
            other => panic!("unknown registry arg type {other:?}"),
        })
        .collect();
    format!("{}({})", f.name, args.join(", "))
}

/// Aggregation-operator probes: parameterised operators get their
/// parameter, everything else is `op(m)`.
fn build_operator_probe(name: &str) -> String {
    match name {
        "topk" | "bottomk" | "limitk" => format!("{name}(1, m)"),
        "limit_ratio" => "limit_ratio(0.5, m)".to_string(),
        "quantile" => "quantile(0.5, m)".to_string(),
        "count_values" => "count_values(\"l\", m)".to_string(),
        _ => format!("{name}(m)"),
    }
}

/// Runs one probe through the full pipeline against a tiny synthetic
/// dataset (one series per selector, labels `{l="a", le="+Inf"}`, five
/// ascending samples inside the window — enough for range functions and
/// for `histogram_quantile`'s bucket grouping). `Ok` ⟺ the construct is
/// genuinely evaluable today.
fn probe_outcome(probe: &str) -> Result<(), String> {
    let expr = pulsus_promql::parse(probe).map_err(|e| format!("parse: {e}"))?;
    let params = PlanParams {
        start_ms: 300_000,
        end_ms: 300_000,
        step_ms: 0,
        lookback_ms: pulsus_promql::DEFAULT_LOOKBACK_MS,
    };
    let query_plan = plan(&expr, params).map_err(|e| format!("plan: {e}"))?;
    let mut data = SeriesData::new();
    for spec in &query_plan.selectors {
        let samples: Vec<Sample> = (1..=5)
            .map(|k| Sample {
                t_ms: k * 60_000,
                v: k as f64,
            })
            .collect();
        data.insert(
            spec.id,
            vec![FetchedSeries {
                fingerprint: spec.id as u64,
                labels: Labels::new(vec![
                    ("l".to_string(), "a".to_string()),
                    ("le".to_string(), "+Inf".to_string()),
                ]),
                samples,
            }],
        );
    }
    pulsus_promql::evaluate(&query_plan, &data)
        .map(|_| ())
        .map_err(|e| format!("evaluate: {e}"))
}

fn classify_probe(kind: &str, name: &str, status: Status, probe: &str, problems: &mut Vec<String>) {
    let outcome = probe_outcome(probe);
    match (status, outcome) {
        (Status::Implemented, Ok(())) => {}
        (Status::Scheduled | Status::Deferred, Err(e)) => {
            // The error kind is printed for visibility, never gated.
            println!("probe {kind} {name} ({probe}) correctly errors: {e}");
        }
        (Status::Implemented, Err(e)) => problems.push(format!(
            "{kind} {name} is marked implemented but its probe {probe:?} failed: {e} — \
             either implement it or flip the manifest status"
        )),
        (Status::Scheduled | Status::Deferred, Ok(())) => problems.push(format!(
            "{kind} {name} is marked {status:?} but its probe {probe:?} evaluated Ok — \
             the evaluator implements it; flip the manifest status (with witness) so \
             coverage cannot drift silently"
        )),
    }
}

#[test]
fn probe_classification_matches_every_manifest_status() {
    let registry = load_registry_verified();
    let manifest = CoverageManifest::load();

    let registry_by_name: BTreeMap<&str, &RegistryFunction> = registry
        .functions
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();

    let mut problems = Vec::new();
    for f in &manifest.functions {
        let reg = registry_by_name
            .get(f.name.as_str())
            .expect("identity test pins this");
        let probe = build_function_probe(reg);
        classify_probe("function", &f.name, f.status, &probe, &mut problems);
    }
    for o in &manifest.aggregation_operators {
        let probe = build_operator_probe(&o.name);
        classify_probe("operator", &o.name, o.status, &probe, &mut problems);
    }
    for l in &manifest.language_features {
        let Some(probe) = &l.probe else { continue };
        classify_probe("language feature", &l.name, l.status, probe, &mut problems);
        // Collector consistency: when the probe parses, the AST walk must
        // report the feature under the same closed-set name the corpus
        // oracle classifies with.
        if let Ok(expr) = pulsus_promql::parse(probe) {
            let constructs = collect_constructs(&expr);
            if !constructs.features.contains(&l.name) {
                problems.push(format!(
                    "language feature {}: probe {probe:?} parses but collect_constructs \
                     does not report the feature — oracle and manifest disagree",
                    l.name
                ));
            }
        }
    }
    assert!(problems.is_empty(), "\n{}", problems.join("\n"));
}

/// Pins today's `implemented` surface to exactly the M2 set (plan AC4).
/// Every later M6 issue flips entries here deliberately — this test makes
/// the flip explicit, never incidental.
#[test]
fn implemented_set_is_exactly_the_m2_surface_today() {
    let manifest = CoverageManifest::load();
    let implemented_fns: BTreeSet<&str> = manifest
        .functions
        .iter()
        .filter(|f| f.status == Status::Implemented)
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(
        implemented_fns,
        BTreeSet::from([
            "rate",
            "irate",
            "increase",
            "delta",
            "avg_over_time",
            "min_over_time",
            "max_over_time",
            "sum_over_time",
            "count_over_time",
            "histogram_quantile",
        ])
    );
    let implemented_ops: BTreeSet<&str> = manifest
        .aggregation_operators
        .iter()
        .filter(|o| o.status == Status::Implemented)
        .map(|o| o.name.as_str())
        .collect();
    assert_eq!(
        implemented_ops,
        BTreeSet::from([
            "sum", "avg", "min", "max", "count", "group", "topk", "bottomk",
        ])
    );
    let implemented_features: BTreeSet<&str> = manifest
        .language_features
        .iter()
        .filter(|f| f.status == Status::Implemented)
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(
        implemented_features,
        BTreeSet::from(["vector-matching-on-ignoring"])
    );
}

// ---------------------------------------------------------------------------
// 5: witnesses replay green through the shared runner (Δ3)
// ---------------------------------------------------------------------------

/// Which manifest dimension a witness claim belongs to — decides which
/// collected-construct set must contain the claimed name (#64 code-review
/// fix 2: a witness must *exercise* its entry, not merely pass).
#[derive(Debug, Clone, Copy)]
enum ClaimKind {
    Function,
    Operator,
    Feature,
}

impl ClaimKind {
    fn label(self) -> &'static str {
        match self {
            ClaimKind::Function => "function",
            ClaimKind::Operator => "operator",
            ClaimKind::Feature => "language feature",
        }
    }
}

#[test]
fn every_implemented_witness_exists_outside_all_buckets_and_passes() {
    let manifest = CoverageManifest::load();
    let skip_manifest = driver::SkipManifest::load();
    let ledger = driver::load_ledger();

    let mut witnesses: Vec<(ClaimKind, &str, &Witness)> = Vec::new();
    for f in &manifest.functions {
        if let Some(w) = &f.witness {
            witnesses.push((ClaimKind::Function, &f.name, w));
        }
    }
    for o in &manifest.aggregation_operators {
        if let Some(w) = &o.witness {
            witnesses.push((ClaimKind::Operator, &o.name, w));
        }
    }
    for l in &manifest.language_features {
        if let Some(w) = &l.witness {
            witnesses.push((ClaimKind::Feature, &l.name, w));
        }
    }

    // Replay each witness file once through the shared runner.
    let mut runs: BTreeMap<String, driver::runner::FileRun> = BTreeMap::new();
    let mut problems = Vec::new();
    for (kind, name, w) in &witnesses {
        let owner = format!("{} {name}", kind.label());
        let owner = &owner;
        if !runs.contains_key(&w.file) {
            let path = driver::base_dir().join("corpus").join(&w.file);
            if !path.is_file() {
                problems.push(format!("{owner}: witness file {} does not exist", w.file));
                continue;
            }
            if let Some(name) = w.file.strip_prefix("upstream/")
                && skip_manifest.entry(name).is_some()
            {
                problems.push(format!(
                    "{owner}: witness file {} is skip-manifested — a witness must lie \
                     outside every skip bucket",
                    w.file
                ));
                continue;
            }
            let text = driver::read_file(&path);
            match driver::runner::run_file(&w.file, &text) {
                Ok(run) => {
                    runs.insert(w.file.clone(), run);
                }
                Err(e) => {
                    problems.push(format!("{owner}: witness file {} failed: {e}", w.file));
                    continue;
                }
            }
        }
        let Some(run) = runs.get(&w.file) else {
            continue;
        };
        let matching: Vec<_> = run.cases.iter().filter(|c| c.query == w.query).collect();
        if matching.is_empty() {
            problems.push(format!(
                "{owner}: witness case `{}` not found in {}",
                w.query, w.file
            ));
            continue;
        }
        for case in matching {
            if !case.passed {
                problems.push(format!(
                    "{owner}: witness case {}:{} `{}` FAILED: {}",
                    w.file, case.line, case.query, case.detail
                ));
            }
            if ledger
                .iter()
                .any(|e| e.file == w.file && e.query == case.query)
            {
                problems.push(format!(
                    "{owner}: witness case `{}` is ledger-allowlisted — a witness must \
                     lie outside every divergence bucket",
                    case.query
                ));
            }
            // #64 code-review fix 2: the witness must *exercise* the
            // claimed entry — its parsed AST must contain the claimed
            // function call / aggregation operator / feature construct,
            // per the same collector the corpus oracle classifies with.
            // An unrelated passing case can no longer satisfy Δ3's dual
            // proof.
            let Some(constructs) = &case.constructs else {
                problems.push(format!(
                    "{owner}: witness case `{}` has no parsed AST — it cannot prove \
                     anything about {name}",
                    case.query
                ));
                continue;
            };
            let exercised = match kind {
                ClaimKind::Function => constructs.functions.contains(*name),
                ClaimKind::Operator => constructs.operators.contains(*name),
                ClaimKind::Feature => constructs.features.contains(*name),
            };
            if !exercised {
                problems.push(format!(
                    "{owner}: witness case `{}` does not exercise {} {name} — its AST \
                     uses functions={:?} operators={:?} features={:?}; point the witness \
                     at a case that actually contains the construct",
                    case.query,
                    kind.label(),
                    constructs.functions,
                    constructs.operators,
                    constructs.features,
                ));
            }
        }
    }
    assert!(problems.is_empty(), "\n{}", problems.join("\n"));
}

// ---------------------------------------------------------------------------
// 6: closed language-feature set + Expr-discriminant completeness (Δ4)
// ---------------------------------------------------------------------------

/// The committed closed language-feature set (plan v2 Δ4, amended by the
/// #64 adjudication on flag 7: `type-and-unit-labels` — the PROM-39
/// metadata-label semantics — is tracked as a 14th feature, scheduled
/// into M6-08 as evaluate-and-decide; amended again by #81:
/// `binop-fill-modifier` — the experimental fill/fill_left/fill_right
/// binary-operator modifiers the planner now rejects by name — is
/// tracked as a 15th feature, scheduled into M6-07, replacing its 33
/// eval-divergence ledger entries with manifest-classified
/// expected-fails) — asserted against the manifest by exact identity,
/// both directions.
const CLOSED_LANGUAGE_FEATURES: &[&str] = &[
    "set-op-and",
    "set-op-or",
    "set-op-unless",
    "vector-matching-on-ignoring",
    "group_left",
    "group_right",
    "atan2",
    "binop-fill-modifier",
    "at-modifier",
    "subquery",
    "duration-expression",
    "utf8-label-names",
    "annotations",
    "native-histogram-values",
    "type-and-unit-labels",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bucket {
    /// Covered by the manifest's `functions` dimension.
    Functions,
    /// Covered by the manifest's `aggregation_operators` dimension.
    AggregationOperators,
    /// Covered by one or more `language_features` entries.
    LanguageFeatures,
    /// Explicitly excluded from feature tracking, with a reason.
    CoreExclusion(&'static str),
}

/// Every `Expr` discriminant of the vendored parser, mapped to exactly
/// one coverage bucket (plan v2 Δ4). [`expr_discriminant_name`]'s
/// exhaustive match is the compile-time completeness guarantee.
const EXPR_MAPPING: &[(&str, Bucket)] = &[
    ("Aggregate", Bucket::AggregationOperators),
    (
        "Unary",
        Bucket::CoreExclusion(
            "M2 core (scalar negation folds in the parser; unary over a \
                               vector is Unsupported and surfaces as a value divergence, \
                               not a tracked feature)",
        ),
    ),
    // Plain arith/comparison Binary is M2 core; the tracked feature
    // entries (set-op-*, atan2, vector matching, group_left/right) cover
    // the rest of the discriminant's surface.
    ("Binary", Bucket::LanguageFeatures),
    ("Paren", Bucket::CoreExclusion("M2 core")),
    ("Subquery", Bucket::LanguageFeatures),
    ("NumberLiteral", Bucket::CoreExclusion("M2 core")),
    ("StringLiteral", Bucket::CoreExclusion("M2 core")),
    (
        "VectorSelector",
        Bucket::CoreExclusion(
            "M2 core (bare selectors; the @ modifier and UTF-8 names it \
                               can carry are tracked as at-modifier/utf8-label-names)",
        ),
    ),
    ("MatrixSelector", Bucket::CoreExclusion("M2 core")),
    ("Call", Bucket::Functions),
    (
        "Extension",
        Bucket::CoreExclusion(
            "non-standard crate extension point — the vendored parser \
                               never produces it (its own doc), so no PromQL text can \
                               reach it",
        ),
    ),
];

/// Compile-time exhaustiveness: a 12th `Expr` variant in a parser bump
/// fails compilation right here, forcing the mapping (and the closed
/// feature set) to be revisited.
fn expr_discriminant_name(expr: &Expr) -> &'static str {
    match expr {
        Expr::Aggregate(_) => "Aggregate",
        Expr::Unary(_) => "Unary",
        Expr::Binary(_) => "Binary",
        Expr::Paren(_) => "Paren",
        Expr::Subquery(_) => "Subquery",
        Expr::NumberLiteral(_) => "NumberLiteral",
        Expr::StringLiteral(_) => "StringLiteral",
        Expr::VectorSelector(_) => "VectorSelector",
        Expr::MatrixSelector(_) => "MatrixSelector",
        Expr::Call(_) => "Call",
        Expr::Extension(_) => "Extension",
    }
}

#[test]
fn language_feature_set_is_closed_and_matches_by_exact_identity() {
    let manifest = CoverageManifest::load();
    let manifest_features: BTreeSet<&str> = manifest
        .language_features
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(
        manifest_features.len(),
        manifest.language_features.len(),
        "duplicate language-feature names in function-coverage.json"
    );
    let closed: BTreeSet<&str> = CLOSED_LANGUAGE_FEATURES.iter().copied().collect();
    assert_eq!(
        closed.len(),
        CLOSED_LANGUAGE_FEATURES.len(),
        "duplicate names in CLOSED_LANGUAGE_FEATURES"
    );
    assert_eq!(
        manifest_features, closed,
        "language_features must match the closed set by exact identity, both directions"
    );
}

#[test]
fn every_expr_discriminant_is_mapped_exactly_once() {
    let names: Vec<&str> = EXPR_MAPPING.iter().map(|(n, _)| *n).collect();
    let unique: BTreeSet<&str> = names.iter().copied().collect();
    assert_eq!(unique.len(), names.len(), "duplicate discriminant mapping");
    assert_eq!(
        names.len(),
        11,
        "the vendored parser has exactly 11 Expr discriminants (compile-time checked by \
         expr_discriminant_name); a parser bump that adds one must extend EXPR_MAPPING"
    );

    // Bind the mapping's names to the real enum through the exhaustive
    // match: every discriminant reachable from parseable text must appear
    // in the mapping.
    for (query, expected_name) in [
        ("sum(m)", "Aggregate"),
        ("-m", "Unary"),
        ("m + m", "Binary"),
        ("(m)", "Paren"),
        ("m[5m:1m]", "Subquery"),
        ("1", "NumberLiteral"),
        ("\"s\"", "StringLiteral"),
        ("m", "VectorSelector"),
        ("rate(m[5m])", "Call"),
    ] {
        let expr = pulsus_promql::parse(query)
            .unwrap_or_else(|e| panic!("discriminant sample {query:?} must parse: {e}"));
        let name = expr_discriminant_name(&expr);
        assert_eq!(name, expected_name, "sample {query:?}");
        assert!(
            unique.contains(name),
            "discriminant {name} (from {query:?}) is not in EXPR_MAPPING"
        );
    }
    // MatrixSelector only appears nested (a bare `m[5m]` at the root is a
    // MatrixSelector expression — parseable directly).
    let expr = pulsus_promql::parse("m[5m]").expect("matrix selector parses");
    assert_eq!(expr_discriminant_name(&expr), "MatrixSelector");
    // Extension is unreachable from text (the parser never produces it) —
    // its mapping entry records the exclusion rationale.
    assert!(unique.contains("Extension"));
}
