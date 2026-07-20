//! Issue #64 (M6-01): the shared promqltest driver — a native replayer for
//! upstream Prometheus `.test` files against the pure
//! `parse -> plan -> evaluate` pipeline, plus the committed-artifact
//! loaders (coverage manifest, vendored-corpus integrity manifest,
//! skip-manifest, eval-divergence ledger) both test binaries
//! (`promqltest_corpus.rs`, `function_coverage.rs`) `#[path]`-include so
//! witnesses replay through the exact same runner (plan v2 Δ3).
//!
//! Everything here is hermetic: no ClickHouse, no network — the store
//! (`store.rs`) stands in for the fetch layer only.
//!
//! Shared-test-module convention: like `pulsus-config`'s and
//! `pulsus-server`'s `tests/support` modules, this module is compiled into
//! more than one test binary, each using a different subset — `dead_code`
//! is allowed here for exactly that reason, not to hide genuinely unused
//! logic.
#![allow(dead_code)]

pub mod grammar;
pub mod histogram_literal;
pub mod runner;
pub mod series;
pub mod store;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// `crates/pulsus-promql/tests/promqltest` — every committed driver
/// artifact lives under here (plan v2 Δ1: fully namespaced, zero contact
/// with the #29 parser corpus in `tests/corpus/`).
pub fn base_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("promqltest")
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub fn read_file(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

// ---------------------------------------------------------------------------
// Coverage manifest (coverage/function-coverage.json)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Implemented,
    Scheduled,
    Deferred,
    /// M7-A5b-i (issue #124): semantics are implemented and proven by
    /// hermetic in-crate unit tests, but a CORPUS witness cannot exist
    /// yet because the construct's ONLY corpus-executable form is gated
    /// behind a still-deferred directive (the pattern's original users,
    /// the 5 native-histogram accessors, were unblocked and flipped to
    /// `implemented` by M7-A6's `{{…}}` grammar landing — the status
    /// itself stays available for the next construct in the same
    /// shape). The probe classifier requires the probe to evaluate `Ok`
    /// exactly like [`Status::Implemented`]; the structural check
    /// requires a `rationale` naming the blocking gap and REJECTS a
    /// corpus `witness` (any witness claimed under this status would be
    /// fake by construction). `function_coverage.rs`'s pinned set for
    /// this status is a CLOSED drift guard (currently empty) so no entry
    /// can adopt it without a deliberate test update.
    ImplementedUnitWitnessed,
}

/// A pointer at the concrete corpus case that proves an `implemented`
/// entry semantically (plan v2 Δ3: `plan() == Ok` alone never yields
/// `implemented`).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Witness {
    /// Relative to `tests/promqltest/corpus/`, e.g.
    /// `proof/m2_functions.test`.
    pub file: String,
    /// The exact query text of the witness eval case.
    pub query: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct FunctionEntry {
    pub name: String,
    pub experimental: bool,
    pub status: Status,
    #[serde(default)]
    pub issue: Option<String>,
    #[serde(default)]
    pub rationale: Option<String>,
    #[serde(default)]
    pub witness: Option<Witness>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AggregationOperatorEntry {
    pub name: String,
    pub experimental: bool,
    pub status: Status,
    #[serde(default)]
    pub issue: Option<String>,
    #[serde(default)]
    pub rationale: Option<String>,
    #[serde(default)]
    pub witness: Option<Witness>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct LanguageFeatureEntry {
    pub name: String,
    pub status: Status,
    #[serde(default)]
    pub issue: Option<String>,
    #[serde(default)]
    pub rationale: Option<String>,
    /// A minimal expression exercising the feature, run through
    /// `parse -> plan -> evaluate` by the probe classifier. `None` only
    /// for features whose surface is a `.test` directive rather than a
    /// PromQL expression (`annotations`, `native-histogram-values`) —
    /// those must carry `probe_rationale` and a non-`implemented` status;
    /// their enforcement lives in the skip-manifest drift test.
    #[serde(default)]
    pub probe: Option<String>,
    #[serde(default)]
    pub probe_rationale: Option<String>,
    #[serde(default)]
    pub witness: Option<Witness>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct CoverageManifest {
    pub prometheus_tag: String,
    pub functions: Vec<FunctionEntry>,
    pub aggregation_operators: Vec<AggregationOperatorEntry>,
    pub language_features: Vec<LanguageFeatureEntry>,
}

impl CoverageManifest {
    pub fn load() -> Self {
        let path = base_dir().join("coverage").join("function-coverage.json");
        serde_json::from_str(&read_file(&path))
            .unwrap_or_else(|e| panic!("invalid {}: {e}", path.display()))
    }

    fn status_of(&self, kind: ConstructKind, name: &str) -> Option<Status> {
        match kind {
            ConstructKind::Function => self
                .functions
                .iter()
                .find(|f| f.name == name)
                .map(|f| f.status),
            ConstructKind::AggregationOperator => self
                .aggregation_operators
                .iter()
                .find(|o| o.name == name)
                .map(|o| o.status),
            ConstructKind::LanguageFeature => self
                .language_features
                .iter()
                .find(|f| f.name == name)
                .map(|f| f.status),
        }
    }

    /// The expected-failure oracle (plan interfaces): a failing case is
    /// *expected* to fail iff at least one construct it uses is
    /// `scheduled`/`deferred`. Returns the first such construct as the
    /// classification reason. A construct name the manifest doesn't know
    /// at all is a hard error — the coverage-identity test guarantees the
    /// manifest is complete, so an unknown name means the collector and
    /// the manifest disagree.
    pub fn classify_expected_fail(&self, constructs: &runner::Constructs) -> Option<String> {
        let lookups = [
            (ConstructKind::Function, &constructs.functions),
            (ConstructKind::AggregationOperator, &constructs.operators),
            (ConstructKind::LanguageFeature, &constructs.features),
        ];
        for (kind, names) in lookups {
            for name in names {
                let status = self.status_of(kind, name).unwrap_or_else(|| {
                    panic!(
                        "construct {name:?} ({kind:?}) collected from a query AST is not in \
                         function-coverage.json — collector and manifest disagree"
                    )
                });
                if matches!(status, Status::Scheduled | Status::Deferred) {
                    return Some(format!("{kind:?} {name} is {status:?}"));
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ConstructKind {
    Function,
    AggregationOperator,
    LanguageFeature,
}

// ---------------------------------------------------------------------------
// Vendored-registry artifacts (coverage/registry-v3.13.json + manifest)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RegistryFunction {
    pub name: String,
    pub arg_types: Vec<String>,
    pub variadic: i64,
    pub return_type: String,
    pub experimental: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RegistryAggregationOperator {
    pub name: String,
    pub experimental: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Registry {
    pub prometheus_tag: String,
    pub prometheus_sha: String,
    pub functions: Vec<RegistryFunction>,
    pub aggregation_operators: Vec<RegistryAggregationOperator>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RegistryManifest {
    pub prometheus_tag: String,
    pub prometheus_sha: String,
    pub sha256: String,
    pub function_count: usize,
    pub experimental_function_count: usize,
    pub aggregation_operator_count: usize,
}

/// Loads the vendored registry, first re-verifying its bytes against
/// `registry-manifest.json` (the #29 F1 integrity pattern: a drifted
/// committed registry fails loudly before any coverage check runs).
pub fn load_registry_verified() -> Registry {
    let dir = base_dir().join("coverage");
    let registry_path = dir.join("registry-v3.13.json");
    let manifest_path = dir.join("registry-manifest.json");
    let registry_text = read_file(&registry_path);
    let manifest: RegistryManifest = serde_json::from_str(&read_file(&manifest_path))
        .unwrap_or_else(|e| panic!("invalid {}: {e}", manifest_path.display()));

    let sha = sha256_hex(registry_text.as_bytes());
    assert_eq!(
        sha, manifest.sha256,
        "registry-v3.13.json bytes do not match registry-manifest.json — re-run \
         coverage/extract-registry.py and recommit both files together"
    );

    let registry: Registry = serde_json::from_str(&registry_text)
        .unwrap_or_else(|e| panic!("invalid {}: {e}", registry_path.display()));
    assert_eq!(registry.prometheus_tag, manifest.prometheus_tag);
    assert_eq!(registry.prometheus_sha, manifest.prometheus_sha);
    assert_eq!(
        registry.functions.len(),
        manifest.function_count,
        "registry function count drifted from registry-manifest.json"
    );
    assert_eq!(
        registry.functions.iter().filter(|f| f.experimental).count(),
        manifest.experimental_function_count,
        "registry experimental-function count drifted from registry-manifest.json"
    );
    assert_eq!(
        registry.aggregation_operators.len(),
        manifest.aggregation_operator_count,
        "registry aggregation-operator count drifted from registry-manifest.json"
    );
    registry
}

// ---------------------------------------------------------------------------
// Upstream-corpus integrity manifest (corpus/upstream/upstream-manifest.json)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Deserialize)]
pub struct UpstreamFileEntry {
    pub name: String,
    pub sha256: String,
    pub lines: usize,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct UpstreamExclusion {
    pub name: String,
    pub reason: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct UpstreamManifest {
    pub prometheus_tag: String,
    pub prometheus_sha: String,
    pub files: Vec<UpstreamFileEntry>,
    pub excluded: Vec<UpstreamExclusion>,
}

/// Verifies every vendored upstream file against the integrity manifest
/// (SHA-256 + line count, both directions: a file on disk missing from
/// the manifest is as fatal as a manifest entry missing on disk), then
/// returns the manifest plus each file's contents keyed by name.
pub fn load_upstream_verified() -> (UpstreamManifest, BTreeMap<String, String>) {
    let dir = base_dir().join("corpus").join("upstream");
    let manifest_path = dir.join("upstream-manifest.json");
    let manifest: UpstreamManifest = serde_json::from_str(&read_file(&manifest_path))
        .unwrap_or_else(|e| panic!("invalid {}: {e}", manifest_path.display()));

    let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("failed to list {}: {e}", dir.display()))
        .map(|entry| entry.expect("readable dir entry").file_name())
        .filter_map(|name| {
            let name = name.to_string_lossy().to_string();
            name.ends_with(".test").then_some(name)
        })
        .collect();
    on_disk.sort();

    let mut in_manifest: Vec<String> = manifest.files.iter().map(|f| f.name.clone()).collect();
    in_manifest.sort();
    assert_eq!(
        on_disk, in_manifest,
        "the .test files on disk under corpus/upstream/ must exactly match \
         upstream-manifest.json (both directions)"
    );

    for excluded in &manifest.excluded {
        assert!(
            !on_disk.contains(&excluded.name),
            "{} is recorded as excluded ({}) but is present on disk",
            excluded.name,
            excluded.reason
        );
    }

    let mut contents = BTreeMap::new();
    for entry in &manifest.files {
        let path = dir.join(&entry.name);
        let text = read_file(&path);
        let sha = sha256_hex(text.as_bytes());
        assert_eq!(
            sha, entry.sha256,
            "{} bytes do not match upstream-manifest.json — the vendored corpus \
             drifted; re-fetch at the pinned SHA and recommit both together",
            entry.name
        );
        let lines = text.lines().count();
        assert_eq!(
            lines, entry.lines,
            "{} line count does not match upstream-manifest.json",
            entry.name
        );
        contents.insert(entry.name.clone(), text);
    }
    (manifest, contents)
}

// ---------------------------------------------------------------------------
// Skip-manifest (corpus/skip-manifest.json) — plan v2 Δ2
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BlockingDirective {
    /// A `grammar::DeferredDirective::name()` string.
    pub directive: String,
    /// The concrete issue that activates this directive family
    /// (`M6-08` for annotation surfaces, `#22` for native-histogram
    /// surfaces — plan v2 Δ2's homes).
    pub activation_issue: String,
}

/// A non-directive skip lever (issue #124, M7-A6 adjudication): the file
/// has ZERO deferred directives (the driver could execute it), but
/// replaying it surfaces a genuine gap outside any tracked directive —
/// new syntax the parser doesn't have, an unimplemented annotation, etc.
/// `reason` is a short human description; `activation_issue` the
/// follow-up tracking it. Unlike [`BlockingDirective`], this carries no
/// drift check (there is no directive presence to re-scan) — periodic
/// re-review is manual.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ManualSkip {
    pub reason: String,
    pub activation_issue: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SkipEntry {
    pub file: String,
    #[serde(default)]
    pub blocking_directives: Vec<BlockingDirective>,
    #[serde(default)]
    pub manual_skip: Option<ManualSkip>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SkipManifest {
    pub files: Vec<SkipEntry>,
}

impl SkipManifest {
    pub fn load() -> Self {
        let path = base_dir().join("corpus").join("skip-manifest.json");
        serde_json::from_str(&read_file(&path))
            .unwrap_or_else(|e| panic!("invalid {}: {e}", path.display()))
    }

    pub fn entry(&self, file: &str) -> Option<&SkipEntry> {
        self.files.iter().find(|e| e.file == file)
    }
}

// ---------------------------------------------------------------------------
// Eval-divergence ledger (corpus/eval-divergences.jsonl) — plan v2 Δ1/Δ2
// ---------------------------------------------------------------------------

/// One human-classified residual divergence: a case that fails for a
/// reason *not* attributable to a scheduled/deferred manifest construct
/// (parser gaps the AST walk can't see, cross-implementation error-text
/// wording, semantic gaps in implemented constructs). Distinct file and
/// schema from the #29 parser ledger (`tests/corpus/
/// expected-divergences.jsonl`), which is untouched.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct LedgerEntry {
    /// Relative to `corpus/`, e.g. `upstream/duration_expression.test`.
    pub file: String,
    /// 1-based line of the `eval` directive (stable: the vendored file is
    /// byte-pinned by the integrity manifest).
    pub line: usize,
    /// The exact query text — a guard against line drift.
    pub query: String,
    pub construct: String,
    pub reason: String,
}

pub fn load_ledger() -> Vec<LedgerEntry> {
    let path = base_dir().join("corpus").join("eval-divergences.jsonl");
    read_file(&path)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            serde_json::from_str(l)
                .unwrap_or_else(|e| panic!("invalid eval-divergences.jsonl line {l:?}: {e}"))
        })
        .collect()
}
