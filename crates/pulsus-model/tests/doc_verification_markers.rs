//! Issue #502 — the register of verification markers in `docs/`, and what
//! each one's evidence is actually made of.
//!
//! # What this registry counts, and what it does not
//!
//! It counts statements in `docs/**/*.md` that carry one of two marker
//! phrases. That is not the defect this issue was opened for. The defect
//! is a statement pointing at a check that cannot reach the case, and a
//! statement can do that while carrying no marker word at all — in which
//! case nothing here sees it.
//!
//! Two statements in this repository were exactly that, and both are named
//! below as witnesses. Each was false, each carried no marker, and each was
//! corrected on issue #502:
//!
//!   * `docs/api.md` §8.1's metrics-envelope bullet, which claimed the
//!     `/api/metrics/*` aliases serve a matrix/vector query envelope while
//!     §4.4 of the same file said they serve the native `{series, metrics}`
//!     body, and while `api_conformance` asserted the native body live on
//!     those four paths.
//!   * `docs/schemas.md` §4.2's partiality paragraph, which described a
//!     `partial` response field that #464 removed.
//!
//! Neither would have been found by widening [`MARKER_RE`]. Finding that
//! class needs a different instrument, and this is not it.
//! [`the_blind_spot_witnesses_are_outside_the_population`] is what keeps
//! those two examples real: each key must still occur exactly once, must
//! still not match the marker pattern, and must still not be a [`MARKERS`]
//! entry. A named example rots; a named example a check keeps honest does
//! not.
//!
//! # What an entry's `evidence` claims, and what it does not
//!
//! [`Evidence`] records where the artefact behind a sentence gets its
//! **expectation** — from the reference, from another literal in our own
//! tree, or nowhere that the search below found. It is a provenance
//! record, not a truth record. It cannot tell a true "verified live" from
//! a false one. It can tell a claim about the reference that is backed
//! only by a comparison between two of our own files from one backed by a
//! reference capture, and that is the whole of what it does.
//!
//! The evidence search was run for the [`Subject::Reference`] entries
//! only. A [`Subject::Internal`] entry carries [`Evidence::None`] because
//! no search was made for it, not because a search came back empty, and
//! the pinned constant counts `Reference` entries alone.
//!
//! # The sweep, with its literal scope
//!
//! ```text
//! $ git ls-files 'docs/*.md' 'docs/**/*.md' | wc -l
//! 30
//! $ git grep -InE '[Vv]erified live|[Vv]erified against' \
//!       -- 'docs/*.md' 'docs/**/*.md' | wc -l
//! 22
//! ```
//!
//! Thirty tracked Markdown documents under `docs/`, twenty-two marker
//! lines, measured on `8f3d0c6d`. `git ls-files 'docs/**'` answers **41**
//! instead, because it counts diagrams and data files the sweep never
//! reads; that is a different scope and not this one. Of the 22, seven
//! assert something about the reference and four of those seven have no
//! reference-derived artefact behind them —
//! [`REFERENCE_MARKERS_NOT_BACKED_BY_A_REFERENCE_ARTEFACT`].
//!
//! Widening the pattern, or covering the same claims where they appear in
//! Rust doc comments, is separate work. Both are named here so the
//! boundary is written down rather than assumed:
//!
//! ```text
//! $ git grep -InE '[Vv]erified live|[Vv]erified against' -- '*.md' | wc -l
//! 23
//! $ git grep -InE '[Vv]erified live|[Vv]erified against' -- . | wc -l
//! 78
//! ```
//!
//! # The walk this test performs, versus the sweep above
//!
//! The test walks the filesystem for `*.md` under `docs/` rather than
//! shelling out to `git`, so it needs no git repository to run. In a clean
//! checkout the two sets are identical (both 30 on `8f3d0c6d`). They part
//! company only in a working tree holding an untracked draft, where the
//! walk sees the draft and the sweep does not — in which case a marker in
//! that draft has to be registered here or removed from the draft.

use std::path::{Path, PathBuf};

/// The literal sweep. Changing it changes what "a marker" means, and is a
/// deliberate act — every count in this file's module doc was measured
/// with this pattern and no other.
const MARKER_RE: &str = r"[Vv]erified live|[Vv]erified against";

/// What the sentence is ABOUT. Only a [`Subject::Reference`] sentence can
/// be wrong in the way issue #502 is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Subject {
    Reference,
    Internal,
}

/// Where the artefact behind the sentence gets its EXPECTATION.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Evidence {
    /// Captured from, or compared live against, the reference. The only
    /// kind that can detect a sentence wrong about the reference.
    FromReference(&'static str),
    /// Compares the document to a literal in our own tree. Detects drift
    /// between two of our files and nothing else.
    FromOurTree(&'static str),
    /// No artefact is recorded here. For a [`Subject::Reference`] entry
    /// that means the search recorded in this module's doc comment found
    /// none — a property of that search, not of the world. For a
    /// [`Subject::Internal`] entry it means no search was made.
    None,
}

/// One statement in a `docs/**/*.md` file carrying a verification marker.
#[derive(Debug)]
struct Marker {
    /// Workspace-relative path.
    file: &'static str,
    /// A literal substring occurring EXACTLY ONCE in `file`, on a line the
    /// marker pattern matches. Uniqueness is the identity — a `contains`
    /// would be satisfied by any other sentence in the file, which is how
    /// a count with no identifier gets attributed to the wrong line.
    key: &'static str,
    subject: Subject,
    evidence: Evidence,
}

/// Every marker line in the sweep's scope, one entry each.
///
/// Ordered by file and then by position, the same order the sweep prints.
const MARKERS: &[Marker] = &[
    Marker {
        file: "docs/116.md",
        key: "Verified against source (file:line below), not docs.",
        // "source" here is OUR source: the entry's own list of verified
        // facts cites `docs/architecture.md` and `crates/pulsus-write/src`.
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/api.md",
        key: "both exemptions verified against a",
        subject: Subject::Reference,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/api.md",
        key: "vector-matching modifiers (semantics oracle-verified against `grafana/loki:3.4.2`)",
        subject: Subject::Reference,
        evidence: Evidence::FromOurTree(
            "crates/pulsus-read/tests/logqltest/corpus/differential_vector_matching.test",
        ),
    },
    Marker {
        file: "docs/api.md",
        key: "issue #335 — verified against grafana/tempo:3.0.2, tightest first",
        subject: Subject::Reference,
        evidence: Evidence::FromReference("crates/pulsus-traceql/tests/accept_surface.rs"),
    },
    Marker {
        file: "docs/api.md",
        key: "under a **type gate** (verified against grafana/tempo:3.0.2)",
        subject: Subject::Reference,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/api.md",
        // ISSUE #510 OWNS THIS SENTENCE and the fixture behind it. The
        // artefact named here compares by() group-attribute rendering
        // live against the reference, which is what `FromReference`
        // records — provenance, not coverage. Its fixture list reaches
        // four of the five per-type arms and not the numeric-attribute
        // one; that gap is #510's subject and is precisely the kind of
        // thing this registry cannot see (see the module doc). If #510's
        // rewrite drops the marker words, this entry goes with them and
        // the constant below is unchanged, because the entry is not one
        // of the four.
        key: "**rendered by its TraceQL type** (verified live against Tempo v3.0.2)",
        subject: Subject::Reference,
        evidence: Evidence::FromReference(
            "crates/pulsus-read/tests/traces_search_grouping_differential.rs",
        ),
    },
    Marker {
        file: "docs/architecture.md",
        key: "verified against a committed SHA-256 manifest",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/architecture.md",
        key: "checksum-verified against a committed manifest, replayed natively",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/architecture.md",
        key: "verified against a client-computed expected shard roster**",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/benchmarks/logs-differential-ledger.md",
        key: "longer re-verified against the reference on any run",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/benchmarks/logs-differential-ledger.md",
        key: "following `direction` — verified against ClickHouse directly",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/benchmarks/m1-logs-late-hydration.md",
        key: "verified live at every committed breadth",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/benchmarks/m1-logs-read-path.md",
        key: "Verified live in this session (podman, ClickHouse 24.8):",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/benchmarks/m1-logs-read-path.md",
        key: "Verified live in this session:",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/benchmarks/m1-logs-read-path.md",
        key: "verified live in this session (podman, four ClickHouse 24.8",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/benchmarks/m2-metrics-label-resolution.md",
        key: "Verified live in this session (podman, ClickHouse 24.8): all 48 cells",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/benchmarks/traces-differential-ledger.md",
        key: "one we should have and do not — and re-verified live against",
        subject: Subject::Reference,
        evidence: Evidence::FromReference(
            "crates/pulsus-read/tests/traces_metrics_filter_accept.rs",
        ),
    },
    Marker {
        file: "docs/features.md",
        key: "vector-matching modifiers, semantics oracle-verified against `grafana/loki:3.4.2`",
        subject: Subject::Reference,
        evidence: Evidence::FromOurTree(
            "crates/pulsus-read/tests/logqltest/corpus/differential_vector_matching.test",
        ),
    },
    Marker {
        file: "docs/features.md",
        // The marked clause is about OUR fixture being re-run in CI, not
        // about what the reference does; the reference claims in that
        // paragraph carry no marker. A reader who disagrees with this
        // reading should say so — moving it to `Reference` with
        // `FromReference("crates/pulsus-traceql/tests/accept_surface.rs")`
        // leaves the pinned count below unchanged either way.
        key: "re-verified live in CI, fail-closed",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/features.md",
        key: "checksum-verified against a committed manifest) replayed against PulsusDB in CI",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/releasing.md",
        key: "this chart version was verified against, not",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
    Marker {
        file: "docs/schemas.md",
        key: "verified against a **client-computed expected shard roster**",
        subject: Subject::Internal,
        evidence: Evidence::None,
    },
];

/// `Subject::Reference` entries whose evidence is not `FromReference`.
///
/// Lowering it requires adding a reference-derived artefact, which is why
/// it is a constant somebody has to edit on purpose rather than a number
/// derived and then forgotten. The four, at the time of writing:
///
/// ```text
///   docs/api.md   "both exemptions verified against a"              None
///   docs/api.md   "under a **type gate** (verified against ...)"    None
///   docs/api.md   LogQL vector matching                             FromOurTree
///   docs/features.md  LogQL vector matching                         FromOurTree
/// ```
const REFERENCE_MARKERS_NOT_BACKED_BY_A_REFERENCE_ARTEFACT: usize = 4;

/// A statement of the defect class that this registry cannot see. Held by
/// [`the_blind_spot_witnesses_are_outside_the_population`] so the module
/// doc's example cannot quietly stop being one.
#[derive(Debug)]
struct Witness {
    file: &'static str,
    key: &'static str,
}

const WITNESSES: &[Witness] = &[
    Witness {
        file: "docs/api.md",
        key: "byte-identical to `/api/traces/v1/metrics/*` for the same request",
    },
    Witness {
        file: "docs/schemas.md",
        key: "the response OMITS `metrics.completedJobs`",
    },
];

fn repo_root() -> PathBuf {
    // `crates/pulsus-model` -> repo root.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the manifest dir is two levels below the repo root")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

/// Every `*.md` under `docs/`, workspace-relative, sorted.
fn doc_files() -> Vec<String> {
    let root = repo_root();
    let mut out = Vec::new();
    let mut stack = vec![root.join("docs")];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}"));
        for entry in entries {
            let entry = entry.unwrap_or_else(|e| panic!("dir entry under {dir:?}: {e}"));
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "md") {
                let rel = path
                    .strip_prefix(&root)
                    .expect("under the repo root")
                    .to_string_lossy()
                    .into_owned();
                out.push(rel);
            }
        }
    }
    out.sort();
    out
}

fn marker_re() -> regex::Regex {
    regex::Regex::new(MARKER_RE).expect("MARKER_RE is a valid pattern")
}

/// Every `(file, line number, line text)` in scope whose text matches the
/// marker pattern.
fn marker_lines() -> Vec<(String, usize, String)> {
    let re = marker_re();
    let mut out = Vec::new();
    for file in doc_files() {
        for (i, line) in read(&file).lines().enumerate() {
            if re.is_match(line) {
                out.push((file.clone(), i + 1, line.to_string()));
            }
        }
    }
    out
}

/// Every marker-bearing line in `docs/` is registered, and an
/// unregistered one is named by file, line and text — so the count is
/// never the only identifier a reader gets.
#[test]
fn every_marker_line_is_registered() {
    let found = marker_lines();
    let mut unregistered = Vec::new();
    for (file, line, text) in &found {
        let registered = MARKERS
            .iter()
            .any(|m| m.file == file && text.contains(m.key));
        if !registered {
            let excerpt: String = text.chars().take(160).collect();
            unregistered.push(format!("{file}:{line}: {excerpt}"));
        }
    }
    assert!(
        unregistered.is_empty(),
        "unregistered marker line(s):\n{}\n\
         Add a `Marker` entry with its `subject` and `evidence`, or remove the marker.",
        unregistered.join("\n")
    );
    assert_eq!(
        found.len(),
        MARKERS.len(),
        "the registry has {} entries and the sweep found {} marker lines; \
         a registry entry whose line is gone is as wrong as a line with no entry.\n\
         lines: {:?}",
        MARKERS.len(),
        found.len(),
        found
            .iter()
            .map(|(f, l, _)| format!("{f}:{l}"))
            .collect::<Vec<_>>()
    );
}

/// Every registry key is unique in its file and lies on a marker line.
///
/// Uniqueness is what makes the key an identifier. A key that occurs twice
/// names two lines, and a key that occurs on a line the pattern does not
/// match names something the sweep never counted.
#[test]
fn every_registry_key_is_unique_and_on_a_marker_line() {
    let re = marker_re();
    for m in MARKERS {
        let text = read(m.file);
        assert_eq!(
            text.matches(m.key).count(),
            1,
            "{}: key is not unique — {:?} occurs {} times",
            m.file,
            m.key,
            text.matches(m.key).count()
        );
        let line = text
            .lines()
            .find(|l| l.contains(m.key))
            .expect("the key occurs, so some line carries it");
        assert!(
            re.is_match(line),
            "{}: key {:?} is on a line the marker pattern does not match",
            m.file,
            m.key
        );
    }
}

/// Every artefact an entry names exists.
///
/// It does not check that the artefact still covers the claim — nothing
/// here can. It checks that a rename or a deletion does not leave a
/// pointer to nothing, which is how an evidence column becomes decoration.
#[test]
fn every_named_artefact_exists() {
    for m in MARKERS {
        let artefact = match m.evidence {
            Evidence::FromReference(p) | Evidence::FromOurTree(p) => p,
            Evidence::None => continue,
        };
        let path = repo_root().join(artefact);
        assert!(
            path.exists(),
            "{}: artefact {artefact} is gone (registry key {:?})",
            m.file,
            m.key
        );
    }
}

/// The count of reference claims not backed by a reference-derived
/// artefact is pinned.
///
/// Promoting an entry to [`Evidence::FromReference`] without adding a
/// reference-derived check moves this number, and the constant then has to
/// be edited — which is the deliberate act the gate exists to force.
#[test]
fn the_unbacked_reference_marker_count_is_pinned() {
    let unbacked: Vec<&Marker> = MARKERS
        .iter()
        .filter(|m| m.subject == Subject::Reference)
        .filter(|m| !matches!(m.evidence, Evidence::FromReference(_)))
        .collect();
    assert_eq!(
        unbacked.len(),
        REFERENCE_MARKERS_NOT_BACKED_BY_A_REFERENCE_ARTEFACT,
        "reference claims with no reference-derived artefact: {:?}",
        unbacked
            .iter()
            .map(|m| format!("{}: {:?}", m.file, m.key))
            .collect::<Vec<_>>()
    );
}

/// The module doc's two blind-spot examples are still examples.
///
/// Each witness must (a) occur exactly once in its file, (b) sit on a line
/// the marker pattern does NOT match, and (c) not be a registry entry. The
/// three together say the same thing three ways: this sentence is a member
/// of the defect class and it is outside the population this file counts.
///
/// What it cannot hold is that the witnesses are still FALSE — issue #502
/// made them true. The false half is in the issue trail and in the ledger.
#[test]
fn the_blind_spot_witnesses_are_outside_the_population() {
    let re = marker_re();
    for w in WITNESSES {
        let text = read(w.file);
        assert_eq!(
            text.matches(w.key).count(),
            1,
            "{}: witness key {:?} not found — the module doc cites an example that no longer \
             exists",
            w.file,
            w.key
        );
        let line = text
            .lines()
            .find(|l| l.contains(w.key))
            .expect("the key occurs, so some line carries it");
        assert!(
            !re.is_match(line),
            "{}: the witness now matches MARKER_RE — it has moved into the counted population \
             and the module doc needs a witness that has not",
            w.file
        );
        assert!(
            !MARKERS.iter().any(|m| m.file == w.file && m.key == w.key),
            "{}: the witness is now a MARKERS entry — it is no longer outside the population",
            w.file
        );
    }
}
