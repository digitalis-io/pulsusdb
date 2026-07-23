//! The byte-frozen golden-corpus gate (issue #56, plan v3 F6/F7):
//!
//! 1. **Drift** — `read_dir` contents must equal `MANIFEST` exactly, and
//!    every `.traceql` needs exactly one `.golden` sibling. Every test
//!    that reads the corpus calls this first.
//! 2. **Golden** — each case's parse outcome (`{:#?}` of the AST or the
//!    error) must byte-match its committed `.golden`.
//! 3. **Round-trip oracle (AC1)** — for every accept case:
//!    `parse(ast.to_string()) == ast` (parse once, render the *parsed*
//!    AST with `Display`, reparse the rendering, compare ASTs).
//! 4. **Token coverage (F6a)** — the set of `TokenKind` discriminants
//!    observed across all accept inputs must equal the declared
//!    `EXPECTED_ACCEPT_TOKENS` set.
//! 5. **Registry mapping (F6b)** — `unsupported/` cases map one-to-one
//!    onto `BOUNDARY_CONSTRUCTS`, both directions.
//! 6. **Dual-role tokens (F6c)** — each context-dependent token has one
//!    accept and one boundary case, enumerated by name.
//!
//! Std-only: no serde, no checksums — the drift + golden + coverage
//! tests are the freeze (see `corpus/PROVENANCE.md`).

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use pulsus_traceql::{
    AggregateOp, BOUNDARY_CONSTRUCTS, ComparisonOp, Field, FieldExpr, Intrinsic, MetricFn,
    PipelineStage, Query, SpansetExpr, StructuralOp, TokenKind, TraceQlError, parse,
};

const SUBDIRS: [&str; 4] = ["accept", "reject", "unsupported", "grafana"];

fn corpus_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("corpus")
}

fn manifest_cases() -> BTreeSet<String> {
    let path = corpus_root().join("MANIFEST");
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

fn discovered_cases() -> BTreeSet<String> {
    let root = corpus_root();
    let mut cases = BTreeSet::new();
    for sub in SUBDIRS {
        let dir = root.join(sub);
        let mut inputs = BTreeSet::new();
        let mut goldens = BTreeSet::new();
        let entries = fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("failed to read_dir {}: {e}", dir.display()));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|e| panic!("failed to read a dir entry in {sub}/: {e}"))
                .path();
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_else(|| panic!("non-UTF-8 corpus file name: {}", path.display()))
                .to_string();
            match path.extension().and_then(|e| e.to_str()) {
                Some("traceql") => {
                    inputs.insert(stem);
                }
                Some("golden") => {
                    goldens.insert(stem);
                }
                _ => panic!("unexpected file in the corpus: {}", path.display()),
            }
        }
        assert_eq!(
            inputs, goldens,
            "corpus drift in {sub}/: every .traceql needs exactly one .golden sibling"
        );
        cases.extend(inputs.into_iter().map(|stem| format!("{sub}/{stem}")));
    }
    cases
}

/// The drift gate — every test that reads the corpus calls this first,
/// so a truncated/renamed/orphaned corpus fails loudly rather than
/// silently changing the pass set.
fn verify_corpus_layout() -> BTreeSet<String> {
    let manifest = manifest_cases();
    let discovered = discovered_cases();
    assert_eq!(
        manifest, discovered,
        "corpus drift: MANIFEST and directory contents differ"
    );
    assert!(!manifest.is_empty(), "the corpus must not be empty");
    manifest
}

/// Reads a case input, stripping the single trailing newline the file
/// format mandates (PROVENANCE.md) — queries never end in `\n`.
fn read_input(case: &str) -> String {
    let path = corpus_root().join(format!("{case}.traceql"));
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    raw.strip_suffix('\n').unwrap_or(&raw).to_string()
}

fn read_golden(case: &str) -> String {
    let path = corpus_root().join(format!("{case}.golden"));
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

/// The `{:#?}` rendering the `.golden` files pin: the AST for `Ok`, the
/// error (variant + spans) for `Err`, plus the trailing newline.
fn render_outcome(input: &str) -> String {
    match parse(input) {
        Ok(ast) => format!("{ast:#?}\n"),
        Err(err) => format!("{err:#?}\n"),
    }
}

#[test]
fn manifest_matches_directory_contents() {
    verify_corpus_layout();
}

#[test]
fn every_case_reproduces_its_pinned_golden() {
    for case in verify_corpus_layout() {
        let input = read_input(&case);
        let golden = read_golden(&case);
        let actual = render_outcome(&input);
        assert_eq!(
            actual, golden,
            "golden mismatch for {case} (input: {input:?}); if the change is intentional, \
             regenerate per corpus/PROVENANCE.md"
        );

        // Keep the case classes honest: accept parses, unsupported is
        // exactly the named boundary error, reject is any *other* error.
        let outcome = parse(&input);
        if case.starts_with("accept/") {
            assert!(outcome.is_ok(), "{case} must parse, got {outcome:?}");
        } else if case.starts_with("unsupported/") {
            assert!(
                matches!(outcome, Err(TraceQlError::NotYetSupported { .. })),
                "{case} must be NotYetSupported, got {outcome:?}"
            );
        } else if case.starts_with("grafana/") {
            // Observed real-Grafana queries (issue #180): the outcome class
            // is intentionally NOT constrained here — a `grafana/` case may
            // parse `Ok`, hit a named `NotYetSupported` boundary, or (today)
            // surface a generic error at an unregistered construct. The
            // golden pins whatever it is; the *class invariant* (generic
            // failures must be ledgered and monotonically shrink) needs the
            // JSON ledger and lives in `tests/conformance.rs`, keeping this
            // file std-only.
        } else {
            assert!(
                matches!(
                    &outcome,
                    Err(err) if !matches!(err, TraceQlError::NotYetSupported { .. })
                ),
                "{case} must be a plain parse error, got {outcome:?}"
            );
        }
    }
}

#[test]
fn accept_cases_round_trip_through_display() {
    for case in verify_corpus_layout() {
        if !case.starts_with("accept/") {
            continue;
        }
        let input = read_input(&case);
        let ast = parse(&input).unwrap_or_else(|e| panic!("{case} must parse, got {e}"));
        let rendered = ast.to_string();
        let reparsed = parse(&rendered)
            .unwrap_or_else(|e| panic!("{case}: rendered form {rendered:?} must reparse, got {e}"));
        assert_eq!(
            reparsed, ast,
            "{case}: parse(ast.to_string()) != ast (rendered: {rendered:?})"
        );
    }
}

/// Every token kind reachable by the accepted grammar (M4 + the issue
/// #172 structural operators `>>`/`~`; `>` was already reachable as a
/// comparison). Boundary-only tokens (`<<`, `!`, `&`, `+`, `-`, `*`,
/// `/`, `[`, `]`) are deliberately absent: they never appear in an
/// accept case.
const EXPECTED_ACCEPT_TOKENS: &[&str] = &[
    "LBrace", "RBrace", "LParen", "RParen", "Comma", "Dot", "Eq", "Neq", "Re", "Nre", "Gt", "Gte",
    "Lt", "Lte", "AndAnd", "OrOr", "Pipe", "Shr", "Tilde", "Ident", "String", "Duration", "Number",
    "Eof",
];

/// Exhaustive by construction: adding a `TokenKind` variant fails to
/// compile until it is classified here, which forces the accept-coverage
/// decision to be made explicitly.
fn kind_name(kind: &TokenKind) -> &'static str {
    match kind {
        TokenKind::LBrace => "LBrace",
        TokenKind::RBrace => "RBrace",
        TokenKind::LParen => "LParen",
        TokenKind::RParen => "RParen",
        TokenKind::LBracket => "LBracket",
        TokenKind::RBracket => "RBracket",
        TokenKind::Comma => "Comma",
        TokenKind::Dot => "Dot",
        TokenKind::Eq => "Eq",
        TokenKind::Neq => "Neq",
        TokenKind::Re => "Re",
        TokenKind::Nre => "Nre",
        TokenKind::Gt => "Gt",
        TokenKind::Gte => "Gte",
        TokenKind::Lt => "Lt",
        TokenKind::Lte => "Lte",
        TokenKind::AndAnd => "AndAnd",
        TokenKind::OrOr => "OrOr",
        TokenKind::Pipe => "Pipe",
        TokenKind::Shr => "Shr",
        TokenKind::Shl => "Shl",
        TokenKind::Tilde => "Tilde",
        TokenKind::Bang => "Bang",
        TokenKind::Amp => "Amp",
        TokenKind::Plus => "Plus",
        TokenKind::Minus => "Minus",
        TokenKind::Star => "Star",
        TokenKind::Slash => "Slash",
        TokenKind::Ident(_) => "Ident",
        TokenKind::String(_) => "String",
        TokenKind::Duration(_) => "Duration",
        TokenKind::Number(_) => "Number",
        TokenKind::Eof => "Eof",
    }
}

#[test]
fn accept_corpus_covers_every_grammar_reachable_token_kind() {
    let mut observed = BTreeSet::new();
    for case in verify_corpus_layout() {
        if !case.starts_with("accept/") {
            continue;
        }
        let input = read_input(&case);
        let tokens = pulsus_traceql::tokenize_for_corpus_gate(&input)
            .unwrap_or_else(|e| panic!("{case} must tokenize, got {e}"));
        observed.extend(tokens.iter().map(|t| kind_name(&t.kind)));
    }
    let expected: BTreeSet<&str> = EXPECTED_ACCEPT_TOKENS.iter().copied().collect();
    assert_eq!(
        expected.len(),
        EXPECTED_ACCEPT_TOKENS.len(),
        "EXPECTED_ACCEPT_TOKENS contains duplicates"
    );
    let missing: Vec<_> = expected.difference(&observed).collect();
    let unexpected: Vec<_> = observed.difference(&expected).collect();
    assert!(
        missing.is_empty() && unexpected.is_empty(),
        "accept-corpus token coverage drift: missing {missing:?}, unexpected {unexpected:?}"
    );
}

#[test]
fn unsupported_cases_map_one_to_one_onto_the_boundary_registry() {
    let mut seen: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for case in verify_corpus_layout() {
        if !case.starts_with("unsupported/") {
            continue;
        }
        let input = read_input(&case);
        match parse(&input) {
            Err(TraceQlError::NotYetSupported { construct, .. }) => {
                seen.entry(construct).or_default().push(case);
            }
            other => panic!("{case} must be NotYetSupported, got {other:?}"),
        }
    }
    let registry: BTreeSet<&str> = BOUNDARY_CONSTRUCTS.iter().map(|(c, _)| *c).collect();
    let produced: BTreeSet<&str> = seen.keys().map(String::as_str).collect();
    let unregistered: Vec<_> = produced.difference(&registry).collect();
    let stale: Vec<_> = registry.difference(&produced).collect();
    assert!(
        unregistered.is_empty() && stale.is_empty(),
        "scope-boundary drift: corpus constructs missing from BOUNDARY_CONSTRUCTS: \
         {unregistered:?}; registry entries with no unsupported/ case: {stale:?}"
    );
}

/// The exact grammar-side role a dual-role token's accept case must
/// exhibit in its parsed AST — asserting the outcome class alone would
/// not prove the token was consumed in the claimed role.
#[derive(Debug, Clone, Copy)]
enum AcceptRole {
    /// The AST contains a field-level comparison using this operator.
    FieldComparison(ComparisonOp),
    /// The AST contains a comparison on the `name` intrinsic (the same
    /// `Ident` token that spells `parent` in the boundary case).
    IntrinsicNameField,
    /// The pipeline contains a zero-arity `count()` aggregate (the same
    /// `Ident` position where the M7 `*_over_time` functions are
    /// boundaries).
    CountAggregate,
    /// The pipeline contains the zero-arity `rate()` metric stage (issue
    /// #59 — the same `Ident` position where `quantile_over_time` stays
    /// an M7 boundary).
    RateMetric,
}

/// F6c: context-dependent tokens pinned in BOTH roles — `>=`/`<`/`<=`
/// as field-level comparisons and spanset-level structural rejects
/// (`>`'s two roles are now BOTH grammar since issue #172 — pinned by
/// `gt_token_appears_in_both_grammar_roles` below); `Ident` as an
/// intrinsic (accept) and as the `parent.` scope (reject); `Ident` as a
/// search aggregate (accept) and as a T7 metrics function (reject). Each
/// entry: (token, accept case, required AST role, boundary case, exact
/// `NotYetSupported` construct).
const DUAL_ROLE_CASES: &[(&str, &str, AcceptRole, &str, &str)] = &[
    (
        ">=",
        "accept/attr_span_number_gte",
        AcceptRole::FieldComparison(ComparisonOp::Gte),
        "unsupported/structural_gte",
        "structural operator '>='",
    ),
    (
        "<",
        "accept/duration_lt",
        AcceptRole::FieldComparison(ComparisonOp::Lt),
        "unsupported/structural_lt",
        "structural operator '<'",
    ),
    (
        "<=",
        "accept/duration_lte",
        AcceptRole::FieldComparison(ComparisonOp::Lte),
        "unsupported/structural_lte",
        "structural operator '<='",
    ),
    (
        "Ident (intrinsic vs parent scope)",
        "accept/intrinsic_name_eq",
        AcceptRole::IntrinsicNameField,
        "unsupported/parent_scope",
        "parent scope",
    ),
    (
        "Ident (search aggregate vs M7 metrics function)",
        "accept/pipeline_count",
        AcceptRole::CountAggregate,
        "unsupported/metrics_quantile_over_time",
        "metrics function 'quantile_over_time'",
    ),
    (
        "Ident (M4 metrics function vs M7 metrics function)",
        "accept/metrics_rate",
        AcceptRole::RateMetric,
        "unsupported/metrics_histogram_over_time",
        "metrics function 'histogram_over_time'",
    ),
];

/// Collects every `(field, op)` comparison pair in a spanset expression,
/// walking both spanset- and field-level binaries.
fn collect_comparisons<'q>(expr: &'q SpansetExpr, out: &mut Vec<(&'q Field, ComparisonOp)>) {
    match expr {
        SpansetExpr::Filter(filter) => {
            if let Some(body) = &filter.body {
                collect_field_comparisons(body, out);
            }
        }
        SpansetExpr::Binary { lhs, rhs, .. } | SpansetExpr::Structural { lhs, rhs, .. } => {
            collect_comparisons(lhs, out);
            collect_comparisons(rhs, out);
        }
    }
}

/// Whether the spanset tree contains a structural node with the given
/// operator (issue #172 — the grammar-role oracle for `>`'s second
/// accepted role).
fn contains_structural(expr: &SpansetExpr, want: StructuralOp) -> bool {
    match expr {
        SpansetExpr::Filter(_) => false,
        SpansetExpr::Binary { lhs, rhs, .. } => {
            contains_structural(lhs, want) || contains_structural(rhs, want)
        }
        SpansetExpr::Structural { op, lhs, rhs } => {
            *op == want || contains_structural(lhs, want) || contains_structural(rhs, want)
        }
    }
}

fn collect_field_comparisons<'q>(expr: &'q FieldExpr, out: &mut Vec<(&'q Field, ComparisonOp)>) {
    match expr {
        FieldExpr::Comparison { field, op, .. } => out.push((field, *op)),
        FieldExpr::Binary { lhs, rhs, .. } => {
            collect_field_comparisons(lhs, out);
            collect_field_comparisons(rhs, out);
        }
    }
}

fn assert_accept_role(token: &str, case: &str, role: AcceptRole, query: &Query) {
    let mut comparisons = Vec::new();
    collect_comparisons(&query.spanset, &mut comparisons);
    match role {
        AcceptRole::FieldComparison(op) => {
            assert!(
                comparisons.iter().any(|(_, cmp)| *cmp == op),
                "dual-role token {token}: {case} must contain a field-level {op:?} \
                 comparison, got {comparisons:?}"
            );
        }
        AcceptRole::IntrinsicNameField => {
            assert!(
                comparisons
                    .iter()
                    .any(|(field, _)| matches!(field, Field::Intrinsic(Intrinsic::Name))),
                "dual-role token {token}: {case} must compare the `name` intrinsic, \
                 got {comparisons:?}"
            );
        }
        AcceptRole::CountAggregate => {
            assert!(
                query.pipeline.iter().any(|stage| matches!(
                    stage,
                    PipelineStage::Aggregate {
                        op: AggregateOp::Count,
                        field: None,
                        ..
                    }
                )),
                "dual-role token {token}: {case} must contain a zero-arity count() \
                 aggregate, got {:?}",
                query.pipeline
            );
        }
        AcceptRole::RateMetric => {
            assert!(
                query
                    .pipeline
                    .iter()
                    .any(|stage| matches!(stage, PipelineStage::Metric(MetricFn::Rate))),
                "dual-role token {token}: {case} must contain the zero-arity rate() \
                 metric stage, got {:?}",
                query.pipeline
            );
        }
    }
}

#[test]
fn dual_role_tokens_appear_in_both_grammar_and_boundary_roles() {
    let cases = verify_corpus_layout();
    for (token, accept_case, accept_role, boundary_case, boundary_construct) in DUAL_ROLE_CASES {
        assert!(
            cases.contains(*accept_case),
            "dual-role token {token}: {accept_case} is missing from the corpus"
        );
        assert!(
            cases.contains(*boundary_case),
            "dual-role token {token}: {boundary_case} is missing from the corpus"
        );

        // Grammar side: the accept case must parse AND its AST must
        // exhibit the specific role (e.g. a Comparison(Gt) node), not
        // merely succeed.
        let query = parse(&read_input(accept_case)).unwrap_or_else(|e| {
            panic!("dual-role token {token}: {accept_case} must parse, got {e}")
        });
        assert_accept_role(token, accept_case, *accept_role, &query);

        // Boundary side: the reject must name the exact construct.
        match parse(&read_input(boundary_case)) {
            Err(TraceQlError::NotYetSupported { construct, .. }) => {
                assert_eq!(
                    construct, *boundary_construct,
                    "dual-role token {token}: {boundary_case} names the wrong construct"
                );
            }
            other => panic!(
                "dual-role token {token}: {boundary_case} must be NotYetSupported, got {other:?}"
            ),
        }
    }
}

/// Issue #172: `>` now has TWO grammar roles (F6c successor to its old
/// accept-vs-boundary pin) — a field-level `Comparison(Gt)` node and the
/// spanset-level `Structural(Child)` node — each proven from its corpus
/// case's parsed AST, not merely from parse success. `>>`/`~` get the
/// same structural-node pin from their moved accept cases.
#[test]
fn gt_token_appears_in_both_grammar_roles() {
    let cases = verify_corpus_layout();
    for case in [
        "accept/duration_gt",
        "accept/structural_gt",
        "accept/structural_shr",
        "accept/structural_tilde",
    ] {
        assert!(cases.contains(case), "{case} is missing from the corpus");
    }

    // Role 1: a field-level comparison.
    let query = parse(&read_input("accept/duration_gt")).expect("accept/duration_gt parses");
    let mut comparisons = Vec::new();
    collect_comparisons(&query.spanset, &mut comparisons);
    assert!(
        comparisons.iter().any(|(_, op)| *op == ComparisonOp::Gt),
        "accept/duration_gt must contain a field-level Gt comparison, got {comparisons:?}"
    );

    // Role 2: the spanset-level structural node (and the same pin for
    // the other two implemented operators).
    for (case, op) in [
        ("accept/structural_gt", StructuralOp::Child),
        ("accept/structural_shr", StructuralOp::Descendant),
        ("accept/structural_tilde", StructuralOp::Sibling),
    ] {
        let query = parse(&read_input(case)).unwrap_or_else(|e| panic!("{case} must parse: {e}"));
        assert!(
            contains_structural(&query.spanset, op),
            "{case} must contain a Structural({op:?}) node, got {:?}",
            query.spanset
        );
    }
}

/// Regenerates every `.golden` from the current parser output. Ignored
/// and env-gated — never runs in CI; see corpus/PROVENANCE.md for the
/// intended workflow (run, review the diff, commit).
#[test]
#[ignore = "golden regeneration helper; PULSUS_TRACEQL_REGEN=1 cargo test -p pulsus-traceql --test corpus -- --ignored regenerate_goldens"]
fn regenerate_goldens() {
    if std::env::var("PULSUS_TRACEQL_REGEN").as_deref() != Ok("1") {
        eprintln!("PULSUS_TRACEQL_REGEN != 1; not touching goldens");
        return;
    }
    for case in verify_corpus_layout() {
        let input = read_input(&case);
        let path = corpus_root().join(format!("{case}.golden"));
        fs::write(&path, render_outcome(&input))
            .unwrap_or_else(|e| panic!("failed to write {}: {e}", path.display()));
    }
}
