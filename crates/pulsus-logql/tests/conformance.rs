//! The LogQL conformance foundation (issue #191, M8-LQ0).
//!
//! A committed, clean-room enumeration of the entire *documented* LogQL
//! surface for the pinned language target (LogQL v3.7.3, grounded in the
//! published grafana.com LogQL documentation) plus a disposition manifest
//! giving every construct exactly one machine-checked home. No upstream
//! source, grammar, lexer, AST, error string, or test corpus is copied,
//! fetched, or vendored — see `tests/conformance/PROVENANCE.md`.
//!
//! The no-silent-gaps guarantee rests on four suite-independent gates, none
//! keyed to any existing-suite file/function name:
//!   * `registry_covers_the_recognized_parser_surface` (#10) cross-checks
//!     the registry against every enum variant / keyword table in
//!     `crates/pulsus-logql/src/ast.rs` — a new or unregistered parser
//!     construct is RED.
//!   * `every_supported_construct_is_exercise_proven_by_its_probe` (#12)
//!     parses each supported construct's canonical probe and asserts its
//!     own AST node is present via `constructs_in(&Expr)`.
//!   * `e2e_cases_exercise_their_mapped_constructs` (#13) reads the 31
//!     committed e2e differential cases and asserts every mapped construct's
//!     AST node is present.
//!   * the env-gated black-box differential (`logql_differential.rs`)
//!     replays each probe against a digest-pinned v3.7.3 reference container
//!     and pins the live verdict against the recorded `oracle`.
//!
//! Pure check functions back the file tests so the RED paths are proven by
//! committed negative-fixture unit tests.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use pulsus_logql::{
    BINARY_OP_KEYWORDS, BinOp, CompareOp, Expr, GroupingKind, LabelFilterExpr, LabelFmt,
    LineFilterOp, LogExpr, LogQlError, MatchGroup, MatchOp, MetricExpr, NumericLiteral,
    ParserStage, REMAINING_UNSUPPORTED_STAGES, RangeAggOp, Stage, UNWRAP_CONVERSIONS, VectorAggOp,
    parse,
};

// Interim gaps route to the epic #191 owns (its parent #190) until the
// LQ-1..n sub-issues are filed, then re-home. Every interim disposition must
// name a valid owning issue.
const VALID_ISSUES: [u64; 1] = [190];
// The published LogQL query-docs root (functional citation literal, black-box
// public-doc use — see PROVENANCE.md). Every registry `doc` must live under it.
const DOCS_PREFIX: &str = "https://grafana.com/docs/loki/";
const REPO_PREFIX: &str = "https://github.com/digitalis-io/pulsusdb/";
const EXPECTED_LANGUAGE: &str = "LogQL";
const EXPECTED_TARGET: &str = "v3.7.3";

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Registry {
    language: String,
    target: String,
    constructs: Vec<Construct>,
}

#[derive(Debug, Deserialize)]
struct Construct {
    id: String,
    category: String,
    #[allow(dead_code)]
    syntax: String,
    doc: String,
    probe: String,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    language: String,
    target: String,
    sha256: String,
    construct_count: usize,
    category_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Deserialize)]
struct Dispositions {
    interim_count_pin: usize,
    entries: Vec<Disposition>,
}

#[derive(Debug, Clone, Deserialize)]
struct Disposition {
    construct: String,
    status: Status,
    // The measured black-box verdict of the v3.7.3 reference container for
    // this construct's probe (tests/conformance/PROVENANCE.md). It makes the
    // differential disposition-driven: an interim construct the reference
    // *accepts* is a tracked compat gap; one it *rejects* is a both-reject
    // agreement — no construct is silently exempted.
    oracle: Oracle,
    #[serde(default)]
    error_construct: Option<String>,
    #[serde(default)]
    evidence: Vec<String>,
    #[serde(default)]
    owning_issue: Option<u64>,
    // Divergence-only (all REQUIRED when status == Divergence): the
    // owner-escalated, oracle-cited record of an intentional deviation.
    #[serde(default)]
    justification: Option<String>,
    #[serde(default)]
    oracle_citation: Option<String>,
    #[serde(default)]
    owner_escalation: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Status {
    Supported,
    InterimNamed,
    InterimGeneric,
    Divergence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Oracle {
    Accept,
    Reject,
}

#[derive(Debug, Deserialize)]
struct CoverageMap {
    sources: Vec<CoverageSource>,
}

#[derive(Debug, Deserialize)]
struct CoverageSource {
    source: String,
    #[allow(dead_code)]
    kind: String,
    constructs: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SeedLedger {
    entries: Vec<LedgerEntry>,
}

#[derive(Debug, Deserialize)]
struct LedgerEntry {
    case_id: String,
    #[allow(dead_code)]
    first_blocking_construct: String,
    owning_issues: Vec<u64>,
}

// The subset of the read e2e fixture the harness needs.
#[derive(Debug, Deserialize)]
struct E2eFixture {
    cases: Vec<E2eCase>,
}

#[derive(Debug, Deserialize)]
struct E2eCase {
    case_id: String,
    query: String,
}

/// A probe's actual parse outcome, reduced to the class the disposition
/// harness reasons about.
#[derive(Debug, PartialEq, Eq)]
enum ProbeClass {
    Parses(Expr),
    Named(String),
    Generic,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

fn conf_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("conformance")
}

fn read(path: &PathBuf) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

fn registry_bytes() -> Vec<u8> {
    let path = conf_dir().join("registry-logql-v3.7.3.json");
    fs::read(&path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

fn load_registry() -> Registry {
    serde_json::from_slice(&registry_bytes()).expect("registry JSON parses")
}

fn load_manifest() -> Manifest {
    serde_json::from_str(&read(&conf_dir().join("registry-manifest.json")))
        .expect("manifest parses")
}

fn load_dispositions() -> Dispositions {
    serde_json::from_str(&read(&conf_dir().join("dispositions.json"))).expect("dispositions parse")
}

fn load_coverage_map() -> CoverageMap {
    serde_json::from_str(&read(&conf_dir().join("coverage-map.json"))).expect("coverage-map parses")
}

fn load_ledger() -> SeedLedger {
    serde_json::from_str(&read(&conf_dir().join("seed-ledger.json"))).expect("seed-ledger parses")
}

/// The repo-root e2e differential fixture (read-only, workspace-relative). A
/// missing/renamed fixture fails loudly — never a silent skip.
fn e2e_fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("test")
        .join("fixtures")
        .join("logs")
        .join("differential.json")
}

fn load_e2e_fixture() -> E2eFixture {
    let path = e2e_fixture_path();
    serde_json::from_str(&read(&path))
        .unwrap_or_else(|e| panic!("failed to parse e2e fixture {}: {e}", path.display()))
}

/// The `{R}` run-id placeholder every e2e case carries, substituted with a
/// dummy so the query parses (tests #8/#13 exercise parse only).
fn substitute_placeholders(query: &str) -> String {
    query.replace("{R}", "lq0run")
}

// ---------------------------------------------------------------------------
// classify + the exercise walker
// ---------------------------------------------------------------------------

fn classify(probe: &str) -> ProbeClass {
    match parse(probe) {
        Ok(e) => ProbeClass::Parses(e),
        Err(LogQlError::NotYetSupported { construct, .. }) => ProbeClass::Named(construct),
        Err(_) => ProbeClass::Generic,
    }
}

/// The F2 exercise walker. Derived PURELY from the public AST (the parse tree
/// is the source of truth); the registry id is only ever the assertion target
/// of tests #12/#13, never an input here. Emits the registry id for each
/// `supported` construct whose AST feature is present. The node→id mapping
/// mirrors the F1 cross-check table and the AST-distinguishability collapse
/// rule (duration/bytes label-filter RHS → one `labelfilter.compare.unit`;
/// double-quote/backtick string → one `statics.string`; `,`/`and` join → one
/// `labelfilter.and`; template internals → one `template.builtin.representative`).
fn constructs_in(expr: &Expr) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    match expr {
        Expr::Log(le) => walk_log(le, &mut out),
        Expr::Metric(me) => walk_metric(me, &mut out),
    }
    out
}

fn walk_log(le: &LogExpr, out: &mut BTreeSet<String>) {
    for m in &le.selector.matchers {
        out.insert(matcher_id(m.op).to_string());
        out.insert("statics.string".to_string());
    }
    for stage in &le.pipeline {
        walk_stage(stage, out);
    }
}

fn matcher_id(op: MatchOp) -> &'static str {
    match op {
        MatchOp::Eq => "matcher.eq",
        MatchOp::Neq => "matcher.neq",
        MatchOp::Re => "matcher.re",
        MatchOp::Nre => "matcher.nre",
    }
}

fn linefilter_id(op: LineFilterOp) -> &'static str {
    match op {
        LineFilterOp::Contains => "linefilter.contains",
        LineFilterOp::NotContains => "linefilter.notcontains",
        LineFilterOp::Regex => "linefilter.regex",
        LineFilterOp::NotRegex => "linefilter.notregex",
    }
}

fn compare_id(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Eq => "labelfilter.compare.eq",
        CompareOp::Neq => "labelfilter.compare.neq",
        CompareOp::Gt => "labelfilter.compare.gt",
        CompareOp::Gte => "labelfilter.compare.gte",
        CompareOp::Lt => "labelfilter.compare.lt",
        CompareOp::Lte => "labelfilter.compare.lte",
    }
}

fn grouping_id(kind: GroupingKind) -> &'static str {
    match kind {
        GroupingKind::By => "grouping.by",
        GroupingKind::Without => "grouping.without",
    }
}

fn walk_stage(stage: &Stage, out: &mut BTreeSet<String>) {
    match stage {
        Stage::LineFilter(lf) => {
            out.insert(linefilter_id(lf.op).to_string());
            out.insert("statics.string".to_string());
        }
        Stage::Parser(p) => {
            out.insert(parser_base_id(p).to_string());
            match p {
                ParserStage::Json { extractions } if !extractions.is_empty() => {
                    out.insert("parser.json.expressions".to_string());
                }
                ParserStage::Logfmt { extractions } if !extractions.is_empty() => {
                    out.insert("parser.logfmt.expressions".to_string());
                }
                ParserStage::Regexp(_) | ParserStage::Pattern(_) => {
                    out.insert("statics.string".to_string());
                }
                _ => {}
            }
        }
        Stage::LabelFilter(lfe) => walk_label_filter(lfe, out),
        Stage::LineFormat(tmpl) => {
            out.insert("fmt.line_format".to_string());
            out.insert("statics.string".to_string());
            if tmpl.contains("{{") {
                out.insert("template.builtin.representative".to_string());
            }
        }
        Stage::LabelFormat(fmts) => {
            for f in fmts {
                out.insert(labelfmt_id(f).to_string());
                if let LabelFmt::Template { tmpl, .. } = f {
                    out.insert("statics.string".to_string());
                    if tmpl.contains("{{") {
                        out.insert("template.builtin.representative".to_string());
                    }
                }
            }
        }
        Stage::Unwrap(u) => {
            out.insert(
                match u.conversion.as_deref() {
                    None => "unwrap.bare",
                    Some("duration") => "unwrap.duration",
                    Some("duration_seconds") => "unwrap.duration_seconds",
                    Some("bytes") => "unwrap.bytes",
                    // Any other conversion is not a registered construct;
                    // leave it unmapped so #10/#12 surface the gap.
                    Some(_) => return,
                }
                .to_string(),
            );
        }
    }
}

fn walk_label_filter(lfe: &LabelFilterExpr, out: &mut BTreeSet<String>) {
    match lfe {
        LabelFilterExpr::Match(m) => {
            out.insert("statics.string".to_string());
            match m.name.as_str() {
                "__error__" => {
                    out.insert("labelfilter.error".to_string());
                }
                "__error_details__" => {
                    out.insert("labelfilter.error_details".to_string());
                }
                _ => {
                    out.insert(labelfilter_match_id(m.op).to_string());
                }
            }
        }
        LabelFilterExpr::Compare { op, rhs, .. } => {
            out.insert(compare_id(*op).to_string());
            out.insert(numeric_id(rhs).to_string());
        }
        LabelFilterExpr::And(a, b) => {
            out.insert("labelfilter.and".to_string());
            walk_label_filter(a, out);
            walk_label_filter(b, out);
        }
        LabelFilterExpr::Or(a, b) => {
            out.insert("labelfilter.or".to_string());
            walk_label_filter(a, out);
            walk_label_filter(b, out);
        }
    }
}

fn walk_metric(me: &MetricExpr, out: &mut BTreeSet<String>) {
    match me {
        MetricExpr::Range { op, range, .. } => {
            out.insert(range_id(*op).to_string());
            out.insert("range.duration_literal".to_string());
            walk_log(&range.selector, out);
        }
        MetricExpr::Vector {
            op,
            grouping,
            inner,
            ..
        } => {
            out.insert(vector_id(*op).to_string());
            if let Some(g) = grouping {
                out.insert(grouping_id(g.kind).to_string());
            }
            walk_metric(inner, out);
        }
        MetricExpr::Literal(_) => {
            out.insert("statics.number".to_string());
        }
        MetricExpr::Binary {
            op,
            modifier,
            lhs,
            rhs,
        } => {
            out.insert(binop_id(*op).to_string());
            if let Some(m) = modifier {
                if m.return_bool {
                    out.insert("binop.bool".to_string());
                }
                if let Some(vm) = &m.matching {
                    out.insert(if vm.on { "match.on" } else { "match.ignoring" }.to_string());
                    if let Some(g) = &vm.group {
                        out.insert(matchgroup_id(g).to_string());
                    }
                }
            }
            walk_metric(lhs, out);
            walk_metric(rhs, out);
        }
    }
}

fn range_id(op: RangeAggOp) -> &'static str {
    match op {
        RangeAggOp::Rate => "range.rate",
        RangeAggOp::CountOverTime => "range.count_over_time",
        RangeAggOp::BytesRate => "range.bytes_rate",
        RangeAggOp::BytesOverTime => "range.bytes_over_time",
        RangeAggOp::SumOverTime => "range.sum_over_time",
        RangeAggOp::AvgOverTime => "range.avg_over_time",
        RangeAggOp::MinOverTime => "range.min_over_time",
        RangeAggOp::MaxOverTime => "range.max_over_time",
        RangeAggOp::StddevOverTime => "range.stddev_over_time",
        RangeAggOp::StdvarOverTime => "range.stdvar_over_time",
        RangeAggOp::QuantileOverTime => "range.quantile_over_time",
        RangeAggOp::FirstOverTime => "range.first_over_time",
        RangeAggOp::LastOverTime => "range.last_over_time",
        RangeAggOp::AbsentOverTime => "range.absent_over_time",
    }
}

fn vector_id(op: VectorAggOp) -> &'static str {
    match op {
        VectorAggOp::Sum => "agg.sum",
        VectorAggOp::Avg => "agg.avg",
        VectorAggOp::Min => "agg.min",
        VectorAggOp::Max => "agg.max",
        VectorAggOp::Count => "agg.count",
        VectorAggOp::Stddev => "agg.stddev",
        VectorAggOp::Stdvar => "agg.stdvar",
        VectorAggOp::Topk => "agg.topk",
        VectorAggOp::Bottomk => "agg.bottomk",
    }
}

fn binop_id(op: BinOp) -> &'static str {
    match op {
        BinOp::Add => "binop.add",
        BinOp::Sub => "binop.sub",
        BinOp::Mul => "binop.mul",
        BinOp::Div => "binop.div",
        BinOp::Mod => "binop.mod",
        BinOp::Pow => "binop.pow",
        BinOp::Eq => "binop.eq",
        BinOp::Neq => "binop.neq",
        BinOp::Gt => "binop.gt",
        BinOp::Gte => "binop.gte",
        BinOp::Lt => "binop.lt",
        BinOp::Lte => "binop.lte",
        BinOp::And => "binop.and",
        BinOp::Or => "binop.or",
        BinOp::Unless => "binop.unless",
    }
}

/// The label-filter `MatchOp` mapping (distinct from the stream-selector
/// `matcher_id` mapping of the same enum). Exhaustive: a new `MatchOp` variant
/// compile-breaks this AND `matcher_id`.
fn labelfilter_match_id(op: MatchOp) -> &'static str {
    match op {
        MatchOp::Eq => "labelfilter.match.eq",
        MatchOp::Neq => "labelfilter.match.neq",
        MatchOp::Re => "labelfilter.match.re",
        MatchOp::Nre => "labelfilter.match.nre",
    }
}

fn numeric_id(rhs: &NumericLiteral) -> &'static str {
    match rhs {
        NumericLiteral::Number(_) => "labelfilter.compare.number",
        NumericLiteral::DurationOrBytes(_) => "labelfilter.compare.unit",
    }
}

fn parser_base_id(p: &ParserStage) -> &'static str {
    match p {
        ParserStage::Json { .. } => "parser.json",
        ParserStage::Logfmt { .. } => "parser.logfmt",
        ParserStage::Regexp(_) => "parser.regexp",
        ParserStage::Pattern(_) => "parser.pattern",
    }
}

fn labelfmt_id(f: &LabelFmt) -> &'static str {
    match f {
        LabelFmt::Rename { .. } => "fmt.label_format.rename",
        LabelFmt::Template { .. } => "fmt.label_format.template",
    }
}

fn matchgroup_id(g: &MatchGroup) -> &'static str {
    match g {
        MatchGroup::Left(_) => "match.group_left",
        MatchGroup::Right(_) => "match.group_right",
    }
}

// ---------------------------------------------------------------------------
// Compiler-exhaustive variant enumerations (test #10's source of truth)
// ---------------------------------------------------------------------------
//
// Each `*_ALL` lists every variant of a covered AST enum. Completeness is
// compiler-enforced: every `*_ALL` entry is mapped through its enum's
// wildcard-free `*_id` match (above) — which `constructs_in` also uses — so
// **adding a variant to any covered enum fails to compile** until it is
// handled in that match. A new variant therefore cannot exist without an
// author editing this module; the co-located `*_ALL` must then list it, and
// test #10 asserts the resulting id is EXACTLY registered. (Prove-it: add a
// dummy variant without extending `*_ALL` → the `*_id` match is non-exhaustive
// → compile error; then map it in `*_ALL` to an unregistered id → #10 RED.)
//
// KNOWN RESIDUAL BOUNDARY (documented, not chased): this makes any new VARIANT
// of a covered enum impossible to introduce silently. Two residuals remain,
// each caught by a different mechanism, none silent: (1) a wholly-new
// top-level AST enum is out of this catalog's scope — but `constructs_in`'s
// wildcard-free structural matches (`Expr`/`Stage`/`MetricExpr`/
// `LabelFilterExpr`) compile-break when such an enum is threaded into the
// walker; (2) non-variant leaf ids driven by struct fields / string tables /
// presence flags (e.g. `statics.string`, `parser.*.expressions`,
// `labelfilter.error`, `fmt.line_format`, `template.builtin.representative`,
// `unwrap.*`, `range.duration_literal`, `binop.bool`, `match.on/ignoring`,
// `labelfilter.and/or`) are exercise-proven per-construct by test #12 and by
// the reverse gate #10b, not by this catalog.

const MATCH_ALL: &[MatchOp] = &[MatchOp::Eq, MatchOp::Neq, MatchOp::Re, MatchOp::Nre];
const COMPARE_ALL: &[CompareOp] = &[
    CompareOp::Eq,
    CompareOp::Neq,
    CompareOp::Gt,
    CompareOp::Gte,
    CompareOp::Lt,
    CompareOp::Lte,
];
const LINEFILTER_ALL: &[LineFilterOp] = &[
    LineFilterOp::Contains,
    LineFilterOp::NotContains,
    LineFilterOp::Regex,
    LineFilterOp::NotRegex,
];
const GROUPING_ALL: &[GroupingKind] = &[GroupingKind::By, GroupingKind::Without];
const RANGE_ALL: &[RangeAggOp] = &[
    RangeAggOp::Rate,
    RangeAggOp::CountOverTime,
    RangeAggOp::BytesRate,
    RangeAggOp::BytesOverTime,
    RangeAggOp::SumOverTime,
    RangeAggOp::AvgOverTime,
    RangeAggOp::MinOverTime,
    RangeAggOp::MaxOverTime,
    RangeAggOp::StddevOverTime,
    RangeAggOp::StdvarOverTime,
    RangeAggOp::QuantileOverTime,
    RangeAggOp::FirstOverTime,
    RangeAggOp::LastOverTime,
    RangeAggOp::AbsentOverTime,
];
const VECTOR_ALL: &[VectorAggOp] = &[
    VectorAggOp::Sum,
    VectorAggOp::Avg,
    VectorAggOp::Min,
    VectorAggOp::Max,
    VectorAggOp::Count,
    VectorAggOp::Stddev,
    VectorAggOp::Stdvar,
    VectorAggOp::Topk,
    VectorAggOp::Bottomk,
];
const BINOP_ALL: &[BinOp] = &[
    BinOp::Add,
    BinOp::Sub,
    BinOp::Mul,
    BinOp::Div,
    BinOp::Mod,
    BinOp::Pow,
    BinOp::Eq,
    BinOp::Neq,
    BinOp::Gt,
    BinOp::Gte,
    BinOp::Lt,
    BinOp::Lte,
    BinOp::And,
    BinOp::Or,
    BinOp::Unless,
];

// Data-carrying enums: representative values (their id functions ignore the
// payload), returned owned since the payloads are not `const`-constructible.
fn parser_all() -> Vec<ParserStage> {
    vec![
        ParserStage::Json {
            extractions: vec![],
        },
        ParserStage::Logfmt {
            extractions: vec![],
        },
        ParserStage::Regexp(String::new()),
        ParserStage::Pattern(String::new()),
    ]
}
fn labelfmt_all() -> Vec<LabelFmt> {
    vec![
        LabelFmt::Rename {
            dst: String::new(),
            src: String::new(),
        },
        LabelFmt::Template {
            dst: String::new(),
            tmpl: String::new(),
        },
    ]
}
fn matchgroup_all() -> Vec<MatchGroup> {
    vec![MatchGroup::Left(vec![]), MatchGroup::Right(vec![])]
}
fn numeric_all() -> Vec<NumericLiteral> {
    vec![
        NumericLiteral::Number(String::new()),
        NumericLiteral::DurationOrBytes(String::new()),
    ]
}

// ---------------------------------------------------------------------------
// Pure checks (shared by the file tests and the negative-fixture unit tests)
// ---------------------------------------------------------------------------

/// F4: a doc citation must have a REAL page-path segment before any `?`/`#`,
/// plus a non-empty `#`-anchor. A bare root, an anchorless page, and a
/// query-string-only root (`DOCS_PREFIX/?q=x#a`) each FAIL.
fn check_doc(doc: &str) -> Result<(), String> {
    let rest = doc
        .strip_prefix(DOCS_PREFIX)
        .ok_or_else(|| format!("doc {doc:?} is not under {DOCS_PREFIX}"))?;
    let (before_frag, frag) = rest
        .split_once('#')
        .ok_or_else(|| format!("doc {doc:?} needs a #anchor"))?;
    if frag.trim().is_empty() {
        return Err(format!("doc {doc:?} needs a non-empty #anchor"));
    }
    let path = before_frag.split('?').next().unwrap_or("");
    if path.trim_matches('/').is_empty() {
        return Err(format!(
            "doc {doc:?} needs a page-path segment before ?/#, not the bare root"
        ));
    }
    Ok(())
}

/// Registry ids and disposition constructs must be a bijection; an
/// un-dispositioned construct (or an orphan disposition) is an error.
fn check_bijection(construct_ids: &[String], disp_constructs: &[String]) -> Result<(), String> {
    let reg: BTreeSet<&str> = construct_ids.iter().map(String::as_str).collect();
    if reg.len() != construct_ids.len() {
        return Err("registry has duplicate construct ids".to_string());
    }
    let disp: BTreeSet<&str> = disp_constructs.iter().map(String::as_str).collect();
    if disp.len() != disp_constructs.len() {
        return Err("dispositions have duplicate constructs".to_string());
    }
    let undispositioned: Vec<_> = reg.difference(&disp).collect();
    let orphan: Vec<_> = disp.difference(&reg).collect();
    if !undispositioned.is_empty() || !orphan.is_empty() {
        return Err(format!(
            "disposition bijection broken: constructs with no disposition {undispositioned:?}; \
             dispositions with no construct {orphan:?}"
        ));
    }
    Ok(())
}

/// A disposition's status must match its probe's actual parse outcome (and
/// carry the status-specific required fields). `divergence` is metadata-only
/// (no probe constraint) and is validated by [`check_divergence`].
fn check_status(d: &Disposition, probe: &str) -> Result<(), String> {
    let class = classify(probe);
    match d.status {
        Status::Supported => match class {
            ProbeClass::Parses(_) => Ok(()),
            other => Err(format!(
                "{}: status `supported` but probe {probe:?} gave {other:?}",
                d.construct
            )),
        },
        Status::InterimNamed => {
            let want = d.error_construct.as_deref().ok_or_else(|| {
                format!("{}: interim-named requires `error_construct`", d.construct)
            })?;
            check_interim_issue(d)?;
            match &class {
                ProbeClass::Named(got) if got == want => {
                    // The Display must name the construct — never a bare
                    // generic error for a real construct.
                    let err = parse(probe).expect_err("interim-named probe must error");
                    if err.to_string().contains(want) {
                        Ok(())
                    } else {
                        Err(format!(
                            "{}: NotYetSupported Display {:?} does not name {want:?}",
                            d.construct,
                            err.to_string()
                        ))
                    }
                }
                ProbeClass::Named(got) => Err(format!(
                    "{}: interim-named error_construct {want:?} but probe named {got:?}",
                    d.construct
                )),
                other => Err(format!(
                    "{}: interim-named but probe {probe:?} gave {other:?} (expected NotYetSupported)",
                    d.construct
                )),
            }
        }
        Status::InterimGeneric => {
            check_interim_issue(d)?;
            match class {
                ProbeClass::Generic => Ok(()),
                other => Err(format!(
                    "{}: interim-generic but probe {probe:?} gave {other:?} \
                     (a construct that now parses or names a boundary must have its \
                     disposition flipped)",
                    d.construct
                )),
            }
        }
        Status::Divergence => check_divergence(d),
    }
}

fn check_interim_issue(d: &Disposition) -> Result<(), String> {
    match d.owning_issue {
        Some(i) if VALID_ISSUES.contains(&i) => Ok(()),
        Some(i) => Err(format!(
            "{}: owning_issue {i} not in {VALID_ISSUES:?}",
            d.construct
        )),
        None => Err(format!(
            "{}: interim-* requires an owning_issue",
            d.construct
        )),
    }
}

/// A `divergence` entry must carry a non-empty justification plus a
/// published-doc oracle citation and an owner-escalation URL of the right
/// shapes, plus an owning issue. Pinned to zero at LQ0 (the schema is
/// validated by the negative-fixture units so later escalations are enforced).
fn check_divergence(d: &Disposition) -> Result<(), String> {
    let justification = d.justification.as_deref().unwrap_or("");
    if justification.trim().is_empty() {
        return Err(format!(
            "{}: divergence requires a non-empty justification",
            d.construct
        ));
    }
    match d.oracle_citation.as_deref() {
        Some(u) if u.starts_with(DOCS_PREFIX) => {}
        _ => {
            return Err(format!(
                "{}: divergence requires an oracle_citation starting {DOCS_PREFIX}",
                d.construct
            ));
        }
    }
    match d.owner_escalation.as_deref() {
        Some(u) if u.starts_with(REPO_PREFIX) => {}
        _ => {
            return Err(format!(
                "{}: divergence requires an owner_escalation starting {REPO_PREFIX}",
                d.construct
            ));
        }
    }
    if d.owning_issue.is_none() {
        return Err(format!(
            "{}: divergence requires an owning_issue",
            d.construct
        ));
    }
    Ok(())
}

/// Number of interim (named + generic) dispositions — the pinned quantity
/// each LQ-1..n PR lowers and LQ-closeout drives to 0.
fn interim_count(entries: &[Disposition]) -> usize {
    entries
        .iter()
        .filter(|d| matches!(d.status, Status::InterimNamed | Status::InterimGeneric))
        .count()
}

// ---------------------------------------------------------------------------
// File tests
// ---------------------------------------------------------------------------

// #1
#[test]
fn registry_matches_its_integrity_manifest() {
    let registry = load_registry();
    let manifest = load_manifest();

    let sha: String = Sha256::digest(registry_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        sha, manifest.sha256,
        "registry SHA-256 drift — an edit to registry-logql-v3.7.3.json must be deliberate \
         and re-pin registry-manifest.json"
    );

    // F3: the language target is mechanically pinned in both files.
    assert_eq!(
        registry.language, EXPECTED_LANGUAGE,
        "registry language pin"
    );
    assert_eq!(registry.target, EXPECTED_TARGET, "registry target pin");
    assert_eq!(
        manifest.language, registry.language,
        "manifest.language must equal registry.language"
    );
    assert_eq!(
        manifest.target, registry.target,
        "manifest.target must equal registry.target"
    );

    assert_eq!(
        registry.constructs.len(),
        manifest.construct_count,
        "construct_count pin mismatch"
    );

    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for c in &registry.constructs {
        *counts.entry(c.category.clone()).or_default() += 1;
    }
    assert_eq!(
        counts, manifest.category_counts,
        "per-category count pin mismatch"
    );

    let ids: BTreeSet<&str> = registry.constructs.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(
        ids.len(),
        registry.constructs.len(),
        "duplicate construct ids in the registry"
    );
    // F4: every entry carries a real page-path + anchor public-doc citation.
    for c in &registry.constructs {
        check_doc(&c.doc).unwrap_or_else(|e| panic!("{}: {e}", c.id));
        assert!(!c.probe.trim().is_empty(), "{}: empty probe", c.id);
    }
}

// #2
#[test]
fn every_registry_construct_has_exactly_one_disposition() {
    let registry = load_registry();
    let disp = load_dispositions();
    let ids: Vec<String> = registry.constructs.iter().map(|c| c.id.clone()).collect();
    let constructs: Vec<String> = disp.entries.iter().map(|d| d.construct.clone()).collect();
    check_bijection(&ids, &constructs).expect("registry <-> disposition bijection");
}

// #3
#[test]
fn disposition_probes_match_their_status() {
    let registry = load_registry();
    let disp = load_dispositions();
    let probes: BTreeMap<&str, &str> = registry
        .constructs
        .iter()
        .map(|c| (c.id.as_str(), c.probe.as_str()))
        .collect();
    for d in &disp.entries {
        let probe = probes
            .get(d.construct.as_str())
            .unwrap_or_else(|| panic!("{}: no registry probe", d.construct));
        check_status(d, probe).expect("disposition status matches its probe");
    }
}

// #4
#[test]
fn evidence_pointers_resolve_to_a_coverage_source() {
    let disp = load_dispositions();
    let cov = load_coverage_map();
    let sources: BTreeSet<&str> = cov.sources.iter().map(|s| s.source.as_str()).collect();
    for d in &disp.entries {
        for ev in &d.evidence {
            assert!(
                sources.contains(ev.as_str()),
                "{}: evidence {ev:?} is not a coverage-map source",
                d.construct
            );
        }
    }
}

// #5
#[test]
fn interim_disposition_count_is_pinned() {
    let disp = load_dispositions();
    assert_eq!(
        interim_count(&disp.entries),
        disp.interim_count_pin,
        "interim_count_pin drift — a status flip must re-pin it (LQ-1..n lower it; LQ-closeout -> 0)"
    );
}

// #6
#[test]
fn divergence_count_is_zero_at_lq0() {
    let disp = load_dispositions();
    let n = disp
        .entries
        .iter()
        .filter(|d| d.status == Status::Divergence)
        .count();
    assert_eq!(n, 0, "AC 8: zero divergence entries at LQ0");
}

// #7
#[test]
fn differential_categories_are_pinned() {
    let disp = load_dispositions();
    let mut supported = 0usize;
    let mut tracked_interim = 0usize; // interim ∧ oracle accepts (a real gap)
    let mut both_reject = 0usize; // interim ∧ oracle rejects (agreement)
    let mut unescalated_divergence = Vec::new(); // supported ∧ oracle rejects
    for d in &disp.entries {
        match (d.status, d.oracle) {
            (Status::Supported, Oracle::Accept) => supported += 1,
            (Status::Supported, Oracle::Reject) => unescalated_divergence.push(&d.construct),
            (Status::Divergence, _) => {}
            (_, Oracle::Accept) => tracked_interim += 1,
            (_, Oracle::Reject) => both_reject += 1,
        }
    }
    // A `supported` construct the reference rejects would be an unrecorded
    // divergence (we more permissive than the oracle) — never allowed to slip
    // in silently.
    assert!(
        unescalated_divergence.is_empty(),
        "supported constructs the reference rejects (unescalated divergences): {unescalated_divergence:?}"
    );
    assert_eq!(supported, 86, "supported (both-accept agreement) count pin");
    assert_eq!(
        tracked_interim, 11,
        "tracked interim gap count pin (interim ∧ oracle accepts, each with an owning issue)"
    );
    assert_eq!(
        both_reject, 2,
        "both-reject agreement count pin (interim ∧ oracle rejects: stage.ip, stage.distinct)"
    );

    // Every tracked interim gap must name an owning issue.
    for d in &disp.entries {
        if d.status != Status::Supported && d.oracle == Oracle::Accept {
            assert!(
                d.owning_issue.is_some(),
                "{}: a tracked interim gap must name an owning issue",
                d.construct
            );
        }
    }
}

// #8
#[test]
fn seed_cases_are_ok_named_or_ledgered() {
    let fixture = load_e2e_fixture();
    assert!(
        !fixture.cases.is_empty(),
        "the e2e fixture must not be empty"
    );
    let ledger = load_ledger();
    let ledgered: BTreeSet<&str> = ledger.entries.iter().map(|e| e.case_id.as_str()).collect();

    for case in &fixture.cases {
        let query = substitute_placeholders(&case.query);
        let class = classify(&query);
        let ok_or_named = matches!(class, ProbeClass::Parses(_) | ProbeClass::Named(_));
        assert!(
            ok_or_named || ledgered.contains(case.case_id.as_str()),
            "{}: a generic parse failure must be recorded in seed-ledger.json, got {class:?}",
            case.case_id
        );
    }

    // Monotone shrink: a ledger entry that no longer reproduces its generic
    // failure (now parses Ok or reaches a named boundary) is stale.
    let case_by_id: BTreeMap<&str, &str> = fixture
        .cases
        .iter()
        .map(|c| (c.case_id.as_str(), c.query.as_str()))
        .collect();
    for entry in &ledger.entries {
        let query = case_by_id
            .get(entry.case_id.as_str())
            .unwrap_or_else(|| panic!("ledger case {:?} is not an e2e case", entry.case_id));
        let class = classify(&substitute_placeholders(query));
        assert_eq!(
            class,
            ProbeClass::Generic,
            "stale ledger entry {:?}: it now resolves to {class:?}; drop it (the ledger only shrinks)",
            entry.case_id
        );
        for i in &entry.owning_issues {
            assert!(
                VALID_ISSUES.contains(i),
                "ledger case {:?}: owning issue {i} not in {VALID_ISSUES:?}",
                entry.case_id
            );
        }
    }
}

// #9
#[test]
fn doc_check_rejects_bare_root_query_and_anchorless() {
    // Bare root (no page, no anchor).
    assert!(check_doc(DOCS_PREFIX).is_err());
    // Page with no `#`.
    assert!(check_doc(&format!("{DOCS_PREFIX}latest/query/log_queries/")).is_err());
    // Empty fragment.
    assert!(check_doc(&format!("{DOCS_PREFIX}latest/query/log_queries/#")).is_err());
    // Query-string-only root (no page-path segment before `?`).
    assert!(check_doc(&format!("{DOCS_PREFIX}?q=x#anchor")).is_err());
    // Not under the docs prefix.
    assert!(check_doc("https://example.com/x#a").is_err());
    // Valid: real page path + non-empty anchor.
    check_doc(&format!(
        "{DOCS_PREFIX}latest/query/log_queries/#line-filter-expression"
    ))
    .expect("a real page + anchor citation is valid");
}

/// The recognized parser surface of `crates/pulsus-logql/src/ast.rs`: every
/// covered AST enum's variants, each paired (via that enum's wildcard-free
/// `*_id` match) with the EXACT registry construct id it must map to. Because
/// the enumeration iterates the compiler-exhaustive `*_ALL` catalogs and maps
/// each entry through the same `*_id` matches `constructs_in` uses, a new
/// variant of any covered enum CANNOT compile without being handled — see the
/// `*_ALL` doc-block for the enforcement argument and the residual boundary.
/// The check in test #10 is EXACT-id membership, never a substring hit, so a
/// genuinely missing registration cannot be masked by a longer id that
/// contains the token. A small set of exact ids that come from `pub(crate)`
/// keyword tables in ast.rs which the test crate cannot import
/// (`UNWRAP_CONVERSIONS`) is listed literally; those are covered per-construct
/// by tests #12/#10b (they are string-table members, not enum variants).
fn recognized_parser_surface() -> BTreeMap<String, &'static str> {
    let mut m: BTreeMap<String, &'static str> = BTreeMap::new();
    let mut put = |id: String, group: &'static str| {
        assert!(
            m.insert(id.clone(), group).is_none(),
            "duplicate surface id {id:?}"
        );
    };

    // Enum-backed surface, iterated over the compiler-exhaustive `*_ALL`
    // catalogs and mapped through the wildcard-free `*_id` matches.
    for &op in MATCH_ALL {
        put(matcher_id(op).to_string(), "MatchOp/selector");
        put(labelfilter_match_id(op).to_string(), "MatchOp/label-filter");
    }
    for &op in COMPARE_ALL {
        put(compare_id(op).to_string(), "CompareOp");
    }
    for &op in LINEFILTER_ALL {
        put(linefilter_id(op).to_string(), "LineFilterOp");
    }
    for &kind in GROUPING_ALL {
        put(grouping_id(kind).to_string(), "GroupingKind");
    }
    for &op in RANGE_ALL {
        put(range_id(op).to_string(), "RangeAggOp");
    }
    for &op in VECTOR_ALL {
        put(vector_id(op).to_string(), "VectorAggOp");
    }
    for &op in BINOP_ALL {
        put(binop_id(op).to_string(), "BinOp");
    }
    for p in parser_all() {
        put(parser_base_id(&p).to_string(), "ParserStage");
    }
    for f in labelfmt_all() {
        put(labelfmt_id(&f).to_string(), "LabelFmt");
    }
    for g in matchgroup_all() {
        put(matchgroup_id(&g).to_string(), "MatchGroup");
    }
    for n in numeric_all() {
        put(numeric_id(&n).to_string(), "NumericLiteral");
    }

    // String-table surface, iterated over the REAL `pub` ast.rs
    // `UNWRAP_CONVERSIONS` table (not hand-copied), so adding a table entry
    // auto-requires an exact registry entry — no drift possible. The id is
    // derived from the canonical table entry by the naming convention
    // `unwrap.<conv>`. The other two `pub` tables are coupled in test #10
    // itself: `REMAINING_UNSUPPORTED_STAGES` (`stage.<kw>`, also asserting
    // interim-named status + the naming probe) and `BINARY_OP_KEYWORDS`
    // (`binop.<kw>`, whose ids overlap `BINOP_ALL` so they are checked against
    // the registry directly rather than re-inserted here).
    for conv in UNWRAP_CONVERSIONS {
        put(format!("unwrap.{conv}"), "UNWRAP_CONVERSIONS");
    }
    m
}

// #10 — the F1 completeness gate against crates/pulsus-logql/src/ast.rs:
// every recognised parser-surface member maps to an EXACTLY-registered id.
#[test]
fn registry_covers_the_recognized_parser_surface() {
    let registry = load_registry();
    let ids: BTreeSet<&str> = registry.constructs.iter().map(|c| c.id.as_str()).collect();

    for (id, group) in recognized_parser_surface() {
        assert!(
            ids.contains(id.as_str()),
            "{group} surface maps to construct id {id:?}, which is not exactly registered — \
             a new/renamed ast.rs variant must be registered under that exact id"
        );
    }

    // REMAINING_UNSUPPORTED_STAGES — iterated over the REAL `pub` ast.rs table
    // (not hand-copied): each must be a live interim-named entry (exact
    // `stage.<kw>` id) whose probe `{app="x"} | <kw>` yields NotYetSupported
    // naming the stage. Adding a keyword to the canonical table with no
    // `stage.<kw>` registry entry ⇒ RED here (no silent accept).
    let disp = load_dispositions();
    let by_construct: BTreeMap<&str, &Disposition> = disp
        .entries
        .iter()
        .map(|d| (d.construct.as_str(), d))
        .collect();
    for kw in REMAINING_UNSUPPORTED_STAGES {
        let id = format!("stage.{kw}");
        let d = by_construct
            .get(id.as_str())
            .unwrap_or_else(|| panic!("unsupported stage {kw:?} has no `{id}` disposition"));
        assert_eq!(
            d.status,
            Status::InterimNamed,
            "{id}: an unsupported stage must be interim-named"
        );
        match classify(&format!("{{app=\"x\"}} | {kw}")) {
            ProbeClass::Named(got) => assert_eq!(&got, kw, "{id}: probe must name the stage"),
            other => panic!("{id}: probe should be NotYetSupported({kw}), got {other:?}"),
        }
    }

    // BINARY_OP_KEYWORDS — iterated over the REAL `pub` ast.rs table: each
    // identifier-shaped binary operator must map to a registered `binop.<kw>`
    // construct. Adding a keyword to the canonical table with no registry entry
    // ⇒ RED (the operator semantics are additionally enum-covered by BINOP_ALL).
    for kw in BINARY_OP_KEYWORDS {
        let id = format!("binop.{kw}");
        assert!(
            ids.contains(id.as_str()),
            "BINARY_OP_KEYWORDS entry {kw:?} maps to construct id {id:?}, which is not registered"
        );
    }
}

// #10b — the REVERSE completeness gate: every id the exercise walker CAN emit
// must itself be a registered construct. Combined with `constructs_in`'s
// exhaustive (wildcard-free) matches — which compile-break when a new ast.rs
// variant is added — this makes it impossible to introduce a construct
// silently: a developer forced to map a new variant in the walker cannot
// satisfy the compile error with a fresh, unregistered id string (this test
// goes RED), and cannot leave the variant unmapped (the match won't compile).
#[test]
fn constructs_in_only_emits_registered_ids() {
    let registry = load_registry();
    let ids: BTreeSet<&str> = registry.constructs.iter().map(|c| c.id.as_str()).collect();

    // Walk every registry probe that parses; the supported probes collectively
    // exercise every walker emit branch that a registered construct reaches.
    let mut emitted: BTreeSet<String> = BTreeSet::new();
    for c in &registry.constructs {
        if let ProbeClass::Parses(e) = classify(&c.probe) {
            emitted.extend(constructs_in(&e));
        }
    }
    // Also fold in the e2e case trees for extra breadth.
    for case in &load_e2e_fixture().cases {
        if let ProbeClass::Parses(e) = classify(&substitute_placeholders(&case.query)) {
            emitted.extend(constructs_in(&e));
        }
    }

    let unregistered: Vec<&String> = emitted
        .iter()
        .filter(|id| !ids.contains(id.as_str()))
        .collect();
    assert!(
        unregistered.is_empty(),
        "constructs_in emits ids that are not registered constructs — a new ast.rs variant was \
         mapped to an unregistered id: {unregistered:?}"
    );
}

// #11
#[test]
fn coverage_map_ids_all_resolve() {
    let registry = load_registry();
    let cov = load_coverage_map();
    let ids: BTreeSet<&str> = registry.constructs.iter().map(|c| c.id.as_str()).collect();
    for s in &cov.sources {
        for c in &s.constructs {
            assert!(
                ids.contains(c.as_str()),
                "coverage source {:?} maps to unknown construct {c:?} — evidence maps to no construct",
                s.source
            );
        }
    }
}

// #12 — the per-construct silent-gap gate.
#[test]
fn every_supported_construct_is_exercise_proven_by_its_probe() {
    let registry = load_registry();
    let disp = load_dispositions();
    let status: BTreeMap<&str, Status> = disp
        .entries
        .iter()
        .map(|d| (d.construct.as_str(), d.status))
        .collect();
    for c in &registry.constructs {
        if status.get(c.id.as_str()) != Some(&Status::Supported) {
            continue;
        }
        match classify(&c.probe) {
            ProbeClass::Parses(e) => {
                let exercised = constructs_in(&e);
                assert!(
                    exercised.contains(&c.id),
                    "{}: supported probe {:?} parses but its AST does not exercise its own id \
                     (exercised: {exercised:?})",
                    c.id,
                    c.probe
                );
            }
            other => panic!(
                "{}: supported probe {:?} did not parse: {other:?}",
                c.id, c.probe
            ),
        }
    }
}

// #13 — the 31 e2e cases exercise their mapped constructs.
#[test]
fn e2e_cases_exercise_their_mapped_constructs() {
    let fixture = load_e2e_fixture();
    let cov = load_coverage_map();
    let disp = load_dispositions();
    let status: BTreeMap<&str, Status> = disp
        .entries
        .iter()
        .map(|d| (d.construct.as_str(), d.status))
        .collect();
    let ledger = load_ledger();
    let ledgered: BTreeSet<&str> = ledger.entries.iter().map(|e| e.case_id.as_str()).collect();

    let mapped: BTreeMap<&str, &Vec<String>> = cov
        .sources
        .iter()
        .filter(|s| s.kind == "e2e-differential")
        .map(|s| (s.source.as_str(), &s.constructs))
        .collect();

    for case in &fixture.cases {
        let constructs = mapped.get(case.case_id.as_str()).unwrap_or_else(|| {
            panic!(
                "e2e case {:?} has no e2e-differential coverage-map source",
                case.case_id
            )
        });
        let query = substitute_placeholders(&case.query);
        let exercised = match classify(&query) {
            ProbeClass::Parses(e) => constructs_in(&e),
            other => {
                assert!(
                    ledgered.contains(case.case_id.as_str()),
                    "{}: does not parse ({other:?}) and is not ledgered",
                    case.case_id
                );
                continue;
            }
        };
        for id in *constructs {
            assert!(
                exercised.contains(id),
                "{}: mapped construct {id:?} is not exercised by the case's parse tree \
                 (exercised: {exercised:?})",
                case.case_id
            );
            assert_eq!(
                status.get(id.as_str()),
                Some(&Status::Supported),
                "{}: mapped construct {id:?} must be `supported`",
                case.case_id
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Negative-fixture unit tests — prove every RED path
// ---------------------------------------------------------------------------

fn divergence_fixture() -> Disposition {
    Disposition {
        construct: "fixture.divergence".to_string(),
        status: Status::Divergence,
        oracle: Oracle::Reject,
        error_construct: None,
        evidence: vec![],
        owning_issue: Some(190),
        justification: Some("parity genuinely infeasible for reason X".to_string()),
        oracle_citation: Some(format!("{DOCS_PREFIX}latest/query/log_queries/#x")),
        owner_escalation: Some(format!("{REPO_PREFIX}issues/191#issuecomment-1")),
    }
}

#[test]
fn undispositioned_construct_is_red() {
    let ids = vec!["a".to_string(), "b".to_string()];
    let disp = vec!["a".to_string()]; // b has no disposition
    assert!(check_bijection(&ids, &disp).is_err());
}

#[test]
fn orphan_disposition_is_red() {
    let ids = vec!["a".to_string()];
    let disp = vec!["a".to_string(), "ghost".to_string()];
    assert!(check_bijection(&ids, &disp).is_err());
}

#[test]
fn a_well_formed_divergence_fixture_validates() {
    check_divergence(&divergence_fixture()).expect("the reference fixture is valid");
}

#[test]
fn divergence_without_owner_escalation_is_red() {
    let mut d = divergence_fixture();
    d.owner_escalation = None;
    assert!(check_divergence(&d).is_err());
}

#[test]
fn divergence_with_non_repo_owner_escalation_is_red() {
    let mut d = divergence_fixture();
    d.owner_escalation = Some("https://example.com/not-a-ruling".to_string());
    assert!(check_divergence(&d).is_err());
}

#[test]
fn divergence_without_oracle_citation_is_red() {
    let mut d = divergence_fixture();
    d.oracle_citation = None;
    assert!(check_divergence(&d).is_err());
}

#[test]
fn divergence_with_non_docs_oracle_citation_is_red() {
    let mut d = divergence_fixture();
    d.oracle_citation = Some("https://example.com/spec".to_string());
    assert!(check_divergence(&d).is_err());
}

#[test]
fn divergence_with_empty_justification_is_red() {
    let mut d = divergence_fixture();
    d.justification = Some("   ".to_string());
    assert!(check_divergence(&d).is_err());
}

#[test]
fn interim_named_without_error_construct_is_red() {
    let d = Disposition {
        construct: "fixture.named".to_string(),
        status: Status::InterimNamed,
        oracle: Oracle::Accept,
        error_construct: None, // missing
        evidence: vec![],
        owning_issue: Some(190),
        justification: None,
        oracle_citation: None,
        owner_escalation: None,
    };
    assert!(check_status(&d, r#"{app="x"} | unpack"#).is_err());
}

#[test]
fn interim_named_without_owning_issue_is_red() {
    let d = Disposition {
        construct: "fixture.named".to_string(),
        status: Status::InterimNamed,
        oracle: Oracle::Accept,
        error_construct: Some("unpack".to_string()),
        evidence: vec![],
        owning_issue: None, // missing
        justification: None,
        oracle_citation: None,
        owner_escalation: None,
    };
    assert!(check_status(&d, r#"{app="x"} | unpack"#).is_err());
}

#[test]
fn interim_generic_without_owning_issue_is_red() {
    let d = Disposition {
        construct: "fixture.generic".to_string(),
        status: Status::InterimGeneric,
        oracle: Oracle::Accept,
        error_construct: None,
        evidence: vec![],
        owning_issue: None,
        justification: None,
        oracle_citation: None,
        owner_escalation: None,
    };
    // A probe that genuinely produces a generic error, so only the missing
    // owning_issue can fail it.
    assert!(check_status(&d, r#"sort(rate({app="x"}[5m]))"#).is_err());
}

#[test]
fn interim_named_mislabelled_as_generic_is_red() {
    // A probe that names a boundary cannot be dispositioned interim-generic.
    let d = Disposition {
        construct: "fixture.named-as-generic".to_string(),
        status: Status::InterimGeneric,
        oracle: Oracle::Accept,
        error_construct: None,
        evidence: vec![],
        owning_issue: Some(190),
        justification: None,
        oracle_citation: None,
        owner_escalation: None,
    };
    assert!(check_status(&d, r#"{app="x"} | unpack"#).is_err());
}

#[test]
fn stale_ledger_entry_that_now_parses_is_red() {
    // The monotone-shrink guard: a case that resolves to `Ok`/`Named` is no
    // longer a generic failure and must be dropped from the ledger.
    assert!(matches!(classify(r#"{app="x"}"#), ProbeClass::Parses(_)));
    assert_ne!(classify(r#"{app="x"}"#), ProbeClass::Generic);
}

#[test]
fn supported_probe_not_exercising_its_own_id_would_be_red() {
    // The #12 mechanism: a supported construct whose probe does not exercise
    // its own id fails. `{app="x"}` exercises `matcher.eq`, never `agg.sum`.
    let e = match classify(r#"{app="x"}"#) {
        ProbeClass::Parses(e) => e,
        other => panic!("expected parse, got {other:?}"),
    };
    let ex = constructs_in(&e);
    assert!(ex.contains("matcher.eq"));
    assert!(!ex.contains("agg.sum"));
}

#[test]
fn e2e_mapping_to_an_absent_construct_would_be_red() {
    // The #13 mechanism: a mapping to a construct not in the case's tree
    // fails. `{app="x"} | json` exercises `parser.json`, never `agg.sum`.
    let e = match classify(r#"{app="x"} | json"#) {
        ProbeClass::Parses(e) => e,
        other => panic!("expected parse, got {other:?}"),
    };
    let ex = constructs_in(&e);
    assert!(ex.contains("parser.json"));
    assert!(!ex.contains("agg.sum"));
}
