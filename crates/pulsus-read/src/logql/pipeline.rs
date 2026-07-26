//! `CompiledPipeline` — the in-engine, post-scan per-line evaluator for
//! LogQL pipeline stages (issue M6-09). Parsers (`json`/`logfmt`/
//! `regexp`/`pattern`), label filters, `line_format`, and `label_format`
//! are opaque to the columnar store (they read the log body), so they
//! evaluate here, over rows stage 3 already fetched — **after** line
//! filters pushed down to the `tokenbf_v1` skip index / PREWHERE reduced
//! the row set (features.md §2; the pushdown itself is
//! [`super::plan::compile_line_filters`]'s job and is untouched by this
//! module).
//!
//! **Allocation discipline (the hot path):** every regex and template is
//! compiled exactly once per query in [`CompiledPipeline::compile`];
//! [`CompiledPipeline::run`] borrows from the body and base labels via
//! `Cow` wherever no rewrite occurs. The `json` stage pays one
//! `serde_json` parse per *surviving* line — bounded by the pushdown-
//! reduced, `LIMIT`-capped scan.
//!
//! **Pushdown split:** line filters positioned *before* the first
//! `line_format` stage reference the original `body` and are pushed down
//! to SQL — [`CompiledPipeline::compile`] skips them (evaluating them
//! twice would be correct but wasted work). Line filters *after* a
//! `line_format` reference the rewritten line and evaluate here.
//!
//! **Pinned semantics** (Tier-1 goldens in
//! `tests/logql_pipeline_golden.rs`; the runtime differential against the
//! pinned oracle container is the e2e parity gate):
//! - `json` flattens nested objects with `_` separators, stringifies
//!   scalars, skips `null`s and arrays; a malformed line is **kept** with
//!   `__error__="JSONParserErr"`.
//! - `logfmt` splits `k=v` pairs (quoted values unescaped); the default is
//!   lenient best-effort — empty-value pairs are dropped and a malformed
//!   line NEVER sets `__error__` (issue #200, reference-matching). The
//!   `--strict` flag tags a malformed line with `__error__="LogfmtParserErr"`;
//!   `--keep-empty` retains empty-value pairs.
//! - `unpack` parses a packed JSON object, promoting a string `_entry` back
//!   to the line and other string fields to labels; a non-object line is
//!   kept with `__error__="JSONParserErr"`.
//! - `decolorize` strips ANSI SGR color escapes; `drop`/`keep` remove/retain
//!   labels (optionally value-matched).
//! - `regexp` named groups become labels; a non-matching line adds no
//!   labels and is kept.
//! - `pattern` `<name>` captures between literal delimiters, `<_>`
//!   discards; a non-matching line adds no labels and is kept.
//! - An extracted label colliding with an existing one lands under
//!   `<name>_extracted`.
//! - String label filters match against the empty string when the label
//!   is missing; numeric label filters drop lines missing the label and
//!   keep (with `__error__="LabelFilterErr"`) lines whose value fails
//!   unit conversion.
//! - `line_format`/`label_format` templates support the `{{.label}}`
//!   field-substitution + literal-text subset; every excluded template
//!   function is individually enumerated
//!   ([`EXCLUDED_TEMPLATE_FUNCTIONS`]) and rejected by name.
//! - One `label_format` stage renders every template against a single
//!   data-map **snapshot** of the label set, so an assignment never
//!   observes an earlier one in the same stage; and `__error__` is not
//!   assignable at all (issue #231, both live-probed).

use std::borrow::Cow;
use std::fmt;
use std::net::IpAddr;

use pulsus_logql::{
    CompareOp, DropKeepElem, LabelFilterExpr, LabelFmt, LabelMatch, LineFilterOp, MatchOp,
    NumericLiteral, ParserStage, Stage,
};

use super::ip::{IpMatcher, line_has_ip_in};
// Shared Go-stdlib string-quoting ports (issue #70): `go_quote` mirrors
// Go stdlib `strconv.Quote` (number branch), `go_time_quote` mirrors Go
// stdlib `time`'s internal `quote` (duration branch) — reused here so the
// `__error_details__` value is byte-exact for ALL label values, not just
// plain ASCII.
use pulsus_promql::eval::quote::{go_quote, go_time_quote};

/// The label carrying parser/filter failure classes (pinned values:
/// `JSONParserErr`, `LogfmtParserErr`, `LabelFilterErr`), filterable like
/// any other label — `| __error__ = ""` drops errored lines.
pub const ERROR_LABEL: &str = "__error__";

/// The companion to [`ERROR_LABEL`]: Loki's human-readable per-error
/// detail (`__error_details__`), set alongside `__error__` at every error
/// site on BOTH the streams (issue #99) and metric (issue #104) paths —
/// the metric `pipeline error: '…' for series: '{…}'` message carries the
/// same byte-exact detail as the streams surface (oracle-confirmed vs
/// grafana/loki:3.4.2; see `tests/golden/logql_error_details/oracle_probe.txt`).
/// Sorts AFTER `__error__` lexically (`"__error__" < "__error_details__"`),
/// so the emitted sorted `labels_json` stays canonical with no plumbing.
pub const ERROR_DETAILS_LABEL: &str = "__error_details__";

/// Loki (grafana/loki:3.4.2, buger/jsonparser) reports this fixed detail
/// for a top-level non-object line — the representative `JSONParserErr`
/// class (oracle_probe.txt [1]). Partial-object inputs take an
/// internal-scanner-state-dependent message (and Loki partially extracts
/// them); those are ledgered off-corpus, not reproduced.
const JSON_ERROR_DETAILS: &str = "Value looks like object, but can't find closing '}' symbol";

/// Errors from compiling a pipeline — all client-caused, surfaced as
/// [`super::error::ReadError::PipelineInvalid`] (400-class).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineError {
    BadRegex(String),
    UnsupportedTemplate(String),
    BadParserExpr(String),
    BadIpFilter(String),
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineError::BadRegex(msg) => write!(f, "bad regex: {msg}"),
            PipelineError::UnsupportedTemplate(name) => write!(
                f,
                "unsupported template function `{name}`: line_format/label_format support \
                 only `{{{{.label}}}}` field substitution and literal text"
            ),
            PipelineError::BadParserExpr(msg) => write!(f, "bad parser expression: {msg}"),
            PipelineError::BadIpFilter(msg) => write!(f, "bad ip() label filter: {msg}"),
        }
    }
}

impl std::error::Error for PipelineError {}

/// One line's pipeline output: the (possibly rewritten) line and the
/// final label set. `labels` is unsorted here; callers sort at emit.
#[derive(Debug)]
pub struct EntryOut<'a> {
    pub line: Cow<'a, str>,
    pub labels: Vec<(Cow<'a, str>, Cow<'a, str>)>,
}

/// Which unit family a numeric label filter compares in — decided at
/// compile time from the RHS literal (plan v1 contract: duration units →
/// f64 seconds, bytes units → f64 bytes, plain number → f64), then
/// applied symmetrically to the label value at run time. Issue M6-10
/// reuses the same families for `unwrap` conversions (`unwrap x` →
/// `Number`, `unwrap duration(x)`/`duration_seconds(x)` → `Duration`,
/// `unwrap bytes(x)` → `Bytes`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnitKind {
    Number,
    Duration,
    Bytes,
}

/// The pinned `__error__` value for a failed `unwrap` conversion on the
/// metric path — oracle-probed (issue M6-10 plan v2 D1: live probe
/// against the pinned reference, which reports `pipeline error:
/// 'SampleExtractionErr' ...` and tags the failed line's label set with
/// `__error__="SampleExtractionErr"`).
pub const SAMPLE_EXTRACTION_ERROR: &str = "SampleExtractionErr";

/// One line's metric-mode pipeline outcome (issue M6-10 plan v2 D1).
#[derive(Debug)]
pub enum MetricRun<'a> {
    /// A stage dropped the line (a filter miss, or the unwrap label was
    /// absent — the oracle silently skips label-less lines, probed live).
    Dropped,
    /// The line survived the full pipeline. `value` is `Some` when an
    /// unwrap conversion succeeded, `None` when the pipeline has no
    /// unwrap stage or the conversion failed (in which case `__error__`
    /// was set in time for downstream filters to consume — a SURVIVING
    /// nonempty `__error__` must fail the metric query, adjudication #1).
    Kept {
        line: Cow<'a, str>,
        value: Option<f64>,
    },
}

/// One alternative of a client-side line filter (M8-LQ2 `linefilter.or`).
/// A filter's alternatives are an `or` disjunction: the stage matches iff
/// ANY alternative matches the current line. An `ip("…")` head/alternative
/// compiles to [`LineMatcher::Ip`] (a range test with no token prefilter,
/// so [`super::plan::is_pushable_line_filter`] keeps it off the SQL push-
/// down and it is evaluated here); a plain value compiles to `Literal`
/// (`|=`/`!=`) or `Regex` (`|~`/`!~`).
#[derive(Debug)]
enum LineMatcher {
    Literal(String),
    Regex(regex::Regex),
    Ip(IpMatcher),
}

#[derive(Debug)]
enum CompiledLabelFilter {
    Match {
        name: String,
        op: MatchOp,
        value: String,
        /// Compiled, fully-anchored (Prometheus matcher semantics) — only
        /// for `Re`/`Nre`.
        re: Option<regex::Regex>,
    },
    Compare {
        name: String,
        op: CompareOp,
        kind: UnitKind,
        threshold: f64,
    },
    /// IP form (M8-LQ2 `labelfilter.ip`): `name = ip("…")` /
    /// `name != ip("…")`. The label value is parsed as an IP and tested for
    /// membership in the compiled range. Unlike the numeric `Compare` filter,
    /// this NEVER errors: a missing label or an unparseable value is simply a
    /// non-match (reference v3.7.3-verified — no `__error__`/`__error_details__`
    /// is ever set). `=` drops the non-match; `!=` keeps it.
    Ip {
        name: String,
        matcher: IpMatcher,
        negated: bool,
    },
    And(Box<CompiledLabelFilter>, Box<CompiledLabelFilter>),
    Or(Box<CompiledLabelFilter>, Box<CompiledLabelFilter>),
}

/// One `json` extraction path segment (`a.b[0].c` / `a["k"]` shapes).
#[derive(Debug, Clone, PartialEq)]
enum JsonPathSeg {
    Field(String),
    Index(usize),
}

#[derive(Debug)]
enum PatternTok {
    Literal(String),
    Capture(String),
    Discard,
}

#[derive(Debug)]
enum TmplPart {
    Lit(String),
    Field(String),
}

#[derive(Debug)]
enum CompiledStage {
    LineFilter {
        /// `or`-disjunction alternatives; the stage matches iff ANY matches.
        matchers: Vec<LineMatcher>,
        /// `!=`/`!~` invert the disjunction (keep iff NONE match).
        negated: bool,
    },
    Json {
        /// Empty = full flatten.
        extractions: Vec<(String, Vec<JsonPathSeg>)>,
    },
    Logfmt {
        /// `--strict`: a malformed line sets `__error__="LogfmtParserErr"`
        /// (default is lenient best-effort, no error — issue #200).
        strict: bool,
        /// `--keep-empty`: retain extracted pairs whose value is empty
        /// (default drops them — issue #200).
        keep_empty: bool,
        /// Empty = all pairs; else `(label, source_key)`.
        extractions: Vec<(String, String)>,
    },
    Regexp(regex::Regex),
    Pattern(Vec<PatternTok>),
    LabelFilter(CompiledLabelFilter),
    LineFormat(Vec<TmplPart>),
    LabelFormat {
        fmts: Vec<CompiledLabelFmt>,
        /// Whether this stage must render its templates against the
        /// reference's per-stage data-map **snapshot** instead of the live
        /// label vector (issue #231). Decided once at compile time by
        /// [`label_format_needs_snapshot`]: `false` — the overwhelmingly
        /// common shape — means both render identically, so the stage keeps
        /// the copy-free live path.
        needs_snapshot: bool,
    },
    /// `| unwrap <label>` / `| unwrap <conversion>(<label>)` — evaluated
    /// **only** on the metric path ([`CompiledPipeline::run_metric_into`]);
    /// inert for stream execution (issue M6-10 adjudication #2: the
    /// non-metric path must not execute the conversion or mutate
    /// `__error__`).
    Unwrap {
        label: String,
        kind: UnitKind,
    },
    /// `| unpack` — parse the line as a packed JSON object, promoting a
    /// string `_entry` field back to the line and other string fields to
    /// labels; a non-object/parse-failure keeps the line with
    /// `__error__="JSONParserErr"` (issue #200). Rewrites the line.
    Unpack,
    /// `| decolorize` — strip ANSI SGR escapes (`\x1b[…m`) from the line
    /// (issue #200); borrowed/zero-alloc when nothing matches. Rewrites
    /// the line.
    Decolorize(regex::Regex),
    /// `| drop <elems>` — remove each listed label when it matches (issue
    /// #200); dropping `__error__` also clears `__error_details__`.
    Drop(Vec<CompiledDropKeep>),
    /// `| keep <elems>` — retain only labels matched by the list (issue
    /// #200).
    Keep(Vec<CompiledDropKeep>),
}

/// One compiled `drop`/`keep` element: a label name plus an optional
/// value matcher (regexes compiled once, fully anchored).
#[derive(Debug)]
struct CompiledDropKeep {
    label: String,
    matcher: Option<CompiledDropKeepMatch>,
}

#[derive(Debug)]
struct CompiledDropKeepMatch {
    op: MatchOp,
    value: String,
    /// Anchored regex for `Re`/`Nre`; `None` for `Eq`/`Neq`.
    re: Option<regex::Regex>,
}

impl CompiledDropKeepMatch {
    /// Whether this matcher matches the given label value (Prometheus
    /// matcher semantics — the same anchoring the label-filter family uses).
    fn matches(&self, value: &str) -> bool {
        match self.op {
            MatchOp::Eq => value == self.value,
            MatchOp::Neq => value != self.value,
            MatchOp::Re => self.re.as_ref().is_some_and(|re| re.is_match(value)),
            MatchOp::Nre => !self.re.as_ref().is_some_and(|re| re.is_match(value)),
        }
    }
}

#[derive(Debug)]
enum CompiledLabelFmt {
    Rename { dst: String, src: String },
    Template { dst: String, tmpl: Vec<TmplPart> },
}

/// The compiled, reusable per-line evaluator (consumed by the streams
/// read path here and by the M6-10 metric-pipeline seam later).
#[derive(Debug)]
pub struct CompiledPipeline {
    stages: Vec<CompiledStage>,
    mutates_labels: bool,
    rewrites_line: bool,
    line_filter_only: bool,
    has_unwrap: bool,
}

impl CompiledPipeline {
    /// Compiles `stages` once per query: regexes, templates, extraction
    /// paths, and numeric RHS literals are all validated/compiled here so
    /// [`CompiledPipeline::run`] never parses anything but the log line.
    ///
    /// `Stage::Unwrap` compiles to a stage that only the metric-mode
    /// entrypoint ([`CompiledPipeline::run_metric_into`]) evaluates
    /// (issue M6-10 plan v2 D1); the streams path keeps it inert
    /// (adjudication #2) — and the planner still rejects `unwrap` on
    /// bare log queries via `PipelineInvalid` before it could reach
    /// `run` anyway.
    pub fn compile(stages: &[Stage]) -> Result<Self, PipelineError> {
        let mut compiled = Vec::new();
        let mut seen_line_format = false;
        let mut mutates_labels = false;
        let mut rewrites_line = false;
        let mut has_unwrap = false;

        for stage in stages {
            match stage {
                Stage::LineFilter(lf) => {
                    // A line filter is pushed down to SQL (and skipped here to
                    // avoid double evaluation) ONLY when it both precedes the
                    // first `line_format` (it references the original `body`)
                    // AND is pushable — i.e. no alternative is an `ip("…")`.
                    // An `ip(…)`/mixed-`or` filter, or any filter following a
                    // `line_format`, is served here client-side, matching
                    // `plan::compile_line_filters`'s pushdown split exactly.
                    if !seen_line_format && super::plan::is_pushable_line_filter(lf) {
                        continue;
                    }
                    let mut matchers = Vec::with_capacity(1 + lf.or_matches.len());
                    for (value, is_ip) in lf.alternatives() {
                        let matcher = if is_ip {
                            // `ip("…")` compiles to a range matcher regardless
                            // of the outer op (`ip(…)` is only ever `|=`/`!=`).
                            LineMatcher::Ip(
                                IpMatcher::parse(value)
                                    .map_err(|e| PipelineError::BadIpFilter(e.to_string()))?,
                            )
                        } else {
                            match lf.op {
                                LineFilterOp::Contains | LineFilterOp::NotContains => {
                                    LineMatcher::Literal(value.to_string())
                                }
                                LineFilterOp::Regex | LineFilterOp::NotRegex => {
                                    // Unanchored, like the SQL pushdown's
                                    // `match(body, ...)`.
                                    LineMatcher::Regex(compile_regex(value)?)
                                }
                            }
                        };
                        matchers.push(matcher);
                    }
                    let negated =
                        matches!(lf.op, LineFilterOp::NotContains | LineFilterOp::NotRegex);
                    compiled.push(CompiledStage::LineFilter { matchers, negated });
                }
                Stage::Parser(p) => {
                    mutates_labels = true;
                    compiled.push(compile_parser(p)?);
                }
                Stage::LabelFilter(expr) => {
                    let filter = compile_label_filter(expr)?;
                    // A numeric comparison can add `__error__` on a
                    // conversion failure — that changes the label set, so
                    // it must route through the fan-out path (correctness
                    // refinement over the plan's parser/label_format-only
                    // trigger; flagged in the implementation notes).
                    if filter_contains_compare(&filter) {
                        mutates_labels = true;
                    }
                    compiled.push(CompiledStage::LabelFilter(filter));
                }
                Stage::LineFormat(tmpl) => {
                    seen_line_format = true;
                    rewrites_line = true;
                    compiled.push(CompiledStage::LineFormat(compile_template(tmpl)?));
                }
                Stage::LabelFormat(fmts) => {
                    mutates_labels = true;
                    compiled.push(compile_label_format(fmts)?);
                }
                Stage::Unwrap(u) => {
                    has_unwrap = true;
                    let kind = match u.conversion.as_deref() {
                        None => UnitKind::Number,
                        Some("duration") | Some("duration_seconds") => UnitKind::Duration,
                        Some("bytes") => UnitKind::Bytes,
                        // The parser only emits the three conversions in
                        // `UNWRAP_CONVERSIONS`; anything else is a named
                        // defensive rejection, never a silent Number.
                        Some(other) => {
                            return Err(PipelineError::BadParserExpr(format!(
                                "unknown unwrap conversion {other:?}"
                            )));
                        }
                    };
                    // Deliberately does NOT set `mutates_labels`: the
                    // streams path never executes unwrap (it stays
                    // byte-identical with/without a trailing unwrap —
                    // adjudication #2); the metric path's grouping keys
                    // off `metric_mutates_labels()` instead.
                    compiled.push(CompiledStage::Unwrap {
                        label: u.label.clone(),
                        kind,
                    });
                }
                Stage::Unpack => {
                    // Unpack rewrites the line (`_entry` becomes the line) and
                    // promotes fields to labels — a following line filter must
                    // therefore evaluate in-engine, so it sets the line-rewrite
                    // gate exactly like `line_format`.
                    mutates_labels = true;
                    rewrites_line = true;
                    seen_line_format = true;
                    compiled.push(CompiledStage::Unpack);
                }
                Stage::Decolorize => {
                    // Decolorize rewrites the line; a following line filter must
                    // evaluate in-engine (the raw body still carries the color
                    // codes ClickHouse would match against).
                    rewrites_line = true;
                    seen_line_format = true;
                    compiled.push(CompiledStage::Decolorize(compile_regex(
                        DECOLORIZE_PATTERN,
                    )?));
                }
                Stage::Drop(elems) => {
                    mutates_labels = true;
                    compiled.push(CompiledStage::Drop(compile_drop_keep(elems)?));
                }
                Stage::Keep(elems) => {
                    mutates_labels = true;
                    compiled.push(CompiledStage::Keep(compile_drop_keep(elems)?));
                }
            }
        }

        // Fast path only when the pipeline is line filters AND every one
        // pushed down (nothing compiled to run). A non-pushable `ip(…)`/
        // mixed-`or` filter compiles a run-stage, so `compiled` is non-empty
        // and the fast path is (correctly) declined — otherwise exec would
        // skip its client-side evaluation entirely.
        let line_filter_only =
            compiled.is_empty() && stages.iter().all(|s| matches!(s, Stage::LineFilter(_)));

        Ok(CompiledPipeline {
            stages: compiled,
            mutates_labels,
            rewrites_line,
            line_filter_only,
            has_unwrap,
        })
    }

    /// The pipeline can change a stream's label set (a parser, a
    /// `label_format`, or a numeric label filter's `__error__`) — the
    /// fan-out-path gate.
    pub fn mutates_labels(&self) -> bool {
        self.mutates_labels
    }

    /// The pipeline rewrites the line text (`line_format`).
    pub fn rewrites_line(&self) -> bool {
        self.rewrites_line
    }

    /// Fast-path gate: the whole pipeline is line filters, all of which
    /// pushed down to SQL — `run` would be the identity.
    pub fn is_line_filter_only(&self) -> bool {
        self.line_filter_only
    }

    /// The pipeline contains an `unwrap` stage (issue M6-10).
    pub fn has_unwrap(&self) -> bool {
        self.has_unwrap
    }

    /// The METRIC-mode fan-out gate: on the metric path a successful
    /// unwrap also changes the label set (the unwrapped label is deleted
    /// from the series — oracle-probed), so client-side aggregation must
    /// group by final label set whenever the pipeline mutates labels OR
    /// unwraps.
    pub fn metric_mutates_labels(&self) -> bool {
        self.mutates_labels || self.has_unwrap
    }

    /// Runs one line through the pipeline, allocating a fresh label
    /// vector — the plan-contract convenience shape. Hot loops use
    /// [`CompiledPipeline::run_into`] with a reused scratch instead
    /// (issue #72 review round 1, finding 3).
    pub fn run<'a>(&'a self, body: &'a str, base: &'a [(String, String)]) -> Option<EntryOut<'a>> {
        let mut labels = Vec::new();
        let line = self.run_into(body, base, &mut labels)?;
        Some(EntryOut { line, labels })
    }

    /// Runs one line through the pipeline into a caller-owned label
    /// buffer (cleared first, capacity reused across rows — all rows in
    /// one query share the `'a` of the fetched row set, so one scratch
    /// vector serves the whole loop). Returns the final line, `None`
    /// when a stage drops it; on `Some`, `labels` holds the final label
    /// set. Values borrow from `body`/`base`/the compiled stages
    /// wherever no rewrite/unescape is needed.
    ///
    /// **Streams-path contract (issue M6-10 adjudication #2):** `unwrap`
    /// stages are inert here — no conversion runs, no `__error__` is
    /// set, no label is removed. Output is byte-identical with and
    /// without a trailing `| unwrap x` (regression-tested below).
    pub fn run_into<'a>(
        &'a self,
        body: &'a str,
        base: &'a [(String, String)],
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    ) -> Option<Cow<'a, str>> {
        match self.run_mode_into(body, base, labels, false) {
            MetricRun::Dropped => None,
            MetricRun::Kept { line, .. } => Some(line),
        }
    }

    /// The metric-path entrypoint (issue M6-10 plan v2 D1): identical to
    /// [`CompiledPipeline::run_into`] except `unwrap` stages EXECUTE — a
    /// successful conversion yields `value = Some(v)` and deletes the
    /// unwrapped label from the set (oracle-probed); a failed conversion
    /// sets `__error__="SampleExtractionErr"` plus the byte-exact
    /// `__error_details__` detail (issue #104) and keeps the raw label,
    /// matching the oracle's failed-series shape, then continues so
    /// post-unwrap `__error__` filters process it in pipeline order; a
    /// MISSING unwrap label drops the line (the oracle silently skips
    /// those, never erroring — probed live).
    pub fn run_metric_into<'a>(
        &'a self,
        body: &'a str,
        base: &'a [(String, String)],
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    ) -> MetricRun<'a> {
        self.run_mode_into(body, base, labels, true)
    }

    fn run_mode_into<'a>(
        &'a self,
        body: &'a str,
        base: &'a [(String, String)],
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
        metric: bool,
    ) -> MetricRun<'a> {
        let mut line: Cow<'a, str> = Cow::Borrowed(body);
        let mut value: Option<f64> = None;
        labels.clear();
        labels.extend(
            base.iter()
                .map(|(k, v)| (Cow::Borrowed(k.as_str()), Cow::Borrowed(v.as_str()))),
        );

        for stage in &self.stages {
            match stage {
                CompiledStage::LineFilter { matchers, negated } => {
                    // `or` disjunction: the line hits iff ANY alternative
                    // matches the current (possibly `line_format`-rewritten)
                    // line bytes. `ip("…")` alternatives scan the line for an
                    // in-range address; literal/regex alternatives use the
                    // substring/regex test.
                    let hit = matchers.iter().any(|m| match m {
                        LineMatcher::Literal(lit) => line.contains(lit.as_str()),
                        LineMatcher::Regex(re) => re.is_match(&line),
                        LineMatcher::Ip(matcher) => line_has_ip_in(matcher, &line),
                    });
                    // `!=`/`!~` keep the line iff NONE of the alternatives match.
                    let keep = if *negated { !hit } else { hit };
                    if !keep {
                        return MetricRun::Dropped;
                    }
                }
                CompiledStage::Json { extractions } => run_json(&line, extractions, labels),
                CompiledStage::Logfmt {
                    strict,
                    keep_empty,
                    extractions,
                } => {
                    // Borrow captures from the body slice when the line
                    // is still the original body; a rewritten
                    // (`line_format`-owned) line cannot be borrowed past
                    // its own reassignment, so its captures are copied.
                    match &line {
                        Cow::Borrowed(text) => {
                            run_logfmt(text, *strict, *keep_empty, extractions, labels, |c| c)
                        }
                        Cow::Owned(text) => {
                            run_logfmt(text, *strict, *keep_empty, extractions, labels, |c| {
                                Cow::Owned(c.into_owned())
                            })
                        }
                    }
                }
                CompiledStage::Regexp(re) => {
                    // A non-matching line adds no labels and is kept.
                    match &line {
                        Cow::Borrowed(text) => {
                            if let Some(caps) = re.captures(text) {
                                for name in re.capture_names().flatten() {
                                    if let Some(m) = caps.name(name) {
                                        add_extracted(
                                            labels,
                                            Cow::Borrowed(name),
                                            Cow::Borrowed(m.as_str()),
                                        );
                                    }
                                }
                            }
                        }
                        Cow::Owned(text) => {
                            if let Some(caps) = re.captures(text) {
                                for name in re.capture_names().flatten() {
                                    if let Some(m) = caps.name(name) {
                                        add_extracted(
                                            labels,
                                            Cow::Borrowed(name),
                                            Cow::Owned(m.as_str().to_string()),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                CompiledStage::Pattern(tokens) => {
                    // Two-pass: validate the full match first (a
                    // non-matching line must add NO labels), then commit
                    // — body-slice borrows on the original line, copies
                    // on a rewritten one; no intermediate vector either
                    // way. Capture names borrow from the compiled tokens.
                    match &line {
                        Cow::Borrowed(text) => {
                            if walk_pattern(text, tokens, &mut |_, _| {}) {
                                walk_pattern(text, tokens, &mut |name, value| {
                                    add_extracted(
                                        labels,
                                        Cow::Borrowed(name),
                                        Cow::Borrowed(value),
                                    );
                                });
                            }
                        }
                        Cow::Owned(text) => {
                            if walk_pattern(text, tokens, &mut |_, _| {}) {
                                walk_pattern(text, tokens, &mut |name, value| {
                                    add_extracted(
                                        labels,
                                        Cow::Borrowed(name),
                                        Cow::Owned(value.to_string()),
                                    );
                                });
                            }
                        }
                    }
                }
                CompiledStage::LabelFilter(filter) => {
                    // Both paths capture the first offending `(kind, raw)`
                    // for the detail string: streams and metric now each
                    // carry `__error_details__` (issue #104). `failed` only
                    // borrows the raw value — an `And`/`Or` sibling can
                    // still absorb this into a definite `Some`, masking the
                    // error entirely, so masked lines never allocate.
                    let mut failed: Option<(UnitKind, &str)> = None;
                    match eval_label_filter(filter, labels, &mut failed) {
                        Some(true) => {}
                        Some(false) => return MetricRun::Dropped,
                        // Conversion failure: keep the line, tag the error
                        // class (pinned semantics, oracle-verified; a later
                        // `__error__=""` filter drops it). Build the owned
                        // detail from the borrowed `raw` before the first
                        // `set_label` mutation (NLL: the borrow of `labels`
                        // held by `failed` must end before `labels` is
                        // mutated).
                        None => {
                            let details =
                                failed.map(|(kind, value)| label_filter_error_details(kind, value));
                            set_label(
                                labels,
                                Cow::Borrowed(ERROR_LABEL),
                                Cow::Borrowed("LabelFilterErr"),
                            );
                            if let Some(details) = details {
                                set_label(
                                    labels,
                                    Cow::Borrowed(ERROR_DETAILS_LABEL),
                                    Cow::Owned(details),
                                );
                            }
                        }
                    }
                }
                CompiledStage::LineFormat(tmpl) => {
                    line = Cow::Owned(render_template(tmpl, labels));
                }
                CompiledStage::LabelFormat {
                    fmts,
                    needs_snapshot,
                } => {
                    // Reference `LabelsFormatter.Process` (fmt.go:407-434,
                    // issue #231): every template in ONE stage renders against
                    // the SAME data map, materialised lazily from the label set
                    // at the first template assignment — so an assignment never
                    // observes an earlier one in the same stage, and a rename
                    // that runs between two templates is invisible to the
                    // second. Renames themselves read the LIVE label set, so a
                    // rename placed before the first template *is* reflected.
                    //
                    // `None` here means "the map is still empty", mirroring the
                    // reference's own `if len(m) == 0 { IntoMap(m) }` refill
                    // guard: while the label set is empty the map is rebuilt at
                    // every template, which is exactly live rendering (reachable
                    // by `| drop`-ing every label — live-probed). The clone is
                    // taken at most once per line and only for the stages whose
                    // result can actually differ (`needs_snapshot`, decided at
                    // compile time), so ordinary `label_format` stages stay
                    // allocation-free here.
                    let mut snapshot: Option<Vec<(Cow<'a, str>, Cow<'a, str>)>> = None;
                    for f in fmts {
                        match f {
                            CompiledLabelFmt::Rename { dst, src } => {
                                // Loki `LabelsFormatter.Process` (fmt.go:414-419):
                                // the rename runs only when the source resolves
                                // present; an absent source is a complete no-op and
                                // dst is NOT created. A present (even empty) source
                                // still renames — matches Loki's parsed-empty case
                                // (`Set(dst,"")`+`Del(src)`). Empty *stream* labels
                                // are non-ingestable (both engines drop them at
                                // write), so this guard matches Loki for every
                                // reachable input (#226).
                                if let Some(value) = remove_label(labels, src) {
                                    set_label(labels, Cow::Borrowed(dst), value);
                                }
                            }
                            CompiledLabelFmt::Template { dst, tmpl } => {
                                let rendered = if *needs_snapshot {
                                    if snapshot.is_none() && !labels.is_empty() {
                                        snapshot = Some(labels.clone());
                                    }
                                    match &snapshot {
                                        Some(snap) => render_template(tmpl, snap),
                                        // Label set empty ⇒ the reference's map
                                        // is empty too; every field renders "".
                                        None => render_template(tmpl, labels),
                                    }
                                } else {
                                    render_template(tmpl, labels)
                                };
                                set_label(labels, Cow::Borrowed(dst), Cow::Owned(rendered));
                            }
                        }
                    }
                }
                CompiledStage::Unwrap { label, kind } => {
                    if !metric {
                        // Streams path: inert by contract (issue M6-10
                        // adjudication #2) — no conversion, no
                        // `__error__`, no label removal.
                        continue;
                    }
                    let Some(raw) = get_label(labels, label) else {
                        // Oracle-probed: a line without the unwrap label
                        // is silently skipped, never an error.
                        return MetricRun::Dropped;
                    };
                    match convert_label_value(*kind, raw) {
                        Some(v) => {
                            // Oracle-probed: a successful unwrap DELETES
                            // the unwrapped label from the series.
                            remove_label(labels, label);
                            value = Some(v);
                        }
                        None => {
                            // Oracle-probed failed-series shape: the raw
                            // label stays, `__error__` is tagged, and the
                            // line continues so a post-unwrap
                            // `__error__` filter sees it in order. The
                            // SampleExtractionErr detail is the same
                            // Go-stdlib parse-error string the label-filter
                            // family renders — `unwrap` and `| <label> <op>`
                            // share the conversion, oracle-confirmed
                            // byte-exact (issue #104). Compute the detail
                            // from `raw` before mutating `labels`.
                            let detail = label_filter_error_details(*kind, raw);
                            set_label(
                                labels,
                                Cow::Borrowed(ERROR_LABEL),
                                Cow::Borrowed(SAMPLE_EXTRACTION_ERROR),
                            );
                            set_label(
                                labels,
                                Cow::Borrowed(ERROR_DETAILS_LABEL),
                                Cow::Owned(detail),
                            );
                            value = None;
                        }
                    }
                }
                CompiledStage::Unpack => {
                    // Reads the current line; returns the promoted `_entry`
                    // (owned) when present. The immutable borrow of `line`
                    // ends when `run_unpack` returns, so reassigning `line`
                    // afterward is sound.
                    if let Some(entry) = run_unpack(line.as_ref(), labels) {
                        line = Cow::Owned(entry);
                    }
                }
                CompiledStage::Decolorize(re) => {
                    // Zero-alloc when nothing matches: `replace_all` returns
                    // a borrow, which we resolve to `None` so no owned line is
                    // built and the borrow of `line` ends before reassignment.
                    let stripped = match re.replace_all(line.as_ref(), "") {
                        Cow::Owned(s) => Some(s),
                        Cow::Borrowed(_) => None,
                    };
                    if let Some(s) = stripped {
                        line = Cow::Owned(s);
                    }
                }
                CompiledStage::Drop(elems) => {
                    for elem in elems {
                        let drop = match &elem.matcher {
                            // Bare label: drop unconditionally (a no-op when
                            // the label is absent).
                            None => true,
                            // Matcher present: drop only when the current
                            // value matches (a missing label is never a match).
                            Some(m) => get_label(labels, &elem.label).is_some_and(|v| m.matches(v)),
                        };
                        if drop {
                            remove_label(labels, &elem.label);
                            // Dropping `__error__` clears its detail companion
                            // too (they are one error record).
                            if elem.label == ERROR_LABEL {
                                remove_label(labels, ERROR_DETAILS_LABEL);
                            }
                        }
                    }
                }
                CompiledStage::Keep(elems) => {
                    // Retain only labels matched by some element (a bare
                    // element retains its label; a matcher-qualified element
                    // retains only on a value match).
                    labels.retain(|(k, v)| {
                        elems.iter().any(|elem| {
                            elem.label == k.as_ref()
                                && match &elem.matcher {
                                    None => true,
                                    Some(m) => m.matches(v),
                                }
                        })
                    });
                }
            }
        }

        MetricRun::Kept { line, value }
    }
}

// ---------------------------------------------------------------------
// Compilation
// ---------------------------------------------------------------------

fn compile_regex(pattern: &str) -> Result<regex::Regex, PipelineError> {
    regex::Regex::new(pattern).map_err(|e| PipelineError::BadRegex(e.to_string()))
}

/// The ANSI SGR (Select Graphic Rendition) color-escape pattern
/// `decolorize` strips (issue #200): an ESC (`\x1b`) `[`, zero or more
/// digits/`;`, terminated by `m`. Pinned to the documented color-code
/// semantics — broader ANSI stripping is deliberately out of scope.
const DECOLORIZE_PATTERN: &str = r"\x1b\[[0-9;]*m";

/// Compiles a `drop`/`keep` element list once per query: each element's
/// optional `Re`/`Nre` matcher is anchored-compiled here so the per-line
/// path never touches the regex compiler (issue #200).
fn compile_drop_keep(elems: &[DropKeepElem]) -> Result<Vec<CompiledDropKeep>, PipelineError> {
    elems
        .iter()
        .map(|e| {
            let matcher = match &e.matcher {
                None => None,
                Some(LabelMatch { op, value }) => Some(CompiledDropKeepMatch {
                    op: *op,
                    value: value.clone(),
                    re: match op {
                        MatchOp::Re | MatchOp::Nre => Some(compile_anchored_regex(value)?),
                        MatchOp::Eq | MatchOp::Neq => None,
                    },
                }),
            };
            Ok(CompiledDropKeep {
                label: e.label.clone(),
                matcher,
            })
        })
        .collect()
}

/// Fully-anchored matcher regex (Prometheus label-matcher semantics) —
/// the same `^(?:...)$` wrapping shape `escape::ch_regex_anchored` uses
/// for the SQL side, compiled locally for in-engine evaluation.
fn compile_anchored_regex(pattern: &str) -> Result<regex::Regex, PipelineError> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
        .map_err(|e| PipelineError::BadRegex(e.to_string()))
}

fn compile_parser(p: &ParserStage) -> Result<CompiledStage, PipelineError> {
    match p {
        ParserStage::Json { extractions } => {
            let mut compiled = Vec::with_capacity(extractions.len());
            for e in extractions {
                compiled.push((e.label.clone(), parse_json_path(&e.expression)?));
            }
            Ok(CompiledStage::Json {
                extractions: compiled,
            })
        }
        ParserStage::Logfmt {
            strict,
            keep_empty,
            extractions,
        } => Ok(CompiledStage::Logfmt {
            strict: *strict,
            keep_empty: *keep_empty,
            extractions: extractions
                .iter()
                .map(|e| (e.label.clone(), e.expression.clone()))
                .collect(),
        }),
        ParserStage::Regexp(pattern) => {
            let re = compile_regex(pattern)?;
            if re.capture_names().flatten().next().is_none() {
                return Err(PipelineError::BadParserExpr(
                    "regexp parser requires at least one named capture group".to_string(),
                ));
            }
            Ok(CompiledStage::Regexp(re))
        }
        ParserStage::Pattern(pattern) => Ok(CompiledStage::Pattern(compile_pattern(pattern)?)),
    }
}

/// Parses a `json` extraction expression: dotted fields, `[N]` array
/// indexes, and `["quoted key"]` segments (`servers[0]`,
/// `request.headers["User-Agent"]`).
fn parse_json_path(expr: &str) -> Result<Vec<JsonPathSeg>, PipelineError> {
    let bad = |msg: &str| PipelineError::BadParserExpr(format!("json expression {expr:?}: {msg}"));
    let mut segs = Vec::new();
    let bytes = expr.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'.' => {
                if segs.is_empty() {
                    return Err(bad("leading '.'"));
                }
                i += 1;
                // Review round 1 finding 2: a dot must introduce a
                // non-empty FIELD segment — `a..b`, trailing `a.`, and
                // `a.[0]` are malformed, never silently normalized.
                if i >= bytes.len() || bytes[i] == b'.' || bytes[i] == b'[' {
                    return Err(bad("'.' must be followed by a field name"));
                }
            }
            b'[' => {
                let close = expr[i..]
                    .find(']')
                    .map(|off| i + off)
                    .ok_or_else(|| bad("unclosed '['"))?;
                let inner = &expr[i + 1..close];
                if let Some(quoted) = inner.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
                    segs.push(JsonPathSeg::Field(quoted.to_string()));
                } else {
                    let idx: usize = inner
                        .parse()
                        .map_err(|_| bad("index must be a number or a quoted key"))?;
                    segs.push(JsonPathSeg::Index(idx));
                }
                i = close + 1;
            }
            _ => {
                let end = expr[i..]
                    .find(['.', '['])
                    .map(|off| i + off)
                    .unwrap_or(expr.len());
                let field = &expr[i..end];
                if field.is_empty() {
                    return Err(bad("empty path segment"));
                }
                segs.push(JsonPathSeg::Field(field.to_string()));
                i = end;
            }
        }
    }
    if segs.is_empty() {
        return Err(bad("empty expression"));
    }
    Ok(segs)
}

fn compile_pattern(pattern: &str) -> Result<Vec<PatternTok>, PipelineError> {
    let bad = |msg: &str| PipelineError::BadParserExpr(format!("pattern {pattern:?}: {msg}"));
    let mut tokens: Vec<PatternTok> = Vec::new();
    let mut rest = pattern;
    let mut captures = 0usize;
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix('<') {
            let close = after.find('>').ok_or_else(|| bad("unclosed '<'"))?;
            let name = &after[..close];
            let is_capture_name =
                !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if is_capture_name {
                let prev_is_capture = matches!(
                    tokens.last(),
                    Some(PatternTok::Capture(_) | PatternTok::Discard)
                );
                if prev_is_capture {
                    return Err(bad("consecutive captures without a literal separator"));
                }
                if name == "_" {
                    tokens.push(PatternTok::Discard);
                } else {
                    tokens.push(PatternTok::Capture(name.to_string()));
                    captures += 1;
                }
                rest = &after[close + 1..];
                continue;
            }
            // Not a capture shape (`<`, `<a b>`, …): literal text.
        }
        // Consume literal text up to the next potential capture.
        let next = rest[1..].find('<').map(|off| off + 1).unwrap_or(rest.len());
        let (lit, tail) = rest.split_at(next);
        match tokens.last_mut() {
            Some(PatternTok::Literal(existing)) => existing.push_str(lit),
            _ => tokens.push(PatternTok::Literal(lit.to_string())),
        }
        rest = tail;
    }
    if captures == 0 {
        return Err(bad("at least one named capture is required"));
    }
    Ok(tokens)
}

fn compile_label_filter(expr: &LabelFilterExpr) -> Result<CompiledLabelFilter, PipelineError> {
    Ok(match expr {
        LabelFilterExpr::Match(m) => CompiledLabelFilter::Match {
            name: m.name.clone(),
            op: m.op,
            value: m.value.clone(),
            re: match m.op {
                MatchOp::Re | MatchOp::Nre => Some(compile_anchored_regex(&m.value)?),
                MatchOp::Eq | MatchOp::Neq => None,
            },
        },
        LabelFilterExpr::Compare { name, op, rhs } => {
            let (kind, threshold) = classify_numeric_literal(rhs)?;
            CompiledLabelFilter::Compare {
                name: name.clone(),
                op: *op,
                kind,
                threshold,
            }
        }
        LabelFilterExpr::Ip {
            name,
            value,
            negated,
        } => {
            let matcher =
                IpMatcher::parse(value).map_err(|e| PipelineError::BadIpFilter(e.to_string()))?;
            CompiledLabelFilter::Ip {
                name: name.clone(),
                matcher,
                negated: *negated,
            }
        }
        LabelFilterExpr::And(a, b) => CompiledLabelFilter::And(
            Box::new(compile_label_filter(a)?),
            Box::new(compile_label_filter(b)?),
        ),
        LabelFilterExpr::Or(a, b) => CompiledLabelFilter::Or(
            Box::new(compile_label_filter(a)?),
            Box::new(compile_label_filter(b)?),
        ),
    })
}

fn filter_contains_compare(f: &CompiledLabelFilter) -> bool {
    match f {
        CompiledLabelFilter::Match { .. } => false,
        CompiledLabelFilter::Compare { .. } => true,
        // The IP label filter never mutates the label set (it cannot error),
        // so it does not force the label-mutating fan-out path.
        CompiledLabelFilter::Ip { .. } => false,
        CompiledLabelFilter::And(a, b) | CompiledLabelFilter::Or(a, b) => {
            filter_contains_compare(a) || filter_contains_compare(b)
        }
    }
}

/// Classifies a numeric RHS literal (plan edge case 4: `5xz` is a named
/// error, never a silent 0) and converts it to the comparison threshold
/// in that unit family's base (seconds / bytes / plain).
fn classify_numeric_literal(lit: &NumericLiteral) -> Result<(UnitKind, f64), PipelineError> {
    match lit {
        NumericLiteral::Number(raw) => {
            let n: f64 = raw.parse().map_err(|_| {
                PipelineError::BadParserExpr(format!("invalid numeric literal {raw:?}"))
            })?;
            Ok((UnitKind::Number, n))
        }
        NumericLiteral::DurationOrBytes(raw) => {
            if let Some(secs) = parse_duration_seconds(raw) {
                Ok((UnitKind::Duration, secs))
            } else if let Some(bytes) = parse_bytes_value(raw) {
                Ok((UnitKind::Bytes, bytes))
            } else {
                Err(PipelineError::BadParserExpr(format!(
                    "literal {raw:?} is neither a duration nor a bytes quantity"
                )))
            }
        }
    }
}

// ---------------------------------------------------------------------
// The shared unit parser (numeric label filters now; unwrap conversions
// in M6-10): duration units → f64 seconds, bytes units → f64 bytes,
// plain number → f64.
// ---------------------------------------------------------------------

const DURATION_UNITS: &[(&str, f64)] = &[
    ("ns", 1e-9),
    ("us", 1e-6),
    ("µs", 1e-6),
    ("ms", 1e-3),
    ("s", 1.0),
    ("m", 60.0),
    ("h", 3_600.0),
    ("d", 86_400.0),
    ("w", 604_800.0),
];

/// The full `dustin/go-humanize` byte-size table (`bytesSizeTable`),
/// matched case-insensitively: bare units (`k`,`ki`,…), `b`-suffixed units
/// (`kb`,`kib`,…), the empty suffix (a bare number is a byte count), and
/// the exa tier (`e`/`eb`=1e18, `ei`/`eib`=2^60). Decimal factors are
/// powers of 1000; binary factors powers of 1024.
const BYTES_UNITS: &[(&str, f64)] = &[
    ("", 1.0),
    ("b", 1.0),
    ("k", 1e3),
    ("kb", 1e3),
    ("ki", 1024.0),
    ("kib", 1024.0),
    ("m", 1e6),
    ("mb", 1e6),
    ("mi", 1024.0 * 1024.0),
    ("mib", 1024.0 * 1024.0),
    ("g", 1e9),
    ("gb", 1e9),
    ("gi", 1024.0 * 1024.0 * 1024.0),
    ("gib", 1024.0 * 1024.0 * 1024.0),
    ("t", 1e12),
    ("tb", 1e12),
    ("ti", 1024.0 * 1024.0 * 1024.0 * 1024.0),
    ("tib", 1024.0 * 1024.0 * 1024.0 * 1024.0),
    ("p", 1e15),
    ("pb", 1e15),
    ("pi", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
    ("pib", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
    ("e", 1e18),
    ("eb", 1e18),
    ("ei", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
    ("eib", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
];

/// Go's `math.MaxUint64` (2^64-1) converted to f64 rounds up to 2^64 (a
/// power of two, so exactly representable); go-humanize's overflow guard
/// `f >= math.MaxUint64` is therefore `product >= 2^64` (#226).
const BYTES_MAX_UINT64_F64: f64 = 18_446_744_073_709_551_616.0;

/// Parses a (possibly compound, possibly fractional) duration to f64
/// seconds: `250ms`, `1h30m`, `1.5s`. `None` when any component's unit is
/// not a duration unit.
pub(crate) fn parse_duration_seconds(raw: &str) -> Option<f64> {
    let mut rest = raw;
    let mut total = 0.0f64;
    let mut matched = false;
    while !rest.is_empty() {
        let num_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if num_end == 0 {
            return None;
        }
        let n: f64 = rest[..num_end].parse().ok()?;
        rest = &rest[num_end..];
        // Longest unit first: `ms` before `m`, `ns`/`us` before `s`.
        let (unit, factor) = DURATION_UNITS
            .iter()
            .filter(|(u, _)| rest.starts_with(u))
            .max_by_key(|(u, _)| u.len())?;
        total += n * factor;
        rest = &rest[unit.len()..];
        matched = true;
    }
    matched.then_some(total)
}

/// Parses a bytes quantity to f64 bytes — a faithful port of
/// `dustin/go-humanize` `ParseBytes` (probed against grafana/loki:3.7.4):
/// `512b`, `5KB`, `1MiB`, bare `1k`/`1ki`, `1,024`, `3 kB`, exa `1eb`. The
/// leading `[0-9 . ,]` run is comma-stripped and `ParseFloat`d; the suffix
/// is trimmed, Unicode-lowercased, and looked up in [`BYTES_UNITS`]; the
/// product is truncated toward zero (Go's `uint64(f)`). `None` on a bad
/// numeric prefix, an unknown suffix, or overflow (`f >= math.MaxUint64`) —
/// the exact three cases [`bytes_parse_error`] renders a detail for.
///
/// The suffix uses Unicode-aware [`str::to_lowercase`] (not
/// `to_ascii_lowercase`) to match Go's `strings.ToLower`: e.g. the Kelvin
/// sign U+212A folds to `k`, so `1\u{212A}B`=1000 like Loki (#226 v5
/// Finding A). The digit scan stays ASCII (`is_ascii_digit`) while Go uses
/// `unicode.IsDigit`: go-humanize slices `s[:lastDigit]` by BYTE while
/// `lastDigit` counts RUNES, so any non-ASCII digit yields a mangled prefix
/// that Go's ASCII `strconv.ParseFloat` rejects — behaviorally identical to
/// rejecting it here (both engines reject; #226 v5 Finding B).
pub(crate) fn parse_bytes_value(raw: &str) -> Option<f64> {
    let num_end = raw
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == ','))
        .unwrap_or(raw.len());
    let num = raw[..num_end].replace(',', "");
    let n: f64 = num.parse().ok()?;
    let unit = raw[num_end..].trim().to_lowercase();
    let (_, factor) = BYTES_UNITS.iter().find(|(u, _)| *u == unit)?;
    let product = n * factor;
    if product >= BYTES_MAX_UINT64_F64 {
        return None;
    }
    Some(product.trunc())
}

/// Converts a label value in `kind`'s unit family; `None` = conversion
/// failure (→ `__error__="LabelFilterErr"`). Unit-family strictness is
/// oracle-verified against the pinned reference (issue #72 review round
/// 1, finding 1):
/// - duration filters REJECT unitless values (upstream's duration parse
///   errors with "missing unit"), never coercing a bare number to
///   seconds;
/// - bytes filters ACCEPT a bare number as a byte count (upstream's
///   bytes parser does);
/// - number filters reject unit-suffixed values (float parse).
fn convert_label_value(kind: UnitKind, value: &str) -> Option<f64> {
    match kind {
        UnitKind::Number => value.parse().ok(),
        UnitKind::Duration => parse_duration_seconds(value),
        // Route entirely through the humanize port: a bare number is the
        // empty-suffix table entry, so the old `value.parse` short-circuit
        // (which bypassed uint64 truncation for e.g. `1.5`) is dropped
        // (#226).
        UnitKind::Bytes => parse_bytes_value(value),
    }
}

/// Streams-path `__error_details__` for a failed numeric label-filter
/// conversion (issue #99, oracle_probe.txt [3]). Byte-exact against
/// grafana/loki:3.4.2 for ALL label values (the offending value is
/// rendered through the same Go-stdlib quoter Loki's error carries):
/// - `Number`: Go `strconv.ParseFloat`'s `NumError`, value wrapped with
///   [`go_quote`] (`strconv.Quote` semantics: named escapes, `\xNN` for
///   C0/DEL, `\uNNNN`/`\UNNNNNNNN` for non-printable runes).
/// - `Duration`: Go `time.ParseDuration`'s `invalid duration` /
///   `missing unit` branches (pinned); the `unknown unit` branch is
///   faithful-format (ledgered — Go consumes valid leading components
///   first, which we do not reproduce) but its interpolated value/unit
///   are quoted byte-exactly via [`go_time_quote`].
/// - `Bytes`: [`bytes_parse_error`] — byte-exact `humanize.ParseBytes`
///   `.Error()` across all three branches (bad prefix / unknown suffix /
///   overflow).
fn label_filter_error_details(kind: UnitKind, value: &str) -> String {
    match kind {
        UnitKind::Number => {
            format!(
                "strconv.ParseFloat: parsing {}: invalid syntax",
                go_quote(value)
            )
        }
        UnitKind::Duration => go_duration_parse_error(value),
        UnitKind::Bytes => bytes_parse_error(value),
    }
}

/// Byte-exact `dustin/go-humanize` `ParseBytes(value).Error()` for a failed
/// byte conversion (#226). Mirrors humanize's control flow so the
/// value-side per-line `__error_details__` matches Loki across all three
/// branches:
/// - bad numeric prefix → `strconv.ParseFloat: parsing "<prefix>": invalid syntax`
///   (the comma-stripped prefix, quoted via the shared Go quoter; empty for
///   a non-numeric value);
/// - unknown suffix → `unhandled size name: <trimmed+lowercased suffix>`
///   (same Unicode-lowered suffix [`parse_bytes_value`] looks up);
/// - overflow → `too large: <ORIGINAL value>`.
///
/// [`parse_bytes_value`] returns `None` in exactly these three cases, so the
/// two functions stay consistent.
fn bytes_parse_error(value: &str) -> String {
    let num_end = value
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == ','))
        .unwrap_or(value.len());
    let num = value[..num_end].replace(',', "");
    if num.parse::<f64>().is_err() {
        return format!(
            "strconv.ParseFloat: parsing {}: invalid syntax",
            go_quote(&num)
        );
    }
    let unit = value[num_end..].trim().to_lowercase();
    if BYTES_UNITS.iter().any(|(u, _)| *u == unit) {
        // Prefix parsed and the suffix is a known unit — the only remaining
        // failure `parse_bytes_value` reports is overflow; Go interpolates
        // the ORIGINAL input string here.
        format!("too large: {value}")
    } else {
        format!("unhandled size name: {unit}")
    }
}

/// Reproduces Go `time.ParseDuration`'s error classification for the two
/// pinned branches (oracle_probe.txt [3]): a value with no leading numeric
/// character is `invalid duration`; an all-numeric value with no unit is
/// `missing unit`. Anything else falls to the faithful-format
/// `unknown unit` branch (ledgered).
fn go_duration_parse_error(value: &str) -> String {
    // Every interpolated value/unit is wrapped with `go_time_quote`
    // (Go stdlib `time`'s internal `quote`, which INCLUDES the
    // surrounding double quotes) — byte-exact for ALL values, not just
    // plain ASCII.
    let body = value.strip_prefix(['+', '-']).unwrap_or(value);
    if body.is_empty() || !body.starts_with(|c: char| c.is_ascii_digit() || c == '.') {
        return format!("time: invalid duration {}", go_time_quote(value));
    }
    let num_end = body
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(body.len());
    let unit = &body[num_end..];
    if unit.is_empty() {
        return format!("time: missing unit in duration {}", go_time_quote(value));
    }
    let unit_end = unit
        .find(|c: char| c.is_ascii_digit() || c == '.')
        .unwrap_or(unit.len());
    format!(
        "time: unknown unit {} in duration {}",
        go_time_quote(&unit[..unit_end]),
        go_time_quote(value),
    )
}

/// A logfmt decoder error the walker reports (issue #200): the 1-based
/// rune position plus the malformed class. Under `--strict` this becomes a
/// `LogfmtParserErr`; the default (lenient) path swallows it after keeping
/// the pairs decoded before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LogfmtErr {
    pos: usize,
    kind: LogfmtErrKind,
}

/// The malformed-token classes `--strict` detects (clean-room from the
/// logfmt grammar + observed reference behaviour; issue #200).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogfmtErrKind {
    /// A `"`-opened value with no closing quote before whitespace/EOF.
    UnterminatedQuote,
    /// An `=` where a key is expected — a token starting with `=`, or an
    /// `=` immediately after a completed bare value (`a=1=2`).
    UnexpectedEquals,
    /// A byte the decoder rejects where a key or unquoted value is
    /// expected — a `"` opening a key, a control byte `<0x20` mid-key, or a
    /// `"` following an unquoted value. Carries the offending byte so the
    /// message can name it (`unexpected '<char>'`), matching the reference
    /// (v3.7.3), which has no static "invalid key" text.
    InvalidKey(char),
}

/// Streams-path `__error_details__` for a `--strict` `LogfmtParserErr`
/// (issue #99 detail-string precedent, extended for #200). Byte-exact for
/// the unterminated-quote class (`pos = runes+1`, oracle_probe.txt [2]);
/// faithful-format (same structure, ledgered position) for the
/// `unexpected '='` class. The `InvalidKey` class renders
/// `unexpected '<char>'` naming the offending byte, matching the reference
/// (v3.7.3) — the `__error__` LABEL is always correct.
fn logfmt_error_details(err: LogfmtErr) -> String {
    let reason = match err.kind {
        LogfmtErrKind::UnterminatedQuote => "unterminated quoted value".to_string(),
        LogfmtErrKind::UnexpectedEquals => "unexpected '='".to_string(),
        LogfmtErrKind::InvalidKey(ch) => format!("unexpected '{ch}'"),
    };
    format!("logfmt syntax error at pos {} : {reason}", err.pos)
}

/// The 1-based rune position of the byte at `byte_off` in `text` — Loki's
/// `pos` numbering.
fn logfmt_rune_pos(text: &str, byte_off: usize) -> usize {
    text[..byte_off].chars().count() + 1
}

// ---------------------------------------------------------------------
// Templates: the `{{.label}}` + literal-text subset. Every Go-template
// function outside the subset is individually enumerated so the
// coverage surface is auditable (M6-09 adjudication item 4) and later
// issues can flip functions one by one.
// ---------------------------------------------------------------------

/// The upstream-documented template function inventory this subset
/// excludes — each one rejected **by name** at compile time.
pub const EXCLUDED_TEMPLATE_FUNCTIONS: &[&str] = &[
    "__line__",
    "__timestamp__",
    "add",
    "addf",
    "alignLeft",
    "alignRight",
    "b64dec",
    "b64enc",
    "bytes",
    "ceil",
    "contains",
    "count",
    "date",
    "default",
    "div",
    "divf",
    "duration",
    "duration_seconds",
    "float64",
    "floor",
    "fromJson",
    "hasPrefix",
    "hasSuffix",
    "indent",
    "int",
    "lower",
    "max",
    "maxf",
    "min",
    "minf",
    "mod",
    "mul",
    "mulf",
    "nindent",
    "now",
    "printf",
    "regexReplaceAll",
    "regexReplaceAllLiteral",
    "repeat",
    "replace",
    "round",
    "sha1sum",
    "sha256sum",
    "sub",
    "subf",
    "substr",
    "title",
    "toDate",
    "toDateInZone",
    "trim",
    "trimAll",
    "trimPrefix",
    "trimSuffix",
    "trunc",
    "unixEpoch",
    "unixEpochMillis",
    "unixEpochNanos",
    "unixToTime",
    "upper",
    "urldecode",
    "urlencode",
    "ToLower",
    "ToUpper",
    "Replace",
    "Trim",
    "TrimLeft",
    "TrimRight",
    "TrimPrefix",
    "TrimSuffix",
    "TrimSpace",
];

fn compile_template(text: &str) -> Result<Vec<TmplPart>, PipelineError> {
    let mut parts = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find("{{") {
        if open > 0 {
            parts.push(TmplPart::Lit(rest[..open].to_string()));
        }
        let after = &rest[open + 2..];
        let close = after.find("}}").ok_or_else(|| {
            PipelineError::BadParserExpr(format!("template {text:?}: unclosed '{{{{'"))
        })?;
        let action = after[..close].trim();
        if let Some(field) = action.strip_prefix('.') {
            let valid =
                !field.is_empty() && field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !valid {
                return Err(PipelineError::BadParserExpr(format!(
                    "template {text:?}: invalid field reference {{{{{action}}}}}"
                )));
            }
            parts.push(TmplPart::Field(field.to_string()));
        } else {
            // Not a field reference: name the first token — the excluded
            // function inventory drives auditability; anything else is
            // still rejected by whatever name it carries.
            let name = action
                .split(|c: char| c.is_whitespace() || c == '(')
                .next()
                .unwrap_or(action)
                .to_string();
            return Err(PipelineError::UnsupportedTemplate(if name.is_empty() {
                "(empty action)".to_string()
            } else {
                name
            }));
        }
        rest = &after[close + 2..];
    }
    if !rest.is_empty() {
        parts.push(TmplPart::Lit(rest.to_string()));
    }
    Ok(parts)
}

fn compile_label_format(fmts: &[LabelFmt]) -> Result<CompiledStage, PipelineError> {
    let mut out = Vec::with_capacity(fmts.len());
    let mut dsts: Vec<&str> = Vec::new();
    for f in fmts {
        let dst = match f {
            LabelFmt::Rename { dst, .. } | LabelFmt::Template { dst, .. } => dst.as_str(),
        };
        // `__error__` is not assignable — the reference rejects the whole
        // query (HTTP 400, `__error__ cannot be formatted`) for BOTH the
        // rename and the template form, and it checks this BEFORE the
        // duplicate-name rule, so `__error__="a", __error__="b"` reports
        // the reserved-label error (live-probed, issue #231). Only
        // `__error__` is reserved: `__error_details__` stays assignable.
        //
        // The message below is the reference's INNER text verbatim; the
        // envelope this gets wrapped in (`bad parser expression: …` here,
        // `invalid pipeline: …` at the API layer, against the reference's
        // `parse error : stage '…' : …`) is a cross-cutting property of every
        // LogQL parse error and is tracked by issue #240 — deliberately NOT
        // changed here, and deliberately not pinned by the tests.
        if dst == ERROR_LABEL {
            return Err(PipelineError::BadParserExpr(format!(
                "{ERROR_LABEL} cannot be formatted"
            )));
        }
        if dsts.contains(&dst) {
            return Err(PipelineError::BadParserExpr(format!(
                "label_format assigns label {dst:?} twice"
            )));
        }
        dsts.push(dst);
        out.push(match f {
            LabelFmt::Rename { dst, src } => CompiledLabelFmt::Rename {
                dst: dst.clone(),
                src: src.clone(),
            },
            LabelFmt::Template { dst, tmpl } => CompiledLabelFmt::Template {
                dst: dst.clone(),
                tmpl: compile_template(tmpl)?,
            },
        });
    }
    Ok(CompiledStage::LabelFormat {
        needs_snapshot: label_format_needs_snapshot(&out),
        fmts: out,
    })
}

/// Does this `label_format` stage need the reference's per-stage data-map
/// snapshot, or is rendering against the live label vector equivalent?
///
/// The reference builds the template data map ONCE per stage — lazily, at the
/// first template assignment — and never refreshes it while it is non-empty,
/// so a template can only observe mutations made *before* that first template
/// ran (issue #231). Live rendering can therefore differ only for a field
/// that some element between the first template and the reading template
/// mutates: a template's destination, or a rename's destination *and* source
/// (a rename deletes the source). For every other field the snapshot and the
/// live vector hold the same value by construction, so the (common) stage
/// that reads nothing it also writes keeps the copy-free path.
fn label_format_needs_snapshot(fmts: &[CompiledLabelFmt]) -> bool {
    // Names mutated at or after the first template assignment.
    let mut mutated: Vec<&str> = Vec::new();
    let mut seen_template = false;
    for f in fmts {
        match f {
            CompiledLabelFmt::Template { dst, tmpl } => {
                let reads_mutated = tmpl.iter().any(|part| match part {
                    TmplPart::Field(name) => mutated.contains(&name.as_str()),
                    TmplPart::Lit(_) => false,
                });
                if seen_template && reads_mutated {
                    return true;
                }
                seen_template = true;
                mutated.push(dst.as_str());
            }
            CompiledLabelFmt::Rename { dst, src } => {
                if seen_template {
                    mutated.push(dst.as_str());
                    mutated.push(src.as_str());
                }
            }
        }
    }
    false
}

// ---------------------------------------------------------------------
// Runtime helpers
// ---------------------------------------------------------------------

fn get_label<'v>(labels: &'v [(Cow<'_, str>, Cow<'_, str>)], name: &str) -> Option<&'v str> {
    labels
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_ref())
}

fn set_label<'a>(
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    name: Cow<'a, str>,
    value: Cow<'a, str>,
) {
    if let Some(entry) = labels.iter_mut().find(|(k, _)| *k == name) {
        entry.1 = value;
    } else {
        labels.push((name, value));
    }
}

fn remove_label<'a>(
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    name: &str,
) -> Option<Cow<'a, str>> {
    let idx = labels.iter().position(|(k, _)| k == name)?;
    Some(labels.remove(idx).1)
}

/// Adds a parser-extracted label, renaming to `<key>_extracted` when the
/// key already exists (pinned collision semantics; a second collision
/// overwrites the `_extracted` slot). Allocation-lean: an already-valid
/// key passes through as-is (borrowed where the caller borrowed it) —
/// sanitization and the collision rename are the only allocating paths.
fn add_extracted<'a>(
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    key: Cow<'a, str>,
    value: Cow<'a, str>,
) {
    let sanitized: Cow<'a, str> = if key_needs_sanitizing(&key) {
        Cow::Owned(sanitize_label_key(&key))
    } else {
        key
    };
    if get_label(labels, &sanitized).is_some() {
        set_label(labels, Cow::Owned(format!("{sanitized}_extracted")), value);
    } else {
        labels.push((sanitized, value));
    }
}

fn key_needs_sanitizing(key: &str) -> bool {
    key.is_empty()
        || key.as_bytes()[0].is_ascii_digit()
        || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Canonical label-key sanitization for parser-extracted keys: characters
/// outside `[a-zA-Z0-9_]` become `_`; a leading digit gains a `_` prefix.
fn sanitize_label_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len() + 1);
    for (i, c) in key.chars().enumerate() {
        if i == 0 && c.is_ascii_digit() {
            out.push('_');
        }
        out.push(if c.is_ascii_alphanumeric() || c == '_' {
            c
        } else {
            '_'
        });
    }
    out
}

fn render_template(parts: &[TmplPart], labels: &[(Cow<'_, str>, Cow<'_, str>)]) -> String {
    // Exact presize (one sizing pass over tiny part/label lists) so the
    // render is a single allocation per row, never a growth series —
    // the allocation-regression suite pins this (review round 2).
    let cap: usize = parts
        .iter()
        .map(|part| match part {
            TmplPart::Lit(s) => s.len(),
            TmplPart::Field(name) => get_label(labels, name).map_or(0, str::len),
        })
        .sum();
    let mut out = String::with_capacity(cap);
    for part in parts {
        match part {
            TmplPart::Lit(s) => out.push_str(s),
            // A missing field renders empty (pinned semantics, plan v3
            // delta 8 / AC2).
            TmplPart::Field(name) => out.push_str(get_label(labels, name).unwrap_or("")),
        }
    }
    out
}

/// Three-valued label-filter evaluation: `Some(true)` keep, `Some(false)`
/// drop, `None` = a numeric conversion failed somewhere the outcome
/// depends on (→ keep + `__error__`). Kleene semantics: a definite
/// `false` under `and` / definite `true` under `or` absorbs an error.
///
/// `failed` borrows the offending raw label value rather than owning it:
/// an `And`/`Or` sibling can still absorb the `None` into a definite
/// `Some(false)`/`Some(true)` (masked, no label ever set), so the owned
/// detail `String` is deferred to the caller's `None` arm — the only
/// place a label is actually written.
fn eval_label_filter<'v>(
    f: &CompiledLabelFilter,
    labels: &'v [(Cow<'_, str>, Cow<'_, str>)],
    failed: &mut Option<(UnitKind, &'v str)>,
) -> Option<bool> {
    match f {
        CompiledLabelFilter::Match {
            name,
            op,
            value,
            re,
        } => {
            // Prometheus matcher semantics: a missing label matches as
            // the empty string.
            let v = get_label(labels, name).unwrap_or("");
            Some(match op {
                MatchOp::Eq => v == value,
                MatchOp::Neq => v != value,
                MatchOp::Re => re.as_ref().is_some_and(|re| re.is_match(v)),
                MatchOp::Nre => !re.as_ref().is_some_and(|re| re.is_match(v)),
            })
        }
        CompiledLabelFilter::Compare {
            name,
            op,
            kind,
            threshold,
        } => {
            // A missing label never satisfies a numeric comparison
            // (dropped, no error); an unconvertible value is the error
            // class.
            let Some(raw) = get_label(labels, name) else {
                return Some(false);
            };
            let Some(v) = convert_label_value(*kind, raw) else {
                // Record the leftmost conversion failure as a borrow —
                // no allocation here. A sibling `And`/`Or` combinator may
                // still absorb this `None` into a definite outcome (the
                // line is masked and no label is ever set), so the owned
                // detail string is built by the caller, only once, only
                // on the surviving `None` outcome.
                if failed.is_none() {
                    *failed = Some((*kind, raw));
                }
                return None;
            };
            Some(match op {
                CompareOp::Eq => v == *threshold,
                CompareOp::Neq => v != *threshold,
                CompareOp::Gt => v > *threshold,
                CompareOp::Gte => v >= *threshold,
                CompareOp::Lt => v < *threshold,
                CompareOp::Lte => v <= *threshold,
            })
        }
        CompiledLabelFilter::Ip {
            name,
            matcher,
            negated,
        } => {
            // Reference v3.7.3 semantics (differential-authoritative): parse the
            // label value as an IP and test range membership. A missing label OR
            // an unparseable value is `match = false` — NEVER an error, so no
            // `__error__`/`__error_details__` is set (this is the key divergence
            // from the numeric label filter, which DOES error on bad values).
            // `=` returns the line iff matched; `!=` iff not matched.
            let matched = get_label(labels, name)
                .and_then(|raw| raw.parse::<IpAddr>().ok())
                .is_some_and(|ip| matcher.contains(&ip));
            Some(if *negated { !matched } else { matched })
        }
        CompiledLabelFilter::And(a, b) => {
            match (
                eval_label_filter(a, labels, failed),
                eval_label_filter(b, labels, failed),
            ) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            }
        }
        CompiledLabelFilter::Or(a, b) => {
            match (
                eval_label_filter(a, labels, failed),
                eval_label_filter(b, labels, failed),
            ) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            }
        }
    }
}

// ---------------------------------------------------------------------
// json
// ---------------------------------------------------------------------

/// Owned key/value output by design: extracted values live inside the
/// per-line `serde_json::Value`, which drops at the end of this stage —
/// the parse itself dominates the cost (bounded to pushdown-surviving
/// rows).
fn run_json<'a>(
    line: &str,
    extractions: &'a [(String, Vec<JsonPathSeg>)],
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
) {
    let parsed: serde_json::Value = match serde_json::from_str(line) {
        Ok(v @ serde_json::Value::Object(_)) => v,
        // A non-object top level (or a parse failure) is the malformed
        // class: line kept, error tagged, detail recorded on both paths.
        _ => {
            set_label(
                labels,
                Cow::Borrowed(ERROR_LABEL),
                Cow::Borrowed("JSONParserErr"),
            );
            set_label(
                labels,
                Cow::Borrowed(ERROR_DETAILS_LABEL),
                Cow::Borrowed(JSON_ERROR_DETAILS),
            );
            return;
        }
    };
    if extractions.is_empty() {
        let mut extracted = Vec::new();
        flatten_json("", &parsed, &mut extracted);
        for (k, v) in extracted {
            add_extracted(labels, Cow::Owned(k), Cow::Owned(v));
        }
    } else {
        for (label, path) in extractions {
            let value = lookup_json_path(&parsed, path)
                .map(json_scalar_to_string)
                .unwrap_or_default();
            add_extracted(labels, Cow::Borrowed(label.as_str()), Cow::Owned(value));
        }
    }
}

/// Full-flatten: nested objects join with `_`; scalars stringify; arrays
/// and nulls are skipped (pinned semantics).
fn flatten_json(prefix: &str, value: &serde_json::Value, out: &mut Vec<(String, String)>) {
    if let serde_json::Value::Object(map) = value {
        for (k, v) in map {
            let key = if prefix.is_empty() {
                k.clone()
            } else {
                format!("{prefix}_{k}")
            };
            match v {
                serde_json::Value::Object(_) => flatten_json(&key, v, out),
                serde_json::Value::Null | serde_json::Value::Array(_) => {}
                scalar => out.push((key, json_scalar_to_string(scalar))),
            }
        }
    }
}

fn lookup_json_path<'v>(
    root: &'v serde_json::Value,
    path: &[JsonPathSeg],
) -> Option<&'v serde_json::Value> {
    let mut cur = root;
    for seg in path {
        cur = match seg {
            JsonPathSeg::Field(name) => cur.get(name)?,
            JsonPathSeg::Index(idx) => cur.get(idx)?,
        };
    }
    Some(cur)
}

/// Scalars stringify without quotes; a targeted extraction that lands on
/// an object/array renders it as compact JSON (pinned semantics).
fn json_scalar_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------
// unpack
// ---------------------------------------------------------------------

/// `| unpack` (issue #200): parse the line as a packed JSON object,
/// promoting a string `_entry` field back to the line (returned owned) and
/// other string fields to labels via the shared `<key>_extracted`
/// collision rule; non-string fields are skipped. A non-object line or a
/// parse failure keeps the line and tags `__error__="JSONParserErr"` with
/// the representative detail — the same malformed class `json` reports.
/// Returns `Some(new_line)` only when a string `_entry` field was present.
fn run_unpack<'a>(line: &str, labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>) -> Option<String> {
    let map = match serde_json::from_str::<serde_json::Value>(line) {
        Ok(serde_json::Value::Object(map)) => map,
        _ => {
            set_label(
                labels,
                Cow::Borrowed(ERROR_LABEL),
                Cow::Borrowed("JSONParserErr"),
            );
            set_label(
                labels,
                Cow::Borrowed(ERROR_DETAILS_LABEL),
                Cow::Borrowed(JSON_ERROR_DETAILS),
            );
            return None;
        }
    };
    let mut new_line = None;
    for (k, v) in map {
        // Only string fields participate; other JSON value types are skipped.
        if let serde_json::Value::String(s) = v {
            if k == "_entry" {
                new_line = Some(s);
            } else {
                add_extracted(labels, Cow::Owned(k), Cow::Owned(s));
            }
        }
    }
    new_line
}

// ---------------------------------------------------------------------
// logfmt
// ---------------------------------------------------------------------

/// Applies the logfmt stage in a single streaming pass (issue #200): each
/// decoded pair is dropped when its value is empty unless `keep_empty`,
/// else committed through `to_cow` — the identity for the original body
/// (captures stay borrowed slices) or a copy for a rewritten line. On the
/// first decoder error the walk stops; under `strict` it tags
/// `__error__="LogfmtParserErr"` plus the per-class detail, otherwise (the
/// lenient default) it swallows the error, keeping the pairs already
/// emitted before it (matching the reference's default best-effort logfmt).
fn run_logfmt<'a, 't>(
    text: &'t str,
    strict: bool,
    keep_empty: bool,
    extractions: &'a [(String, String)],
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    to_cow: impl Fn(Cow<'t, str>) -> Cow<'a, str>,
) {
    let result = if extractions.is_empty() {
        walk_logfmt(text, &mut |k, v| {
            if !keep_empty && v.is_empty() {
                return;
            }
            add_extracted(labels, to_cow(Cow::Borrowed(k)), to_cow(v));
        })
    } else {
        // Targeted extraction: resolve each requested source key to its
        // first occurrence. The error verdict is identical across the
        // per-key walks, so it is captured once (the first).
        let mut err = Ok(());
        for (label, source_key) in extractions {
            let mut found: Option<Cow<'t, str>> = None;
            let res = walk_logfmt(text, &mut |k, v| {
                if found.is_none() && k == source_key {
                    found = Some(v);
                }
            });
            if err.is_ok() {
                err = res;
            }
            let value = found.map(&to_cow).unwrap_or(Cow::Borrowed(""));
            // The same empty-drop rule applies to a targeted miss/empty.
            if !keep_empty && value.is_empty() {
                continue;
            }
            add_extracted(labels, Cow::Borrowed(label.as_str()), value);
        }
        err
    };
    if let Err(err) = result
        && strict
    {
        set_label(
            labels,
            Cow::Borrowed(ERROR_LABEL),
            Cow::Borrowed("LogfmtParserErr"),
        );
        set_label(
            labels,
            Cow::Borrowed(ERROR_DETAILS_LABEL),
            Cow::Owned(logfmt_error_details(err)),
        );
    }
}

/// Minimal logfmt walk (issue #200): whitespace-separated `key[=value]`
/// tokens, double-quoted values with `\"`/`\\` escapes, bare keys emitting
/// an empty value. Values are borrowed slices of `text` except quoted
/// values containing an escape (the only owned path). Pairs are emitted to
/// `sink` as they decode (including any preceding a later error). Returns
/// `Err` on the first malformed token — an unterminated quote, an
/// unexpected `=`, or an invalid key — carrying its 1-based rune position;
/// the caller decides strict (error) vs lenient (swallow, keep the pairs).
fn walk_logfmt<'t>(
    text: &'t str,
    sink: &mut impl FnMut(&'t str, Cow<'t, str>),
) -> Result<(), LogfmtErr> {
    let bytes = text.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        // Skip inter-token whitespace.
        while i < len && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= len {
            break;
        }
        // A token opening with `=` has an empty key.
        if bytes[i] == b'=' {
            return Err(LogfmtErr {
                pos: logfmt_rune_pos(text, i),
                kind: LogfmtErrKind::UnexpectedEquals,
            });
        }
        // Key: a maximal run of bytes that are not whitespace/`=`/`"`/control.
        // A `"` or a control byte mid-key is an invalid key.
        let key_start = i;
        while i < len {
            let b = bytes[i];
            if b == b'=' || b.is_ascii_whitespace() {
                break;
            }
            if b == b'"' || b < 0x20 {
                return Err(LogfmtErr {
                    pos: logfmt_rune_pos(text, i),
                    kind: LogfmtErrKind::InvalidKey(b as char),
                });
            }
            i += 1;
        }
        let key = &text[key_start..i];
        let mut value: Cow<'t, str> = Cow::Borrowed("");
        if i < len && bytes[i] == b'=' {
            i += 1; // consume '='
            if i < len && bytes[i] == b'"' {
                i += 1; // opening quote
                let content_start = i;
                let mut escaped = false;
                let mut closed_at: Option<usize> = None;
                let mut chars = text[content_start..].char_indices();
                while let Some((off, c)) = chars.next() {
                    match c {
                        '\\' => {
                            escaped = true;
                            chars.next();
                        }
                        '"' => {
                            closed_at = Some(content_start + off);
                            break;
                        }
                        _ => {}
                    }
                }
                let Some(close) = closed_at else {
                    // Unterminated quote at EOF: pos is one past the final rune.
                    return Err(LogfmtErr {
                        pos: logfmt_rune_pos(text, len),
                        kind: LogfmtErrKind::UnterminatedQuote,
                    });
                };
                let raw = &text[content_start..close];
                value = if escaped {
                    let mut out = String::with_capacity(raw.len());
                    let mut cs = raw.chars();
                    while let Some(c) = cs.next() {
                        if c == '\\' {
                            if let Some(esc) = cs.next() {
                                out.push(esc);
                            }
                        } else {
                            out.push(c);
                        }
                    }
                    Cow::Owned(out)
                } else {
                    Cow::Borrowed(raw)
                };
                i = close + 1; // past the closing quote
            } else {
                // Bare value: a run of bytes up to the next whitespace or
                // `=`. A value ending at `=` (a second `key=` with no
                // separating whitespace) leaves the loop pointing at that
                // `=`, which the next iteration reports as an unexpected `=`
                // — the completed pair is emitted first (streaming decode).
                let val_start = i;
                while i < len {
                    let b = bytes[i];
                    // A `"` terminates the unquoted value the same way an `=`
                    // does: the completed pair is emitted, then the next
                    // iteration's key walk reports the `"` as unexpected at
                    // key position (reference v3.7.3: `a=1"b"` keeps `a="1"`
                    // and errors `unexpected '"'` at the pos of the `"`).
                    if b.is_ascii_whitespace() || b == b'=' || b == b'"' {
                        break;
                    }
                    i += 1;
                }
                value = Cow::Borrowed(&text[val_start..i]);
            }
        }
        if !key.is_empty() {
            sink(key, value);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// pattern
// ---------------------------------------------------------------------

/// Greedy left-to-right pattern walk: `false` = the line doesn't fit the
/// pattern (the caller must discard any sink output — `run_into` walks
/// once with a no-op sink first, so a non-matching line adds no labels).
/// Capture names borrow from the compiled tokens (`'n`), values are
/// slices of `text` (`'t`) — zero allocation.
fn walk_pattern<'n, 't>(
    text: &'t str,
    tokens: &'n [PatternTok],
    sink: &mut impl FnMut(&'n str, &'t str),
) -> bool {
    let mut rest = text;
    let mut i = 0;
    while i < tokens.len() {
        match &tokens[i] {
            PatternTok::Literal(lit) => {
                let Some(after) = rest.strip_prefix(lit.as_str()) else {
                    return false;
                };
                rest = after;
                i += 1;
            }
            PatternTok::Capture(_) | PatternTok::Discard => {
                // A capture extends to the next literal's first
                // occurrence, or to the end of the line for a trailing
                // capture.
                let (captured, remaining) = match tokens.get(i + 1) {
                    Some(PatternTok::Literal(next_lit)) => {
                        let Some(at) = rest.find(next_lit.as_str()) else {
                            return false;
                        };
                        (&rest[..at], &rest[at..])
                    }
                    // compile_pattern rejects consecutive captures, so
                    // the successor is always a literal or nothing.
                    _ => (rest, ""),
                };
                if let PatternTok::Capture(name) = &tokens[i] {
                    sink(name.as_str(), captured);
                }
                rest = remaining;
                i += 1;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_seconds_parses_single_compound_and_fractional_literals() {
        assert_eq!(parse_duration_seconds("250ms"), Some(0.25));
        assert_eq!(parse_duration_seconds("1h30m"), Some(5_400.0));
        assert_eq!(parse_duration_seconds("1.5s"), Some(1.5));
        assert_eq!(parse_duration_seconds("2w"), Some(1_209_600.0));
        assert_eq!(parse_duration_seconds("5xz"), None);
        assert_eq!(parse_duration_seconds("5KB"), None);
        assert_eq!(parse_duration_seconds(""), None);
    }

    #[test]
    fn bytes_value_parses_decimal_and_binary_units_case_insensitively() {
        assert_eq!(parse_bytes_value("512b"), Some(512.0));
        assert_eq!(parse_bytes_value("5KB"), Some(5_000.0));
        assert_eq!(parse_bytes_value("5kb"), Some(5_000.0));
        assert_eq!(parse_bytes_value("1MiB"), Some(1_048_576.0));
        assert_eq!(parse_bytes_value("1h"), None);
        assert_eq!(parse_bytes_value("5xz"), None);
    }

    #[test]
    fn bytes_value_matches_humanize_parse_bytes_table() {
        // Probed exact against grafana/loki:3.7.4 `humanize.ParseBytes`.
        // uint64 truncation toward zero.
        assert_eq!(parse_bytes_value("1.5"), Some(1.0));
        assert_eq!(parse_bytes_value("1.9KiB"), Some(1_945.0));
        // Bare units (no `b` suffix).
        assert_eq!(parse_bytes_value("1k"), Some(1_000.0));
        assert_eq!(parse_bytes_value("1ki"), Some(1_024.0));
        // Comma separators are stripped.
        assert_eq!(parse_bytes_value("1,024"), Some(1_024.0));
        assert_eq!(parse_bytes_value("1,024b"), Some(1_024.0));
        // Space between number and unit is trimmed.
        assert_eq!(parse_bytes_value("3 kB"), Some(3_000.0));
        assert_eq!(parse_bytes_value("1.5 mib"), Some(1_572_864.0));
        // Empty suffix = a bare byte count.
        assert_eq!(parse_bytes_value("1000"), Some(1_000.0));
        // Exa tier: `e`/`eb`=1e18, `ei`/`eib`=2^60.
        assert_eq!(parse_bytes_value("1e"), Some(1e18));
        assert_eq!(parse_bytes_value("1eb"), Some(1e18));
        assert_eq!(parse_bytes_value("1ei"), Some(1_152_921_504_606_846_976.0));
        assert_eq!(parse_bytes_value("1eib"), Some(1_152_921_504_606_846_976.0));
        // Overflow: `f >= math.MaxUint64` (2^64) is rejected. 16*2^60 == 2^64.
        assert_eq!(parse_bytes_value("16eib"), None);
        // Just below the boundary is accepted (15*2^60 < 2^64).
        assert_eq!(
            parse_bytes_value("15eib"),
            Some(15.0 * 1_152_921_504_606_846_976.0)
        );
    }

    #[test]
    fn bytes_value_folds_unicode_suffix_like_go_to_lower() {
        // #226 v5 Finding A: Go `strings.ToLower` folds the Kelvin sign
        // U+212A to `k`, so these are Loki-valid. Unicode-aware
        // `to_lowercase` matches; an ASCII-only fold would reject them.
        assert_eq!(parse_bytes_value("1\u{212A}B"), Some(1_000.0));
        assert_eq!(parse_bytes_value("1\u{212A}"), Some(1_000.0));
        assert_eq!(parse_bytes_value("1\u{212A}iB"), Some(1_024.0));
    }

    #[test]
    fn bytes_value_rejects_unicode_digits_like_loki() {
        // #226 v5 Finding B: go-humanize slices the numeric prefix by BYTE
        // while counting runes, so a non-ASCII digit yields a mangled
        // prefix that Go's ASCII ParseFloat rejects. An ASCII digit scan
        // rejects the same inputs — accept/reject identical (both engines
        // reject; only the garbage detail string would differ).
        assert_eq!(parse_bytes_value("\u{FF11}\u{FF12}\u{FF13}KB"), None); // fullwidth 123
        assert_eq!(parse_bytes_value("\u{0663}KB"), None); // Arabic-Indic 3
    }

    #[test]
    fn bytes_parse_error_mirrors_humanize_three_branches() {
        // Probed exact against grafana/loki:3.7.4.
        assert_eq!(bytes_parse_error("5xb"), "unhandled size name: xb");
        assert_eq!(bytes_parse_error("1Kx"), "unhandled size name: kx");
        assert_eq!(bytes_parse_error("16eib"), "too large: 16eib");
        assert_eq!(
            bytes_parse_error("5.5.5b"),
            "strconv.ParseFloat: parsing \"5.5.5\": invalid syntax"
        );
        assert_eq!(
            bytes_parse_error("abc"),
            "strconv.ParseFloat: parsing \"\": invalid syntax"
        );
        assert_eq!(
            bytes_parse_error("-3b"),
            "strconv.ParseFloat: parsing \"\": invalid syntax"
        );
    }

    #[test]
    fn a_rejected_unit_literal_is_a_named_compile_error_never_a_silent_zero() {
        let err = classify_numeric_literal(&NumericLiteral::DurationOrBytes("5xz".to_string()))
            .unwrap_err();
        assert!(matches!(err, PipelineError::BadParserExpr(_)));
        assert!(err.to_string().contains("5xz"));
    }

    #[test]
    fn fractional_duration_label_filters_parse_compile_and_evaluate() {
        // AC7 (#90): grafana/loki:3.4.2 accepts fractional durations in
        // label-filter RHS position (time.ParseDuration); the lexer fix
        // makes `1.5s`/`0.3s`/`.5s` reach parse_duration_seconds as one
        // Duration token. Each parses, compiles, and thresholds correctly.
        for (query, threshold_secs) in [
            (r#"{a="b"} | logfmt | latency > 1.5s"#, 1.5),
            (r#"{a="b"} | logfmt | latency >= 0.3s"#, 0.3),
            (r#"{a="b"} | logfmt | latency > .5s"#, 0.5),
        ] {
            let compiled = CompiledPipeline::compile(&stages_of(query)).expect(query);
            let base = vec![("app".to_string(), "x".to_string())];
            // A line above the threshold survives; one below is dropped.
            let above = format!("latency={}s", threshold_secs + 1.0);
            let below = format!("latency={}s", threshold_secs / 2.0);
            assert!(
                compiled.run(&above, &base).is_some(),
                "{query}: {above:?} should survive"
            );
            assert!(
                compiled.run(&below, &base).is_none(),
                "{query}: {below:?} should be dropped"
            );
        }
    }

    #[test]
    fn fractional_unwrap_duration_pipeline_parses_and_compiles() {
        // AC7 (#90): the unwrap + fractional label-filter form parses.
        let compiled = CompiledPipeline::compile(&stages_of(
            r#"sum_over_time({a="b"} | logfmt | unwrap duration(d) | latency > 1.5s [5m])"#,
        ))
        .expect("fractional unwrap pipeline compiles");
        let base = vec![("app".to_string(), "x".to_string())];
        // Metric extraction still works end-to-end.
        let mut labels = Vec::new();
        let _ = compiled.run_metric_into("d=250ms latency=2s", &base, &mut labels);
    }

    #[test]
    fn fractional_range_duration_is_rejected_matching_loki() {
        // AC7 (#90): grafana/loki:3.4.2 REJECTS fractional RANGE `[...]`
        // durations; our range parser (duration::parse_duration,
        // integer-only) rejects them too. The lexer now hands the whole
        // `1.5h` as one Duration token, so the error is a clean
        // InvalidDuration-class message naming the literal.
        for query in [
            r#"count_over_time({a="b"}[1.5h])"#,
            r#"count_over_time({a="b"}[.5s])"#,
        ] {
            assert!(
                pulsus_logql::parse(query).is_err(),
                "{query} must be rejected (Loki parity)"
            );
        }
    }

    #[test]
    fn every_excluded_template_function_is_rejected_by_name() {
        for func in EXCLUDED_TEMPLATE_FUNCTIONS {
            let tmpl = format!("{{{{ {func} .x }}}}");
            match compile_template(&tmpl) {
                Err(PipelineError::UnsupportedTemplate(name)) => {
                    assert_eq!(&name, func, "template {tmpl:?}");
                }
                other => panic!("expected {func} to be UnsupportedTemplate, got {other:?}"),
            }
        }
    }

    #[test]
    fn the_field_substitution_subset_compiles_and_renders() {
        let parts = compile_template("{{.method}} -> {{.path}}!").unwrap();
        let labels = vec![
            (Cow::Borrowed("method"), Cow::Borrowed("GET")),
            (Cow::Borrowed("path"), Cow::Borrowed("/x")),
        ];
        assert_eq!(render_template(&parts, &labels), "GET -> /x!");
    }

    #[test]
    fn pattern_compile_rejects_zero_captures_and_consecutive_captures() {
        assert!(matches!(
            compile_pattern("no captures here"),
            Err(PipelineError::BadParserExpr(_))
        ));
        assert!(matches!(
            compile_pattern("<a><b>"),
            Err(PipelineError::BadParserExpr(_))
        ));
        assert!(compile_pattern("<a> <b>").is_ok());
    }

    // -----------------------------------------------------------------
    // Issue M6-10: unwrap evaluation — metric-mode only.
    // -----------------------------------------------------------------

    fn stages_of(query: &str) -> Vec<Stage> {
        let expr = pulsus_logql::parse(query).expect("parse");
        match expr {
            pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Range { range, .. }) => {
                range.selector.pipeline
            }
            pulsus_logql::Expr::Log(log) => log.pipeline,
            other => panic!("unexpected expr shape: {other:?}"),
        }
    }

    /// Issue #201 regression: an `ip(…)` line filter has no token/skip-index
    /// prefilter, so it does NOT push down — it compiles a run-stage and must
    /// therefore decline the `is_line_filter_only` fast path. If the gate
    /// wrongly reported `true`, exec would skip the client-side IP scan and the
    /// filter would silently no-op. A plain literal line filter still qualifies.
    #[test]
    fn an_ip_only_line_filter_is_not_line_filter_only() {
        let ip_only =
            CompiledPipeline::compile(&stages_of(r#"{a="b"} |= ip("10.0.0.0/8")"#)).unwrap();
        assert!(
            !ip_only.is_line_filter_only(),
            "an ip() line filter must not take the pushdown-only fast path"
        );

        // Positive control: a pure literal line filter DOES push down fully and
        // keeps the fast path.
        let literal = CompiledPipeline::compile(&stages_of(r#"{a="b"} |= "boom""#)).unwrap();
        assert!(
            literal.is_line_filter_only(),
            "a pure literal line filter should remain line-filter-only"
        );
    }

    /// Adjudication #2 regression: the STREAMS path is byte-identical
    /// with and without a trailing `| unwrap x` — no conversion, no
    /// `__error__`, no label removal.
    #[test]
    fn streams_run_is_byte_identical_with_and_without_a_trailing_unwrap() {
        let without =
            CompiledPipeline::compile(&stages_of(r#"sum_over_time({a="b"} | logfmt [5m])"#))
                .unwrap();
        let with = CompiledPipeline::compile(&stages_of(
            r#"sum_over_time({a="b"} | logfmt | unwrap duration(took) [5m])"#,
        ))
        .unwrap();
        let base = vec![("app".to_string(), "x".to_string())];
        for body in [
            "took=250ms level=info",
            "took=abc level=warn", // would FAIL conversion on the metric path
            "level=error",         // unwrap label missing entirely
        ] {
            let a = without.run(body, &base).expect("kept");
            let b = with.run(body, &base).expect("kept");
            assert_eq!(a.line, b.line, "body {body:?}");
            assert_eq!(a.labels, b.labels, "body {body:?}");
            assert!(
                !b.labels.iter().any(|(k, _)| k == ERROR_LABEL),
                "streams path must never tag an unwrap error: {body:?}"
            );
        }
    }

    #[test]
    fn metric_run_extracts_the_converted_value_and_deletes_the_unwrapped_label() {
        let compiled = CompiledPipeline::compile(&stages_of(
            r#"sum_over_time({a="b"} | logfmt | unwrap duration(took) [5m])"#,
        ))
        .unwrap();
        let base = vec![("app".to_string(), "x".to_string())];
        let mut labels = Vec::new();
        let MetricRun::Kept { value, .. } =
            compiled.run_metric_into("took=250ms level=info", &base, &mut labels)
        else {
            panic!("expected the line to be kept");
        };
        assert_eq!(value, Some(0.25));
        assert!(
            !labels.iter().any(|(k, _)| k == "took"),
            "successful unwrap must delete the unwrapped label (oracle-probed): {labels:?}"
        );
        assert!(labels.iter().any(|(k, v)| k == "level" && v == "info"));
    }

    #[test]
    fn metric_run_tags_sample_extraction_err_on_a_failed_conversion_and_keeps_the_line() {
        let compiled = CompiledPipeline::compile(&stages_of(
            r#"sum_over_time({a="b"} | logfmt | unwrap duration(took) [5m])"#,
        ))
        .unwrap();
        let base = vec![("app".to_string(), "x".to_string())];
        let mut labels = Vec::new();
        let MetricRun::Kept { value, .. } =
            compiled.run_metric_into("took=abc level=warn", &base, &mut labels)
        else {
            panic!("a failed conversion keeps the line (a later __error__ filter may drop it)");
        };
        assert_eq!(value, None);
        assert!(
            labels
                .iter()
                .any(|(k, v)| k == ERROR_LABEL && v == SAMPLE_EXTRACTION_ERROR),
            "{labels:?}"
        );
        assert!(
            labels.iter().any(|(k, v)| k == "took" && v == "abc"),
            "the raw label stays on the failed line (oracle failed-series shape): {labels:?}"
        );
    }

    #[test]
    fn metric_run_drops_a_line_whose_unwrap_label_is_missing() {
        let compiled = CompiledPipeline::compile(&stages_of(
            r#"sum_over_time({a="b"} | logfmt | unwrap duration(took) [5m])"#,
        ))
        .unwrap();
        let base = vec![("app".to_string(), "x".to_string())];
        let mut labels = Vec::new();
        assert!(matches!(
            compiled.run_metric_into("level=error", &base, &mut labels),
            MetricRun::Dropped
        ));
    }

    /// A post-unwrap `| __error__ = ""` filter consumes the failed line
    /// in pipeline order (plan v2 D1) — the exact oracle-probed shape.
    #[test]
    fn a_post_unwrap_error_filter_drops_the_failed_line_and_keeps_the_good_one() {
        let compiled = CompiledPipeline::compile(&stages_of(
            r#"sum_over_time({a="b"} | logfmt | unwrap duration(took) | __error__ = "" [5m])"#,
        ))
        .unwrap();
        let base = vec![("app".to_string(), "x".to_string())];
        let mut labels = Vec::new();
        assert!(matches!(
            compiled.run_metric_into("took=abc", &base, &mut labels),
            MetricRun::Dropped
        ));
        assert!(matches!(
            compiled.run_metric_into("took=100ms", &base, &mut labels),
            MetricRun::Kept {
                value: Some(v),
                ..
            } if v == 0.1
        ));
    }

    #[test]
    fn unwrap_conversion_families_match_the_label_filter_unit_parser() {
        let base = vec![("a".to_string(), "b".to_string())];
        for (query, body, expected) in [
            // Bare unwrap: plain float parse.
            (
                r#"sum_over_time({a="b"} | logfmt | unwrap v [5m])"#,
                "v=42",
                42.0,
            ),
            // duration_seconds is an alias of duration.
            (
                r#"sum_over_time({a="b"} | logfmt | unwrap duration_seconds(v) [5m])"#,
                "v=1h30m",
                5_400.0,
            ),
            (
                r#"sum_over_time({a="b"} | logfmt | unwrap bytes(v) [5m])"#,
                "v=5KB",
                5_000.0,
            ),
        ] {
            let compiled = CompiledPipeline::compile(&stages_of(query)).unwrap();
            let mut labels = Vec::new();
            let MetricRun::Kept { value, .. } = compiled.run_metric_into(body, &base, &mut labels)
            else {
                panic!("expected {query} over {body:?} to keep the line");
            };
            assert_eq!(value, Some(expected), "{query} over {body:?}");
        }
    }

    #[test]
    fn json_path_parses_dotted_indexed_and_quoted_segments() {
        assert_eq!(
            parse_json_path(r#"request.headers["User-Agent"]"#).unwrap(),
            vec![
                JsonPathSeg::Field("request".to_string()),
                JsonPathSeg::Field("headers".to_string()),
                JsonPathSeg::Field("User-Agent".to_string()),
            ]
        );
        assert_eq!(
            parse_json_path("servers[0]").unwrap(),
            vec![
                JsonPathSeg::Field("servers".to_string()),
                JsonPathSeg::Index(0),
            ]
        );
        assert!(parse_json_path("").is_err());
        assert!(parse_json_path("a[b").is_err());
    }

    // -----------------------------------------------------------------
    // Issues #99 + #104: a compound `and`/`or` label filter can absorb a
    // sibling's conversion failure into a definite outcome (masking) —
    // `eval_label_filter`'s `failed` capture must never allocate on that
    // path, and a genuinely-surviving error must stay byte-exact.
    // -----------------------------------------------------------------

    #[test]
    fn masked_and_drops_the_line_and_emits_no_error_label_streams() {
        let compiled = CompiledPipeline::compile(&stages_of(
            r#"{a="b"} | logfmt | level = "warn" and took > 250ms"#,
        ))
        .unwrap();
        let base = vec![("a".to_string(), "b".to_string())];
        // `level = "warn"` is definite-false; `and` absorbs the sibling's
        // conversion failure on `took=bad` without ever setting a label.
        assert!(compiled.run("level=info took=bad", &base).is_none());
    }

    #[test]
    fn masked_or_keeps_the_line_and_emits_no_error_label_streams() {
        let compiled = CompiledPipeline::compile(&stages_of(
            r#"{a="b"} | logfmt | level = "info" or took > 250ms"#,
        ))
        .unwrap();
        let base = vec![("a".to_string(), "b".to_string())];
        // `level = "info"` is definite-true; `or` absorbs the sibling's
        // conversion failure on `took=bad` without ever setting a label.
        let out = compiled
            .run("level=info took=bad", &base)
            .expect("or-true absorbs the failure and keeps the line");
        assert!(!out.labels.iter().any(|(k, _)| k == ERROR_LABEL));
        assert!(!out.labels.iter().any(|(k, _)| k == ERROR_DETAILS_LABEL));
    }

    #[test]
    fn masked_or_keeps_the_line_and_emits_no_error_label_metric() {
        let compiled = CompiledPipeline::compile(&stages_of(
            r#"sum_over_time({a="b"} | logfmt | level = "info" or took > 250ms [5m])"#,
        ))
        .unwrap();
        let base = vec![("a".to_string(), "b".to_string())];
        let mut labels = Vec::new();
        let MetricRun::Kept { .. } =
            compiled.run_metric_into("level=info took=bad", &base, &mut labels)
        else {
            panic!("or-true absorbs the failure and keeps the line");
        };
        assert!(!labels.iter().any(|(k, _)| k == ERROR_LABEL));
        assert!(!labels.iter().any(|(k, _)| k == ERROR_DETAILS_LABEL));
    }

    #[test]
    fn a_genuine_compound_error_still_emits_byte_exact_error_labels() {
        let base = vec![("a".to_string(), "b".to_string())];
        for query in [
            // `or`: both sides fail to produce a definite `true`
            // (`level = "warn"` is false, `took > 250ms` errors) → `None`.
            r#"{a="b"} | logfmt | level = "warn" or took > 250ms"#,
            // `and`: both sides fail to produce a definite `false`
            // (`level = "info"` is true, `took > 250ms` errors) → `None`.
            r#"{a="b"} | logfmt | level = "info" and took > 250ms"#,
        ] {
            let compiled = CompiledPipeline::compile(&stages_of(query)).unwrap();
            let out = compiled
                .run("level=info took=bad", &base)
                .unwrap_or_else(|| panic!("{query}: an unabsorbed error keeps the line"));
            assert!(
                out.labels
                    .iter()
                    .any(|(k, v)| k == ERROR_LABEL && v == "LabelFilterErr"),
                "{query}: {:?}",
                out.labels
            );
            assert!(
                out.labels.iter().any(|(k, v)| k == ERROR_DETAILS_LABEL
                    && v == "time: invalid duration \"bad\""),
                "{query}: {:?}",
                out.labels
            );
        }
    }

    // -----------------------------------------------------------------
    // label_format parity (issue #231) — every expectation below was
    // live-probed against the pinned reference (grafana/loki:3.7.4).
    // -----------------------------------------------------------------

    /// Runs `query` over `line` with the `{svc="x"}` base stream (no name
    /// collision with the extracted keys) and returns the final label set
    /// sorted by name, as owned pairs.
    fn label_format_labels(query: &str, line: &str) -> Vec<(String, String)> {
        let base = vec![("svc".to_string(), "x".to_string())];
        let compiled = CompiledPipeline::compile(&stages_of(query)).expect(query);
        let out = compiled
            .run(line, &base)
            .unwrap_or_else(|| panic!("{query}: line unexpectedly dropped"));
        let mut got: Vec<(String, String)> = out
            .labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        got.sort();
        got
    }

    fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        out.sort();
        out
    }

    #[test]
    fn a_label_format_template_cannot_observe_an_earlier_assignment_in_the_same_stage() {
        // Reference: `aa="XHello"`, `bb="[]"` — the data map is snapshotted
        // once per stage, so `bb` sees no `aa`.
        assert_eq!(
            label_format_labels(
                r#"{svc="x"} | logfmt | label_format aa="X{{.a}}", bb="[{{.aa}}]""#,
                "a=Hello b=World",
            ),
            owned(&[
                ("a", "Hello"),
                ("aa", "XHello"),
                ("b", "World"),
                ("bb", "[]"),
                ("svc", "x"),
            ]),
        );
        // Chained: neither the second nor the third assignment resolves.
        assert_eq!(
            label_format_labels(
                r#"{svc="x"} | logfmt | label_format aa="X{{.a}}", bb="{{.aa}}", cc="{{.bb}}""#,
                "a=Hello b=World",
            ),
            owned(&[
                ("a", "Hello"),
                ("aa", "XHello"),
                ("b", "World"),
                ("bb", ""),
                ("cc", ""),
                ("svc", "x"),
            ]),
        );
    }

    #[test]
    fn a_template_overwriting_its_own_source_is_invisible_to_a_later_template() {
        // Reference: `a="ZHello"` (the overwrite lands) but `z="Hello"` —
        // the snapshot still holds the pre-stage value of `a`.
        assert_eq!(
            label_format_labels(
                r#"{svc="x"} | logfmt | label_format a="Z{{.a}}", z="{{.a}}""#,
                "a=Hello b=World",
            ),
            owned(&[
                ("a", "ZHello"),
                ("b", "World"),
                ("svc", "x"),
                ("z", "Hello"),
            ]),
        );
    }

    #[test]
    fn a_rename_between_two_templates_is_invisible_to_the_later_template() {
        // Renames read the LIVE label set, but they do not refresh the
        // snapshot: `t2` still resolves `a` after the rename deleted it.
        assert_eq!(
            label_format_labels(
                r#"{svc="x"} | logfmt | label_format t1="{{.a}}", r=a, t2="{{.a}}""#,
                "a=Hello b=World",
            ),
            owned(&[
                ("b", "World"),
                ("r", "Hello"),
                ("svc", "x"),
                ("t1", "Hello"),
                ("t2", "Hello"),
            ]),
        );
    }

    #[test]
    fn a_rename_before_the_first_template_is_visible_to_it() {
        // The map is materialised lazily AT the first template, so
        // everything the stage did before that point is in it.
        assert_eq!(
            label_format_labels(
                r#"{svc="x"} | logfmt | label_format lvl=a, t="{{.lvl}}""#,
                "a=Hello b=World",
            ),
            owned(&[
                ("b", "World"),
                ("lvl", "Hello"),
                ("svc", "x"),
                ("t", "Hello"),
            ]),
        );
    }

    #[test]
    fn each_label_format_stage_takes_its_own_snapshot() {
        // Two stages ⇒ two maps: the second stage sees the first's output.
        assert_eq!(
            label_format_labels(
                r#"{svc="x"} | logfmt | label_format z="{{.a}}" | label_format y="{{.z}}""#,
                "a=Hello b=World",
            ),
            owned(&[
                ("a", "Hello"),
                ("b", "World"),
                ("svc", "x"),
                ("y", "Hello"),
                ("z", "Hello"),
            ]),
        );
    }

    #[test]
    fn an_empty_label_set_rebuilds_the_snapshot_until_it_is_non_empty() {
        // The reference refills the map whenever it is still EMPTY
        // (`if len(m) == 0`), which `| drop`-ing every label makes
        // reachable: `n2` sees `n1`, but `n3` no longer sees `n2` because
        // the map became non-empty at the `n2` refill.
        assert_eq!(
            label_format_labels(
                r#"{svc="x"} | logfmt | drop svc, p | label_format n1="X", n2="{{.n1}}", n3="{{.n2}}""#,
                "p=1",
            ),
            owned(&[("n1", "X"), ("n2", "X"), ("n3", "")]),
        );
        // Same rule with a rename in the middle: the refill at `n2` reads
        // the post-rename live set (no `n1` left), then freezes.
        assert_eq!(
            label_format_labels(
                r#"{svc="x"} | logfmt | drop svc, p | label_format n1="X", r=n1, n2="{{.n1}}", n3="{{.r}}""#,
                "p=1",
            ),
            owned(&[("n2", ""), ("n3", "X"), ("r", "X")]),
        );
    }

    #[test]
    fn a_snapshotted_template_still_resolves_the_error_labels() {
        // The reference's map is built from ALL label categories plus the
        // error pair, so `{{.__error__}}` renders inside `label_format`.
        assert_eq!(
            label_format_labels(
                r#"{svc="x"} | json | label_format e="[{{.__error__}}]""#,
                "not json at all",
            ),
            owned(&[
                ("svc", "x"),
                ("e", "[JSONParserErr]"),
                (ERROR_LABEL, "JSONParserErr"),
                (ERROR_DETAILS_LABEL, JSON_ERROR_DETAILS),
            ]),
        );
    }

    #[test]
    fn the_snapshot_is_only_taken_for_stages_whose_result_can_differ() {
        // Perf guard: the copy is confined to the self-referential shapes.
        // Every ordinary `label_format` keeps rendering against the live
        // label vector.
        for (query, want) in [
            (r#"{a="b"} | label_format x="{{.y}}""#, false),
            (
                r#"{a="b"} | label_format lvl=level, tag="v-{{.code}}""#,
                false,
            ),
            (r#"{a="b"} | label_format x="{{.y}}", z="{{.y}}""#, false),
            // A rename BEFORE the first template is already in the map.
            (r#"{a="b"} | label_format lvl=level, t="{{.lvl}}""#, false),
            // Reads a label an earlier template in the stage wrote.
            (r#"{a="b"} | label_format x="1", z="{{.x}}""#, true),
            // Reads a label an interleaved rename moved.
            (r#"{a="b"} | label_format x="1", r=y, z="{{.y}}""#, true),
            (r#"{a="b"} | label_format x="1", r=y, z="{{.r}}""#, true),
        ] {
            let stages = stages_of(query);
            let compiled = CompiledPipeline::compile(&stages).expect(query);
            let got = compiled.stages.iter().find_map(|s| match s {
                CompiledStage::LabelFormat { needs_snapshot, .. } => Some(*needs_snapshot),
                _ => None,
            });
            assert_eq!(got, Some(want), "{query}");
        }
    }

    #[test]
    fn assigning_the_error_label_is_rejected_in_both_label_format_forms() {
        // Reference (HTTP 400): `__error__ cannot be formatted` — for the
        // template form, the rename form, and regardless of position.
        //
        // Only the reference-matching INNER text is asserted. The surrounding
        // envelope (`bad parser expression: …` vs the reference's
        // `parse error : stage '…' : …`) is a cross-cutting property of the
        // whole LogQL error surface, tracked by issue #240 — pinning it here
        // would enshrine wording #240 is going to change.
        for query in [
            r#"{a="b"} | label_format __error__="boom""#,
            r#"{a="b"} | label_format __error__=level"#,
            r#"{a="b"} | label_format ok="1", __error__="boom""#,
            // The reserved-name check runs BEFORE the duplicate-name rule.
            r#"{a="b"} | label_format __error__="a", __error__="b""#,
        ] {
            let err = CompiledPipeline::compile(&stages_of(query)).expect_err(query);
            assert!(matches!(err, PipelineError::BadParserExpr(_)), "{query}");
            let text = err.to_string();
            assert!(
                text.contains("__error__ cannot be formatted"),
                "{query}: {text}"
            );
        }
    }

    #[test]
    fn assigning_the_error_details_label_is_accepted_like_the_reference() {
        // Only `__error__` is reserved: the reference accepts
        // `__error_details__` (200) and sets it.
        let got = label_format_labels(
            r#"{a="b"} | logfmt | label_format __error_details__="boom""#,
            "p=1",
        );
        assert!(
            got.contains(&(ERROR_DETAILS_LABEL.to_string(), "boom".to_string())),
            "{got:?}"
        );
    }
}
