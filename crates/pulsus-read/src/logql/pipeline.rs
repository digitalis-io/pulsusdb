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
//! - `logfmt` splits `k=v` pairs (bare key ⇒ empty value, quoted values
//!   unescaped); an unterminated quote keeps the line with
//!   `__error__="LogfmtParserErr"`.
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

use std::borrow::Cow;
use std::fmt;

use pulsus_logql::{
    CompareOp, LabelFilterExpr, LabelFmt, LineFilterOp, MatchOp, NumericLiteral, ParserStage, Stage,
};

/// The label carrying parser/filter failure classes (pinned values:
/// `JSONParserErr`, `LogfmtParserErr`, `LabelFilterErr`), filterable like
/// any other label — `| __error__ = ""` drops errored lines.
pub const ERROR_LABEL: &str = "__error__";

/// Errors from compiling a pipeline — all client-caused, surfaced as
/// [`super::error::ReadError::PipelineInvalid`] (400-class).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineError {
    BadRegex(String),
    UnsupportedTemplate(String),
    BadParserExpr(String),
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

#[derive(Debug)]
enum LineMatcher {
    Literal(String),
    Regex(regex::Regex),
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
        op: LineFilterOp,
        matcher: LineMatcher,
    },
    Json {
        /// Empty = full flatten.
        extractions: Vec<(String, Vec<JsonPathSeg>)>,
    },
    Logfmt {
        /// Empty = all pairs; else `(label, source_key)`.
        extractions: Vec<(String, String)>,
    },
    Regexp(regex::Regex),
    Pattern(Vec<PatternTok>),
    LabelFilter(CompiledLabelFilter),
    LineFormat(Vec<TmplPart>),
    LabelFormat(Vec<CompiledLabelFmt>),
    /// `| unwrap <label>` / `| unwrap <conversion>(<label>)` — evaluated
    /// **only** on the metric path ([`CompiledPipeline::run_metric_into`]);
    /// inert for stream execution (issue M6-10 adjudication #2: the
    /// non-metric path must not execute the conversion or mutate
    /// `__error__`).
    Unwrap {
        label: String,
        kind: UnitKind,
    },
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
                    if !seen_line_format {
                        // Pushed down to SQL by `plan::compile_line_filters`
                        // — never re-evaluated here.
                        continue;
                    }
                    let matcher = match lf.op {
                        LineFilterOp::Contains | LineFilterOp::NotContains => {
                            LineMatcher::Literal(lf.value.clone())
                        }
                        LineFilterOp::Regex | LineFilterOp::NotRegex => {
                            // Unanchored, like the SQL pushdown's
                            // `match(body, ...)`.
                            LineMatcher::Regex(compile_regex(&lf.value)?)
                        }
                    };
                    compiled.push(CompiledStage::LineFilter { op: lf.op, matcher });
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
                    compiled.push(CompiledStage::LabelFormat(compile_label_format(fmts)?));
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
            }
        }

        Ok(CompiledPipeline {
            stages: compiled,
            mutates_labels,
            rewrites_line,
            line_filter_only: stages.iter().all(|s| matches!(s, Stage::LineFilter(_))),
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
    /// sets `__error__="SampleExtractionErr"` (and keeps the raw label,
    /// matching the oracle's failed-series shape) and continues, so
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
                CompiledStage::LineFilter { op, matcher } => {
                    let hit = match matcher {
                        LineMatcher::Literal(lit) => line.contains(lit.as_str()),
                        LineMatcher::Regex(re) => re.is_match(&line),
                    };
                    let keep = match op {
                        LineFilterOp::Contains | LineFilterOp::Regex => hit,
                        LineFilterOp::NotContains | LineFilterOp::NotRegex => !hit,
                    };
                    if !keep {
                        return MetricRun::Dropped;
                    }
                }
                CompiledStage::Json { extractions } => run_json(&line, extractions, labels),
                CompiledStage::Logfmt { extractions } => {
                    // Borrow captures from the body slice when the line
                    // is still the original body; a rewritten
                    // (`line_format`-owned) line cannot be borrowed past
                    // its own reassignment, so its captures are copied.
                    match &line {
                        Cow::Borrowed(text) => run_logfmt(text, extractions, labels, |c| c),
                        Cow::Owned(text) => {
                            run_logfmt(text, extractions, labels, |c| Cow::Owned(c.into_owned()))
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
                CompiledStage::LabelFilter(filter) => match eval_label_filter(filter, labels) {
                    Some(true) => {}
                    Some(false) => return MetricRun::Dropped,
                    // Conversion failure: keep the line, tag the error
                    // class (pinned semantics, oracle-verified; a later
                    // `__error__=""` filter drops it).
                    None => set_label(
                        labels,
                        Cow::Borrowed(ERROR_LABEL),
                        Cow::Borrowed("LabelFilterErr"),
                    ),
                },
                CompiledStage::LineFormat(tmpl) => {
                    line = Cow::Owned(render_template(tmpl, labels));
                }
                CompiledStage::LabelFormat(fmts) => {
                    for f in fmts {
                        match f {
                            CompiledLabelFmt::Rename { dst, src } => {
                                let value = remove_label(labels, src).unwrap_or_default();
                                set_label(labels, Cow::Borrowed(dst), value);
                            }
                            CompiledLabelFmt::Template { dst, tmpl } => {
                                let rendered = render_template(tmpl, labels);
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
                            // `__error__` filter sees it in order.
                            set_label(
                                labels,
                                Cow::Borrowed(ERROR_LABEL),
                                Cow::Borrowed(SAMPLE_EXTRACTION_ERROR),
                            );
                            value = None;
                        }
                    }
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
        ParserStage::Logfmt { extractions } => Ok(CompiledStage::Logfmt {
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

/// Bytes units, decimal and binary, matched case-insensitively (`5KB`,
/// `5kb`, `1MiB`, …).
const BYTES_UNITS: &[(&str, f64)] = &[
    ("kib", 1024.0),
    ("mib", 1024.0 * 1024.0),
    ("gib", 1024.0 * 1024.0 * 1024.0),
    ("tib", 1024.0 * 1024.0 * 1024.0 * 1024.0),
    ("pib", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
    ("kb", 1e3),
    ("mb", 1e6),
    ("gb", 1e9),
    ("tb", 1e12),
    ("pb", 1e15),
    ("b", 1.0),
];

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

/// Parses a bytes quantity to f64 bytes: `512b`, `5KB`, `1MiB`
/// (case-insensitive units, no compounding). `None` when the suffix is
/// not a bytes unit.
pub(crate) fn parse_bytes_value(raw: &str) -> Option<f64> {
    let num_end = raw
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(raw.len());
    if num_end == 0 {
        return None;
    }
    let n: f64 = raw[..num_end].parse().ok()?;
    let unit = raw[num_end..].to_ascii_lowercase();
    let (_, factor) = BYTES_UNITS.iter().find(|(u, _)| *u == unit)?;
    Some(n * factor)
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
        UnitKind::Bytes => value.parse().ok().or_else(|| parse_bytes_value(value)),
    }
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

fn compile_label_format(fmts: &[LabelFmt]) -> Result<Vec<CompiledLabelFmt>, PipelineError> {
    let mut out = Vec::with_capacity(fmts.len());
    let mut dsts: Vec<&str> = Vec::new();
    for f in fmts {
        let dst = match f {
            LabelFmt::Rename { dst, .. } | LabelFmt::Template { dst, .. } => dst.as_str(),
        };
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
    Ok(out)
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
fn eval_label_filter(
    f: &CompiledLabelFilter,
    labels: &[(Cow<'_, str>, Cow<'_, str>)],
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
            let v = convert_label_value(*kind, raw)?;
            Some(match op {
                CompareOp::Eq => v == *threshold,
                CompareOp::Neq => v != *threshold,
                CompareOp::Gt => v > *threshold,
                CompareOp::Gte => v >= *threshold,
                CompareOp::Lt => v < *threshold,
                CompareOp::Lte => v <= *threshold,
            })
        }
        CompiledLabelFilter::And(a, b) => {
            match (eval_label_filter(a, labels), eval_label_filter(b, labels)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            }
        }
        CompiledLabelFilter::Or(a, b) => {
            match (eval_label_filter(a, labels), eval_label_filter(b, labels)) {
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
        // class: line kept, error tagged.
        _ => {
            set_label(
                labels,
                Cow::Borrowed(ERROR_LABEL),
                Cow::Borrowed("JSONParserErr"),
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
// logfmt
// ---------------------------------------------------------------------

/// Applies the logfmt stage: pass 1 validates the whole line (a
/// malformed line contributes NO pairs, only the error label), pass 2
/// commits pairs through `to_cow` — the identity for the original body
/// (captures stay borrowed slices) or a copy for a rewritten line.
fn run_logfmt<'a, 't>(
    text: &'t str,
    extractions: &'a [(String, String)],
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    to_cow: impl Fn(Cow<'t, str>) -> Cow<'a, str>,
) {
    if walk_logfmt(text, &mut |_, _| {}).is_err() {
        set_label(
            labels,
            Cow::Borrowed(ERROR_LABEL),
            Cow::Borrowed("LogfmtParserErr"),
        );
        return;
    }
    if extractions.is_empty() {
        // Infallible: pass 1 above validated the line.
        let _ = walk_logfmt(text, &mut |k, v| {
            add_extracted(labels, to_cow(Cow::Borrowed(k)), to_cow(v));
        });
    } else {
        for (label, source_key) in extractions {
            let mut found: Option<Cow<'t, str>> = None;
            let _ = walk_logfmt(text, &mut |k, v| {
                if found.is_none() && k == source_key {
                    found = Some(v);
                }
            });
            let value = found.map(&to_cow).unwrap_or(Cow::Borrowed(""));
            add_extracted(labels, Cow::Borrowed(label.as_str()), value);
        }
    }
}

/// Minimal logfmt walk: space-separated `key=value` pairs, double-quoted
/// values with `\"`/`\\` escapes, bare keys => empty value. Values are
/// borrowed slices of `text` except quoted values containing an escape
/// (unescaping forces the only owned path). The only malformed class is
/// an unterminated quoted value (`Err`, sink output must be discarded).
fn walk_logfmt<'t>(text: &'t str, sink: &mut impl FnMut(&'t str, Cow<'t, str>)) -> Result<(), ()> {
    let mut rest = text.trim_start();
    while !rest.is_empty() {
        let key_end = rest
            .find(|c: char| c == '=' || c.is_whitespace())
            .unwrap_or(rest.len());
        let key = &rest[..key_end];
        rest = &rest[key_end..];
        let mut value: Cow<'t, str> = Cow::Borrowed("");
        if let Some(after_eq) = rest.strip_prefix('=') {
            if let Some(after_quote) = after_eq.strip_prefix('"') {
                let mut escaped = false;
                let mut closed_at = None;
                let mut chars = after_quote.char_indices();
                while let Some((i, c)) = chars.next() {
                    match c {
                        '\\' => {
                            escaped = true;
                            chars.next();
                        }
                        '"' => {
                            closed_at = Some(i);
                            break;
                        }
                        _ => {}
                    }
                }
                let Some(end) = closed_at else {
                    return Err(()); // unterminated quote
                };
                let raw = &after_quote[..end];
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
                rest = &after_quote[end + 1..];
            } else {
                let val_end = after_eq.find(char::is_whitespace).unwrap_or(after_eq.len());
                value = Cow::Borrowed(&after_eq[..val_end]);
                rest = &after_eq[val_end..];
            }
        }
        if !key.is_empty() {
            sink(key, value);
        }
        rest = rest.trim_start();
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
    fn a_rejected_unit_literal_is_a_named_compile_error_never_a_silent_zero() {
        let err = classify_numeric_literal(&NumericLiteral::DurationOrBytes("5xz".to_string()))
            .unwrap_err();
        assert!(matches!(err, PipelineError::BadParserExpr(_)));
        assert!(err.to_string().contains("5xz"));
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
}
