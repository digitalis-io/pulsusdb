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
//! - `line_format`/`label_format` bodies are FULL Go `text/template`
//!   programs with the reference's 67-function map (issue #230,
//!   [`super::template`]): a compile-error is a 400, a per-line
//!   execution error tags `TemplateFormatErr` + the engine's error
//!   detail and keeps the line (unchanged for `line_format`;
//!   destination unset for `label_format`).
//! - One `label_format` stage renders every template against a single
//!   data-map **snapshot** of the label set, so an assignment never
//!   observes an earlier one in the same stage; and `__error__` is not
//!   assignable at all (issue #231, both live-probed).
//! - The `__error__`/`__error_details__` pair lives OUT-OF-BAND in
//!   [`ErrorSlots`] (issue #238), mirroring the reference's two plain
//!   string fields on the label builder (`pkg/logql/log/labels.go:119-121`)
//!   — never as ordinary vector entries. Consequences (all live-probed at
//!   v3.7.4): a `label_format` rename cannot see the slots, `keep` always
//!   retains the pair (plus `__preserve_error__`), `drop __error__` resets
//!   only the err slot (the details slot survives, gated), string
//!   `__error__` filters and `drop` matchers read the slots while
//!   `__error_details__` filters read the vector, `ip(...)` passes an
//!   errored line unconditionally, and a numeric-filter failure never
//!   overwrites an earlier error (first error wins). The pair merges into
//!   the emitted set once, at [`ErrorSlots::merge_into`], gated on the
//!   builder-dirty bit (`hasDel() || hasAdd()`, `labels.go:554-563`).

use std::borrow::Cow;
use std::fmt;
use std::net::IpAddr;

use pulsus_logql::walk;
use pulsus_logql::{
    CompareOp, DropKeepElem, LabelFilterExpr, LabelFmt, LabelMatch, LineFilterOp, MatchOp,
    NumericLiteral, ParserStage, Stage,
};

use super::ip::{IpMatcher, line_has_ip_in};
use super::labels::{EMPTY_STRUCTURED_METADATA, StructuredMetadataCtx};
use super::template::{self, Part as TmplPart, Template, TemplateEnv, TemplateKind};
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

/// The reference's `errTemplateFormat` (`pkg/logql/log/error.go:9`): the
/// per-line error class a FAILING template execution sets — the query
/// succeeds, the line keeps flowing (issue #230; `fmt.go:252-256`,
/// `:426-429`).
pub const TEMPLATE_FORMAT_ERROR: &str = "TemplateFormatErr";

/// The reference's `PreserveErrorLabel` (`pkg/logqlmodel/error.go:26`).
/// PulsusDB never sets it, but `label_format __preserve_error__=…` can, and
/// `keep` must then retain it (`pkg/logql/log/keep_labels.go:51-58`,
/// live-probed — issue #238). No other handling exists, by design.
pub const PRESERVE_ERROR_LABEL: &str = "__preserve_error__";

/// The reference's OUT-OF-BAND error pair: two plain `string` fields on the
/// label builder (`pkg/logql/log/labels.go:119-121`), NOT entries in the
/// label set. EMPTY MEANS UNSET — `HasErr()` is `b.err != ""`
/// (`labels.go:245`), `HasErrorDetails()` is `b.errDetails != ""`
/// (`labels.go:268`), and `appendErrors` emits each slot only when non-empty
/// (`labels.go:430-444`). Deliberately NOT `Option<Cow>`: an empty reserved
/// value must be indistinguishable from an unset slot (issue #238 round-3
/// finding 1). A name may be populated here AND in the label vector at the
/// same time; the two are mutated independently and the slot wins at emit
/// (`labels.go:430-444`, `516-521` — live-probed at v3.7.4).
#[derive(Debug, Default)]
struct ErrorSlots<'a> {
    err: Cow<'a, str>,
    details: Cow<'a, str>,
    /// `hasDel() || hasAdd()` (`labels.go:212-223`): any label ADDED to or
    /// REMOVED from the vector by any stage, plus the per-entry
    /// structured-metadata merge (the reference adds SM through the builder
    /// — `pipeline.go:104`). Monotone: never cleared.
    dirty: bool,
}

impl<'a> ErrorSlots<'a> {
    /// `HasErr()` — `labels.go:245`.
    fn has_err(&self) -> bool {
        !self.err.is_empty()
    }

    /// `HasErrorDetails()` — `labels.go:268`.
    fn has_details(&self) -> bool {
        !self.details.is_empty()
    }

    /// `SetErr`/`ResetError` (`labels.go:234`, `:249`) are ONE operation:
    /// assignment. Assigning "" clears, exactly as `Add` does for an empty
    /// reserved SM value (`labels.go:399-407`).
    fn set_err(&mut self, v: Cow<'a, str>) {
        self.err = v;
    }

    /// `SetErrorDetails`/`ResetErrorDetails` (`labels.go:255`, `:259`).
    fn set_details(&mut self, v: Cow<'a, str>) {
        self.details = v;
    }

    fn reset_err(&mut self) {
        self.err = Cow::Borrowed("");
    }

    fn reset_details(&mut self) {
        self.details = Cow::Borrowed("");
    }

    /// `labelValue`'s `GetErr()` (`label_filter.go:418-421`) and the `drop`
    /// matcher's `GetErr` (`drop_labels.go:69`) — UNGATED: label filters,
    /// `drop` matchers and `ip` read the slot itself, not the materialised
    /// view (probed: `drop __error__="JSONParserErr"` matches on a clean
    /// builder), and "" for an unset slot by construction.
    fn err_str(&self) -> &str {
        &self.err
    }

    /// `GetErrorDetails` (`drop_labels.go:76`) — UNGATED, see [`Self::err_str`].
    fn details_str(&self) -> &str {
        &self.details
    }

    /// The pair AS MATERIALISED right now: the fast-path gate at
    /// `labels.go:555`/`:573`/`:593`/`:673` (`!hasDel() && !hasAdd() &&
    /// !HasErr()` returns the untouched base set), then `labels.go:519`
    /// (`HasErr() || HasErrorDetails()`), then `appendErrors`' per-slot
    /// non-empty guard. A lone details slot on a CLEAN builder is invisible
    /// (live-probed: `| json | drop __error__` emits no details; adding any
    /// `Del`/`Set` makes them reappear).
    fn visible(&self) -> (Option<&str>, Option<&str>) {
        if !(self.dirty || self.has_err()) {
            return (None, None);
        }
        (
            self.has_err().then(|| self.err.as_ref()),
            self.has_details().then(|| self.details.as_ref()),
        )
    }

    /// UNGATED slot accessor for the `label_format`/`line_format` data map —
    /// the caller supplies the gate captured at map-build time ([`StageMap`]).
    /// Returns `None` for an EMPTY slot, so the vector entry (if any) is then
    /// consulted, matching `appendErrors` never appending an empty slot into
    /// the map.
    fn raw_slot(&self, name: &str) -> Option<&str> {
        match name {
            ERROR_LABEL if self.has_err() => Some(&self.err),
            ERROR_DETAILS_LABEL if self.has_details() => Some(&self.details),
            _ => None,
        }
    }

    /// `appendErrors` (`labels.go:430-444`) over [`Self::visible`]'s gate.
    /// Called ONCE, on the kept path only, immediately before
    /// `MetricRun::Kept` — dropped lines never merge. Consumes `self` so the
    /// slot `Cow`s MOVE into the vector: zero new allocations (the owned
    /// detail `String` built at the error site is `set_label`-ed here, same
    /// allocation count as the pre-#238 direct write).
    fn merge_into(self, labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>) {
        if !(self.dirty || self.has_err()) {
            return;
        }
        if !self.err.is_empty() {
            set_label(labels, Cow::Borrowed(ERROR_LABEL), self.err);
        }
        if !self.details.is_empty() {
            set_label(labels, Cow::Borrowed(ERROR_DETAILS_LABEL), self.details);
        }
    }
}

/// The reference's per-stage template data map (`fmt.go:423-425`), built
/// lazily at the first template assignment and frozen once non-empty
/// (issue #231); issue #238 adds the error-pair gate state, captured ONCE
/// at build time — `dirty` can flip mid-stage (each `Set` dirties), so a
/// per-template re-read would let a second template see details the first
/// could not (live-probed guard: two templates reading `__error_details__`
/// after `drop __error__` + `drop nosuch=""` both render the SAME value).
struct StageMap<'a> {
    /// Present ONLY when the #231 compile-time `needs_snapshot` gate is set;
    /// `None` = render against the live vector (the zero-copy fast path).
    ///
    /// A [`template::LabelSnapshot`], not a bare `Vec` (issue #260 review
    /// round 2): the copy duplicates every OWNED value in the label set —
    /// including template output this row already charged for — and its
    /// only constructor charges the row budget for exactly what it
    /// duplicates. The field's type is what makes an uncharged snapshot
    /// unrepresentable rather than merely absent.
    snapshot: Option<template::LabelSnapshot<'a>>,
    /// The error-pair AS THE MAP FROZE IT (issue #230): pre-#230 the slot
    /// values were invariant inside a `label_format` stage (the old U5
    /// note), so a gate bool sufficed; a FULL template can now fail per
    /// line and OVERWRITE the slots mid-stage (`fmt.go:426-429`), while
    /// the reference's data map — built once, `fmt.go:423-425` — keeps
    /// showing the values from map-build time. Snapshotting the values
    /// (owned, only when the visibility gate was open at build)
    /// reproduces that exactly.
    frozen_err: Option<String>,
    frozen_details: Option<String>,
}

/// Loki (grafana/loki:3.4.2, buger/jsonparser) reports this fixed detail
/// for a top-level non-object line — the representative `JSONParserErr`
/// class (oracle_probe.txt [1]). Partial-object inputs take an
/// internal-scanner-state-dependent message (and Loki partially extracts
/// them); those are ledgered off-corpus, not reproduced.
const JSON_ERROR_DETAILS: &str = "Value looks like object, but can't find closing '}' symbol";

/// A `line_format`/`label_format` render breached the per-render
/// output-byte budget (issue #230 follow-up;
/// [`super::template::MAX_TEMPLATE_RENDER_BYTES`]). Query-aborting: the
/// exec layer maps it to the bounded 422
/// (`TooBroadReason::TemplateOutputBytes`) — never a per-line
/// `TemplateFormatErr`, never a truncation, never an OOM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemplateBudgetExceeded {
    pub budget_bytes: u64,
}

impl TemplateBudgetExceeded {
    pub(crate) fn new() -> Self {
        TemplateBudgetExceeded {
            budget_bytes: super::template::MAX_TEMPLATE_RENDER_BYTES,
        }
    }
}

impl fmt::Display for TemplateBudgetExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "template render exceeded the {}-byte output budget",
            self.budget_bytes
        )
    }
}

impl std::error::Error for TemplateBudgetExceeded {}

/// Errors from compiling a pipeline — all client-caused, surfaced as
/// [`super::error::ReadError::PipelineInvalid`] (400-class).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineError {
    BadRegex(String),
    /// A `line_format`/`label_format` template body the template engine
    /// rejects at compile time (issue #230). Carries the reference's full
    /// message (`invalid line template: template: line:1: …` /
    /// `invalid template for label '<dst>': …`).
    InvalidTemplate(String),
    BadParserExpr(String),
    BadIpFilter(String),
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineError::BadRegex(msg) => write!(f, "bad regex: {msg}"),
            PipelineError::InvalidTemplate(msg) => f.write_str(msg),
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
#[derive(Debug, Clone)]
enum LineMatcher {
    Literal(String),
    Regex(regex::Regex),
    Ip(IpMatcher),
}

/// One node of a compiled label filter, in **post-order**.
///
/// The three leaf variants keep byte-identical variant names, field
/// names and field order to the tree this replaced, so leaf rendering
/// stays derive-generated and `CompiledPipeline`'s `Debug` bytes are
/// unchanged (pinned by `tests/golden/plan_walk_characterization.txt`,
/// captured from the derive before this conversion).
#[derive(Debug, Clone)]
enum LfOp {
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
    And,
    Or,
}

/// The inline verdict-stack width.
///
/// **Derived so that NO parser-admissible filter can spill**, which is
/// what makes the zero-allocation-per-row claim true for every shape we
/// accept rather than for the common one. Post-order over a LEFT-DEEP
/// chain — what `parse_label_filter_or`'s `while` builds — has
/// `max_stack == 2` regardless of width; the worst case is full RIGHT
/// nesting, whose verdict stack is one deeper than the parse depth, and
/// `pulsus-logql`'s `LABEL_FILTER_MAX_DEPTH` bounds that at 91. 96
/// leaves headroom and costs 96 bytes of stack (`Option<bool>` is one
/// byte).
///
/// `max_stack_never_spills_for_any_parser_admissible_filter` measures
/// the deepest filter the parser accepts and asserts it fits, so raising
/// the parser guard without raising this reddens a named test rather
/// than silently reintroducing a per-row allocation.
const LF_INLINE_STACK: usize = 96;

/// A compiled label filter, **flattened** (issue #272).
///
/// The tree this replaced recursed on `compile`, on `Debug`/`Clone`
/// glue, and once per node per row in `eval` — and a flat
/// `a or b or c …` chain compiles into a LEFT-DEEP tree, so query WIDTH
/// became compiled DEPTH. Measured before this change: `compile` aborted
/// a 2 MiB stack at **3,000 terms / 33,696 bytes**, well inside #279's
/// 131,072-byte admission cap.
///
/// Flat post-order removes the recursion AND the per-row allocation a
/// tree walk needs: evaluation is a linear scan over `ops` with an
/// on-stack verdict array.
#[derive(Clone)]
struct CompiledLabelFilter {
    ops: Vec<LfOp>,
    /// The verdict stack's exact high-water mark.
    max_stack: u32,
    /// Precomputed replacement for the former `filter_contains_compare`
    /// walk. Not rendered by `Debug` — the bytes must not move.
    has_compare: bool,
}

impl CompiledLabelFilter {
    /// The index of each internal node's two operands, recovered from
    /// post-order in one linear pass, plus the root index. Used only by
    /// `Debug`, which is not a hot path.
    fn tree_shape(&self) -> (Vec<(usize, usize)>, usize) {
        let mut pending: Vec<usize> = Vec::new();
        let mut kids: Vec<(usize, usize)> = vec![(0, 0); self.ops.len()];
        for (i, op) in self.ops.iter().enumerate() {
            if matches!(op, LfOp::And | LfOp::Or) {
                let r = pending.pop().unwrap_or(0);
                let l = pending.pop().unwrap_or(0);
                kids[i] = (l, r);
            }
            pending.push(i);
        }
        let root = pending.pop().unwrap_or(0);
        (kids, root)
    }
}

impl fmt::Debug for CompiledLabelFilter {
    /// Byte-equivalent to the `#[derive(Debug)]` on the TREE this
    /// replaced, in both modes: leaves delegate to `LfOp`'s own derive
    /// (same variant/field names and order), and `And`/`Or` are rendered
    /// as the two-field tuple variants they were. Iterative — an
    /// explicit frame stack, never one machine frame per node.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let alt = f.alternate();
        let (kids, root) = self.tree_shape();
        if self.ops.is_empty() {
            return f.write_str("<empty>");
        }
        // (op index, step, indentation level)
        let mut stack: Vec<(usize, u8, usize)> = vec![(root, 0, 0)];
        while let Some((i, step, level)) = stack.pop() {
            match &self.ops[i] {
                leaf @ (LfOp::Match { .. } | LfOp::Compare { .. } | LfOp::Ip { .. }) => {
                    walk::dbg_own(f, leaf, alt, level)?;
                }
                LfOp::And | LfOp::Or => {
                    let name = if matches!(self.ops[i], LfOp::And) {
                        "And"
                    } else {
                        "Or"
                    };
                    let (l, r) = kids[i];
                    match step {
                        0 => {
                            walk::dbg_open_tuple(f, name, alt, level)?;
                            stack.push((i, 1, level));
                            stack.push((l, 0, level + 1));
                        }
                        1 => {
                            walk::dbg_sep(f, alt, level)?;
                            stack.push((i, 2, level));
                            stack.push((r, 0, level + 1));
                        }
                        _ => walk::dbg_close_tuple(f, alt, level)?,
                    }
                }
            }
        }
        Ok(())
    }
}

/// One `json` extraction path segment (`a.b[0].c` / `a["k"]` shapes).
#[derive(Debug, Clone, PartialEq)]
enum JsonPathSeg {
    Field(String),
    Index(usize),
}

#[derive(Debug, Clone)]
enum PatternTok {
    Literal(String),
    Capture(String),
    Discard,
}

#[derive(Debug, Clone)]
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
    LineFormat {
        tmpl: Template,
        /// Some field names `__error__`/`__error_details__` — decided once
        /// at compile time ([`template_reads_error_pair`]) so every ordinary
        /// template keeps the slot-blind zero-copy render (issue #238).
        /// Always true for `Template::Full` (a full-language template can
        /// reach the pair dynamically — `index . "__error__"`).
        reads_err: bool,
    },
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
    /// #200). `__error__`/`__error_details__` elements reset the OUT-OF-BAND
    /// slot only — bare form unconditionally, matcher form iff the matcher
    /// matches the slot value — and never touch a vector entry of that name
    /// (`drop_labels.go:51-86`; issue #238).
    Drop(Vec<CompiledDropKeep>),
    /// `| keep <elems>` — retain only labels matched by the list (issue
    /// #200). `__error__`/`__error_details__`/`__preserve_error__` vector
    /// entries are ALWAYS retained by name, and the out-of-band slots are
    /// untouched (`keep_labels.go:22`, `:51-57`; issue #238).
    Keep(Vec<CompiledDropKeep>),
}

/// One compiled `drop`/`keep` element: a label name plus an optional
/// value matcher (regexes compiled once, fully anchored).
#[derive(Debug, Clone)]
struct CompiledDropKeep {
    label: String,
    matcher: Option<CompiledDropKeepMatch>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
enum CompiledLabelFmt {
    /// `label_format dst=src` where `dst != src`.
    Rename { dst: String, src: String },
    /// `label_format x=x`. The reference runs `Set(ParsedLabel, dst, v)` and
    /// THEN `Del(src)` (`fmt.go:417-418`), so when the two names are equal a
    /// *resolved* rename net-DELETES the label (and still dirties the
    /// builder); an unresolved one stays a complete no-op. Split at compile
    /// time so the hot path keeps a comparison-free arm (issue #238,
    /// live-probed: `{r1} | label_format env=env` drops `env`).
    RenameSelf { name: String },
    Template {
        dst: String,
        tmpl: Template,
        /// Some field names `__error__`/`__error_details__` — the same
        /// compile-time gate as `CompiledStage::LineFormat`'s `reads_err`.
        reads_err: bool,
    },
}

/// The compiled, reusable per-line evaluator (consumed by the streams
/// read path here and by the M6-10 metric-pipeline seam later).
#[derive(Debug, Clone)]
pub struct CompiledPipeline {
    stages: Vec<CompiledStage>,
    /// The template execution environment (issue #230): the `Local`
    /// zone + wall clock the reference resolves from the process.
    /// Tests/the corpus runner override it via
    /// [`CompiledPipeline::with_template_env`] to pin determinism.
    template_env: TemplateEnv,
    mutates_labels: bool,
    rewrites_line: bool,
    line_filter_only: bool,
    has_unwrap: bool,
    /// Compile-state carry so [`CompiledPipeline::extended_with`] resumes
    /// EXACTLY where [`CompiledPipeline::compile`] stopped (issue #221):
    /// whether a line-rewriting stage was seen (a tail line filter after
    /// it could not push down)…
    seen_line_format: bool,
    /// …and whether every SOURCE stage so far was a line filter (the
    /// `line_filter_only` fast-path derivation).
    all_line_filter_source: bool,
}

/// Compile state carried across stages, so [`CompiledPipeline::compile`]
/// and [`CompiledPipeline::extended_with`] share ONE per-stage
/// implementation ([`compile_stage`]) — no second compiler to drift
/// (issue #221).
#[derive(Debug, Clone, Copy)]
struct CompileState {
    seen_line_format: bool,
    mutates_labels: bool,
    rewrites_line: bool,
    has_unwrap: bool,
    all_line_filter_source: bool,
}

impl Default for CompileState {
    fn default() -> Self {
        CompileState {
            seen_line_format: false,
            mutates_labels: false,
            rewrites_line: false,
            has_unwrap: false,
            // Vacuously true over the empty prefix; any non-line-filter
            // source stage clears it.
            all_line_filter_source: true,
        }
    }
}

/// Compiles ONE source stage, updating the carried [`CompileState`].
/// `Ok(None)` = the stage is pushed down to SQL and skipped here (the
/// pushable-line-filter case). Extracted verbatim from the former
/// `compile` loop body (issue #221) so `compile` and `extended_with`
/// cannot drift.
fn compile_stage(
    stage: &Stage,
    st: &mut CompileState,
) -> Result<Option<CompiledStage>, PipelineError> {
    if !matches!(stage, Stage::LineFilter(_)) {
        st.all_line_filter_source = false;
    }
    match stage {
        Stage::LineFilter(lf) => {
            // A line filter is pushed down to SQL (and skipped here to
            // avoid double evaluation) ONLY when it both precedes the
            // first `line_format` (it references the original `body`)
            // AND is pushable — i.e. no alternative is an `ip("…")`.
            // An `ip(…)`/mixed-`or` filter, or any filter following a
            // `line_format`, is served here client-side, matching
            // `plan::compile_line_filters`'s pushdown split exactly.
            if !st.seen_line_format && super::plan::is_pushable_line_filter(lf) {
                return Ok(None);
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
            let negated = matches!(lf.op, LineFilterOp::NotContains | LineFilterOp::NotRegex);
            Ok(Some(CompiledStage::LineFilter { matchers, negated }))
        }
        Stage::Parser(p) => {
            st.mutates_labels = true;
            Ok(Some(compile_parser(p)?))
        }
        Stage::LabelFilter(expr) => {
            let filter = compile_label_filter(expr)?;
            // A numeric comparison can add `__error__` on a
            // conversion failure — that changes the label set, so
            // it must route through the fan-out path (correctness
            // refinement over the plan's parser/label_format-only
            // trigger; flagged in the implementation notes).
            if filter.has_compare {
                st.mutates_labels = true;
            }
            Ok(Some(CompiledStage::LabelFilter(filter)))
        }
        Stage::LineFormat(tmpl) => {
            st.seen_line_format = true;
            st.rewrites_line = true;
            let compiled = compile_template(tmpl, TemplateKind::Line)?;
            if matches!(compiled, Template::Full(_)) {
                // A full-language template can FAIL per line, which sets
                // the error pair — a label-set change, so the metric
                // fan-out must group by final labels (the Parts shapes
                // cannot error and keep the cheaper path).
                st.mutates_labels = true;
            }
            let reads_err = template_reads_error_pair(&compiled);
            Ok(Some(CompiledStage::LineFormat {
                tmpl: compiled,
                reads_err,
            }))
        }
        Stage::LabelFormat(fmts) => {
            st.mutates_labels = true;
            Ok(Some(compile_label_format(fmts)?))
        }
        Stage::Unwrap(u) => {
            st.has_unwrap = true;
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
            Ok(Some(CompiledStage::Unwrap {
                label: u.label.clone(),
                kind,
            }))
        }
        Stage::Unpack => {
            // Unpack rewrites the line (`_entry` becomes the line) and
            // promotes fields to labels — a following line filter must
            // therefore evaluate in-engine, so it sets the line-rewrite
            // gate exactly like `line_format`.
            st.mutates_labels = true;
            st.rewrites_line = true;
            st.seen_line_format = true;
            Ok(Some(CompiledStage::Unpack))
        }
        Stage::Decolorize => {
            // Decolorize rewrites the line; a following line filter must
            // evaluate in-engine (the raw body still carries the color
            // codes ClickHouse would match against).
            st.rewrites_line = true;
            st.seen_line_format = true;
            Ok(Some(CompiledStage::Decolorize(compile_regex(
                DECOLORIZE_PATTERN,
            )?)))
        }
        Stage::Drop(elems) => {
            st.mutates_labels = true;
            Ok(Some(CompiledStage::Drop(compile_drop_keep(elems)?)))
        }
        Stage::Keep(elems) => {
            st.mutates_labels = true;
            Ok(Some(CompiledStage::Keep(compile_drop_keep(elems)?)))
        }
    }
}

/// The compiled-stage slot width, exported so the variants arena can
/// bound a cloned stage list's retained heap without duplicating this
/// enum's layout (issue #221). `CompiledStage` itself stays private; only
/// its size crosses the boundary.
pub(crate) const COMPILED_STAGE_SLOT_BYTES: usize = size_of::<CompiledStage>();

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
        let mut st = CompileState::default();
        let mut compiled = Vec::new();
        for stage in stages {
            if let Some(cs) = compile_stage(stage, &mut st)? {
                compiled.push(cs);
            }
        }
        Ok(Self::from_parts(compiled, st))
    }

    /// A clone of `self` with `tail` compiled and appended, RESUMING the
    /// compile state (`seen_line_format` etc.), so the result is
    /// behaviourally identical to `compile(source ++ tail)` for ANY tail
    /// (issue #221 — the variants pipeline arena). The clone SHARES every
    /// already-compiled regex program with `self`: `regex::Regex` is
    /// `Arc`-backed, so a clone costs a fresh lazily-populated cache
    /// pool, never a recompiled program — the coder must NOT "simplify"
    /// this back to `compile(common ++ tail)` per variant, which would
    /// recompile every common-pipeline regex per distinct tail (a real
    /// OOM vector: each program is bounded only by the crate's 10 MiB
    /// `nfa_size_limit`).
    pub fn extended_with(&self, tail: &[Stage]) -> Result<Self, PipelineError> {
        let mut st = CompileState {
            seen_line_format: self.seen_line_format,
            mutates_labels: self.mutates_labels,
            rewrites_line: self.rewrites_line,
            has_unwrap: self.has_unwrap,
            all_line_filter_source: self.all_line_filter_source,
        };
        let mut stages = self.stages.clone();
        for stage in tail {
            if let Some(cs) = compile_stage(stage, &mut st)? {
                stages.push(cs);
            }
        }
        Ok(Self::from_parts(stages, st))
    }

    /// The ONE assembly point `compile`/`extended_with` share, so the
    /// `line_filter_only` fast-path derivation cannot drift between them:
    /// fast path only when the pipeline is line filters AND every one
    /// pushed down (nothing compiled to run). A non-pushable `ip(…)`/
    /// mixed-`or` filter compiles a run-stage, so `stages` is non-empty
    /// and the fast path is (correctly) declined.
    fn from_parts(stages: Vec<CompiledStage>, st: CompileState) -> Self {
        let line_filter_only = stages.is_empty() && st.all_line_filter_source;
        CompiledPipeline {
            stages,
            template_env: TemplateEnv::process(),
            mutates_labels: st.mutates_labels,
            rewrites_line: st.rewrites_line,
            line_filter_only,
            has_unwrap: st.has_unwrap,
            seen_line_format: st.seen_line_format,
            all_line_filter_source: st.all_line_filter_source,
        }
    }

    /// Structural equality of two compiled pipelines' LABEL-FILTER
    /// programs — **including the two fields `Debug` does not render**,
    /// `max_stack` and `has_compare`.
    ///
    /// Test-support, and narrowly scoped: it exists because #272's
    /// widest-chain stack gate must prove a `Clone` carried the whole
    /// program, and a rendered-length comparison catches OMISSION but
    /// not same-width CORRUPTION — a clone that kept `ops.len()` while
    /// changing an op, or that dropped either unrendered field, renders
    /// identically. Compiled regex programs are compared by their source
    /// pattern, which is the only stable identity a `regex::Regex` has.
    #[doc(hidden)]
    pub fn label_filter_programs_eq(&self, other: &Self) -> bool {
        fn programs(p: &CompiledPipeline) -> Vec<&CompiledLabelFilter> {
            p.stages
                .iter()
                .filter_map(|s| match s {
                    CompiledStage::LabelFilter(f) => Some(f),
                    _ => None,
                })
                .collect()
        }
        fn op_eq(a: &LfOp, b: &LfOp) -> bool {
            match (a, b) {
                (
                    LfOp::Match {
                        name: an,
                        op: ao,
                        value: av,
                        re: ar,
                    },
                    LfOp::Match {
                        name: bn,
                        op: bo,
                        value: bv,
                        re: br,
                    },
                ) => {
                    an == bn
                        && ao == bo
                        && av == bv
                        && ar.as_ref().map(regex::Regex::as_str)
                            == br.as_ref().map(regex::Regex::as_str)
                }
                (
                    LfOp::Compare {
                        name: an,
                        op: ao,
                        kind: ak,
                        threshold: at,
                    },
                    LfOp::Compare {
                        name: bn,
                        op: bo,
                        kind: bk,
                        threshold: bt,
                    },
                ) => an == bn && ao == bo && ak == bk && at.to_bits() == bt.to_bits(),
                (
                    LfOp::Ip {
                        name: an,
                        matcher: am,
                        negated: ag,
                    },
                    LfOp::Ip {
                        name: bn,
                        matcher: bm,
                        negated: bg,
                    },
                ) => an == bn && am == bm && ag == bg,
                (LfOp::And, LfOp::And) | (LfOp::Or, LfOp::Or) => true,
                (LfOp::Match { .. }, _)
                | (LfOp::Compare { .. }, _)
                | (LfOp::Ip { .. }, _)
                | (LfOp::And, _)
                | (LfOp::Or, _) => false,
            }
        }
        let (a, b) = (programs(self), programs(other));
        a.len() == b.len()
            && a.iter().zip(b.iter()).all(|(x, y)| {
                x.max_stack == y.max_stack
                    && x.has_compare == y.has_compare
                    && x.ops.len() == y.ops.len()
                    && x.ops.iter().zip(y.ops.iter()).all(|(p, q)| op_eq(p, q))
            })
    }

    /// Overrides the template execution environment (tests + the
    /// hermetic corpus runner: pinned UTC zone and injectable `now`).
    pub fn with_template_env(mut self, env: TemplateEnv) -> Self {
        self.template_env = env;
        self
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
    pub fn run<'a>(
        &'a self,
        body: &'a str,
        base: &'a [(String, String)],
        ts_ns: i64,
    ) -> Result<Option<EntryOut<'a>>, TemplateBudgetExceeded> {
        let mut labels = Vec::new();
        let Some(line) = self.run_into(body, base, ts_ns, &mut labels)? else {
            return Ok(None);
        };
        Ok(Some(EntryOut { line, labels }))
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
        ts_ns: i64,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    ) -> Result<Option<Cow<'a, str>>, TemplateBudgetExceeded> {
        self.run_into_with_sm(body, base, ts_ns, &EMPTY_STRUCTURED_METADATA, labels)
    }

    /// As [`CompiledPipeline::run_into`], for a row carrying per-entry
    /// structured metadata (issue #238). `base` is the stream labels merged
    /// with the ORDINARY SM entries only; `sm` carries the reserved-name
    /// routing outcome and `has_ordinary` (see
    /// [`StructuredMetadataCtx`]). The reference adds structured metadata
    /// through the label builder (`pkg/logql/log/pipeline.go:104` →
    /// `LabelsBuilder.Add`), which routes reserved names into the
    /// out-of-band error slots and marks the builder dirty for ordinary
    /// entries — live-probed A/B: identical query and line, SM present vs
    /// absent, `| json | drop __error__` emits `__error_details__` only in
    /// the (ordinary-)SM case.
    pub fn run_into_with_sm<'a>(
        &'a self,
        body: &'a str,
        base: &'a [(String, String)],
        ts_ns: i64,
        sm: &'a StructuredMetadataCtx,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    ) -> Result<Option<Cow<'a, str>>, TemplateBudgetExceeded> {
        match self.run_mode_into(body, base, ts_ns, sm, labels, false)? {
            (MetricRun::Dropped, _) => Ok(None),
            (MetricRun::Kept { line, .. }, _) => Ok(Some(line)),
        }
    }

    /// As [`CompiledPipeline::run_into`], additionally reporting whether the
    /// pipeline FINISHED with the out-of-band err slot set — the reference's
    /// `HasErr()` (`labels.go:245`), evaluated before materialization
    /// (issue #238 review round 7, the ninth site). This is NOT the same
    /// question as "does the emitted set contain an `__error__` label": a
    /// parser may extract an ORDINARY label literally named `__error__`
    /// (`parser.go:160` `Set(ParsedLabel, …)` — never the slot) while
    /// `HasErr()` stays false, and conversely a slot error always merges
    /// into the emitted set. Detected-fields auto-detection keys its
    /// parser-failure test on THIS flag, not on a label-name scan
    /// (reference-captured: `{"__error__":"","foo":"x"}` and
    /// `{"__error__":"mine","foo":"x"}` both auto-detect as json with `foo`
    /// retained).
    pub(crate) fn run_into_reporting_err<'a>(
        &'a self,
        body: &'a str,
        base: &'a [(String, String)],
        ts_ns: i64,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    ) -> Result<(Option<Cow<'a, str>>, bool), TemplateBudgetExceeded> {
        match self.run_mode_into(body, base, ts_ns, &EMPTY_STRUCTURED_METADATA, labels, false)? {
            (MetricRun::Dropped, has_err) => Ok((None, has_err)),
            (MetricRun::Kept { line, .. }, has_err) => Ok((Some(line), has_err)),
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
        ts_ns: i64,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    ) -> Result<MetricRun<'a>, TemplateBudgetExceeded> {
        // `MetricScanRow` carries no structured metadata (issue #249), so the
        // metric path always seeds empty slots.
        Ok(self
            .run_mode_into(body, base, ts_ns, &EMPTY_STRUCTURED_METADATA, labels, true)?
            .0)
    }

    /// The second element of the return is the reference's `HasErr()` at the
    /// end of the pipeline — the pre-materialization slot state
    /// [`CompiledPipeline::run_into_reporting_err`] surfaces.
    fn run_mode_into<'a>(
        &'a self,
        body: &'a str,
        base: &'a [(String, String)],
        ts_ns: i64,
        sm: &'a StructuredMetadataCtx,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
        metric: bool,
    ) -> Result<(MetricRun<'a>, bool), TemplateBudgetExceeded> {
        let mut line: Cow<'a, str> = Cow::Borrowed(body);
        let mut value: Option<f64> = None;
        // A successfully-unwrapped label pending deletion (issue #221): the
        // reference's post-`unwrap` label filters run over the STILL-PRESENT
        // label (`labelSampleExtractor.Process` applies `postFilters`
        // before the grouping step deletes the unwrapped label from the
        // result series), so `| unwrap v | v > 1` filters by the raw label
        // value. The deletion is therefore DEFERRED to the end of the stage
        // loop — only label filters can follow `unwrap` (parser rule), so
        // this is exactly the reference's ordering.
        let mut unwrapped: Option<&str> = None;
        // **The template render budget's lifetime is the ROW** (issue
        // #260). Every render this row performs — the `line_format`
        // rewrite and each `Template::Full` `label_format` destination —
        // charges against THIS ledger, because every one of those outputs
        // is RETAINED (into `line`, or into the label set via
        // `set_label`) and they are all live at the same time. A budget
        // constructed per `render_full` bounded one output while the
        // NUMBER of live outputs was bounded only by the query-text cap
        // (>4 000 destinations of 64 MiB each fit inside 131 072 bytes).
        // Two `Cell`s on the stack — no allocation, so the per-row
        // allocation gates are untouched.
        let render_budget = template::RenderBudget::default();
        labels.clear();
        labels.extend(
            base.iter()
                .map(|(k, v)| (Cow::Borrowed(k.as_str()), Cow::Borrowed(v.as_str()))),
        );
        // Seed the out-of-band pair from the row's structured-metadata
        // routing outcome (issue #238): a `Cow::Borrowed("")` seed is exactly
        // the `Default`, so the no-SM path is bit-identical to pre-#238.
        let mut errs = ErrorSlots {
            err: Cow::Borrowed(sm.err.as_str()),
            details: Cow::Borrowed(sm.details.as_str()),
            dirty: sm.has_ordinary,
        };

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
                        return Ok((MetricRun::Dropped, errs.has_err()));
                    }
                }
                CompiledStage::Json { extractions } => {
                    run_json(&line, extractions, labels, &mut errs)
                }
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
                        Cow::Borrowed(text) => run_logfmt(
                            text,
                            *strict,
                            *keep_empty,
                            extractions,
                            labels,
                            &mut errs,
                            |c| c,
                        ),
                        Cow::Owned(text) => run_logfmt(
                            text,
                            *strict,
                            *keep_empty,
                            extractions,
                            labels,
                            &mut errs,
                            |c| Cow::Owned(c.into_owned()),
                        ),
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
                                            &mut errs.dirty,
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
                                            &mut errs.dirty,
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
                                        &mut errs.dirty,
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
                                        &mut errs.dirty,
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
                    match eval_label_filter(filter, labels, &errs, &mut failed) {
                        Some(true) => {}
                        Some(false) => return Ok((MetricRun::Dropped, errs.has_err())),
                        // Conversion failure: keep the line, tag the error
                        // class in the out-of-band slots (pinned semantics,
                        // oracle-verified; a later `__error__=""` filter
                        // drops it) — but ONLY when no earlier error is set:
                        // the reference guards every numeric-family write on
                        // `!lbs.HasErr()` (`label_filter.go:184`, `:204`,
                        // `:252`, `:272`, `:315`, `:335` — "first error
                        // wins", live-probed). When the guard blocks, the
                        // detail `String` is never built (alloc budget).
                        None => {
                            if !errs.has_err() {
                                let details = failed
                                    .map(|(kind, value)| label_filter_error_details(kind, value));
                                errs.set_err(Cow::Borrowed("LabelFilterErr"));
                                if let Some(details) = details {
                                    errs.set_details(Cow::Owned(details));
                                }
                            }
                        }
                    }
                }
                CompiledStage::LineFormat { tmpl, reads_err } => {
                    // The error-pair gate is evaluated inline at this stage
                    // (no `StageMap` needed), and only when the compile-time
                    // `reads_err` flag says the template can name the pair —
                    // every other `line_format` keeps the slot-blind
                    // zero-copy render (issue #238). Full templates (#230)
                    // render through the engine; a FAILED render keeps the
                    // line UNCHANGED and tags `TemplateFormatErr` + the
                    // byte-exact detail (`fmt.go:252-256`) — the query
                    // succeeds.
                    // **One TYPE, not one charge per site** (issue #260
                    // review round 2). Every arm yields a
                    // `template::Retained`, which can only be built by a
                    // constructor that charges the row budget first — so
                    // a fourth render shape is an arm that will not
                    // compile until it charges, rather than a site a
                    // sweep has to find.
                    let rendered: Result<template::Retained, template::TemplateExecError> =
                        match tmpl {
                            Template::Simple(name) => {
                                let errors_visible = errs.dirty || errs.has_err();
                                let v = if *reads_err && errors_visible {
                                    errs.raw_slot(name)
                                } else {
                                    None
                                };
                                let v = v.or_else(|| get_label(labels, name)).unwrap_or("");
                                match template::Retained::copy(&render_budget, v) {
                                    Ok(r) => Ok(r),
                                    Err(template::BudgetExhausted) => {
                                        return Err(TemplateBudgetExceeded::new());
                                    }
                                }
                            }
                            Template::Parts(parts) => {
                                let errors_visible = errs.dirty || errs.has_err();
                                // SHARED reborrows so the lookup closure
                                // is `Clone` — `Retained::concat` walks
                                // the piece iterator twice (that is what
                                // removes the caller-supplied length),
                                // and a `&mut` capture is not cloneable.
                                let read_labels: &[(Cow<'a, str>, Cow<'a, str>)] = labels;
                                let read_errs: &ErrorSlots<'a> = &errs;
                                let lookup = move |name: &str| -> &str {
                                    if *reads_err
                                        && errors_visible
                                        && let Some(v) = read_errs.raw_slot(name)
                                    {
                                        return v;
                                    }
                                    get_label(read_labels, name).unwrap_or("")
                                };
                                match template::Retained::concat(
                                    &render_budget,
                                    part_pieces(parts, lookup),
                                ) {
                                    Ok(r) => Ok(r),
                                    Err(template::BudgetExhausted) => {
                                        return Err(TemplateBudgetExceeded::new());
                                    }
                                }
                            }
                            Template::Full(prog) => {
                                let errors_visible = errs.dirty || errs.has_err();
                                let (pair_err, pair_details) = if errors_visible {
                                    (
                                        errs.has_err().then(|| errs.err_str().to_string()),
                                        errs.has_details().then(|| errs.details_str().to_string()),
                                    )
                                } else {
                                    (None, None)
                                };
                                template::render_full(
                                    prog,
                                    labels,
                                    pair_err.as_deref(),
                                    pair_details.as_deref(),
                                    &line,
                                    ts_ns,
                                    &self.template_env,
                                    &render_budget,
                                )
                            }
                        };
                    match rendered {
                        // Byte model internally, U+FFFD at the boundary
                        // (owner-ratified, issue #230 adjudication 2);
                        // the valid-UTF-8 case is a move, not a copy.
                        Ok(rendered) => line = rendered.into_cow(),
                        Err(e) if e.budget_breach => {
                            // Render-budget breach: abort the QUERY
                            // (bounded 422) — never a per-line tag.
                            return Err(TemplateBudgetExceeded::new());
                        }
                        Err(e) => {
                            // Only the full engine has a per-line failure
                            // mode; the fast paths cannot fail except on
                            // the budget, which returned above.
                            errs.set_err(Cow::Borrowed(TEMPLATE_FORMAT_ERROR));
                            errs.set_details(Cow::Owned(e.msg));
                        }
                    }
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
                    // guard: while the map is empty it is rebuilt at every
                    // template, which is exactly live rendering (reachable by
                    // `| drop`-ing every label — live-probed). The map is
                    // empty iff the vector is empty AND neither error slot is
                    // VISIBLE (`errs.visible()` — a lone gated-off details
                    // slot does not make it non-empty, issue #238). The clone
                    // is taken at most once per line and only for the stages
                    // whose result can actually differ (`needs_snapshot`,
                    // decided at compile time), so ordinary `label_format`
                    // stages stay allocation-free here. The error-pair gate
                    // (`errors_visible`) is captured ONCE, at map build —
                    // `dirty` flips as elements `Set`/`Del`, and a
                    // per-template re-read would diverge (StageMap doc).
                    //
                    // `errs` itself is INVARIANT across this stage: no
                    // element can write the slots (`__error__` is a
                    // compile-time 400, `__error_details__` writes the
                    // vector, and the `{{.label}}`-only subset has no
                    // template-execution failure path — `fmt.go:426-429` is
                    // unreachable by type, U5). Only `dirty` changes.
                    let mut map: Option<StageMap<'a>> = None;
                    for f in fmts {
                        match f {
                            CompiledLabelFmt::Rename { dst, src } => {
                                // Loki `LabelsFormatter.Process` (fmt.go:414-419):
                                // the rename runs only when the source resolves
                                // present — from the LABEL SET only
                                // (`GetWithCategory`, labels.go:293-314): the
                                // out-of-band error slots are invisible here, so
                                // `label_format x=__error__` on a slot-errored
                                // line is a complete no-op while a parser-
                                // extracted vector `__error__` DOES rename
                                // (both live-probed, issue #238). An absent
                                // source is a complete no-op and dst is NOT
                                // created. A present (even empty) source still
                                // renames — matches Loki's parsed-empty case
                                // (`Set(dst,"")`+`Del(src)`). Empty *stream*
                                // labels are non-ingestable (both engines drop
                                // them at write), so this guard matches Loki for
                                // every reachable input (#226). A resolved
                                // rename runs `Set`+`Del` and therefore dirties
                                // the builder (`fmt.go:416-418`).
                                if let Some(value) = remove_label(labels, src) {
                                    set_label(labels, Cow::Borrowed(dst), value);
                                    errs.dirty = true;
                                }
                            }
                            CompiledLabelFmt::RenameSelf { name } => {
                                // `Set(dst, v)` THEN `Del(src)` with dst == src
                                // net-deletes the label (`fmt.go:417-418`,
                                // live-probed); resolved ⇒ dirty, unresolved ⇒
                                // complete no-op (issue #238).
                                if remove_label(labels, name).is_some() {
                                    errs.dirty = true;
                                }
                            }
                            CompiledLabelFmt::Template {
                                dst,
                                tmpl,
                                reads_err,
                            } => {
                                if map.is_none() {
                                    let (ve, vd) = errs.visible();
                                    let m_empty = labels.is_empty() && ve.is_none() && vd.is_none();
                                    if !m_empty {
                                        let gate = errs.dirty || errs.has_err();
                                        let snapshot = match needs_snapshot
                                            .then(|| {
                                                template::LabelSnapshot::take(
                                                    &render_budget,
                                                    labels,
                                                )
                                            })
                                            .transpose()
                                        {
                                            Ok(snapshot) => snapshot,
                                            Err(template::BudgetExhausted) => {
                                                return Err(TemplateBudgetExceeded::new());
                                            }
                                        };
                                        map = Some(StageMap {
                                            snapshot,
                                            // Freeze the pair VALUES at map
                                            // build (issue #230): a failing
                                            // Full template overwrites the
                                            // slots mid-stage, but the
                                            // reference's once-built map
                                            // keeps showing these.
                                            frozen_err: (gate && errs.has_err())
                                                .then(|| errs.err_str().to_string()),
                                            frozen_details: (gate && errs.has_details())
                                                .then(|| errs.details_str().to_string()),
                                        });
                                    }
                                }
                                let render_labels: &[(Cow<'a, str>, Cow<'a, str>)] = map
                                    .as_ref()
                                    .and_then(|m| m.snapshot.as_ref())
                                    .map_or(&*labels, template::LabelSnapshot::as_slice);
                                let pair_err = map.as_ref().and_then(|m| m.frozen_err.as_deref());
                                let pair_details =
                                    map.as_ref().and_then(|m| m.frozen_details.as_deref());
                                // As the `line_format` arm: ONE type,
                                // charged on construction, so a new
                                // render shape cannot reach a retained
                                // destination uncharged.
                                let rendered: Result<
                                    template::Retained,
                                    template::TemplateExecError,
                                > = match tmpl {
                                    Template::Simple(name) => {
                                        let slot = if *reads_err {
                                            frozen_slot(name, pair_err, pair_details)
                                        } else {
                                            None
                                        };
                                        let v = slot
                                            .or_else(|| get_label(render_labels, name))
                                            .unwrap_or("");
                                        match template::Retained::copy(&render_budget, v) {
                                            Ok(r) => Ok(r),
                                            Err(template::BudgetExhausted) => {
                                                return Err(TemplateBudgetExceeded::new());
                                            }
                                        }
                                    }
                                    Template::Parts(parts) => {
                                        let lookup = move |n: &str| -> &str {
                                            if *reads_err
                                                && let Some(v) =
                                                    frozen_slot(n, pair_err, pair_details)
                                            {
                                                return v;
                                            }
                                            get_label(render_labels, n).unwrap_or("")
                                        };
                                        match template::Retained::concat(
                                            &render_budget,
                                            part_pieces(parts, lookup),
                                        ) {
                                            Ok(r) => Ok(r),
                                            Err(template::BudgetExhausted) => {
                                                return Err(TemplateBudgetExceeded::new());
                                            }
                                        }
                                    }
                                    Template::Full(prog) => template::render_full(
                                        prog,
                                        render_labels,
                                        pair_err,
                                        pair_details,
                                        &line,
                                        ts_ns,
                                        &self.template_env,
                                        &render_budget,
                                    ),
                                };
                                match rendered {
                                    Ok(rendered) => {
                                        set_label(labels, Cow::Borrowed(dst), rendered.into_cow());
                                        // Every template assignment is a
                                        // `Set` (`fmt.go:431`) — dirty.
                                        errs.dirty = true;
                                    }
                                    Err(e) if e.budget_breach => {
                                        // Render-budget breach: abort the
                                        // QUERY (bounded 422 — issue #230
                                        // follow-up), never a per-line tag.
                                        return Err(TemplateBudgetExceeded::new());
                                    }
                                    Err(e) => {
                                        // `fmt.go:426-429`: destination NOT
                                        // set, the stage continues, the LAST
                                        // error wins (SetErr overwrites).
                                        errs.set_err(Cow::Borrowed(TEMPLATE_FORMAT_ERROR));
                                        errs.set_details(Cow::Owned(e.msg));
                                    }
                                }
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
                        return Ok((MetricRun::Dropped, errs.has_err()));
                    };
                    match convert_label_value(*kind, raw) {
                        Some(v) => {
                            // Oracle-probed: a successful unwrap DELETES the
                            // unwrapped label from the RESULT series — but
                            // only after any post-`unwrap` label filters ran
                            // over it (deferred; see `unwrapped` above).
                            unwrapped = Some(label);
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
                            // byte-exact (issue #104). The write is
                            // UNGUARDED, unlike the numeric-filter family —
                            // the reference has no `HasErr` check here
                            // (`metrics_extraction.go:222-223`). Compute the
                            // detail from `raw` before mutating the slots.
                            let detail = label_filter_error_details(*kind, raw);
                            errs.set_err(Cow::Borrowed(SAMPLE_EXTRACTION_ERROR));
                            errs.set_details(Cow::Owned(detail));
                            value = None;
                        }
                    }
                }
                CompiledStage::Unpack => {
                    // Reads the current line; returns the promoted `_entry`
                    // (owned) when present. The immutable borrow of `line`
                    // ends when `run_unpack` returns, so reassigning `line`
                    // afterward is sound.
                    if let Some(entry) = run_unpack(line.as_ref(), labels, &mut errs) {
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
                        // The error pair: reset the OUT-OF-BAND slot only —
                        // bare form unconditionally, matcher form iff the
                        // matcher matches the SLOT value — and `continue`
                        // without touching a vector entry of that name and
                        // without dirtying (`ResetError`/`ResetErrorDetails`
                        // are not `Del`s — `drop_labels.go:51-58`, `:68-81`;
                        // issue #238, live-probed: `| logfmt | json | drop
                        // __error__` keeps the parsed vector `__error__`).
                        if elem.label == ERROR_LABEL {
                            let reset = match &elem.matcher {
                                None => true,
                                Some(m) => m.matches(errs.err_str()),
                            };
                            if reset {
                                errs.reset_err();
                            }
                            continue;
                        }
                        if elem.label == ERROR_DETAILS_LABEL {
                            let reset = match &elem.matcher {
                                None => true,
                                Some(m) => m.matches(errs.details_str()),
                            };
                            if reset {
                                errs.reset_details();
                            }
                            continue;
                        }
                        match &elem.matcher {
                            // Bare label: drop unconditionally (a no-op when
                            // the label is absent — and only a PRESENT label
                            // dirties, `drop_labels.go:59-61`).
                            None => {
                                if remove_label(labels, &elem.label).is_some() {
                                    errs.dirty = true;
                                }
                            }
                            // Matcher present: the matcher evaluates against
                            // the value, or "" when the label is ABSENT, and
                            // a match `Del`s (⇒ dirties) even then
                            // (`drop_labels.go:80-82` Dels unconditionally on
                            // a match; live-probed: `drop nosuch=""` dirties
                            // the builder, `drop nosuch="zzz"` does not).
                            // Removing an absent label stays a vector no-op.
                            Some(m) => {
                                let matched =
                                    m.matches(get_label(labels, &elem.label).unwrap_or(""));
                                if matched {
                                    remove_label(labels, &elem.label);
                                    errs.dirty = true;
                                }
                            }
                        }
                    }
                }
                CompiledStage::Keep(elems) => {
                    // Retain only labels matched by some element (a bare
                    // element retains its label; a matcher-qualified element
                    // retains only on a value match) — plus the reference's
                    // special names, ALWAYS retained regardless of the list
                    // (`keep_labels.go:22`, `:51-57`; issue #238). The
                    // out-of-band slots are untouched; the builder dirties
                    // iff at least one label was actually removed.
                    let before = labels.len();
                    labels.retain(|(k, v)| {
                        k == ERROR_LABEL
                            || k == ERROR_DETAILS_LABEL
                            || k == PRESERVE_ERROR_LABEL
                            || elems.iter().any(|elem| {
                                elem.label == k.as_ref()
                                    && match &elem.matcher {
                                        None => true,
                                        Some(m) => m.matches(v),
                                    }
                            })
                    });
                    if labels.len() != before {
                        errs.dirty = true;
                    }
                }
            }
        }

        // The deferred successful-unwrap deletion (issue #221): the label
        // leaves the result series only now, AFTER every post-`unwrap`
        // filter processed it — the reference's ordering.
        if let Some(label) = unwrapped {
            remove_label(labels, label);
        }
        // The ONE merge point (`appendErrors` over the `visible()` gate):
        // kept lines only — a dropped line never merges (issue #238). The
        // slot state is captured BEFORE the merge consumes the slots.
        let has_err = errs.has_err();
        errs.merge_into(labels);
        Ok((MetricRun::Kept { line, value }, has_err))
    }
}

// ---------------------------------------------------------------------
// Compilation
// ---------------------------------------------------------------------

/// The single construction site for a regex-compilation failure. Reports
/// the pattern the CLIENT wrote, never `compile_anchored_regex`'s `^(?:…)$`
/// rewrite (issue #240). Deterministic rule, no text sniffing: if the
/// user's own pattern fails to compile, its error is the message;
/// otherwise the observed (anchored) error is used unchanged, so a
/// size-limit-class failure of the wrapped form is never misreported.
/// Issue #246 replaces this body and nowhere else.
///
/// NOTE: `label_replace` is not in PulsusDB's LogQL grammar today
/// (`plan.rs` rejects it at parse) — see issue #276, which adds it. The
/// reference genuinely DOES report the WRAPPED form at that one site, so
/// once #276 lands this seam must NOT be "consistency fixed" to wrap.
fn bad_regex(user_pattern: &str, observed: &regex::Error) -> PipelineError {
    let msg = match regex::Regex::new(user_pattern) {
        Err(e) => e.to_string(),
        Ok(_) => observed.to_string(),
    };
    PipelineError::BadRegex(msg)
}

/// Validation-only entry for [`super::escape::ch_regex_unanchored_checked`]
/// (issue #240): every path that turns a user regex into SQL compiles
/// exactly the form it will emit, first.
pub(super) fn validate_unanchored_regex(p: &str) -> Result<(), PipelineError> {
    compile_regex(p).map(|_| ())
}

/// Validation-only entry for [`super::escape::ch_regex_anchored_checked`].
pub(super) fn validate_anchored_regex(p: &str) -> Result<(), PipelineError> {
    compile_anchored_regex(p).map(|_| ())
}

fn compile_regex(pattern: &str) -> Result<regex::Regex, PipelineError> {
    regex::Regex::new(pattern).map_err(|e| bad_regex(pattern, &e))
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
    regex::Regex::new(&format!("^(?:{pattern})$")).map_err(|e| bad_regex(pattern, &e))
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

/// Issue #272: emits the flat post-order program directly.
///
/// The AST walk is iterative (`walk::postorder_into` over SCC-1), so a
/// flat `a or b or c …` chain — a LEFT-DEEP `LabelFilterExpr` — no
/// longer costs one machine frame per term. `ops` is reserved once at
/// the exact node count; the FIRST error still wins in source order,
/// because post-order visits the leaves left to right and `?` returns on
/// the first failing one.
fn compile_label_filter(expr: &LabelFilterExpr) -> Result<CompiledLabelFilter, PipelineError> {
    // A single-leaf filter — `| x="1"`, by far the common shape — needs
    // no traversal at all, so it costs exactly ONE allocation (the `ops`
    // vector), matching the pre-#272 profile of zero `Box`es plus this
    // one. Without it the walk's own node vector and work-stack chunk
    // would land on every per-variant compile.
    let mut nodes: Vec<&LabelFilterExpr> = Vec::new();
    let leaf_only = !matches!(expr, LabelFilterExpr::And(..) | LabelFilterExpr::Or(..));
    if leaf_only {
        nodes.push(expr);
    } else {
        walk::postorder_into::<pulsus_logql::LabelFilterScc>(expr, &mut nodes);
    }
    let mut ops: Vec<LfOp> = Vec::with_capacity(nodes.len());
    let mut has_compare = false;
    let mut live: u32 = 0;
    let mut max_stack: u32 = 0;
    for n in nodes {
        let op = match n {
            LabelFilterExpr::Match(m) => LfOp::Match {
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
                has_compare = true;
                LfOp::Compare {
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
                let matcher = IpMatcher::parse(value)
                    .map_err(|e| PipelineError::BadIpFilter(e.to_string()))?;
                // The IP label filter never mutates the label set (it
                // cannot error), so it does not force the label-mutating
                // fan-out path — `has_compare` stays as it was.
                LfOp::Ip {
                    name: name.clone(),
                    matcher,
                    negated: *negated,
                }
            }
            LabelFilterExpr::And(..) => LfOp::And,
            LabelFilterExpr::Or(..) => LfOp::Or,
        };
        live = match op {
            LfOp::And | LfOp::Or => live.saturating_sub(2).saturating_add(1),
            _ => live.saturating_add(1),
        };
        max_stack = max_stack.max(live);
        ops.push(op);
    }
    Ok(CompiledLabelFilter {
        ops,
        max_stack,
        has_compare,
    })
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

/// Compiles a template body with the full engine (issue #230); the
/// `{{.label}}`-only shapes come back as the byte-identical
/// `Simple`/`Parts` fast paths (the reference keeps the same `simpleKey`
/// shortcut, `fmt.go:218-228`).
fn compile_template(text: &str, kind: TemplateKind) -> Result<Template, PipelineError> {
    template::compile(text, kind).map_err(|e| {
        let prefix = match kind {
            TemplateKind::Line => "invalid line template: ".to_string(),
            // The label prefix is built at the call site (needs the
            // destination name); pass through unprefixed here.
            TemplateKind::Label => String::new(),
        };
        PipelineError::InvalidTemplate(format!("{prefix}{e}"))
    })
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
        // The message below is the reference's INNER text verbatim. The
        // envelope: `PipelineError::Display` still prepends its own
        // `bad parser expression: ` marker here, while the API-layer
        // `ReadError::PipelineInvalid` wrapper renders the reason BARE
        // (issue #240 removed its fixed prefix — the removed bytes are
        // recorded once, in docs/benchmarks/logs-differential-ledger.md,
        // and deliberately not quoted in `crates/` so AC1's zero-hit grep
        // cannot rot). The reference's own `parse error : stage '…' : …`
        // wording remains an accepted cosmetic divergence.
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
            // The self-rename (`x=x`) splits at COMPILE time: the reference
            // runs `Set(dst, v)` then `Del(src)` (`fmt.go:417-418`), so equal
            // names net-DELETE a resolved source (issue #238). The reserved-
            // destination and duplicate-destination rejections above run
            // BEFORE this split, so `label_format __error__=__error__` stays
            // a 400.
            LabelFmt::Rename { dst, src } if dst == src => {
                CompiledLabelFmt::RenameSelf { name: dst.clone() }
            }
            LabelFmt::Rename { dst, src } => CompiledLabelFmt::Rename {
                dst: dst.clone(),
                src: src.clone(),
            },
            LabelFmt::Template { dst, tmpl } => {
                let compiled = compile_template(tmpl, TemplateKind::Label).map_err(|e| {
                    // `fmt.go:381`: invalid template for label '<dst>': <err>
                    let PipelineError::InvalidTemplate(inner) = &e else {
                        return e;
                    };
                    PipelineError::InvalidTemplate(format!(
                        "invalid template for label '{dst}': {inner}"
                    ))
                })?;
                let reads_err = template_reads_error_pair(&compiled);
                CompiledLabelFmt::Template {
                    dst: dst.clone(),
                    tmpl: compiled,
                    reads_err,
                }
            }
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
            CompiledLabelFmt::Template { dst, tmpl, .. } => {
                // A Full template's read set is unknowable statically
                // (dot map, dynamic `index`) — treat it as reading
                // everything (issue #230).
                let reads_mutated = match tmpl {
                    Template::Simple(name) => mutated.contains(&name.as_str()),
                    Template::Parts(parts) => parts.iter().any(|part| match part {
                        TmplPart::Field(name) => mutated.contains(&name.as_str()),
                        TmplPart::Lit(_) => false,
                    }),
                    Template::Full(_) => !mutated.is_empty(),
                };
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
            // A self-rename mutates exactly one name (destination == source
            // — a resolved one deletes it), so one push, not two (#238).
            CompiledLabelFmt::RenameSelf { name } => {
                if seen_template {
                    mutated.push(name.as_str());
                }
            }
        }
    }
    false
}

/// True iff some field in the template names `__error__`/`__error_details__`
/// — decided once per query at compile time (issue #238), so every template
/// that does not name the pair keeps the slot-blind zero-copy
/// [`render_template`] path.
fn template_reads_error_pair(tmpl: &Template) -> bool {
    match tmpl {
        Template::Simple(name) => name == ERROR_LABEL || name == ERROR_DETAILS_LABEL,
        Template::Parts(parts) => parts.iter().any(|part| match part {
            TmplPart::Field(name) => name == ERROR_LABEL || name == ERROR_DETAILS_LABEL,
            TmplPart::Lit(_) => false,
        }),
        // A full template can reach the pair dynamically (`index .
        // "__error__"`, `range .`): conservatively gate it in. The gate
        // costs nothing when no error is set.
        Template::Full(_) => true,
    }
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
/// Every call is a `Set` on the reference's builder, so it marks the
/// builder `dirty` (`labels.go:216-222`; issue #238) — a parser that
/// writes nothing (e.g. a failed `json` parse) never reaches here and
/// therefore does not dirty.
fn add_extracted<'a>(
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    key: Cow<'a, str>,
    value: Cow<'a, str>,
    dirty: &mut bool,
) {
    let sanitized: Cow<'a, str> = if key_needs_sanitizing(&key) {
        Cow::Owned(sanitize_label_key(&key))
    } else {
        key
    };
    *dirty = true;
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

/// A `Parts` render as the PIECE SEQUENCE it is — text literals verbatim,
/// field actions resolved through `lookup`.
///
/// Issue #260 review round 3: the former `presize_parts`/`fill_parts`
/// pair handed `Retained::assemble` a length AND a writer, which made the
/// charge something the constructor had to TRUST. Handing it the pieces
/// instead lets it size and write from the same source, so there is no
/// number for a caller to get wrong. The iterator is `Clone` (the
/// constructor walks it twice, exactly as the split pair did) and cheap —
/// a `Map` over a slice, no allocation, so the single-allocation render
/// the alloc gates pin is unchanged.
fn part_pieces<'p, 'l, F>(parts: &'p [TmplPart], lookup: F) -> impl Iterator<Item = &'l str> + Clone
where
    F: Fn(&str) -> &'l str + Clone + 'p,
    'p: 'l,
{
    parts.iter().map(move |part| match part {
        TmplPart::Lit(s) => s.as_str(),
        // A missing field renders empty (pinned semantics, plan v3
        // delta 8 / AC2).
        TmplPart::Field(name) => lookup(name),
    })
}

/// `convertBytes` for the `bytes` template function (issue #230). The
/// `duration`/`duration_seconds` template functions carry their own full
/// `time.ParseDuration` port instead (the label-filter conversion path
/// is pinned to the filter-reachable subset and rejects e.g. negative
/// durations the template function must accept).
pub(crate) fn convert_bytes_value(raw: &str) -> Result<f64, String> {
    convert_label_value(UnitKind::Bytes, raw)
        .ok_or_else(|| label_filter_error_details(UnitKind::Bytes, raw))
}

/// The FROZEN error-pair lookup for `label_format` templates (issue
/// #230; see [`StageMap`]): a non-empty frozen slot overrides the
/// same-named vector entry.
fn frozen_slot<'m>(name: &str, err: Option<&'m str>, details: Option<&'m str>) -> Option<&'m str> {
    match name {
        ERROR_LABEL => err,
        ERROR_DETAILS_LABEL => details,
        _ => None,
    }
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
/// Issue #272: a LINEAR SCAN over the flat post-order program.
///
/// **Contract 3 is preserved exactly**: post-order visits the leaves in
/// SOURCE order and evaluates BOTH operands of every `And`/`Or`
/// unconditionally, so `failed` still records the LEFTMOST conversion
/// failure and the Kleene tables below are transcribed unchanged. No
/// Sethi-Ullman reordering, no associativity normalisation, no rank
/// side-table.
///
/// **Per-row cost.** Strictly less than the tree walk it replaces: a
/// contiguous `Vec<LfOp>` instead of `Box`-pointer chasing plus a call
/// frame per node, and an on-stack `[Option<bool>; LF_INLINE_STACK]`
/// verdict array instead of any allocation. A left-deep chain — what the
/// parser builds for `a or b or c …` — has `max_stack == 2` regardless
/// of width, so width never spills to the heap.
fn eval_label_filter<'v>(
    filter: &CompiledLabelFilter,
    labels: &'v [(Cow<'_, str>, Cow<'_, str>)],
    errs: &ErrorSlots<'_>,
    failed: &mut Option<(UnitKind, &'v str)>,
) -> Option<bool> {
    let mut inline = [None; LF_INLINE_STACK];
    // Unreachable for any parser-produced filter (see
    // `LF_INLINE_STACK`); retained for programmatically constructed
    // trees, which the corpus runner and `extended_with` can both carry.
    // `max_stack <= ops.len() <= N`, itself bounded by admission.
    let mut spill: Vec<Option<bool>> = if filter.max_stack as usize > LF_INLINE_STACK {
        vec![None; filter.max_stack as usize]
    } else {
        Vec::new()
    };
    let vals: &mut [Option<bool>] = if spill.is_empty() {
        &mut inline
    } else {
        &mut spill
    };
    let mut top = 0usize;
    for op in &filter.ops {
        let v = match op {
            LfOp::Match {
                name,
                op,
                value,
                re,
            } => {
                // `labelValue` (`label_filter.go:418-424`): `__error__` — and
                // ONLY `__error__` — resolves the out-of-band slot; every other
                // name, INCLUDING `__error_details__`, falls through to the
                // vector (issue #238, live-probed: `| json | __error_details__
                // != ""` matches nothing on an errored line). Prometheus
                // matcher semantics otherwise: a missing label matches as the
                // empty string — which an unset slot also reads as.
                let v = if name == ERROR_LABEL {
                    errs.err_str()
                } else {
                    get_label(labels, name).unwrap_or("")
                };
                Some(match op {
                    MatchOp::Eq => v == value,
                    MatchOp::Neq => v != value,
                    MatchOp::Re => re.as_ref().is_some_and(|re| re.is_match(v)),
                    MatchOp::Nre => !re.as_ref().is_some_and(|re| re.is_match(v)),
                })
            }
            LfOp::Compare {
                name,
                op,
                kind,
                threshold,
            } => {
                // A missing label never satisfies a numeric comparison
                // (dropped, no error); an unconvertible value is the error
                // class.
                // Issue #272: the leaf arms used to be a recursive call, so
                // an early `return` exited only that leaf. In the linear scan
                // it would exit the whole program, so each is now the arm's
                // VALUE.
                let Some(raw) = get_label(labels, name) else {
                    vals[top] = Some(false);
                    top += 1;
                    continue;
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
                    vals[top] = None;
                    top += 1;
                    continue;
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
            LfOp::Ip {
                name,
                matcher,
                negated,
            } => {
                // `ip.go:123-127`: an errored line passes the ip filter
                // UNCONDITIONALLY — for `=` AND `!=` alike — and the
                // short-circuit reads the SLOT, not the vector (issue #238,
                // live-probed: `| json | addr = ip(...)` and `!= ip(...)` both
                // keep the errored line; after `drop __error__` both drop it,
                // and a stream label `__error__` does NOT trip it).
                if errs.has_err() {
                    vals[top] = Some(true);
                    top += 1;
                    continue;
                }
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
            LfOp::And => {
                // Post-order leaves [.., lhs, rhs] on the tail; BOTH were
                // evaluated, in source order, before this op runs.
                let rhs = vals[top - 1];
                let lhs = vals[top - 2];
                top -= 2;
                match (lhs, rhs) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                }
            }
            LfOp::Or => {
                let rhs = vals[top - 1];
                let lhs = vals[top - 2];
                top -= 2;
                match (lhs, rhs) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                }
            }
        };
        vals[top] = v;
        top += 1;
    }
    match top {
        1 => vals[0],
        // Unreachable: a post-order program consumes exactly two
        // verdicts per boolean op and pushes one, so exactly one
        // survives an `ops` list built by `compile_label_filter`.
        _ => unreachable!("a post-order label-filter program leaves exactly one verdict"),
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
    errs: &mut ErrorSlots<'a>,
) {
    let parsed: serde_json::Value = match serde_json::from_str(line) {
        Ok(v @ serde_json::Value::Object(_)) => v,
        // A non-object top level (or a parse failure) is the malformed
        // class: line kept, error tagged in the out-of-band slots
        // (UNGUARDED — the reference's parser error write has no `HasErr`
        // check, `parser.go:437-444`), detail recorded on both paths. No
        // label is written, so a failed parse does NOT dirty the builder
        // (issue #238 — the details-visibility gate depends on this).
        _ => {
            errs.set_err(Cow::Borrowed("JSONParserErr"));
            errs.set_details(Cow::Borrowed(JSON_ERROR_DETAILS));
            return;
        }
    };
    if extractions.is_empty() {
        let mut extracted = Vec::new();
        flatten_json("", &parsed, &mut extracted);
        for (k, v) in extracted {
            add_extracted(labels, Cow::Owned(k), Cow::Owned(v), &mut errs.dirty);
        }
    } else {
        for (label, path) in extractions {
            let value = lookup_json_path(&parsed, path)
                .map(json_scalar_to_string)
                .unwrap_or_default();
            add_extracted(
                labels,
                Cow::Borrowed(label.as_str()),
                Cow::Owned(value),
                &mut errs.dirty,
            );
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
fn run_unpack<'a>(
    line: &str,
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    errs: &mut ErrorSlots<'a>,
) -> Option<String> {
    let map = match serde_json::from_str::<serde_json::Value>(line) {
        Ok(serde_json::Value::Object(map)) => map,
        // Slot write, no label, no dirty — see `run_json` (issue #238).
        _ => {
            errs.set_err(Cow::Borrowed("JSONParserErr"));
            errs.set_details(Cow::Borrowed(JSON_ERROR_DETAILS));
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
                add_extracted(labels, Cow::Owned(k), Cow::Owned(s), &mut errs.dirty);
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
    errs: &mut ErrorSlots<'a>,
    to_cow: impl Fn(Cow<'t, str>) -> Cow<'a, str>,
) {
    let result = if extractions.is_empty() {
        walk_logfmt(text, &mut |k, v| {
            if !keep_empty && v.is_empty() {
                return;
            }
            add_extracted(labels, to_cow(Cow::Borrowed(k)), to_cow(v), &mut errs.dirty);
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
            add_extracted(
                labels,
                Cow::Borrowed(label.as_str()),
                value,
                &mut errs.dirty,
            );
        }
        err
    };
    if let Err(err) = result
        && strict
    {
        // Slot writes (UNGUARDED — the reference's parser error write has no
        // `HasErr` check); the pairs already emitted above dirtied per label
        // (issue #238).
        errs.set_err(Cow::Borrowed("LogfmtParserErr"));
        errs.set_details(Cow::Owned(logfmt_error_details(err)));
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

    /// Issue #240 AC6: the `bad_regex` seam reports the USER's pattern —
    /// a label-filter regex failure (`| a=~"("`) carries the compile
    /// error for `(` and never for `compile_anchored_regex`'s `^(?:()$`
    /// rewrite, asserted on the WHOLE message. Reverting the seam (back
    /// to `e.to_string()` of the anchored error) reddens this.
    #[test]
    #[allow(clippy::invalid_regex)] // deliberate: derives the expected error for an invalid pattern
    fn bad_regex_reports_the_users_pattern_not_the_anchored_rewrite() {
        let user_err = regex::Regex::new("(").expect_err("must not compile");
        let expected = format!("bad regex: {user_err}");
        let err = CompiledPipeline::compile(&stages_of(r#"{service_name="x"} | a=~"(""#))
            .expect_err("must reject");
        assert_eq!(err.to_string(), expected, "the whole message, byte-exact");
        assert!(!err.to_string().contains("^(?:"), "{err}");
    }

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
                compiled
                    .run(&above, &base, 0)
                    .expect("no budget breach")
                    .is_some(),
                "{query}: {above:?} should survive"
            );
            assert!(
                compiled
                    .run(&below, &base, 0)
                    .expect("no budget breach")
                    .is_none(),
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
        let _ = compiled.run_metric_into("d=250ms latency=2s", &base, 0, &mut labels);
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
    fn the_field_substitution_subset_compiles_and_renders() {
        let compiled = compile_template("{{.method}} -> {{.path}}!", TemplateKind::Line).unwrap();
        let Template::Parts(parts) = compiled else {
            panic!("expected the field-substitution subset to derive Parts");
        };
        let labels = vec![
            (Cow::Borrowed("method"), Cow::Borrowed("GET")),
            (Cow::Borrowed("path"), Cow::Borrowed("/x")),
        ];
        let budget = template::RenderBudget::default();
        let lookup = |name: &str| get_label(&labels, name).unwrap_or("");
        let rendered = template::Retained::concat(&budget, part_pieces(&parts, lookup))
            .expect("well inside the budget");
        assert_eq!(rendered.as_str(), "GET -> /x!");
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
        // Issue #272: E0509 — re-bind through a reference.
        match &expr {
            pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Range { range, .. }) => {
                range.selector.pipeline.clone()
            }
            pulsus_logql::Expr::Log(log) => log.pipeline.clone(),
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
            let a = without
                .run(body, &base, 0)
                .expect("no budget breach")
                .expect("kept");
            let b = with
                .run(body, &base, 0)
                .expect("no budget breach")
                .expect("kept");
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
        let MetricRun::Kept { value, .. } = compiled
            .run_metric_into("took=250ms level=info", &base, 0, &mut labels)
            .expect("no budget breach")
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
        let MetricRun::Kept { value, .. } = compiled
            .run_metric_into("took=abc level=warn", &base, 0, &mut labels)
            .expect("no budget breach")
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
            compiled
                .run_metric_into("level=error", &base, 0, &mut labels)
                .expect("no budget breach"),
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
            compiled
                .run_metric_into("took=abc", &base, 0, &mut labels)
                .expect("no budget breach"),
            MetricRun::Dropped
        ));
        assert!(matches!(
            compiled
                .run_metric_into("took=100ms", &base, 0, &mut labels)
                .expect("no budget breach"),
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
            let MetricRun::Kept { value, .. } = compiled
                .run_metric_into(body, &base, 0, &mut labels)
                .expect("no budget breach")
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

    /// Issue #272 finding 3: the zero-allocation-per-row claim must hold
    /// for every shape the parser ADMITS, not just the common one.
    ///
    /// Full right nesting is the worst case for the verdict stack. This
    /// finds the deepest filter the parser accepts and asserts its
    /// `max_stack` fits inline — so raising `LABEL_FILTER_MAX_DEPTH`
    /// without raising `LF_INLINE_STACK` reddens this rather than
    /// silently reintroducing a per-row heap allocation.
    #[test]
    fn max_stack_never_spills_for_any_parser_admissible_filter() {
        let mut deepest: Option<(usize, u32)> = None;
        for depth in 1..400usize {
            // `x="0" and (x="1" and (x="2" and …))` — full right
            // nesting, the worst case for the verdict stack.
            let mut q = String::from(r#"{a="b"} | "#);
            for i in 0..depth {
                q.push_str(&format!("x=\"{i}\""));
                if i + 1 < depth {
                    q.push_str(" and (");
                }
            }
            for _ in 0..depth.saturating_sub(1) {
                q.push(')');
            }
            let Ok(expr) = pulsus_logql::parse(&q) else {
                break; // the parser's own depth guard rejected it
            };
            let stages = match &expr {
                pulsus_logql::Expr::Log(l) => l.pipeline.clone(),
                other => panic!("unexpected fixture shape: {other:?}"),
            };
            let compiled = CompiledPipeline::compile(&stages).expect("compile");
            let ms = compiled
                .stages
                .iter()
                .filter_map(|s| match s {
                    CompiledStage::LabelFilter(f) => Some(f.max_stack),
                    _ => None,
                })
                .max()
                .expect("a label-filter stage");
            deepest = Some((depth, ms));
        }
        let (depth, max_stack) = deepest.expect("at least one filter parsed");
        assert!(
            depth > 64,
            "the probe stopped at depth {depth}, below the old inline width — it would not \
             have seen a spill"
        );
        assert!(
            (max_stack as usize) <= LF_INLINE_STACK,
            "the deepest parser-admissible filter (depth {depth}) needs a verdict stack of \
             {max_stack}, above LF_INLINE_STACK = {LF_INLINE_STACK} — per-row evaluation \
             would allocate"
        );
    }

    #[test]
    fn masked_and_drops_the_line_and_emits_no_error_label_streams() {
        let compiled = CompiledPipeline::compile(&stages_of(
            r#"{a="b"} | logfmt | level = "warn" and took > 250ms"#,
        ))
        .unwrap();
        let base = vec![("a".to_string(), "b".to_string())];
        // `level = "warn"` is definite-false; `and` absorbs the sibling's
        // conversion failure on `took=bad` without ever setting a label.
        assert!(
            compiled
                .run("level=info took=bad", &base, 0)
                .expect("no budget breach")
                .is_none()
        );
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
            .run("level=info took=bad", &base, 0)
            .expect("no budget breach")
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
        let MetricRun::Kept { .. } = compiled
            .run_metric_into("level=info took=bad", &base, 0, &mut labels)
            .expect("no budget breach")
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
                .run("level=info took=bad", &base, 0)
                .expect("no budget breach")
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
            .run(line, &base, 0)
            .expect("no budget breach")
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

    // -----------------------------------------------------------------
    // Issue #238: the out-of-band error pair — pipeline-path rows of the
    // Delta C''.3 matrix (structured-metadata shapes enter through a
    // pre-routed `StructuredMetadataCtx`; the raw-SM → ctx routing itself
    // is pinned in `exec.rs`'s tests, which assert these exact
    // merged-base/ctx pairs). Every expected set is a literal reference
    // capture (grafana/loki:3.7.4, discover_log_levels: false). Each test
    // names its row id and the wrong rule(s) it kills; `DET` is the fixed
    // JSONParserErr detail.
    // -----------------------------------------------------------------

    fn sm(err: &str, details: &str, has_ordinary: bool) -> StructuredMetadataCtx {
        StructuredMetadataCtx {
            err: err.to_string(),
            details: details.to_string(),
            has_ordinary,
        }
    }

    /// Runs `query` over `body` with the given (post-routing) merged base
    /// and SM ctx; returns the sorted emitted label set, `None` = dropped.
    fn run_sm_labels(
        query: &str,
        body: &str,
        base: &[(&str, &str)],
        sm: &StructuredMetadataCtx,
    ) -> Option<Vec<(String, String)>> {
        let compiled = CompiledPipeline::compile(&stages_of(query)).expect(query);
        let base: Vec<(String, String)> = base
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let mut labels = Vec::new();
        compiled
            .run_into_with_sm(body, &base, 0, sm, &mut labels)
            .expect("no budget breach")?;
        let mut got: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        got.sort();
        Some(got)
    }

    const DET: &str = JSON_ERROR_DETAILS;

    /// C1 (kills W5): no SM, `| json | drop __error__` — clean builder, the
    /// lone details slot is invisible.
    #[test]
    fn c1_bare_drop_on_a_clean_builder_hides_the_details() {
        let got = run_sm_labels(
            r#"{x="y"} | json | drop __error__"#,
            "a=Hello b=World",
            &[("service_name", "v0")],
            &EMPTY_STRUCTURED_METADATA,
        );
        assert_eq!(got, Some(owned(&[("service_name", "v0")])));
    }

    /// C2 (kills W12; the dirty-seed site's killer): ordinary SM dirties the
    /// builder, so the orphaned details DO surface — even with no error set
    /// (`HasErr() || HasErrorDetails()` is an OR, `labels.go:519`).
    #[test]
    fn c2_ordinary_sm_dirt_surfaces_the_orphaned_details() {
        let got = run_sm_labels(
            r#"{x="y"} | json | drop __error__"#,
            "a=Hello b=World",
            &[("service_name", "v1"), ("trace_id", "abc")],
            &sm("", "", true),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_DETAILS_LABEL, DET),
                ("service_name", "v1"),
                ("trace_id", "abc"),
            ]))
        );
    }

    /// C3 (kills W11, W13, and W15 on the pipeline path): a reserved-err SM
    /// entry emits on a CLEAN builder (`HasErr` opens the gate by itself),
    /// and no empty `__error_details__` is fabricated.
    #[test]
    fn c3_reserved_err_sm_emits_on_a_clean_builder() {
        let got = run_sm_labels(
            r#"{x="y"}"#,
            "a=Hello b=World",
            &[("service_name", "v2")],
            &sm("boom", "", false),
        );
        assert_eq!(
            got,
            Some(owned(&[(ERROR_LABEL, "boom"), ("service_name", "v2")]))
        );
    }

    /// C4 (kills W1, W5, W8): the reserved SM entry lives in the SLOT, so
    /// `drop __error__` resets it (json's error overwrote "boom" first) and
    /// — the builder being clean — nothing survives.
    #[test]
    fn c4_reserved_err_sm_is_droppable_and_leaves_a_clean_builder() {
        let got = run_sm_labels(
            r#"{x="y"} | json | drop __error__"#,
            "a=Hello b=World",
            &[("service_name", "v2")],
            &sm("boom", "", false),
        );
        assert_eq!(got, Some(owned(&[("service_name", "v2")])));
    }

    /// C5 (kills W1, W5, W8): a lone reserved-details SM entry is invisible
    /// on the bare (pipeline-path) selector.
    #[test]
    fn c5_reserved_details_sm_alone_is_invisible() {
        let got = run_sm_labels(
            r#"{x="y"}"#,
            "a=Hello b=World",
            &[("service_name", "v3")],
            &sm("", "bdet", false),
        );
        assert_eq!(got, Some(owned(&[("service_name", "v3")])));
    }

    /// C6 (kills W1, W8, W12): mixed reserved-err + ordinary SM — after the
    /// drop the details surface (dirty) but no `__error__` survives.
    #[test]
    fn c6_mixed_sm_drop_err_keeps_details_only() {
        let got = run_sm_labels(
            r#"{x="y"} | json | drop __error__"#,
            "a=Hello b=World",
            &[("service_name", "v9"), ("trace_id", "abc")],
            &sm("boom", "", true),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_DETAILS_LABEL, DET),
                ("service_name", "v9"),
                ("trace_id", "abc"),
            ]))
        );
    }

    /// C8 (the ONLY killer of W6a; also kills W3a, W12): a STREAM label
    /// `__error__` is a vector entry — `drop __error__` resets the slot and
    /// must NOT remove it.
    #[test]
    fn c8_drop_error_never_removes_a_vector_entry_of_that_name() {
        let got = run_sm_labels(
            r#"{x="y"} | json | drop __error__"#,
            "a=Hello b=World",
            &[
                ("__error__", "streamerr"),
                ("service_name", "v10"),
                ("__error___extracted", "boom"),
            ],
            &sm("", "", true),
        );
        assert_eq!(
            got,
            Some(owned(&[
                ("__error__", "streamerr"),
                ("__error___extracted", "boom"),
                (ERROR_DETAILS_LABEL, DET),
                ("service_name", "v10"),
            ]))
        );
    }

    /// C10 (kills W1, W2): an empty reserved-err SM value contributes no
    /// label at all on the bare (pipeline-path) selector.
    #[test]
    fn c10_empty_reserved_err_sm_emits_nothing() {
        let got = run_sm_labels(
            r#"{x="y"}"#,
            "a=Hello b=World",
            &[("service_name", "v4")],
            &sm("", "", false),
        );
        assert_eq!(got, Some(owned(&[("service_name", "v4")])));
    }

    /// C9 (kills W10a): the string `__error__` filter reads the SLOT — empty
    /// here — never the stream-label vector entry.
    #[test]
    fn c9_error_match_filter_reads_the_slot_not_the_vector() {
        let base = &[
            ("__error__", "streamerr"),
            ("service_name", "v10"),
            ("__error___extracted", "boom"),
        ];
        let kept = run_sm_labels(
            r#"{x="y"} | __error__ = """#,
            "a=Hello b=World",
            base,
            &sm("", "", true),
        );
        assert!(kept.is_some(), "slot is empty -> `= \"\"` keeps the line");
        let dropped = run_sm_labels(
            r#"{x="y"} | __error__ = "streamerr""#,
            "a=Hello b=World",
            base,
            &sm("", "", true),
        );
        assert!(dropped.is_none(), "the stream label must not satisfy it");
    }

    /// C11 (kills W2): an EMPTY reserved-err SM value is an UNSET slot — no
    /// `ip()` short-circuit, and the absent `addr` drops the line.
    #[test]
    fn c11_empty_reserved_err_sm_does_not_trip_the_ip_short_circuit() {
        let got = run_sm_labels(
            r#"{x="y"} | addr = ip("1.2.3.4")"#,
            "a=Hello b=World",
            &[("service_name", "v4")],
            &sm("", "", false),
        );
        assert_eq!(got, None);
    }

    /// C12 (kills W2): an empty err slot does NOT block the numeric-filter
    /// error write (`!HasErr()` is `err != ""`).
    #[test]
    fn c12_empty_err_slot_does_not_block_the_label_filter_error() {
        let got = run_sm_labels(
            r#"{x="y"} | logfmt | a > 5"#,
            "a=Hello b=World",
            &[("service_name", "v4")],
            &sm("", "", false),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_LABEL, "LabelFilterErr"),
                (
                    ERROR_DETAILS_LABEL,
                    "strconv.ParseFloat: parsing \"Hello\": invalid syntax"
                ),
                ("a", "Hello"),
                ("b", "World"),
                ("service_name", "v4"),
            ]))
        );
    }

    /// C12's contrast partner (first error wins): a NON-empty err slot
    /// blocks the write.
    #[test]
    fn c12b_reserved_err_sm_blocks_the_label_filter_error() {
        let got = run_sm_labels(
            r#"{x="y"} | logfmt | a > 5"#,
            "a=Hello b=World",
            &[("service_name", "v2")],
            &sm("boom", "", false),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_LABEL, "boom"),
                ("a", "Hello"),
                ("b", "World"),
                ("service_name", "v2"),
            ]))
        );
    }

    /// C13 (kills W1, W2, W5): empty err + non-empty details, clean builder
    /// — nothing emits.
    #[test]
    fn c13_empty_err_plus_details_on_a_clean_builder_emits_nothing() {
        let got = run_sm_labels(
            r#"{x="y"}"#,
            "a=Hello b=World",
            &[("service_name", "v6")],
            &sm("", "bdet", false),
        );
        assert_eq!(got, Some(owned(&[("service_name", "v6")])));
    }

    /// C14 (kills W1, W2): both reserved values empty — nothing emits.
    #[test]
    fn c14_both_reserved_sm_values_empty_emit_nothing() {
        let got = run_sm_labels(
            r#"{x="y"}"#,
            "a=Hello b=World",
            &[("service_name", "v12")],
            &sm("", "", false),
        );
        assert_eq!(got, Some(owned(&[("service_name", "v12")])));
    }

    /// C15 (kills W2): an EMPTY details slot does not mask a vector write.
    #[test]
    fn c15_empty_details_slot_does_not_mask_an_assignment() {
        let got = run_sm_labels(
            r#"{x="y"} | label_format __error_details__="boom""#,
            "a=Hello b=World",
            &[("service_name", "v5")],
            &sm("", "", false),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_DETAILS_LABEL, "boom"),
                ("service_name", "v5"),
            ]))
        );
    }

    /// C16 (kills W12, W13; C15's contrast partner): a NON-empty details
    /// slot masks the same assignment once the Set dirties the builder —
    /// and no empty `__error__` is fabricated next to it.
    #[test]
    fn c16_nonempty_details_slot_masks_the_assignment() {
        let got = run_sm_labels(
            r#"{x="y"} | label_format __error_details__="boom""#,
            "a=Hello b=World",
            &[("service_name", "v6")],
            &sm("", "bdet", false),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_DETAILS_LABEL, "bdet"),
                ("service_name", "v6"),
            ]))
        );
    }

    /// C19 (kills a gate-always-closed reading): details + ordinary SM
    /// emit on the bare selector (dirty opens the gate).
    #[test]
    fn c19_details_with_ordinary_sm_emit_on_the_bare_selector() {
        let got = run_sm_labels(
            r#"{x="y"}"#,
            "a=Hello b=World",
            &[("service_name", "v11"), ("trace_id", "abc")],
            &sm("", "bdet", true),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_DETAILS_LABEL, "bdet"),
                ("service_name", "v11"),
                ("trace_id", "abc"),
            ]))
        );
    }

    /// C20 (kills W2): the template gate was CLOSED at map build (clean
    /// builder), so `d` renders empty — while the Set's dirt re-surfaces the
    /// details at emit.
    #[test]
    fn c20_template_gate_closed_at_map_build_renders_empty() {
        let got = run_sm_labels(
            r#"{x="y"} | label_format d="[{{.__error_details__}}]""#,
            "a=Hello b=World",
            &[("service_name", "v6")],
            &sm("", "bdet", false),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_DETAILS_LABEL, "bdet"),
                ("d", "[]"),
                ("service_name", "v6"),
            ]))
        );
    }

    /// C21 (the ONLY killer of W4, also kills W2): the gate is captured
    /// ONCE per map build — the second template must NOT see the details
    /// even though the first template's Set dirtied the builder.
    #[test]
    fn c21_the_map_gate_is_captured_once_not_per_template() {
        let got = run_sm_labels(
            r#"{x="y"} | label_format d="[{{.__error_details__}}]", e="[{{.__error_details__}}]""#,
            "a=Hello b=World",
            &[("service_name", "v6")],
            &sm("", "bdet", false),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_DETAILS_LABEL, "bdet"),
                ("d", "[]"),
                ("e", "[]"),
                ("service_name", "v6"),
            ]))
        );
    }

    /// C22 (kills W7 on the pipeline path): an EMPTY-valued ordinary SM
    /// entry does not dirty the builder (`has_ordinary` counts non-empty
    /// only), so the details stay invisible. The stray `trace_id=""` in the
    /// base is the pre-existing ingest divergence #259 — its expectation
    /// changes to the reference's `{sn}` when #259 lands; the
    /// `__error_details__` half is reference-exact today.
    #[test]
    fn c22_empty_ordinary_sm_does_not_open_the_details_gate() {
        let got = run_sm_labels(
            r#"{x="y"} | json | drop __error__"#,
            "a=Hello b=World",
            &[("service_name", "w2"), ("trace_id", "")],
            &sm("", "bdet", false),
        );
        assert_eq!(
            got,
            Some(owned(&[("service_name", "w2"), ("trace_id", "")]))
        );
    }

    /// C23 — DECLARED REGRESSION PIN (evidence for no rule): every listed
    /// reading yields the same outcome, because the slot value is "" and a
    /// missing vector entry also reads "".
    #[test]
    fn c23_pin_empty_err_slot_matches_like_an_absent_label() {
        let base = &[("service_name", "v4")];
        assert!(
            run_sm_labels(
                r#"{x="y"} | __error__ = """#,
                "a=Hello b=World",
                base,
                &sm("", "", false),
            )
            .is_some()
        );
        assert!(
            run_sm_labels(
                r#"{x="y"} | __error__ != """#,
                "a=Hello b=World",
                base,
                &sm("", "", false),
            )
            .is_none()
        );
    }

    /// C24 (regression pin for W12's neighborhood): empty details + ordinary
    /// SM — the pipeline's own details emit under dirt.
    #[test]
    fn c24_empty_details_sm_with_ordinary_dirt_still_orphans_json_details() {
        let got = run_sm_labels(
            r#"{x="y"} | json | drop __error__"#,
            "a=Hello b=World",
            &[("service_name", "v8"), ("trace_id", "abc")],
            &sm("", "", true),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_DETAILS_LABEL, DET),
                ("service_name", "v8"),
                ("trace_id", "abc"),
            ]))
        );
    }

    /// C26 (kills W3b on the pipeline path): base `__error_details__` + the
    /// suffixed SM twin are BOTH ordinary vector entries; the empty details
    /// slot must not overwrite the stream value once `label_format` dirties.
    #[test]
    fn c26_suffixed_details_sm_stays_ordinary_through_a_pipeline() {
        let got = run_sm_labels(
            r#"{x="y"} | label_format x="1""#,
            "a=Hello b=World",
            &[
                ("__error_details__", "streamdet"),
                ("service_name", "v13"),
                ("__error_details___extracted", "smdet"),
            ],
            &sm("", "", true),
        );
        assert_eq!(
            got,
            Some(owned(&[
                ("__error_details__", "streamdet"),
                ("__error_details___extracted", "smdet"),
                ("service_name", "v13"),
                ("x", "1"),
            ]))
        );
    }

    /// C27 (the ONLY killer of W6b): `drop __error_details__` resets the
    /// slot and must NOT remove the stream-label vector entry.
    #[test]
    fn c27_drop_details_never_removes_a_vector_entry_of_that_name() {
        let got = run_sm_labels(
            r#"{x="y"} | json | drop __error_details__"#,
            "a=Hello b=World",
            &[
                ("__error_details__", "streamdet"),
                ("service_name", "v13"),
                ("__error_details___extracted", "smdet"),
            ],
            &sm("", "", true),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_LABEL, "JSONParserErr"),
                ("__error_details__", "streamdet"),
                ("__error_details___extracted", "smdet"),
                ("service_name", "v13"),
            ]))
        );
    }

    /// C28 (the ONLY killer of W10c): the `drop __error__=""` matcher reads
    /// the SLOT ("boom") — an absent vector entry reading "" must not match.
    #[test]
    fn c28_drop_error_matcher_reads_the_slot() {
        let got = run_sm_labels(
            r#"{x="y"} | drop __error__="""#,
            "a=Hello b=World",
            &[("service_name", "v2")],
            &sm("boom", "", false),
        );
        assert_eq!(
            got,
            Some(owned(&[(ERROR_LABEL, "boom"), ("service_name", "v2")]))
        );
    }

    /// C30 (the ONLY killer of W9): with the vector emptied, the map is
    /// non-empty ONLY because of the visible err slot — a slots-blind
    /// emptiness test would discard the capture and rebuild live, letting
    /// `e` see `d`.
    #[test]
    fn c30_a_slots_only_map_counts_as_non_empty() {
        let got = run_sm_labels(
            r#"{x="y"} | drop service_name | label_format d="[{{.__error__}}]", e="[{{.d}}]""#,
            "a=Hello b=World",
            &[("service_name", "v2")],
            &sm("boom", "", false),
        );
        assert_eq!(
            got,
            Some(owned(&[
                (ERROR_LABEL, "boom"),
                ("d", "[boom]"),
                ("e", "[]"),
            ]))
        );
    }

    /// C31 (the ONLY killer of W10b): the `ip()` short-circuit reads the
    /// SLOT — a stream-label `__error__` must not trip it, so the absent
    /// `addr` drops the line.
    #[test]
    fn c31_ip_short_circuit_reads_the_slot_not_the_vector() {
        let got = run_sm_labels(
            r#"{x="y"} | addr = ip("1.2.3.4")"#,
            "a=Hello b=World",
            &[
                ("__error__", "streamerr"),
                ("service_name", "v10"),
                ("__error___extracted", "boom"),
            ],
            &sm("", "", true),
        );
        assert_eq!(got, None);
    }

    /// C32 (the ONLY killer of W14): a `keep` that does not name the pair
    /// leaves the SLOTS untouched — the err still emits.
    #[test]
    fn c32_keep_never_clears_the_slots() {
        let got = run_sm_labels(
            r#"{x="y"} | keep service_name"#,
            "a=Hello b=World",
            &[("service_name", "v2")],
            &sm("boom", "", false),
        );
        assert_eq!(
            got,
            Some(owned(&[(ERROR_LABEL, "boom"), ("service_name", "v2")]))
        );
    }

    /// AC12/AC19: the error-aware render is selected by a COMPILE-TIME flag
    /// — `false` for every template that does not name the pair.
    #[test]
    fn template_reads_error_pair_gates_the_error_aware_render() {
        for (query, want) in [
            (r#"{a="b"} | label_format x="{{.a}}""#, false),
            (r#"{a="b"} | label_format x="{{.__error__}}""#, true),
            (r#"{a="b"} | label_format x="{{.__error_details__}}""#, true),
        ] {
            let compiled = CompiledPipeline::compile(&stages_of(query)).expect(query);
            let got = compiled.stages.iter().find_map(|s| match s {
                CompiledStage::LabelFormat { fmts, .. } => fmts.iter().find_map(|f| match f {
                    CompiledLabelFmt::Template { reads_err, .. } => Some(*reads_err),
                    _ => None,
                }),
                _ => None,
            });
            assert_eq!(got, Some(want), "{query}");
        }
        for (query, want) in [
            (r#"{a="b"} | line_format "{{.a}}""#, false),
            (r#"{a="b"} | line_format "{{.__error__}}""#, true),
            (r#"{a="b"} | line_format "{{.__error_details__}}""#, true),
        ] {
            let compiled = CompiledPipeline::compile(&stages_of(query)).expect(query);
            let got = compiled.stages.iter().find_map(|s| match s {
                CompiledStage::LineFormat { reads_err, .. } => Some(*reads_err),
                _ => None,
            });
            assert_eq!(got, Some(want), "{query}");
        }
    }

    /// AC27: the self-rename split is decided at COMPILE time; the reserved
    /// destination is still rejected before the split.
    #[test]
    fn a_self_rename_compiles_to_its_own_variant() {
        let compiled =
            CompiledPipeline::compile(&stages_of(r#"{a="b"} | label_format x=x, y=z"#)).unwrap();
        let fmts = compiled
            .stages
            .iter()
            .find_map(|s| match s {
                CompiledStage::LabelFormat { fmts, .. } => Some(fmts),
                _ => None,
            })
            .unwrap();
        assert!(
            matches!(&fmts[0], CompiledLabelFmt::RenameSelf { name } if name == "x"),
            "{fmts:?}"
        );
        assert!(
            matches!(&fmts[1], CompiledLabelFmt::Rename { dst, src } if dst == "y" && src == "z"),
            "{fmts:?}"
        );
        let err =
            CompiledPipeline::compile(&stages_of(r#"{a="b"} | label_format __error__=__error__"#))
                .expect_err("reserved destination");
        assert!(
            err.to_string().contains("__error__ cannot be formatted"),
            "{err}"
        );
    }

    /// The slots are INVARIANT across a `label_format` stage: assigning
    /// `__error_details__` writes the vector, and the untouched slot wins
    /// at emit (v1 contract bullet; also b12 §1).
    #[test]
    fn errs_are_invariant_across_a_label_format_stage() {
        assert_eq!(
            label_format_labels(
                r#"{svc="x"} | json | label_format __error_details__="boom""#,
                "not json at all",
            ),
            owned(&[
                ("svc", "x"),
                (ERROR_LABEL, "JSONParserErr"),
                (ERROR_DETAILS_LABEL, JSON_ERROR_DETAILS),
            ]),
        );
    }

    /// D8 (AC17): `run_into` vs `run_into_with_sm` (ordinary SM) on the
    /// identical bare-drop input differ in EXACTLY the `__error_details__`
    /// entry — the SM dirty seed is the only delta.
    #[test]
    fn run_into_and_run_into_with_sm_differ_in_exactly_the_details_entry() {
        let base = &[("service_name", "d8")];
        let query = r#"{x="y"} | json | drop __error__"#;
        let plain = {
            let compiled = CompiledPipeline::compile(&stages_of(query)).unwrap();
            let base: Vec<(String, String)> = base
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect();
            let mut labels = Vec::new();
            compiled
                .run_into("a=Hello b=World", &base, 0, &mut labels)
                .expect("kept");
            let mut got: Vec<(String, String)> = labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            got.sort();
            got
        };
        let with_sm =
            run_sm_labels(query, "a=Hello b=World", base, &sm("", "", true)).expect("kept");
        assert_eq!(plain, owned(&[("service_name", "d8")]));
        let mut expected = plain.clone();
        expected.push((ERROR_DETAILS_LABEL.to_string(), DET.to_string()));
        expected.sort();
        assert_eq!(with_sm, expected);
    }
}
