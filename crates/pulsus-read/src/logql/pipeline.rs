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
//! - An extracted label colliding with the ORIGINAL stream labels or a
//!   live structured-metadata entry lands under `<name>_extracted`; one
//!   colliding with a name already EXTRACTED on this line is dropped
//!   entirely, so the first extraction of a name wins (issue #334 —
//!   [`ExtractionState`] carries the reference's three separate rules,
//!   and names the parsers that do not follow the last one).
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
use std::cell::Cell;
use std::fmt;
use std::net::IpAddr;

use pulsus_logql::walk;
use pulsus_logql::{
    CompareOp, DropKeepElem, LabelFilterExpr, LabelFmt, LabelMatch, LineFilterOp, MatchOp,
    NumericLiteral, ParserStage, Stage,
};

use super::ip::{IpMatcher, line_has_ip_in};
use super::labels::{EMPTY_STRUCTURED_METADATA, StructuredMetadataCtx};
use super::logfmt_expr;
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

/// The reference's `errLabelFilter` (`pkg/logql/log/error.go:8`): the
/// per-line error class a numeric-family label filter sets when the label
/// value will not convert (`label_filter.go:184`, `:204`, `:252`, `:272`,
/// `:315`, `:335` — each guarded on `!lbs.HasErr()`, so the FIRST error
/// wins). Named because the value is READ back inside the same filter:
/// the reference's `SetErr` is immediate, so a later `__error__ = "…"`
/// leaf in the same `| <label filter>` sees it (issue #248 round 2).
pub const LABEL_FILTER_ERROR: &str = "LabelFilterErr";

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

/// WHICH per-row output budget a row breached — the two are separate
/// ledgers with separate ceilings and separate 422 reasons, and this
/// field is what keeps each error's wording tied to the counter that
/// actually refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowBudget {
    /// The `line_format`/`label_format` render output the row RETAINS
    /// (issues #230/#260; [`super::template::MAX_TEMPLATE_RENDER_BYTES`]).
    TemplateRender,
    /// The flattened label KEYS a bare `| json` builds for the row
    /// (issue #287; [`MAX_JSON_FLATTEN_KEY_BYTES`]).
    JsonFlattenKeys,
}

/// A row breached one of the two per-row output-byte budgets.
/// Query-aborting: the exec layer maps it to the bounded 422
/// ([`super::error::TooBroadReason::TemplateOutputBytes`] /
/// [`super::error::TooBroadReason::JsonFlattenKeyBytes`]) — never a
/// per-line `__error__` tag, never a truncation, never an OOM.
///
/// One type rather than two so the six `run*` entrypoints keep one error
/// channel; `budget` names the ledger and `budget_bytes` its ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowBudgetExceeded {
    pub budget: RowBudget,
    pub budget_bytes: u64,
}

impl RowBudgetExceeded {
    pub(crate) fn template() -> Self {
        RowBudgetExceeded {
            budget: RowBudget::TemplateRender,
            budget_bytes: super::template::MAX_TEMPLATE_RENDER_BYTES,
        }
    }

    pub(crate) fn json_flatten_keys() -> Self {
        RowBudgetExceeded {
            budget: RowBudget::JsonFlattenKeys,
            budget_bytes: MAX_JSON_FLATTEN_KEY_BYTES,
        }
    }
}

impl fmt::Display for RowBudgetExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.budget {
            RowBudget::TemplateRender => write!(
                f,
                "template render exceeded the {}-byte output budget",
                self.budget_bytes
            ),
            RowBudget::JsonFlattenKeys => write!(
                f,
                "`| json` flattened-key expansion exceeded the {}-byte per-line budget",
                self.budget_bytes
            ),
        }
    }
}

impl std::error::Error for RowBudgetExceeded {}

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
    /// non-match — no `__error__`/`__error_details__` is ever set.
    /// `=` drops the non-match; `!=` keeps it.
    ///
    /// **Two divergences from the reference live in that last sentence, both
    /// pre-dating and untouched by issue #248, both in `filterTy`
    /// (`pkg/logql/log/ip.go:123-145 @ v3.7.4`) rather than in where the
    /// filter may sit.** They are reported on #248 for a follow-up, not fixed
    /// here, and are container-measured on `grafana/loki:3.7.4`:
    ///
    /// 1. A **missing label** is `false` for `!=` too in the reference —
    ///    `lbs.Get` failing returns before the `=`/`!=` switch (`ip.go:128-132`),
    ///    so the line is DROPPED. PulsusDB keeps it.
    /// 2. The reference **scans** the label value for an embedded address
    ///    (`ipFilter.filter`, `ip.go:183-226` — the same routine the `ip()`
    ///    LINE filter uses, and the one our [`super::ip::line_has_ip_in`]
    ///    mirrors), so `addr="10.1.2.3:8080"` and `addr="client 10.1.2.3 ok"`
    ///    both match `ip("10.0.0.0/8")`. PulsusDB parses the WHOLE value and
    ///    misses both.
    ///
    /// The matcher is always well formed: a MALFORMED `ip()` pattern that the
    /// reference does not reject compiles to [`LfOp::IpMalformed`] instead.
    Ip {
        name: String,
        matcher: IpMatcher,
        negated: bool,
    },
    /// A `name = ip("…")` / `name != ip("…")` filter whose PATTERN is
    /// malformed, in a position where the reference never surfaces the
    /// pattern error (issue #248 — see [`LabelFilterSite`]).
    ///
    /// The reference's `NewIPLabelFilter` cannot fail: it stores the parse
    /// error on the node and leaves the matcher nil
    /// (`pkg/logql/log/ip.go:94-103 @ v3.7.4`), so the filter stays in the
    /// program and runs. It then evaluates to `true` on an entry carrying a
    /// pipeline error and `false` otherwise — for `=` AND `!=` alike, and
    /// whatever the label holds, because the `HasErr` and nil-matcher checks
    /// both precede the `=`/`!=` switch (`ip.go:123-145 @ v3.7.4`).
    ///
    /// It therefore carries no name, no matcher and no negation: none of
    /// those inputs can change its verdict.
    IpMalformed,
    And,
    Or,
}

/// Where a `| <label filter>` expression sits in the pipeline, which is the
/// whole of what decides whether a malformed `ip()` pattern is REPORTED or
/// silently deferred (issue #248).
///
/// **Two variants because the reference's grammar has exactly two sites.**
/// `git grep -n labelFilter v3.7.4 -- pkg/logql/syntax/syntax.y` returns
/// nine lines: its `%type` declaration (58), its own production
/// (302, 307-311), and exactly two USES elsewhere — line 221
/// (`PIPE labelFilter`, a pipeline stage) and line 160
/// (`unwrapExpr PIPE labelFilter`, a post-`unwrap` filter). `| drop` and
/// `| keep` take `namedMatchers` (371-373), not a `labelFilter`, so no
/// third site can carry an `ip()` pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LabelFilterSite {
    /// A `| <label filter>` PIPELINE stage. The reference builds its stage
    /// through `LabelFilterExpr.Stage()`
    /// (`pkg/logql/syntax/ast.go:801-809 @ v3.7.4`), which type-switches on
    /// the stage's WHOLE filterer and returns `ip.PatternError()` only when
    /// that filterer IS the `ip()` filter — the one place in the reference a
    /// deferred pattern error is ever surfaced.
    PipelineStage,
    /// A post-`unwrap` filter. The reference reduces `Unwrap.PostFilters`
    /// with `log.ReduceAndLabelFilter`
    /// (`pkg/logql/syntax/extractor.go:76,187 @ v3.7.4`), which builds the
    /// filterer directly and never calls `Stage()` — so a malformed pattern
    /// is accepted here in EVERY position, a lone filter included
    /// (container-measured on `grafana/loki:3.7.4`).
    PostUnwrap,
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
/// leaves headroom and costs 96 bytes of stack (`bool` is one byte).
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
    /// The `or` SHORT-CIRCUIT table (issue #248 round 2). `or_jumps[i]`
    /// is the op index evaluation resumes at when op `i` yields `true`,
    /// or [`NO_JUMP`]; **EMPTY when the program contains no `Or`**, which
    /// is every single-leaf and every `and`-only filter, so the common
    /// shapes pay neither the allocation nor a per-op lookup miss.
    ///
    /// It exists because the reference's `or` really does skip its right
    /// operand — `BinaryLabelFilter.Process` returns at
    /// `if !b.And && lok` (`pkg/logql/log/label_filter.go:90-98 @
    /// v3.7.4`) — and a skipped operand is a numeric filter that never
    /// gets to call `SetErr`. Measured on `grafana/loki:3.7.4` with
    /// `n=bad n2=5`: `| logfmt | n2 > 1 or n > 1` returns the line
    /// WITHOUT `__error__`, while `| logfmt | n > 1 or n2 > 1` returns it
    /// WITH. Nothing else about a leaf is observable, so skipping and
    /// suppressing would agree everywhere else; the error is the whole
    /// reason the table is here.
    ///
    /// Not rendered by `Debug` (it is derived from `ops`, and the bytes
    /// must not move), but compared by
    /// [`CompiledPipeline::label_filter_programs_eq`].
    or_jumps: Vec<u32>,
}

/// The [`CompiledLabelFilter::or_jumps`] sentinel: this op does not
/// short-circuit.
const NO_JUMP: u32 = u32::MAX;

impl CompiledLabelFilter {
    /// Where evaluation resumes when op `i` yields `true`, if op `i` is
    /// the LEFT operand of an `or`.
    fn or_jump(&self, i: usize) -> Option<usize> {
        match self.or_jumps.get(i) {
            Some(&t) if t != NO_JUMP => Some(t as usize),
            _ => None,
        }
    }

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
                leaf @ (LfOp::Match { .. }
                | LfOp::Compare { .. }
                | LfOp::Ip { .. }
                | LfOp::IpMalformed) => {
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

/// A range aggregation's own `by`/`without` clause, normalized once at
/// plan time into the shape the per-row label projection needs (issue
/// #344).
///
/// **This is the reference's `(groups, without, noLabels)` triple**, and
/// it is normalized here for exactly the reason it is normalized there:
/// `pkg/logql/log/metrics_extraction.go:154-158 @ v3.7.4` folds the
/// UNWRAPPED LABEL into the group list before any sample is extracted —
///
/// ```text
/// if len(groups) == 0 || without {
///     without = true
///     groups = append(groups, labelName)
///     sort.Strings(groups)
/// }
/// ```
///
/// — so "no grouping at all" and `without (…)` are the SAME code path
/// there, both of which delete the unwrapped label, while `by (L)` with a
/// non-empty `L` takes a different one that does not. The reference never
/// deletes the unwrapped label anywhere else (`streamLabelSampleExtractor
/// ::Process`, `metrics_extraction.go:202-230 @ v3.7.4`, only READS it),
/// which is why `max_over_time({…} | unwrap v [5m]) by (v)` keeps `v` in
/// the output series — captured from the pinned container as
/// `{v="1"} 1, {v="5"} 5, {v="7"} 7, {v="10"} 10`. PulsusDB's own
/// unwrapped-label deletion therefore has to live INSIDE this projection
/// rather than beside it, or that shape would silently lose the label.
///
/// The projection itself is `LabelsBuilder::GroupedLabels`
/// (`pkg/logql/log/labels.go:664-688 @ v3.7.4`) with its `withResult`
/// (`:690-721`) / `withoutResult` (`:723-769`) arms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeGrouping {
    /// `by ()` — the reference's `Grouping.Singleton()`
    /// (`pkg/logql/syntax/ast.go:1550-1553 @ v3.7.4`), which sets
    /// `noLabels` and makes `GroupedLabels` return
    /// `EmptyLabelsResult` unconditionally (`labels.go:669-671`): every
    /// sample in the selector collapses into ONE `{}` series.
    Singleton,
    /// `by (L)`, `L` non-empty — keep exactly the names in `L` (sorted
    /// and deduplicated here; the retain is idempotent in the names, so
    /// `by (fp, fp) == by (fp)` as issue #288 established and this
    /// construct's own capture re-confirmed). The unwrapped label is NOT
    /// deleted — see the type doc.
    By(Vec<String>),
    /// `without (L)` (`L` possibly empty) — drop the names in `L` AND the
    /// unwrapped label, keeping everything else including parser-derived
    /// labels. `without ()` is therefore the identity with respect to the
    /// ungrouped form, which is the reference's `Grouping.Noop()`
    /// (`ast.go:1544-1547 @ v3.7.4`).
    Without(Vec<String>),
}

impl RangeGrouping {
    /// Normalizes a parsed `by`/`without` clause. `sort` + `dedup` is a
    /// plan-time cost paid once, so the per-row projection is a
    /// `binary_search` rather than a linear scan of a client-supplied
    /// list.
    pub fn from_ast(g: &pulsus_logql::Grouping) -> Self {
        if matches!(g.kind, pulsus_logql::GroupingKind::By) && g.labels.is_empty() {
            return RangeGrouping::Singleton;
        }
        let mut names = g.labels.clone();
        names.sort_unstable();
        names.dedup();
        match g.kind {
            pulsus_logql::GroupingKind::By => RangeGrouping::By(names),
            pulsus_logql::GroupingKind::Without => RangeGrouping::Without(names),
        }
    }

    fn contains(names: &[String], name: &str) -> bool {
        names.binary_search_by(|n| n.as_str().cmp(name)).is_ok()
    }

    /// Applies the projection in place over the pipeline's final label
    /// set. `unwrapped` is the successfully-unwrapped label pending
    /// deletion — the reference folds it into the `without` list, so it
    /// is deleted by the `Without` arm and by the ungrouped default, and
    /// retained by `By`.
    fn project(&self, labels: &mut Vec<(Cow<'_, str>, Cow<'_, str>)>, unwrapped: Option<&str>) {
        match self {
            // `labels.go:669-671` — `noLabels` short-circuits every other
            // arm, so nothing (not even a parser-derived label) survives.
            RangeGrouping::Singleton => labels.clear(),
            // `withResult`, `labels.go:690-721`: the output is built FROM
            // the group list, so a name the label set does not carry is
            // simply absent — never materialized as `name=""`. A retain
            // over the label set has exactly that property.
            RangeGrouping::By(names) => labels.retain(|(k, _)| Self::contains(names, k)),
            // `withoutResult`, `labels.go:723-769`: every base and added
            // label except the group names survives.
            RangeGrouping::Without(names) => labels.retain(|(k, _)| {
                !Self::contains(names, k) && unwrapped.is_none_or(|u| k.as_ref() != u)
            }),
        }
    }
}

/// The compiled, reusable per-line evaluator (consumed by the streams
/// read path here and by the M6-10 metric-pipeline seam later).
#[derive(Debug, Clone)]
pub struct CompiledPipeline {
    stages: Vec<CompiledStage>,
    /// The template execution environment (issue #230): the `Local`
    /// zone + wall clock. The zone is SERVER CONFIGURATION
    /// (`reader.template_timezone`, default UTC — issue #311), not the
    /// host's, so two nodes sharing a config render identically.
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
    /// Whether the stages being compiled were offered to the SQL
    /// pushdown. TRUE for [`CompiledPipeline::compile`] — a scan plan
    /// renders its own pipeline's pushable line filters into SQL, so
    /// compiling them again here would double-evaluate them. FALSE for
    /// every stage [`CompiledPipeline::extended_with`] appends (issue
    /// #397): a variants TAIL is never rendered into SQL — the one scan
    /// is planned from the COMMON range alone — so eliding a pushable
    /// filter there DROPS it, and the query answers with rows it asked
    /// to exclude.
    pushdown_active: bool,
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
            pushdown_active: true,
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
            //
            // …and only when a pushdown actually took place (issue
            // #397): a variants tail's stages are never rendered into
            // SQL, so `pushdown_active` is false for them and the
            // filter compiles client-side however pushable it looks.
            if st.pushdown_active
                && !st.seen_line_format
                && super::plan::is_pushable_line_filter(lf)
            {
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
            // Issue #248: the parser admits label filters after `| unwrap`
            // only, so a label-filter stage reached with the unwrap already
            // compiled IS one of the reference's `Unwrap.PostFilters` — the
            // one position where it never surfaces an `ip()` pattern error.
            let site = if st.has_unwrap {
                LabelFilterSite::PostUnwrap
            } else {
                LabelFilterSite::PipelineStage
            };
            let filter = compile_label_filter(expr, site)?;
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
    ///
    /// One state field is NOT resumed: `pushdown_active` is seeded FALSE
    /// (issue #397). `self`'s own stages were offered to the SQL
    /// pushdown, `tail`'s never are — the variants scan is planned from
    /// the COMMON range alone — so a pushable line filter in `tail` must
    /// compile client-side or it is silently dropped.
    pub fn extended_with(&self, tail: &[Stage]) -> Result<Self, PipelineError> {
        let mut st = CompileState {
            seen_line_format: self.seen_line_format,
            mutates_labels: self.mutates_labels,
            rewrites_line: self.rewrites_line,
            has_unwrap: self.has_unwrap,
            all_line_filter_source: self.all_line_filter_source,
            pushdown_active: false,
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
            // Issue #311: the SERVER-CONFIGURED zone (default UTC), never
            // the host's `$TZ`/`/etc/localtime`.
            template_env: template::configured_env(),
            mutates_labels: st.mutates_labels,
            rewrites_line: st.rewrites_line,
            line_filter_only,
            has_unwrap: st.has_unwrap,
            seen_line_format: st.seen_line_format,
            all_line_filter_source: st.all_line_filter_source,
        }
    }

    /// Structural equality of two compiled pipelines' LABEL-FILTER
    /// programs — **including the three fields `Debug` does not render**,
    /// `max_stack`, `has_compare` and `or_jumps`.
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
                // A malformed-pattern leaf is a CONSTANT (issue #248): two of
                // them behave identically whatever label or operator they were
                // written with, so they compare equal.
                (LfOp::IpMalformed, LfOp::IpMalformed)
                | (LfOp::And, LfOp::And)
                | (LfOp::Or, LfOp::Or) => true,
                (LfOp::Match { .. }, _)
                | (LfOp::Compare { .. }, _)
                | (LfOp::Ip { .. }, _)
                | (LfOp::IpMalformed, _)
                | (LfOp::And, _)
                | (LfOp::Or, _) => false,
            }
        }
        let (a, b) = (programs(self), programs(other));
        a.len() == b.len()
            && a.iter().zip(b.iter()).all(|(x, y)| {
                x.max_stack == y.max_stack
                    && x.has_compare == y.has_compare
                    && x.or_jumps == y.or_jumps
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
    ) -> Result<Option<EntryOut<'a>>, RowBudgetExceeded> {
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
    ) -> Result<Option<Cow<'a, str>>, RowBudgetExceeded> {
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
    ) -> Result<Option<Cow<'a, str>>, RowBudgetExceeded> {
        match self.run_mode_into(body, base, ts_ns, sm, None, labels, false, None)? {
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
    ) -> Result<(Option<Cow<'a, str>>, bool), RowBudgetExceeded> {
        self.run_into_reporting_err_with_json_paths(body, base, ts_ns, labels, None)
    }

    /// As [`CompiledPipeline::run_into_reporting_err`], additionally
    /// capturing each full-flatten `| json` leaf's RAW key path into
    /// `json_paths` (issue #254) — the reference's `NewJSONParser(true)`
    /// mode, which ONLY `/detected_fields` uses
    /// (`pkg/querier/queryrange/detected_fields.go:410` vs the query
    /// path's `NewJSONParser(false)`, `pkg/logql/syntax/ast.go:758 @
    /// v3.7.4`). Every other entrypoint passes `None` and allocates
    /// nothing for it.
    ///
    /// The sink is addressed by LABEL POSITION and is cleared by each
    /// `| json` stage that runs, so after a successful call
    /// `json_paths.get(i)` is the path for `labels[i]`.
    pub(crate) fn run_into_reporting_err_with_json_paths<'a>(
        &'a self,
        body: &'a str,
        base: &'a [(String, String)],
        ts_ns: i64,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
        json_paths: Option<&mut JsonPaths>,
    ) -> Result<(Option<Cow<'a, str>>, bool), RowBudgetExceeded> {
        match self.run_mode_into(
            body,
            base,
            ts_ns,
            &EMPTY_STRUCTURED_METADATA,
            None,
            labels,
            false,
            json_paths,
        )? {
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
    ///
    /// `grouping` (issue #344) is the range aggregation's own
    /// `by`/`without` clause, applied to the final label set exactly
    /// where the reference applies it — inside the sample extractor,
    /// before the sample reaches any window
    /// (`streamLabelSampleExtractor::Process` ends in
    /// `l.builder.GroupedLabels()`, `metrics_extraction.go:229 @
    /// v3.7.4`). It is passed per CALL rather than compiled into the
    /// pipeline because `variants(...)` SHARES one compiled pipeline
    /// across variants with identical tails
    /// ([`super::variants::VariantArena`]) while each variant carries its
    /// own grouping.
    pub fn run_metric_into<'a>(
        &'a self,
        body: &'a str,
        base: &'a [(String, String)],
        ts_ns: i64,
        grouping: Option<&RangeGrouping>,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    ) -> Result<MetricRun<'a>, RowBudgetExceeded> {
        self.run_metric_into_with_sm(
            body,
            base,
            ts_ns,
            &EMPTY_STRUCTURED_METADATA,
            grouping,
            labels,
        )
    }

    /// As [`CompiledPipeline::run_metric_into`], for a row carrying per-entry
    /// structured metadata (issue #249) — the exact metric sibling of
    /// [`CompiledPipeline::run_into_with_sm`], and the same contract: `base`
    /// is the stream labels ALREADY merged with the ordinary metadata entries
    /// (`merge_labels_with_structured_metadata`), and `sm` carries the
    /// reserved-name routing outcome plus `has_ordinary`.
    ///
    /// The reference merges at exactly this point, and unconditionally: both
    /// sample extractors call `builder.Add(StructuredMetadataLabel, …)` as
    /// their FIRST act — `streamLineSampleExtractor.Process`
    /// (`pkg/logql/log/metrics_extraction.go:102-104 @ v3.7.4`) and
    /// `streamLabelSampleExtractor.Process` for unwrap (`:202-205`) — and the
    /// `NoopStage` short-circuit at `:104-108` sits AFTER the `Add`, so even a
    /// query with no pipeline stages merges. `extractor.go:76-85` routes every
    /// range aggregation through one of those two, so there is no third path.
    pub fn run_metric_into_with_sm<'a>(
        &'a self,
        body: &'a str,
        base: &'a [(String, String)],
        ts_ns: i64,
        sm: &'a StructuredMetadataCtx,
        grouping: Option<&RangeGrouping>,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    ) -> Result<MetricRun<'a>, RowBudgetExceeded> {
        Ok(self
            .run_mode_into(body, base, ts_ns, sm, grouping, labels, true, None)?
            .0)
    }

    /// The second element of the return is the reference's `HasErr()` at the
    /// end of the pipeline — the pre-materialization slot state
    /// [`CompiledPipeline::run_into_reporting_err`] surfaces.
    #[allow(clippy::too_many_arguments)]
    fn run_mode_into<'a>(
        &'a self,
        body: &'a str,
        base: &'a [(String, String)],
        ts_ns: i64,
        sm: &'a StructuredMetadataCtx,
        grouping: Option<&RangeGrouping>,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
        metric: bool,
        mut json_paths: Option<&mut JsonPaths>,
    ) -> Result<(MetricRun<'a>, bool), RowBudgetExceeded> {
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
        // The `| json` full-flatten key budget's lifetime is the ROW too,
        // and for the same reason (issue #287): every `| json` stage's
        // flattened keys are retained in the label set simultaneously,
        // and the number of stages is bounded only by the query-text cap.
        let json_key_budget = JsonKeyBudget::default();
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
        // The reference's per-line builder categories (issue #334). The
        // merged `base` is stream labels first, then the row's ordinary
        // structured metadata — see `StructuredMetadataCtx::stream_label_count`.
        let stream_len = sm.stream_label_count.unwrap_or(base.len()).min(base.len());
        let (stream_base, sm_base) = base.split_at(stream_len);
        let mut st = ExtractionState {
            stream: stream_base,
            sm: sm_base,
            parsed_over_sm: Vec::new(),
            removed_parsed: Vec::new(),
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
                CompiledStage::Json { extractions } => run_json(
                    &line,
                    extractions,
                    labels,
                    &mut st,
                    &mut errs,
                    &json_key_budget,
                    json_paths.as_deref_mut(),
                )?,
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
                            LogfmtFlags {
                                strict: *strict,
                                keep_empty: *keep_empty,
                            },
                            extractions,
                            labels,
                            &mut st,
                            &mut errs,
                            |c| c,
                        ),
                        Cow::Owned(text) => run_logfmt(
                            text,
                            LogfmtFlags {
                                strict: *strict,
                                keep_empty: *keep_empty,
                            },
                            extractions,
                            labels,
                            &mut st,
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
                                            &mut st,
                                            Cow::Borrowed(name),
                                            KeyOrigin::Line,
                                            Cow::Borrowed(m.as_str()),
                                            OnAlreadyExtracted::Skip,
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
                                            &mut st,
                                            Cow::Borrowed(name),
                                            KeyOrigin::Line,
                                            Cow::Owned(m.as_str().to_string()),
                                            OnAlreadyExtracted::Skip,
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
                                        &mut st,
                                        Cow::Borrowed(name),
                                        KeyOrigin::Line,
                                        Cow::Borrowed(value),
                                        OnAlreadyExtracted::Skip,
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
                                        &mut st,
                                        Cow::Borrowed(name),
                                        KeyOrigin::Line,
                                        Cow::Owned(value.to_string()),
                                        OnAlreadyExtracted::Skip,
                                        &mut errs.dirty,
                                    );
                                });
                            }
                        }
                    }
                }
                CompiledStage::LabelFilter(filter) => {
                    // `failed` is the error the reference's `SetErr` has
                    // ALREADY written by the time the filter finishes — the
                    // evaluator sets it at the failing leaf and every later
                    // leaf reads it (issue #248 round 2). It borrows the
                    // offending raw value; only the owned detail `String` is
                    // deferred to here, and only on the KEPT path, because a
                    // dropped line discards the reference's builder and with
                    // it the error. Both paths carry `__error_details__`:
                    // streams (issue #99) and metric (issue #104).
                    let mut failed: Option<(UnitKind, &str)> = None;
                    if !eval_label_filter(filter, labels, &errs, &mut failed) {
                        return Ok((MetricRun::Dropped, errs.has_err()));
                    }
                    // Keep the line and tag the error class in the
                    // out-of-band slots (a later `__error__=""` filter drops
                    // it) — but ONLY when no earlier error is set: the
                    // reference guards every numeric-family write on
                    // `!lbs.HasErr()` (`label_filter.go:184`, `:204`, `:252`,
                    // `:272`, `:315`, `:335` — "first error wins",
                    // live-probed). When the guard blocks, the detail
                    // `String` is never built (alloc budget).
                    if failed.is_some() && !errs.has_err() {
                        let details =
                            failed.map(|(kind, value)| label_filter_error_details(kind, value));
                        errs.set_err(Cow::Borrowed(LABEL_FILTER_ERROR));
                        if let Some(details) = details {
                            errs.set_details(Cow::Owned(details));
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
                                        return Err(RowBudgetExceeded::template());
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
                                        return Err(RowBudgetExceeded::template());
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
                            return Err(RowBudgetExceeded::template());
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
                                // The `Set` is a `Set(ParsedLabel, …)`
                                // (`fmt.go:417`), so `dst` counts as
                                // extracted for every later parser, and the
                                // `Del` of `src` leaves whatever `src`
                                // already recorded standing (issue #334).
                                let src_extracted = st.is_extracted(labels, src);
                                if let Some(value) = remove_label(labels, src) {
                                    st.note_removed(src_extracted, Cow::Borrowed(src.as_str()));
                                    let dst = st.note_parsed_set(Cow::Borrowed(dst.as_str()));
                                    set_label(labels, dst, value);
                                    errs.dirty = true;
                                }
                            }
                            CompiledLabelFmt::RenameSelf { name } => {
                                // `Set(dst, v)` THEN `Del(src)` with dst == src
                                // net-deletes the label (`fmt.go:417-418`,
                                // live-probed); resolved ⇒ dirty, unresolved ⇒
                                // complete no-op (issue #238).
                                if remove_label(labels, name).is_some() {
                                    // The `Set` ran before the `Del`
                                    // (`fmt.go:417-418`), so the name is
                                    // extracted for the rest of the line even
                                    // though no label survives — issue #334,
                                    // container cell `| json | label_format
                                    // a=a | json` over `{"a":1}` emits
                                    // NOTHING.
                                    st.note_removed(true, Cow::Borrowed(name.as_str()));
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
                                                return Err(RowBudgetExceeded::template());
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
                                                return Err(RowBudgetExceeded::template());
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
                                                return Err(RowBudgetExceeded::template());
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
                                        // A `Set(ParsedLabel, …)`
                                        // (`fmt.go:431`), so `dst` is
                                        // extracted for every later parser
                                        // (issue #334) — and dirty.
                                        let dst = st.note_parsed_set(Cow::Borrowed(dst.as_str()));
                                        set_label(labels, dst, rendered.into_cow());
                                        errs.dirty = true;
                                    }
                                    Err(e) if e.budget_breach => {
                                        // Render-budget breach: abort the
                                        // QUERY (bounded 422 — issue #230
                                        // follow-up), never a per-line tag.
                                        return Err(RowBudgetExceeded::template());
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
                    if let Some(entry) = run_unpack(line.as_ref(), labels, &mut st, &mut errs) {
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
                                // `Del` does not un-record what a parsed
                                // `Set` extracted (`labels.go:322-331` never
                                // touches `parserKeyHints`), so a dropped
                                // parsed name still blocks a later parser —
                                // issue #334, container cell `| json | drop a
                                // | json` over `{"a":1}` emits NOTHING.
                                let was = st.is_extracted(labels, &elem.label);
                                if remove_label(labels, &elem.label).is_some() {
                                    st.note_removed(was, Cow::Borrowed(elem.label.as_str()));
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
                                    let was = st.is_extracted(labels, &elem.label);
                                    remove_label(labels, &elem.label);
                                    st.note_removed(was, Cow::Borrowed(elem.label.as_str()));
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
                    // Same `Del` rule as `drop` (issue #334): a name this
                    // stage removes stays extracted if a parsed `Set` put it
                    // there. Collected first, because the retain borrows
                    // `labels` while `is_extracted` reads it.
                    let dropped: Vec<Cow<'a, str>> = labels
                        .iter()
                        .filter(|(k, v)| {
                            !(k == ERROR_LABEL
                                || k == ERROR_DETAILS_LABEL
                                || k == PRESERVE_ERROR_LABEL
                                || elems.iter().any(|elem| {
                                    elem.label == k.as_ref()
                                        && match &elem.matcher {
                                            None => true,
                                            Some(m) => m.matches(v),
                                        }
                                }))
                        })
                        .map(|(k, _)| k.clone())
                        .collect();
                    for name in dropped {
                        let was = st.is_extracted(labels, &name);
                        st.note_removed(was, name);
                    }
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

        // The ONE merge point (`appendErrors` over the `visible()` gate):
        // kept lines only — a dropped line never merges (issue #238). The
        // slot state is captured BEFORE the merge consumes the slots.
        //
        // Read BEFORE the grouping projection below, not just before the
        // merge: `GroupedLabels` (`labels.go:665-668 @ v3.7.4`) returns
        // the UNGROUPED label set whenever `HasErr()`, "before applying
        // grouping otherwise the error might get lost". PulsusDB fails the
        // whole metric query on a surviving nonempty `__error__`
        // (`check_surviving_error`), so keeping the ungrouped set here is
        // what guarantees the error label is still present for that check
        // to find — a `by (fp)` projection would otherwise drop
        // `__error__` and turn a named 400 into a silent wrong answer.
        let has_err = errs.has_err();
        // The deferred successful-unwrap deletion (issue #221): the label
        // leaves the result series only now, AFTER every post-`unwrap`
        // filter processed it — the reference's ordering. Issue #344 folds
        // it into the grouping projection, exactly as the reference folds
        // it into the extractor's `without` list
        // (`metrics_extraction.go:154-158 @ v3.7.4`): the ungrouped and
        // `without` forms delete it, `by (L)` does not.
        if has_err {
            // `labels.go:665-668` — on an errored line `GroupedLabels`
            // returns `b.LabelsResult()`, the builder's labels UNTOUCHED.
            // Nothing else in the reference ever deletes the unwrapped
            // label (`streamLabelSampleExtractor::Process` only READS it,
            // `metrics_extraction.go:202-230`); it goes only via the
            // `without` list the extractor folds it into, and that list is
            // part of the grouping this branch skips. So an errored line
            // keeps it — including the shape this branch exists for: a
            // SUCCESSFUL unwrap followed by a failing post-unwrap filter,
            // where `unwrapped` is `Some` and the label is still present.
            //
            // Both the grouped and the ungrouped forms take this arm,
            // because the reference has ONE `GroupedLabels` and a nil
            // grouping reaches its `HasErr` check the same way.
        } else {
            // The deferred successful-unwrap deletion (issue #221): the
            // label leaves the result series only now, AFTER every
            // post-`unwrap` filter processed it — the reference's
            // ordering. Issue #344 folds it into the grouping projection,
            // exactly as the reference folds it into the extractor's
            // `without` list (`metrics_extraction.go:154-158 @ v3.7.4`):
            // the ungrouped and `without` forms delete it, `by (L)` does
            // not.
            match grouping {
                None => {
                    if let Some(label) = unwrapped {
                        remove_label(labels, label);
                    }
                }
                Some(g) => g.project(labels, unwrapped),
            }
        }
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
///
/// **This body is NOT going to be replaced with Go's wording, and the
/// instruction that used to stand here saying it would is withdrawn**
/// (issue #246, owner rulings 2026-07-26 and 2026-08-08). What is in
/// scope is the STATUS and the accept/reject decision — both `400` here
/// and there, pinned point for point by
/// `tests/logql_regex_accept_matrix.rs`. The prose is not, for two
/// measured reasons: nothing branches on it (the reference's own four
/// non-vendor occurrences of `error parsing regexp` are all in its
/// `_test.go` files), and byte parity is unreachable without porting
/// Go's parser — its `Error.Expr` is the offending SUB-TOKEN rather than
/// the pattern (`vendor/github.com/grafana/regexp/syntax/parse.go:16-22
/// @ v3.7.4`), and that port was refused on #331. The wording difference
/// is ledgered under `logql-error-envelope`; where the two sides
/// disagree about the DECISION rather than the words, the classes are
/// enumerated in `logql-regex-accept-surface-divergence` and owned by
/// #400.
///
/// NOTE: `label_replace` (issue #276) is the ONE deliberate exception —
/// LIVE, not dormant: the reference genuinely reports the WRAPPED
/// `^(?:…)$` form at that single site, so
/// `plan::LabelReplaceSpec::compile` deliberately does NOT route through
/// this seam, and neither side may be "consistency fixed" toward the
/// other (pinned by
/// `label_replace_bad_regex_reports_the_wrapped_form_not_the_users_pattern`).
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

/// **Issue #400 Stage 2: the constructs RE2 decidably rejects, refused
/// BEFORE the compile that would silently read them as something else.**
///
/// The Rust `regex` crate does not merely accept `a**`, `[[:foo:]]`,
/// `\u{263A}`, `(?x)a` and their kin — it reads them as a DIFFERENT
/// pattern (`a**` becomes `(a*)*`, which matches every line), while
/// grafana/loki v3.7.4 answers `400`. So a compile is the wrong
/// adjudicator here and the pre-check runs ahead of it.
///
/// `pulsus_re2::re2_definitely_rejects` is reject-only and conservative
/// in one direction: it never claims a pattern the reference serves
/// (asserted over the whole 4,315-pattern frozen corpus in
/// `pulsus-re2/tests/re2_reject_classes.rs`, every flagged member probed
/// at the container). A `false` changes nothing — the compile decides as
/// it always did — so no query that worked before can stop working for a
/// reason this function invented.
///
/// The message names the CONSTRUCT, not the engine's prose, and **no
/// parity is claimed for its text**: #246's owner rulings (2026-07-26,
/// 2026-08-08) pin the status and the accept/reject decision only. It
/// does not route through [`bad_regex`] because there is no
/// `regex::Error` to report — the pattern was never compiled.
fn re2_reject_precheck(pattern: &str) -> Result<(), PipelineError> {
    match pulsus_re2::re2_rejection_construct(pattern) {
        None => Ok(()),
        Some(construct) => Err(PipelineError::BadRegex(format!(
            "error parsing regexp: {construct}: `{pattern}`"
        ))),
    }
}

/// Issue #291: every user pattern on this path compiles through
/// `pulsus_re2::compile_user_regex`, which refuses it BEFORE the HIR
/// translation when translating it could allocate more than
/// `MAX_REGEX_COMPILE_TRANSIENT_BYTES`. The engine's own accept/reject
/// decision and wording are untouched — an `Engine` error still routes to
/// [`bad_regex`], and only the new over-budget refusal is a new message.
fn compile_regex(pattern: &str) -> Result<regex::Regex, PipelineError> {
    re2_reject_precheck(pattern)?;
    pulsus_re2::compile_user_regex(pattern).map_err(|e| match e {
        pulsus_re2::RegexCompileError::Engine(e) => bad_regex(pattern, &e),
        e @ pulsus_re2::RegexCompileError::TooLarge { .. } => {
            PipelineError::BadRegex(e.to_string())
        }
    })
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
    re2_reject_precheck(pattern)?;
    // Issue #291: the ANCHORED string is what gets compiled, so it is the
    // one the budget estimates.
    pulsus_re2::compile_user_regex_anchored(pattern).map_err(|e| match e {
        pulsus_re2::RegexCompileError::Engine(e) => bad_regex(pattern, &e),
        e @ pulsus_re2::RegexCompileError::TooLarge { .. } => {
            PipelineError::BadRegex(e.to_string())
        }
    })
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
        } => {
            // Issue #247: an extraction EXPRESSION has its own grammar in
            // the reference (`pkg/logql/log/logfmt/ @ v3.7.4`), refused at
            // `Stage()` and surfaced as a 400 — so it is refused here, at
            // the pipeline compile every entry point runs before any I/O.
            // A bare `| logfmt` has no extractions, so the loop body never
            // runs and `detected.rs`'s `LOGFMT_PARSER` still cannot fail.
            let mut compiled: Vec<(String, String)> = Vec::with_capacity(extractions.len());
            for e in extractions {
                let key = logfmt_expr::parse_logfmt_expr(&e.expression).map_err(|msg| {
                    PipelineError::BadParserExpr(format!(
                        "logfmt expression {:?}: {msg}",
                        e.expression
                    ))
                })?;
                // `paths[exp.Identifier] = path` (`pkg/logql/log/parser.go:521
                // @ v3.7.4`) is a MAP assignment, so a REPEATED identifier
                // keeps only its LAST source key and is pre-seeded/scanned
                // once. Every expression is still parsed first, so a later
                // bad one is still refused. Measured on `grafana/loki:3.7.4`
                // over `b=1 a-b=2 x=3`: `| logfmt a="b", a="nosuch"` answers
                // `a=""` (the surviving expression misses) and
                // `| logfmt a="nosuch", a="b"` answers `a="1"`.
                //
                // The slot keeps the FIRST declaration's POSITION. The
                // reference has no position at all here (map iteration), and
                // position is only ever consulted as the tie-break between
                // two DIFFERENT identifiers sharing one source key — see
                // `logfmt_target_for` and the
                // `logfmt-expression-duplicate-source-key-tiebreak` ledger
                // entry.
                match compiled.iter_mut().find(|(id, _)| *id == e.label) {
                    Some(slot) => slot.1 = key,
                    None => compiled.push((e.label.clone(), key)),
                }
            }
            Ok(CompiledStage::Logfmt {
                strict: *strict,
                keep_empty: *keep_empty,
                extractions: compiled,
            })
        }
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
fn compile_label_filter(
    expr: &LabelFilterExpr,
    site: LabelFilterSite,
) -> Result<CompiledLabelFilter, PipelineError> {
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
    // Issue #248 — the ONE condition under which a malformed `ip()` pattern
    // is reported, and the exact analogue of `LabelFilterExpr.Stage()`
    // (`pkg/logql/syntax/ast.go:801-809 @ v3.7.4`): the stage's whole
    // filterer must BE the `ip()` filter. `leaf_only` says the expression is
    // a single node, so inside the `Ip` arm below it says that node is this
    // one. Parentheses are transparent in both grammars
    // (`syntax.y:307 @ v3.7.4` yields the inner filter, and our parser
    // returns it unwrapped), so `| (a=ip("x"))` reports like `| a=ip("x")`.
    // Anywhere else — nested under `and`/`or`/`,`, or any post-`unwrap`
    // position — the error is dropped and the filter runs as
    // [`LfOp::IpMalformed`].
    let report_pattern_error = leaf_only && matches!(site, LabelFilterSite::PipelineStage);
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
                // The IP label filter never mutates the label set (it
                // cannot error), so it does not force the label-mutating
                // fan-out path — `has_compare` stays as it was.
                match IpMatcher::parse(value) {
                    Ok(matcher) => LfOp::Ip {
                        name: name.clone(),
                        matcher,
                        negated: *negated,
                    },
                    Err(e) if report_pattern_error => {
                        return Err(PipelineError::BadIpFilter(e.to_string()));
                    }
                    Err(_) => LfOp::IpMalformed,
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
    let or_jumps = build_or_jumps(&ops);
    Ok(CompiledLabelFilter {
        ops,
        max_stack,
        has_compare,
        or_jumps,
    })
}

/// The `or` short-circuit table for a post-order program — see
/// [`CompiledLabelFilter::or_jumps`].
///
/// Two linear passes, both allocation-free when the program has no `Or`
/// (the single-leaf and `and`-only shapes).
///
/// Pass 1 recovers each internal node's operands from post-order the same
/// way [`CompiledLabelFilter::tree_shape`] does, and for every `Or` marks
/// its LEFT operand with the index just past that `Or`.
///
/// Pass 2 CHAINS, and it is the half that is easy to get wrong: an `Or`
/// that is itself the left operand of an enclosing `Or` must hand its own
/// destination down. `a or b or c` parses left-deep, so its program is
/// `[a, b, Or1, c, Or2]`; the reference evaluates `Left = (a or b)`,
/// short-circuits inside it on `a`, returns `true` to `Or2`, which
/// short-circuits again — `c` is never evaluated at all. Marking `a` with
/// `Or1 + 1 = 3` would land on `c` and evaluate it. Parents always sit at
/// a HIGHER index than their children in post-order, so one descending
/// pass resolves every chain: when `i`'s provisional target is
/// `parent + 1` and `parent` has itself been resolved, `i` inherits it.
fn build_or_jumps(ops: &[LfOp]) -> Vec<u32> {
    if !ops.iter().any(|o| matches!(o, LfOp::Or)) {
        return Vec::new();
    }
    let mut jumps = vec![NO_JUMP; ops.len()];
    let mut pending: Vec<usize> = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        if matches!(op, LfOp::And | LfOp::Or) {
            // Same `unwrap_or(0)` convention as `tree_shape`: a
            // post-order program built by `compile_label_filter` always
            // has both operands on the stack.
            pending.pop();
            let l = pending.pop().unwrap_or(0);
            if matches!(op, LfOp::Or) {
                jumps[l] = (i + 1) as u32;
            }
        }
        pending.push(i);
    }
    for i in (0..ops.len()).rev() {
        if jumps[i] != NO_JUMP {
            let parent_or = jumps[i] as usize - 1;
            if jumps[parent_or] != NO_JUMP {
                jumps[i] = jumps[parent_or];
            }
        }
    }
    jumps
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
            } else if let Some(bytes) = parse_query_bytes_literal(raw) {
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
/// The byte-unit suffixes the reference accepts in QUERY TEXT — exactly
/// the 21 observable spellings, case-sensitive (issue #350, probed
/// exhaustively on BOTH grafana/loki:3.7.3 and :3.7.4 — every case
/// variant of every humanize-table suffix; the two versions agree).
/// This is a strict subset of the humanize table [`parse_bytes_value`]
/// accepts for LABEL VALUES, because the reference's LEXER gates the
/// suffix runes to `B i k K M G T P` (`isBytesSizeRune`,
/// `pkg/logql/syntax/lex.go`) BEFORE `humanize.ParseBytes` runs:
/// * lowercase `b`, `m`, `g`, `t`, `p`, `e` are not size runes, so
///   `1b`/`1kb`/`1mb`/`1gb`/`1tb`/`1pb`/`1eb` all reject (`1m` etc. are
///   DURATIONS — [`classify_numeric_literal`] tries duration first);
/// * the `P` and `E` tiers are dead in practice even where the rune
///   table admits them: Go's number scanner consumes `1P…`/`1E…` as
///   hex-float/scientific exponent forms and errors first (probed:
///   `'P' exponent requires hexadecimal mantissa` / `exponent has no
///   digits` on both versions) — no peta, no exa.
const QUERY_BYTES_SUFFIXES: &[&str] = &[
    "B", "K", "KB", "Ki", "KiB", "k", "kB", "ki", "kiB", "M", "MB", "Mi", "MiB", "G", "GB", "Gi",
    "GiB", "T", "TB", "Ti", "TiB",
];

/// Parses a QUERY-side byte literal (a label-filter RHS such as
/// `| size >= 1KiB`) to f64 bytes, matching the reference's accepted
/// set exactly (issue #350):
/// * the suffix must be one of the 21 [`QUERY_BYTES_SUFFIXES`]
///   spellings, case-sensitive — everything else (`1b`, `1kb`, `1pb`,
///   `1024b`, `KIB`, …) is the reference's parse-time 400, surfaced
///   here as the `neither a duration nor a bytes quantity` rejection;
/// * fractional mantissas are legal (`1.5KiB`, `1.KiB`, `.5KiB` —
///   probed 200), commas/spaces are not (the lexer never admits them
///   into one token, matching the reference);
/// * a value at or past `math.MaxUint64` rejects
///   (`999999999TiB` — probed 400), via [`parse_bytes_value`]'s
///   overflow guard;
/// * a ZERO-valued literal rejects (`0B`, `0KB`, `0.5B` → truncated 0 —
///   probed on both versions/both configs: the reference's frontend
///   re-renders the threshold via `humanize.Bytes(0)` = `0B`, whose
///   re-parse fails `binary literal has no digits`, a clean 400 on the
///   default config and a retry-storm 500 under the comparison config —
///   both-reject either way).
///
/// LABEL-VALUE conversion deliberately keeps the FULL case-insensitive
/// humanize table ([`parse_bytes_value`], unchanged): the reference
/// converts values with `humanize.ParseBytes` directly, so `size=1eb`
/// as DATA still reads as 1e18 while `>= 1eb` as QUERY TEXT rejects.
pub(crate) fn parse_query_bytes_literal(raw: &str) -> Option<f64> {
    let num_end = raw
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(raw.len());
    let suffix = &raw[num_end..];
    if !QUERY_BYTES_SUFFIXES.contains(&suffix) {
        return None;
    }
    let bytes = parse_bytes_value(raw)?;
    // The zero-valued rejection (see above). `parse_bytes_value`
    // truncates toward zero, so `0.5B` is 0 here, as upstream.
    if bytes == 0.0 {
        return None;
    }
    Some(bytes)
}

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

/// Where `name` sits in the label vector, if anywhere. The parsers look a
/// key up ONCE and then pass this around (see
/// [`ExtractionState::is_extracted_at`]).
fn label_position(labels: &[(Cow<'_, str>, Cow<'_, str>)], name: &str) -> Option<usize> {
    labels.iter().position(|(k, _)| k == name)
}

fn contains_label(labels: &[(Cow<'_, str>, Cow<'_, str>)], name: &str) -> bool {
    label_position(labels, name).is_some()
}

/// `set_label_at` for a caller that has already located the slot: writes
/// into `at` when it is `Some`, appends otherwise, and returns the
/// position either way.
fn put_label_at<'a>(
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    at: Option<usize>,
    name: Cow<'a, str>,
    value: Cow<'a, str>,
) -> usize {
    match at {
        Some(idx) => {
            labels[idx].1 = value;
            idx
        }
        None => {
            labels.push((name, value));
            labels.len() - 1
        }
    }
}

fn set_label<'a>(
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    name: Cow<'a, str>,
    value: Cow<'a, str>,
) {
    set_label_at(labels, name, value);
}

/// [`set_label`] returning the POSITION the label now occupies — the
/// address [`JsonPaths`] keys its capture by, so a collision rename that
/// overwrites an existing slot re-points that slot's path instead of
/// appending an orphan.
fn set_label_at<'a>(
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    name: Cow<'a, str>,
    value: Cow<'a, str>,
) -> usize {
    if let Some(idx) = labels.iter().position(|(k, _)| *k == name) {
        labels[idx].1 = value;
        idx
    } else {
        labels.push((name, value));
        labels.len() - 1
    }
}

fn remove_label<'a>(
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    name: &str,
) -> Option<Cow<'a, str>> {
    let idx = labels.iter().position(|(k, _)| k == name)?;
    Some(labels.remove(idx).1)
}

/// The reference's per-line builder state that a FLAT label vector cannot
/// represent (issue #334) — which names the stream ORIGINALLY carried,
/// which structured-metadata names are still live, and which names a
/// `Set(ParsedLabel, …)` has already recorded as extracted.
///
/// Three reference facts, each read from a DIFFERENT place, and they do
/// not agree with one another (all `@ v3.7.4`):
///
/// - `BaseHas(key)` (`pkg/logql/log/labels.go:278-280`) asks `b.base`,
///   the stream's labels as the builder was created. `Del` never touches
///   it, so `| drop x | json` over `{"x":1}` STILL renames to
///   `x_extracted`.
/// - `HasInCategory(key, StructuredMetadataLabel)`
///   (`pkg/logql/log/labels.go:273-275`) asks the LIVE category, which
///   `Del` does empty — so `| drop m | json` over `{"m":1}` does NOT
///   rename.
/// - `ParserHint.Extracted(key)` (`pkg/logql/log/parser_hints.go:68-71`)
///   is a per-line set that every `Set(ParsedLabel, …)` records
///   (`labels.go:378-385`) and nothing ever un-records, cleared once per
///   line by `streamPipeline.Process` → `builder.Reset()`
///   (`pkg/logql/log/pipeline.go:222` → `labels.go:195-203`). Every
///   IMPLICIT parser consults it AFTER the rename and SKIPS on a hit, so
///   the FIRST extraction of a name wins and later ones vanish — across
///   stages, and even after the label itself was dropped.
///
/// **How the two derived sets below stand in for the reference's two
/// explicit ones.** A name is a LIVE structured-metadata entry iff the
/// row contributed it AND it is still in the label vector AND no parsed
/// `Set` has taken it over — those are exactly the two ways an entry
/// leaves `b.add[StructuredMetadataLabel]` (`Del`, `labels.go:322-331`;
/// and `Set(ParsedLabel, …)`'s `deleteWithCategory`, `labels.go:347-349`).
/// A name is EXTRACTED iff it is in the label vector and is neither an
/// original stream label nor a live structured-metadata entry — i.e. the
/// only thing that could have put it there is a parsed `Set` — OR a
/// `Del` removed it after a parsed `Set` had put it there. Both
/// correction lists stay EMPTY unless a stage actually removes or
/// overwrites something, so the common pipeline allocates nothing for
/// this state.
struct ExtractionState<'a, 'r> {
    /// The reference's `b.base` — the stream's own labels, immutable for
    /// the row.
    stream: &'r [(String, String)],
    /// The ordinary structured-metadata names the row contributed, under
    /// the post-rename names
    /// [`super::labels::merge_labels_with_structured_metadata`] gave them
    /// — which is the shape the reference's own category holds them in.
    sm: &'r [(String, String)],
    /// Names in [`Self::sm`] that a parsed `Set` has taken over, so the
    /// vector entry under that name is no longer the structured-metadata
    /// one (the reference's `deleteWithCategory(StructuredMetadataLabel,
    /// n)` inside `Set`).
    parsed_over_sm: Vec<Cow<'a, str>>,
    /// Names a `Del` removed AFTER a parsed `Set` had recorded them —
    /// still extracted for the rest of the line, though no longer in the
    /// vector (`Del` does not touch `ParserHint.extracted`).
    removed_parsed: Vec<Cow<'a, str>>,
}

/// What a parser does when its resolved key is already extracted.
///
/// **Every parser here reads the same document in the same order and
/// writes in the order it reads.** That is the shared rule, and it is
/// what a first version of this got wrong by treating the expression
/// forms as a special case: they consult `ParserHint.Extracted` for
/// their own identifiers where the implicit parsers do — no such check
/// exists in `JSONExpressionParser.Process`
/// (`pkg/logql/log/parser.go:671-731 @ v3.7.4`), and
/// `LogfmtExpressionParser` bypasses it through `alwaysExtract`
/// (`parser.go:552-585`) — so a repeat overwrites instead of vanishing.
/// It does NOT make the WRITE ORDER a different question: the
/// last-writer is still whichever the DOCUMENT reaches last, which is
/// why `| json a="p", a="q"` is `a="2"` over `{"p":1,"q":2}` and `a="1"`
/// over `{"q":2,"p":1}` (both captured at v3.7.4). See
/// [`run_json_targets`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnAlreadyExtracted {
    /// Implicit parsers (`json` full flatten, `logfmt`, `regexp`,
    /// `pattern`, `unpack`): the first extraction of a name wins.
    Skip,
    /// Expression parsers: `Set` regardless, last write wins.
    Overwrite,
}

impl<'a> ExtractionState<'a, '_> {
    fn stream_has(&self, key: &str) -> bool {
        self.stream.iter().any(|(k, _)| k == key)
    }

    fn sm_has(&self, key: &str) -> bool {
        self.sm.iter().any(|(k, _)| k == key)
    }

    /// `HasInCategory(key, StructuredMetadataLabel)`, given whether `key`
    /// is currently in the label vector.
    fn live_sm_at(&self, key: &str, present: bool) -> bool {
        self.sm_has(key) && present && !self.parsed_over_sm.iter().any(|k| k == key)
    }

    /// `BaseHas(key) || HasInCategory(key, StructuredMetadataLabel)` — the
    /// predicate every parser applies before deciding to rename.
    fn collides_at(&self, key: &str, present: bool) -> bool {
        self.stream_has(key) || self.live_sm_at(key, present)
    }

    /// `ParserHint.Extracted(key)`, given whether `key` is currently in
    /// the label vector.
    ///
    /// **Every hot caller passes a presence it has ALREADY looked up.**
    /// The vector scan is the whole cost of these predicates on a wide
    /// line — a `| json` over W keys already costs W scans of a W-long
    /// vector, and the quadratic term is multiplied by however many
    /// times ONE leaf looks its key up. So resolving the key, testing it
    /// and writing it share a single lookup on the unrenamed path; a
    /// rename pays a second, and only fires for a name the stream or the
    /// metadata supplies.
    fn is_extracted_at(&self, key: &str, present: bool) -> bool {
        (present && !self.collides_at(key, present)) || self.removed_parsed.iter().any(|k| k == key)
    }

    /// [`Self::collides_at`] for a caller that has not looked the key up.
    fn collides(&self, labels: &[(Cow<'a, str>, Cow<'a, str>)], key: &str) -> bool {
        // `sm_has` is a handful of entries and short-circuits, so the
        // vector scan only happens for a name the metadata supplies.
        self.stream_has(key)
            || (self.sm_has(key) && self.live_sm_at(key, contains_label(labels, key)))
    }

    /// [`Self::is_extracted_at`] for a caller that has not looked the key up.
    fn is_extracted(&self, labels: &[(Cow<'a, str>, Cow<'a, str>)], key: &str) -> bool {
        self.is_extracted_at(key, contains_label(labels, key))
    }

    /// Book-keeping for one `Set(ParsedLabel, key, …)`, returning the key
    /// so the caller can go on to `Set` it: only a key the structured
    /// metadata ALSO supplies needs recording, and only to say the vector
    /// entry under that name is no longer the metadata one. Takes the key
    /// by value because the recording path keeps a copy that has to live
    /// as long as the row, which a borrow of the caller's local cannot.
    fn note_parsed_set(&mut self, key: Cow<'a, str>) -> Cow<'a, str> {
        if self.sm_has(&key) && !self.parsed_over_sm.contains(&key) {
            self.parsed_over_sm.push(key.clone());
        }
        key
    }

    /// Book-keeping for one `Del(name)`: a name that WAS extracted stays
    /// extracted, so it has to survive leaving the vector.
    fn note_removed(&mut self, was_extracted: bool, name: Cow<'a, str>) {
        if was_extracted && !self.removed_parsed.contains(&name) {
            self.removed_parsed.push(name);
        }
    }
}

/// Adds a parser-extracted label the way the reference's parsers do
/// (issue #334): sanitize, rename to `<key>_extracted` iff the ORIGINAL
/// stream or a LIVE structured-metadata entry holds the name, then — for
/// an implicit parser — drop the extraction entirely if that resolved
/// name was already extracted on this line, and otherwise `Set` it
/// (an upsert, so an expression parser's repeat wins the slot).
///
/// Allocation-lean: an already-valid key that does not collide passes
/// through as-is (borrowed where the caller borrowed it) — sanitization
/// and the collision rename are the only allocating paths, and the
/// book-keeping allocates only for a key the structured metadata also
/// supplies.
///
/// A `Set` marks the builder `dirty` (`labels.go:216-222`; issue #238); a
/// SKIPPED extraction never reaches `Set` and therefore does not dirty,
/// and a parser that writes nothing (e.g. a failed `json` parse) never
/// reaches here at all.
///
/// `origin` decides whether the name is sanitized — see [`KeyOrigin`].
fn add_extracted<'a>(
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    st: &mut ExtractionState<'a, '_>,
    key: Cow<'a, str>,
    origin: KeyOrigin,
    value: Cow<'a, str>,
    mode: OnAlreadyExtracted,
    dirty: &mut bool,
) {
    let sanitized: Cow<'a, str> = if origin == KeyOrigin::Line && key_needs_sanitizing(&key) {
        Cow::Owned(sanitize_label_key(&key))
    } else {
        key
    };
    // ONE lookup on the common (unrenamed) path — see
    // `ExtractionState::is_extracted_at`. A rename costs a second one,
    // and only fires for a name the stream or the metadata supplies.
    let at = label_position(labels, &sanitized);
    let (resolved, at) = if st.collides_at(&sanitized, at.is_some()) {
        let renamed: Cow<'a, str> = Cow::Owned(format!("{sanitized}{DUPLICATE_SUFFIX}"));
        let at = label_position(labels, &renamed);
        (renamed, at)
    } else {
        (sanitized, at)
    };
    if mode == OnAlreadyExtracted::Skip && st.is_extracted_at(&resolved, at.is_some()) {
        return;
    }
    *dirty = true;
    let resolved = st.note_parsed_set(resolved);
    put_label_at(labels, at, resolved, value);
}

/// Where an extracted label's NAME came from, which is what decides
/// whether it is sanitized (issue #392).
///
/// **The reference sanitizes only the first.** A key read out of the LINE
/// goes through `sanitizeLabelKey`; the IDENTIFIER a user writes in the
/// query as an extraction destination does not — `NewLogfmtExpressionParser`
/// merely VALIDATES it (`model.UTF8Validation.IsValidLabelName(exp.Identifier)`,
/// `pkg/logql/log/parser.go:518 @ v3.7.4`) and then uses it verbatim as
/// the map key it later `Set`s.
///
/// Measured on the pinned v3.7.4 container over the line `ax=7 bx=8`:
/// `sum by (éx) (count_over_time({…} | logfmt éx="ax" [1m]))` returns
/// `{"metric":{"éx":""}}` — the label is named `éx` — while
/// `sum by (_x) (…)` over the SAME query returns `{"metric":{}}`, so the
/// sanitized spelling does not exist there.
///
/// **This is a no-op for every query that parsed before #392.** A
/// query-side destination was `[A-Za-z_][A-Za-z0-9_]*` by construction
/// until #392 widened the lexer, and `key_needs_sanitizing` is false for
/// every such name — pinned by
/// `sanitizing_a_query_destination_was_always_a_no_op_for_ascii_identifiers`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyOrigin {
    /// A key decoded from the LOG LINE — logfmt/json/regexp/pattern keys.
    /// Sanitized, mirroring the reference's own line-key sanitizer.
    Line,
    /// An IDENTIFIER the user wrote in the QUERY as an extraction
    /// destination (`| logfmt <id>="…"`, `| json <id>="…"`). Used
    /// verbatim.
    QueryIdentifier,
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

/// Two-valued label-filter evaluation — `true` keep, `false` drop —
/// with the error state carried LEFT TO RIGHT through the program, the
/// way the reference carries it on its `LabelsBuilder`.
///
/// **Issue #248 round 2: this used to be three-valued, and that was the
/// defect.** A numeric conversion failure returned `None`, the Kleene
/// tables propagated it, and the caller committed `__error__` only on a
/// surviving `None`. The reference has no third value: a failing
/// numeric-family filter calls `SetErr` right there and returns **true**
/// (`pkg/logql/log/label_filter.go:182-188` bytes, `:250-256` duration,
/// `:313-319` numeric @ v3.7.4). Deferring the commit made the error
/// INVISIBLE to every later leaf that reads it in the same filter —
/// `ip()`, a malformed `ip()`, and `__error__` itself — and dropped
/// lines the reference keeps. Measured on `grafana/loki:3.7.4` over a
/// line `n=bad n2=5 addr=1.2.3.4 b=y`:
///
/// | `| logfmt |` filter | reference | pre-fix PulsusDB |
/// |---|---|---|---|
/// | `n > 1 and addr=ip("nope")` | kept, `__error__` | dropped |
/// | `n > 1 and addr=ip("10.0.0.0/8")` | kept, `__error__` | dropped |
/// | `n > 1 and __error__ = "LabelFilterErr"` | kept, `__error__` | dropped |
/// | `n > 1 or n2 > 1` | kept, `__error__` | kept, NO error |
///
/// `failed` therefore no longer means "maybe an error": it IS the error,
/// recorded the instant it happens, and `has_err` below reads it. What
/// stays deferred is only the owned detail `String`, which the caller
/// builds on the KEPT path — the reference's own error is unobservable
/// on a dropped line (the builder is discarded), so a dropped line still
/// allocates nothing.
///
/// **`or` short-circuits, `and` does not** — `BinaryLabelFilter.Process`
/// returns early only at `if !b.And && lok` (`label_filter.go:90-98`).
/// That asymmetry is load-bearing precisely because of the error: the
/// skipped operand never gets to `SetErr`. See
/// [`CompiledLabelFilter::or_jumps`].
///
/// Issue #272: a LINEAR SCAN over the flat post-order program, which
/// visits the leaves in SOURCE order — the order the reference's
/// left-to-right recursion visits them in — so `failed` records the
/// leftmost EVALUATED conversion failure, which is what
/// `if !lbs.HasErr()` makes first-wins.
///
/// **Per-row cost.** A contiguous `Vec<LfOp>` instead of `Box`-pointer
/// chasing plus a call frame per node, and an on-stack
/// `[bool; LF_INLINE_STACK]` verdict array instead of any allocation. A
/// left-deep chain — what the parser builds for `a or b or c …` — has
/// `max_stack == 2` regardless of width, so width never spills to the
/// heap. The short-circuit strictly REMOVES work.
fn eval_label_filter<'v>(
    filter: &CompiledLabelFilter,
    labels: &'v [(Cow<'_, str>, Cow<'_, str>)],
    errs: &ErrorSlots<'_>,
    failed: &mut Option<(UnitKind, &'v str)>,
) -> bool {
    let mut inline = [false; LF_INLINE_STACK];
    // Unreachable for any parser-produced filter (see
    // `LF_INLINE_STACK`); retained for programmatically constructed
    // trees, which the corpus runner and `extended_with` can both carry.
    // `max_stack <= ops.len() <= N`, itself bounded by admission.
    let mut spill: Vec<bool> = if filter.max_stack as usize > LF_INLINE_STACK {
        vec![false; filter.max_stack as usize]
    } else {
        Vec::new()
    };
    let vals: &mut [bool] = if spill.is_empty() {
        &mut inline
    } else {
        &mut spill
    };
    // `lbs.HasErr()` as the reference reports it at the START of this
    // stage. An error already set by an earlier stage BLOCKS this
    // filter's own `SetErr` (`if !lbs.HasErr()`, first-wins) and is what
    // the error-reading leaves see even before this filter fails.
    let pre_err = errs.has_err();
    let mut top = 0usize;
    let mut i = 0usize;
    while i < filter.ops.len() {
        // `lbs.HasErr()` AS OF THIS OP — the whole point of the round-2
        // fix. Recomputed per op because a `Compare` to its left may have
        // set it since the last read.
        let has_err = pre_err || failed.is_some();
        let v = match &filter.ops[i] {
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
                    // `GetErr()` reads the slot the reference has ALREADY
                    // written, including one written by a `Compare` to the
                    // left of this leaf in this same filter — measured:
                    // `| logfmt | n > 1 and __error__ = "LabelFilterErr"`
                    // returns the line with `n=bad`.
                    if pre_err {
                        errs.err_str()
                    } else if failed.is_some() {
                        LABEL_FILTER_ERROR
                    } else {
                        ""
                    }
                } else {
                    get_label(labels, name).unwrap_or("")
                };
                match op {
                    MatchOp::Eq => v == value,
                    MatchOp::Neq => v != value,
                    MatchOp::Re => re.as_ref().is_some_and(|re| re.is_match(v)),
                    MatchOp::Nre => !re.as_ref().is_some_and(|re| re.is_match(v)),
                }
            }
            LfOp::Compare {
                name,
                op,
                kind,
                threshold,
            } => match get_label(labels, name) {
                // A missing label never satisfies a numeric comparison —
                // `lbs.Get` failing returns `false` with NO error
                // (`label_filter.go:176-180`, `:244-248`, `:307-311`).
                None => false,
                Some(raw) => match convert_label_value(*kind, raw) {
                    None => {
                        // The reference SETS the error here and returns
                        // TRUE. We record the same first-wins choice as a
                        // BORROW of the offending value — no allocation on
                        // this path, and none at all if the line is later
                        // dropped, which is exactly when the reference's
                        // own error becomes unobservable.
                        if failed.is_none() {
                            *failed = Some((*kind, raw));
                        }
                        true
                    }
                    Some(v) => match op {
                        CompareOp::Eq => v == *threshold,
                        CompareOp::Neq => v != *threshold,
                        CompareOp::Gt => v > *threshold,
                        CompareOp::Gte => v >= *threshold,
                        CompareOp::Lt => v < *threshold,
                        CompareOp::Lte => v <= *threshold,
                    },
                },
            },
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
                if has_err {
                    true
                } else {
                    // Reference v3.7.3 semantics (differential-authoritative): parse the
                    // label value as an IP and test range membership. A missing label OR
                    // an unparseable value is `match = false` — NEVER an error, so no
                    // `__error__`/`__error_details__` is set (this is the key divergence
                    // from the numeric label filter, which DOES error on bad values).
                    // `=` returns the line iff matched; `!=` iff not matched.
                    let matched = get_label(labels, name)
                        .and_then(|raw| raw.parse::<IpAddr>().ok())
                        .is_some_and(|ip| matcher.contains(&ip));
                    if *negated { !matched } else { matched }
                }
            }
            // Issue #248: a malformed pattern the reference did not reject.
            // `ip.go:123-145 @ v3.7.4` checks `HasErr` first (pass), then the
            // label, then `f.ip == nil` — all BEFORE the `=`/`!=` switch, so
            // the verdict is `true` on an errored entry and `false` on every
            // other, for both operators. Container-measured on
            // `grafana/loki:3.7.4`: `| logfmt | addr != ip("nope") or b="y"`
            // returns only the `b="y"` line, so `!=` does NOT negate to true;
            // and `| logfmt | n > 1 and addr=ip("nope")` over `n=bad` returns
            // the line, which is why `has_err` and not `pre_err`.
            LfOp::IpMalformed => has_err,
            LfOp::And => {
                // Post-order leaves [.., lhs, rhs] on the tail; BOTH were
                // evaluated, in source order, before this op runs — `and`
                // never short-circuits in the reference either.
                let rhs = vals[top - 1];
                let lhs = vals[top - 2];
                top -= 2;
                lhs && rhs
            }
            LfOp::Or => {
                // Reached only when the left operand was FALSE: a true one
                // jumped past this op (see `or_jumps`).
                let rhs = vals[top - 1];
                let lhs = vals[top - 2];
                top -= 2;
                lhs || rhs
            }
        };
        vals[top] = v;
        top += 1;
        if v && let Some(target) = filter.or_jump(i) {
            // The skipped region is exactly this `or`'s right operand plus
            // the `Or` op(s) it feeds, which together consume one verdict
            // and push one — so leaving the left operand's `true` in place
            // keeps the stack exactly as the skipped ops would have.
            i = target;
            continue;
        }
        i += 1;
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

/// The per-ROW ceiling on the bytes of flattened label KEYS a bare
/// `| json` may build (issue #287). **Value: 64 MiB.**
///
/// **What it bounds, exactly:** the sum of
/// [`super::charge::alloc_block_bytes`]`(key.len())` over every key
/// string [`flatten_json`] allocates while the row is in the pipeline —
/// the emitted leaf label names AND the intermediate object prefixes
/// they are built from, across ALL `| json` stages of the row — plus,
/// when json-path capture is on (issue #254; only `/detected_fields`
/// turns it on), everything [`JsonPaths::record`] allocates: each raw
/// path component, the per-leaf path spine and the slot table. The
/// capture is on the ledger because it has its own `Θ(L²)` axis the key
/// charges cannot see — an ancestor that trims to whitespace costs zero
/// key bytes and full path bytes per descendant leaf (see
/// [`JsonPaths`]). It bounds nothing else: not the extracted VALUES
/// (their bytes are
/// linear in the line — each scalar's text appears once in the input),
/// not the parsed `serde_json::Value`, not the targeted-extraction form
/// `| json a="b.c"` (whose label names come from the compiled stage and
/// are never derived from the line). Charged BEFORE each key
/// allocation, and never released within the row.
///
/// The charge is the retained BLOCK, not the request (review round 1,
/// finding 1 as re-judged in round 2): `String::with_capacity(n)`
/// guarantees `capacity >= n` and nothing more, so a bare-length charge
/// would have left the allocator's slop uncounted.
/// `alloc_block_bytes` is this crate's pinned over-approximation of
/// that block for ONE exactly-reserved allocation — the shape
/// [`flatten_json`] builds. Above its 32-byte floor it is `2 x`, so the
/// 64 MiB ceiling admits **32 MiB of key CONTENT** per row.
///
/// **Why a BYTE bound and not a key COUNT.** The reference builds each
/// leaf label name by joining the whole ancestor path
/// (grafana/loki v3.7.4, `pkg/logql/log/parser.go` —
/// `JSONParser.parseLabelValue` interns
/// `buildSanitizedPrefixFromBuffer()`'s `_`-joined prefix buffer), and
/// PulsusDB emits the identical names, so this expansion is PARITY and
/// the names must not change. But it makes the emitted key bytes grow
/// as `Θ(L²)` in the line length `L`: for `{"<p bytes>":{"k00000":0, …
/// ×m}}` the input is `p + 11m + 6` bytes and the keys are `m·(p + 7)`
/// bytes, maximised at `≈ L²/44`. Measured: a 65 536-byte line yields
/// 97 615 872 key bytes (1 489.5×), and 1 MiB extrapolates to ~23.3 GiB.
/// A cap on the NUMBER of keys is therefore not a bound at all — the
/// 64 KiB construction emits only 2 979 of them.
///
/// The reference is UNBOUNDED here (no cap in `JSONParser`, none in
/// `LabelsBuilder.Set`, `pkg/logql/log/labels.go:344`) — the same shape
/// as [`super::template::MAX_TEMPLATE_RENDER_BYTES`]: a bounded 422 is
/// the ruled behaviour where the reference OOMs. The ceiling is the
/// template budget's, for the template budget's reason ("one line may
/// not out-allocate a whole query's retained state"); at 64 MiB of
/// charged blocks — 32 MiB of key content — the worst-SHAPED line it
/// refuses is ~38 KiB, while a normally-shaped line, whose key bytes
/// are a small multiple of `L`, passes at tens of MiB.
pub const MAX_JSON_FLATTEN_KEY_BYTES: u64 = 64 * 1024 * 1024;

/// The same compile-time inequality the template budget carries: one
/// line's key expansion may never out-allocate a whole query's
/// retained-state budget.
const _: () = assert!(
    MAX_JSON_FLATTEN_KEY_BYTES <= super::charge::MAX_CLIENT_AGG_GROUP_BYTES,
    "one line's json key expansion may not out-allocate a whole query's retention budget"
);

/// The countdown ledger [`flatten_json`] charges against, fresh per ROW
/// and SHARED by every `| json` stage the row runs — the issue #260
/// lesson applied to the parser axis: a per-STAGE ledger would bound one
/// stage while the NUMBER of stages is bounded only by the query-text
/// cap, and each `| json` after the first re-flattens the same line into
/// a fresh set of `_extracted`-suffixed labels that are all live at once.
///
/// One `Cell` on the stack, like [`template::RenderBudget`] — no
/// allocation, so the per-row allocation gates are untouched — and
/// interior mutability so the charge is callable behind a shared borrow
/// while the row's label vector is mutably borrowed.
#[derive(Debug)]
struct JsonKeyBudget {
    remaining: Cell<u64>,
}

impl Default for JsonKeyBudget {
    fn default() -> Self {
        JsonKeyBudget {
            remaining: Cell::new(MAX_JSON_FLATTEN_KEY_BYTES),
        }
    }
}

impl JsonKeyBudget {
    /// Charges the heap BLOCK a `content_bytes`-long key will retain,
    /// BEFORE the string holding it exists. Refuses on the ledger, so a
    /// breach means the key was never allocated.
    ///
    /// The conversion from content length to retained block lives HERE
    /// and nowhere else, so no caller can charge a bare length (issue
    /// #287 review round 1, finding 1 as re-judged in round 2: `len` is
    /// the request size, and `String::with_capacity(len).capacity()` is
    /// only guaranteed `>= len` — the allocator's slop on top of the
    /// request is real and has to be inside the charge).
    /// [`super::charge::alloc_block_bytes`] is this crate's pinned
    /// over-approximation of that block for ONE exactly-reserved
    /// allocation, which is exactly the shape the caller builds.
    fn charge_key(&self, content_bytes: usize) -> Result<(), RowBudgetExceeded> {
        self.charge_block(super::charge::alloc_block_bytes(content_bytes as u64))
    }

    /// Charges a block a caller derived through a DIFFERENT pinned model
    /// than [`JsonKeyBudget::charge_key`]'s exactly-reserved one — today
    /// only [`JsonPaths::record`]'s geometrically-grown slot spine, via
    /// [`super::charge::grown_alloc_bytes`]. Charging a bare CONTENT
    /// length through here is the mistake `charge_key` exists to prevent,
    /// so callers pass a block, never a length (the same split
    /// `detected.rs`'s `field_entry_bytes` already uses).
    fn charge_block(&self, block: u64) -> Result<(), RowBudgetExceeded> {
        let remaining = self.remaining.get();
        if block > remaining {
            return Err(RowBudgetExceeded::json_flatten_keys());
        }
        self.remaining.set(remaining - block);
        Ok(())
    }
}

/// The RAW JSON key path behind each label a full-flatten `| json`
/// emitted, addressed by the label's POSITION in the row's label vector.
///
/// This is the reference's `LabelsBuilder.jsonPaths` map
/// (`pkg/logql/log/labels.go:128,415,421 @ v3.7.4`), and like the
/// reference it is **opt-in per run**: only `/detected_fields` builds its
/// json parser with capture on (`NewJSONParser(true)`,
/// `pkg/querier/queryrange/detected_fields.go:410`), while the query path
/// builds it off (`pkg/logql/syntax/ast.go:758`). The query path
/// therefore pays one `Option` branch per emitted leaf and allocates
/// nothing.
///
/// The buffer is CALLER-OWNED so `/detected_fields`' per-row feeder can
/// recycle its spine across sampled rows, exactly as it recycles the
/// label scratch; [`JsonPaths::clear`] keeps that capacity.
///
/// **Everything it allocates is charged to the ROW's
/// [`JsonKeyBudget`] before it exists** (review round 1, medium). The
/// capture is not a free rider on the key charges: a leaf's path is the
/// RAW ancestor chain, and an ancestor that trims to whitespace
/// contributes NOTHING to any label name while contributing its full
/// length to every descendant leaf's path — so `{"<p spaces>":{"k0":0, …
/// ×m}}` allocates `m·p` path bytes against `0` charged key bytes. That
/// is the same `Θ(L²)` shape [`MAX_JSON_FLATTEN_KEY_BYTES`] exists to
/// bound, on an axis the key charges cannot see, so it is priced on the
/// same ledger rather than a new one: a breach is the SAME
/// `RowBudget::JsonFlattenKeys` 422 the `| json` flatten already raises
/// on this endpoint (`tests/logql_json_flatten_budget.rs::
/// the_detected_fields_auto_parse_pass_surfaces_the_breach`), never a new
/// rejection surface.
///
/// **Every owned container in the capture path, and where its charge
/// sits relative to its growth** (review round 2, high — an earlier
/// revision priced three of the four and stated a peak that the fourth
/// falsified; the table is exhaustive so the omission cannot recur
/// silently):
///
/// | container | grows with | charge |
/// |---|---|---|
/// | [`JsonPathCapture::stack`] (`Vec<&str>`) | JSON nesting DEPTH | `grown_alloc_bytes` high-water delta in [`JsonPathCapture::push`], **before** the element lands |
/// | [`JsonPaths::slots`] (`Vec<Option<Vec<String>>>`) | label COUNT | `grown_alloc_bytes` high-water delta in [`JsonPaths::record`], **before** `resize_with` |
/// | the per-leaf `path` spine (`Vec<String>`) | that leaf's depth | `charge_key(parts × size_of::<String>())`, **before** `Vec::with_capacity` |
/// | each path component (`String`) | that component's length | `charge_key(part.len())`, **before** `to_string()` |
///
/// There is no fifth: `stack` holds `&str` borrowed from the parsed
/// value (only its spine allocates), `slots`' elements are the `path`
/// vectors already in the table, and `JsonPathCapture` itself is a stack
/// local. All four charge the ROW's [`JsonKeyBudget`].
///
/// **Peak live set, stated for exactly what the ledger covers.** The
/// four containers above plus [`flatten_json`]'s key strings are all
/// deducted from ONE 64 MiB countdown of `alloc_block_bytes`-rounded
/// blocks, so their combined live heap for a row is
/// `≤ MAX_JSON_FLATTEN_KEY_BYTES`. Across rows it is one row at a time
/// (`DetectedRowFeeder` streams), and the accumulator's SURVIVING copy is
/// charged separately against the per-REQUEST
/// `MAX_DETECTED_FIELD_BYTES` — the same row-ledger/request-ledger split
/// the keys and the retained values already use.
///
/// What this ledger does **not** bound, unchanged by this change and NOT
/// claimed: the row's label vector spine and the parsed
/// `serde_json::Value`. Both predate the capture and sit outside issue
/// #287's model, which prices key STRINGS.
///
/// **That residual is owned by issue #241** (formerly #265), so it
/// carries an owning issue rather than sitting as an orphan note. Its
/// settled shape, established by reading rather than assumed: it is
/// **per-ROW, not query-lifetime** — one row is live at a time on this
/// path, so nothing here accumulates across a scan — and the half that
/// IS charged is charged already, since
/// [`MAX_JSON_FLATTEN_KEY_BYTES`] covers both the flatten's key strings
/// and all four capture containers tabulated above. What remains
/// uncharged is the pair named in the previous paragraph and nothing
/// else.
#[derive(Debug, Default)]
pub(crate) struct JsonPaths {
    /// Indexed by position in the label vector; `None` for a label the
    /// flatten did not produce (a base label, or a leaf that was skipped).
    slots: Vec<Option<Vec<String>>>,
    /// Slot-spine block bytes already charged for the CURRENT run, so a
    /// growing table is charged its DELTA rather than its total once per
    /// leaf. Reset by [`JsonPaths::clear`], which every `| json` stage
    /// calls on entry — a second stage re-charges from zero, which
    /// over-charges and never under-charges.
    charged_slots: u64,
}

impl JsonPaths {
    /// Drops the previous row's paths, keeping the spine's capacity.
    pub(crate) fn clear(&mut self) {
        self.slots.clear();
        self.charged_slots = 0;
    }

    /// The raw path for the label at `idx`, or `None` when the flatten
    /// captured none for it — the reference's `GetJSONPath` miss.
    pub(crate) fn get(&self, idx: usize) -> Option<&[String]> {
        self.slots.get(idx)?.as_deref()
    }

    /// Records `stack ++ [leaf]` for the label now at `idx`, overwriting
    /// any earlier path at that position (the reference's `SetJSONPath`
    /// map write, `labels.go:415`).
    ///
    /// Charge-then-allocate, in three steps, so NOTHING grows before it
    /// is priced: the slot table's geometric growth
    /// ([`super::charge::grown_alloc_bytes`], delta only), then the path
    /// vector's exactly-reserved spine, then each component's `String`
    /// immediately before it is cloned. A refusal returns with the
    /// offending allocation never made; the partially-built `path` is a
    /// local that drops on the way out, and the row aborts to the
    /// documented 422 (a partial label set is never observed —
    /// [`flatten_json`]'s own contract).
    fn record(
        &mut self,
        idx: usize,
        stack: &[&str],
        leaf: &str,
        budget: &JsonKeyBudget,
    ) -> Result<(), RowBudgetExceeded> {
        if self.slots.len() <= idx {
            let want = super::charge::grown_alloc_bytes(
                ((idx + 1) * size_of::<Option<Vec<String>>>()) as u64,
            );
            if want > self.charged_slots {
                budget.charge_block(want - self.charged_slots)?;
                self.charged_slots = want;
            }
            self.slots.resize_with(idx + 1, || None);
        }
        let parts = stack.len() + 1;
        budget.charge_key(parts * size_of::<String>())?;
        let mut path = Vec::with_capacity(parts);
        for part in stack.iter().copied().chain(std::iter::once(leaf)) {
            budget.charge_key(part.len())?;
            path.push(part.to_string());
        }
        debug_assert_eq!(path.len(), parts, "the charged spine must be the built one");
        self.slots[idx] = Some(path);
        Ok(())
    }
}

/// The walk-local half of the capture: the RAW key parts of the objects
/// currently open, mirroring the reference's `JSONParser.prefixBuffer`
/// (`pkg/logql/log/parser.go:107-115,128-135 @ v3.7.4`) — pushed on
/// entering an object, popped on leaving it (`parseObject`'s explicit
/// `j.prefixBuffer = j.prefixBuffer[:prefixLen]` rollback). Parts that
/// trim EMPTY are pushed too: `buildSanitizedPrefixFromBuffer` skips them
/// when building the label name, `buildJSONPathFromPrefixBuffer` does
/// not, so `{"x":{"":1}}` is label `x` with path `["x",""]`.
///
/// `stack` is an OWNED container that grows with JSON nesting depth,
/// which the line controls, so it is charged like every other one — see
/// [`JsonPathCapture::push`] and the container table on [`JsonPaths`].
/// It holds `&'v str` borrowed from the parsed value, so only the SPINE
/// ever allocates.
#[derive(Debug)]
struct JsonPathCapture<'v, 'o> {
    stack: Vec<&'v str>,
    /// High-water block bytes already charged for `stack`. `Vec` never
    /// shrinks on `pop`, so charging the peak once — rather than per
    /// push — is both sound and free of double-charging when the walk
    /// re-descends to a depth it has already paid for.
    charged_stack: u64,
    out: &'o mut JsonPaths,
}

impl<'v> JsonPathCapture<'v, '_> {
    /// Charge-then-push: the spine's geometric growth is priced BEFORE
    /// the element lands, so a refusal returns with the stack never
    /// having grown.
    fn push(&mut self, part: &'v str, budget: &JsonKeyBudget) -> Result<(), RowBudgetExceeded> {
        let want =
            super::charge::grown_alloc_bytes(((self.stack.len() + 1) * size_of::<&str>()) as u64);
        if want > self.charged_stack {
            budget.charge_block(want - self.charged_stack)?;
            self.charged_stack = want;
        }
        self.stack.push(part);
        Ok(())
    }

    /// Leaving an object (`parseObject`'s rollback). Never refunds: the
    /// ledger is a countdown over PEAK blocks and the spine keeps its
    /// capacity.
    fn pop(&mut self) {
        self.stack.pop();
    }
}

/// A JSON value that keeps an object's fields in WIRE ORDER and keeps
/// every one of a repeated key's occurrences (issue #334).
///
/// `serde_json::Value` can do neither: without the `preserve_order`
/// feature its object is a `BTreeMap`, so fields arrive SORTED and a
/// repeated key silently keeps only the last occurrence. That was
/// invisible while a colliding key produced a second `<key>_extracted`
/// label, and becomes observable the moment the FIRST extraction of a
/// name wins — `{"a.b":2,"a-b":1}` is `a_b="2"` at the container and
/// `a_b="1"` under a sorted walk, and `{"a":1,"a":2}` is `a="1"` at the
/// container and unreachable under a de-duplicating one. The reference
/// walks the raw bytes with `jsonparser.ObjectEach`
/// (`pkg/logql/log/parser.go:82 @ v3.7.4`), which is document order with
/// every occurrence delivered — captured at v3.7.4 for both orders and
/// for `{"a":{"b":1},"a":{"c":2}}`, which yields BOTH `a_b` and `a_c`.
///
/// The TARGETED form needs it too (issue #334 review round 1): the
/// reference resolves `| json a="p", a="q"` with `jsonparser.EachKey`
/// (`parser.go:692 @ v3.7.4`), which walks the DOCUMENT and fires a
/// callback per matched path as it passes it, so the winner is decided
/// by document order and not by the order the expressions were written
/// — `{"q":2,"p":1}` gives `a="1"` at the container. See
/// [`run_json_targets`].
///
/// Scalars stay `serde_json::Value` so their pinned rendering
/// (`float_roundtrip` number formatting included) is untouched, and an
/// object reached by a targeted path is converted back
/// ([`wire_json_to_value`]) so ITS rendering is untouched too.
///
/// **Depth is bounded by the parser, not by this type.** `serde_json`'s
/// deserializer refuses a document nested past its own recursion limit,
/// so no line can build a `WireJson` deeper than that and neither its
/// `Drop` nor the walks over it can run away. That is the bound
/// `serde_json::Value` already carried here before this type existed,
/// and it is why `| json` keeps an ordinary recursive tree instead of
/// #272's iterative-dismantle machinery, which exists for the query AST
/// — bounded only by the query-text cap.
///
/// The bound is a DEPENDENCY's, not ours, so it is not left as a
/// measurement in a comment: `tests/recursion_census.rs` carries the
/// exemption that rests on it and, beside it,
/// `the_depth_bound_the_exemption_rests_on_is_enforced_by_the_parser`,
/// which fails if the limit moves in either direction or disappears.
#[derive(Debug)]
enum WireJson {
    /// Fields in the order the document listed them, duplicates included.
    Object(Vec<(String, WireJson)>),
    /// Elements in order. A targeted `[i]` path may index into an array
    /// and continue into an object, so the order and the duplicates
    /// inside one are observable exactly as they are anywhere else —
    /// `qq="arr[2].k", qq="arr[0].k", qq="arr[1].k"` resolves by ASCENDING
    /// INDEX, and a repeated key inside an element resolves to its first
    /// occurrence. Round 2 of this issue left arrays as
    /// `serde_json::Value` and got both wrong.
    Array(Vec<WireJson>),
    /// Any scalar, `serde_json`'s own value so its pinned rendering
    /// (`float_roundtrip` number formatting included) is untouched.
    Leaf(serde_json::Value),
}

struct WireJsonVisitor;

impl<'de> serde::de::Visitor<'de> for WireJsonVisitor {
    type Value = WireJson;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any valid JSON value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<WireJson, E> {
        Ok(WireJson::Leaf(serde_json::Value::Bool(v)))
    }

    fn visit_i64<E>(self, v: i64) -> Result<WireJson, E> {
        Ok(WireJson::Leaf(serde_json::Value::from(v)))
    }

    fn visit_u64<E>(self, v: u64) -> Result<WireJson, E> {
        Ok(WireJson::Leaf(serde_json::Value::from(v)))
    }

    fn visit_f64<E>(self, v: f64) -> Result<WireJson, E> {
        Ok(WireJson::Leaf(serde_json::Value::from(v)))
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<WireJson, E> {
        Ok(WireJson::Leaf(serde_json::Value::String(v.to_owned())))
    }

    fn visit_string<E>(self, v: String) -> Result<WireJson, E> {
        Ok(WireJson::Leaf(serde_json::Value::String(v)))
    }

    fn visit_none<E>(self) -> Result<WireJson, E> {
        Ok(WireJson::Leaf(serde_json::Value::Null))
    }

    fn visit_some<D: serde::Deserializer<'de>>(self, d: D) -> Result<WireJson, D::Error> {
        d.deserialize_any(self)
    }

    fn visit_unit<E>(self) -> Result<WireJson, E> {
        Ok(WireJson::Leaf(serde_json::Value::Null))
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<WireJson, A::Error> {
        let mut out = Vec::new();
        while let Some(v) = seq.next_element::<WireJson>()? {
            out.push(v);
        }
        Ok(WireJson::Array(out))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<WireJson, A::Error> {
        // `push`, never `insert`: document order, duplicates kept.
        let mut out = Vec::new();
        while let Some(entry) = map.next_entry::<String, WireJson>()? {
            out.push(entry);
        }
        Ok(WireJson::Object(out))
    }
}

impl<'de> serde::Deserialize<'de> for WireJson {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(WireJsonVisitor)
    }
}

/// The FIRST JSON value in `line`, with whatever follows it ignored —
/// issue #389 part A.
///
/// `serde_json::from_str` is `Deserializer::from_str` plus `end()`, and
/// that `end()` is the whole difference: it demands end-of-input after
/// the value, so `{"a":1}trailing` is a parse error here where the
/// reference answers `a="1"`. The reference's scanner simply stops.
/// `jsonparser.ObjectEach` returns `nil` the moment it reaches the
/// object's closing `}` and never looks further
/// (`vendor/github.com/grafana/jsonparser/parser.go:1108-1112,1155-1160
/// @ v3.7.4`), and `EachKey`'s dispatch has no default case, so a byte it
/// does not recognise is skipped rather than refused (`:568-577`).
/// Dropping `end()` reproduces that for a line whose trailing bytes come
/// AFTER a complete value; a line malformed INSIDE the value is still
/// refused here and is not (the residual ledgered as
/// `json-nonvalidating-scan-residual`).
///
/// The recursion bound is unchanged: this is the same deserializer with
/// the same limit, reached by a different spelling — see [`WireJson`] and
/// `tests/recursion_census.rs`.
fn parse_wire_json_prefix(line: &str) -> Result<WireJson, serde_json::Error> {
    let mut de = serde_json::Deserializer::from_str(line);
    serde::Deserialize::deserialize(&mut de)
}

/// Owned key/value output by design: extracted values live inside the
/// per-line parsed value, which drops at the end of this stage — the
/// parse itself dominates the cost (bounded to pushdown-surviving rows).
///
/// `budget` is the ROW's ([`MAX_JSON_FLATTEN_KEY_BYTES`]); only the
/// full-flatten arm spends from it, since only that arm derives label
/// names from the line.
///
/// `paths` is the opt-in [`JsonPaths`] sink (see its doc): it is CLEARED
/// on entry — including on the malformed-line early return, so a failed
/// json attempt never leaves a previous attempt's paths visible — and
/// only the full-flatten arm writes to it, matching the reference, whose
/// `SetJSONPath` calls live exclusively in `JSONParser.parseLabelValue`
/// and never in `JSONExpressionParser` (`pkg/logql/log/parser.go:161,196
/// @ v3.7.4`).
fn run_json<'a>(
    line: &str,
    extractions: &'a [(String, Vec<JsonPathSeg>)],
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    st: &mut ExtractionState<'a, '_>,
    errs: &mut ErrorSlots<'a>,
    budget: &JsonKeyBudget,
    paths: Option<&mut JsonPaths>,
) -> Result<(), RowBudgetExceeded> {
    let mut paths = paths;
    if let Some(p) = paths.as_deref_mut() {
        p.clear();
    }
    // A non-object top level (or a parse failure) is the malformed class:
    // line kept, error tagged in the out-of-band slots (UNGUARDED — the
    // reference's parser error write has no `HasErr` check,
    // `parser.go:437-444`), detail recorded on both paths. No label is
    // written, so a failed parse does NOT dirty the builder (issue #238 —
    // the details-visibility gate depends on this).
    let malformed = |errs: &mut ErrorSlots<'a>| {
        errs.set_err(Cow::Borrowed("JSONParserErr"));
        errs.set_details(Cow::Borrowed(JSON_ERROR_DETAILS));
    };
    if extractions.is_empty() {
        // The full flatten derives label NAMES from the document, so it
        // needs the wire-order, duplicate-preserving shape (issue #334).
        let parsed: WireJson = match parse_wire_json_prefix(line) {
            Ok(v @ WireJson::Object(_)) => v,
            _ => {
                malformed(errs);
                return Ok(());
            }
        };
        let mut capture = paths.map(|out| JsonPathCapture {
            stack: Vec::new(),
            charged_stack: 0,
            out,
        });
        flatten_json(
            "",
            Depth::Root,
            &parsed,
            &mut FlattenSink {
                labels,
                st,
                dirty: &mut errs.dirty,
            },
            budget,
            capture.as_mut(),
        )?;
    } else {
        // THE TARGETED FORM HAS ITS OWN VALIDITY GATE, and it is not the
        // flatten arm's (issue #389 part A). `JSONExpressionParser.Process`
        // (`pkg/logql/log/parser.go:664-670,726-732 @ v3.7.4`) tests
        // exactly two things before scanning, and neither is "does the
        // line parse":
        //
        // - an EMPTY line returns with no label written at all — not the
        //   missing-path fill, not an error;
        // - `isValidJSONStart` looks at ONE RAW BYTE, `line[0]`, and
        //   whitespace is NOT skipped, so ` {"a":1}` is refused where
        //   `{"a":1}trailing` is admitted.
        //
        // Everything past that gate is the non-validating scan, whose
        // misses are the missing-path fill: `{garbage`, `[1,2]junk` and
        // `"hello"trailing` all answer `a=""` with NO error. Routing this
        // arm through the flatten arm's "must parse to an object" test got
        // all eight of those rows wrong in both directions.
        if line.is_empty() {
            return Ok(());
        }
        // `addErrLabel(errJSON, nil, lbs)` — a NIL error, so `SetErr` runs
        // and `SetErrorDetails` does not (`parser.go:734-742`). The detail
        // slot stays as the previous stage left it.
        if !matches!(line.as_bytes()[0], b'"' | b'{' | b'[') {
            errs.set_err(Cow::Borrowed("JSONParserErr"));
            return Ok(());
        }
        // The targeted form needs the wire-order, duplicate-preserving
        // shape (issue #334 review round 1): its winner is decided by
        // DOCUMENT order, not by the order the expressions were written,
        // and a repeated document key resolves to its FIRST occurrence.
        //
        // A parse failure past the first-byte gate is NOT an error here:
        // the reference's scan finds nothing and the fill writes `""`, so
        // an empty document reproduces it exactly.
        let parsed = parse_wire_json_prefix(line).unwrap_or(WireJson::Object(Vec::new()));
        run_json_targets(line, &parsed, extractions, labels, st, &mut errs.dirty);
    }
    Ok(())
}

/// `| json <id>="<path>", …` — the reference's `JSONExpressionParser`
/// (`pkg/logql/log/parser.go:671-731 @ v3.7.4`), whose whole behaviour is
/// `jsonparser.EachKey`'s (`vendor/github.com/grafana/jsonparser/parser.go:383-495`).
///
/// **It is a DOCUMENT walk, not a loop over the expressions**, and that is
/// the one thing about it worth stating, because writing it the obvious
/// way — resolve each requested path, write in expression order — gets
/// four separate cells wrong. `EachKey` scans the line once and fires the
/// callback the moment it passes a value whose path was asked for, so:
///
/// - **document order decides the winner.** `| json a="p", a="q"` is
///   `a="2"` over `{"p":1,"q":2}` and `a="1"` over `{"q":2,"p":1}` — the
///   expression list is identical, only the line moved. Captured at
///   v3.7.4, both directions, and at three paths.
/// - **each path fires AT MOST ONCE** (`pathFlags[pi]`, `parser.go:466`),
///   so a repeated document key resolves to its FIRST occurrence:
///   `| json a="p"` over `{"p":1,"p":2}` is `a="1"`, and the same holds
///   one level down.
/// - **several expressions naming ONE path all fire**, at that key, in
///   the order they were written (the inner `for pi, p := range paths`),
///   so `| json a="p", b="p"` sets both.
/// - **the missing-path fill is guarded and does not rename.** Only when
///   FEWER callbacks fired than there are expressions does the reference
///   walk the identifiers and `Set(ParsedLabel, id, "")` for each one the
///   label set does not already hold (`parser.go:714-720`) — reading and
///   writing the RAW identifier, with no `_extracted` rename. So
///   `| json a="p", a="nosuch"` over `{"p":1}` keeps `a="1"` rather than
///   blanking it, and `| json a="nosuch"` against a stream label `a`
///   leaves that label alone. Both captured.
///
/// Unlike the implicit parsers this never consults
/// `ParserHint.Extracted`, so [`OnAlreadyExtracted::Overwrite`]: each
/// fired write is a plain `Set`, renamed on a stream/metadata collision
/// like any other parsed label.
fn run_json_targets<'a>(
    line: &str,
    root: &WireJson,
    extractions: &'a [(String, Vec<JsonPathSeg>)],
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    st: &mut ExtractionState<'a, '_>,
    dirty: &mut bool,
) {
    let mut walk = TargetWalk {
        line,
        extractions,
        flagged: vec![false; extractions.len()],
        flagged_count: 0,
        fired: 0,
    };
    let mut stack: Vec<&str> = Vec::with_capacity(4);
    walk.visit(root, &mut stack, labels, st, dirty);
    // `matches < len(j.ids)` (`parser.go:714`): the fill runs only when
    // some expression produced no write at all.
    if walk.fired < extractions.len() {
        for (id, _) in extractions {
            if get_label(labels, id).is_none() {
                *dirty = true;
                let id = st.note_parsed_set(Cow::Borrowed(id.as_str()));
                set_label(labels, id, Cow::Borrowed(""));
            }
        }
    }
}

/// The per-line state of one targeted `| json` stage's document walk —
/// `EachKey`'s `pathFlags`/`pathsMatched` plus the callback count the
/// missing-path fill is gated on.
struct TargetWalk<'a, 'l> {
    /// The RAW line the walked tree was parsed from — issue #389 part B.
    /// A fired extraction that lands on an object or an array hands back
    /// the document's own bytes, so the span has to be resolvable, and it
    /// is resolved LAZILY (only when such an extraction fires) rather
    /// than carried inside every [`WireJson`] node: measured, the eager
    /// shape costs +48% / +103% / +545% on flat / nested / 100-deep
    /// lines, on every `| json` row, for a value only a container
    /// extraction ever reads.
    line: &'l str,
    extractions: &'a [(String, Vec<JsonPathSeg>)],
    /// `pathFlags` — an expression that has already resolved is inert for
    /// the rest of the line, which is what makes a repeated document key
    /// resolve to its FIRST occurrence.
    flagged: Vec<bool>,
    /// `pathsMatched` — when every expression is flagged the reference
    /// returns from the scan; nothing later could match.
    flagged_count: usize,
    /// The reference's `matches`: callbacks that actually FIRED. Lower
    /// than `flagged_count` when a path selected an array element that
    /// turned out not to exist (`EachKey` flags it either way).
    fired: usize,
}

impl<'a> TargetWalk<'a, '_> {
    /// Walks `node`, whose position in the document is `stack`, firing
    /// every expression whose path resolves there.
    fn visit<'v>(
        &mut self,
        node: &'v WireJson,
        stack: &mut Vec<&'v str>,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
        st: &mut ExtractionState<'a, '_>,
        dirty: &mut bool,
    ) {
        if self.flagged_count == self.extractions.len() {
            return;
        }
        match node {
            WireJson::Object(fields) => {
                for (key, value) in fields {
                    stack.push(key);
                    self.fire_exact(stack, value, labels, st, dirty);
                    self.visit(value, stack, labels, st, dirty);
                    stack.pop();
                    if self.flagged_count == self.extractions.len() {
                        return;
                    }
                }
            }
            // `EachKey`'s `case '['` branch: every not-yet-flagged
            // expression whose next segment indexes THIS array is resolved
            // here, in the order the expressions were written, and is
            // flagged whether or not the element exists.
            WireJson::Array(items) => self.fire_indexed(items, stack, labels, st, dirty),
            WireJson::Leaf(_) => {}
        }
    }

    /// Fires every expression whose path is EXACTLY `stack`.
    fn fire_exact(
        &mut self,
        stack: &[&str],
        value: &WireJson,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
        st: &mut ExtractionState<'a, '_>,
        dirty: &mut bool,
    ) {
        for i in 0..self.extractions.len() {
            if self.flagged[i] || !path_is_fields(&self.extractions[i].1, stack) {
                continue;
            }
            self.flagged[i] = true;
            self.flagged_count += 1;
            self.fired += 1;
            let v = self.target_value(i, value);
            self.write(i, v, labels, st, dirty);
        }
    }

    /// One fired extraction's label value — `readValue`'s arms as the
    /// expression parser reaches them (`pkg/logql/log/parser.go:700-706 @
    /// v3.7.4`).
    ///
    /// A scalar renders exactly as it always has. A CONTAINER is the
    /// document's own bytes ([`container_value`]), which is why the walk
    /// carries the raw line at all.
    fn target_value(&self, i: usize, node: &WireJson) -> String {
        match node {
            WireJson::Leaf(v) => json_scalar_to_string(v),
            container => container_value(self.line, &self.extractions[i].1, container),
        }
    }

    /// Fires every expression that indexes the array at `stack`.
    ///
    /// **The outer loop is over ELEMENTS, not over expressions.**
    /// `EachKey`'s array branch collects the wanted indices, then hands
    /// the array to `ArrayEach`, which walks it once in order; only
    /// inside one element does it scan the expressions
    /// (`vendor/github.com/grafana/jsonparser/parser.go:497-540 @
    /// v3.7.4`). So competing indices of one array resolve by ASCENDING
    /// INDEX and the expression list breaks ties only within a single
    /// element — captured at v3.7.4:
    /// `qq="arr[2].k", qq="arr[0].k", qq="arr[1].k"` over three elements
    /// is the THIRD element's value, where writing in expression order
    /// gives the second's.
    fn fire_indexed(
        &mut self,
        items: &[WireJson],
        stack: &[&str],
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
        st: &mut ExtractionState<'a, '_>,
        dirty: &mut bool,
    ) {
        // `arrIdxFlags` — the set of element indices some expression
        // wants. An index past the end is still wanted; it simply never
        // comes up in the walk, and the reference flags those paths at
        // the end of `ArrayEach` having fired nothing.
        let wanted: Vec<(usize, usize)> = (0..self.extractions.len())
            .filter(|&i| !self.flagged[i])
            .filter_map(|i| {
                let path = &self.extractions[i].1;
                if path.len() <= stack.len() || !path_is_fields(&path[..stack.len()], stack) {
                    return None;
                }
                match path[stack.len()] {
                    JsonPathSeg::Index(idx) => Some((idx, i)),
                    JsonPathSeg::Field(_) => None,
                }
            })
            .collect();
        if wanted.is_empty() {
            return;
        }
        for (element, item) in items.iter().enumerate() {
            for &(idx, i) in &wanted {
                if idx != element {
                    continue;
                }
                self.flagged[i] = true;
                self.flagged_count += 1;
                // `searchKeys` on the element, then `if of != -1` — a
                // miss stays flagged but fires no callback.
                let path = &self.extractions[i].1;
                let Some(found) = lookup_wire_path(item, &path[stack.len() + 1..]) else {
                    continue;
                };
                self.fired += 1;
                let v = self.target_value(i, found);
                self.write(i, v, labels, st, dirty);
            }
        }
        // An index past the end resolves to nothing but is still spent.
        for &(idx, i) in &wanted {
            if idx >= items.len() && !self.flagged[i] {
                self.flagged[i] = true;
                self.flagged_count += 1;
            }
        }
    }

    fn write(
        &mut self,
        i: usize,
        value: String,
        labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
        st: &mut ExtractionState<'a, '_>,
        dirty: &mut bool,
    ) {
        add_extracted(
            labels,
            st,
            Cow::Borrowed(self.extractions[i].0.as_str()),
            KeyOrigin::QueryIdentifier,
            Cow::Owned(value),
            OnAlreadyExtracted::Overwrite,
            dirty,
        );
    }
}

/// Whether `path` is exactly the field chain `stack` — the reference's
/// `len(p) == level && equalStr(key, p[level-1]) && sameTree(...)`. An
/// `[i]` segment never satisfies it; those are the array branch's.
fn path_is_fields(path: &[JsonPathSeg], stack: &[&str]) -> bool {
    path.len() == stack.len()
        && path
            .iter()
            .zip(stack)
            .all(|(seg, want)| matches!(seg, JsonPathSeg::Field(name) if name == want))
}

/// The reference's `searchKeys` inside ONE array element
/// (`vendor/github.com/grafana/jsonparser/parser.go:203-320 @ v3.7.4`):
/// it scans left to right and returns at the FIRST full path match, so a
/// repeated key inside the element resolves to its first occurrence.
fn lookup_wire_path<'v>(root: &'v WireJson, path: &[JsonPathSeg]) -> Option<&'v WireJson> {
    let mut cur = root;
    for seg in path {
        cur = match (seg, cur) {
            (JsonPathSeg::Field(name), WireJson::Object(fields)) => {
                &fields.iter().find(|(k, _)| k == name)?.1
            }
            (JsonPathSeg::Index(idx), WireJson::Array(items)) => items.get(*idx)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// A targeted extraction's value, re-rendered from the parsed tree.
///
/// **This is no longer how a container extraction answers** (issue #389
/// part B): the reference writes the value's RAW BYTES, so key order and
/// whitespace survive, and [`container_value`] does that. This is only
/// its fallback for the case where the span search cannot find the node
/// the walk found — which the `debug_assert` in `container_value` says
/// cannot happen, and which must still not panic in release.
fn wire_json_to_string(node: &WireJson) -> String {
    match node {
        WireJson::Leaf(v) => json_scalar_to_string(v),
        container => json_scalar_to_string(&wire_json_to_value(container)),
    }
}

/// A `WireJson` node as the `serde_json::Value` it would have been —
/// needed only by [`wire_json_to_string`] and by [`container_value`]'s
/// debug-build agreement check.
fn wire_json_to_value(node: &WireJson) -> serde_json::Value {
    match node {
        WireJson::Leaf(v) => v.clone(),
        WireJson::Array(items) => {
            serde_json::Value::Array(items.iter().map(wire_json_to_value).collect())
        }
        WireJson::Object(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), wire_json_to_value(v)))
                .collect(),
        ),
    }
}

// ---------------------------------------------------------------------
// issue #389 part B — a targeted extraction that lands on a container
// hands back the DOCUMENT'S OWN BYTES
// ---------------------------------------------------------------------

/// `readValue`'s container arms as the expression parser reaches them
/// (`pkg/logql/log/parser.go:700-706 @ v3.7.4`):
///
/// ```text
/// case jsonparser.Object: lbs.Set(ParsedLabel, key, string(data))
/// default:                lbs.Set(ParsedLabel, key, unescapeJSONString(data))
/// ```
///
/// `data` is `data[offset:endOffset]` out of the line
/// (`vendor/github.com/grafana/jsonparser/parser.go:878-940 @ v3.7.4`),
/// so an OBJECT is copied VERBATIM — keys in document order, duplicates
/// kept, inner whitespace and escape spellings intact — and an ARRAY
/// falls to `default`, which is the same span with
/// [`unescape_json_text`] run over it. Container-captured at v3.7.4:
/// `{"o":{"b":1,  "a":2}}` answers `{"b":1,  "a":2}` there and answered
/// `{"a":2,"b":1}` here; `{"o":["a\"b",  "c"]}` answers `["a"b",  "c"]`.
///
/// **The span is resolved LAZILY, only for a container that actually
/// fired.** Carrying it inside every [`WireJson`] node is the tidy
/// answer and it is O(n·d) — measured at +48% / +103% / +545% on flat /
/// nested / 100-deep lines, on every `| json` row, for a value only this
/// arm reads. Here the cost is paid once per fired container extraction.
///
/// The span search and [`TargetWalk`] are two mechanisms that have to
/// agree on WHICH node the path selects, so every debug build asserts
/// they do. `serde_json::Value` equality is the right relation: it
/// ignores key order, whitespace and escape spelling — precisely the
/// things this function exists to preserve — and still catches a wrong
/// node. A search miss falls back to the old rendering rather than
/// panicking in release.
fn container_value(line: &str, path: &[JsonPathSeg], node: &WireJson) -> String {
    let Some(span) = raw_span_for_path(line, path) else {
        debug_assert!(
            false,
            "the walk fired on a container the span search cannot reach: {path:?} in {line:?}"
        );
        return wire_json_to_string(node);
    };
    debug_assert!(
        span_is_node(span, node),
        "the raw span and the walked node are different documents: {span:?}"
    );
    match node {
        WireJson::Object(_) => span.to_owned(),
        WireJson::Array(_) => unescape_json_text(span),
        // Unreachable: `TargetWalk::target_value` routes leaves to
        // `json_scalar_to_string`. Rendering the node is still the right
        // answer if it ever arrives — the span of a leaf carries its
        // quotes, which a label value must not.
        WireJson::Leaf(_) => wire_json_to_string(node),
    }
}

/// Whether `span` and `node` are the same JSON document — the agreement
/// between the two mechanisms [`container_value`] rests on, asserted in
/// every debug build so the whole hermetic suite checks it.
///
/// It compares in LOCKSTEP rather than by building a value out of each
/// side, and that is deliberate on two counts. It allocates nothing on
/// the paths this arm takes, so `tests/logql_pipeline_alloc.rs` still
/// measures the arm rather than its own invariant check — a
/// `serde_json::Value` on both sides costs more per key than the
/// re-rendering this issue REMOVED, which would have made that gate
/// unable to see the improvement at all. And an in-order walk is
/// STRICTLY STRONGER than `Value` equality here: both sides keep
/// document order and duplicate keys, so `{"a":1,"a":2}` is compared as
/// itself instead of collapsing to its last write on both sides.
/// Whitespace and escape SPELLING are still out of scope for it, as
/// they must be — those are what the span preserves and the node
/// cannot.
fn span_is_node(span: &str, node: &WireJson) -> bool {
    let mut de = serde_json::Deserializer::from_str(span);
    matches!(
        serde::de::DeserializeSeed::deserialize(SameAs(node), &mut de),
        Ok(true)
    )
}

/// [`span_is_node`]'s seed: it is its own visitor, and each container
/// arm recurses with the matching child node. Depth is the NODE's, which
/// `serde_json` already bounded when it built it.
struct SameAs<'n>(&'n WireJson);

impl<'de> serde::de::DeserializeSeed<'de> for SameAs<'_> {
    type Value = bool;

    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> serde::de::Visitor<'de> for SameAs<'_> {
    type Value = bool;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the JSON value the walk resolved to")
    }

    fn visit_bool<E>(self, v: bool) -> Result<bool, E> {
        Ok(matches!(self.0, WireJson::Leaf(serde_json::Value::Bool(b)) if *b == v))
    }

    // The number arms read the leaf's own accessor rather than building a
    // `Value` to compare against: both sides decode the SAME bytes with
    // the same deserializer, so they take the same arm of `Number`, and
    // reading it back costs nothing.
    fn visit_i64<E>(self, v: i64) -> Result<bool, E> {
        Ok(matches!(self.0, WireJson::Leaf(serde_json::Value::Number(n)) if n.as_i64() == Some(v)))
    }

    fn visit_u64<E>(self, v: u64) -> Result<bool, E> {
        Ok(matches!(self.0, WireJson::Leaf(serde_json::Value::Number(n)) if n.as_u64() == Some(v)))
    }

    fn visit_f64<E>(self, v: f64) -> Result<bool, E> {
        // `to_bits`, not `==`: a float is compared for IDENTITY here, and
        // `-0.0 == 0.0` would let a wrong node through.
        Ok(matches!(
            self.0,
            WireJson::Leaf(serde_json::Value::Number(n))
                if n.as_f64().map(f64::to_bits) == Some(v.to_bits())
        ))
    }

    fn visit_str<E>(self, v: &str) -> Result<bool, E> {
        Ok(matches!(self.0, WireJson::Leaf(serde_json::Value::String(s)) if s == v))
    }

    fn visit_none<E>(self) -> Result<bool, E> {
        Ok(matches!(self.0, WireJson::Leaf(serde_json::Value::Null)))
    }

    fn visit_some<D: serde::Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
        d.deserialize_any(self)
    }

    fn visit_unit<E>(self) -> Result<bool, E> {
        Ok(matches!(self.0, WireJson::Leaf(serde_json::Value::Null)))
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<bool, A::Error> {
        let WireJson::Array(items) = self.0 else {
            drain_seq(seq)?;
            return Ok(false);
        };
        let mut agree = true;
        for item in items {
            match seq.next_element_seed(SameAs(item))? {
                Some(true) => {}
                // A shorter document, or a child that disagrees.
                _ => {
                    agree = false;
                    break;
                }
            }
        }
        // A longer document disagrees too, and the rest must be consumed
        // either way: `deserialize_any` checks for the closing bracket
        // once the visitor returns.
        let extra = drain_seq(seq)?;
        Ok(agree && extra == 0)
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<bool, A::Error> {
        let WireJson::Object(fields) = self.0 else {
            drain_map(map)?;
            return Ok(false);
        };
        let mut agree = true;
        for (key, value) in fields {
            match map.next_key_seed(KeyIs(key))? {
                Some(true) => {
                    if !map.next_value_seed(SameAs(value))? {
                        agree = false;
                        break;
                    }
                }
                Some(false) => {
                    map.next_value::<serde::de::IgnoredAny>()?;
                    agree = false;
                    break;
                }
                None => {
                    agree = false;
                    break;
                }
            }
        }
        let extra = drain_map(map)?;
        Ok(agree && extra == 0)
    }
}

/// Consumes the rest of a sequence, returning how many elements were
/// left. Every visitor that stops early owes `serde_json` this.
fn drain_seq<'de, A: serde::de::SeqAccess<'de>>(mut seq: A) -> Result<usize, A::Error> {
    let mut n = 0;
    while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
        n += 1;
    }
    Ok(n)
}

/// [`drain_seq`] for a map.
fn drain_map<'de, A: serde::de::MapAccess<'de>>(mut map: A) -> Result<usize, A::Error> {
    let mut n = 0;
    while map
        .next_entry::<serde::de::IgnoredAny, serde::de::IgnoredAny>()?
        .is_some()
    {
        n += 1;
    }
    Ok(n)
}

/// The raw span of the value `path` selects in `line`, resolved the way
/// [`TargetWalk`] resolves it.
///
/// The walk is a PRE-ORDER document walk that fires at the first node
/// whose root path is `path`, so the resolution rule is "the first
/// occurrence at each level whose REMAINING path resolves", not a naive
/// first-occurrence descent. Container-captured at v3.7.4, and the two
/// rows a naive descent gets wrong:
///
/// | line | `a="o"` | `a="o.z"` | `a="o[0]"` |
/// |---|---|---|---|
/// | `{"o":{"z":1},"o":[[7],  8]}` | `{"z":1}` | `1` | `[7]` |
/// | `{"o":[1],"o":{"z":{"w":  9}}}` | `[1]` | `{"w":  9}` | `1` |
///
/// A pre-order walk enumerates the candidates for one path in
/// lexicographic order of their per-level occurrence numbers, because an
/// object field's whole subtree is visited before its next sibling — so
/// a depth-first search that tries occurrence 0, 1, 2 … at each level
/// and backtracks reaches the same node first.
///
/// **The `[i]` segment ends the backtracking**, because the walk's array
/// branch does. `EachKey`'s array case resolves the REST of the path
/// inside the chosen element with `searchKeys`, a straight first-match
/// scan, and flags the expression whether or not that succeeds
/// (`vendor/github.com/grafana/jsonparser/parser.go:497-540 @ v3.7.4`;
/// mirrored here by [`lookup_wire_path`]) — so once an array is reached
/// at the path's field prefix, nothing later in the document can win.
///
/// **Iterative on purpose.** The frame stack is a `Vec`: a path is
/// bounded only by the query-text cap (`| json a="a.a.a…"`) and a
/// document by nothing, and `serde_json`'s own value-skipping is
/// iterative too, so no input can drive this into the stack.
fn raw_span_for_path<'l>(line: &'l str, path: &[JsonPathSeg]) -> Option<&'l str> {
    if path.is_empty() {
        return None;
    }
    // Every segment before the first `[i]` is a field — that is what
    // makes `head` the boundary between the two halves.
    let head = path
        .iter()
        .position(|s| matches!(s, JsonPathSeg::Index(_)))
        .unwrap_or(path.len());
    // `frames[d]` is the span the first `d` segments resolved to, paired
    // with the occurrence of segment `d` to try inside it.
    let mut frames: Vec<(&'l str, usize)> = Vec::with_capacity(head + 1);
    frames.push((line, 0));
    loop {
        let (span, occ) = *frames.last()?;
        let d = frames.len() - 1;
        if d < head {
            let JsonPathSeg::Field(name) = &path[d] else {
                return None;
            };
            if let Some(child) = field_nth(span, name, occ) {
                frames.push((child, 0));
                continue;
            }
        } else if head == path.len() {
            // An all-field path: the walk fires at the first node in
            // document order whose root path is `path`, and this is it.
            return Some(span);
        } else {
            let JsonPathSeg::Index(idx) = path[head] else {
                return None;
            };
            // The array branch runs only on an ARRAY node; the walk
            // passes straight over a non-array occurrence and keeps
            // looking, so that is a backtrack and not a miss. A
            // `RawValue` span never carries leading whitespace (measured
            // against serde_json 1.0.150), and this arm's root span is
            // the line, which `isValidJSONStart` has already gated.
            if span.as_bytes().first() == Some(&b'[') {
                return element_at(span, idx).and_then(|e| lookup_raw_path(e, &path[head + 1..]));
            }
        }
        // This frame is spent: back up a level and try the next
        // occurrence of the segment that produced it.
        frames.pop();
        frames.last_mut()?.1 += 1;
    }
}

/// [`lookup_wire_path`] over raw spans — the straight first-match scan
/// the reference runs INSIDE a chosen array element, with no
/// backtracking and a miss at any segment ending it.
fn lookup_raw_path<'l>(root: &'l str, path: &[JsonPathSeg]) -> Option<&'l str> {
    let mut cur = root;
    for seg in path {
        cur = match seg {
            JsonPathSeg::Field(name) => field_nth(cur, name, 0)?,
            JsonPathSeg::Index(idx) => element_at(cur, *idx)?,
        };
    }
    Some(cur)
}

/// The raw span of the value under the `skip`-th (0-based) occurrence of
/// `key` in the object `raw`, in document order. `None` when `raw` is
/// not an object, is malformed, or holds fewer than `skip + 1`
/// occurrences.
///
/// Costs one scan of the object and NO allocation for the value: the
/// value is captured as a `&RawValue` borrowed out of `raw` and every
/// other value is skipped with `IgnoredAny`, which `serde_json` resolves
/// iteratively. The whole object is drained because `deserialize_map`
/// checks for the closing brace after the visitor returns.
fn field_nth<'l>(raw: &'l str, key: &str, skip: usize) -> Option<&'l str> {
    use serde::Deserializer as _;

    struct FieldNth<'k> {
        key: &'k str,
        skip: usize,
    }

    impl<'de> serde::de::Visitor<'de> for FieldNth<'_> {
        type Value = Option<&'de serde_json::value::RawValue>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a JSON object")
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(
            mut self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut out = None;
            while let Some(hit) = map.next_key_seed(KeyIs(self.key))? {
                if hit && out.is_none() {
                    if self.skip == 0 {
                        out = Some(map.next_value::<&serde_json::value::RawValue>()?);
                        continue;
                    }
                    self.skip -= 1;
                }
                map.next_value::<serde::de::IgnoredAny>()?;
            }
            Ok(out)
        }
    }

    let mut de = serde_json::Deserializer::from_str(raw);
    let found = (&mut de).deserialize_map(FieldNth { key, skip }).ok()?;
    found.map(serde_json::value::RawValue::get)
}

/// An object-key seed that answers "is this key the one we want?"
/// without materialising it: an unescaped key arrives borrowed and an
/// escaped one through `serde_json`'s reused scratch buffer, and both
/// route to the same comparison.
struct KeyIs<'k>(&'k str);

impl<'de> serde::de::DeserializeSeed<'de> for KeyIs<'_> {
    type Value = bool;

    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
        struct V<'k>(&'k str);
        impl<'de> serde::de::Visitor<'de> for V<'_> {
            type Value = bool;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON object key")
            }
            fn visit_str<E>(self, v: &str) -> Result<bool, E> {
                Ok(v == self.0)
            }
        }
        d.deserialize_str(V(self.0))
    }
}

/// The raw span of element `idx` of the array `raw`. `None` when `raw`
/// is not an array, is malformed, or is shorter than `idx + 1`.
fn element_at(raw: &str, idx: usize) -> Option<&str> {
    use serde::Deserializer as _;

    struct ElementAt(usize);

    impl<'de> serde::de::Visitor<'de> for ElementAt {
        type Value = Option<&'de serde_json::value::RawValue>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a JSON array")
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            let mut out = None;
            let mut i = 0usize;
            loop {
                if i == self.0 {
                    match seq.next_element::<&serde_json::value::RawValue>()? {
                        Some(v) => out = Some(v),
                        None => break,
                    }
                } else if seq.next_element::<serde::de::IgnoredAny>()?.is_none() {
                    break;
                }
                i += 1;
            }
            Ok(out)
        }
    }

    let mut de = serde_json::Deserializer::from_str(raw);
    let found = (&mut de).deserialize_seq(ElementAt(idx)).ok()?;
    found.map(serde_json::value::RawValue::get)
}

/// `unescapeJSONString` over a whole value span
/// (`pkg/logql/log/parser.go:269-283 @ v3.7.4`): `jsonparser.Unescape`
/// (`vendor/github.com/grafana/jsonparser/escape.go:130-171`) and then,
/// only if the result carries one, every U+FFFD mapped to a SPACE
/// (`parser.go:44-49,278-281` — "the rune error replacement is rejected
/// by Prometheus hence replacing them with space").
///
/// `Unescape` returns its input untouched when it holds no backslash, so
/// a span with no escape is copied verbatim. Otherwise every `\`
/// sequence in the span is decoded — including ones inside the array's
/// structure, which is why `["a\"b",  "c"]` comes back as `["a"b",  "c"]`
/// with its two spaces intact.
///
/// An INVALID escape makes `Unescape` fail and `unescapeJSONString`
/// return the empty string. It is unreachable here — the span comes out
/// of a document `serde_json` has already accepted, and `serde_json`
/// refuses a bad escape and a lone surrogate — but the arm is the
/// reference's, not a panic.
fn unescape_json_text(span: &str) -> String {
    let Some(first) = span.find('\\') else {
        return map_rune_error(span);
    };
    let mut out = String::with_capacity(span.len());
    out.push_str(&span[..first]);
    let bytes = span.as_bytes();
    let mut i = first;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            // `unescapeToUTF8` (`escape.go:96-121`).
            let Some((consumed, ch)) = unescape_one(bytes, i) else {
                return String::new();
            };
            out.push(ch);
            i += consumed;
            continue;
        }
        // "Copy everything up until the next backslash."
        let rest = &span[i..];
        match rest.find('\\') {
            Some(n) => {
                out.push_str(&rest[..n]);
                i += n;
            }
            None => {
                out.push_str(rest);
                break;
            }
        }
    }
    if out.contains(char::REPLACEMENT_CHARACTER) {
        out = out.replace(char::REPLACEMENT_CHARACTER, " ");
    }
    out
}

/// `strings.Map(removeInvalidUtf, res)` on a span `Unescape` returned
/// unchanged — allocating the copy the caller needs either way.
fn map_rune_error(span: &str) -> String {
    if span.contains(char::REPLACEMENT_CHARACTER) {
        span.replace(char::REPLACEMENT_CHARACTER, " ")
    } else {
        span.to_owned()
    }
}

/// One escape sequence at `bytes[at]`, as `(bytes consumed, the rune it
/// decodes to)` — `unescapeToUTF8`'s table plus `decodeUnicodeEscape`
/// (`escape.go:64-121 @ v3.7.4`). `None` is the reference's `(-1, -1)`.
fn unescape_one(bytes: &[u8], at: usize) -> Option<(usize, char)> {
    let e = *bytes.get(at + 1)?;
    let simple = match e {
        b'"' => '"',
        b'\\' => '\\',
        b'/' => '/',
        b'b' => '\u{8}',
        b'f' => '\u{c}',
        b'n' => '\n',
        b'r' => '\r',
        b't' => '\t',
        b'u' => return decode_unicode_escape(bytes, at),
        _ => return None,
    };
    Some((2, simple))
}

/// `decodeUnicodeEscape` (`escape.go:64-79 @ v3.7.4`). A code unit
/// outside the surrogate range is the rune; one inside it MUST be
/// followed by a second `\uXXXX` whose value is at least `0xDC00`, and
/// the pair combines. Note that the reference reads the second escape's
/// four hex digits WITHOUT checking that a `\u` precedes them, and that
/// `utf8.EncodeRune` writes U+FFFD for a rune the combination puts out
/// of range — both reproduced, and both unreachable through
/// `serde_json`, which refuses such a document outright.
fn decode_unicode_escape(bytes: &[u8], at: usize) -> Option<(usize, char)> {
    let r = decode_single_unicode_escape(bytes, at)?;
    if !(0xD800..=0xDFFF).contains(&r) {
        return Some((6, char::from_u32(r).unwrap_or(char::REPLACEMENT_CHARACTER)));
    }
    let low = decode_single_unicode_escape(bytes, at + 6)?;
    if low < 0xDC00 {
        return None;
    }
    let combined = 0x10000 + ((r - 0xD800) << 10) + (low - 0xDC00);
    Some((
        12,
        char::from_u32(combined).unwrap_or(char::REPLACEMENT_CHARACTER),
    ))
}

/// `decodeSingleUnicodeEscape` (`escape.go:37-53 @ v3.7.4`): the four hex
/// digits at `at + 2`, with the `\u` prefix ASSUMED and not checked.
fn decode_single_unicode_escape(bytes: &[u8], at: usize) -> Option<u32> {
    let hex = bytes.get(at + 2..at + 6)?;
    let mut out = 0u32;
    for b in hex {
        out = (out << 4) | hex_val(*b)?;
    }
    Some(out)
}

/// `h2I` (`escape.go:24-34 @ v3.7.4`).
fn hex_val(b: u8) -> Option<u32> {
    match b {
        b'0'..=b'9' => Some(u32::from(b - b'0')),
        b'A'..=b'F' => Some(u32::from(b - b'A' + 10)),
        b'a'..=b'f' => Some(u32::from(b - b'a' + 10)),
        _ => None,
    }
}

/// Full-flatten: nested objects join with `_`; scalars stringify; arrays
/// and nulls are skipped (pinned semantics).
///
/// **Key names are the reference's** (issue #287 review round 2, finding
/// 3 — grafana/loki v3.7.4, `pkg/logql/log/parser.go`
/// `buildSanitizedPrefixFromBuffer` over `pkg/logql/log/util.go:42`
/// `appendSanitized`), built by [`push_sanitized_part`]. Before this,
/// raw JSON keys were joined and the whole result sanitized afterwards
/// by `add_extracted`, which agrees with the reference for every key
/// that needs no trimming but not otherwise — container-captured
/// divergences, all now fixed: `{" a ":1}` gave `_a_` (reference `a`),
/// `{"x":{" b ":1}}` gave `x__b_` (`x_b`), `{"  ":{"b":1}}` gave `___b`
/// (`b`), `{"x":{"  ":1}}` gave `x___` (`x`), `{"x":{"":1}}` gave `x_`
/// (`x`), `{"":1}` emitted a label with an EMPTY NAME (the reference
/// drops the field).
///
/// **Collision renames are path-aware too** (fix round 2). A leaf whose
/// built key is taken by the original stream labels or a live
/// structured-metadata entry is renamed with `_extracted`, and WHERE the
/// suffix lands depends on depth, because the reference has two code
/// paths: a top-level field appends it to the SANITIZED key
/// (`parser.go:152-153`), while a nested field appends it to the RAW
/// final path part and rebuilds the sanitized path
/// (`parser.go:183-187`), so trimming and rune-mapping run over the
/// suffixed part. The orders differ observably: for base label `x`,
/// `{"x":{"":1}}` is `x__extracted` (the part trimmed empty, but
/// suffixed it survives as `_extracted` plus its separator), and for
/// base `x_y`, `{"x":{" y ":1}}` is `x_y__extracted` (the trailing
/// space, trimmed in the unsuffixed build, now sanitizes to `_`) —
/// both container-captured, with the depth-0 counterpart `{" x ":1}`
/// → `x_extracted` pinning the asymmetry. Insertion therefore happens
/// HERE, leaf by leaf during the walk ([`insert_flattened`], replacing
/// the earlier flatten-then-`add_extracted` two-phase): the rename
/// needs `(prefix, raw part, depth)`, which only the walk knows, and
/// the first-wins check must see leaves already inserted by THIS walk
/// (`{"a-b":1,"a.b":2}` DROPS the second — issue #334). A breach
/// mid-walk can leave earlier leaves in `labels`; every caller
/// propagates [`RowBudgetExceeded`] into the whole query's 422, so a
/// partial set is never observed. `add_extracted` itself is untouched
/// and stays there for the other parsers.
///
/// **The walk order is the DOCUMENT's** ([`WireJson`]) — which key of a
/// colliding pair survives depends on it, so a sorted or de-duplicating
/// walk answers differently in both directions.
///
/// Every key string this builds — the leaf label names and the
/// intermediate object prefixes alike — is charged to `budget` BEFORE it
/// is allocated, so a breach returns with the key never having existed
/// (see [`MAX_JSON_FLATTEN_KEY_BYTES`] for what that bound is and is
/// not). Skipped values (`null`, arrays) build no key and so are charged
/// nothing.
///
/// **What the charge covers** (issue #287 review round 1, finding 1, as
/// re-judged in round 2): the charge is
/// [`super::charge::alloc_block_bytes`] of the key's byte length, the
/// same provable over-approximation of a retained heap block every other
/// charge site in this crate uses. Its documented precondition is ONE
/// exactly-reserved allocation, which is what
/// `String::with_capacity(len)` + appends totalling exactly `len` is —
/// the capacity cannot be less than `len`, so no reallocation happens,
/// and whatever slop the allocator adds on top of the single `len`-byte
/// request is inside the model. The earlier version charged the bare
/// `len` and asserted `capacity == len`, which the language does not
/// guarantee. A `format!` join would instead make TWO requests
/// (`prefix.len()` then a doubled `2·prefix.len()`), breaking the
/// precondition rather than the arithmetic —
/// `tests/logql_json_key_alloc_gate.rs` measures the requests and fails
/// on that shape.
fn flatten_json<'v>(
    prefix: &str,
    depth: Depth,
    value: &'v WireJson,
    sink: &mut FlattenSink<'_, '_, '_>,
    budget: &JsonKeyBudget,
    mut capture: Option<&mut JsonPathCapture<'v, '_>>,
) -> Result<(), RowBudgetExceeded> {
    if let WireJson::Object(map) = value {
        for (k, v) in map {
            if matches!(
                v,
                WireJson::Array(_) | WireJson::Leaf(serde_json::Value::Null)
            ) {
                continue;
            }
            // `bytes.TrimSpace` per part. Rust's `str::trim` and Go's
            // `unicode.IsSpace` are both the Unicode `White_Space`
            // property, so the trimmed part is the reference's.
            let part = k.trim();
            if part.is_empty() {
                // The reference's `buildSanitizedPrefixFromBuffer`
                // `continue`s on a part that trims empty, so it
                // contributes neither characters NOR a separator.
                match v {
                    // Transparent: the child is named by the prefix alone.
                    WireJson::Object(_) => {
                        // The reference pushes the raw key onto
                        // `prefixBuffer` BEFORE testing whether it
                        // sanitizes to anything (`nextKeyPrefix`), so an
                        // empty-trimming ancestor still occupies a path
                        // slot; `parseObject` pops it on the way out.
                        if let Some(c) = capture.as_deref_mut() {
                            c.push(k, budget)?;
                        }
                        let res = flatten_json(
                            prefix,
                            Depth::Nested,
                            v,
                            sink,
                            budget,
                            capture.as_deref_mut(),
                        );
                        if let Some(c) = capture.as_deref_mut() {
                            c.pop();
                        }
                        res?;
                    }
                    // At depth 0 the reference's `parseLabelValue` takes
                    // `sanitizeLabelKey(key, true) == ""` as `!ok` and
                    // DROPS the field; deeper, the key is the prefix —
                    // and a collision rename still suffixes the RAW
                    // (empty-trimming) part, so it stops vanishing
                    // (container cell: base `x`, `{"x":{"":1}}` →
                    // `x__extracted`).
                    WireJson::Leaf(scalar) if !prefix.is_empty() => {
                        budget.charge_key(prefix.len())?;
                        insert_flattened(
                            sink,
                            prefix.to_string(),
                            prefix,
                            k,
                            Depth::Nested,
                            json_scalar_to_string(scalar),
                            budget,
                            capture.as_deref_mut(),
                        )?;
                    }
                    _ => {}
                }
                continue;
            }
            let len = sanitized_part_len(prefix, part);
            budget.charge_key(len)?;
            let mut key = String::with_capacity(len);
            push_sanitized_part(&mut key, prefix, part);
            debug_assert_eq!(
                key.len(),
                len,
                "the charged length must be the built length"
            );
            match v {
                WireJson::Object(_) => {
                    if let Some(c) = capture.as_deref_mut() {
                        c.push(k, budget)?;
                    }
                    let res =
                        flatten_json(&key, Depth::Nested, v, sink, budget, capture.as_deref_mut());
                    if let Some(c) = capture.as_deref_mut() {
                        c.pop();
                    }
                    res?;
                }
                WireJson::Leaf(scalar) => insert_flattened(
                    sink,
                    key,
                    prefix,
                    k,
                    depth,
                    json_scalar_to_string(scalar),
                    budget,
                    capture.as_deref_mut(),
                )?,
                // Unreachable — the loop `continue`s on an array above,
                // as `parseObject` handles only string/number/boolean/
                // object (`parser.go:105-114`).
                WireJson::Array(_) => {}
            }
        }
    }
    Ok(())
}

/// Which of the reference's two key-construction paths a leaf is on:
/// `parseLabelValue`'s empty-prefix-buffer branch for a top-level field
/// (`parser.go:139`) versus the buffer-built branch for anything deeper
/// (`parser.go:171`). NOT derivable from `prefix.is_empty()`: a nested
/// field whose ancestor parts all trimmed empty (`{"  ":{" b ":1}}`)
/// has an empty prefix but sits on the nested branch, and its collision
/// rename follows the nested rule (container cell: base `b` →
/// `b__extracted`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Depth {
    Root,
    Nested,
}

/// Everything one `| json` full flatten WRITES to, as a single value:
/// the label vector, the per-line extraction state ([`ExtractionState`])
/// and the builder's dirty bit. Bundled because the walk threads all
/// three through every level of recursion and every leaf.
struct FlattenSink<'a, 'r, 'l> {
    labels: &'l mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    st: &'l mut ExtractionState<'a, 'r>,
    dirty: &'l mut bool,
}

/// The reference's `_extracted` collision suffix (`parser.go:25`).
const DUPLICATE_SUFFIX: &str = "_extracted";

/// Inserts one flattened leaf, applying the reference's depth-aware
/// collision rename (see [`flatten_json`]'s doc for the two orders and
/// the container cells pinning them) and then its first-wins skip
/// (issue #334).
///
/// The rename fires only for a name the ORIGINAL stream carried or that
/// a LIVE structured-metadata entry holds — [`ExtractionState`] for why
/// those two are read from different places. A name already extracted on
/// this line (by an earlier leaf of THIS walk, by an earlier stage, or
/// before a `drop` removed it) makes the leaf vanish entirely: the
/// reference `return`s before `Set` (`pkg/logql/log/parser.go:156-158`
/// at depth 0, `:190-194` deeper @ v3.7.4), so the leaf writes no label,
/// records no path and — since it never reaches `Set` — does not dirty
/// the builder (`labels.go:216-222`).
///
/// The renamed key is a fresh exactly-reserved allocation charged
/// before it is built — the unsuffixed `key` it replaces was already
/// charged when built, which the budget's sentence covers (every key
/// string the flatten allocates, not every key it emits).
///
/// `capture` (opt-in, see [`JsonPaths`]) records the raw path against the
/// FINAL label name, renamed or not — the reference sets it after the
/// `_extracted` rebuild and against the suffixed key
/// (`pkg/logql/log/parser.go:152-163` at depth 0, `:183-198` deeper @
/// v3.7.4), and its `buildJSONPathFromPrefixBuffer` trims the suffix back
/// off the raw part, which is why the path recorded here is always the
/// UNSUFFIXED `raw_part`.
#[allow(clippy::too_many_arguments)]
fn insert_flattened<'v>(
    sink: &mut FlattenSink<'_, '_, '_>,
    key: String,
    prefix: &str,
    raw_part: &'v str,
    depth: Depth,
    value: String,
    budget: &JsonKeyBudget,
    capture: Option<&mut JsonPathCapture<'v, '_>>,
) -> Result<(), RowBudgetExceeded> {
    let FlattenSink { labels, st, dirty } = sink;
    let at = label_position(labels, &key);
    let renamed = st.collides_at(&key, at.is_some());
    let resolved = if renamed {
        match depth {
            // Top level: suffix the SANITIZED key (`parser.go:152-153`).
            Depth::Root => {
                let len = key.len() + DUPLICATE_SUFFIX.len();
                budget.charge_key(len)?;
                let mut s = String::with_capacity(len);
                s.push_str(&key);
                s.push_str(DUPLICATE_SUFFIX);
                s
            }
            // Nested: suffix the RAW final part and rebuild the sanitized
            // path (`parser.go:183-187`).
            Depth::Nested => {
                let len = suffixed_part_len(prefix, raw_part);
                budget.charge_key(len)?;
                let mut s = String::with_capacity(len);
                push_suffixed_part(&mut s, prefix, raw_part);
                debug_assert_eq!(s.len(), len, "the charged length must be the built length");
                s
            }
        }
    } else {
        key
    };
    let at = if renamed {
        label_position(labels, &resolved)
    } else {
        at
    };
    if st.is_extracted_at(&resolved, at.is_some()) {
        return Ok(());
    }
    **dirty = true;
    let resolved = st.note_parsed_set(Cow::Owned(resolved));
    let idx = put_label_at(labels, at, resolved, Cow::Owned(value));
    if let Some(c) = capture {
        c.out.record(idx, &c.stack, raw_part, budget)?;
    }
    Ok(())
}

/// Whether `part` passes through the reference's sanitizer byte for
/// byte — the memcpy fast path, and the reason the char-wise walk below
/// costs nothing on ordinary keys.
fn is_clean_label_part(part: &str) -> bool {
    part.as_bytes()
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
}

/// The byte length [`push_sanitized_part`] will write, computed without
/// allocating so the ledger can charge first.
///
/// Every rune the sanitizer rejects becomes exactly ONE `_`, so the
/// output length is the RUNE count of `part` (its byte length when the
/// part is clean), plus the `_` separator when `prefix` is non-empty,
/// plus the leading `_` the reference adds for a digit-initial part when
/// nothing has been emitted yet.
fn sanitized_part_len(prefix: &str, part: &str) -> usize {
    let body = if is_clean_label_part(part) {
        part.len()
    } else {
        part.chars().count()
    };
    let separator = usize::from(!prefix.is_empty());
    let digit_guard =
        usize::from(prefix.is_empty() && part.as_bytes().first().is_some_and(u8::is_ascii_digit));
    prefix.len() + separator + digit_guard + body
}

/// Appends `prefix` + `_` + sanitized `part` — the reference's
/// `buildSanitizedPrefixFromBuffer`/`appendSanitized` pair, with the
/// separator and the digit guard keyed on whether anything has been
/// emitted yet (`len(to) == 0` there, `prefix.is_empty()` here).
///
/// `part` must already be trimmed and non-empty (the caller's
/// `continue` handles the other case).
fn push_sanitized_part(out: &mut String, prefix: &str, part: &str) {
    if prefix.is_empty() {
        if part.as_bytes().first().is_some_and(u8::is_ascii_digit) {
            out.push('_');
        }
    } else {
        out.push_str(prefix);
        out.push('_');
    }
    if is_clean_label_part(part) {
        out.push_str(part);
        return;
    }
    for c in part.chars() {
        out.push(if c.is_ascii_alphanumeric() || c == '_' {
            c
        } else {
            '_'
        });
    }
}

/// The byte length [`push_suffixed_part`] will write, computed without
/// allocating so the ledger can charge first. The suffixed part is
/// `raw_part` + `_extracted` put through the same trim + rune map as any
/// other part; since the suffix ends in a letter, `TrimSpace(raw +
/// suffix)` strips only `raw_part`'s LEADING whitespace — its trailing
/// whitespace is now interior and counts one `_` per rune, and a part
/// that trimmed empty contributes exactly `_extracted`.
fn suffixed_part_len(prefix: &str, raw_part: &str) -> usize {
    let part = raw_part.trim_start();
    let body = if is_clean_label_part(part) {
        part.len()
    } else {
        part.chars().count()
    };
    let separator = usize::from(!prefix.is_empty());
    let digit_guard =
        usize::from(prefix.is_empty() && part.as_bytes().first().is_some_and(u8::is_ascii_digit));
    prefix.len() + separator + digit_guard + body + DUPLICATE_SUFFIX.len()
}

/// Appends `prefix` + `_` + sanitized (`raw_part` + `_extracted`) — the
/// reference's nested collision rebuild: `parseLabelValue` replaces the
/// final prefix-buffer entry with the RAW key plus `duplicateSuffix`
/// (`parser.go:183-187`) and reruns `buildSanitizedPrefixFromBuffer`,
/// so the separator and the digit guard key on whether anything has
/// been emitted yet, exactly as in [`push_sanitized_part`] — but over
/// the SUFFIXED part, which is never empty even when `raw_part` trims
/// empty.
fn push_suffixed_part(out: &mut String, prefix: &str, raw_part: &str) {
    let part = raw_part.trim_start();
    if prefix.is_empty() {
        if part.as_bytes().first().is_some_and(u8::is_ascii_digit) {
            out.push('_');
        }
    } else {
        out.push_str(prefix);
        out.push('_');
    }
    if is_clean_label_part(part) {
        out.push_str(part);
    } else {
        for c in part.chars() {
            out.push(if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            });
        }
    }
    out.push_str(DUPLICATE_SUFFIX);
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

/// The packed-entry key `| unpack` promotes back to the log line
/// (`logqlmodel.PackedEntryKey`, `pkg/logqlmodel/logqlmodel.go @ v3.7.4`).
const PACKED_ENTRY_KEY: &str = "_entry";

/// `| unpack` (issue #200): parse the line as a packed JSON object,
/// promoting a string `_entry` field back to the line (returned owned) and
/// other string fields to labels; non-string fields are skipped. A
/// non-object line or a parse failure keeps the line and tags
/// `__error__="JSONParserErr"` with the representative detail — the same
/// malformed class `json` reports. Returns `Some(new_line)` only when a
/// string `_entry` field was present.
///
/// **TWO PHASES, and the split is observable** (issue #334). The
/// reference resolves each field's key and BUFFERS the pair
/// (`UnpackParser.unpack`, `pkg/logql/log/parser.go:797-828 @ v3.7.4`),
/// then `Set`s the whole buffer only `if isPacked` — only when a string
/// `_entry` field was seen (`parser.go:832-838`). Two consequences the
/// streaming shape cannot reproduce, both container-captured at v3.7.4:
///
/// - an object WITHOUT `_entry` contributes no labels at all
///   (`{"a":"1","a":"2"}` under `| unpack` yields none);
/// - within one stage a repeated key is LAST-wins, not first-wins,
///   because `RecordExtracted` happens at the flush and so no earlier
///   field of the same stage is ever "already extracted"
///   (`{"a":"1","a":"2","_entry":"x"}` yields `a="2"`, and with a stream
///   label `a` it yields `a_extracted="2"`) — the opposite of every other
///   parser.
///
/// The collision and already-extracted tests read the RAW key, before
/// sanitization, and the SANITIZED form of the (possibly suffixed) key is
/// what gets buffered — the reference's own order (`parser.go:807-820`:
/// `BaseHas(key)`/`Extracted(key)` on the interned raw key,
/// `sanitizeLabelKey(key, true)` only at the buffer push). Container cell:
/// a stream label `a_b` with a packed field `a-b` OVERWRITES it, no
/// `_extracted`, because `BaseHas("a-b")` is false.
fn run_unpack<'a>(
    line: &str,
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    st: &mut ExtractionState<'a, '_>,
    errs: &mut ErrorSlots<'a>,
) -> Option<String> {
    // `UnpackParser.Process`'s OWN gate (`parser.go:753-762 @ v3.7.4`),
    // not the flatten arm's (issue #389 part A): an empty line returns
    // before anything at all is written, and the object test is ONE RAW
    // BYTE — whitespace is not skipped, so ` {"a":"1"}` is refused where
    // `{"_entry":"hi"}trailing` is admitted.
    if line.is_empty() {
        return None;
    }
    if line.as_bytes()[0] != b'{' {
        errs.set_err(Cow::Borrowed("JSONParserErr"));
        errs.set_details(Cow::Borrowed(JSON_ERROR_DETAILS));
        return None;
    }
    let fields = match parse_wire_json_prefix(line) {
        Ok(WireJson::Object(fields)) => fields,
        // Slot write, no label, no dirty — see `run_json` (issue #238).
        _ => {
            errs.set_err(Cow::Borrowed("JSONParserErr"));
            errs.set_details(Cow::Borrowed(JSON_ERROR_DETAILS));
            return None;
        }
    };
    let mut new_line = None;
    // The reference's `lbsBuffer`. `Vec::new()` allocates nothing until a
    // field lands in it, so an unpacked line with no promotable field
    // stays allocation-free.
    let mut buffered: Vec<(String, String)> = Vec::new();
    for (k, v) in fields {
        // Only string fields participate; other JSON value types are skipped.
        let WireJson::Leaf(serde_json::Value::String(s)) = v else {
            continue;
        };
        if k == PACKED_ENTRY_KEY {
            new_line = Some(s);
            continue;
        }
        let resolved = if st.collides(labels, &k) {
            format!("{k}{DUPLICATE_SUFFIX}")
        } else {
            k
        };
        if st.is_extracted(labels, &resolved) {
            continue;
        }
        let name = if key_needs_sanitizing(&resolved) {
            sanitize_label_key(&resolved)
        } else {
            resolved
        };
        buffered.push((name, s));
    }
    // `isPacked` — no promoted entry, no flush, so nothing was extracted.
    new_line.as_ref()?;
    for (name, value) in buffered {
        errs.dirty = true;
        let name = st.note_parsed_set(Cow::Owned(name));
        set_label(labels, name, Cow::Owned(value));
    }
    new_line
}

// ---------------------------------------------------------------------
// logfmt
// ---------------------------------------------------------------------

/// `| logfmt`'s two switches, carried as one value rather than as a pair
/// of bare `bool` parameters.
#[derive(Debug, Clone, Copy)]
struct LogfmtFlags {
    /// `--strict`: stop at the first decoder error and tag it.
    strict: bool,
    /// `--keep-empty`: keep a pair whose value is empty.
    keep_empty: bool,
}

/// `sanitizeLabelKey(raw, true)` (`pkg/logql/log/util.go:22-38 @ v3.7.4`)
/// compared against `target`, WITHOUT materialising the sanitized key —
/// this runs once per decoded line key per extraction, so it must not
/// allocate.
///
/// **Equivalent to `sanitize_label_key(raw.trim_ascii()) == target`**, and
/// pinned as that identity by
/// `sanitized_key_eq_agrees_with_sanitize_label_key`. Keeping the two in
/// step matters: the bare `| logfmt` arm renames a line key through
/// [`sanitize_label_key`], and the targeted arm decides which extraction
/// a line key writes to through this — a spelling difference between them
/// would be a divergence with no reference behind it.
///
/// The trim is the reference's `strings.TrimSpace`, narrowed to ASCII
/// because [`walk_logfmt`] can emit neither an empty key nor one holding
/// ASCII whitespace (`walk_logfmt_never_emits_an_empty_or_whitespace_bearing_key`),
/// so it is an identity on every reachable key; [`sanitize_label_key`]
/// does not trim at all, and the two therefore cannot disagree on a key
/// that actually arrives. A key bearing non-ASCII Unicode whitespace is
/// the bare arm's pre-existing question (issue #200 ground), untouched
/// here so both arms keep answering it the same way.
///
/// Rune-wise, not byte-wise: one multi-byte rune sanitizes to exactly one
/// `_`, matching `strings.Map`.
fn sanitized_key_eq(raw: &str, target: &str) -> bool {
    let raw = raw.trim_ascii();
    if raw.is_empty() {
        return false;
    }
    let mut want = target.chars();
    // `if isPrefix && key[0] >= '0' && key[0] <= '9' { key = "_" + key }`
    // — the FIRST BYTE of the trimmed key, before the rune mapping.
    if raw.as_bytes()[0].is_ascii_digit() && want.next() != Some('_') {
        return false;
    }
    for c in raw.chars() {
        let mapped = if c.is_ascii_alphanumeric() || c == '_' {
            c
        } else {
            '_'
        };
        if want.next() != Some(mapped) {
            return false;
        }
    }
    // Both sides exhausted together, or `target` is merely a prefix.
    want.next().is_none()
}

/// Which extraction identifier a decoded line key writes to, if any.
///
/// The reference renames first (`pkg/logql/log/parser.go:594-599 @
/// v3.7.4`: the SANITIZED line key equals some identifier's SOURCE key)
/// and only then looks the result up in `l.expressions` (`:601`), so a
/// source-key match beats a line key that merely spells an identifier.
/// Measured on `grafana/loki:3.7.4` over `p=1 q=2`: `| logfmt q="p"`
/// answers `q="2"` — `p` renames to `q` and writes `1`, then `q` matches
/// no source key but IS an identifier and overwrites with `2`.
///
/// Ties across identifiers are broken by QUERY ORDER: `for id, orig :=
/// range keys` (`:594`) ranges a Go map, so the reference has no answer
/// to break — see the `logfmt-expression-duplicate-source-key-tiebreak`
/// ledger entry for the measured split.
fn logfmt_target_for<'e>(targets: &'e [(String, String)], raw_key: &str) -> Option<&'e str> {
    for (id, source) in targets {
        if sanitized_key_eq(raw_key, source) {
            return Some(id.as_str());
        }
    }
    for (id, _) in targets {
        if sanitized_key_eq(raw_key, id) {
            return Some(id.as_str());
        }
    }
    None
}

/// `pkg/logql/log/parser.go:545-550 @ v3.7.4`: every extraction identifier
/// the stream and the LIVE structured metadata do not already supply is
/// `Set` to `""` **before the line is scanned** — an unconditional
/// overwrite, so a later `| logfmt` stage resets an earlier stage's parsed
/// value. A colliding identifier is skipped entirely (no `_extracted`
/// rename here; that happens only on a hit, in [`add_extracted`]).
///
/// This is what makes a targeted MISS emit an empty label. The
/// `--keep-empty` rule cannot reach it: `LogfmtExpressionParserExpr.Stage()`
/// passes only `l.Strict` to `NewLogfmtExpressionParser`
/// (`pkg/logql/syntax/ast.go:1073-1075 @ v3.7.4`), so the expression
/// parser carries no `keepEmpty` field at all. Measured on
/// `grafana/loki:3.7.4` over `b=1 a-b=2 x=3`: `| logfmt a="nosuch"` and
/// `| logfmt --keep-empty a="nosuch"` both answer `a=""`.
fn seed_logfmt_identifiers<'a>(
    targets: &'a [(String, String)],
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    st: &mut ExtractionState<'a, '_>,
    dirty: &mut bool,
) {
    for (id, _) in targets {
        // `!lbs.BaseHas(id) && !lbs.HasInCategory(id, StructuredMetadataLabel)`.
        if st.collides(labels, id) {
            continue;
        }
        // A `Set` dirties the builder (`labels.go:216-222`; issue #238).
        *dirty = true;
        let id = st.note_parsed_set(Cow::Borrowed(id.as_str()));
        set_label(labels, id, Cow::Borrowed(""));
    }
}

/// Applies the logfmt stage in a single streaming pass (issue #200). On
/// the first decoder error the walk stops; under `strict` it tags
/// `__error__="LogfmtParserErr"` plus the per-class detail, otherwise (the
/// lenient default) it swallows the error, keeping the pairs already
/// emitted before it (matching the reference's default best-effort
/// logfmt). Values are committed through `to_cow` — the identity for the
/// original body (captures stay borrowed slices) or a copy for a
/// rewritten line.
///
/// **The two arms are different parsers in the reference, and they agree
/// on almost nothing.** `LogfmtParser.Process`
/// (`pkg/logql/log/parser.go:380-430 @ v3.7.4`) drops an empty value
/// unless `keepEmpty` and maps a `U+FFFD` inside a value to a space;
/// `LogfmtExpressionParser.Process` (`:531-624`) has neither rule and
/// instead:
///
/// - pre-seeds every identifier to `""` before the line is read
///   ([`seed_logfmt_identifiers`]) — which is why a miss emits an empty
///   label and why `--keep-empty` is inert on this arm;
/// - makes ONE document-order pass, choosing each pair's destination by
///   SANITIZED line key ([`logfmt_target_for`]), so the last matching
///   pair in the line wins — measured over `b=2 a=1`, `| logfmt a="b"`
///   answers `a="1"`, not `a="2"`;
/// - EMPTIES a value containing `U+FFFD` (`:590-592`, `val = nil`) where
///   the implicit parser maps it to a space (`:417-419`) — measured over
///   `a=x\u{FFFD}y b=2`, `| logfmt a="a"` answers `a=""` while bare
///   `| logfmt` answers `a="x y"`.
///
/// Issue #393; every measurement above is from `grafana/loki:3.7.4`,
/// digest `sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`,
/// and is pinned by `logqltest/corpus/b24_logfmt_expr_eval.test`.
///
/// **One walk, not one per extraction**, which is also why this is
/// cheaper than what it replaces: the targeted arm used to run a full
/// [`walk_logfmt`] per extraction. Pinned by the E-independence gate in
/// `tests/logql_pipeline_alloc.rs`.
fn run_logfmt<'a, 't>(
    text: &'t str,
    flags: LogfmtFlags,
    extractions: &'a [(String, String)],
    labels: &mut Vec<(Cow<'a, str>, Cow<'a, str>)>,
    st: &mut ExtractionState<'a, '_>,
    errs: &mut ErrorSlots<'a>,
    to_cow: impl Fn(Cow<'t, str>) -> Cow<'a, str>,
) {
    let LogfmtFlags { strict, keep_empty } = flags;
    let result = if extractions.is_empty() {
        walk_logfmt(text, &mut |k, v| {
            if !keep_empty && v.is_empty() {
                return;
            }
            add_extracted(
                labels,
                st,
                to_cow(Cow::Borrowed(k)),
                KeyOrigin::Line,
                to_cow(v),
                OnAlreadyExtracted::Skip,
                &mut errs.dirty,
            );
        })
    } else {
        // `LogfmtExpressionParser.Process` (`parser.go:531-624 @ v3.7.4`):
        // pre-seed, then ONE document-order pass. `keep_empty` is
        // deliberately not consulted — this parser has no such field.
        seed_logfmt_identifiers(extractions, labels, st, &mut errs.dirty);
        walk_logfmt(text, &mut |raw_key, val| {
            let Some(id) = logfmt_target_for(extractions, raw_key) else {
                return;
            };
            // `parser.go:590-592` — the EXPRESSION parser empties such a
            // value; the implicit parser maps it to a space (`:417-419`),
            // which the bare arm above keeps doing (issue #200 ground).
            let val = if val.contains(char::REPLACEMENT_CHARACTER) {
                Cow::Borrowed("")
            } else {
                to_cow(val)
            };
            add_extracted(
                labels,
                st,
                Cow::Borrowed(id),
                KeyOrigin::QueryIdentifier,
                val,
                OnAlreadyExtracted::Overwrite,
                &mut errs.dirty,
            );
        })
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

    /// **Issue #247, the unmatchable-key invariant.** The logfmt
    /// extraction sub-grammar expresses "this path matches nothing" as a
    /// source key `walk_logfmt` can never emit — the empty string, or one
    /// containing ASCII whitespace (a multi-element path joins with a
    /// space). That is only sound if the walker really cannot emit such
    /// a key, so it is asserted here over hostile lines rather than
    /// argued in a comment.
    ///
    /// **Both halves were broken and watched to fail.** Deleting the
    /// `is_ascii_whitespace()` break in the key scan reddens this with
    /// `"a b=1" emitted key "a b"`. The empty half has TWO redundant
    /// enforcers — the leading-`=` `UnexpectedEquals` guard and the
    /// `if !key.is_empty()` gate on the `sink` call — and deleting either
    /// alone leaves this green; deleting both reddens it with `"=1"
    /// emitted an EMPTY key`. Deleting the `b'"' || b < 0x20` invalid-key
    /// guard does NOT redden it, and that is correct: a key holding a
    /// quote or a control byte is neither empty nor whitespace-bearing,
    /// so it is outside what this invariant claims.
    #[test]
    fn walk_logfmt_never_emits_an_empty_or_whitespace_bearing_key() {
        let lines = [
            "=1",
            "\"\"=1",
            "a=",
            "\"a\"=1",
            "a b=1",
            "a=1 =2",
            "   ",
            "",
            "\t\n \t",
            "a=\"unterminated",
            "a\u{1}b=1",
            "a=1\u{7}b=2",
            "a=\"x y\" b=2",
            "a==1",
            "=",
            "b=1 a-b=2 x=3 b.c=4",
            "a.b=1 a/b=2 a:b=3 a,b=4 a[0]=5",
            "é=1 日本=2",
            "a=1 a=2 a=3",
        ];
        let mut seen = 0usize;
        for line in lines {
            // The walk's Err is irrelevant here: the sink fires for every
            // pair decoded BEFORE any error, and those are exactly the
            // keys a targeted extraction compares against.
            let _ = walk_logfmt(line, &mut |k, _v| {
                seen += 1;
                assert!(!k.is_empty(), "{line:?} emitted an EMPTY key");
                assert!(
                    !k.bytes().any(|b| b.is_ascii_whitespace()),
                    "{line:?} emitted key {k:?}, which contains ASCII whitespace"
                );
            });
        }
        assert!(seen >= 20, "the hostile lines emitted only {seen} keys");
    }

    /// **Issue #247, the same invariant end to end.** The three shapes
    /// whose resolved source key is unmatchable — a quoted path holding a
    /// space, a two-element path, and an empty path — extract nothing
    /// from a line that carries every one of their pieces. Measured on
    /// the pinned v3.7.4 container over this same line: all three give
    /// `a=""` there too (a miss on both sides; the reference joins with
    /// `fmt.Sprintf("%v", paths...)`, `pkg/logql/log/parser.go:546 @
    /// v3.7.4`, and compares against a sanitized key).
    ///
    /// **The `--keep-empty` flags below no longer do anything** (issue
    /// #393): a targeted miss emits `a=""` either way, because the
    /// expression parser pre-seeds its identifiers and carries no
    /// `keepEmpty` field. They are kept as written so the row still reads
    /// as the #247 case it was captured for; the flagless spellings are
    /// asserted beside them.
    #[test]
    fn an_unmatchable_resolved_key_extracts_nothing() {
        const LINE: &str = "b=1 a-b=2 x=3";
        for query in [
            r#"{s="m"} | logfmt --keep-empty a="\"b c\"""#,
            r#"{s="m"} | logfmt --keep-empty a="b \"x\"""#,
            r#"{s="m"} | logfmt --keep-empty a="\"\"""#,
            r#"{s="m"} | logfmt a="\"b c\"""#,
            r#"{s="m"} | logfmt a="b \"x\"""#,
            r#"{s="m"} | logfmt a="\"\"""#,
        ] {
            let compiled = CompiledPipeline::compile(&stages_of(query))
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let mut out = Vec::new();
            compiled
                .run_into(LINE, &[], 0, &mut out)
                .expect("within budget");
            let a = out
                .iter()
                .find(|(k, _)| k == "a")
                .map(|(_, v)| v.to_string());
            assert_eq!(
                a,
                Some(String::new()),
                "{query} must extract nothing — visible as an EMPTY label, which since #393 is \
                 what a targeted miss emits with or without `--keep-empty`"
            );
        }
        // The control: a matchable key over the same line does extract.
        let compiled =
            CompiledPipeline::compile(&stages_of(r#"{s="m"} | logfmt a="b""#)).expect("compiles");
        let mut out = Vec::new();
        compiled
            .run_into(LINE, &[], 0, &mut out)
            .expect("within budget");
        assert_eq!(
            out.iter()
                .find(|(k, _)| k == "a")
                .map(|(_, v)| v.to_string()),
            Some("1".to_string())
        );
    }

    // -----------------------------------------------------------------
    // Issue #393 — what a SURVIVING `| logfmt <id>="<expr>"` evaluates to.
    //
    // Every criterion below asserts EMITTED LABEL NAMES AND VALUES. Each
    // expectation was captured from `grafana/loki:3.7.4` (buildinfo read
    // from the running process: {"version":"3.7.4","revision":"b318f282",
    // "branch":"release-3.7.x"}) on 2026-08-10 through
    // `/loki/api/v1/query_range`, and each is also pinned end to end by
    // `tests/logqltest/corpus/b24_logfmt_expr_eval.test`.
    // -----------------------------------------------------------------

    /// The label set `query` emits over `line`, with `base` as the stream
    /// labels — sorted, so the assertion is about the SET and not about
    /// the order the pre-seed happens to append in.
    fn logfmt_labels(query: &str, line: &str, base: &[(&str, &str)]) -> Vec<(String, String)> {
        let base: Vec<(String, String)> = base
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let compiled =
            CompiledPipeline::compile(&stages_of(query)).unwrap_or_else(|e| panic!("{query}: {e}"));
        let mut out = Vec::new();
        compiled
            .run_into(line, &base, 0, &mut out)
            .expect("within budget")
            .unwrap_or_else(|| panic!("{query}: the line was dropped"));
        let mut got: Vec<(String, String)> = out
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        got.sort();
        got
    }

    fn sorted_pairs(expected: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = expected
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        out.sort();
        out
    }

    /// **AC1.** A targeted MISS emits an EMPTY label, and `--keep-empty`
    /// has nothing to do with it: `LogfmtExpressionParserExpr.Stage()`
    /// passes only `l.Strict` (`pkg/logql/syntax/ast.go:1073-1075 @
    /// v3.7.4`), so this parser carries no `keepEmpty` field — the label
    /// comes from the unconditional pre-seed
    /// (`pkg/logql/log/parser.go:545-550`).
    ///
    /// Fails on `ec774ee`: without the flag the label was absent
    /// entirely, because the targeted arm consulted the bare arm's
    /// empty-drop rule.
    #[test]
    fn a_targeted_miss_emits_an_empty_label_regardless_of_keep_empty() {
        const LINE: &str = "b=1 a-b=2 x=3";
        for query in [
            r#"{s="m"} | logfmt a="nosuch""#,
            r#"{s="m"} | logfmt --keep-empty a="nosuch""#,
        ] {
            assert_eq!(
                logfmt_labels(query, LINE, &[]),
                sorted_pairs(&[("a", "")]),
                "{query}"
            );
        }
        // A miss on an EMPTY line is the same case: the pre-seed runs
        // before the decoder is even reset.
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt a="nosuch""#, "", &[]),
            sorted_pairs(&[("a", "")])
        );
        // …and a line key whose VALUE is empty reaches the same answer by
        // the other route (`b= x=3` → `a=""` on the container).
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt a="b""#, "b= x=3", &[]),
            sorted_pairs(&[("a", "")])
        );
    }

    /// **AC2.** The LINE key is sanitized before it is compared against a
    /// source key (`parser.go:575` → `util.go:22-38`), and because the
    /// pass is document-order with no first-hit latch, the LAST matching
    /// pair wins.
    ///
    /// Fails on `ec774ee` with `a="1"`, `a="2"`, absent and absent
    /// respectively — that commit compared the RAW line key.
    ///
    /// This also rules out the plausible "sanitize the SOURCE key at
    /// compile time and keep comparing raw line keys" fix: that matches
    /// only the literal `p_q` pair and answers `a="2"` on the second row,
    /// and misses the third entirely.
    #[test]
    fn a_line_key_is_sanitized_before_it_is_compared() {
        for (query, line, want) in [
            (r#"{s="m"} | logfmt a="a_b""#, "a_b=1 a-b=2", "2"),
            (r#"{s="m"} | logfmt a="p_q""#, "p.q=1 p_q=2 p-q=3", "3"),
            (r#"{s="m"} | logfmt a="b_c""#, "b=1 a-b=2 x=3 b.c=4", "4"),
            // A leading-digit line key gains the reference's `_` prefix.
            (r#"{s="m"} | logfmt a="_1x""#, "1x=5 y=6", "5"),
        ] {
            assert_eq!(
                logfmt_labels(query, line, &[]),
                sorted_pairs(&[("a", want)]),
                "{query} over {line:?}"
            );
        }
        // The mirror the sanitization makes possible: a quoted path
        // holding a `.` can no longer match the raw `b.c` pair, because
        // the line key it would have to equal is `b_c`.
        assert_eq!(
            logfmt_labels(
                r#"{s="m"} | logfmt a="\"b.c\"""#,
                "b=1 a-b=2 x=3 b.c=4",
                &[]
            ),
            sorted_pairs(&[("a", "")])
        );
    }

    /// **AC3.** An extraction IDENTIFIER aliases a line key. The
    /// reference renames a line key to an identifier when it equals that
    /// identifier's SOURCE key (`parser.go:594-599`) and only then asks
    /// whether the resulting name is an identifier at all (`:601`) — so a
    /// pair the query never named as a source still lands, under its own
    /// name, whenever that name is one of the destinations.
    ///
    /// Fails on `ec774ee` on all four rows.
    #[test]
    fn an_extraction_identifier_aliases_a_line_key() {
        assert_eq!(
            logfmt_labels(
                r#"{s="m"} | logfmt a="x", b="nosuch""#,
                "b=1 a-b=2 x=3",
                &[]
            ),
            sorted_pairs(&[("a", "3"), ("b", "1")]),
            "`b` is not the source of anything, but it IS an identifier"
        );
        // Source-key match BEATS identifier match: `p` renames to `q` and
        // writes `1`, then `q` (an identifier, no source match) overwrites
        // with `2`.
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt q="p""#, "p=1 q=2", &[]),
            sorted_pairs(&[("q", "2")])
        );
        // The alias is compared SANITIZED too — `a-b` spells `a_b`.
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt a_b="nosuch""#, "a-b=7", &[]),
            sorted_pairs(&[("a_b", "7")])
        );
        // Two identifiers, one of which aliases nothing on this line.
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt a="b", b="a""#, "b=1 a-b=2 x=3", &[]),
            sorted_pairs(&[("a", "1"), ("b", "")])
        );
    }

    /// **AC4 — the discriminator.** Document order, last write wins.
    ///
    /// Over `b=2 a=1` the reference answers `a="1"`: the pair `b` renames
    /// to `a` and writes `2`, then the pair `a` aliases the identifier and
    /// OVERWRITES with `1`. `ec774ee` answers `a="2"`, and so does the
    /// plausible "keep the per-extraction source-key lookup, just emit an
    /// empty string on a miss" fix — which is exactly why this row is
    /// here and why parts A, B and C cannot ship apart.
    ///
    /// The mirror line is the direction control: it is GREEN on
    /// `ec774ee`, so the test cannot be passed by inverting a bias.
    #[test]
    fn the_last_matching_line_key_in_document_order_wins() {
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt a="b""#, "b=2 a=1", &[]),
            sorted_pairs(&[("a", "1")])
        );
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt a="b""#, "a=1 b=2", &[]),
            sorted_pairs(&[("a", "2")])
        );
    }

    /// **AC5.** A repeated identifier keeps only its LAST source key
    /// (`parser.go:521`'s map assignment).
    ///
    /// Fails on `ec774ee` with `a="1"` and `a="1"`.
    #[test]
    fn a_repeated_identifier_keeps_only_its_last_source_key() {
        assert_eq!(
            logfmt_labels(
                r#"{s="m"} | logfmt a="b", a="nosuch""#,
                "b=1 a-b=2 x=3",
                &[]
            ),
            sorted_pairs(&[("a", "")])
        );
        assert_eq!(
            logfmt_labels(
                r#"{s="m"} | logfmt a="nosuch", a="b""#,
                "b=1 a-b=2 x=3",
                &[]
            ),
            sorted_pairs(&[("a", "1")])
        );
    }

    /// **AC5, second half.** The pre-seed is an UNCONDITIONAL `Set`
    /// (`parser.go:547-549`), so a later `| logfmt` stage RESETS a label
    /// an earlier stage parsed rather than leaving it standing.
    ///
    /// Fails on `ec774ee` with `a="3"`.
    #[test]
    fn a_later_logfmt_stage_reseeds_an_earlier_parsed_label() {
        assert_eq!(
            logfmt_labels(
                r#"{s="m"} | logfmt a="x" | logfmt a="nosuch""#,
                "b=1 a-b=2 x=3",
                &[]
            ),
            sorted_pairs(&[("a", "")])
        );
    }

    /// **AC6.** A `U+FFFD` anywhere in the value EMPTIES it on this arm
    /// (`parser.go:590-592`, `val = nil`), where the implicit parser maps
    /// it to a space (`:417-419`).
    ///
    /// The bare half is the containment control and must NOT change: the
    /// implicit parser's space mapping is issue #200's ground, so we
    /// still hand back the rune untouched there and that stays a
    /// divergence (`a="x y"` on the container, `a="x\u{FFFD}y"` here).
    ///
    /// Fails on `ec774ee` on the first half only (`a="x\u{FFFD}y"`).
    #[test]
    fn a_replacement_rune_empties_a_targeted_value() {
        const LINE: &str = "a=x\u{FFFD}y b=2";
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt a="a""#, LINE, &[]),
            sorted_pairs(&[("a", "")])
        );
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt"#, LINE, &[]),
            sorted_pairs(&[("a", "x\u{FFFD}y"), ("b", "2")]),
            "the BARE arm is #200's ground and must not move"
        );
    }

    /// **AC7 — the #334 containment control.** An identifier the STREAM
    /// supplies is not pre-seeded at all (`parser.go:547`'s `!BaseHas`
    /// guard), and a hit on it renames to `_extracted` in the write path
    /// (`:602-604`) exactly as before. Both rows already agreed with the
    /// container on `ec774ee` and must keep agreeing.
    #[test]
    fn a_targeted_identifier_the_stream_supplies_is_not_pre_seeded() {
        const LINE: &str = "b=1 a-b=2 x=3";
        const STREAM: &[(&str, &str)] = &[("s", "m")];
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt s="nosuch""#, LINE, STREAM),
            sorted_pairs(&[("s", "m")]),
            "a colliding identifier is skipped by the pre-seed, not renamed by it"
        );
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt s="b""#, LINE, STREAM),
            sorted_pairs(&[("s", "m"), ("s_extracted", "1")])
        );
    }

    /// **The tie-break, and why it is OURS.** Where two identifiers share
    /// one source key the reference iterates a Go map (`for id, orig :=
    /// range keys`, `parser.go:594`) and has no answer: measured on
    /// `grafana/loki:3.7.4`, 30 repetitions of one query against one
    /// pushed line split **25 / 5** between `{a="3", b=""}` and
    /// `{a="", b="3"}`, and a second shape split **21 / 9**. (Command in
    /// the `logfmt-expression-duplicate-source-key-tiebreak` ledger
    /// entry; an independent earlier run of the same shapes split 23/7
    /// and 16/14, which is the same finding.)
    ///
    /// We take QUERY ORDER — the one order a user can predict from what
    /// they wrote. This is a determinism we are ADDING, not a divergence
    /// we are choosing; do not "fix" it back toward a coin flip.
    #[test]
    fn two_identifiers_sharing_a_source_key_are_broken_by_query_order() {
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt a="x", b="x""#, "x=3 y=4", &[]),
            sorted_pairs(&[("a", "3"), ("b", "")])
        );
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt b="x", a="x""#, "x=3 y=4", &[]),
            sorted_pairs(&[("a", ""), ("b", "3")]),
            "swapping the declarations swaps the winner — that is what makes it query order"
        );
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt a="x", b="y", c="x""#, "x=3 y=4", &[]),
            sorted_pairs(&[("a", "3"), ("b", "4"), ("c", "")])
        );
    }

    /// **A strict decoder error does not swallow the pre-seed.** The
    /// seeding happens before `l.dec.Reset(line)` (`parser.go:545-552`),
    /// so the empty labels survive a line that fails to decode at all.
    /// Container-measured over `=x b=1`: `| logfmt --strict a="b"`
    /// answers `__error__="LogfmtParserErr"` **and** `a=""`; `ec774ee`
    /// answered with the error alone.
    ///
    /// The lenient half stays divergent and that is deliberate — the
    /// reference's lenient decoder RESUMES after a recoverable error
    /// (`parser.go:564-571`) and ours stops, which is issue #200's
    /// ground, not this one's. So `| logfmt a="b"` over this line is
    /// `a=""` here and `a="1"` there, and this test pins OUR answer
    /// rather than pretending the gap is closed.
    #[test]
    fn a_pre_seeded_identifier_survives_a_strict_decoder_error() {
        let got = logfmt_labels(r#"{s="m"} | logfmt --strict a="b""#, "=x b=1", &[]);
        assert!(
            got.contains(&("a".to_string(), String::new())),
            "the pre-seeded empty label must survive the error: {got:?}"
        );
        assert!(
            got.contains(&("__error__".to_string(), "LogfmtParserErr".to_string())),
            "{got:?}"
        );
        assert_eq!(
            logfmt_labels(r#"{s="m"} | logfmt a="b""#, "=x b=1", &[]),
            sorted_pairs(&[("a", "")]),
            "lenient recovery is issue #200's ground; the reference answers `a=\"1\"` here"
        );
    }

    /// [`sanitized_key_eq`] IS [`sanitize_label_key`], compared — the two
    /// arms must not drift apart, because the bare arm renames a line key
    /// through the second while the targeted arm routes it through the
    /// first.
    ///
    /// Replacing the non-alphanumeric branch's `'_'` with the character
    /// itself reddens this on `a-b`; dropping the leading-digit `_`
    /// reddens it on `1x`.
    #[test]
    fn sanitized_key_eq_agrees_with_sanitize_label_key() {
        let raws = [
            "b",
            "a-b",
            "a_b",
            "b.c",
            "p-q",
            "1x",
            "0",
            "_",
            "é",
            "日本",
            "a",
            "abc",
            "a1",
            "a\u{00a0}b",
            "x\u{1}y",
            " a ",
            "\ta\t",
            " ",
            "",
            "a--b",
            "__",
            "9_9",
            "aB9_",
        ];
        let targets = [
            "b", "a_b", "a-b", "b_c", "p_q", "_1x", "_0", "_", "__", "a", "abc", "a1", "a__b",
            "_9_9", "aB9_", "x_y", "",
        ];
        for raw in raws {
            for target in targets {
                let want = {
                    let trimmed = raw.trim_ascii();
                    !trimmed.is_empty() && sanitize_label_key(trimmed) == target
                };
                assert_eq!(
                    sanitized_key_eq(raw, target),
                    want,
                    "sanitized_key_eq({raw:?}, {target:?}) disagrees with \
                     sanitize_label_key({raw:?}) = {:?}",
                    sanitize_label_key(raw.trim_ascii())
                );
            }
        }
    }

    /// **Issue #392.** A non-ASCII extraction destination keeps its
    /// bytes. The destination is an IDENTIFIER written in the QUERY, not
    /// a key read out of the line, and the reference does not sanitize
    /// it — `NewLogfmtExpressionParser` only validates it
    /// (`model.UTF8Validation.IsValidLabelName(exp.Identifier)`,
    /// `pkg/logql/log/parser.go:518 @ v3.7.4`).
    ///
    /// Measured on the pinned v3.7.4 container over `ax=7 bx=8`:
    /// `sum by (éx) (count_over_time({…} | logfmt éx="ax" [1m]))` yields
    /// `{"metric":{"éx":""}}`, and `sum by (_x) (…)` over the same query
    /// yields `{"metric":{}}` — so `_x` is NOT the name there.
    ///
    /// Fails on `ff0fb09` twice over: the query does not parse, and the
    /// destination was routed through `sanitize_label_key` (which
    /// rendered `éx` as `_x`) — the second half is what [`KeyOrigin`]
    /// fixes, and reverting the `KeyOrigin::QueryIdentifier` argument to
    /// `KeyOrigin::Line` reddens this with `got ["_x"]` (run while this
    /// landed).
    #[test]
    fn a_non_ascii_extracted_label_keeps_its_bytes() {
        for (query, line) in [
            (r#"{s="m"} | logfmt éx="ax""#, "ax=7 bx=8"),
            (r#"{s="m"} | json éx="ax""#, r#"{"ax":"7","bx":"8"}"#),
        ] {
            let compiled = CompiledPipeline::compile(&stages_of(query))
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let mut out = Vec::new();
            compiled.run_into(line, &[], 0, &mut out).expect("budget");
            let names: Vec<String> = out.iter().map(|(k, _)| k.to_string()).collect();
            assert!(
                names.iter().any(|k| k == "éx"),
                "{query}: expected a label named `éx`, got {names:?}"
            );
            assert!(
                !names.iter().any(|k| k == "_x"),
                "{query}: the destination was sanitized to `_x`; the reference keeps the \
                 identifier verbatim (parser.go:518)"
            );
            assert_eq!(
                out.iter()
                    .find(|(k, _)| k == "éx")
                    .map(|(_, v)| v.to_string()),
                Some("7".to_string()),
                "{query}: the value must still come from the source key"
            );
        }
    }

    /// **The containment property for [`KeyOrigin`]** (issue #392): the
    /// carve-out cannot change any query that parsed before #392,
    /// because a query-side destination was `[A-Za-z_][A-Za-z0-9_]*` by
    /// construction until the lexer widened, and `key_needs_sanitizing`
    /// is false for every such name. Proved over the whole ASCII
    /// identifier alphabet rather than argued, so "the exemption is where
    /// the bug lives" has a failure mode here.
    #[test]
    fn sanitizing_a_query_destination_was_always_a_no_op_for_ascii_identifiers() {
        let lead: Vec<char> = ('a'..='z').chain('A'..='Z').chain(['_']).collect();
        let rest: Vec<char> = ('a'..='z')
            .chain('A'..='Z')
            .chain('0'..='9')
            .chain(['_'])
            .collect();
        for &l in &lead {
            assert!(
                !key_needs_sanitizing(&l.to_string()),
                "{l:?} is a legal pre-#392 identifier and must not need sanitizing"
            );
            for &r in &rest {
                let name = format!("{l}{r}");
                assert!(
                    !key_needs_sanitizing(&name),
                    "{name:?} is a legal pre-#392 identifier and must not need sanitizing"
                );
            }
        }
        // And the LINE origin is untouched: a line key still sanitizes.
        assert!(key_needs_sanitizing("é"));
        assert!(key_needs_sanitizing("a-b"));
        assert!(key_needs_sanitizing("3a"));
        assert_eq!(sanitize_label_key("é"), "_");
    }

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

    /// Issue #350 — the QUERY-side literal parser accepts EXACTLY the
    /// reference's 21 spellings (probed exhaustively on both v3.7.3 and
    /// v3.7.4), case-sensitive, at their humanize values.
    #[test]
    fn query_bytes_literal_accepts_exactly_the_reference_21_spellings() {
        let expected: &[(&str, f64)] = &[
            ("B", 1.0),
            ("k", 1e3),
            ("kB", 1e3),
            ("ki", 1024.0),
            ("kiB", 1024.0),
            ("K", 1e3),
            ("KB", 1e3),
            ("Ki", 1024.0),
            ("KiB", 1024.0),
            ("M", 1e6),
            ("MB", 1e6),
            ("Mi", 1024.0 * 1024.0),
            ("MiB", 1024.0 * 1024.0),
            ("G", 1e9),
            ("GB", 1e9),
            ("Gi", 1024.0 * 1024.0 * 1024.0),
            ("GiB", 1024.0 * 1024.0 * 1024.0),
            ("T", 1e12),
            ("TB", 1e12),
            ("Ti", 1024.0f64.powi(4)),
            ("TiB", 1024.0f64.powi(4)),
        ];
        assert_eq!(expected.len(), 21);
        for (suf, factor) in expected {
            assert_eq!(
                parse_query_bytes_literal(&format!("2{suf}")),
                Some(2.0 * factor),
                "2{suf}"
            );
        }
        // Every OTHER case variant of every humanize suffix rejects
        // (the probed complement): representative members of each
        // rejected class.
        // Includes every spelling the pre-#350 case_folding census
        // carried as a false "version difference" — their end-to-end
        // rejection is decided HERE, the layer that census cannot see.
        for raw in [
            "1b", "1kb", "1Kb", "1kib", "1Kib", "1KIB", "1KIb", "1kI", "1m", "1mb", "1mB", "1Mb",
            "1mi", "1mib", "1g", "1gb", "1gi", "1gib", "1t", "1tb", "1ti", "1tib", "1p", "1pb",
            "1pi", "1pib", "1P", "1PB", "1Pi", "1PiB", "1e", "1eb", "1ei", "1eib", "1E", "1EB",
            "1Ei", "1EiB", "1xb", "1BB", "1iB",
        ] {
            assert_eq!(parse_query_bytes_literal(raw), None, "{raw}");
        }
    }

    /// Issue #350 — the probed query-literal edges: fractional mantissas
    /// accept; zero-valued literals (including fractions truncating to
    /// zero) reject; overflow rejects; comma/space shapes reject.
    #[test]
    fn query_bytes_literal_edges_match_the_probed_reference() {
        // Fractional forms — probed 200.
        assert_eq!(parse_query_bytes_literal("1.5KiB"), Some(1_536.0));
        assert_eq!(parse_query_bytes_literal("1.KiB"), Some(1_024.0));
        assert_eq!(parse_query_bytes_literal(".5KiB"), Some(512.0));
        assert_eq!(parse_query_bytes_literal("01B"), Some(1.0));
        assert_eq!(
            parse_query_bytes_literal("17TiB"),
            Some(17.0 * 1024.0f64.powi(4))
        );
        // Zero-valued — probed 400 on both versions (the reference's
        // frontend re-renders the threshold as `0B` and cannot re-parse
        // its own rendering).
        for raw in ["0B", "0KB", "0KiB", "0k", "0Ti", "0.5B", ".5B"] {
            assert_eq!(parse_query_bytes_literal(raw), None, "{raw}");
        }
        // Overflow — probed 400 (`unexpected $end` after the lexer
        // refuses the token).
        assert_eq!(parse_query_bytes_literal("999999999TiB"), None);
        // Comma/space shapes — probed 400 (never one token upstream;
        // never one token in our lexer either — belt and braces here).
        for raw in ["1,024B", "1 KiB", "3 kB", "1KiB2", "1B1B"] {
            assert_eq!(parse_query_bytes_literal(raw), None, "{raw}");
        }
        // The VALUE-side parser deliberately still takes the full
        // humanize table — the split IS the reference's behaviour.
        assert_eq!(parse_bytes_value("1kb"), Some(1_000.0));
        assert_eq!(parse_bytes_value("1eb"), Some(1e18));
        assert_eq!(parse_bytes_value("0KB"), Some(0.0));
    }

    /// Issue #350 — the classify seam: a rejected query spelling is the
    /// named literal rejection (never a silent 0 and never the old
    /// humanize acceptance), a duration keeps winning the ambiguous
    /// spellings (`1m` is a minute, never a megabyte), and accepted
    /// byte spellings classify as bytes.
    #[test]
    fn classify_routes_query_byte_literals_through_the_strict_set() {
        match classify_numeric_literal(&NumericLiteral::DurationOrBytes("1KiB".to_string())) {
            Ok((UnitKind::Bytes, v)) => assert_eq!(v, 1024.0),
            other => panic!("expected bytes 1024, got {other:?}"),
        }
        match classify_numeric_literal(&NumericLiteral::DurationOrBytes("1m".to_string())) {
            Ok((UnitKind::Duration, v)) => assert_eq!(v, 60.0),
            other => panic!("expected duration 60s, got {other:?}"),
        }
        for raw in ["1b", "1kb", "1pb", "1024b", "0B"] {
            match classify_numeric_literal(&NumericLiteral::DurationOrBytes(raw.to_string())) {
                Err(PipelineError::BadParserExpr(msg)) => assert!(
                    msg.contains("is neither a duration nor a bytes quantity"),
                    "{raw}: {msg}"
                ),
                other => panic!("{raw}: expected the literal rejection, got {other:?}"),
            }
        }
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
        let _ = compiled.run_metric_into("d=250ms latency=2s", &base, 0, None, &mut labels);
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
            .run_metric_into("took=250ms level=info", &base, 0, None, &mut labels)
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

    // -----------------------------------------------------------------
    // Issue #344 — the range-aggregation grouping projection.
    // -----------------------------------------------------------------

    /// Runs one line through an unwrapping pipeline under `grouping` and
    /// returns the sorted final label set. `logfmt` puts `v` and `region`
    /// in the label set; `app` comes from the base (stream) labels.
    fn grouped_labels(grouping: Option<&RangeGrouping>, body: &str) -> Vec<(String, String)> {
        let compiled = CompiledPipeline::compile(&stages_of(
            r#"max_over_time({a="b"} | logfmt | unwrap v [5m])"#,
        ))
        .unwrap();
        let base = vec![("app".to_string(), "x".to_string())];
        let mut labels = Vec::new();
        let MetricRun::Kept { .. } = compiled
            .run_metric_into(body, &base, 0, grouping, &mut labels)
            .expect("no budget breach")
        else {
            panic!("expected the line to be kept");
        };
        let mut out: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        out.sort();
        out
    }

    fn pairs(kv: &[(&str, &str)]) -> Vec<(String, String)> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn by(names: &[&str]) -> RangeGrouping {
        RangeGrouping::from_ast(&pulsus_logql::Grouping {
            kind: pulsus_logql::GroupingKind::By,
            labels: names.iter().map(|s| s.to_string()).collect(),
        })
    }

    fn without(names: &[&str]) -> RangeGrouping {
        RangeGrouping::from_ast(&pulsus_logql::Grouping {
            kind: pulsus_logql::GroupingKind::Without,
            labels: names.iter().map(|s| s.to_string()).collect(),
        })
    }

    /// Every arm of the projection, against the reference's rules —
    /// `GroupedLabels` (`pkg/logql/log/labels.go:664-688 @ v3.7.4`) over
    /// the group list `LabelExtractorWithStages` normalises
    /// (`pkg/logql/log/metrics_extraction.go:154-158 @ v3.7.4`). The
    /// container-captured end-to-end evidence for the same rules is
    /// `b18_range_agg_grouping.test`; this pins them at the one place
    /// they are implemented.
    #[test]
    fn the_range_grouping_projection_matches_every_reference_arm() {
        const BODY: &str = "v=1 region=eu";

        // No grouping: today's behaviour, and the reference's
        // `without (v)` default — the unwrapped label is deleted, nothing
        // else is.
        assert_eq!(
            grouped_labels(None, BODY),
            pairs(&[("app", "x"), ("region", "eu")])
        );

        // `without ()` is the identity: `Grouping.Noop()` (ast.go:1544)
        // normalises to the same `without (v)` the ungrouped form takes.
        assert_eq!(
            grouped_labels(Some(&without(&[])), BODY),
            grouped_labels(None, BODY)
        );

        // `without (L)` drops L AND the unwrapped label.
        assert_eq!(
            grouped_labels(Some(&without(&["region"])), BODY),
            pairs(&[("app", "x")])
        );

        // `by (L)` keeps exactly L…
        assert_eq!(
            grouped_labels(Some(&by(&["region"])), BODY),
            pairs(&[("region", "eu")])
        );

        // …including the UNWRAPPED label, which `by` does not delete —
        // the case that would silently vanish if the projection sat
        // beside the unwrap deletion instead of subsuming it. Captured:
        // `max_over_time(… | unwrap v [5m]) by (v)` answers `{v="1"} 1`.
        assert_eq!(
            grouped_labels(Some(&by(&["v"])), BODY),
            pairs(&[("v", "1")])
        );

        // An absent name is NOT materialised as `name=""`.
        assert_eq!(grouped_labels(Some(&by(&["nosuch"])), BODY), pairs(&[]));

        // `by ()` is `Singleton()` ⇒ `noLabels` ⇒ the empty set, which
        // short-circuits ahead of every other arm (labels.go:669-671).
        assert_eq!(grouped_labels(Some(&by(&[])), BODY), pairs(&[]));

        // Duplicates dedupe in both directions (issue #288's rule,
        // re-confirmed for this construct against the container).
        assert_eq!(
            grouped_labels(Some(&by(&["region", "region"])), BODY),
            grouped_labels(Some(&by(&["region"])), BODY)
        );
        assert_eq!(
            grouped_labels(Some(&without(&["region", "region"])), BODY),
            grouped_labels(Some(&without(&["region"])), BODY)
        );

        // The normalisation itself: `by ()` is the Singleton arm, not an
        // empty `By` list (which would keep nothing for a different and
        // accidental reason).
        assert_eq!(by(&[]), RangeGrouping::Singleton);
        assert_eq!(without(&[]), RangeGrouping::Without(Vec::new()));
        assert_eq!(
            by(&["b", "a", "b"]),
            RangeGrouping::By(vec!["a".to_string(), "b".to_string()]),
            "the names are sorted and deduplicated once, at plan time"
        );
    }

    /// `GroupedLabels` returns the UNGROUPED set when `HasErr()`
    /// (`labels.go:665-668 @ v3.7.4`: "before applying grouping otherwise
    /// the error might get lost"). For PulsusDB that is load-bearing
    /// rather than cosmetic: `check_surviving_error` fails the query on a
    /// nonempty `__error__`, and a `by (region)` projection would
    /// otherwise DROP `__error__` and turn a named 400 into a silently
    /// wrong answer.
    #[test]
    fn an_errored_line_keeps_its_ungrouped_labels_so_the_error_survives_grouping() {
        let compiled = CompiledPipeline::compile(&stages_of(
            r#"max_over_time({a="b"} | logfmt | unwrap duration(took) [5m])"#,
        ))
        .unwrap();
        let base = vec![("app".to_string(), "x".to_string())];
        for grouping in [None, Some(&by(&["region"])), Some(&without(&["region"]))] {
            let mut labels = Vec::new();
            let MetricRun::Kept { .. } = compiled
                .run_metric_into("took=abc region=eu", &base, 0, grouping, &mut labels)
                .expect("no budget breach")
            else {
                panic!("a failed conversion keeps the line");
            };
            assert!(
                labels
                    .iter()
                    .any(|(k, v)| k == ERROR_LABEL && v == SAMPLE_EXTRACTION_ERROR),
                "the error must survive the projection for {grouping:?}: {labels:?}"
            );
            assert!(
                labels.iter().any(|(k, _)| k == "app"),
                "the errored line's labels stay UNGROUPED for {grouping:?}: {labels:?}"
            );
        }
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
            .run_metric_into("took=abc level=warn", &base, 0, None, &mut labels)
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
                .run_metric_into("level=error", &base, 0, None, &mut labels)
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
                .run_metric_into("took=abc", &base, 0, None, &mut labels)
                .expect("no budget breach"),
            MetricRun::Dropped
        ));
        assert!(matches!(
            compiled
                .run_metric_into("took=100ms", &base, 0, None, &mut labels)
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
                .run_metric_into(body, &base, 0, None, &mut labels)
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
    // Issues #99 + #104: a compound `and`/`or` label filter can leave a
    // sibling's conversion failure unobservable — either because the
    // line is DROPPED (the reference's error goes down with the
    // discarded builder) or because `or` short-circuited past the leaf
    // that would have failed. `eval_label_filter` must never allocate
    // the detail string on those paths, and a genuinely-surviving error
    // must stay byte-exact.
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
        // `level = "warn"` is false. `and` does not short-circuit, so
        // `took > 250ms` still runs and still fails on `took=bad` — but
        // `false && true` drops the line, and the reference's error goes
        // down with the discarded builder, so no label is ever written.
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
        // `level = "info"` is true, and `or` SHORT-CIRCUITS
        // (`label_filter.go:92-94`), so `took > 250ms` never runs on
        // `took=bad` and no error is ever set.
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
            .run_metric_into("level=info took=bad", &base, 0, None, &mut labels)
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
            // `or`: `level = "warn"` is false, so no short-circuit —
            // `took > 250ms` runs, sets the error and returns true.
            r#"{a="b"} | logfmt | level = "warn" or took > 250ms"#,
            // `and`: never short-circuits — `took > 250ms` runs, sets the
            // error and returns true, and `true && true` keeps the line.
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

    /// `stream_label_count: None` — these rows are handed a pre-merged
    /// base with no recorded split, so every base entry counts as a stream
    /// label. That is exactly right for what they test (the reserved-name
    /// routing of issue #238) and is only distinguishable from the real
    /// split by a `drop` of an ORDINARY structured-metadata name followed
    /// by a re-parse, which no row here does; the category split itself is
    /// covered end to end, through the real merge, by
    /// `tests/logql_json_key_sanitization.rs` (`c13`–`c15`, `c18`).
    fn sm(err: &str, details: &str, has_ordinary: bool) -> StructuredMetadataCtx {
        StructuredMetadataCtx {
            err: err.to_string(),
            details: details.to_string(),
            has_ordinary,
            stream_label_count: None,
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

    // -----------------------------------------------------------------
    // Issue #248: a MALFORMED `ip()` label-filter pattern is rejected in
    // exactly one position and silently deferred everywhere else.
    //
    // Rule read off the reference: `NewIPLabelFilter` cannot fail — it
    // stores the error and leaves the matcher nil
    // (`pkg/logql/log/ip.go:94-103 @ v3.7.4`) — and the ONLY caller of
    // `PatternError()` is `LabelFilterExpr.Stage()`
    // (`pkg/logql/syntax/ast.go:801-809 @ v3.7.4`; `git grep -n PatternError
    // pkg/` at v3.7.4 returns its declaration at `ip.go:117-121` and that one
    // call site), which type-switches on the stage's whole filterer.
    // Post-`unwrap` filters never reach it:
    // they are reduced with `log.ReduceAndLabelFilter`
    // (`pkg/logql/syntax/extractor.go:76,187 @ v3.7.4`).
    //
    // Every accept/reject and every verdict below was also measured on
    // `grafana/loki:3.7.4` (digest
    // `sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`,
    // `discover_log_levels: false`); the corpus file
    // `tests/logqltest/corpus/b20_nested_ip.test` carries the captures.
    // -----------------------------------------------------------------

    /// The one reported position: the malformed filter IS the whole
    /// `| <label filter>` stage. Parentheses are transparent in both
    /// grammars (`syntax.y:307 @ v3.7.4`), so the wrapped form reports too.
    #[test]
    fn a_lone_malformed_ip_pipeline_stage_is_rejected() {
        for query in [
            r#"{x="y"} | logfmt | addr = ip("nope")"#,
            r#"{x="y"} | logfmt | (addr = ip("nope"))"#,
            r#"{x="y"} | logfmt | addr != ip("nope")"#,
            r#"{x="y"} | logfmt | addr = ip("")"#,
            r#"{x="y"} | logfmt | addr = ip("10.0.0.0/99")"#,
            // A second stage is still a stage of its own.
            r#"{x="y"} | logfmt | b="x" | addr = ip("nope")"#,
        ] {
            let err = CompiledPipeline::compile(&stages_of(query))
                .expect_err(query)
                .to_string();
            assert!(err.contains("ip()"), "{query}: {err}");
        }
    }

    /// Nested under any of the reference's four `labelFilter` combinators
    /// (`and`, `,`, `or`, and parenthesised nesting) the pattern error is
    /// never surfaced — the reference answers 200.
    #[test]
    fn a_malformed_ip_nested_under_and_or_comma_compiles() {
        for query in [
            r#"{x="y"} | logfmt | addr = ip("nope") and b="x""#,
            r#"{x="y"} | logfmt | b="x" and addr = ip("nope")"#,
            r#"{x="y"} | logfmt | addr = ip("nope"), b="x""#,
            r#"{x="y"} | logfmt | addr = ip("nope") or b="x""#,
            r#"{x="y"} | logfmt | b="x" or addr = ip("nope")"#,
            r#"{x="y"} | logfmt | addr != ip("nope") and b="x""#,
            r#"{x="y"} | logfmt | (addr = ip("nope") and b="x")"#,
            r#"{x="y"} | logfmt | (addr = ip("nope") or b="zzz") or b="x""#,
            // The nested form does not poison a LATER lone stage's report,
            // and a later lone stage does not un-defer the nested one.
            r#"{x="y"} | logfmt | addr = ip("nope") or b="x" | b="x""#,
        ] {
            CompiledPipeline::compile(&stages_of(query)).expect(query);
        }
    }

    /// Post-`unwrap` filters are reduced, never staged, so EVERY position
    /// is accepted there — a lone malformed filter included (measured: the
    /// reference answers 200 for `… | unwrap val | addr = ip("nope")` and
    /// 400 for the same filter placed before the `unwrap`).
    #[test]
    fn a_malformed_ip_after_unwrap_compiles_in_every_position() {
        for query in [
            r#"sum_over_time({x="y"} | logfmt | unwrap val | addr = ip("nope") [5m])"#,
            r#"sum_over_time({x="y"} | logfmt | unwrap val | (addr = ip("nope")) [5m])"#,
            r#"sum_over_time({x="y"} | logfmt | unwrap val | addr != ip("nope") [5m])"#,
            r#"sum_over_time({x="y"} | logfmt | unwrap val | addr = ip("nope") and b="x" [5m])"#,
            r#"sum_over_time({x="y"} | logfmt | unwrap val | b="x" | addr = ip("nope") [5m])"#,
        ] {
            CompiledPipeline::compile(&stages_of(query)).expect(query);
        }
        // The same filter one stage earlier — before the `unwrap` — is a
        // pipeline stage again, and reports.
        let query = r#"sum_over_time({x="y"} | logfmt | addr = ip("nope") | unwrap val [5m])"#;
        CompiledPipeline::compile(&stages_of(query)).expect_err(query);
    }

    /// The verdict on a clean entry is `false` for `=` AND `!=` alike —
    /// `ip.go`'s nil-matcher check precedes the operator switch, so `!=`
    /// does not negate to `true`. Discriminating measurement: the
    /// reference returns only the `b="y"` line for the `!=` query.
    #[test]
    fn a_malformed_nested_ip_is_false_for_both_operators() {
        let kept = |query: &str, body: &str| {
            run_sm_labels(
                query,
                body,
                &[("service_name", "ipnest")],
                &EMPTY_STRUCTURED_METADATA,
            )
            .is_some()
        };
        let eq_or = r#"{x="y"} | logfmt | addr = ip("nope") or b="x""#;
        assert!(kept(eq_or, "addr=10.1.2.3 b=x"), "the `or` arm holds");
        assert!(!kept(eq_or, "addr=10.1.2.3 b=y"), "both arms false");

        let ne_or = r#"{x="y"} | logfmt | addr != ip("nope") or b="y""#;
        assert!(
            !kept(ne_or, "addr=10.1.2.3 b=x"),
            "`!=` must NOT negate the malformed leaf to true"
        );
        assert!(kept(ne_or, "addr=192.168.1.1 b=y"), "the `or` arm holds");

        let eq_and = r#"{x="y"} | logfmt | addr = ip("nope") and b="x""#;
        assert!(!kept(eq_and, "addr=10.1.2.3 b=x"), "the `and` is false");
    }

    /// On an ERRORED entry the malformed leaf passes unconditionally, the
    /// same short-circuit a well-formed `ip()` takes (`ip.go:124-127`
    /// checks `HasErr` before the nil-matcher check).
    #[test]
    fn a_malformed_nested_ip_passes_an_errored_entry() {
        let kept = |query: &str, body: &str| {
            run_sm_labels(
                query,
                body,
                &[("service_name", "ipjson")],
                &EMPTY_STRUCTURED_METADATA,
            )
        };
        for query in [
            r#"{x="y"} | json | addr = ip("nope") or b="zzz""#,
            r#"{x="y"} | json | addr != ip("nope") or b="zzz""#,
        ] {
            let got = kept(query, "not json at all").unwrap_or_else(|| panic!("{query}"));
            assert!(
                got.iter().any(|(k, _)| k == ERROR_LABEL),
                "{query}: {got:?}"
            );
        }
        // A CLEAN entry takes the `false` branch, so the `and` drops it.
        assert_eq!(
            kept(
                r#"{x="y"} | json | addr = ip("nope") and b="zzz""#,
                r#"{"addr":"10.1.2.3","b":"x"}"#
            ),
            None
        );
    }

    // -----------------------------------------------------------------
    // Issue #248 round 2: the error state a numeric conversion sets is
    // visible to every leaf to its RIGHT in the same `| <label filter>`,
    // and `or` short-circuits so a leaf to the right of a true operand
    // never sets one. All expectations below were measured on the same
    // pinned `grafana/loki:3.7.4`; the corpus block
    // `b20_nested_ip.test` §"the error set to the LEFT of the leaf that
    // reads it" carries them as captures.
    // -----------------------------------------------------------------

    /// `run` over `{x="y"}` with the line `body`, returning the final
    /// label set, or `None` when the pipeline dropped the line.
    fn filtered_labels(query: &str, body: &str) -> Option<Vec<(String, String)>> {
        let compiled = CompiledPipeline::compile(&stages_of(query)).expect(query);
        let base = vec![("x".to_string(), "y".to_string())];
        Some(
            compiled
                .run(body, &base, 0)
                .expect("no budget breach")?
                .labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn error_of(labels: &[(String, String)]) -> Option<&str> {
        labels
            .iter()
            .find(|(k, _)| k == ERROR_LABEL)
            .map(|(_, v)| v.as_str())
    }

    /// A failed numeric conversion sets `__error__` where it happens, so
    /// an `ip()` leaf — malformed or well formed — and an `__error__`
    /// matcher to its RIGHT all see it. The entry kept is the one whose
    /// `addr` was never tested: `192.168.1.1` is NOT in `10.0.0.0/8`.
    #[test]
    fn an_error_set_left_of_a_leaf_that_reads_it_is_visible_to_that_leaf() {
        for query in [
            r#"{x="y"} | logfmt | n > 1 and addr = ip("nope")"#,
            r#"{x="y"} | logfmt | n > 1, addr = ip("nope")"#,
            r#"{x="y"} | logfmt | n > 1 and addr != ip("nope")"#,
            r#"{x="y"} | logfmt | n > 1 and addr = ip("10.0.0.0/8")"#,
            r#"{x="y"} | logfmt | n > 1 and addr != ip("10.0.0.0/8")"#,
            r#"{x="y"} | logfmt | n > 1 and __error__ = "LabelFilterErr""#,
            // The duration and bytes families set the same error.
            r#"{x="y"} | logfmt | d > 1s and addr = ip("nope")"#,
        ] {
            let labels = filtered_labels(query, "n=bad d=bad addr=192.168.1.1 b=x")
                .unwrap_or_else(|| panic!("{query}: the errored entry must survive"));
            assert_eq!(error_of(&labels), Some(LABEL_FILTER_ERROR), "{query}");
        }
        // The MIRROR IMAGE: with the operands swapped the `ip()` leaf runs
        // before the error exists, is false, and the `and` drops the line.
        assert_eq!(
            filtered_labels(
                r#"{x="y"} | logfmt | addr = ip("nope") and n > 1"#,
                "n=bad addr=192.168.1.1 b=x"
            ),
            None
        );
        // And a CLEAN entry never takes the error branch at all: n=5 > 1
        // holds, so the malformed leaf decides, and it is false.
        assert_eq!(
            filtered_labels(
                r#"{x="y"} | logfmt | n > 1 and addr = ip("nope")"#,
                "n=5 addr=10.1.2.3 b=y"
            ),
            None
        );
    }

    /// The same on the METRIC path, which runs the identical stage but
    /// through `run_metric_into`.
    #[test]
    fn an_error_set_left_of_an_ip_leaf_is_visible_to_it_on_the_metric_path() {
        let query = r#"count_over_time({x="y"} | logfmt | n > 1 and addr = ip("nope") [5m])"#;
        let compiled = CompiledPipeline::compile(&stages_of(query)).expect(query);
        let base = vec![("x".to_string(), "y".to_string())];
        let mut labels = Vec::new();
        let MetricRun::Kept { .. } = compiled
            .run_metric_into("n=bad addr=192.168.1.1 b=x", &base, 0, None, &mut labels)
            .expect("no budget breach")
        else {
            panic!("the errored entry must survive the malformed ip() leaf");
        };
        assert!(
            labels
                .iter()
                .any(|(k, v)| k == ERROR_LABEL && v == LABEL_FILTER_ERROR),
            "{labels:?}"
        );
    }

    /// `or` short-circuits (`label_filter.go:92-94`) and `and` does not,
    /// so the SAME two operands in the two orders return the same entry
    /// with and without `__error__`. This is the pair the reference was
    /// measured on.
    #[test]
    fn an_or_with_a_true_left_operand_never_evaluates_the_right_one() {
        let line = "n=bad b=x";
        let short_circuited = filtered_labels(r#"{x="y"} | logfmt | b = "x" or n > 1"#, line)
            .expect("the `or` holds on its left operand");
        assert_eq!(
            error_of(&short_circuited),
            None,
            "the right operand never ran, so nothing set an error: {short_circuited:?}"
        );
        let evaluated = filtered_labels(r#"{x="y"} | logfmt | n > 1 or b = "x""#, line)
            .expect("the failed conversion returns true, which the `or` carries");
        assert_eq!(error_of(&evaluated), Some(LABEL_FILTER_ERROR));
    }

    /// The short-circuit CHAINS. `a or b or c` parses left-deep, so `a`
    /// being true must skip `b`, `c` AND both `Or` ops — a jump that
    /// stopped one level up would land on `c` and evaluate it.
    #[test]
    fn an_or_short_circuit_chains_through_a_left_deep_chain() {
        let labels = filtered_labels(
            r#"{x="y"} | logfmt | b = "x" or c = "zzz" or n > 1"#,
            "n=bad b=x c=q",
        )
        .expect("the leftmost operand holds");
        assert_eq!(
            error_of(&labels),
            None,
            "`n > 1` sits two `or`s to the right of a true operand: {labels:?}"
        );
        // Four wide, to show the chain is not two-deep by accident.
        let labels = filtered_labels(
            r#"{x="y"} | logfmt | b = "x" or c = "zzz" or c = "qqq" or n > 1"#,
            "n=bad b=x c=q",
        )
        .expect("the leftmost operand holds");
        assert_eq!(error_of(&labels), None, "{labels:?}");
    }

    /// A RIGHT-nested `or` short-circuits within its own subtree without
    /// inheriting the enclosing one's destination — the case the chaining
    /// pass must NOT apply.
    #[test]
    fn a_right_nested_or_short_circuits_only_past_its_own_operand() {
        let labels = filtered_labels(
            r#"{x="y"} | logfmt | b = "zzz" or (c = "q" or n > 1)"#,
            "n=bad b=x c=q",
        )
        .expect("the inner `or`'s left operand holds");
        assert_eq!(
            error_of(&labels),
            None,
            "the inner `or` skipped `n > 1`: {labels:?}"
        );
        // The enclosing `or` still combines correctly when the inner one
        // is false throughout: `n > 1` runs, errors, and returns true.
        let labels = filtered_labels(
            r#"{x="y"} | logfmt | b = "zzz" or (c = "zzz" or n > 1)"#,
            "n=bad b=x c=q",
        )
        .expect("the failed conversion returns true");
        assert_eq!(error_of(&labels), Some(LABEL_FILTER_ERROR));
    }

    /// An `and` NEVER short-circuits, so its right operand runs even when
    /// the left is false — and the verdict stack survives a mixed tree.
    #[test]
    fn an_and_between_two_or_chains_keeps_the_verdict_stack_balanced() {
        // (true-by-short-circuit) and (false or true) -> kept.
        let labels = filtered_labels(
            r#"{x="y"} | logfmt | (b = "x" or n > 1) and (c = "zzz" or c = "q")"#,
            "n=bad b=x c=q",
        )
        .expect("both sides of the `and` hold");
        assert_eq!(error_of(&labels), None, "{labels:?}");
        // The same shape with the right side false drops the line.
        assert_eq!(
            filtered_labels(
                r#"{x="y"} | logfmt | (b = "x" or n > 1) and (c = "zzz" or c = "yyy")"#,
                "n=bad b=x c=q"
            ),
            None
        );
    }

    /// The FIRST error wins across a whole filter, and it is the first in
    /// EVALUATION order — the reference's `if !lbs.HasErr()` guard on a
    /// left-to-right walk. `n` fails before `d` does, so the detail is
    /// the float message.
    #[test]
    fn the_first_conversion_failure_in_evaluation_order_owns_the_detail() {
        let labels = filtered_labels(r#"{x="y"} | logfmt | n > 1 and d > 1s"#, "n=bad d=bad")
            .expect("both operands return true");
        assert_eq!(error_of(&labels), Some(LABEL_FILTER_ERROR));
        assert_eq!(
            labels
                .iter()
                .find(|(k, _)| k == ERROR_DETAILS_LABEL)
                .map(|(_, v)| v.as_str()),
            Some("strconv.ParseFloat: parsing \"bad\": invalid syntax"),
            "{labels:?}"
        );
        let labels = filtered_labels(r#"{x="y"} | logfmt | d > 1s and n > 1"#, "n=bad d=bad")
            .expect("both operands return true");
        assert_eq!(
            labels
                .iter()
                .find(|(k, _)| k == ERROR_DETAILS_LABEL)
                .map(|(_, v)| v.as_str()),
            Some("time: invalid duration \"bad\""),
            "{labels:?}"
        );
    }

    /// `or_jumps` is EMPTY unless the program contains an `Or` — the
    /// common shapes must not pay an allocation for a table of sentinels.
    #[test]
    fn or_jumps_is_allocated_only_for_programs_that_contain_an_or() {
        let program = |query: &str| {
            let compiled = CompiledPipeline::compile(&stages_of(query)).expect(query);
            compiled
                .stages
                .iter()
                .find_map(|s| match s {
                    CompiledStage::LabelFilter(f) => Some(f.clone()),
                    _ => None,
                })
                .expect("a label-filter stage")
        };
        for query in [
            r#"{x="y"} | logfmt | b = "x""#,
            r#"{x="y"} | logfmt | b = "x" and n > 1"#,
            r#"{x="y"} | logfmt | b = "x" and (n > 1 and c = "q")"#,
        ] {
            assert!(program(query).or_jumps.is_empty(), "{query}");
        }
        // `a or b or c` -> [a, b, Or, c, Or]: `a` chains past BOTH ops to
        // the end, and the inner `Or` (index 2) jumps past the outer one.
        let f = program(r#"{x="y"} | logfmt | b = "x" or c = "q" or n > 1"#);
        assert_eq!(f.ops.len(), 5);
        assert_eq!(f.or_jumps, vec![5, NO_JUMP, 5, NO_JUMP, NO_JUMP]);
        // `a or (b or c)` -> [a, b, c, Or, Or]: the inner `Or` is a RIGHT
        // operand, so its left child jumps only past the inner op.
        let f = program(r#"{x="y"} | logfmt | b = "x" or (c = "q" or n > 1)"#);
        assert_eq!(f.ops.len(), 5);
        assert_eq!(f.or_jumps, vec![5, 4, NO_JUMP, NO_JUMP, NO_JUMP]);
    }

    // -----------------------------------------------------------------
    // Issue #334 — the parsed-key collision rules that need the
    // stream/structured-metadata split. Every expected set below is a
    // literal capture from grafana/loki:3.7.4 (buildinfo revision
    // b318f282), taken by pushing the row and reading `query_range`
    // back; the non-metadata cells live in
    // `tests/logqltest/corpus/b21_key_collisions.test`, which the live
    // replay leg re-checks against the same image.
    // -----------------------------------------------------------------

    /// Runs `query` over `body` with a stream label set and a row's
    /// ORDINARY structured metadata, merged the way the read path merges
    /// them and with the split recorded, so the reference's two
    /// different collision reads are both reachable. Returns the sorted
    /// emitted label set.
    fn run_split_labels(
        query: &str,
        body: &str,
        stream: &[(&str, &str)],
        sm: &[(&str, &str)],
    ) -> Vec<(String, String)> {
        let mut base: Vec<(String, String)> = stream
            .iter()
            .chain(sm.iter())
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        // No case here has a stream/metadata name collision, so the
        // concatenation is what the merge would build.
        base.dedup_by(|a, b| a.0 == b.0);
        let ctx = StructuredMetadataCtx {
            err: String::new(),
            details: String::new(),
            has_ordinary: !sm.is_empty(),
            stream_label_count: Some(stream.len()),
        };
        let compiled = CompiledPipeline::compile(&stages_of(query)).expect(query);
        let mut labels = Vec::new();
        compiled
            .run_into_with_sm(body, &base, 0, &ctx, &mut labels)
            .expect("no budget breach")
            .expect("no query here drops its line");
        let mut got: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        got.sort();
        got
    }

    /// A LIVE structured-metadata name forces the rename, and the repeat
    /// then resolves to the same suffixed name and loses to the first —
    /// the rename runs BEFORE the already-extracted test
    /// (`parser.go:150-158 @ v3.7.4`).
    #[test]
    fn a_live_structured_metadata_collision_renames_then_keeps_the_first() {
        assert_eq!(
            run_split_labels(r#"{x="y"} | json"#, r#"{"m":1,"m":2}"#, &[], &[("m", "MV")]),
            vec![
                ("m".to_string(), "MV".to_string()),
                ("m_extracted".to_string(), "1".to_string()),
            ]
        );
    }

    /// Dropping a structured-metadata name empties the LIVE category, so
    /// the rename stops firing and the leaf lands under the bare name —
    /// while a stream label dropped the same way keeps renaming, because
    /// `BaseHas` reads a set `Del` cannot touch.
    #[test]
    fn dropping_metadata_frees_the_name_dropping_a_stream_label_does_not() {
        assert_eq!(
            run_split_labels(
                r#"{x="y"} | drop m | json"#,
                r#"{"m":1}"#,
                &[],
                &[("m", "MV")]
            ),
            vec![("m".to_string(), "1".to_string())]
        );
        assert_eq!(
            run_split_labels(
                r#"{x="y"} | drop m | json"#,
                r#"{"m":1}"#,
                &[("m", "SV")],
                &[]
            ),
            vec![("m_extracted".to_string(), "1".to_string())]
        );
    }

    /// …and the name the freed leaf took is then EXTRACTED, so a second
    /// parser finds it taken and adds nothing.
    #[test]
    fn a_name_freed_by_dropping_metadata_is_extracted_once_a_parser_takes_it() {
        assert_eq!(
            run_split_labels(
                r#"{x="y"} | drop m | json | json"#,
                r#"{"m":1}"#,
                &[],
                &[("m", "MV")]
            ),
            vec![("m".to_string(), "1".to_string())]
        );
    }
}
