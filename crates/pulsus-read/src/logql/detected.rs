//! The pure field-detection core for `/api/logs/v1/detected_labels` and
//! `/api/logs/v1/detected_fields` (issue #170, docs/api.md §2.6) —
//! hermetic, no ClickHouse access. Semantics pinned against the repo's
//! interop reference at its pinned tag:
//!
//! - [`STATIC_DETECTED_LABELS`] + the ID-likeness keep rule (the SQL half
//!   lives in [`super::sql::detected_labels`]'s `non_id_values` predicate);
//! - [`determine_type`]'s closed six-type set and its pinned detection
//!   order (int → float → boolean → duration → bytes → string), reusing
//!   the already-oracle-verified unit converters
//!   [`super::pipeline::parse_duration_seconds`] /
//!   [`super::pipeline::parse_bytes_value`];
//! - [`auto_parse_into`]'s json-first / logfmt-fallback per-line detection
//!   (success = the parser set no `__error__` label — the reference's
//!   `HasErr()` analog), evaluated via the SAME [`CompiledPipeline`]
//!   parser stages the query path runs;
//! - [`FieldAccumulator`]'s first-seen field cap, exact cardinality
//!   (documented improvement over the reference's hyperloglog sketch —
//!   registered as `detected-cardinality-exact-not-estimated` in
//!   docs/benchmarks/logs-differential-ledger.md), per-observation type
//!   re-detection (last observation wins, matching the reference's
//!   per-entry re-detect), encounter-order deduped parser attribution,
//!   and — issue #244 — a server-side byte ceiling
//!   ([`MAX_DETECTED_FIELD_BYTES`]) charged BEFORE every retaining
//!   allocation; a refused charge freezes the field's value set and keeps
//!   serving (`retention_capped`, surfaced as `pulsus_partial`), never an
//!   error.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use pulsus_logql::{ParserStage, Stage};

use super::pipeline::{
    CompiledPipeline, ERROR_DETAILS_LABEL, ERROR_LABEL, parse_bytes_value, parse_duration_seconds,
};

/// One `/detected_labels` response entry: a kept stream-index key and its
/// exact value cardinality over the query window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedLabelOut {
    pub label: String,
    pub cardinality: u64,
}

/// One `/detected_fields` response entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedFieldOut {
    pub label: String,
    /// One of the pinned closed set: `string` | `int` | `float` |
    /// `boolean` | `duration` | `bytes`.
    pub field_type: &'static str,
    /// Exact distinct-value count over the sampled entries (the reference
    /// reports a hyperloglog estimate — documented improvement).
    pub cardinality: u64,
    /// `"json"`/`"logfmt"` in encounter order, deduped; empty for fields
    /// observed only without parser attribution (structured metadata /
    /// query-pipeline extractions).
    pub parsers: Vec<&'static str>,
}

/// A `/detected_fields` engine result (issue #170 plan v2): `truncated`
/// is set only when the fetch-until-limit paging loop stopped because the
/// byte scan budget was spent; `retention_capped` (issue #244) when the
/// [`MAX_DETECTED_FIELD_BYTES`] retention budget refused at least one
/// distinct value or field name, so some `cardinality` may under-report.
/// Either is surfaced as the additive `pulsus_partial` response key
/// (omitted when false, the #90 wire convention).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedFields {
    pub fields: Vec<DetectedFieldOut>,
    pub truncated: bool,
    pub retention_capped: bool,
}

/// Labels the reference always keeps regardless of ID-likeness
/// (`containsAllIDTypes` is only consulted for non-static labels).
pub(super) const STATIC_DETECTED_LABELS: [&str; 4] = ["cluster", "namespace", "instance", "pod"];

/// `true` iff `key` is one of the reference's always-kept static labels.
pub(super) fn is_static_detected_label(key: &str) -> bool {
    STATIC_DETECTED_LABELS.contains(&key)
}

/// Go `strconv.ParseBool`'s token set minus `"1"`/`"0"` — those are
/// unreachable here because the int check runs first in the pinned
/// detection order.
const BOOL_TOKENS: [&str; 10] = [
    "t", "T", "TRUE", "true", "True", "f", "F", "FALSE", "false", "False",
];

/// The pinned six-type detection, in the reference's exact order:
/// int → float → boolean → duration → bytes → string. Duration/bytes
/// reuse the oracle-verified label-filter converters (the reference's own
/// detection calls the same `time.ParseDuration`/`humanize.ParseBytes`
/// family those were pinned against; residual margins — hex floats,
/// `d`/`w` duration suffixes, spaced byte quantities — are documented in
/// docs/api.md §2.6).
pub(super) fn determine_type(value: &str) -> &'static str {
    if value.parse::<i64>().is_ok() {
        return "int";
    }
    if value.parse::<f64>().is_ok() {
        return "float";
    }
    if BOOL_TOKENS.contains(&value) {
        return "boolean";
    }
    if parse_duration_seconds(value).is_some() {
        return "duration";
    }
    if parse_bytes_value(value).is_some() {
        return "bytes";
    }
    "string"
}

/// Per-request byte budget over everything [`FieldAccumulator`] RETAINS
/// (issue #244). A count cap is not a byte bound (#227), so this is
/// denominated in bytes and charged BEFORE each retaining allocation with
/// the SAME model as `super::exec::{alloc_block_bytes, grown_alloc_bytes,
/// map_entry_bytes}`. 64 MiB was the house per-query retained-state
/// ceiling when #244 chose it; issue #236 later raised
/// `super::exec::MAX_CLIENT_AGG_GROUP_BYTES` to 256 MiB for a reason
/// specific to the aggregation GROUP axis, which does not apply here, so
/// this stays an independent 64 MiB (a literal, never a derived link —
/// the same treatment `super::template::MAX_TEMPLATE_RENDER_BYTES` got).
pub const MAX_DETECTED_FIELD_BYTES: u64 = 64 * 1024 * 1024;

/// Check-then-add, clamp-never-error. A REFUSED charge does not mutate
/// `charged` — the `traces::exec::ByteBudget` contract (a failed charge
/// never carries a phantom byte for an allocation that was refused before
/// it happened); unlike that budget, a refusal here CLAMPS (sets `capped`)
/// instead of erroring — the #236 lesson (a new rejection surface is its
/// own bug).
#[derive(Debug)]
pub(super) struct RetentionBudget {
    charged: u64,
    peak: u64,
    budget: u64,
    capped: bool,
}

impl RetentionBudget {
    fn new(budget: u64) -> Self {
        Self {
            charged: 0,
            peak: 0,
            budget,
            capped: false,
        }
    }

    /// `true` = the charge was accepted (and `charged` grew by `bytes`);
    /// `false` = refused, `charged` unchanged, `capped` set.
    fn charge(&mut self, bytes: u64) -> bool {
        let next = self.charged.saturating_add(bytes);
        if next > self.budget {
            self.capped = true;
            return false;
        }
        self.charged = next;
        self.peak = self.peak.max(next);
        true
    }

    fn capped(&self) -> bool {
        self.capped
    }

    fn charged(&self) -> u64 {
        self.charged
    }

    fn peak_charged(&self) -> u64 {
        self.peak
    }
}

/// One detected field's accumulating state.
#[derive(Debug)]
struct FieldState {
    field_type: &'static str,
    values: HashSet<String>,
    parsers: Vec<&'static str>,
    /// The budget refused a growth: type still re-detects and parsers still
    /// append (both bounded), the VALUE set stops growing.
    frozen: bool,
}

/// A provable upper bound on the retained heap one admitted field NAME
/// costs: the map entry's table share, the owned key `String` (map
/// insertion may route through growth paths, so the geometric
/// [`super::exec::grown_alloc_bytes`] bound is used), and the `parsers`
/// vector's `with_capacity(2)` buffer — which NEVER reallocates because
/// the parser universe is the closed pair `json`/`logfmt`.
fn field_entry_bytes(name: &str) -> u64 {
    super::exec::map_entry_bytes(size_of::<(String, FieldState)>())
        .saturating_add(super::exec::grown_alloc_bytes(name.len() as u64))
        .saturating_add(super::exec::alloc_block_bytes(
            2 * size_of::<&'static str>() as u64,
        ))
}

/// `HashSet<String>` is `HashMap<String, ()>`; `()` is a ZST, so the slot is
/// `size_of::<String>()` and [`super::exec::map_entry_bytes`] applies
/// verbatim. `to_string()` reserves EXACTLY the length —
/// [`super::exec::alloc_block_bytes`] is the precedented bound
/// (`label_set_bytes`).
fn value_entry_bytes(value: &str) -> u64 {
    super::exec::map_entry_bytes(size_of::<String>())
        .saturating_add(super::exec::alloc_block_bytes(value.len() as u64))
}

/// The post-admission tail of [`FieldAccumulator::observe_pair`], factored
/// free so the `&mut` budget and the `&mut` field state (both reached
/// through `self`) can be borrowed simultaneously. The ORDER is the
/// contract: the charge precedes the `value.to_string()` clone.
fn observe_admitted(
    budget: &mut RetentionBudget,
    state: &mut FieldState,
    value: &str,
    parser: Option<&'static str>,
) {
    state.field_type = determine_type(value);
    if !state.frozen && !state.values.contains(value) {
        if budget.charge(value_entry_bytes(value)) {
            state.values.insert(value.to_string());
        } else {
            state.frozen = true; // clamp + serve
        }
    }
    if let Some(p) = parser
        && !state.parsers.contains(&p)
    {
        state.parsers.push(p);
    }
}

/// Accumulates detected fields across sampled entries: the first
/// `field_limit` distinct field names win (later names are skipped
/// entirely, values uncounted — the reference's `fieldCount < limit`
/// gate), each observation re-detects the type (last wins) and inserts
/// the exact value, and parser attribution appends deduped in encounter
/// order. Everything it RETAINS is charged against a byte budget BEFORE
/// the allocation (issue #244; [`MAX_DETECTED_FIELD_BYTES`]).
#[derive(Debug)]
pub(super) struct FieldAccumulator {
    field_limit: u32,
    fields: HashMap<String, FieldState>,
    budget: RetentionBudget,
}

impl FieldAccumulator {
    pub(super) fn new(field_limit: u32) -> Self {
        Self::with_byte_budget(field_limit, MAX_DETECTED_FIELD_BYTES)
    }

    /// Test seam: a caller-chosen retention budget (production always uses
    /// [`MAX_DETECTED_FIELD_BYTES`] via [`FieldAccumulator::new`]).
    pub(super) fn with_byte_budget(field_limit: u32, budget: u64) -> Self {
        Self {
            field_limit,
            fields: HashMap::new(),
            budget: RetentionBudget::new(budget),
        }
    }

    /// Structured-metadata pairs: fields with no parser attribution.
    pub(super) fn observe_structured_metadata(&mut self, pairs: &[(String, String)]) {
        self.observe_parsed(pairs, None);
    }

    /// Parsed pairs — from the query pipeline's own extractions
    /// (`parser = None`) or from [`auto_parse_into`]'s json/logfmt
    /// detection (`parser = Some(...)`).
    pub(super) fn observe_parsed(
        &mut self,
        pairs: &[(String, String)],
        parser: Option<&'static str>,
    ) {
        for (key, value) in pairs {
            self.observe_pair(key, value, parser);
        }
    }

    /// One observed `key = value` pair, borrowed — nothing is cloned until
    /// the retention budget has approved the bytes (issue #244).
    /// `__error__`/`__error_details__` never become fields.
    pub(super) fn observe_pair(&mut self, key: &str, value: &str, parser: Option<&'static str>) {
        if key == ERROR_LABEL || key == ERROR_DETAILS_LABEL {
            return;
        }
        if !self.fields.contains_key(key) {
            if self.fields.len() >= self.field_limit as usize {
                return;
            }
            // Charge BEFORE `key.to_string()`. A refused name is absent
            // entirely — never inserted, never counted. (The
            // `contains_key` + `get_mut` double lookup is deliberate: the
            // entry API would demand an owned key before the charge.)
            if !self.budget.charge(field_entry_bytes(key)) {
                return;
            }
            self.fields.insert(
                key.to_string(),
                FieldState {
                    field_type: "string",
                    values: HashSet::new(),
                    parsers: Vec::with_capacity(2),
                    frozen: false,
                },
            );
        }
        // Present by construction: either it already existed or the
        // insert above just admitted it.
        let Some(state) = self.fields.get_mut(key) else {
            return;
        };
        observe_admitted(&mut self.budget, state, value, parser);
    }

    /// Bytes the retention budget has accepted so far.
    pub(super) fn charged(&self) -> u64 {
        self.budget.charged()
    }

    /// The high-water mark of [`FieldAccumulator::charged`] (identical to
    /// it today — nothing discharges — kept as the named runtime term the
    /// witness gates bound).
    pub(super) fn peak_charged(&self) -> u64 {
        self.budget.peak_charged()
    }

    /// Final response entries, sorted by label (deterministic wire order —
    /// a documented divergence from the reference's Go map order), plus
    /// whether the retention budget ever refused a charge (issue #244).
    ///
    /// Allocates the response `Vec<DetectedFieldOut>` — RESPONSE-scoped, at
    /// most `field_limit` (<= 5000) entries, once per request. Outside every
    /// per-row window; the budget covers what is RETAINED during
    /// accumulation, and this vector is the accumulation's output, not part
    /// of it.
    pub(super) fn finish(self) -> (Vec<DetectedFieldOut>, bool) {
        let capped = self.budget.capped();
        let mut out: Vec<DetectedFieldOut> = self
            .fields
            .into_iter()
            .map(|(label, state)| DetectedFieldOut {
                label,
                field_type: state.field_type,
                cardinality: state.values.len() as u64,
                parsers: state.parsers,
            })
            .collect();
        out.sort_by(|a, b| a.label.cmp(&b.label));
        (out, capped)
    }
}

/// A bare full-flatten parser stage compiled once per process — compiling
/// a parser with no extractions/regexes cannot fail (no user input
/// reaches the compiler), so the `expect` is a documented invariant.
static JSON_PARSER: LazyLock<CompiledPipeline> = LazyLock::new(|| {
    CompiledPipeline::compile(&[Stage::Parser(ParserStage::Json {
        extractions: Vec::new(),
    })])
    .expect("a bare json parser stage always compiles")
});

static LOGFMT_PARSER: LazyLock<CompiledPipeline> = LazyLock::new(|| {
    CompiledPipeline::compile(&[Stage::Parser(ParserStage::Logfmt {
        // Lenient default (issue #200): auto-detection mirrors the
        // reference's default `| logfmt` — a malformed line best-effort
        // extracts and never sets `__error__`.
        strict: false,
        keep_empty: false,
        extractions: Vec::new(),
    })])
    .expect("a bare logfmt parser stage always compiles")
});

/// Json-first / logfmt-fallback auto-detection on one (post-pipeline)
/// line, via [`CompiledPipeline`] over a bare parser stage — success = the
/// parser finished with the OUT-OF-BAND err slot unset (the reference's
/// `HasErr()`, surfaced by `run_into_reporting_err`; issue #238 review
/// round 7): try json; on failure reset and try logfmt; on failure the
/// entry contributes no auto-parsed fields. Deliberately NOT a scan of the
/// emitted labels for the `__error__` NAME: a parser-extracted ordinary
/// label literally named `__error__` (`parser.go:160`, `Set(ParsedLabel,
/// …)` — never the slot) leaves `HasErr()` false and must not fail the
/// detection. Reference-captured (`grafana/loki:3.7.4`,
/// `discover_log_levels: false`): `{"__error__":"","foo":"x"}` AND
/// `{"__error__":"mine","foo":"x"}` both detect `foo` as a json field
/// (and neither surfaces `__error__` itself as a field —
/// [`FieldAccumulator::observe_pair`]'s name exclusion matches that);
/// `not json at all` yields no fields. Writes the winning parser's
/// extracted pairs into the caller's reused scratch (`run_into` clears it
/// per attempt) and returns the parser name; on failure `out` is left
/// cleared (issue #244 — no per-row owned `Vec<(String, String)>` is
/// built; a slot `__error_details__` never leaks — an erroring parser is
/// a failure wholesale).
pub(super) fn auto_parse_into<'l>(
    line: &'l str,
    out: &mut Vec<(Cow<'l, str>, Cow<'l, str>)>,
) -> Option<&'static str> {
    for (name, parser) in [("json", &*JSON_PARSER), ("logfmt", &*LOGFMT_PARSER)] {
        // Auto-detection probes carry no row timestamp; the probe
        // pipelines are bare parsers (no templates), so 0 is inert and a
        // template render-budget breach is unreachable — a defensive Err
        // maps to "not this format" (plan v1 §4: detected.rs passes 0).
        let (kept, has_err) = parser
            .run_into_reporting_err(line, &[], 0, out)
            .unwrap_or((None, false));
        if kept.is_some() && !has_err {
            return Some(name);
        }
    }
    out.clear();
    None
}

/// AC 13's baseline ONLY (issue #244): the pre-#244 owned-return form of
/// the auto-parse pass — byte-for-byte the shipped `auto_parse` at merge
/// base `a627a6c` (== the plan-pinned `d145ded` `detected.rs:241-254` plus
/// the #230 timestamp argument and defensive `unwrap_or` that landed with
/// `a627a6c`), including its per-row `let mut labels = Vec::new()` and the
/// owned `(String, String)` pair collect. Never called by any production
/// path (AC 13e); exists so the helper-level before/after measures exactly
/// the shape change.
#[doc(hidden)]
pub fn auto_parse_legacy_shape(line: &str) -> Option<(&'static str, Vec<(String, String)>)> {
    for (name, parser) in [("json", &*JSON_PARSER), ("logfmt", &*LOGFMT_PARSER)] {
        let mut labels = Vec::new();
        let (kept, has_err) = parser
            .run_into_reporting_err(line, &[], 0, &mut labels)
            .unwrap_or((None, false));
        if kept.is_some() && !has_err {
            let pairs = labels
                .into_iter()
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
            return Some((name, pairs));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The pre-#244 convenience shape, over the production
    /// [`auto_parse_into`] — the tests below assert detection semantics,
    /// which the scratch-reuse refactor did not change.
    fn auto_parse(line: &str) -> Option<(&'static str, Vec<(String, String)>)> {
        let mut out = Vec::new();
        let parser = auto_parse_into(line, &mut out)?;
        let pairs = out
            .into_iter()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        Some((parser, pairs))
    }

    // -- determine_type: the pinned table --------------------------------

    #[test]
    fn determine_type_detects_ints_before_floats_and_floats_before_strings() {
        assert_eq!(determine_type("42"), "int");
        assert_eq!(determine_type("-7"), "int");
        assert_eq!(determine_type("1.5"), "float");
        assert_eq!(determine_type("-0.25"), "float");
        assert_eq!(determine_type("hello"), "string");
        assert_eq!(determine_type(""), "string");
    }

    #[test]
    fn determine_type_detects_the_parse_bool_token_set() {
        for v in [
            "t", "T", "TRUE", "true", "True", "f", "F", "FALSE", "false", "False",
        ] {
            assert_eq!(determine_type(v), "boolean", "{v}");
        }
        // `1`/`0` are ints (caught first in the pinned order) — exactly
        // like the reference, whose int check precedes ParseBool.
        assert_eq!(determine_type("1"), "int");
        assert_eq!(determine_type("0"), "int");
        // Other Go-ParseBool-rejects stay strings.
        assert_eq!(determine_type("tRuE"), "string");
    }

    #[test]
    fn determine_type_detects_durations_and_bytes_after_numbers() {
        assert_eq!(determine_type("1.5h"), "duration");
        assert_eq!(determine_type("250ms"), "duration");
        assert_eq!(determine_type("1h30m"), "duration");
        assert_eq!(determine_type("42MiB"), "bytes");
        assert_eq!(determine_type("512b"), "bytes");
        assert_eq!(determine_type("5KB"), "bytes");
    }

    // -- auto_parse: json-first, logfmt fallback --------------------------

    #[test]
    fn auto_parse_prefers_json_for_a_valid_json_object() {
        let (parser, pairs) = auto_parse(r#"{"level":"info","count":7}"#).expect("parsed");
        assert_eq!(parser, "json");
        assert!(pairs.contains(&("level".to_string(), "info".to_string())));
        assert!(pairs.contains(&("count".to_string(), "7".to_string())));
    }

    #[test]
    fn auto_parse_falls_back_to_logfmt_on_malformed_json() {
        let (parser, pairs) = auto_parse(r#"method=GET status=200"#).expect("parsed");
        assert_eq!(parser, "logfmt");
        assert!(pairs.contains(&("method".to_string(), "GET".to_string())));
        assert!(pairs.contains(&("status".to_string(), "200".to_string())));
    }

    #[test]
    fn auto_parse_treats_a_lenient_logfmt_line_as_logfmt_even_when_it_extracts_nothing() {
        // Issue #200: the default `| logfmt` is lenient — an unterminated
        // quote no longer sets `__error__`, so a non-JSON line best-effort
        // parses as logfmt (contributing no clean fields here: `plain` is a
        // dropped empty bare key, and the unterminated `x=` yields nothing).
        // This matches the reference's lenient default; the old "both error
        // => None" state is no longer reachable via a malformed logfmt line.
        let (parser, pairs) = auto_parse(r#"plain x="unterminated"#).expect("lenient logfmt");
        assert_eq!(parser, "logfmt");
        assert!(pairs.is_empty(), "no clean fields, got {pairs:?}");
    }

    // -- auto_parse keys on the error SLOT, not the label name (issue #238
    // review round 7, the ninth site). Expected values are literal captures
    // from the pinned reference container (grafana/loki:3.7.4,
    // `discover_log_levels: false`, no per-entry SM): /detected_fields over
    // one-line streams. Wrong rules named per row:
    //   NAME  = the pre-fix emitted-label NAME scan (`any(k == "__error__")`)
    //   VALUE = a non-emptiness scan (`any(k == "__error__" && !v.is_empty())`)
    // ---------------------------------------------------------------------

    /// Capture: `{"__error__":"","foo":"x"}` -> field `foo`, parsers
    /// ["json"]. Correct: `("json", pairs)` with `foo=x` retained (the
    /// parsed ordinary `__error__` label rides along in the pairs and is
    /// excluded from FIELDS by name downstream — see
    /// `error_labels_are_excluded_from_fields`, matching the capture, which
    /// lists no `__error__` field). Under NAME: `("logfmt", [])` — `foo`
    /// and the json attribution are lost (this row's killer). Under VALUE:
    /// same as correct — this row is a DECLARED PIN for VALUE; its
    /// discriminator is the sibling below.
    #[test]
    fn auto_parse_accepts_json_with_an_empty_parsed_error_named_label() {
        let (parser, pairs) = auto_parse(r#"{"__error__":"","foo":"x"}"#).expect("parsed");
        assert_eq!(parser, "json");
        assert!(
            pairs.contains(&("foo".to_string(), "x".to_string())),
            "foo must survive: {pairs:?}"
        );
        assert!(
            pairs.contains(&("__error__".to_string(), String::new())),
            "the parsed ORDINARY __error__ label is an ordinary pair: {pairs:?}"
        );
    }

    /// Capture: `{"__error__":"mine","foo":"x"}` -> field `foo`, parsers
    /// ["json"]. Correct: `("json", pairs)` with `foo=x` retained. Under
    /// NAME: `("logfmt", [])`. Under VALUE: `("logfmt", [])` too ("mine" is
    /// non-empty) — this row kills BOTH wrong rules, which is why the
    /// review rejected the "obvious" non-emptiness fix.
    #[test]
    fn auto_parse_accepts_json_with_a_nonempty_parsed_error_named_label() {
        let (parser, pairs) = auto_parse(r#"{"__error__":"mine","foo":"x"}"#).expect("parsed");
        assert_eq!(parser, "json");
        assert!(
            pairs.contains(&("foo".to_string(), "x".to_string())),
            "foo must survive: {pairs:?}"
        );
        assert!(pairs.contains(&("__error__".to_string(), "mine".to_string())));
    }

    /// A GENUINE json parse failure still falls back: the json parser sets
    /// the err SLOT (JSONParserErr), so detection moves on to logfmt, which
    /// extracts `foo`/`bar` (capture: fields foo+bar, parsers ["logfmt"]).
    /// If the slot state stopped being threaded out (has_err always false),
    /// json would win with ZERO pairs and the logfmt attribution would be
    /// lost — this row is the thread-through guard. (`not json at all`
    /// yields no fields on both stores: lenient logfmt drops the empty
    /// bare keys — pinned by
    /// `auto_parse_treats_a_lenient_logfmt_line_as_logfmt_even_when_it_extracts_nothing`.)
    #[test]
    fn auto_parse_still_falls_back_on_a_genuine_json_slot_error() {
        let (parser, pairs) = auto_parse("foo=x bar=y").expect("parsed");
        assert_eq!(parser, "logfmt");
        assert!(pairs.contains(&("foo".to_string(), "x".to_string())));
        assert!(pairs.contains(&("bar".to_string(), "y".to_string())));
        // And the slot error never leaks into the winner's pairs.
        assert!(!pairs.iter().any(|(k, _)| k == ERROR_LABEL), "{pairs:?}");
    }

    // -- FieldAccumulator --------------------------------------------------

    #[test]
    fn error_labels_are_excluded_from_fields() {
        let mut acc = FieldAccumulator::new(100);
        acc.observe_parsed(
            &owned(&[
                ("__error__", "JSONParserErr"),
                ("__error_details__", "x"),
                ("ok", "1"),
            ]),
            None,
        );
        let (fields, _) = acc.finish();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].label, "ok");
    }

    #[test]
    fn field_limit_caps_on_first_seen_names_and_skips_later_names_entirely() {
        let mut acc = FieldAccumulator::new(2);
        acc.observe_parsed(&owned(&[("a", "1"), ("b", "2"), ("c", "3")]), None);
        // `a` is already admitted — later observations still count.
        acc.observe_parsed(&owned(&[("a", "4"), ("c", "5")]), None);
        let (fields, _) = acc.finish();
        assert_eq!(
            fields.iter().map(|f| f.label.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"],
            "the first 2 distinct names win; `c` is skipped entirely"
        );
        assert_eq!(fields[0].cardinality, 2, "a saw values 1 and 4");
    }

    #[test]
    fn cardinality_is_exact_over_distinct_values() {
        let mut acc = FieldAccumulator::new(100);
        acc.observe_parsed(&owned(&[("k", "x")]), None);
        acc.observe_parsed(&owned(&[("k", "y")]), None);
        acc.observe_parsed(&owned(&[("k", "x")]), None);
        let (fields, _) = acc.finish();
        assert_eq!(fields[0].cardinality, 2);
    }

    #[test]
    fn type_is_re_detected_per_observation_and_the_last_wins() {
        let mut acc = FieldAccumulator::new(100);
        acc.observe_parsed(&owned(&[("k", "42")]), None);
        assert_eq!(acc.fields["k"].field_type, "int");
        acc.observe_parsed(&owned(&[("k", "hello")]), None);
        let (fields, _) = acc.finish();
        assert_eq!(fields[0].field_type, "string", "last observation wins");
    }

    #[test]
    fn structured_metadata_fields_carry_no_parser_and_parsed_fields_dedupe_parsers() {
        let mut acc = FieldAccumulator::new(100);
        acc.observe_structured_metadata(&owned(&[("trace_id", "abc")]));
        acc.observe_parsed(&owned(&[("level", "info")]), Some("json"));
        acc.observe_parsed(&owned(&[("level", "warn")]), Some("json"));
        acc.observe_parsed(&owned(&[("level", "err")]), Some("logfmt"));
        let (fields, _) = acc.finish();
        let trace = fields
            .iter()
            .find(|f| f.label == "trace_id")
            .expect("sm field");
        assert!(
            trace.parsers.is_empty(),
            "SM fields have no parser attribution"
        );
        let level = fields.iter().find(|f| f.label == "level").expect("level");
        assert_eq!(
            level.parsers,
            vec!["json", "logfmt"],
            "encounter order, deduped"
        );
    }

    #[test]
    fn finish_sorts_fields_by_label() {
        let mut acc = FieldAccumulator::new(100);
        acc.observe_parsed(&owned(&[("zeta", "1"), ("alpha", "2")]), None);
        let (fields, _) = acc.finish();
        assert_eq!(
            fields.iter().map(|f| f.label.as_str()).collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );
    }

    #[test]
    fn static_detected_labels_match_the_reference_set() {
        assert!(is_static_detected_label("cluster"));
        assert!(is_static_detected_label("namespace"));
        assert!(is_static_detected_label("instance"));
        assert!(is_static_detected_label("pod"));
        assert!(!is_static_detected_label("env"));
    }

    // -- Issue #244: the charged retention budget --------------------------

    /// AC 1(a): after ANY observation sequence, `charged() <= budget`.
    #[test]
    fn charged_bytes_never_exceed_the_budget_over_any_sequence() {
        let budget = 4 * 1024;
        let mut acc = FieldAccumulator::with_byte_budget(100, budget);
        for i in 0..500u32 {
            acc.observe_pair(&format!("k{}", i % 7), &format!("value-{i}"), None);
            assert!(
                acc.charged() <= budget,
                "charged {} exceeded budget {budget} at observation {i}",
                acc.charged()
            );
        }
        let (_, capped) = acc.finish();
        assert!(capped, "the tiny budget must have refused something");
    }

    /// AC 1(b): a refused FIELD admission leaves the name absent and
    /// `charged()` unchanged (the check-then-add contract: a failed charge
    /// never mutates the counter). The budget is sized so the second
    /// name's admission is provably the refused charge.
    #[test]
    fn a_refused_field_admission_is_absent_and_charges_nothing() {
        let first = "alpha";
        let second = "omega";
        // Enough for the first name + its value, NOT for a second name.
        let budget = field_entry_bytes(first) + value_entry_bytes("1") + 1;
        assert!(
            budget < field_entry_bytes(first) + value_entry_bytes("1") + field_entry_bytes(second),
            "budget must refuse the second admission"
        );
        let mut acc = FieldAccumulator::with_byte_budget(100, budget);
        acc.observe_pair(first, "1", None);
        let charged_before = acc.charged();
        acc.observe_pair(second, "2", None);
        assert_eq!(
            acc.charged(),
            charged_before,
            "a refused admission must not mutate the charge"
        );
        let (fields, capped) = acc.finish();
        assert!(capped);
        assert_eq!(fields.len(), 1, "the refused name is absent: {fields:?}");
        assert_eq!(fields[0].label, first);
    }

    /// AC 1(c): a refused VALUE leaves the value absent, `charged()`
    /// unchanged, and the field frozen.
    #[test]
    fn a_refused_value_is_absent_charges_nothing_and_freezes_the_field() {
        let budget = field_entry_bytes("k") + value_entry_bytes("small") + 1;
        let wide = "w".repeat(4096);
        assert!(
            budget < field_entry_bytes("k") + value_entry_bytes("small") + value_entry_bytes(&wide),
            "budget must refuse the wide value"
        );
        let mut acc = FieldAccumulator::with_byte_budget(100, budget);
        acc.observe_pair("k", "small", None);
        let charged_before = acc.charged();
        acc.observe_pair("k", &wide, None);
        assert_eq!(acc.charged(), charged_before);
        assert!(acc.fields["k"].frozen, "the refusal freezes the field");
        let (fields, capped) = acc.finish();
        assert!(capped);
        assert_eq!(
            fields[0].cardinality, 1,
            "the refused value was never inserted"
        );
    }

    /// AC 2: clamp, never reject — 500 distinct values across 50 fields
    /// under a tiny budget still serve (under-reported cardinalities,
    /// `retention_capped == true`, no error anywhere on the path).
    #[test]
    fn a_tiny_budget_clamps_and_serves_never_errors() {
        let mut acc = FieldAccumulator::with_byte_budget(100, 4 * 1024);
        for i in 0..500u32 {
            acc.observe_pair(&format!("field{}", i % 50), &format!("v{i}"), None);
        }
        let (fields, capped) = acc.finish();
        assert!(capped, "retention_capped must be set");
        assert!(!fields.is_empty(), "the response still serves");
        let total: u64 = fields.iter().map(|f| f.cardinality).sum();
        assert!(
            total < 500,
            "cardinalities must under-report under the clamp, got {total}"
        );
    }

    /// AC 3: a frozen field still re-detects its type (a DIFFERENT
    /// `determine_type` outcome) and appends the OTHER parser name, while
    /// `cardinality` stays unchanged.
    #[test]
    fn a_frozen_field_still_re_detects_type_and_appends_parsers() {
        let budget = field_entry_bytes("k") + value_entry_bytes("42") + 1;
        let mut acc = FieldAccumulator::with_byte_budget(100, budget);
        acc.observe_pair("k", "42", Some("json"));
        assert_eq!(acc.fields["k"].field_type, "int");
        // Refused (freezes): a value with a different detected type and
        // the other parser name.
        acc.observe_pair("k", "hello-world-wide-value", Some("logfmt"));
        assert!(acc.fields["k"].frozen);
        let (fields, capped) = acc.finish();
        assert!(capped);
        assert_eq!(
            fields[0].field_type, "string",
            "type re-detected (last wins)"
        );
        assert_eq!(fields[0].parsers, vec!["json", "logfmt"], "parser appended");
        assert_eq!(fields[0].cardinality, 1, "value set frozen");
    }

    /// AC 4: the field-NAME axis is charged before the clone —
    /// `field_entry_bytes` strictly increases across >= 3 distinct name
    /// lengths, and at 64 KiB names `admitted × field_entry_bytes(name)`
    /// stays within the budget.
    #[test]
    fn field_name_bytes_are_charged_by_length() {
        let short = "a";
        let mid = "a".repeat(64);
        let long = "a".repeat(65_536);
        assert!(field_entry_bytes(short) < field_entry_bytes(&mid));
        assert!(field_entry_bytes(&mid) < field_entry_bytes(&long));

        let budget = 1024 * 1024;
        let mut acc = FieldAccumulator::with_byte_budget(5000, budget);
        for i in 0..512u32 {
            let name = format!("{i:05}-{}", "n".repeat(65_536));
            acc.observe_pair(&name, "v", None);
        }
        let (fields, capped) = acc.finish();
        assert!(capped, "512 x 64 KiB names must breach 1 MiB");
        let per_name = field_entry_bytes(&format!("{:05}-{}", 0, "n".repeat(65_536)));
        assert!(
            (fields.len() as u64) * per_name <= budget,
            "admitted count {} x per-name charge {per_name} must fit the budget {budget}",
            fields.len()
        );
    }
}
