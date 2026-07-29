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
//! - [`auto_parse`]'s json-first / logfmt-fallback per-line detection
//!   (success = the parser set no `__error__` label — the reference's
//!   `HasErr()` analog), evaluated via the SAME [`CompiledPipeline`]
//!   parser stages the query path runs;
//! - [`FieldAccumulator`]'s first-seen field cap, exact cardinality
//!   (documented improvement over the reference's hyperloglog sketch),
//!   per-observation type re-detection (last observation wins, matching
//!   the reference's per-entry re-detect), and encounter-order deduped
//!   parser attribution.

use std::collections::{BTreeSet, HashMap};
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
/// byte scan budget was spent — surfaced as the additive `pulsus_partial`
/// response key (omitted when false, the #90 wire convention).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedFields {
    pub fields: Vec<DetectedFieldOut>,
    pub truncated: bool,
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

/// One detected field's accumulating state.
#[derive(Debug)]
struct FieldState {
    field_type: &'static str,
    values: BTreeSet<String>,
    parsers: Vec<&'static str>,
}

/// Accumulates detected fields across sampled entries: the first
/// `field_limit` distinct field names win (later names are skipped
/// entirely, values uncounted — the reference's `fieldCount < limit`
/// gate), each observation re-detects the type (last wins) and inserts
/// the exact value, and parser attribution appends deduped in encounter
/// order.
#[derive(Debug)]
pub(super) struct FieldAccumulator {
    field_limit: u32,
    fields: HashMap<String, FieldState>,
}

impl FieldAccumulator {
    pub(super) fn new(field_limit: u32) -> Self {
        Self {
            field_limit,
            fields: HashMap::new(),
        }
    }

    /// Structured-metadata pairs: fields with no parser attribution.
    pub(super) fn observe_structured_metadata(&mut self, pairs: &[(String, String)]) {
        self.observe_parsed(pairs, None);
    }

    /// Parsed pairs — from the query pipeline's own extractions
    /// (`parser = None`) or from [`auto_parse`]'s json/logfmt detection
    /// (`parser = Some(...)`). `__error__`/`__error_details__` never
    /// become fields.
    pub(super) fn observe_parsed(
        &mut self,
        pairs: &[(String, String)],
        parser: Option<&'static str>,
    ) {
        for (key, value) in pairs {
            if key == ERROR_LABEL || key == ERROR_DETAILS_LABEL {
                continue;
            }
            if !self.fields.contains_key(key.as_str()) {
                if self.fields.len() >= self.field_limit as usize {
                    continue;
                }
                self.fields.insert(
                    key.clone(),
                    FieldState {
                        field_type: "string",
                        values: BTreeSet::new(),
                        parsers: Vec::new(),
                    },
                );
            }
            // Present by construction: either it already existed or the
            // insert above just admitted it.
            let Some(state) = self.fields.get_mut(key.as_str()) else {
                continue;
            };
            state.field_type = determine_type(value);
            if !state.values.contains(value.as_str()) {
                state.values.insert(value.clone());
            }
            if let Some(p) = parser
                && !state.parsers.contains(&p)
            {
                state.parsers.push(p);
            }
        }
    }

    /// Final response entries, sorted by label (deterministic wire order —
    /// a documented divergence from the reference's Go map order).
    pub(super) fn finish(self) -> Vec<DetectedFieldOut> {
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
        out
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
/// [`FieldAccumulator::observe_parsed`]'s name exclusion matches that);
/// `not json at all` yields no fields. Returns the winning parser name and
/// its extracted pairs (a slot `__error_details__` never leaks — an
/// erroring parser is a failure wholesale).
pub(super) fn auto_parse(line: &str) -> Option<(&'static str, Vec<(String, String)>)> {
    for (name, parser) in [("json", &*JSON_PARSER), ("logfmt", &*LOGFMT_PARSER)] {
        let mut labels = Vec::new();
        // Auto-detection probes carry no row timestamp; the probe
        // pipelines are bare parsers (no templates), so 0 is inert and a
        // template render-budget breach is unreachable — a defensive Err
        // maps to "not this format" (plan v1 §4: detected.rs passes 0).
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
        let fields = acc.finish();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].label, "ok");
    }

    #[test]
    fn field_limit_caps_on_first_seen_names_and_skips_later_names_entirely() {
        let mut acc = FieldAccumulator::new(2);
        acc.observe_parsed(&owned(&[("a", "1"), ("b", "2"), ("c", "3")]), None);
        // `a` is already admitted — later observations still count.
        acc.observe_parsed(&owned(&[("a", "4"), ("c", "5")]), None);
        let fields = acc.finish();
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
        let fields = acc.finish();
        assert_eq!(fields[0].cardinality, 2);
    }

    #[test]
    fn type_is_re_detected_per_observation_and_the_last_wins() {
        let mut acc = FieldAccumulator::new(100);
        acc.observe_parsed(&owned(&[("k", "42")]), None);
        assert_eq!(acc.fields["k"].field_type, "int");
        acc.observe_parsed(&owned(&[("k", "hello")]), None);
        let fields = acc.finish();
        assert_eq!(fields[0].field_type, "string", "last observation wins");
    }

    #[test]
    fn structured_metadata_fields_carry_no_parser_and_parsed_fields_dedupe_parsers() {
        let mut acc = FieldAccumulator::new(100);
        acc.observe_structured_metadata(&owned(&[("trace_id", "abc")]));
        acc.observe_parsed(&owned(&[("level", "info")]), Some("json"));
        acc.observe_parsed(&owned(&[("level", "warn")]), Some("json"));
        acc.observe_parsed(&owned(&[("level", "err")]), Some("logfmt"));
        let fields = acc.finish();
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
        let fields = acc.finish();
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
}
