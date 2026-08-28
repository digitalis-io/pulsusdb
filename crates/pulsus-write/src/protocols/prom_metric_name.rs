//! Prometheus-compatible OTLP metric/label naming (issue #461).
//!
//! A port of `github.com/prometheus/otlptranslator v1.0.0` — the module
//! Prometheus v3.13.0 pins in its `go.mod` — plus the caller-side
//! `job`/`instance` derivation from
//! `storage/remote/otlptranslator/prometheusremotewrite/metrics_to_prw.go`
//! @ `v3.13.0` (`40af9c2cdc0eda00f3622e867a27f6359f7295f3`). Every rule
//! below cites the reference line it reproduces so the next reader can
//! check it.
//!
//! **This module is deliberately independent of
//! [`crate::protocols::label_name`].** That module ports the older
//! `otlptranslator` Loki v3.7.4 vendors: it *collapses* runs of invalid
//! characters and has no underscore-prefix rule. Prometheus v3.13.0
//! configures `LabelNamePreserveMultipleUnderscores: true` and
//! `LabelNameUnderscoreSanitization: true` (`config.DefaultOTLPConfig`),
//! so `日本` normalizes to `__` here and to `_` there, and `_priv`
//! becomes `key_priv` here and stays `_priv` there. Reusing it would
//! produce rejection messages that differ from the reference on exactly
//! the cases they are meant to reproduce.
//!
//! Both `LabelNameUnderscoreSanitization` and
//! `LabelNamePreserveMultipleUnderscores` are fixed at the reference's own
//! defaults here rather than exposed as knobs — upstream keeps them only
//! "for backwards compatibility" and marks the first `Deprecated`
//! (`label_namer.go:167-177 @ otlptranslator@v1.0.0`).

use opentelemetry_proto::tonic::common::v1::KeyValue;
use pulsus_config::OtlpTranslationStrategy;
use pulsus_model::canonicalize_label_key;

use crate::protocols::otlp_metrics::any_value_to_string;

/// OTLP metric kinds as the namer sees them
/// (`metric_type.go:336-351 @ otlptranslator@v1.0.0`). Only
/// `MonotonicCounter` and `Gauge` change the built name; the rest are
/// carried so the mapping from an OTLP `Metric` stays total and explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Unknown,
    NonMonotonicCounter,
    MonotonicCounter,
    Gauge,
    Histogram,
    ExponentialHistogram,
    Summary,
}

/// OTLP unit -> Prometheus base unit (`metric_namer.go:34-68 @
/// otlptranslator@v1.0.0`). Twenty-six entries; asserted as a full table by
/// this module's tests, because inventing or dropping one silently changes
/// a stored metric name.
const UNIT_MAP: &[(&str, &str)] = &[
    // Time
    ("d", "days"),
    ("h", "hours"),
    ("min", "minutes"),
    ("s", "seconds"),
    ("ms", "milliseconds"),
    ("us", "microseconds"),
    ("ns", "nanoseconds"),
    // Bytes
    ("By", "bytes"),
    ("KiBy", "kibibytes"),
    ("MiBy", "mebibytes"),
    ("GiBy", "gibibytes"),
    ("TiBy", "tibibytes"),
    ("KBy", "kilobytes"),
    ("MBy", "megabytes"),
    ("GBy", "gigabytes"),
    ("TBy", "terabytes"),
    // SI
    ("m", "meters"),
    ("V", "volts"),
    ("A", "amperes"),
    ("J", "joules"),
    ("W", "watts"),
    ("g", "grams"),
    // Misc
    ("Cel", "celsius"),
    ("Hz", "hertz"),
    ("1", ""),
    ("%", "percent"),
];

/// OTLP "per" unit -> Prometheus per-unit (`metric_namer.go:72-80 @
/// otlptranslator@v1.0.0`). Seven entries.
const PER_UNIT_MAP: &[(&str, &str)] = &[
    ("s", "second"),
    ("m", "minute"),
    ("h", "hour"),
    ("d", "day"),
    ("w", "week"),
    ("mo", "month"),
    ("y", "year"),
];

/// The OTLP resource attribute whose value becomes the second half of
/// `job` (`semconv v1.26.0`, read at `metrics_to_prw.go:420`).
pub const SERVICE_NAME_KEY: &str = "service.name";
/// The OTLP resource attribute prefixed onto `job` when present
/// (`metrics_to_prw.go:422-424`).
pub const SERVICE_NAMESPACE_KEY: &str = "service.namespace";
/// The OTLP resource attribute that becomes `instance`
/// (`metrics_to_prw.go:428-430`).
pub const SERVICE_INSTANCE_ID_KEY: &str = "service.instance.id";

/// The three attributes `target_info` drops from its own label set
/// (`helper.go:504-508 @ v3.13.0`), and whose presence alone makes a
/// resource ineligible for a `target_info` series.
pub const IDENTIFYING_RESOURCE_ATTRS: &[&str] = &[
    SERVICE_NAMESPACE_KEY,
    SERVICE_NAME_KEY,
    SERVICE_INSTANCE_ID_KEY,
];

/// `isValidCompliantMetricChar` (`metric_namer.go:209-214`): a-z, A-Z,
/// 0-9, `:`. Note `_` is **not** in this set — which is why
/// `normalizeName`'s `FieldsFunc` treats an underscore as a token
/// separator.
fn is_valid_compliant_metric_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == ':'
}

/// `replaceInvalidMetricChar` (`metric_namer.go:217-222`).
fn replace_invalid_metric_char(c: char) -> char {
    if is_valid_compliant_metric_char(c) {
        c
    } else {
        '_'
    }
}

/// Go's `strings.FieldsFunc`: split at every rune for which `is_sep` is
/// true, dropping empty fields. A *run* of separators therefore yields no
/// token at all — the rule that turns `http.服务.duration` into
/// `http_duration_seconds` rather than `http___duration_seconds`.
fn fields_func(s: &str, is_sep: impl Fn(char) -> bool) -> Vec<&str> {
    s.split(|c: char| is_sep(c))
        .filter(|token| !token.is_empty())
        .collect()
}

/// `collapseMultipleUnderscores` (`strconv.go:482-505`).
fn collapse_multiple_underscores(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_was_underscore = false;
    for c in s.chars() {
        if c == '_' {
            if !prev_was_underscore {
                out.push('_');
                prev_was_underscore = true;
            }
        } else {
            out.push(c);
            prev_was_underscore = false;
        }
    }
    out
}

/// `cleanUpUnit` (`unit_namer.go:123-129`): map invalid metric characters
/// to `_`, collapse underscore runs, then strip **one** leading `_`
/// (Go's `strings.TrimPrefix`, not `TrimLeft`).
fn clean_up_unit(unit: &str) -> String {
    let mapped: String = unit.chars().map(replace_invalid_metric_char).collect();
    let collapsed = collapse_multiple_underscores(&mapped);
    collapsed
        .strip_prefix('_')
        .map(str::to_string)
        .unwrap_or(collapsed)
}

/// `unitMapGetOrDefault` (`unit_namer.go:75-80`).
fn unit_map_get_or_default(unit: &str) -> String {
    UNIT_MAP
        .iter()
        .find(|(k, _)| *k == unit)
        .map(|(_, v)| (*v).to_string())
        .unwrap_or_else(|| unit.to_string())
}

/// `perUnitMapGetOrDefault` (`unit_namer.go:84-89`).
fn per_unit_map_get_or_default(per_unit: &str) -> String {
    PER_UNIT_MAP
        .iter()
        .find(|(k, _)| *k == per_unit)
        .map(|(_, v)| (*v).to_string())
        .unwrap_or_else(|| per_unit.to_string())
}

/// `buildUnitSuffixes` (`unit_namer.go:94-120`): split the unit at the
/// first `/`; a component that is blank or contains `{`/`}` contributes
/// nothing. The per-unit gains its `per_` prefix here.
fn build_unit_suffixes(unit: &str) -> (String, String) {
    let mut main_unit_suffix = String::new();
    let mut per_unit_suffix = String::new();

    let (head, tail) = match unit.split_once('/') {
        Some((head, tail)) => (head, Some(tail)),
        None => (unit, None),
    };

    let main_unit_otel = head.trim();
    if !main_unit_otel.is_empty() && !main_unit_otel.contains(['{', '}']) {
        main_unit_suffix = unit_map_get_or_default(main_unit_otel);
    }

    if let Some(tail) = tail.filter(|t| !t.is_empty()) {
        let per_unit_otel = tail.trim();
        if !per_unit_otel.is_empty() && !per_unit_otel.contains(['{', '}']) {
            per_unit_suffix = per_unit_map_get_or_default(per_unit_otel);
        }
        if !per_unit_suffix.is_empty() {
            per_unit_suffix = format!("per_{per_unit_suffix}");
        }
    }

    (main_unit_suffix, per_unit_suffix)
}

/// `otlptranslator.UnitNamer` (`unit_namer.go:26-71`). The reference's
/// OTLP receiver builds metadata units with `UnitNamer{}`, i.e.
/// `UTF8Allowed: false`, under **every** strategy
/// (`metrics_to_prw.go:181`).
#[derive(Debug, Clone, Copy)]
pub struct UnitNamer {
    pub utf8_allowed: bool,
}

impl UnitNamer {
    /// `UnitNamer.Build` (`unit_namer.go:46-71`).
    pub fn build(&self, unit: &str) -> String {
        let (mut main_unit, mut per_unit) = build_unit_suffixes(unit);
        if !self.utf8_allowed {
            main_unit = clean_up_unit(&main_unit);
            per_unit = clean_up_unit(&per_unit);
        }

        let mut u = if !main_unit.is_empty() && !per_unit.is_empty() {
            format!("{main_unit}_{per_unit}")
        } else if !main_unit.is_empty() {
            main_unit
        } else {
            per_unit
        };

        // Strip one leading and one trailing underscore, as the reference
        // does with byte slicing.
        if u.starts_with('_') {
            u.remove(0);
        }
        if u.ends_with('_') {
            u.pop();
        }
        u
    }
}

/// `addUnitTokens` (`metric_namer.go:273-298`). Note the membership test
/// is `slices.Contains(nameTokens, suffix)` — a *token* test, not the
/// `strings.HasSuffix` the UTF-8 branch uses. That difference is why
/// `queue.bytes.sent` with unit `By` becomes `queue_bytes_sent_total`
/// under the escaping strategies and `queue.bytes.sent_bytes_total` under
/// `NoUTF8EscapingWithSuffixes`.
fn add_unit_tokens(
    mut name_tokens: Vec<String>,
    mut main_unit_suffix: String,
    mut per_unit_suffix: String,
) -> Vec<String> {
    if name_tokens.contains(&main_unit_suffix) {
        main_unit_suffix = String::new();
    }

    if per_unit_suffix == "per_" {
        per_unit_suffix = String::new();
    } else {
        if let Some(stripped) = per_unit_suffix.strip_suffix('_') {
            per_unit_suffix = stripped.to_string();
        }
        if name_tokens.contains(&per_unit_suffix) {
            per_unit_suffix = String::new();
        }
    }

    if !per_unit_suffix.is_empty()
        && let Some(stripped) = main_unit_suffix.strip_suffix('_')
    {
        main_unit_suffix = stripped.to_string();
    }

    if !main_unit_suffix.is_empty() {
        name_tokens.push(main_unit_suffix);
    }
    if !per_unit_suffix.is_empty() {
        name_tokens.push(per_unit_suffix);
    }
    name_tokens
}

/// `removeItem` (`metric_namer.go:301-309`): removes **every** occurrence,
/// which is why a monotonic `bar.total.total` becomes `bar_total` and not
/// `bar_total_total`.
fn remove_item(slice: Vec<String>, value: &str) -> Vec<String> {
    slice.into_iter().filter(|entry| entry != value).collect()
}

/// `normalizeName` (`metric_namer.go:225-265`) with `namespace == ""` —
/// the reference's OTLP receiver never sets `Settings.Namespace`.
fn normalize_name(name: &str, unit: &str, kind: MetricKind) -> String {
    let mut name_tokens: Vec<String> = fields_func(name, |c| !is_valid_compliant_metric_char(c))
        .into_iter()
        .map(str::to_string)
        .collect();

    let (main_unit_suffix, per_unit_suffix) = build_unit_suffixes(unit);
    name_tokens = add_unit_tokens(
        name_tokens,
        clean_up_unit(&main_unit_suffix),
        clean_up_unit(&per_unit_suffix),
    );

    if kind == MetricKind::MonotonicCounter {
        name_tokens = remove_item(name_tokens, "total");
        name_tokens.push("total".to_string());
    }

    if unit == "1" && kind == MetricKind::Gauge {
        name_tokens = remove_item(name_tokens, "ratio");
        name_tokens.push("ratio".to_string());
    }

    let mut normalized = name_tokens.join("_");
    if normalized.starts_with(|c: char| c.is_ascii_digit()) {
        normalized.insert(0, '_');
    }
    normalized
}

/// `trimSuffixAndDelimiter` (`metric_namer.go:356-361`): trims **one**
/// trailing occurrence plus the delimiter before it.
fn trim_suffix_and_delimiter(name: &str, suffix: &str) -> String {
    if name.ends_with(suffix) && name.len() > suffix.len() + 1 {
        name[..name.len() - (suffix.len() + 1)].to_string()
    } else {
        name.to_string()
    }
}

/// `otlptranslator.MetricNamer` (`metric_namer.go:99-206, 311-352`), with
/// `Namespace` always `""`.
#[derive(Debug, Clone, Copy)]
pub struct MetricNamer {
    pub with_suffixes: bool,
    pub utf8_allowed: bool,
}

impl MetricNamer {
    /// `NewMetricNamer` (`metric_namer.go:107-113`).
    pub fn from_strategy(strategy: OtlpTranslationStrategy) -> Self {
        MetricNamer {
            with_suffixes: strategy.should_add_suffixes(),
            utf8_allowed: !strategy.should_escape(),
        }
    }

    /// `MetricNamer.Build` (`metric_namer.go:151-156`). `Err` carries the
    /// reference's message with the offending name quoted — see
    /// `docs/benchmarks/metrics-differential-ledger.md`
    /// (`otlp-reject-message-escape-syntax`) for the bounded difference
    /// between Rust's `{:?}` and Go's `%q`.
    pub fn build(&self, name: &str, unit: &str, kind: MetricKind) -> Result<String, String> {
        if self.utf8_allowed {
            // `buildMetricName` has an `error` return that it never sets
            // (`metric_namer.go:311-352`).
            return Ok(self.build_metric_name(name, unit, kind));
        }
        self.build_compliant_metric_name(name, unit, kind)
    }

    /// `buildCompliantMetricName` (`metric_namer.go:158-206`), including
    /// its deferred post-conditions: an empty result, and a non-empty
    /// result that differs from the input and consists only of
    /// underscores, are both rejections.
    fn build_compliant_metric_name(
        &self,
        name: &str,
        unit: &str,
        kind: MetricKind,
    ) -> Result<String, String> {
        let normalized = if self.with_suffixes {
            normalize_name(name, unit, kind)
        } else {
            // The simple case: `_` is NOT a separator here
            // (`metric_namer.go:186-188`), so `a__b` survives intact.
            let mut metric_name =
                fields_func(name, |c| !is_valid_compliant_metric_char(c) && c != '_').join("_");
            if metric_name.starts_with(|c: char| c.is_ascii_digit()) {
                metric_name.insert(0, '_');
            }
            metric_name
        };

        if normalized.is_empty() {
            return Err(format!(
                "normalization for metric {name:?} resulted in empty name"
            ));
        }
        if normalized != name && normalized.chars().all(|c| c == '_') {
            return Err(format!(
                "normalization for metric {name:?} resulted in invalid name {normalized:?}"
            ));
        }
        Ok(normalized)
    }

    /// `buildMetricName` (`metric_namer.go:311-352`). The reference
    /// registers the `_ratio`, `_total` and per-unit appends as `defer`s,
    /// so they run in LIFO order *after* the main unit suffix is decided —
    /// reproduced literally below.
    fn build_metric_name(&self, input_name: &str, unit: &str, kind: MetricKind) -> String {
        let mut name = input_name.to_string();
        if !self.with_suffixes {
            return name;
        }

        let mut append_ratio = false;
        if unit == "1" && kind == MetricKind::Gauge {
            name = trim_suffix_and_delimiter(&name, "ratio");
            append_ratio = true;
        }

        let mut append_total = false;
        if kind == MetricKind::MonotonicCounter {
            name = trim_suffix_and_delimiter(&name, "total");
            append_total = true;
        }

        let (main_unit_suffix, per_unit_suffix) = build_unit_suffixes(unit);
        let mut append_per: Option<String> = None;
        if !per_unit_suffix.is_empty() {
            name = trim_suffix_and_delimiter(&name, &per_unit_suffix);
            append_per = Some(per_unit_suffix);
        }
        // The inner-most suffix: tested with `HasSuffix`, not token
        // membership (`metric_namer.go:347`).
        if !main_unit_suffix.is_empty() && !name.ends_with(&main_unit_suffix) {
            name = format!("{name}_{main_unit_suffix}");
        }

        // The deferred appends, LIFO.
        if let Some(per) = append_per {
            name = format!("{name}_{per}");
        }
        if append_total {
            name.push_str("_total");
        }
        if append_ratio {
            name.push_str("_ratio");
        }
        name
    }
}

/// `hasUnderscoresOnly` (`label_namer.go:222-229`). An empty string
/// satisfies it, exactly as Go's empty range does.
fn has_underscores_only(label: &str) -> bool {
    label.chars().all(|c| c == '_')
}

/// `otlptranslator.LabelNamer` (`label_namer.go:165-220`) at Prometheus
/// v3.13.0's defaults: `UnderscoreLabelSanitization: true` and
/// `PreserveMultipleUnderscores: true`.
#[derive(Debug, Clone, Copy)]
pub struct LabelNamer {
    pub utf8_allowed: bool,
}

impl LabelNamer {
    /// From a translation strategy: `AllowUTF8 = !ShouldEscape`
    /// (`metric_namer.go:111`, `metrics_to_prw.go:414-418`).
    pub fn from_strategy(strategy: OtlpTranslationStrategy) -> Self {
        LabelNamer {
            utf8_allowed: !strategy.should_escape(),
        }
    }

    /// `LabelNamer.Build` (`label_namer.go:194-220`).
    ///
    /// The character mapping is `sanitizeLabelName(name, true)`
    /// (`strconv.go:419-434`): every rune outside `[a-zA-Z0-9]` becomes
    /// exactly one `_`, underscore runs preserved. That is
    /// [`canonicalize_label_key`] rune-for-rune — its allow-list is
    /// `[a-zA-Z0-9_]` and it maps everything else to one `_`, and `_`
    /// itself maps to `_` on both sides — so this reuses it rather than
    /// restating the table.
    pub fn build(&self, label: &str) -> Result<String, String> {
        if label.is_empty() {
            return Err("label name is empty".to_string());
        }

        if self.utf8_allowed {
            if has_underscores_only(label) {
                return Err(format!("label name {label:?} contains only underscores"));
            }
            return Ok(label.to_string());
        }

        let mut normalized = canonicalize_label_key(label);

        if normalized.starts_with(|c: char| c.is_ascii_digit()) {
            normalized.insert_str(0, "key_");
        } else if normalized.starts_with('_') && !normalized.starts_with("__") {
            normalized.insert_str(0, "key");
        }

        if has_underscores_only(&normalized) {
            return Err(format!(
                "normalization for label name {label:?} resulted in invalid name {normalized:?}"
            ));
        }

        Ok(normalized)
    }
}

/// The first resource attribute whose key is `key`, rendered
/// (`pcommon.Map.Get` returns the first match).
fn first_attr(attrs: &[KeyValue], key: &str) -> Option<String> {
    attrs
        .iter()
        .find(|kv| kv.key == key)
        .map(|kv| any_value_to_string(kv.value.as_ref()))
}

/// `setResourceContext`'s `job`/`instance` derivation
/// (`metrics_to_prw.go:420-430 @ v3.13.0`). `None` when the derived value
/// is the empty string, because `createAttributes` only sets each label
/// when it is non-empty (`helper.go:141-146`).
///
/// Note the asymmetry the reference has and we reproduce: `service.name`
/// absent means **no** `job` even when `service.namespace` is present,
/// while `service.name` present-but-empty with a namespace yields
/// `"<ns>/"`.
pub fn job_and_instance(resource_attrs: &[KeyValue]) -> (Option<String>, Option<String>) {
    let job = first_attr(resource_attrs, SERVICE_NAME_KEY).map(|service_name| {
        match first_attr(resource_attrs, SERVICE_NAMESPACE_KEY) {
            Some(namespace) => format!("{namespace}/{service_name}"),
            None => service_name,
        }
    });
    let instance = first_attr(resource_attrs, SERVICE_INSTANCE_ID_KEY);
    (
        job.filter(|v| !v.is_empty()),
        instance.filter(|v| !v.is_empty()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The unit maps are the reference's whole tables, not a spot check:
    /// `metric_namer.go:34-68` and `:72-80` @ `otlptranslator@v1.0.0`.
    /// Inventing or dropping one entry silently changes a stored metric
    /// name, so the assertion is the full list, in the reference's own
    /// order.
    #[test]
    fn the_unit_maps_are_the_references_full_tables() {
        assert_eq!(
            UNIT_MAP,
            &[
                ("d", "days"),
                ("h", "hours"),
                ("min", "minutes"),
                ("s", "seconds"),
                ("ms", "milliseconds"),
                ("us", "microseconds"),
                ("ns", "nanoseconds"),
                ("By", "bytes"),
                ("KiBy", "kibibytes"),
                ("MiBy", "mebibytes"),
                ("GiBy", "gibibytes"),
                ("TiBy", "tibibytes"),
                ("KBy", "kilobytes"),
                ("MBy", "megabytes"),
                ("GBy", "gigabytes"),
                ("TBy", "terabytes"),
                ("m", "meters"),
                ("V", "volts"),
                ("A", "amperes"),
                ("J", "joules"),
                ("W", "watts"),
                ("g", "grams"),
                ("Cel", "celsius"),
                ("Hz", "hertz"),
                ("1", ""),
                ("%", "percent"),
            ]
        );
        assert_eq!(UNIT_MAP.len(), 26);
        assert_eq!(
            PER_UNIT_MAP,
            &[
                ("s", "second"),
                ("m", "minute"),
                ("h", "hour"),
                ("d", "day"),
                ("w", "week"),
                ("mo", "month"),
                ("y", "year"),
            ]
        );
        assert_eq!(PER_UNIT_MAP.len(), 7);
    }

    /// The reference's own doc-comment examples, verbatim:
    /// `metric_namer.go:87-98` and `:142-150`.
    #[test]
    fn metric_namer_reproduces_the_references_doc_comment_examples() {
        let namer = MetricNamer {
            with_suffixes: true,
            utf8_allowed: false,
        };
        assert_eq!(
            namer
                .build("http.server.duration", "s", MetricKind::Histogram)
                .expect("valid"),
            "http_server_duration_seconds"
        );
        assert_eq!(
            namer
                .build("requests.count", "1", MetricKind::MonotonicCounter)
                .expect("valid"),
            "requests_count_total"
        );
        assert_eq!(
            namer
                .build("memory.usage", "By", MetricKind::Gauge)
                .expect("valid"),
            "memory_usage_bytes"
        );
    }

    /// `unit_namer.go:23-25` and `:42-45`.
    #[test]
    fn unit_namer_reproduces_the_references_doc_comment_examples() {
        let namer = UnitNamer {
            utf8_allowed: false,
        };
        assert_eq!(namer.build("s"), "seconds");
        assert_eq!(namer.build("By/s"), "bytes_per_second");
        assert_eq!(namer.build("requests/s"), "requests_per_second");
        assert_eq!(namer.build(""), "");
        assert_eq!(namer.build("1"), "");
    }

    /// `label_namer.go:190-193`.
    #[test]
    fn label_namer_reproduces_the_references_doc_comment_examples() {
        let namer = LabelNamer {
            utf8_allowed: false,
        };
        assert_eq!(namer.build("http.method").expect("valid"), "http_method");
        assert_eq!(namer.build("123invalid").expect("valid"), "key_123invalid");
        assert_eq!(namer.build("__reserved__").expect("valid"), "__reserved__");
    }

    /// The two rejection messages, and the one accept that is easy to
    /// reject by mistake. Their exact text is asserted against the live
    /// reference by `tests/otlp_prom_translation.rs`; here they pin the
    /// verdict boundary.
    #[test]
    fn label_namer_rejects_empty_and_all_underscore_results() {
        let namer = LabelNamer {
            utf8_allowed: false,
        };
        assert_eq!(namer.build("").unwrap_err(), "label name is empty");
        assert_eq!(
            namer.build("--").unwrap_err(),
            "normalization for label name \"--\" resulted in invalid name \"__\""
        );
        assert_eq!(
            namer.build("日本").unwrap_err(),
            "normalization for label name \"日本\" resulted in invalid name \"__\""
        );
        // Two runes, two underscores — the preserve-multiple-underscores
        // default. The Loki-vintage translator in
        // `crate::protocols::label_name` collapses these to one, which is
        // why this module does not reuse it.
        assert_eq!(namer.build("naïve").expect("accepted"), "na_ve");
        assert_eq!(
            namer.build("multi__under").expect("accepted"),
            "multi__under"
        );
    }

    /// Under a UTF-8-allowing strategy the namer validates almost nothing:
    /// only an empty name and an all-underscore name are refused, and the
    /// refusal message is a different one (`label_namer.go:199-204`).
    #[test]
    fn label_namer_passes_utf8_names_through() {
        let namer = LabelNamer { utf8_allowed: true };
        assert_eq!(namer.build("a.b").expect("accepted"), "a.b");
        assert_eq!(namer.build("café").expect("accepted"), "café");
        assert_eq!(namer.build("").unwrap_err(), "label name is empty");
        assert_eq!(
            namer.build("__").unwrap_err(),
            "label name \"__\" contains only underscores"
        );
    }

    /// `removeItem` strips EVERY `total` token, so a monotonic
    /// `bar.total.total` is `bar_total` — not `bar_total_total`, which is
    /// what the no-suffix strategy produces, and not `bar.total_total`,
    /// which is what the UTF-8 strategy's trim-one rule produces. The three
    /// answers are captured from three live containers in
    /// `tests/fixtures/otlp-metrics/prom-translation/cases.json`; this
    /// pins the branch that distinguishes them.
    #[test]
    fn the_three_total_stripping_rules_disagree_by_design() {
        let compliant = MetricNamer {
            with_suffixes: true,
            utf8_allowed: false,
        };
        let utf8 = MetricNamer {
            with_suffixes: true,
            utf8_allowed: true,
        };
        let no_suffix = MetricNamer {
            with_suffixes: false,
            utf8_allowed: false,
        };
        let name = "bar.total.total";
        assert_eq!(
            compliant
                .build(name, "", MetricKind::MonotonicCounter)
                .expect("valid"),
            "bar_total"
        );
        assert_eq!(
            utf8.build(name, "", MetricKind::MonotonicCounter)
                .expect("valid"),
            "bar.total_total"
        );
        assert_eq!(
            no_suffix
                .build(name, "", MetricKind::MonotonicCounter)
                .expect("valid"),
            "bar_total_total"
        );
    }

    /// `strings.FieldsFunc` treats a RUN of invalid characters as one
    /// separator that produces no token, so a CJK segment vanishes rather
    /// than becoming underscores — the opposite of the per-rune label rule
    /// in the same module.
    #[test]
    fn metric_names_drop_invalid_runs_where_label_names_replace_each_rune() {
        let namer = MetricNamer {
            with_suffixes: true,
            utf8_allowed: false,
        };
        assert_eq!(
            namer
                .build("http.服务.duration", "s", MetricKind::Gauge)
                .expect("valid"),
            "http_duration_seconds"
        );
        let labels = LabelNamer {
            utf8_allowed: false,
        };
        assert_eq!(labels.build("café").expect("valid"), "caf_");
    }

    /// The empty-name and all-underscore rejections
    /// (`metric_namer.go:158-177`). The all-underscore branch is only
    /// reachable from the no-suffix path, whose `FieldsFunc` predicate
    /// keeps `_` inside a token.
    #[test]
    fn metric_namer_rejections_carry_the_references_messages() {
        let compliant = MetricNamer {
            with_suffixes: true,
            utf8_allowed: false,
        };
        for name in ["...", "", "._."] {
            assert_eq!(
                compliant.build(name, "", MetricKind::Gauge).unwrap_err(),
                format!("normalization for metric {name:?} resulted in empty name")
            );
        }
        let no_suffix = MetricNamer {
            with_suffixes: false,
            utf8_allowed: false,
        };
        assert_eq!(
            no_suffix.build("._.", "", MetricKind::Gauge).unwrap_err(),
            "normalization for metric \"._.\" resulted in invalid name \"_\""
        );
    }

    /// `job` is `service.namespace + "/" + service.name`, or `service.name`
    /// alone; an absent `service.name` yields no `job` even when the
    /// namespace is present, and an empty derived value yields none at all
    /// (`metrics_to_prw.go:420-430`, `helper.go:141-146`).
    #[test]
    fn job_and_instance_follow_the_references_derivation() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
        fn kv(key: &str, value: &str) -> KeyValue {
            KeyValue {
                key: key.to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(value.to_string())),
                }),
                key_strindex: 0,
            }
        }
        assert_eq!(
            job_and_instance(&[kv("service.name", "svc")]),
            (Some("svc".to_string()), None)
        );
        assert_eq!(
            job_and_instance(&[kv("service.name", "svc"), kv("service.namespace", "ns")]),
            (Some("ns/svc".to_string()), None)
        );
        assert_eq!(
            job_and_instance(&[kv("service.namespace", "ns")]),
            (None, None)
        );
        assert_eq!(job_and_instance(&[kv("service.name", "")]), (None, None));
        assert_eq!(
            job_and_instance(&[kv("service.name", ""), kv("service.namespace", "ns")]),
            (Some("ns/".to_string()), None)
        );
        assert_eq!(
            job_and_instance(&[kv("service.instance.id", "pod-7")]),
            (None, Some("pod-7".to_string()))
        );
        assert_eq!(
            job_and_instance(&[kv("service.instance.id", "")]),
            (None, None)
        );
    }
}
