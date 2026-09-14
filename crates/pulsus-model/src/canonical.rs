//! Label name rules for ingest (docs/architecture.md §2.3). Two rules, each
//! written in exactly one function:
//! - [`log_label_name`]: the name an OTLP log resource or scope attribute
//!   key, or a structured-metadata name on either push encoding, is stored
//!   under — the reference's label namer (issue #507). The rejection rule in
//!   `pulsus-write`'s `protocols::label_name` calls it, so what is refused
//!   and what is stored cannot drift apart. One stored name is not this
//!   function's output: a resource attribute other than `service.name` whose
//!   name lands on `service_name` is stored as `service_name_extracted`
//!   (issue #379), a collision rename applied after it.
//! - [`canonicalize_label_key`]: each character outside `[a-zA-Z0-9_]`
//!   becomes its own `_`, with no collapsing. The OTLP metrics namer builds on
//!   it, and `LabelSet::from_normalized` groups by it (pushed stream labels,
//!   remote-write labels, the bound check over the eighteen OTLP index
//!   attributes, and the render of already-resolved metadata).
//!
//! The physical `service` column is filled from the resolved `service_name`
//! slot, which both rules name alike (`service.name` -> `service_name`).

/// The label key that carries the metric name (`__name__` in Prometheus
/// exposition format). Always excluded from
/// [`crate::fingerprint::metric_fingerprint`]'s buffer.
pub const METRIC_NAME_LABEL: &str = "__name__";

/// The label key that carries the OTel `service.name` resource attribute:
/// `"service.name"` -> `"service_name"`, which both of this module's namers
/// return for that key.
pub const SERVICE_NAME_LABEL: &str = "service_name";

/// Canonicalizes a single label key: every Unicode scalar value outside
/// `[a-zA-Z0-9_]` becomes a single `_` (docs/architecture.md §2.3's metrics
/// bullet; e.g. `service.name` -> `service_name`, `a..b` -> `a__b`). This is
/// not the log attribute rule: see [`log_label_name`].
///
/// Operates per-*character*, not per-byte: a multi-byte UTF-8 codepoint
/// outside the ASCII allow-list collapses to exactly one `_`, so a
/// canonicalized key's length in bytes never exceeds the input's length in
/// `char`s, and multi-byte input never produces a run of `_` characters
/// proportional to its byte width.
pub fn canonicalize_label_key(key: &str) -> String {
    key.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// The name a structured-metadata name, an OTLP scope attribute key or an
/// OTLP resource attribute key is stored under (issue #507): the reference's
/// label namer, `otlptranslator.LabelNamer{}.Build`
/// (`vendor/github.com/prometheus/otlptranslator/label_namer.go:65-91 @
/// v3.7.4`), with the zero-valued options its three call sites construct —
/// `pkg/loghttp/push/otlp.go:610`, `pkg/distributor/distributor.go:689` and
/// `pkg/chunkenc/symbols.go:120`, all `LabelNamer{}`, so `UTF8Allowed`,
/// `UnderscoreLabelSanitization` and `PreserveMultipleUnderscores` are all
/// false.
///
/// The rule, which is `sanitizeLabelName(name, false)`
/// (`strconv.go:30-71 @ v3.7.4`) plus `Build`'s leading-digit prefix:
///
/// 1. ASCII letters and digits are kept (`isValidCompliantLabelChar`,
///    `strconv.go:74-76`).
/// 2. Every run of other characters becomes one `_`. `_` is itself not a
///    valid character there, so `a__b` is stored as `a_b`.
/// 3. A name of at least 4 bytes that starts and ends with `__` is
///    "reserved" (`isReservedLabel`, `strconv.go:81-90`): the affixes are
///    kept around the collapsed middle, so `__foo__` stays `__foo__` and
///    `--error--` becomes `_error_`.
/// 4. A result starting with a digit gains `key_` (`label_namer.go:80-81`).
///
/// Defined for admissible names only. The rejection rule — an empty name,
/// and a name whose stored form is nothing but underscores — is
/// `crates/pulsus-write/src/protocols/label_name.rs`, which calls this so
/// that what is refused and what is stored come from one function.
///
/// One stored name is not this function's output: an OTLP resource
/// attribute other than `service.name` whose name lands on `service_name` is
/// stored as `service_name_extracted` (issue #379), a collision rename
/// applied after it.
pub fn log_label_name(name: &str) -> String {
    // `isReservedLabel`: at least 4 bytes, and both affixes. Byte-oriented
    // on the reference (Go string slicing), and `_` is ASCII, so the byte
    // prefix and suffix tests are exact.
    let reserved = name.len() >= 4 && name.starts_with("__") && name.ends_with("__");
    let inner = if reserved {
        &name[2..name.len() - 2]
    } else {
        name
    };
    let mut out = String::with_capacity(name.len() + if reserved { 4 } else { 0 });
    if reserved {
        out.push_str("__");
    }
    let mut prev_was_underscore = false;
    for c in inner.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c);
            prev_was_underscore = false;
        } else if !prev_was_underscore {
            out.push('_');
            prev_was_underscore = true;
        }
    }
    if reserved {
        out.push_str("__");
    }
    // `out` is non-empty for any non-empty `name`, and holds only ASCII, so
    // the reference's byte-indexed first-rune test is exactly this one.
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert_str(0, "key_");
    }
    out
}

/// `log_label_name(name) == name`, without allocating (issue #507): the
/// fast-path test in [`crate::resolve_structured_metadata`], which renames
/// nothing when every name it was handed is already its stored name.
pub fn is_log_label_name(name: &str) -> bool {
    let reserved = name.len() >= 4 && name.starts_with("__") && name.ends_with("__");
    let inner = if reserved {
        &name[2..name.len() - 2]
    } else {
        name
    };
    let mut prev_was_underscore = false;
    let mut first = true;
    for c in inner.chars() {
        if c.is_ascii_alphanumeric() {
            if first && !reserved && c.is_ascii_digit() {
                // The stored name would gain `key_`.
                return false;
            }
            prev_was_underscore = false;
        } else if c == '_' && !prev_was_underscore {
            prev_was_underscore = true;
        } else {
            // A run of two, or any character the namer would replace.
            return false;
        }
        first = false;
    }
    // A reserved name's stored form keeps its affixes, so the affixes
    // themselves never make it differ; an inner that is empty leaves
    // `____`, which is `name` again.
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The names issue #507 measured against the reference, and the names
    /// `crates/pulsus-write/src/protocols/label_name.rs` replays its
    /// rejection rule over. Both halves are here because
    /// [`log_label_name`] is the one function behind the rejection rule and
    /// the stored name.
    const NAME_MATRIX: &[&str] = &[
        // issue #507's measured table
        "a.b",
        "a..b",
        "a__b",
        "9bad",
        "1.2",
        "_x",
        "__a",
        "___a___",
        "__foo__",
        "__error__",
        "__error_details__",
        "__error___extracted",
        "--error--",
        "service..name",
        "service_name",
        "service.name",
        "k8s..pod",
        "k8s.pod.name",
        "9zone",
        "naïve",
        "ok ",
        "ok",
        "a_b",
        "key_9bad",
        "key_9zone",
        "_error_",
        "detected.level",
        "log.level",
        "severity..text",
        "scope.attr.foo",
        "s..x",
        "9s",
        "a-b",
        "a b",
        "A.B",
        "team",
        "env",
        "region",
        "stream_ordinal",
        "cloud.region",
        "container.name",
        "http.method",
        // the rejection rule's matrix
        "",
        " ",
        "  ",
        "\t",
        "_",
        "__",
        "____",
        ".",
        "...",
        "-_-",
        "µ",
        "日本",
        "é",
        "🙂",
        "__é__",
        "Ωmega",
        "nai\u{308}ve",
        "a\u{1}b",
        "9",
        "9.",
        "٣0",
        "a__",
        "__x",
        "__a_",
        " a ",
        "a_",
        "\u{0}",
        "\u{1b}",
        "\u{7f}",
        "\u{80}",
        "\u{a0}",
        "\u{3000}",
        "\u{200b}",
        "\u{301}",
    ];

    /// **N1 (issue #507): the stored name is the reference's.** Every row of
    /// the plan's measured table, whose right column came from the pinned
    /// reference build.
    #[test]
    fn log_label_name_is_the_references_stored_name() {
        for (input, stored) in [
            ("a.b", "a_b"),
            ("a..b", "a_b"),
            ("a__b", "a_b"),
            ("9bad", "key_9bad"),
            ("1.2", "key_1_2"),
            ("_x", "_x"),
            ("__a", "_a"),
            ("___a___", "___a___"),
            ("__foo__", "__foo__"),
            ("__error__", "__error__"),
            ("__error_details__", "__error_details__"),
            ("__error___extracted", "_error_extracted"),
            ("--error--", "_error_"),
            ("service..name", "service_name"),
            ("k8s..pod", "k8s_pod"),
            ("9zone", "key_9zone"),
            ("naïve", "na_ve"),
            ("ok ", "ok_"),
        ] {
            assert_eq!(log_label_name(input), stored, "{input:?}");
        }
        // The two names the extracted-field group key read watches keep
        // their names, so its presence rule is unchanged.
        assert_eq!(log_label_name("__error__"), "__error__");
        assert_eq!(log_label_name("__error_details__"), "__error_details__");
    }

    /// **N2 (issue #507), first half: the namer is a fixed point of
    /// itself**, which is why stored data needs no second rename — the
    /// reference renames again when it reads, and that is a no-op on names
    /// it wrote.
    #[test]
    fn log_label_name_is_a_fixed_point_of_itself() {
        for name in NAME_MATRIX {
            let once = log_label_name(name);
            assert_eq!(log_label_name(&once), once, "{name:?} -> {once:?}");
        }
    }

    /// **N2, second half: the allocation-free predicate is the equality.**
    /// [`is_log_label_name`] must answer exactly `log_label_name(n) == n`,
    /// because it is what decides whether a metadata entry is renamed at
    /// all.
    #[test]
    fn is_log_label_name_agrees_with_log_label_name() {
        for name in NAME_MATRIX {
            assert_eq!(
                is_log_label_name(name),
                log_label_name(name) == **name,
                "{name:?} (stored as {:?})",
                log_label_name(name)
            );
        }
    }

    #[test]
    fn dot_separated_otel_key_normalizes_to_underscore() {
        assert_eq!(canonicalize_label_key("service.name"), "service_name");
        assert_eq!(canonicalize_label_key("service.name"), SERVICE_NAME_LABEL);
    }

    #[test]
    fn multi_segment_otel_key_normalizes_every_separator() {
        assert_eq!(canonicalize_label_key("k8s.pod.name"), "k8s_pod_name");
    }

    #[test]
    fn leading_digit_is_left_unchanged() {
        // Digits are already in the allow-list at any position; this
        // function does not prefix or otherwise special-case leading
        // digits — that is a caller-side concern (out of scope, issue #4).
        assert_eq!(canonicalize_label_key("9lives"), "9lives");
    }

    #[test]
    fn already_canonical_key_is_unchanged() {
        assert_eq!(canonicalize_label_key("service_name"), "service_name");
    }

    #[test]
    fn multi_byte_unicode_codepoint_collapses_to_one_underscore() {
        // "café" -> 'c','a','f' pass through, 'é' (2 UTF-8 bytes, one
        // `char`) collapses to exactly one `_`, not one per byte.
        assert_eq!(canonicalize_label_key("café"), "caf_");
        // A 3-byte and a 4-byte codepoint each still collapse to one `_`.
        assert_eq!(canonicalize_label_key("中"), "_");
        assert_eq!(canonicalize_label_key("😀"), "_");
    }

    #[test]
    fn empty_key_canonicalizes_to_empty_string() {
        assert_eq!(canonicalize_label_key(""), "");
    }

    #[test]
    fn metric_name_label_constant_is_dunder_name() {
        assert_eq!(METRIC_NAME_LABEL, "__name__");
    }
}
