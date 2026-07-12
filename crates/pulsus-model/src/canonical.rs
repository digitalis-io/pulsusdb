//! Label key canonicalization: the sole authority for turning an OTel
//! attribute key into a Prometheus-style label name (docs/architecture.md
//! §2.2). One function drives the label key, the OTel attribute, and the
//! physical `service` column value (issue #4 AC#3) — there is exactly one
//! place this rule is expressed.

/// The label key that carries the metric name (`__name__` in Prometheus
/// exposition format). Always excluded from
/// [`crate::fingerprint::metric_fingerprint`]'s buffer.
pub const METRIC_NAME_LABEL: &str = "__name__";

/// The normalized label key that carries the OTel `service.name` resource
/// attribute after [`canonicalize_label_key`] (`"service.name"` ->
/// `"service_name"`).
pub const SERVICE_NAME_LABEL: &str = "service_name";

/// Canonicalizes a single label key: every Unicode scalar value outside
/// `[a-zA-Z0-9_]` becomes a single `_` (docs/architecture.md §2.2, e.g.
/// `service.name` -> `service_name`, `k8s.pod.name` -> `k8s_pod_name`).
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

#[cfg(test)]
mod tests {
    use super::*;

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
