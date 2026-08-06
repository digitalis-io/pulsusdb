//! The reference's label-NAME admissibility rule for the two ingest seams
//! that accept a *free-form* name — Loki-push structured metadata and OTLP
//! attributes (issue #259).
//!
//! Every such name reaches exactly one predicate on the reference,
//! `otlptranslator.LabelNamer.Build`
//! (`vendor/github.com/prometheus/otlptranslator/label_namer.go:66-90 @
//! v3.7.4`), which both *normalizes* the name and *rejects* two classes of
//! name outright. It is reached from exactly two ingest sites:
//!
//! | reference site @ v3.7.4 | names it covers |
//! |---|---|
//! | `pkg/distributor/distributor.go:689-706` | every Loki-push entry's structured-metadata names, both push encodings |
//! | `pkg/loghttp/push/otlp.go:603-614` (`attributeToLabels`) | every OTLP resource / scope / record attribute key |
//!
//! (`pkg/chunkenc/symbols.go:120-135` calls it too, but that is a storage
//! re-normalization of already-admitted names, not an ingest gate.)
//!
//! This module is the ingest-side counterpart of the REJECT half only. The
//! NORMALIZE half — `sanitize` plus the `key_` prefix — is replicated here
//! solely because condition (2) below is evaluated on the *sanitized* name,
//! and is deliberately NOT used to rename anything: PulsusDB stores an
//! admitted name under [`canonicalize_label_key`](pulsus_model::canonicalize_label_key),
//! which differs from the reference's renaming for some admitted inputs (see
//! the module test `sanitize_differs_from_our_storage_canonicalization`).
//!
//! Distinct from `loki_push::is_valid_label_name`, which is the STREAM-label
//! grammar `[a-zA-Z_][a-zA-Z0-9_]*`: a stream label name is parsed, not
//! sanitized, so `a.b` is a hard 400 there and a stored `a_b` here.

use crate::error::LogsIngestError;

/// `Build`'s two rejection conditions, checked in the reference's order and
/// reported with the reference's error text verbatim
/// (`label_namer.go:66-90 @ v3.7.4`):
///
/// 1. `len(label) == 0` -> `label name is empty`. EXACTLY the empty string;
///    nothing is trimmed, so this is a third empty-name rule distinct from
///    both empty-VALUE strips in [`pulsus_model::retain_non_empty_values`] /
///    [`pulsus_model::strip_empty_valued_labels`].
/// 2. the sanitized name consists only of `_` ->
///    `normalization for label name %q resulted in invalid name %q`. A
///    whitespace-only name lands HERE, not in (1): `" "` sanitizes to `"_"`.
///
/// Measured against `grafana/loki:3.7.4` (image ID `fe5a84aafad8`, index
/// digest `sha256:87f0a067…`, git revision `b318f282`), pushing each name as
/// structured metadata on both push encodings and as an OTLP attribute —
/// every row below reproduced identically on all three:
///
/// | name | verdict |
/// |---|---|
/// | `""` | rejected, `label name is empty` |
/// | `" "`, `"  "`, `"\t"` | rejected, sanitizes to `"_"` |
/// | `"_"`, `"__"` | rejected, sanitizes to `"_"` |
/// | `"____"` | rejected, sanitizes to `"____"` (reserved-label path) |
/// | `"."`, `"..."`, `"-_-"` | rejected, sanitizes to `"_"` |
/// | `"a.b"`, `"a..b"`, `"a__b"`, `"9bad"`, `"_x"`, `"__foo__"`, `"naïve"`, `"ok "`, `"ok"` | accepted |
///
/// The check runs on the RAW name, BEFORE any empty-value strip: measured, a
/// pair with name `" "` and value `""` is rejected rather than silently
/// stripped, on all three seams.
pub(crate) fn validate_label_name(name: &str) -> Result<(), LogsIngestError> {
    if name.is_empty() {
        return Err(LogsIngestError::InvalidLabelName(
            "label name is empty".to_string(),
        ));
    }
    let normalized = sanitize(name);
    if normalized.bytes().all(|b| b == b'_') {
        return Err(LogsIngestError::InvalidLabelName(format!(
            "normalization for label name {name:?} resulted in invalid name {normalized:?}"
        )));
    }
    Ok(())
}

/// [`validate_label_name`] over a borrowed name sequence, so a caller can
/// charge the check without cloning anything on the reject path.
pub(crate) fn validate_label_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> Result<(), LogsIngestError> {
    for name in names {
        validate_label_name(name)?;
    }
    Ok(())
}

/// `LabelNamer.Build`'s normalization for the zero-valued `LabelNamer{}` both
/// reference call sites construct — `UTF8Allowed`,
/// `UnderscoreLabelSanitization` and `PreserveMultipleUnderscores` all false.
/// That fixes the shape to `sanitizeLabelName(name, false)` plus the leading-
/// digit `key_` prefix (`label_namer.go:73-85`, `strconv.go:30-70 @ v3.7.4`):
///
/// - a rune outside `[a-zA-Z0-9]` becomes `_`, and CONSECUTIVE such runes
///   collapse to a single `_` — note `_` is itself not a "valid compliant
///   char" (`strconv.go:73-75`), so `a__b` collapses to `a_b`;
/// - a name that both starts and ends with `__` and is at least 4 bytes long
///   is "reserved": the affixes are stripped, the middle sanitized, and the
///   affixes restored (`strconv.go:81-89`), which is why `"____"` survives as
///   `"____"` where `"__"` collapses to `"_"`;
/// - a result starting with a digit is prefixed `key_`.
///
/// Used ONLY to evaluate the all-underscores rejection above. The result is
/// never stored; see the module docs.
fn sanitize(name: &str) -> String {
    // `isReservedLabel`: len >= 4 AND starts and ends with `__`
    // (`strconv.go:81-89 @ v3.7.4`). Byte-oriented on the reference (Go string
    // slicing), and `_` is ASCII, so byte prefix/suffix tests are exact.
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

    // `unicode.IsDigit(rune(normalizedName[0]))` (`label_namer.go:80-81`).
    // `out` is non-empty here for any non-empty `name` (a reserved name
    // carries its affixes; any other name emits at least one char for its
    // first rune), and every byte in it is ASCII, so the reference's
    // byte-indexed first-rune test is exactly this one.
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert_str(0, "key_");
    }
    out
}

#[cfg(test)]
mod tests {
    use pulsus_model::canonicalize_label_key;

    use super::*;

    fn reject_message(name: &str) -> String {
        match validate_label_name(name) {
            Err(LogsIngestError::InvalidLabelName(message)) => message,
            other => panic!("expected {name:?} to be rejected, got {other:?}"),
        }
    }

    #[test]
    fn the_empty_name_is_rejected_with_the_references_first_message() {
        // Condition (1). Byte-identical to what `grafana/loki:3.7.4` writes
        // into the push response body.
        assert_eq!(reject_message(""), "label name is empty");
    }

    #[test]
    fn a_whitespace_only_name_is_rejected_by_the_normalization_rule_not_the_empty_rule() {
        // The discriminating pair: `""` and `" "` are BOTH rejected, but by
        // different conditions and with different text — so "empty name"
        // means exactly `len == 0`, and nothing is trimmed anywhere.
        assert_eq!(
            reject_message(" "),
            r#"normalization for label name " " resulted in invalid name "_""#
        );
        assert_eq!(
            reject_message("  "),
            r#"normalization for label name "  " resulted in invalid name "_""#
        );
        assert_eq!(
            reject_message("\t"),
            r#"normalization for label name "\t" resulted in invalid name "_""#
        );
    }

    #[test]
    fn an_all_underscore_name_is_rejected_and_four_underscores_take_the_reserved_path() {
        // `__` is under the 4-byte reserved threshold and collapses to `_`;
        // `____` is at it, so the affixes are preserved around an empty
        // middle. Both measured on the container.
        assert_eq!(
            reject_message("_"),
            r#"normalization for label name "_" resulted in invalid name "_""#
        );
        assert_eq!(
            reject_message("__"),
            r#"normalization for label name "__" resulted in invalid name "_""#
        );
        assert_eq!(
            reject_message("____"),
            r#"normalization for label name "____" resulted in invalid name "____""#
        );
    }

    #[test]
    fn a_punctuation_only_name_is_rejected() {
        assert_eq!(
            reject_message("."),
            r#"normalization for label name "." resulted in invalid name "_""#
        );
        assert_eq!(
            reject_message("..."),
            r#"normalization for label name "..." resulted in invalid name "_""#
        );
        assert_eq!(
            reject_message("-_-"),
            r#"normalization for label name "-_-" resulted in invalid name "_""#
        );
    }

    #[test]
    fn every_name_the_reference_admits_is_admitted() {
        // The accept half of the measured table, so a future tightening of
        // the predicate cannot silently start rejecting admitted input.
        for name in [
            "ok", "a.b", "a..b", "a__b", "9bad", "_x", "__foo__", "naïve", "ok ", "a_", "__x",
        ] {
            assert!(
                validate_label_name(name).is_ok(),
                "{name:?} must be admitted"
            );
        }
    }

    #[test]
    fn validate_label_names_reports_the_first_offender_and_admits_a_clean_list() {
        assert_eq!(
            match validate_label_names(["ok", "", "_"]) {
                Err(LogsIngestError::InvalidLabelName(message)) => message,
                other => panic!("expected a rejection, got {other:?}"),
            },
            "label name is empty"
        );
        assert!(validate_label_names(["ok", "a.b"]).is_ok());
    }

    #[test]
    fn sanitize_matches_the_references_measured_renaming() {
        // Read back from `grafana/loki:3.7.4` with `categorize-labels` after
        // pushing each name as structured metadata.
        for (input, expected) in [
            ("a.b", "a_b"),
            ("a..b", "a_b"),
            ("a__b", "a_b"),
            ("9bad", "key_9bad"),
            ("_x", "_x"),
            ("__foo__", "__foo__"),
            ("naïve", "na_ve"),
            ("ok ", "ok_"),
            ("ok", "ok"),
        ] {
            assert_eq!(sanitize(input), expected, "sanitize({input:?})");
        }
    }

    #[test]
    fn sanitize_differs_from_our_storage_canonicalization() {
        // Pinned so the divergence stays visible: this module's `sanitize`
        // exists to evaluate the REJECT rule only, and PulsusDB stores an
        // admitted name under `canonicalize_label_key` instead. These three
        // inputs are admitted by both and stored under different keys —
        // flagged on issue #259, not fixed there.
        for name in ["a..b", "a__b", "9bad"] {
            assert_ne!(
                sanitize(name),
                canonicalize_label_key(name),
                "{name:?} is expected to render differently"
            );
        }
        // …while the common case agrees, so the divergence is narrow.
        assert_eq!(sanitize("a.b"), canonicalize_label_key("a.b"));
    }
}
