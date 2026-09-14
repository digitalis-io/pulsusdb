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
//! This module is the REJECT half. The NORMALIZE half is
//! [`log_label_name`](pulsus_model::log_label_name), which condition (2)
//! below is evaluated on and which is also the name every admitted name is
//! stored under (issue #507) — except a resource attribute other than
//! `service.name` landing on `service_name`, which is stored as
//! `service_name_extracted` (issue #379) — so the two halves cannot drift
//! apart (the
//! module test `the_rejection_rule_and_storage_name_alike`).
//!
//! Distinct from `loki_push::is_valid_label_name`, which is the STREAM-label
//! grammar `[a-zA-Z_][a-zA-Z0-9_]*`: a stream label name is parsed, not
//! sanitized, so `a.b` is a hard 400 there and a stored `a_b` here.
//!
//! **The two seams do not carry the same bytes.** `Build`'s text reaches the
//! wire bare on the push transport and wrapped in `symbolizer lookup: ` on
//! the OTLP one, so this module exposes one entry point per seam —
//! [`validate_label_names`] and [`validate_otlp_attribute_names`]. See the
//! latter for the reference lines and the measurement.

use pulsus_model::log_label_name;

use crate::error::LogsIngestError;

/// The OTLP transport's envelope around `Build`'s text — the reference's own
/// `fmt.Errorf("symbolizer lookup: %w", err)`
/// (`pkg/loghttp/push/otlp.go:613 @ v3.7.4`). See
/// [`validate_otlp_attribute_names`].
const SYMBOLIZER_LOOKUP_PREFIX: &str = "symbolizer lookup: ";

/// `Build`'s two rejection conditions, checked in the reference's order and
/// reported with the reference's message — verbatim except for how an
/// unprintable name is echoed, see the parity note below
/// (`label_namer.go:66-90 @ v3.7.4`):
///
/// 1. `len(label) == 0` -> `label name is empty`. EXACTLY the empty string;
///    nothing is trimmed. This is rule 4 of the four distinct notions of
///    "empty" docs/api.md §8.2 lists side by side, and it is none of the
///    other three — the pair-wise value strip
///    ([`pulsus_model::retain_non_empty_values`]), the by-name value delete
///    and its delete-after-normalization half (both
///    [`pulsus_model::resolve_structured_metadata`]).
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
/// | `"µ"`, `"日本"`, `"é"`, `"🙂"` | rejected — every non-ASCII rune is an invalid char, and consecutive ones collapse to ONE `_` |
/// | `"__é__"` | rejected, sanitizes to `"_____"` (reserved affixes around a collapsed middle) |
/// | `"a.b"`, `"a..b"`, `"a__b"`, `"9bad"`, `"_x"`, `"__foo__"`, `"naïve"`, `"ok "`, `"ok"` | accepted |
/// | `"Ωmega"`, `"a\u{1}b"`, `"9"`, `"1.2"`, `"__a"`, `"___a___"` | accepted — one surviving ASCII alphanumeric is enough |
///
/// The check runs on the RAW name, BEFORE any empty-value strip: measured, a
/// pair with name `" "` and value `""` is rejected rather than silently
/// stripped, on all three seams.
///
/// **Verdict parity is exhaustive, message parity is not.** The whole rule
/// was replayed against the reference's OWN code — `label_namer.go` +
/// `strconv.go @ v3.7.4` compiled verbatim and driven over an 80-name matrix
/// covering the shapes above plus control characters, C1 bytes, NUL, NBSP,
/// ideographic space, zero-width space, lone combining marks, RTL and
/// Indic-digit names, `key_`-prefix shapes and every reserved-affix near
/// miss. Accept/reject agrees on **80 of 80**. The rejection *text* agrees on
/// 71 of 80: the nine that differ all echo the offending name through Go's
/// `%q` there and Rust's `{:?}` here, which spell an escape differently
/// (`"\x01"` vs `"\u{1}"`, `"\x00"` vs `"\0"`, `"\u00a0"` vs `"\u{a0}"`) or
/// disagree on whether a combining mark is printable (Go prints U+0301 raw,
/// Rust escapes it).
///
/// **That difference is user-reachable and user-visible.** What is bounded is
/// the character CLASS, never the reachability: a lone combining mark is
/// precisely the sort of name this rule refuses, so a client keying
/// structured metadata `"\u{301}"` reads `…label name "́"…` (the raw UTF-8
/// bytes `cc 81`) from the reference and `…label name "\u{301}"…` from
/// PulsusDB — measured at the wire on both push encodings and on OTLP. Only
/// the quoted echo differs: the name is refused either way, with the same
/// condition and the same sentence. Comparing the two renderings over all
/// 1 112 064 codepoints, they agree on every assigned, printable,
/// non-combining character — the only letters/digits/punctuation/symbols in
/// the whole disagreeing set are U+FF9E and U+FF9F, the two halfwidth
/// katakana sound marks (which are `Grapheme_Extend`). Not fixed because Go's
/// `%q` printability is `strconv.IsPrint`'s own generated table (Unicode
/// categories L/M/N/P/S plus ASCII space) while Rust's `{:?}` additionally
/// escapes `Grapheme_Extend`, and neither table is reachable from the other's
/// standard library: matching bytes would mean vendoring a Unicode category
/// table and pinning it to the Go release that built the container image,
/// which moves independently of ours. Registered as a USER-VISIBLE divergence
/// in docs/api.md §8.2 and
/// docs/benchmarks/logs-differential-ledger.md; pinned by
/// `the_escape_syntax_for_an_unprintable_name_is_our_runtimes_not_the_references`.
pub(crate) fn validate_label_name(name: &str) -> Result<(), LogsIngestError> {
    match inadmissible_reason(name) {
        Some(reason) => Err(LogsIngestError::InvalidLabelName(reason)),
        None => Ok(()),
    }
}

/// [`validate_label_name`]'s two conditions as a bare message, so each
/// transport can add its own envelope (or none) without a second pass over
/// the name. `None` when the name is admissible.
fn inadmissible_reason(name: &str) -> Option<String> {
    if name.is_empty() {
        return Some("label name is empty".to_string());
    }
    // The REJECT half is evaluated on the stored name, which is
    // `pulsus_model::log_label_name` (issue #507), so the two cannot drift
    // apart.
    let normalized = log_label_name(name);
    if normalized.bytes().all(|b| b == b'_') {
        return Some(format!(
            "normalization for label name {name:?} resulted in invalid name {normalized:?}"
        ));
    }
    None
}

/// [`validate_label_name`] over a borrowed name sequence, so a caller can
/// charge the check without cloning anything on the reject path. This is the
/// **Loki-push** seam's entry point: the reference returns `Build`'s error
/// bare there (`pkg/distributor/distributor.go:703-706 @ v3.7.4`), so the
/// message carries no envelope — see [`validate_otlp_attribute_names`] for
/// the transport that adds one.
pub(crate) fn validate_label_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> Result<(), LogsIngestError> {
    for name in names {
        validate_label_name(name)?;
    }
    Ok(())
}

/// The **OTLP** seam's entry point: the same rule, reported with the
/// reference's own `symbolizer lookup: ` prefix.
///
/// The reference wraps `Build`'s error exactly once, at its single OTLP
/// attribute seam — `fmt.Errorf("symbolizer lookup: %w", err)`
/// (`pkg/loghttp/push/otlp.go:613 @ v3.7.4`) — and that wrapped text reaches
/// the wire unchanged: `attributesToLabels` propagates it as-is
/// (`otlp.go:582-601`), `otlpToLokiPushRequest` returns it unwrapped from
/// both the resource loop (`otlp.go:212-214`) and the scope loop
/// (`otlp.go:317-319`), `ParseOTLPRequest` from `otlp.go:48-52`, and
/// `pushHandler` writes `err.Error()` into the response body
/// (`pkg/distributor/http.go:88-98`). Nothing re-wraps it, so the prefix
/// appears exactly once.
///
/// Measured on `grafana/loki:3.7.4`, same empty attribute key, both
/// transports:
///
/// | transport | response body (bytes) |
/// |---|---|
/// | `POST /otlp/v1/logs` | `\x12&symbolizer lookup: label name is empty` (a `google.rpc.Status` carrying field 2 only) |
/// | `POST /loki/api/v1/push` | `label name is empty\n` (plain text) |
///
/// (The reference recurses `attributeToLabels` into a map-valued attribute
/// and wraps a second time there, `otlp.go:620-628` — unreachable for us:
/// our receiver renders a map attribute as one string-valued label rather
/// than flattening it into `prefix_key` labels, a pre-existing #109
/// difference, so an inner key never reaches this check.)
pub(crate) fn validate_otlp_attribute_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> Result<(), LogsIngestError> {
    for name in names {
        if let Some(reason) = inadmissible_reason(name) {
            return Err(LogsIngestError::InvalidLabelName(format!(
                "{SYMBOLIZER_LOOKUP_PREFIX}{reason}"
            )));
        }
    }
    Ok(())
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
            "ok",
            "a.b",
            "a..b",
            "a__b",
            "9bad",
            "_x",
            "__foo__",
            "naïve",
            "ok ",
            "a_",
            "__x",
            // Shapes the first implementation did not pick, all replayed
            // against the reference's own compiled `Build` and against
            // `grafana/loki:3.7.4`: one surviving ASCII alphanumeric is
            // enough, wherever it sits and whatever surrounds it.
            "Ωmega",        // a non-ASCII letter next to ASCII ones
            "nai\u{308}ve", // decomposed `naïve` — the combining mark is just another invalid rune
            "a\u{1}b",      // a control character between two admissible ones
            "9",
            "1.2",
            "9.",
            "٣0", // an Arabic-Indic digit is NOT an ASCII digit, so this sanitizes to `_0`
            "__a",
            "a__",
            "__a_",
            "___a___",
            " a ",
            "key_9bad",
        ] {
            assert!(
                validate_label_name(name).is_ok(),
                "{name:?} must be admitted"
            );
        }
    }

    /// The reference accepts `naïve` and refuses `µ` and `日本` — one Latin
    /// name keeps an ASCII letter, the other two sanitize to a lone `_`. The
    /// discriminating trio for "non-ASCII" being no rule at all: what counts
    /// is whether ANY `[a-zA-Z0-9]` survives (`isValidCompliantLabelChar`,
    /// `strconv.go:73-75 @ v3.7.4`).
    ///
    /// Measured on `grafana/loki:3.7.4` as push structured metadata: `naïve`
    /// -> `204`, `µ` and `日本` -> the normalization message. And as an OTLP
    /// resource attribute: `naïve` is admitted by the name check, the other
    /// two return `400 symbolizer lookup: normalization for label name …`.
    #[test]
    fn a_non_ascii_name_is_judged_only_by_whether_an_ascii_alphanumeric_survives() {
        assert!(validate_label_name("naïve").is_ok());
        assert_eq!(
            reject_message("µ"),
            r#"normalization for label name "µ" resulted in invalid name "_""#
        );
        assert_eq!(
            reject_message("日本"),
            r#"normalization for label name "日本" resulted in invalid name "_""#
        );
        // Two adjacent invalid runes collapse to ONE `_`, which is why a
        // two-character CJK name reports `"_"` rather than `"__"`.
        assert_eq!(log_label_name("日本"), "_");
    }

    /// The reserved-affix path over a multi-byte middle. `__é__` is 8 bytes,
    /// starts and ends with `__`, so the affixes are held back and the middle
    /// sanitized to a single `_` — `"_____"`, five underscores, which is
    /// still all-underscores and therefore refused. Slicing the middle is
    /// byte-indexed on both sides and cannot split a character here: the
    /// first two and last two bytes are ASCII `_`.
    ///
    /// Measured on `grafana/loki:3.7.4`: `normalization for label name
    /// "__é__" resulted in invalid name "_____"`.
    #[test]
    fn the_reserved_path_slices_a_multibyte_middle_without_splitting_it() {
        assert_eq!(
            reject_message("__é__"),
            r#"normalization for label name "__é__" resulted in invalid name "_____""#
        );
        assert_eq!(
            reject_message("__________"),
            r#"normalization for label name "__________" resulted in invalid name "_____""#
        );
        assert_eq!(log_label_name("__ab__"), "__ab__");
    }

    /// The one place our message is NOT the reference's, pinned so it cannot
    /// grow silently.
    ///
    /// Both sides echo the offending name through their runtime's debug
    /// quoter — Go's `%q` (`fmt.Errorf` in `label_namer.go:87 @ v3.7.4`) and
    /// Rust's `{:?}` — and the two spell escapes differently. Every entry
    /// below is the FULL known set for the 80-name matrix this rule was
    /// replayed over; the left column is what `grafana/loki:3.7.4` wrote on
    /// the wire for the same name, measured.
    ///
    /// **These are reachable names, and the difference is user-visible.** The
    /// last row is the demonstration: a lone U+0301 is a legitimate
    /// structured-metadata key to send, it is refused by both sides, and the
    /// two refusal bodies differ — the reference's carries the raw two bytes
    /// `cc 81` inside the quotes where ours carries the eight ASCII bytes
    /// `\u{301}` (measured on both push encodings and on OTLP, issue #259
    /// re-review). What is bounded is the CLASS of characters that differ,
    /// not whether a client can hit one: comparing `%q` with `{:?}` over all
    /// 1 112 064 codepoints, the two agree on every assigned printable
    /// non-combining character, and everything they disagree about is a
    /// control character, a format/separator character, an unassigned or
    /// private-use codepoint, or a `Grapheme_Extend` mark — the only
    /// exceptions carrying a letter/number/punctuation/symbol category at all
    /// are U+FF9E and U+FF9F, themselves `Grapheme_Extend`. Verdict and
    /// condition are unaffected: all of these are refused by both, with the
    /// same sentence.
    #[test]
    fn the_escape_syntax_for_an_unprintable_name_is_our_runtimes_not_the_references() {
        for (name, reference_text, ours) in [
            ("\u{1}", r#""\x01""#, r#""\u{1}""#),
            ("\u{0}", r#""\x00""#, r#""\0""#),
            ("\u{1b}", r#""\x1b""#, r#""\u{1b}""#),
            ("\u{7f}", r#""\x7f""#, r#""\u{7f}""#),
            ("\u{80}", r#""\u0080""#, r#""\u{80}""#),
            ("\u{a0}", r#""\u00a0""#, r#""\u{a0}""#),
            ("\u{3000}", r#""\u3000""#, r#""\u{3000}""#),
            ("\u{200b}", r#""\u200b""#, r#""\u{200b}""#),
            ("\u{301}", "\"\u{301}\"", r#""\u{301}""#),
        ] {
            assert_ne!(reference_text, ours, "{name:?} would not be a divergence");
            assert_eq!(
                reject_message(name),
                format!("normalization for label name {ours} resulted in invalid name \"_\""),
                "our rendering of {name:?}"
            );
        }
        // The reachable case, asserted at the byte level because that is
        // where it bites: the reference's response body carries U+0301 as its
        // raw two UTF-8 bytes inside the quotes, ours as eight ASCII ones.
        assert_eq!("\"\u{301}\"".as_bytes(), b"\"\xcc\x81\"");
        assert!(
            reject_message("\u{301}").is_ascii(),
            "ours escapes the mark instead of emitting it"
        );
        // …and a name built only from characters the two quoters agree on
        // renders identically, which is what keeps the difference to the
        // class named above rather than to any refused name.
        for name in ["µ", "日本", "🙂", "__é__", "\t", "\"", "\\", "-_-"] {
            assert!(validate_label_name(name).is_err(), "{name:?}");
        }
        assert_eq!(
            reject_message("\t"),
            r#"normalization for label name "\t" resulted in invalid name "_""#
        );
        assert_eq!(
            reject_message("🙂"),
            r#"normalization for label name "🙂" resulted in invalid name "_""#
        );
    }

    /// The OTLP seam's envelope, which the push seam does not have.
    #[test]
    fn only_the_otlp_seam_carries_the_symbolizer_lookup_prefix() {
        let otlp = match validate_otlp_attribute_names(["ok", ""]) {
            Err(LogsIngestError::InvalidLabelName(message)) => message,
            other => panic!("expected a rejection, got {other:?}"),
        };
        assert_eq!(otlp, "symbolizer lookup: label name is empty");

        let otlp_normalization = match validate_otlp_attribute_names([" "]) {
            Err(LogsIngestError::InvalidLabelName(message)) => message,
            other => panic!("expected a rejection, got {other:?}"),
        };
        assert_eq!(
            otlp_normalization,
            r#"symbolizer lookup: normalization for label name " " resulted in invalid name "_""#
        );

        // The push seam reports the same condition with no prefix at all…
        assert_eq!(reject_message(""), "label name is empty");
        // …and the prefix is added once, not per name.
        assert_eq!(otlp.matches("symbolizer lookup: ").count(), 1);
        assert!(validate_otlp_attribute_names(["ok", "a.b"]).is_ok());
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
    fn the_stored_name_matches_the_references_measured_renaming() {
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
            assert_eq!(log_label_name(input), expected, "log_label_name({input:?})");
        }
    }

    /// **The rejection rule and the stored name are one function** (issue
    /// #507). This module evaluates its second condition on
    /// `pulsus_model::log_label_name`'s result, and that same result is what
    /// an admitted name is stored under, so a name cannot be admitted under
    /// one rule and stored under another.
    #[test]
    fn the_rejection_rule_and_storage_name_alike() {
        for (name, stored) in [("a..b", "a_b"), ("a__b", "a_b"), ("9bad", "key_9bad")] {
            assert!(validate_label_name(name).is_ok(), "{name:?} is admitted");
            assert_eq!(
                log_label_name(name),
                stored,
                "{name:?} is stored as {stored:?}"
            );
        }
        // The metrics namer is a different rule and stays one: it escapes
        // each character on its own, so a run survives as a run.
        assert_eq!(canonicalize_label_key("a..b"), "a__b");
    }
}
