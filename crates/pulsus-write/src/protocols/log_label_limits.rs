//! The four per-stream label bounds Loki enforces at ingest, shared by both
//! log receivers (issue #374).
//!
//! Reference: `pkg/distributor/validator.go:157-199 @ v3.7.4`
//! (`Validator.ValidateLabels`), reached from
//! `pkg/distributor/distributor.go:1380 @ v3.7.4` (`parseStreamLabels`) —
//! before the label set is hashed, cached, forwarded or written. Loki runs it
//! for **every** log push transport: `PushHandler` (`/loki/api/v1/push`) and
//! `OTLPPushHandler` (`/otlp/v1/logs`) both funnel through
//! `Distributor.pushHandler` -> `PushWithResolver` -> `parseStreamLabels`
//! (`pkg/distributor/http.go:28-33 @ v3.7.4`), so PulsusDB applies it on the
//! Loki-push path *and* the OTLP logs path.
//!
//! The checks, in Loki's order (the order is observable: a stream that breaks
//! several bounds reports only the first, measured against the pinned
//! `grafana/loki:3.7.4` container, revision `b318f282`):
//!
//! | # | check | limit | message (`pkg/validation/validate.go:58-69 @ v3.7.4`) |
//! |---|---|---|---|
//! | 1 | label names per stream | 15 | `entry for stream '%s' has %d label names; limit %d` |
//! | 2 | label name length | 1024 B | `stream '%s' has label name too long: '%s'` |
//! | 3 | label value length | 2048 B | `stream '%s' has label value too long: '%s'` |
//! | 4 | duplicate label name | — | `stream '%s' has duplicate label name: '%s'` |
//!
//! Limits are the reference's flag defaults —
//! `validation.max-label-names-per-series` = 15,
//! `validation.max-length-label-name` = 1024,
//! `validation.max-length-label-value` = 2048
//! (`pkg/validation/limits.go:324-326 @ v3.7.4`). PulsusDB has no per-tenant
//! override surface, so the defaults are the values.
//!
//! Two rules are inherited from what the reference has already done to the
//! label set by the time `ValidateLabels` sees it, and both are load-bearing
//! for accept-surface parity:
//!
//! - **Empty values do not exist.** `syntax.ParseLabels` ends with
//!   `ls.WithoutEmpty()` (`pkg/logql/syntax/parser.go:279-296 @ v3.7.4`), and
//!   the OTLP translation sets labels through a `labels.Builder` whose `Set`
//!   deletes on `""`. So an empty-valued label is neither counted nor
//!   length-checked. Measured: `{"z"*2000: ""}` -> `204` on the container,
//!   while `{"z"*2000: "c"*3000}` -> `400 label name too long`.
//! - **`service_name` is not counted.** `ValidateLabels` decrements the label
//!   count when `service_name` is present
//!   (`pkg/distributor/validator.go:169-174 @ v3.7.4`), because the reference's
//!   own push parser injects one (`discover_service_name`). PulsusDB does not
//!   inject it — but the decrement still applies to a *client-supplied*
//!   `service_name`, so without it we would reject a 16-label stream the
//!   reference accepts. Measured: 15 arbitrary labels + `service_name` ->
//!   `204`; 16 arbitrary labels + `service_name` -> `400 has 16 label names`.
//!   The effective rule either way is "at most 15 labels other than
//!   `service_name`".
//!
//! Duplicate names only reach check 4 on a transport that can carry them.
//! The reference's JSON push body and its OTLP translation both build the set
//! through a map/builder, so a repeated key collapses (measured: JSON
//! `{"foo":"bar","foo":"barf"}` -> `204`); the protobuf `StreamAdapter.labels`
//! literal preserves them (measured: `{foo="bar", foo="barf"}` -> `400
//! duplicate label name: 'foo'`). PulsusDB's transports have exactly the same
//! shape — `BoundedLabelMap` is a `BTreeMap` and OTLP attributes go through
//! `LabelSet::from_normalized` — so check 4 is reachable on the protobuf
//! literal path and vacuous elsewhere, matching.

use pulsus_model::SERVICE_NAME_LABEL;

use crate::error::LogsIngestError;

/// `validation.max-label-names-per-series` default
/// (`pkg/validation/limits.go:326 @ v3.7.4`). Counted after empty-valued
/// labels are excluded and after the `service_name` decrement.
pub const MAX_LABEL_NAMES_PER_STREAM: usize = 15;

/// `validation.max-length-label-name` default
/// (`pkg/validation/limits.go:324 @ v3.7.4`), in bytes.
pub const MAX_LABEL_NAME_BYTES: usize = 1024;

/// `validation.max-length-label-value` default
/// (`pkg/validation/limits.go:325 @ v3.7.4`), in bytes.
pub const MAX_LABEL_VALUE_BYTES: usize = 2048;

/// Applies the four bounds to one stream's labels, in the reference's order.
///
/// `pairs` is the stream's labels as the transport produced them — **not**
/// deduplicated, so check 4 can see a repeat, and in any order (this function
/// sorts by name, as the reference's `labels.Labels` already is, so the label
/// a multi-breach stream is reported against is the same one).
///
/// Call this only for a stream that carries at least one entry: the reference
/// skips an entry-less stream before validating it
/// (`pkg/distributor/distributor.go:639-641 @ v3.7.4`).
///
/// Errors are [`LogsIngestError::LabelLimit`] — HTTP `400`, body = the
/// reference's message verbatim.
pub fn validate_stream_labels<'a, I>(pairs: I) -> Result<(), LogsIngestError>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    // Empty-valued labels are dropped, never counted or length-checked (see
    // the module docs: `WithoutEmpty` / `Builder::Set`). Borrowed throughout —
    // the accept path allocates one `Vec` of `(&str, &str)` per stream and
    // nothing else; the reject path clones only to render the message.
    let mut effective: Vec<(&str, &str)> = pairs
        .into_iter()
        .filter(|(_, value)| !value.is_empty())
        .collect();

    // Check 1 runs before the per-label loop, so a stream that is both too
    // wide and carries an over-long name reports the count
    // (`validator.go:178-181 @ v3.7.4`; measured on the container).
    let mut count = effective.len();
    if effective
        .iter()
        .any(|(name, _)| *name == SERVICE_NAME_LABEL)
    {
        count -= 1;
    }
    if count > MAX_LABEL_NAMES_PER_STREAM {
        return Err(LogsIngestError::LabelLimit(format!(
            "entry for stream '{}' has {count} label names; limit {MAX_LABEL_NAMES_PER_STREAM}",
            render_stream(&effective)
        )));
    }

    // `labels.Labels` is name-sorted, which is what makes the reference's
    // `lastLabelName` comparison a complete duplicate test and what fixes
    // *which* label a multi-breach stream is reported against.
    effective.sort_by(|a, b| a.0.cmp(b.0));

    let mut last_name: Option<&str> = None;
    for (name, value) in &effective {
        if name.len() > MAX_LABEL_NAME_BYTES {
            return Err(LogsIngestError::LabelLimit(format!(
                "stream '{}' has label name too long: '{name}'",
                render_stream(&effective)
            )));
        } else if value.len() > MAX_LABEL_VALUE_BYTES {
            return Err(LogsIngestError::LabelLimit(format!(
                "stream '{}' has label value too long: '{value}'",
                render_stream(&effective)
            )));
        } else if last_name == Some(name) {
            return Err(LogsIngestError::LabelLimit(format!(
                "stream '{}' has duplicate label name: '{name}'",
                render_stream(&effective)
            )));
        }
        last_name = Some(name);
    }
    Ok(())
}

/// Renders a label set as the Prometheus literal the reference interpolates
/// into every one of the four messages — `{a="b", c="d"}`, `", "`-separated,
/// values Go-`strconv.Quote`-escaped
/// (`prometheus/model/labels.Labels.String`). Only the five escapes
/// PulsusDB's own literal parser round-trips (`\\`, `\"`, `\n`, `\t`, `\r`)
/// are emitted; other control bytes are passed through rather than
/// `\xNN`-escaped, which is a rendering difference inside an error string and
/// never an accept/reject one.
fn render_stream(pairs: &[(&str, &str)]) -> String {
    let mut out = String::from("{");
    let mut sorted: Vec<&(&str, &str)> = pairs.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    for (i, (name, value)) in sorted.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(name);
        out.push_str("=\"");
        for ch in value.chars() {
            match ch {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                '\n' => out.push_str("\\n"),
                '\t' => out.push_str("\\t"),
                '\r' => out.push_str("\\r"),
                other => out.push(other),
            }
        }
        out.push('"');
    }
    out.push('}');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(pairs: &[(&str, &str)]) -> Result<(), LogsIngestError> {
        validate_stream_labels(pairs.iter().copied())
    }

    fn err(pairs: &[(&str, &str)]) -> String {
        check(pairs).unwrap_err().to_string()
    }

    #[test]
    fn fifteen_labels_are_accepted() {
        let names: Vec<String> = (0..15).map(|i| format!("l{i}")).collect();
        let pairs: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "v")).collect();
        assert!(check(&pairs).is_ok());
    }

    #[test]
    fn sixteen_labels_are_rejected_with_the_reference_message() {
        let names: Vec<String> = (0..16).map(|i| format!("l{i}")).collect();
        let pairs: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "v")).collect();
        let message = err(&pairs);
        assert!(
            message.ends_with("' has 16 label names; limit 15"),
            "{message}"
        );
        assert!(message.starts_with("entry for stream '{l0=\"v\", l1=\"v\", l10=\"v\""));
    }

    /// `validator.go:169-174 @ v3.7.4`. Measured on `grafana/loki:3.7.4`:
    /// 15 arbitrary labels plus `service_name` answers `204`.
    #[test]
    fn service_name_does_not_count_toward_the_label_limit() {
        let names: Vec<String> = (0..15).map(|i| format!("l{i}")).collect();
        let mut pairs: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "v")).collect();
        pairs.push(("service_name", "checkout"));
        assert!(check(&pairs).is_ok());
    }

    /// The decrement is worth exactly one label, not more: 16 arbitrary
    /// labels plus `service_name` is `400 ... has 16 label names` upstream.
    #[test]
    fn service_name_decrement_is_worth_exactly_one_label() {
        let names: Vec<String> = (0..16).map(|i| format!("l{i}")).collect();
        let mut pairs: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "v")).collect();
        pairs.push(("service_name", "checkout"));
        assert!(err(&pairs).ends_with("' has 16 label names; limit 15"));
    }

    /// `WithoutEmpty` (`parser.go:296 @ v3.7.4`): 15 real labels plus an
    /// empty-valued 16th answers `204` upstream.
    #[test]
    fn empty_valued_labels_are_not_counted() {
        let names: Vec<String> = (0..15).map(|i| format!("l{i}")).collect();
        let mut pairs: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "v")).collect();
        pairs.push(("extra", ""));
        assert!(check(&pairs).is_ok());
    }

    /// An empty-valued label is not length-checked either — measured:
    /// `{"z"*2000: ""}` answers `204` upstream.
    #[test]
    fn empty_valued_label_escapes_the_name_length_bound() {
        let name = "z".repeat(2000);
        assert!(check(&[(name.as_str(), "")]).is_ok());
    }

    #[test]
    fn label_name_at_1024_bytes_is_accepted_and_1025_is_rejected() {
        let at = "a".repeat(MAX_LABEL_NAME_BYTES);
        assert!(check(&[(at.as_str(), "v")]).is_ok());
        let over = "a".repeat(MAX_LABEL_NAME_BYTES + 1);
        let message = err(&[(over.as_str(), "v")]);
        assert_eq!(
            message,
            format!("stream '{{{over}=\"v\"}}' has label name too long: '{over}'")
        );
    }

    #[test]
    fn label_value_at_2048_bytes_is_accepted_and_2049_is_rejected() {
        let at = "b".repeat(MAX_LABEL_VALUE_BYTES);
        assert!(check(&[("app", at.as_str())]).is_ok());
        let over = "b".repeat(MAX_LABEL_VALUE_BYTES + 1);
        let message = err(&[("app", over.as_str())]);
        assert_eq!(
            message,
            format!("stream '{{app=\"{over}\"}}' has label value too long: '{over}'")
        );
    }

    #[test]
    fn duplicate_label_name_is_rejected_even_when_the_values_are_identical() {
        assert_eq!(
            err(&[("foo", "bar"), ("foo", "barf")]),
            "stream '{foo=\"bar\", foo=\"barf\"}' has duplicate label name: 'foo'"
        );
        assert_eq!(
            err(&[("foo", "bar"), ("foo", "bar")]),
            "stream '{foo=\"bar\", foo=\"bar\"}' has duplicate label name: 'foo'"
        );
    }

    #[test]
    fn duplicates_are_detected_regardless_of_input_order() {
        assert!(err(&[("foo", "1"), ("zzz", "2"), ("foo", "3")]).contains("duplicate label name"));
    }

    /// Check 1 precedes checks 2-4: a stream that is too wide *and* carries
    /// an over-long name/value reports the count. Measured on the container.
    #[test]
    fn count_breach_outranks_a_name_or_value_breach() {
        let names: Vec<String> = (0..16).map(|i| format!("l{i}")).collect();
        let long_name = "z".repeat(2000);
        let long_value = "c".repeat(3000);
        let mut pairs: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "v")).collect();
        pairs.push((long_name.as_str(), long_value.as_str()));
        assert!(err(&pairs).contains("has 17 label names; limit 15"));
    }

    /// Check 2 precedes check 3 *for the same label*: measured on the
    /// container, `{"z"*2000: "c"*3000}` reports the name.
    #[test]
    fn name_breach_outranks_a_value_breach_on_the_same_label() {
        let long_name = "z".repeat(2000);
        let long_value = "c".repeat(3000);
        assert!(
            err(&[(long_name.as_str(), long_value.as_str())]).contains("has label name too long")
        );
    }

    /// ...but the loop is name-sorted, so an earlier label's value breach
    /// wins over a later label's name breach. Measured: `{aaa="c"*3000,
    /// "z"*2000="v"}` reports `has label value too long`.
    #[test]
    fn sorted_iteration_order_decides_which_label_is_reported() {
        let long_name = "z".repeat(2000);
        let long_value = "c".repeat(3000);
        assert!(
            err(&[("aaa", long_value.as_str()), (long_name.as_str(), "v")])
                .contains("has label value too long")
        );
    }

    #[test]
    fn an_empty_label_set_is_accepted() {
        assert!(check(&[]).is_ok());
    }

    #[test]
    fn rendered_stream_escapes_quotes_and_backslashes() {
        let over = "b".repeat(MAX_LABEL_VALUE_BYTES + 1);
        let message = err(&[("app", over.as_str()), ("q", "a\"b\\c\nd")]);
        assert!(message.contains(r#"q="a\"b\\c\nd""#), "{message}");
    }
}
