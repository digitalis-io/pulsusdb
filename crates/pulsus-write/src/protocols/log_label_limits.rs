//! The per-stream label rules Loki applies at ingest — the empty-value drop
//! that fixes stream identity, the internal-stream exemption, and the four
//! bounds — shared by both log receivers (issue #374).
//!
//! Reference: `pkg/distributor/validator.go:157-199 @ v3.7.4`
//! (`Validator.ValidateLabels`), reached from
//! `pkg/distributor/distributor.go:1370-1387 @ v3.7.4` (`parseStreamLabels`) —
//! before the label set is hashed, cached, forwarded or written. Loki runs it
//! for **every** log push transport: `PushHandler` (`/loki/api/v1/push`) and
//! `OTLPPushHandler` (`/otlp/v1/logs`) both funnel through
//! `Distributor.pushHandler` -> `PushWithResolver` -> `parseStreamLabels`
//! (`pkg/distributor/http.go:28-33 @ v3.7.4`), so PulsusDB applies it on the
//! Loki-push path *and* the OTLP logs path — though not on the same data
//! there: see [`validate_otlp_index_labels`], because only 18 named resource
//! attributes become stream labels upstream and the rest become structured
//! metadata, which this validator never sees.
//!
//! # The empty-value drop is part of the stream's identity, not of validation
//!
//! `parseStreamLabels` calls `syntax.ParseLabels`, which ends with
//! `ls.WithoutEmpty()` (`pkg/logql/syntax/parser.go:279-296 @ v3.7.4`, whose
//! comment says why: *"Empty label values are equivalent to absent labels in
//! Prometheus, but they unfortunately alter the Hash values created… Therefore
//! we must normalize early in the write path"*). The filtered set is what
//! `ValidateLabels` sees, what `labels.StableHash(ls)` hashes, and what is
//! written back as `stream.Labels = ls.String()`
//! (`distributor.go:1380-1386 @ v3.7.4`). Every transport lands there: the
//! Loki-push JSON and protobuf bodies arrive as a label literal, and the OTLP
//! translation renders one (`pkg/loghttp/push/otlp.go:244 @ v3.7.4`) that the
//! distributor then re-parses.
//!
//! [`StreamLabels`] is that point for us: it is the only way to obtain the
//! pairs, it drops empty values on construction, and the same value is both
//! validated and handed to `LabelSet::from_normalized`. So `{a="1",
//! ignored=""}` and `{a="1"}` are one stream with one fingerprint here as they
//! are upstream — a label set that validates cannot be a different label set
//! from the one that is stored.
//!
//! Empty-valued **structured metadata** is a different mechanism (a
//! `labels.Builder` reset in the per-entry loop,
//! `distributor.go:698-723 @ v3.7.4`) applied to different data, and is issue
//! #259; it is not addressed here.
//!
//! # Internal streams are exempt from all four bounds
//!
//! `validator.go:164-167 @ v3.7.4` returns early for a stream carrying
//! `__aggregated_metric__` or `__pattern__`
//! (`pkg/util/constants/internal_streams.go:4-7 @ v3.7.4`). PulsusDB never
//! *generates* such a stream — neither label name appears anywhere in this
//! repository outside this module — but both are ordinary, client-settable
//! label names on both sides (they pass Loki's `syntax.ParseLabels` and our
//! `is_valid_label_name`), so the exemption is reachable from a client push and
//! is therefore part of the accept surface, not an internal-producer
//! convenience. It is adopted for that reason. It skips only the four *parity*
//! bounds: the structural caps that bound decode-time materialization
//! (request body size, decoded-byte budget, per-stream raw label pairs) still
//! apply to an internal stream, as `maxStreamLabelsSize` still does upstream.
//!
//! # The four bounds, in Loki's order
//!
//! The order is observable: a stream that breaks several bounds reports only
//! the first (measured against the pinned `grafana/loki:3.7.4` container,
//! revision `b318f282`).
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
//! `service_name` is not counted: `ValidateLabels` decrements the label count
//! when it is present (`validator.go:169-174 @ v3.7.4`), because the
//! reference's own push parser injects one when a stream has none
//! (`pkg/loghttp/push/push.go:441-453 @ v3.7.4`, on by default). PulsusDB does
//! not inject it, but the decrement still applies to a *client-supplied*
//! `service_name`, so without it we would reject a 16-label stream the
//! reference accepts. Measured: 15 arbitrary labels + `service_name` -> `204`;
//! 16 arbitrary labels + `service_name` -> `400 … has 16 label names`. The
//! effective rule either way is "at most 15 labels other than `service_name`".
//!
//! That same unconditional injection is why `MissingLabelsErrorMsg` ("error at
//! least one label pair is required per stream", `validator.go:159-163`) is not
//! implemented here: with the reference's default `discover_service_name` a
//! non-internal stream always carries at least `service_name`, so the check is
//! unreachable upstream. A stream whose labels are *all* empty-valued is
//! stored by PulsusDB with no labels and by the reference as
//! `{service_name="unknown_service"}` — the pre-existing `service_name`
//! injection divergence, ledgered separately, not a new one.
//!
//! Duplicate names only reach check 4 on a transport that can carry them.
//! The reference's JSON push body and its OTLP translation both build the set
//! through a map (`pkg/loghttp/labels.go:26-40`, `otlp.go:193`), so a repeated
//! key collapses (measured: JSON `{"foo":"bar","foo":"barf"}` -> `204`); the
//! protobuf `StreamAdapter.labels` literal preserves them (measured:
//! `{foo="bar", foo="barf"}` -> `400 duplicate label name: 'foo'`). PulsusDB's
//! transports have exactly the same shape — the JSON `stream` object decodes
//! into a `BTreeMap` and OTLP attributes go through `LabelSet::from_normalized`
//! — so check 4 is reachable on the protobuf literal path and vacuous
//! elsewhere, matching.

use pulsus_model::SERVICE_NAME_LABEL;
use thiserror::Error;

/// One stream's breach of one of the four bounds. Carries the reference's
/// message verbatim (`pkg/validation/validate.go:58-69 @ v3.7.4`) with no
/// PulsusDB prefix, because that message is the `400` body a client reads.
///
/// Not a [`crate::error::LogsIngestError`] variant: a breach is **stream-local**
/// upstream — `PushWithResolver` discards the offending stream, writes the rest
/// and answers `400` afterwards (`pkg/distributor/distributor.go:645-655,
/// 780-790, 929 @ v3.7.4`) — so it never aborts a request the way a decode or
/// structural failure does. It is accumulated into
/// [`crate::ParsedLogs::stream_errors`] instead.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{0}")]
pub struct LabelLimitError(String);

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

/// `constants.AggregatedMetricLabel`
/// (`pkg/util/constants/internal_streams.go:5 @ v3.7.4`).
pub const AGGREGATED_METRIC_LABEL: &str = "__aggregated_metric__";

/// `constants.PatternLabel`
/// (`pkg/util/constants/internal_streams.go:7 @ v3.7.4`).
pub const PATTERN_LABEL: &str = "__pattern__";

/// One log stream's labels as the reference has them at
/// `parseStreamLabels`: empty values dropped and name-sorted, i.e. the output
/// of `syntax.ParseLabels` (`pkg/logql/syntax/parser.go:279-296 @ v3.7.4`).
///
/// The type exists so that the set which is validated is, by construction, the
/// set which is fingerprinted and stored — adopting `WithoutEmpty` for one and
/// not the other would make `{a="1", ignored=""}` accepted by both PulsusDB and
/// the reference but stored under different identities here.
///
/// Duplicate names are **kept**: the reference keeps them too (its promql
/// metric parser does), which is what makes bound 4 reachable on the protobuf
/// literal transport. Sorting is by name only and stable, so equal names stay
/// in wire order exactly as `labels.Labels` has them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamLabels(Vec<(String, String)>);

impl StreamLabels {
    /// Applies `WithoutEmpty` and the name sort. The only constructor.
    pub fn from_pairs<I>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let mut kept: Vec<(String, String)> = pairs
            .into_iter()
            .filter(|(_, value)| !value.is_empty())
            .collect();
        // `labels.Labels` is name-sorted at the point `ValidateLabels` runs,
        // which is what makes the reference's `lastLabelName` comparison a
        // complete duplicate test and what fixes *which* label a multi-breach
        // stream is reported against. `sort_by` is stable, so duplicates keep
        // their wire order.
        kept.sort_by(|a, b| a.0.cmp(&b.0));
        Self(kept)
    }

    /// The retained pairs, name-sorted.
    pub fn pairs(&self) -> &[(String, String)] {
        &self.0
    }

    /// Consumes into the pairs a `LabelSet` is built from — the same value
    /// [`Self::validate`] inspected.
    pub fn into_pairs(self) -> Vec<(String, String)> {
        self.0
    }

    /// True for a stream the reference exempts from all four bounds
    /// (`validator.go:157-167 @ v3.7.4`).
    fn is_internal(&self) -> bool {
        self.0
            .iter()
            .any(|(name, _)| name == AGGREGATED_METRIC_LABEL || name == PATTERN_LABEL)
    }

    /// Applies the four bounds in the reference's order.
    ///
    /// Call this only for a stream that carries at least one entry: the
    /// reference skips an entry-less stream before validating it
    /// (`pkg/distributor/distributor.go:639-641 @ v3.7.4`).
    ///
    /// Errors are [`LabelLimitError`] — the reference's message verbatim,
    /// which the receiver accumulates and answers `400` with.
    pub fn validate(&self) -> Result<(), LabelLimitError> {
        if self.is_internal() {
            return Ok(());
        }

        // Check 1 runs before the per-label loop, so a stream that is both too
        // wide and carries an over-long name reports the count
        // (`validator.go:176-181 @ v3.7.4`; measured on the container).
        let mut count = self.0.len();
        if self.0.iter().any(|(name, _)| name == SERVICE_NAME_LABEL) {
            count -= 1;
        }
        if count > MAX_LABEL_NAMES_PER_STREAM {
            return Err(LabelLimitError(format!(
                "entry for stream '{}' has {count} label names; limit {MAX_LABEL_NAMES_PER_STREAM}",
                self.render()
            )));
        }

        let mut last_name: Option<&str> = None;
        for (name, value) in &self.0 {
            if name.len() > MAX_LABEL_NAME_BYTES {
                return Err(LabelLimitError(format!(
                    "stream '{}' has label name too long: '{name}'",
                    self.render()
                )));
            } else if value.len() > MAX_LABEL_VALUE_BYTES {
                return Err(LabelLimitError(format!(
                    "stream '{}' has label value too long: '{value}'",
                    self.render()
                )));
            } else if last_name == Some(name.as_str()) {
                return Err(LabelLimitError(format!(
                    "stream '{}' has duplicate label name: '{name}'",
                    self.render()
                )));
            }
            last_name = Some(name);
        }
        Ok(())
    }

    /// Renders the set as the literal the reference interpolates into all four
    /// messages. That literal is `stream.Labels`, which `parseStreamLabels`
    /// has by then set to `ls.String()` — Prometheus'
    /// `labels.Labels.stringImpl` (`model/labels/labels_common.go:57-80`,
    /// vendored at `v3.7.4`): `{`, `", "`-separated, each name
    /// `strconv.Quote`d iff it is not a legacy-valid label name, `=`, each
    /// value always `strconv.Quote`d, `}`.
    fn render(&self) -> String {
        let mut out = String::from("{");
        for (i, (name, value)) in self.0.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            if is_legacy_label_name(name) {
                out.push_str(name);
            } else {
                push_go_quoted(&mut out, name);
            }
            out.push('=');
            push_go_quoted(&mut out, value);
        }
        out.push('}');
        out
    }
}

/// The resource attributes the reference turns into **stream labels** on its
/// OTLP logs endpoint, canonicalized into our label form.
///
/// `distributor.otlp.default_resource_attributes_as_index_labels`
/// (`pkg/loghttp/push/otlp_config.go:56-73 @ v3.7.4`), in the OTel spelling:
/// `service.name`, `service.namespace`, `service.instance.id`,
/// `deployment.environment`, `deployment.environment.name`, `cloud.region`,
/// `cloud.availability_zone`, `k8s.cluster.name`, `k8s.namespace.name`,
/// `k8s.pod.name`, `k8s.container.name`, `container.name`,
/// `k8s.replicaset.name`, `k8s.deployment.name`, `k8s.statefulset.name`,
/// `k8s.daemonset.name`, `k8s.cronjob.name`, `k8s.job.name`. Every *other*
/// resource attribute becomes structured metadata there
/// (`OTLPConfig.actionForAttribute`'s fallthrough), and structured metadata
/// never reaches `ValidateLabels`.
///
/// Sorted, so the lookup below is a binary search.
const OTLP_INDEX_LABELS: [&str; 18] = [
    "cloud_availability_zone",
    "cloud_region",
    "container_name",
    "deployment_environment",
    "deployment_environment_name",
    "k8s_cluster_name",
    "k8s_container_name",
    "k8s_cronjob_name",
    "k8s_daemonset_name",
    "k8s_deployment_name",
    "k8s_job_name",
    "k8s_namespace_name",
    "k8s_pod_name",
    "k8s_replicaset_name",
    "k8s_statefulset_name",
    "service_instance_id",
    "service_name",
    "service_namespace",
];

/// Validates an OTLP resource's labels, charging the four bounds on the subset
/// the reference indexes as stream labels ([`OTLP_INDEX_LABELS`]).
///
/// PulsusDB stores every resource attribute as a stream label (issue #109);
/// the reference stores only those 18 that way and routes the rest to
/// structured metadata, which `ValidateLabels` never sees. Charging the bounds
/// on our whole set would refuse ordinary OTLP payloads the reference accepts —
/// measured on `grafana/loki@sha256:87f0a067…`: a resource carrying `app` with
/// a 2049-byte value, or 16 arbitrary attributes, answers `204` there. Charging
/// them on the indexed subset reproduces its answer in both directions.
///
/// The `LabelSet` is sorted, key-unique and empty-free by construction (see
/// [`StreamLabels::from_pairs`], applied before it was built), so the filtered
/// view is exactly the subset of what was stored.
pub fn validate_otlp_index_labels(labels: &pulsus_model::LabelSet) -> Result<(), LabelLimitError> {
    StreamLabels::from_pairs(
        labels
            .iter()
            .filter(|(name, _)| OTLP_INDEX_LABELS.binary_search(name).is_ok())
            .map(|(name, value)| (name.to_string(), value.to_string())),
    )
    .validate()
}

/// `model.LegacyValidation.IsValidLabelName`
/// (`common/model/labels.go`, vendored at `v3.7.4`): `[a-zA-Z_][a-zA-Z0-9_]*`,
/// empty rejected. Identical to `loki_push::is_valid_label_name`, restated here
/// because this one governs *rendering*, not acceptance — the Loki-push
/// transports reject an invalid name outright, but an OTLP attribute key is
/// canonicalized rather than rejected and can still start with a digit.
fn is_legacy_label_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    matches!(first, b'A'..=b'Z' | b'a'..=b'z' | b'_')
        && bytes.all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

/// Appends `s` as Go's `strconv.Quote` would (`go/src/strconv/quote.go`,
/// `quoteWith`/`appendEscapedRune`), which is what Prometheus' `Labels.String`
/// uses for every label value.
///
/// Exact for every code point below `U+0080`, which is where all of Go's
/// symbolic escapes live: `\"` `\\` `\a` `\b` `\f` `\n` `\r` `\t` `\v`, and
/// `\xNN` for the remaining C0 controls and `DEL`. The input is always a Rust
/// `String`, so Go's invalid-UTF-8 (`\xNN` per bad byte) and invalid-rune
/// (`�`) branches are unreachable.
///
/// **Residual:** a code point at or above `U+0080` is emitted verbatim, where
/// Go emits `\uXXXX`/`\UXXXXXXXX` for the ones `strconv.IsPrint` rejects —
/// non-ASCII spaces and format/unassigned code points such as `U+00A0` or
/// `U+200B`. Reproducing that needs Go's 750-line `isPrint` range tables; the
/// difference is confined to the bytes of a `400` body for a stream that has
/// *already* breached a bound, and never changes what is accepted. Recorded in
/// docs/benchmarks/logs-differential-ledger.md.
fn push_go_quoted(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{b}' => out.push_str("\\v"),
            ch if (ch as u32) < 0x20 || ch as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> StreamLabels {
        StreamLabels::from_pairs(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
        )
    }

    fn check(pairs: &[(&str, &str)]) -> Result<(), LabelLimitError> {
        labels(pairs).validate()
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

    /// The drop is not a validation-only filter: the pairs handed on for
    /// fingerprinting and storage are the same ones that were validated, so a
    /// stream cannot be accepted under one identity and stored under another.
    #[test]
    fn an_empty_valued_label_is_absent_from_the_pairs_that_are_stored() {
        let set = labels(&[("a", "1"), ("ignored", ""), ("b", "2")]);
        assert_eq!(
            set.clone().into_pairs(),
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string())
            ]
        );
        assert_eq!(set, labels(&[("b", "2"), ("a", "1")]));
    }

    /// `validator.go:164-167 @ v3.7.4`: an internal stream returns before all
    /// four bounds, so a 100-label `__pattern__` stream is accepted.
    #[test]
    fn an_internal_stream_is_exempt_from_every_bound() {
        for internal in [AGGREGATED_METRIC_LABEL, PATTERN_LABEL] {
            let names: Vec<String> = (0..100).map(|i| format!("l{i}")).collect();
            let mut pairs: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "v")).collect();
            pairs.push((internal, "x"));
            assert!(check(&pairs).is_ok(), "{internal} count bound");

            let long_name = "z".repeat(MAX_LABEL_NAME_BYTES + 1);
            let long_value = "c".repeat(MAX_LABEL_VALUE_BYTES + 1);
            assert!(
                check(&[(internal, "x"), (long_name.as_str(), long_value.as_str())]).is_ok(),
                "{internal} length bounds"
            );

            assert!(
                check(&[(internal, "x"), ("dup", "1"), ("dup", "2")]).is_ok(),
                "{internal} duplicate bound"
            );
        }
    }

    /// The exemption keys off the labels that survive `WithoutEmpty`, exactly
    /// as `ls.Has` does upstream: an empty-valued `__pattern__` is not there
    /// to exempt anything.
    #[test]
    fn an_empty_valued_internal_label_does_not_exempt_the_stream() {
        let names: Vec<String> = (0..16).map(|i| format!("l{i}")).collect();
        let mut pairs: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "v")).collect();
        pairs.push((PATTERN_LABEL, ""));
        assert!(err(&pairs).ends_with("' has 16 label names; limit 15"));
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

    /// `WithoutEmpty` runs before the duplicate test, so a repeated name whose
    /// second copy is empty-valued is not a duplicate — upstream the parser
    /// keeps both and `WithoutEmpty` then drops one.
    #[test]
    fn a_repeat_whose_other_copy_is_empty_valued_is_not_a_duplicate() {
        assert!(check(&[("foo", "bar"), ("foo", "")]).is_ok());
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

    /// Check 1 precedes check 4 too: a stream that is both too wide and
    /// carries a duplicate name reports the count, because the count check is
    /// outside the per-label loop.
    #[test]
    fn count_breach_outranks_a_duplicate_breach() {
        let names: Vec<String> = (0..16).map(|i| format!("l{i}")).collect();
        let mut pairs: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "v")).collect();
        pairs.push(("l0", "again"));
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

    /// Checks 2 and 3 are inside the same loop as check 4 and are tested
    /// first, so a duplicate name sorted after an over-long value loses; a
    /// duplicate sorted before it wins.
    #[test]
    fn a_value_breach_outranks_a_later_duplicate_but_not_an_earlier_one() {
        let long_value = "c".repeat(MAX_LABEL_VALUE_BYTES + 1);
        assert!(
            err(&[("aaa", long_value.as_str()), ("zzz", "1"), ("zzz", "2")])
                .contains("has label value too long")
        );
        assert!(
            err(&[("zzz", long_value.as_str()), ("aaa", "1"), ("aaa", "2")])
                .contains("has duplicate label name: 'aaa'")
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

    /// Go's `strconv.Quote` symbolic escapes and `\xNN` fallback, which
    /// Prometheus' `Labels.String` applies to every value
    /// (`labels_common.go:57-80`).
    ///
    /// The expectation is the whole ASCII range as `strconv.Quote` renders it,
    /// transcribed from Go itself (go1.25.5):
    ///
    /// ```go
    /// for r := rune(0); r < 0x80; r++ { fmt.Print(strconv.Quote(string(r))[1:len(strconv.Quote(string(r)))-1]) }
    /// ```
    ///
    /// Quoting is per-rune and context-free, so quoting the concatenation is
    /// the concatenation of the quotings.
    #[test]
    fn rendered_values_escape_the_whole_ascii_range_exactly_as_go_does() {
        const GO_QUOTED_ASCII: &str = concat!(
            r"\x00\x01\x02\x03\x04\x05\x06\a\b\t\n\v\f\r\x0e\x0f",
            r"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f",
            " !\\\"#$%&'()*+,-./0123456789:;<=>?@",
            r"ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_",
            "`abcdefghijklmnopqrstuvwxyz{|}~",
            r"\x7f",
        );
        let ascii: String = (0u8..0x80).map(|b| b as char).collect();
        let mut out = String::new();
        push_go_quoted(&mut out, &ascii);
        assert_eq!(out, format!("\"{GO_QUOTED_ASCII}\""));
    }

    /// The `\x0b`/`\x0c` case the reference renders symbolically reaches a real
    /// bound message, not just the helper.
    #[test]
    fn a_control_byte_in_a_sibling_label_is_escaped_in_the_bound_message() {
        let over = "b".repeat(MAX_LABEL_VALUE_BYTES + 1);
        let message = err(&[("app", over.as_str()), ("ctl", "x\u{b}y\u{c}z\u{8}")]);
        assert!(message.contains(r#"ctl="x\vy\fz\b""#), "{message}");
    }

    /// A name that is not legacy-valid is `strconv.Quote`d by
    /// `Labels.stringImpl`. Unreachable from the Loki-push transports (which
    /// reject such a name outright) but reachable from an OTLP attribute key,
    /// whose canonicalization leaves a leading digit alone.
    #[test]
    fn a_non_legacy_label_name_is_quoted_in_the_rendered_stream() {
        let over = "b".repeat(MAX_LABEL_VALUE_BYTES + 1);
        let message = err(&[("9x", over.as_str())]);
        assert!(message.starts_with(r#"stream '{"9x"="#), "{message}");
    }

    /// Non-ASCII is passed through, which is Go's behaviour for the printable
    /// ones. Pins the documented residual: `U+00A0` would be ` ` there.
    #[test]
    fn non_ascii_values_are_passed_through() {
        let mut out = String::new();
        push_go_quoted(&mut out, "naïve→\u{a0}");
        assert_eq!(out, "\"naïve→\u{a0}\"");
    }
}
