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
//! there: see [`validate_otlp_index_labels`], because only 18 raw resource
//! attribute names become stream labels upstream and the rest become
//! structured metadata, which this validator never sees.
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
//! (`pkg/loghttp/push/push.go:441-456 @ v3.7.4`, on by default). PulsusDB
//! injects it too ([`super::service_name`], issue #379), and the decrement
//! applies equally to a *client-supplied* `service_name`, so a stream carrying
//! 15 real labels plus one is accepted on both sides. Measured: 15 arbitrary
//! labels + `service_name` -> `204`; 16 arbitrary labels + `service_name` ->
//! `400 … has 16 label names`. The effective rule either way is "at most 15
//! labels other than `service_name`", and because synthesis runs *before*
//! validation the label set rendered into all four messages is the one
//! carrying the synthesized label.
//!
//! # The empty-label check is first, and it is reachable
//!
//! `MissingLabelsErrorMsg` ("error at least one label pair is required per
//! stream", `pkg/validation/validate.go:25 @ v3.7.4`) is the first check in
//! [`StreamLabels::validate`], ahead of the internal-stream early return,
//! which is where `validator.go:158-167 @ v3.7.4` has it.
//!
//! An earlier revision of this comment claimed the check was "unreachable
//! upstream" and used that claim to justify not implementing it. The claim is
//! false, and it was load-bearing (issue #379). It holds for
//! `/loki/api/v1/push`: under the stock discovery list every non-internal push
//! stream gains a non-empty `service_name`, so the set is never empty by the
//! time it is validated. It fails for `/otlp/v1/logs`, where the discovery
//! algorithm is a different one: `container.name` is an index attribute AND a
//! discovery name, a `container.name=""` attribute therefore writes
//! `service_name=""` — the OTLP path has no non-empty guard — and sets
//! `hasServiceName`, which suppresses the `unknown_service` fallback
//! (`pkg/loghttp/push/otlp.go:193-220 @ v3.7.4`). Both labels then strip away
//! and nothing is left. Measured on stock `grafana/loki:3.7.4` (revision
//! `b318f282`): `400 error at least one label pair is required per stream`.
//! Pinned by `an_empty_stream_label_set_is_the_reference_message` here, by
//! `an_empty_index_attribute_value_empties_the_stream` in `otlp_logs.rs`, and
//! — in the direction that matters more — by
//! `a_resource_with_no_index_attributes_still_validates`, since charging this
//! check on a subset that does not include the synthesized slot would refuse
//! `{zzz="v"}`, which the reference accepts.
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
//!
//! # What these bounds do not cover: a stored label can exceed them
//!
//! On the OTLP logs path the bounds are charged on the eighteen resource
//! attributes the reference indexes as stream labels
//! ([`validate_otlp_index_labels`]), while PulsusDB stores **every** resource
//! attribute as a stream label (issue #109). So a label that is indexed here —
//! queryable, and part of the stream's fingerprint — can be wider than
//! [`MAX_LABEL_VALUE_BYTES`], carry a name longer than
//! [`MAX_LABEL_NAME_BYTES`], and take the stored label count past
//! [`MAX_LABEL_NAMES_PER_STREAM`]. Two paths reach it:
//!
//! - an attribute whose raw name is not one of the eighteen —
//!   `{app: "x"*2049}` stores a 2049-byte `app` label, and enough such
//!   attributes store a set wider than fifteen;
//! - an attribute whose raw name merely *canonicalizes onto* one of the
//!   eighteen — `{k8s.pod.name: "ok", k8s_pod_name: "x"*2049}`. Only
//!   `k8s.pod.name` is validated; both collapse onto `k8s_pod_name` in
//!   storage, and `from_normalized`'s frozen collision rule (issue #4) keeps
//!   the greatest *original* key, where `_` (0x5F) sorts after `.` (0x2E). The
//!   unvalidated near-miss therefore always wins, and the stored value of a
//!   validated label is one no bound was ever charged on.
//!
//! **`service_name` is the one name the second path no longer reaches**
//! (issue #379). That slot is resolved by
//! [`super::service_name::otlp_service_name`] from the raw attributes and
//! written last, exactly as the reference's `streamLabels[LabelServiceName] =
//! …` map assignment is, so `from_normalized` is never asked to decide it and
//! `{service.name: "ok", service_name: "x"*2049}` now stores `"ok"` — the
//! value the bound was charged on — on both the value and the fingerprint.
//! The seventeen other indexed names are unchanged.
//!
//! This is not a divergence note. Both examples are accepted by the reference
//! too — it routes those attributes to structured metadata, which is unbounded
//! there as well — so the accept surface this module exists to match still
//! agrees. It is recorded because it is inconsistent on PulsusDB's own terms:
//! this module introduces a bound, and a path exists that stores an indexed
//! label exceeding it, which a reader of the bound would not expect.
//!
//! Neither fix belongs to this module. Charging the bounds on every attribute
//! refuses ordinary OTLP payloads the reference accepts (measured — see
//! [`validate_otlp_index_labels`]); not indexing the other attributes changes
//! stored stream identities. The second is issue #109, which owns the
//! difference in *what* we index and therefore owns this gap.
//!
//! The Loki-push transports have no such gap: every pair in the pushed literal
//! is a stream label and every one of them is validated. Pinned by
//! `an_index_attribute_and_its_near_miss_collide_on_the_unvalidated_value` in
//! `otlp_logs.rs` (stored labels and fingerprint, for a name the slot does not
//! govern, alongside the `service_name` case it now does) and by
//! `an_otlp_near_miss_spelling_stores_an_over_wide_indexed_label` in
//! `crates/pulsus-server/tests/loki_push_live.rs` (read back out of
//! ClickHouse).

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
///
/// A stored OTLP stream can carry more labels than this — see the module doc's
/// *What these bounds do not cover*.
pub const MAX_LABEL_NAMES_PER_STREAM: usize = 15;

/// `validation.max-length-label-name` default
/// (`pkg/validation/limits.go:324 @ v3.7.4`), in bytes.
///
/// A stored OTLP stream label's name can be longer than this — see the module
/// doc's *What these bounds do not cover*.
pub const MAX_LABEL_NAME_BYTES: usize = 1024;

/// `validation.max-length-label-value` default
/// (`pkg/validation/limits.go:325 @ v3.7.4`), in bytes.
///
/// A stored OTLP stream label's value can be longer than this — see the module
/// doc's *What these bounds do not cover*.
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
///
/// The inner `Vec` is private and there is deliberately no `Default`, no
/// `From` and no second constructor, so "[`Self::from_pairs`] is the only way
/// to obtain a stream's pairs" is enforced by the compiler rather than by
/// convention. [`Self::set_service_name`] is the one mutator, and it exists
/// because the reference's own ordering forces it: `service_name` synthesis
/// sits *between* the strip and the bounds (`pkg/loghttp/push/push.go:425-456`
/// -> `pkg/distributor/distributor.go:1370-1387 @ v3.7.4`), so the set that is
/// validated is the set after synthesis. Doing it before `from_pairs` instead
/// would charge the bounds on a set the reference had already stripped, and
/// doing it after `validate` would render a different literal into the four
/// bound messages than the reference renders.
#[derive(Debug, Clone, PartialEq, Eq)]
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

    /// Inserts or replaces `service_name`, restoring the name sort — the
    /// reference's `lb.Set(LabelServiceName, serviceName).Labels()`
    /// (`pkg/loghttp/push/push.go:451-452 @ v3.7.4`) and, on the OTLP path,
    /// its `streamLabels[LabelServiceName] = …` map assignment
    /// (`pkg/loghttp/push/otlp.go:201,219 @ v3.7.4`). A plain overwrite in
    /// both cases: whatever the slot held loses.
    ///
    /// An EMPTY `value` is dropped rather than stored, because both reference
    /// paths render the set back into a label literal that
    /// `parseStreamLabels` re-parses through `WithoutEmpty`
    /// (`push.go:456`, `otlp.go:244` -> `distributor.go:1370 @ v3.7.4`). That
    /// is only reachable from OTLP, where the slot can legitimately be `""`.
    ///
    /// Any pre-existing `service_name` pairs are removed first, so the
    /// duplicate-name bound cannot be tripped by synthesis itself; on the push
    /// path this is a no-op, since a set already carrying the label is one the
    /// resolver declines to touch.
    pub fn set_service_name(&mut self, value: &str) {
        self.0.retain(|(name, _)| name != SERVICE_NAME_LABEL);
        if value.is_empty() {
            return;
        }
        // The pairs are name-sorted, so the insertion point is the first name
        // that is not less than `service_name` — an O(log n) probe rather than
        // a re-sort of the whole set.
        let at = self
            .0
            .partition_point(|(name, _)| name.as_str() < SERVICE_NAME_LABEL);
        self.0
            .insert(at, (SERVICE_NAME_LABEL.to_string(), value.to_string()));
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

    /// Applies the empty-set check and then the four bounds, in the
    /// reference's order.
    ///
    /// Call this only for a stream that carries at least one entry: the
    /// reference skips an entry-less stream before validating it
    /// (`pkg/distributor/distributor.go:639-641 @ v3.7.4`).
    ///
    /// Errors are [`LabelLimitError`] — the reference's message verbatim,
    /// which the receiver accumulates and answers `400` with.
    pub fn validate(&self) -> Result<(), LabelLimitError> {
        // `validator.go:158-162 @ v3.7.4` — ahead of the internal-stream early
        // return, which is where the reference has it. The ORDER is not
        // observable on either side (both predicates read the post-strip set,
        // and an empty set carries no internal label), so it is copied rather
        // than claimed: `an_internal_label_cannot_survive_into_the_empty_check`
        // asserts the mutual exclusivity instead. See the module doc for why
        // the check is reachable at all.
        if self.0.is_empty() {
            return Err(LabelLimitError(
                "error at least one label pair is required per stream".to_string(),
            ));
        }

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
/// OTLP logs endpoint, in their **raw OTel spelling** — the spelling the
/// selection is made on.
///
/// `distributor.otlp.default_resource_attributes_as_index_labels`
/// (`pkg/loghttp/push/otlp_config.go:56-73 @ v3.7.4`). Every *other* resource
/// attribute becomes structured metadata (`OTLPConfig.actionForAttribute`'s
/// fallthrough at `otlp_config.go:88-99`), and structured metadata never
/// reaches `ValidateLabels`.
///
/// **Raw, not canonicalized, and that ordering is observable.** `otlp.go:193 @
/// v3.7.4` calls `otlpConfig.ActionForResourceAttribute(k)` on the wire key `k`
/// and only *then* calls `attributeToLabels(k, …)`, which canonicalizes via
/// `otlptranslator.LabelNamer.Build` (`otlp.go:610-614 @ v3.7.4`); the match
/// inside `actionForAttribute` is `cfgAttr == attribute`, exact string equality
/// (`otlp_config.go:88-99 @ v3.7.4`). So an attribute whose raw name merely
/// *canonicalizes into* this list — `service_name`, `service-name`, anything of
/// the form `service?name` — is structured metadata upstream and is bounded by
/// nothing. Measured on `grafana/loki@sha256:87f0a067…`: a resource carrying
/// `{service_name: "x"*2049}` answers `204`, where `{service.name: "x"*2049}`
/// answers `400`.
///
/// Sorted, so the lookup below is a binary search (pinned by
/// `the_index_attribute_list_is_sorted_for_binary_search`).
const OTLP_INDEX_ATTRIBUTES: [&str; 18] = [
    "cloud.availability_zone",
    "cloud.region",
    "container.name",
    "deployment.environment",
    "deployment.environment.name",
    "k8s.cluster.name",
    "k8s.container.name",
    "k8s.cronjob.name",
    "k8s.daemonset.name",
    "k8s.deployment.name",
    "k8s.job.name",
    "k8s.namespace.name",
    "k8s.pod.name",
    "k8s.replicaset.name",
    "k8s.statefulset.name",
    "service.instance.id",
    "service.name",
    "service.namespace",
];

/// True for a raw OTel resource attribute key the reference promotes to a
/// stream label ([`OTLP_INDEX_ATTRIBUTES`]) — a binary search into the sorted
/// table. The selection is on the RAW key, before canonicalization, exactly as
/// `otlpConfig.ActionForResourceAttribute(k)` is
/// (`pkg/loghttp/push/otlp.go:180-193 @ v3.7.4`).
///
/// Exposed because [`super::service_name::otlp_service_name`] scans the same
/// subset: the reference's discovery loop is nested inside the `IndexLabel`
/// branch (`otlp.go:192-207 @ v3.7.4`), so an attribute that is not indexed
/// cannot be discovered. One list, two readers.
pub fn is_otlp_index_attribute(key: &str) -> bool {
    OTLP_INDEX_ATTRIBUTES.binary_search(&key).is_ok()
}

/// The eighteen raw names themselves, for tests that must enumerate them
/// rather than test membership.
pub fn otlp_index_attributes() -> &'static [&'static str] {
    &OTLP_INDEX_ATTRIBUTES
}

/// Validates an OTLP resource's labels, charging the four bounds on the subset
/// the reference indexes as stream labels — selected from the **raw** attribute
/// names ([`OTLP_INDEX_ATTRIBUTES`]) and canonicalized afterwards, which is the
/// order `otlp.go:180-212 @ v3.7.4` does it in — plus the resolved
/// `service_name` slot, which is part of that same map upstream.
///
/// PulsusDB stores every resource attribute as a stream label (issue #109);
/// the reference stores only those 18 that way and routes the rest to
/// structured metadata, which `ValidateLabels` never sees. Charging the bounds
/// on our whole set would refuse ordinary OTLP payloads the reference accepts —
/// measured on `grafana/loki@sha256:87f0a067…`: a resource carrying `app` with
/// a 2049-byte value, or 16 arbitrary attributes, answers `204` there. Charging
/// them on the indexed subset reproduces its answer in both directions.
///
/// `LabelSet::from_normalized` performs the canonicalize-then-collapse that
/// upstream's `attributeToLabels` + `streamLabels[name] = value` map assignment
/// performs, and it is the *same* call that decides what is stored, so within
/// the filtered subset the value a bound is charged on is the value that would
/// be written. The resulting set is then run through [`StreamLabels`] for
/// `WithoutEmpty` and the name sort, exactly as `parseStreamLabels` re-parses
/// the rendered literal upstream.
///
/// **Only within the subset.** Storage collapses the *whole* attribute list
/// (issue #109), so a non-indexed attribute canonicalizing onto an indexed name
/// can win that collapse and be stored under a name this function validated a
/// different value for — `{k8s.pod.name: "ok", k8s_pod_name: "x"*2049}` stores
/// 2049 bytes under `k8s_pod_name`. See the module doc's *What these bounds do
/// not cover*; the fix belongs to issue #109. `service_name` is the exception,
/// since `service_name` is the parameter below rather than a collapse winner.
///
/// Two raw index attributes that collide on one canonical name (only reachable
/// from a repeated wire key, since the 18 raw names canonicalize to 18 distinct
/// labels) resolve by `from_normalized`'s frozen rule (issue #4) rather than by
/// upstream's last-write-wins; that is the pre-existing collision divergence,
/// not a bound of its own — see docs/benchmarks/logs-differential-ledger.md.
///
/// `service_name` is [`super::service_name::otlp_service_name`]'s answer for
/// this resource, applied last. The reference validates the POST-synthesis
/// `streamLabels` map (`otlp.go:174-244` -> `distributor.go:1370 @ v3.7.4`),
/// which is why it is a parameter and not something this function could
/// derive: a resource with no index attributes at all still validates a
/// one-label set here, `{service_name="unknown_service"}`, and would otherwise
/// be refused as empty where the reference answers `204`.
pub fn validate_otlp_index_labels<I>(
    raw_attributes: I,
    service_name: &str,
) -> Result<(), LabelLimitError>
where
    I: IntoIterator<Item = (String, String)>,
{
    let indexed: Vec<(String, String)> = raw_attributes
        .into_iter()
        .filter(|(key, _)| is_otlp_index_attribute(key))
        .collect();
    let (labels, _collisions) = pulsus_model::LabelSet::from_normalized(indexed);
    let mut stream_labels = StreamLabels::from_pairs(
        labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string())),
    );
    stream_labels.set_service_name(service_name);
    stream_labels.validate()
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

    /// The one mutator (issue #379): insert, replace, and drop-when-empty,
    /// each preserving the name sort the duplicate bound depends on.
    #[test]
    fn set_service_name_inserts_replaces_and_drops_in_sort_order() {
        let mut set = labels(&[("zzz", "1"), ("app", "2")]);
        set.set_service_name("checkout");
        assert_eq!(
            set.pairs(),
            [
                ("app".to_string(), "2".to_string()),
                ("service_name".to_string(), "checkout".to_string()),
                ("zzz".to_string(), "1".to_string()),
            ],
            "inserted at its sorted position, between `app` and `zzz`"
        );

        // A plain overwrite, as the reference's map assignment is.
        set.set_service_name("other");
        assert_eq!(set.pairs()[1].1, "other");
        assert_eq!(set.pairs().len(), 3, "replaced, not appended");

        // An empty slot is dropped, because the reference re-parses the
        // rendered literal through `WithoutEmpty`.
        set.set_service_name("");
        assert_eq!(set.pairs().len(), 2);
        assert!(set.pairs().iter().all(|(n, _)| n != SERVICE_NAME_LABEL));

        // Setting on a set that is nothing else leaves a valid one-label
        // stream, which is what a bare OTLP resource stores.
        let mut only = labels(&[]);
        only.set_service_name(super::super::service_name::UNKNOWN_SERVICE);
        assert!(only.validate().is_ok());
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
    ///
    /// Asserted alongside a non-empty sibling because the strip is what makes
    /// the over-long name unreachable, and a set that strips to NOTHING is
    /// refused by the empty-set check instead (issue #379) — which would pass
    /// this test for the wrong reason.
    #[test]
    fn empty_valued_label_escapes_the_name_length_bound() {
        let name = "z".repeat(2000);
        assert!(check(&[(name.as_str(), ""), ("app", "v")]).is_ok());
        // The name bound is genuinely what is being escaped: the same name
        // with a value is refused.
        assert!(
            err(&[(name.as_str(), "v"), ("app", "v")]).contains("has label name too long"),
            "the bound must still fire for a non-empty value"
        );
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
    /// other copy is empty-valued is not a duplicate — upstream the parser
    /// keeps both and `WithoutEmpty` then drops one.
    ///
    /// **Both orders**, because the rule that applies here is the one that
    /// drops the empty *pair* (`ls.WithoutEmpty()`,
    /// `pkg/logql/syntax/parser.go:279-296 @ v3.7.4`), not the delete-by-name
    /// rule a `labels.Builder` applies to structured metadata
    /// (`distributor.go:698-722 @ v3.7.4`, issue #259). Delete-by-name would
    /// lose the surviving twin as well, so the two rules disagree on exactly
    /// this input and only the pair rule keeps the label. Measured on
    /// `grafana/loki@sha256:87f0a067…`: both orders are `204` and store the
    /// non-empty value, while `{d="one", d="two"}` is `400`.
    #[test]
    fn a_repeat_whose_other_copy_is_empty_valued_is_not_a_duplicate() {
        for pairs in [[("foo", "bar"), ("foo", "")], [("foo", ""), ("foo", "bar")]] {
            assert!(check(&pairs).is_ok(), "{pairs:?}");
            assert_eq!(
                labels(&pairs).into_pairs(),
                vec![("foo".to_string(), "bar".to_string())],
                "the surviving twin is kept, not deleted by name: {pairs:?}"
            );
        }
        assert!(err(&[("foo", "one"), ("foo", "two")]).contains("duplicate label name"));
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

    /// AC7 (issue #379). This inverts an earlier expectation named
    /// `an_empty_label_set_is_accepted`, which encoded the false claim that
    /// `MissingLabelsErrorMsg` could not be reached from any receiver.
    /// Measured on
    /// `grafana/loki@sha256:87f0a067…` (3.7.4, stock config): an OTLP resource
    /// carrying `container.name=""` answers exactly this body.
    #[test]
    fn an_empty_stream_label_set_is_the_reference_message() {
        assert_eq!(
            err(&[]),
            "error at least one label pair is required per stream"
        );
        // Reachable through the strip, not only from a literally empty set.
        assert_eq!(
            err(&[("only", ""), ("empty", "")]),
            "error at least one label pair is required per stream"
        );
    }

    /// The check is placed ahead of the internal-stream early return because
    /// `validator.go:158-167 @ v3.7.4` places it there, but **the ordering is
    /// not observable and this test does not claim it is**: both predicates
    /// read the set AFTER `WithoutEmpty`, and a set that is empty cannot carry
    /// an internal label, so the two branches are mutually exclusive on both
    /// sides. That mutual exclusivity is the assertable part, and it is what
    /// is asserted here — moving the check below the early return leaves every
    /// test in this file green, deliberately.
    ///
    /// What the check must not do is swallow the exemption, so the exemption
    /// is re-asserted beside it.
    #[test]
    fn an_internal_label_cannot_survive_into_the_empty_check() {
        for internal in [AGGREGATED_METRIC_LABEL, PATTERN_LABEL] {
            // Empty-valued: stripped away, so the set is empty AND not
            // internal — this input reaches the empty check either way.
            let stripped = labels(&[(internal, "")]);
            assert!(stripped.pairs().is_empty(), "{internal}");
            assert!(!stripped.is_internal(), "{internal}");
            assert_eq!(
                stripped.validate().unwrap_err().to_string(),
                "error at least one label pair is required per stream",
                "{internal}"
            );

            // Non-empty: internal, non-empty, and still exempt from all four
            // bounds.
            let mut pairs: Vec<(&str, &str)> = vec![(internal, "1")];
            let names: Vec<String> = (0..16).map(|i| format!("l{i}")).collect();
            pairs.extend(names.iter().map(|n| (n.as_str(), "v")));
            assert!(labels(&pairs).is_internal(), "{internal}");
            assert!(check(&pairs).is_ok(), "{internal}");
        }
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
    // -- the OTLP index-label subset --------------------------------------

    /// Mirrors the receiver: the slot is resolved from the same raw
    /// attributes the subset is selected from ([`super::otlp_logs::parse`]),
    /// so a case that passes here passes for the reason it passes at the wire.
    fn otlp(attrs: &[(&str, &str)]) -> Result<(), LabelLimitError> {
        let raw: Vec<(String, String)> = attrs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let slot = crate::protocols::service_name::otlp_service_name(&raw);
        validate_otlp_index_labels(raw, &slot)
    }

    /// `binary_search` is only a correct membership test on a sorted slice, so
    /// the list's order is load-bearing, not cosmetic: an out-of-order entry
    /// would silently stop being an index attribute.
    #[test]
    fn the_index_attribute_list_is_sorted_for_binary_search() {
        let mut sorted = OTLP_INDEX_ATTRIBUTES;
        sorted.sort_unstable();
        assert_eq!(OTLP_INDEX_ATTRIBUTES, sorted);
        for name in OTLP_INDEX_ATTRIBUTES {
            assert!(
                OTLP_INDEX_ATTRIBUTES.binary_search(&name).is_ok(),
                "{name} is not findable"
            );
        }
    }

    /// Enumerated from the reference's own list
    /// (`otlp_config.go:56-73 @ v3.7.4`), not from the cases we happened to
    /// think of: for **every** one of the 18, the raw dotted spelling is an
    /// index label and is bounded, and the same name spelled with the
    /// separators already canonicalized is *not* an index label and is bounded
    /// by nothing — because `ActionForResourceAttribute` matches the wire key
    /// by exact string equality before `attributeToLabels` canonicalizes
    /// (`otlp.go:193 @ v3.7.4`). Measured on the container for
    /// `service.name`/`service_name`; the rule is the same string comparison
    /// for the other 17.
    #[test]
    fn every_index_attribute_is_bounded_in_its_raw_spelling_only() {
        let over = "b".repeat(MAX_LABEL_VALUE_BYTES + 1);
        for raw in OTLP_INDEX_ATTRIBUTES {
            let message = otlp(&[(raw, over.as_str())])
                .expect_err("raw index attribute must be bounded")
                .to_string();
            assert!(message.contains("has label value too long"), "{raw}");

            let canonical = pulsus_model::canonicalize_label_key(raw);
            assert_ne!(canonical, raw, "{raw} must have a distinct canonical form");
            assert!(
                otlp(&[(canonical.as_str(), over.as_str())]).is_ok(),
                "{canonical} must not be an index label"
            );
        }
    }

    /// The same collision through the other separators a client can spell:
    /// `service-name`, `service name` and `service/name` all canonicalize to
    /// `service_name` here and are all structured metadata upstream.
    #[test]
    fn a_key_that_merely_canonicalizes_into_the_index_list_is_not_bounded() {
        let over = "b".repeat(MAX_LABEL_VALUE_BYTES + 1);
        for spelling in [
            "service_name",
            "service-name",
            "service name",
            "service/name",
        ] {
            assert_eq!(
                pulsus_model::canonicalize_label_key(spelling),
                SERVICE_NAME_LABEL
            );
            assert!(otlp(&[(spelling, over.as_str())]).is_ok(), "{spelling}");
        }
    }

    /// ...and it does not count towards the 15 either: 15 raw index attributes
    /// plus any number of near-miss spellings is accepted.
    #[test]
    fn near_miss_spellings_do_not_count_towards_the_label_bound() {
        let mut attrs: Vec<(&str, &str)> = OTLP_INDEX_ATTRIBUTES[..15]
            .iter()
            .map(|k| (*k, "v"))
            .collect();
        attrs.push(("service_name", "checkout"));
        attrs.push(("k8s_pod_name", "p"));
        attrs.push(("cloud-region", "r"));
        assert!(otlp(&attrs).is_ok());
    }

    /// `service.name` is the only one of the 18 whose canonical form is
    /// discounted from the count (`validator.go:169-174 @ v3.7.4`), so 15 raw
    /// index attributes plus it is 15 counted, and 16 without it is 16.
    #[test]
    fn the_count_bound_over_index_attributes_discounts_service_name() {
        let sixteen: Vec<(&str, &str)> = OTLP_INDEX_ATTRIBUTES[..16]
            .iter()
            .map(|k| (*k, "v"))
            .collect();
        assert!(sixteen.iter().all(|(k, _)| *k != "service.name"));
        assert!(
            otlp(&sixteen)
                .unwrap_err()
                .to_string()
                .contains("has 16 label names; limit 15")
        );

        let mut fifteen_plus_service: Vec<(&str, &str)> = OTLP_INDEX_ATTRIBUTES[..15]
            .iter()
            .map(|k| (*k, "v"))
            .collect();
        fifteen_plus_service.push(("service.name", "checkout"));
        assert!(otlp(&fifteen_plus_service).is_ok());
    }

    /// The 18 raw names canonicalize to 18 distinct labels, so the duplicate
    /// bound is unreachable on this transport from *distinct* keys — matching
    /// upstream, where `streamLabels` is a map. Only a repeated wire key can
    /// collide, and `from_normalized` collapses that before the bound is
    /// charged, exactly as the map assignment does.
    #[test]
    fn index_attributes_cannot_collide_into_a_duplicate() {
        let canonical: std::collections::BTreeSet<String> = OTLP_INDEX_ATTRIBUTES
            .iter()
            .map(|k| pulsus_model::canonicalize_label_key(k))
            .collect();
        assert_eq!(canonical.len(), OTLP_INDEX_ATTRIBUTES.len());
        assert!(otlp(&[("k8s.pod.name", "a"), ("k8s.pod.name", "b")]).is_ok());
    }

    /// An empty-valued index attribute is dropped before the bounds, and a
    /// non-indexed attribute never reaches them at all.
    #[test]
    fn the_index_subset_drops_empty_values_and_ignores_non_indexed_attributes() {
        let over = "b".repeat(MAX_LABEL_VALUE_BYTES + 1);
        assert!(otlp(&[("k8s.pod.name", "")]).is_ok());
        assert!(otlp(&[("app", over.as_str())]).is_ok());
        let long_key = "a".repeat(MAX_LABEL_NAME_BYTES + 1);
        assert!(otlp(&[(long_key.as_str(), "v")]).is_ok());
    }
}
