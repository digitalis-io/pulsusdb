//! OTLP logs parser (issue #8 architect plan, docs/architecture.md §4): a
//! pure `bytes -> ExportLogsServiceRequest -> ParsedLogs` pipeline with no
//! I/O. **Resource** attributes flatten through the frozen canonical label
//! model (`pulsus_model::LabelSet::from_normalized` -> `stream_fingerprint`,
//! issue #4) as stream labels; the log record's `InstrumentationScope`
//! (name, version, and attributes) lands in per-entry **structured
//! metadata**, never stream labels (issue #109 — Loki 3.4.2 parity), so
//! scope leaves the stream fingerprint. Fingerprints and the `service`
//! column derive *only* via `pulsus-model`, never re-derived here.

use std::collections::HashSet;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ScopeLogs};
use prost::Message;
use pulsus_model::{
    Date, Fingerprint, LabelSet, SERVICE_NAME_LABEL, UnixNano, canonicalize_label_key,
    stream_fingerprint,
};

use crate::error::LogsIngestError;
use crate::protocols::label_name::validate_otlp_attribute_names;
use crate::protocols::log_label_limits;
use crate::protocols::log_level::{
    self, AttributeLookup, DETECTED_LEVEL, LevelDiscovery, MetadataView, OtlpView,
};
use crate::protocols::loki_push::render_structured_metadata;
use crate::protocols::service_name;

/// A `SeverityNumber` outside this range (including the `0`/unset default)
/// resolves to severity `0` (architect plan: `severity = severity_number`
/// if `1..=24` else `0`) — the valid `SeverityNumber` enum range per the
/// OTLP logs data model (`TRACE`=1 .. `FATAL4`=24).
const VALID_SEVERITY_RANGE: std::ops::RangeInclusive<i32> = 1..=24;

/// The log-ingest knobs the server threads into every log receiver, the
/// structural mirror of
/// [`MetricIngestSettings`](crate::protocols::otlp_metrics::MetricIngestSettings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogIngestSettings {
    /// `writer.discover_log_levels` / `PULSUS_DISCOVER_LOG_LEVELS` (issue
    /// #483): whether a `detected_level` structured-metadata pair is
    /// synthesized for every entry.
    pub discover_log_levels: bool,
}

impl Default for LogIngestSettings {
    /// The reference's own default: level discovery is ON.
    fn default() -> Self {
        LogIngestSettings {
            discover_log_levels: true,
        }
    }
}

impl LogIngestSettings {
    /// The knob in the form the rule takes it.
    pub fn discovery(&self) -> LevelDiscovery {
        if self.discover_log_levels {
            LevelDiscovery::On
        } else {
            LevelDiscovery::Off
        }
    }
}

/// One `log_samples` row (docs/schemas.md §3.1), produced by [`parse`].
#[derive(Debug, Clone, PartialEq)]
pub struct LogRow {
    pub service: String,
    pub fingerprint: Fingerprint,
    pub timestamp_ns: UnixNano,
    pub severity: i8,
    pub body: String,
    /// Per-entry structured metadata (issue #97), stored as a canonical
    /// sorted-key JSON String — the same representation as
    /// `log_streams.labels` (`LabelSet::to_canonical_json`; docs/schemas.md
    /// §1 rejects `Map(String,String)` for label-shaped data). Empty string
    /// = no structured metadata. On the OTLP path this carries the log
    /// record's `InstrumentationScope` — `scope_name`/`scope_version` (each
    /// empty-suppressed) plus scope attributes under sanitized keys (issue
    /// #109, Loki 3.4.2 parity); on the Loki-push path it carries the
    /// entry's `structuredMetadata` pairs. Both funnel through the identical
    /// [`structured_metadata_json`] seam, so the stored String is
    /// byte-identical in shape across transports.
    pub structured_metadata: String,
}

/// One `log_streams` row (docs/schemas.md §3.1) for a single
/// `(fingerprint, month)` pair this request's rows touch. A stream touched
/// in `N` distinct UTC months within one request yields `N` `StreamRow`s
/// (architect plan amendment: the monthly `log_streams`/`log_streams_idx`
/// partitions require one row per stream per month, not one per stream).
#[derive(Debug, Clone, PartialEq)]
pub struct StreamRow {
    /// `toStartOfMonth(timestamp_ns)` in UTC, derived from the same
    /// per-record `timestamp_ns` used for the `LogRow`s in this month —
    /// backfilled records therefore register their historical month, not
    /// `now_ns`'s month.
    pub month: Date,
    pub fingerprint: Fingerprint,
    pub service: String,
    pub labels: LabelSet,
    /// The `ReplacingMergeTree` version column — `now_ns` (handler-
    /// injected receive time), distinct from `month`'s record timestamp.
    pub updated_ns: i64,
}

/// The normalized output of [`parse`]: rows destined for `log_samples` and
/// `log_streams`, plus per-request accounting the writer surfaces either
/// as a metric (`collisions`) or as an OTLP partial-success response
/// (`rejected`, `rejected_message`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ParsedLogs {
    pub rows: Vec<LogRow>,
    pub streams: Vec<StreamRow>,
    /// Sum of every `(resource, scope)` label set's normalized-key
    /// collision count (`LabelSet::from_normalized`'s lossy-resolution
    /// counter) across the whole request — never swallowed, surfaced for
    /// the writer's collision metric.
    pub collisions: u64,
    /// Count of individual log *records* dropped during parsing (not
    /// requests — a malformed/truncated protobuf is a whole-request
    /// [`LogsIngestError`], never a `rejected` count).
    pub rejected: u64,
    /// The first rejection's error message, surfaced verbatim as the OTLP
    /// `partial_success.error_message`.
    pub rejected_message: Option<String>,
    /// Stream-local validation failures (issue #374): one message per stream
    /// whose labels breached a per-stream bound. Those streams are **not** in
    /// `rows`/`streams`; every other stream in the request is, and the receiver
    /// admits them and then answers `400` with these messages.
    ///
    /// That is the reference's shape, not a PulsusDB choice:
    /// `PushWithResolver` `continue`s past a stream whose labels fail
    /// validation, accumulates the error, writes the streams that passed and
    /// only then returns the `400`
    /// (`pkg/distributor/distributor.go:645-655, 780-790, 929 @ v3.7.4`). A
    /// client does not lose the good streams of a mixed batch, which matters
    /// because a well-behaved log shipper does not retry a `400`.
    ///
    /// Distinct from `rejected`/`rejected_message`, which are per-*record*
    /// drops reported through OTLP partial success with a `2xx`.
    pub stream_errors: Vec<String>,
}

/// Decodes a (decompressed) OTLP `/v1/logs` request body. The sole
/// decode boundary: a malformed/truncated protobuf is a whole-request,
/// atomic failure (architect plan) — never partially applied.
pub fn decode(body: &[u8]) -> Result<ExportLogsServiceRequest, LogsIngestError> {
    // Wire pre-scan (issue #115, track 5): reject an over-cap / over-deep
    // request by walking the raw protobuf bytes BEFORE `decode` materializes
    // the amplified structure (malformed bodies deferred to `decode` below).
    crate::protocols::otlp_prescan::prescan_logs(body)?;
    Ok(ExportLogsServiceRequest::decode(body)?)
}

/// Decodes a (decompressed) OTLP/JSON (proto3-JSON) `/v1/logs` request body —
/// the `Content-Type: application/json` sibling of [`decode`] (issue #76).
/// Feeds the exact same `Export*ServiceRequest` into the exact same [`parse`],
/// so protobuf and JSON of one logical payload yield byte-identical rows. The
/// canonical protojson mapping (hex trace/span IDs, camelCase, u64-as-string,
/// base64 `bytesValue`) is supplied by `opentelemetry-proto`'s `with-serde`
/// impls; a malformed body is the same whole-request atomic failure as a bad
/// protobuf, mapped to 400/code 3 via [`LogsIngestError::DecodeJson`].
pub fn decode_json(body: &[u8]) -> Result<ExportLogsServiceRequest, LogsIngestError> {
    // Issue #115 track 6b: bounded proto3-JSON building wrappers replace the
    // vendored derive's UNBOUNDED repeated-field decode, rejecting a DoS-shaped
    // body DURING deserialization at the SAME per-level / aggregate / depth
    // thresholds the protobuf wire pre-scan (`otlp_prescan`) enforces (mirrors
    // `otlp_traces::decode_json`, track 6a).
    crate::protocols::otlp_json::decode_logs(body)
}

/// Parses a decoded `ExportLogsServiceRequest` into normalized rows. Pure:
/// a function of `req` and `now_ns` only, no I/O, no clock reads — the
/// caller (the ingest handler) is the only clock/IO boundary, so `parse`
/// itself is trivially unit-testable and deterministic across calls with
/// identical arguments.
///
/// `Err` iff a body/attribute `AnyValue` tree nests deeper than
/// [`otlp_depth::MAX_ANYVALUE_DEPTH`](crate::protocols::otlp_depth::MAX_ANYVALUE_DEPTH)
/// — a whole-request, atomic structural failure (400 / `code = 3`), exactly
/// like a decode error — or iff the request carries no log records at all
/// ([`LogsIngestError::MissingStreams`], 422; issue #374). The depth check is
/// charged first, so a request that is both over-deep and record-less is the
/// `400`, not the `422` (see the body). Malformed
/// per-record timestamps stay per-record partial-success rejections inside
/// the `Ok`.
pub fn parse(
    req: &ExportLogsServiceRequest,
    now_ns: i64,
    settings: LogIngestSettings,
) -> Result<ParsedLogs, LogsIngestError> {
    // Whole-request `AnyValue` recursion-depth guard (finding #54): reject a
    // maliciously deep body/attribute tree before any value is rendered or a
    // row materialized, so the recursive `any_value_to_string` render below
    // can never overflow the stack. This makes `parse` fallible (it was
    // previously infallible) — a whole-request, atomic 400/`code = 3` reject,
    // the same class as a decode failure.
    crate::protocols::otlp_depth::ensure_logs_anyvalue_depth(req)?;

    // A request carrying no log records at all is `422`, not an empty success
    // (issue #374). Upstream's OTLP translation returns an EMPTY push request
    // for one — `if ld.LogRecordCount() == 0 { return &logproto.PushRequest{},
    // nil }`, the first statement of `otlpToLokiPushRequest`
    // (`pkg/loghttp/push/otlp.go:144-146 @ v3.7.4`) — and `PushWithResolver`
    // then refuses a stream-less push (`pkg/distributor/distributor.go:579-581
    // @ v3.7.4`). `LogRecordCount()` sums the records over every
    // `(ResourceLogs, ScopeLogs)` pair, so `{}`, `{"resourceLogs":[]}`, a
    // resource with no `scopeLogs`, and a scope with an empty `logRecords` are
    // one case, and all four measure `422` on
    // `grafana/loki@sha256:87f0a067…`. Charged here: after decode, before any
    // per-record work, and before the per-stream label bounds — but NOT before
    // the `AnyValue` depth cap. Both transports charge that cap inside decode
    // (`otlp_prescan::prescan_logs` for protobuf, `otlp_json::AnyValueSeed`
    // for JSON) and `ensure_logs_anyvalue_depth` above repeats it, so a
    // record-less body that ALSO nests an attribute past
    // `MAX_ANYVALUE_DEPTH` answers `400` here where the reference answers this
    // `422`. Measured on both transports, and present on the branch point
    // `5969a94` too — the depth cap is a PulsusDB-only bound the reference has
    // no equivalent of, so its ordering against this check is ours to state,
    // not the reference's to dictate. Ledger residual 6 (`ingest-label-bounds`).
    if req
        .resource_logs
        .iter()
        .all(|rl| rl.scope_logs.iter().all(|sl| sl.log_records.is_empty()))
    {
        return Err(LogsIngestError::MissingStreams);
    }

    let mut out = ParsedLogs::default();
    // Dedups stream registration within this request by `(fingerprint,
    // month)` (architect plan amendment) — a fingerprint-only key would
    // suppress a needed monthly row for a cross-month/backfilled request.
    let mut seen_streams: HashSet<(Fingerprint, Date)> = HashSet::new();

    for resource_logs in &req.resource_logs {
        // Stream labels are the resource's attributes ONLY (scope is
        // structured metadata, not a stream label; issue #109), so there is
        // exactly one label set per `ResourceLogs` — built once here rather
        // than re-derived for every scope, and never per record.
        let resource_attrs = resource_logs
            .resource
            .as_ref()
            .map(|resource| resource.attributes.as_slice())
            .unwrap_or(&[]);
        // `service_name` discovery (issue #379), resolved ONCE per resource
        // from the RAW attributes in wire order and then used both for the
        // validated subset and for the stored set, so the two cannot
        // disagree. This is the reference's OTLP algorithm, which is not its
        // push algorithm — see [`service_name`]'s module doc for the measured
        // table of inputs on which the two answer differently.
        let raw_attributes = attr_pairs(resource_attrs)?;
        let service_name = service_name::otlp_service_name(&raw_attributes);
        let (labels, collisions) = build_stream_labels(&raw_attributes, &service_name);
        // `WithoutEmpty` + the four per-stream label bounds (issue #374).
        // `WithoutEmpty` is applied inside `build_stream_labels` above, before
        // `from_normalized`, because the reference drops empty values before
        // hashing on this transport too — its OTLP translation renders a label
        // literal (`pkg/loghttp/push/otlp.go:244 @ v3.7.4`) which the
        // distributor re-parses through `syntax.ParseLabels`
        // (`distributor.go:1370 @ v3.7.4`).
        //
        // The four bounds, though, are charged on a SUBSET, selected from the
        // RAW attribute names: upstream splits a resource's attributes in two
        // (`otlp.go:180-212 @ v3.7.4`) by calling
        // `otlpConfig.ActionForResourceAttribute(k)` on the wire key `k` and
        // only then canonicalizing it with `attributeToLabels`
        // (`otlp.go:193,610-614 @ v3.7.4`). The 18 names in
        // `distributor.otlp.default_resource_attributes_as_index_labels`
        // (`pkg/loghttp/push/otlp_config.go:56-73 @ v3.7.4`) become stream
        // labels, and every other attribute becomes structured metadata, which
        // `ValidateLabels` never sees. PulsusDB indexes them all (issue #109),
        // so charging the bounds on our whole set would refuse ordinary OTLP
        // payloads the reference accepts: measured against
        // `grafana/loki@sha256:87f0a067…`, a resource carrying `app` with a
        // 2049-byte value, or 16 arbitrary attributes, answers `204` there and
        // stores the attribute as structured metadata. So would selecting on
        // the CANONICALIZED name: `{service_name: "x"*2049}` is `204` there,
        // because the raw key `service_name` is not one of the 18 — only
        // `service.name` is. Selecting on the raw names reproduces its answer
        // either way, so `attr_pairs` is re-walked here rather than the built
        // `LabelSet` being filtered.
        //
        // The remainder is not left unbounded upstream — it lands under the
        // structured-metadata limits at `ValidateEntry` (64 kB and 128 entries
        // per line, `pkg/validation/limits.go:60-61 @ v3.7.4`) — but those are
        // per-entry limits on data PulsusDB stores as stream labels, so mapping
        // them belongs with the #109 attribute-placement decision, not here.
        //
        // A breach drops this resource's streams and is reported as a `400`
        // after the rest of the request is written (`distributor.go:645-655,
        // 780-790, 929`), not as a whole-request `Err` and not as a per-record
        // partial-success drop. Skipped when the resource carries no records at
        // all: upstream skips an entry-less stream before validating it
        // (`pkg/distributor/distributor.go:639-641 @ v3.7.4`).
        let has_records = resource_logs
            .scope_logs
            .iter()
            .any(|scope_logs| !scope_logs.log_records.is_empty());
        if has_records
            && let Err(err) = log_label_limits::validate_otlp_index_labels(
                raw_attributes.iter().cloned(),
                &service_name,
            )
        {
            out.stream_errors.push(err.to_string());
            continue;
        }
        for scope_logs in &resource_logs.scope_logs {
            out.collisions += collisions as u64;
            let fingerprint = stream_fingerprint(&labels);
            let service = labels.service().to_string();
            // The scope's per-entry structured metadata, computed once per
            // ScopeLogs and cloned onto every record it contains.
            // Issue #483: with level discovery on the stored string is
            // per-RECORD, so a single shared string is no longer reusable.
            // The shape is a SPLICE — the sorted JSON is rendered once per
            // scope with a hole where the `detected_level` member belongs,
            // and each record fills the hole. Rebuilding the resolved pair
            // list and re-canonicalizing per record was the alternative;
            // `otlp_level_alloc.rs` is the measurement that chose between
            // them, not an assertion that one is faster.
            let scope_pairs = build_scope_metadata_pairs(scope_logs)?;
            let scope_metadata = match settings.discovery() {
                LevelDiscovery::Off => {
                    ScopeMetadata::Shared(render_structured_metadata(scope_pairs))
                }
                LevelDiscovery::On => ScopeMetadata::Spliced {
                    splice: LevelSplice::for_scope(&scope_pairs),
                    pairs: scope_pairs,
                },
            };

            for record in &scope_logs.log_records {
                // A log record's attribute KEYS are validated even though
                // issue #109's placement stores none of their values (only
                // resource attributes and the scope reach storage here): on
                // the reference every non-dropped record attribute goes
                // through the same `attributeToLabels` — and therefore the
                // same `LabelNamer.Build` — that its resource and scope
                // attributes do (`pkg/loghttp/push/otlp.go:488-499 @ v3.7.4`,
                // reaching `:603-614`), with the default log-attribute action
                // being `StructuredMetadata`
                // (`ActionForLogAttribute` -> `actionForAttribute`'s fallthrough,
                // `pkg/loghttp/push/otlp_config.go:90-116 @ v3.7.4`). Measured
                // on `grafana/loki:3.7.4`: a record attribute keyed `""` is
                // `400 symbolizer lookup: label name is empty` and one keyed
                // `" "` the normalization message, while PulsusDB answered
                // `200` before this check existed — an admissibility hole one
                // function away from the two seams that already had it.
                // Whole-request 400, the reference's own class here (its
                // `rangeErr` aborts `ParseOTLPRequest`), so it precedes the
                // per-record partial-success rejections below.
                validate_otlp_attribute_names(record.attributes.iter().map(|kv| kv.key.as_str()))?;
                let timestamp_ns = match resolve_timestamp_ns(record, now_ns) {
                    Ok(ts) => ts,
                    Err(message) => {
                        out.rejected += 1;
                        if out.rejected_message.is_none() {
                            out.rejected_message = Some(message);
                        }
                        continue;
                    }
                };

                // `log_samples` is partitioned by the RAW sample day
                // (`toDate(fromUnixTimestamp64Nano(timestamp_ns))`) and its
                // delete-TTL evaluates `intDiv(timestamp_ns, 1000000000)` in
                // the 32-bit `DateTime` domain (issue #137, mirroring #131's
                // trace fix), so a record is storage-safe only when its day
                // lies in `0..=49_709` (1970-01-01 to 2106-02-06): a day in
                // `49_710..=65_535` partitions correctly but exceeds
                // `u32::MAX` in the TTL seconds arithmetic, and a later day
                // falls outside the `Date` range entirely — even when its
                // month-start still fits (e.g. 2149-06-07 = day 65536 has
                // month-start 2149-06-01 = day 65530). Gate acceptance on
                // the DAY, then derive the month for the `log_streams`
                // registration (guaranteed `Some` once the day is in range,
                // but kept fallible — no `.unwrap()` on untrusted input).
                // Saturating either would orphan or silently early-expire
                // the sample, so the record is rejected into partial
                // success.
                let month = match (
                    Date::start_of_day_utc_datetime_safe(timestamp_ns),
                    Date::start_of_month_utc(timestamp_ns),
                ) {
                    (Some(_day), Some(month)) => month,
                    _ => {
                        out.rejected += 1;
                        if out.rejected_message.is_none() {
                            out.rejected_message = Some(format!(
                                "log record timestamp {timestamp_ns} is outside the \
                                 supported storage time range (1970-01-01 to 2106-02-06 UTC)"
                            ));
                        }
                        continue;
                    }
                };
                if seen_streams.insert((fingerprint, month)) {
                    out.streams.push(StreamRow {
                        month,
                        fingerprint,
                        service: service.clone(),
                        labels: labels.clone(),
                        updated_ns: now_ns,
                    });
                }

                let body = any_value_to_string(record.body.as_ref());
                let record_metadata = match &scope_metadata {
                    // The scope's structured metadata (issue #109), shared by
                    // every record in this ScopeLogs.
                    ScopeMetadata::Shared(shared) => shared.clone(),
                    ScopeMetadata::Spliced { pairs, splice } => {
                        let view = OtlpView {
                            scope_pairs: pairs,
                            severity_text: record.severity_text.as_str(),
                            record_attributes: RecordAttributes(&record.attributes),
                        };
                        // The RAW severity number, not the stored `i8`:
                        // `resolve_severity` collapses everything outside
                        // `1..=24` to `0`, which would merge "absent, read the
                        // line" (o1, o12) with "25 or 30, answer `unknown` and
                        // never read the line" (p1, p2).
                        let outcome =
                            log_level::resolve(&labels, &view, &body, Some(record.severity_number));
                        // `LeaveStored` carries no value: the reference
                        // rewrote a `detected_level` pair this entry does not
                        // store (a record attribute) and left the scope's own
                        // pair alone, so the stored value is the scope's,
                        // un-normalized (cases p10, p11).
                        let level = outcome
                            .value()
                            .or_else(|| view.stored_level())
                            .unwrap_or(log_level::UNKNOWN);
                        splice.render(level)
                    }
                };
                out.rows.push(LogRow {
                    service: service.clone(),
                    fingerprint,
                    timestamp_ns: UnixNano(timestamp_ns),
                    severity: resolve_severity(record.severity_number),
                    body,
                    structured_metadata: record_metadata,
                });
            }
        }
    }

    Ok(out)
}

/// Flattens `resource.attributes` — and ONLY those — into the stream
/// [`LabelSet`] via [`LabelSet::from_normalized`] (issue #109: scope name/
/// version/attributes are structured metadata, not stream labels — Loki
/// 3.4.2 parity). A collision between two resource attributes resolves by
/// `from_normalized`'s frozen deterministic rule (issue #4) and is counted,
/// never swapped. Because scope no longer enters this set, `stream_fingerprint`
/// is a pure function of the resource labels — a stream pushed with vs.
/// without scope fingerprints identically, exactly as Loki does.
///
/// An EMPTY-VALUED attribute is dropped (issue #259 — pair-wise, the
/// [`pulsus_model::retain_non_empty_values`] rule, applied inside
/// [`log_label_limits::StreamLabels::from_pairs`] so that issue #374's bounds
/// are charged on the survivors): the reference's OTLP path collects the promoted labels into a map
/// and renders it back into a label-set literal
/// (`pkg/loghttp/push/otlp.go:240-250 @ v3.7.4`) that the distributor
/// re-parses with `syntax.ParseLabels`/`WithoutEmpty`
/// (`pkg/logql/syntax/parser.go:279-296 @ v3.7.4`), so an empty-valued stream
/// label never reaches storage there either. This keeps the fingerprint a
/// function of the non-empty labels only — the hash-determinism reason the
/// reference gives inline. Measured on `grafana/loki:3.7.4` with
/// `categorize-labels` (so stream labels are read apart from structured
/// metadata) and an attribute the reference actually promotes — its default
/// only indexes an allow-list, `pkg/loghttp/push/otlp_config.go:56-74 @
/// v3.7.4`, whereas PulsusDB promotes every resource attribute (a
/// pre-existing, separate difference): a resource carrying
/// `deployment.environment=""` yields the same stream as one omitting it,
/// while `deployment.environment=" "` yields a distinct one.
///
/// The pair-wise primitive is the right one at a stream-label seam; the
/// structured-metadata seam below runs the reference's `labels.Builder`
/// ([`pulsus_model::resolve_structured_metadata`]) instead. For a LITERALLY
/// duplicated
/// attribute key the reference is order-dependent — it maps the promoted
/// attributes last-write-wins before stripping (`otlp.go:193`), measured as
/// `cloud.region=""` then `="eu"` -> `eu` kept, and `="eu"` then `=""` ->
/// dropped. PulsusDB resolves a duplicate key by `from_normalized`'s frozen
/// order-independent rule (issue #4) instead, which already diverges there and
/// is unchanged by this strip: pair-wise leaves the non-empty twin for
/// `from_normalized` to resolve exactly as it did before #259. By-name would
/// NOT have been neutral — it would drop both twins and change a case the
/// reference keeps.
fn build_stream_labels(
    raw_attributes: &[(String, String)],
    service_name: &str,
) -> (LabelSet, usize) {
    // `StreamLabels::from_pairs` applies `WithoutEmpty` (issue #374) BEFORE
    // `from_normalized`: an empty-valued resource attribute is neither
    // validated nor stored, so it cannot change the stream's fingerprint and
    // cannot win a normalized-key collision. The reference drops it in
    // `parseStreamLabels`, which this transport reaches too — its OTLP
    // translation renders a label literal that the distributor re-parses
    // through `syntax.ParseLabels` (`pkg/loghttp/push/otlp.go:244`,
    // `pkg/distributor/distributor.go:1370 @ v3.7.4`).
    //
    // `service_name` is the resolved slot (issue #379) and it is
    // AUTHORITATIVE: every raw attribute that canonicalizes onto that name is
    // dropped first, then the slot is appended. Upstream that name is written
    // by a plain map assignment (`otlp.go:193,201,219 @ v3.7.4`) which no
    // other attribute can reach — a raw `service_name` or `service-name`
    // attribute is not an index attribute there, so it becomes structured
    // metadata and never touches the stream label. `from_normalized`'s frozen
    // greatest-key/greatest-value rule (issue #4) is therefore never asked to
    // decide `service_name` on this path; it still decides every other
    // collision, unchanged.
    //
    // What this costs, stated plainly: PulsusDB stores a `service_name`
    // near-miss attribute nowhere, where the reference stores it as structured
    // metadata. That is the #109 attribute-placement difference showing
    // through, and it is ledgered under this issue's residual rather than
    // fixed here.
    let mut pairs: Vec<(String, String)> = raw_attributes
        .iter()
        .filter(|(key, _)| canonicalize_label_key(key) != SERVICE_NAME_LABEL)
        .cloned()
        .collect();
    pairs.push((SERVICE_NAME_LABEL.to_string(), service_name.to_string()));
    LabelSet::from_normalized(log_label_limits::StreamLabels::from_pairs(pairs).into_pairs())
}

/// Builds the per-entry structured-metadata JSON String carrying a log
/// record's `InstrumentationScope` (issue #109 — Loki 3.4.2 parity, live-
/// probe-pinned). Absent scope -> `""`.
///
/// Loki's placement rule is an ordered list `[scope attributes in wire order …,
/// scope_name (iff non-empty), scope_version (iff non-empty)]`, resolved to
/// unique sanitized keys by **last-write-wins per sanitized key**:
///
/// - **(a)** a scope attribute whose sanitized key collides with
///   `scope_name`/`scope_version` LOSES — identity is appended last, so it
///   overwrites the attribute regardless of the attribute's value or list
///   position.
/// - **(b)** two attributes sanitizing to the same key resolve to the LAST in
///   wire order (NOT by key/value — the property `LabelSet::from_normalized`'s
///   order-independent greatest-key/greatest-value rule cannot satisfy).
/// - **(c)** an empty-valued scope *attribute* is DROPPED, as is any other
///   empty-valued structured-metadata pair; scope *name*/*version* stay
///   empty-suppressed at their own append site (#108).
///
///   Rule (c) is the one that changed with the reference version, and it is
///   the reason this comment no longer says "KEPT" (issue #259). Loki 3.4.2's
///   distributor mutated an entry's structured metadata in place, with no
///   empty-value filter anywhere on the path
///   (`pkg/distributor/distributor.go:548-557 @ v3.4.2` — assignments to
///   `structuredMetadata[i].Name/.Value`, no builder); at the pinned v3.7.4
///   the same block routes through Prometheus' `labels.Builder`, which deletes
///   empty-valued base labels by name
///   (`pkg/distributor/distributor.go:698-722 @ v3.7.4`). Both halves are
///   measured, each on its own container, with the SAME OTLP body: a scope
///   named `N` version `1.0` carrying attributes `team=""` + `keep="1"`, plus
///   record attributes `sm_empty=""` + `sm_keep="2"`. `grafana/loki:3.4.2`
///   (`4fa045d3`) returns `keep`, `sm_keep`, **and** `team=""`, `sm_empty=""`;
///   `grafana/loki:3.7.4` (`b318f282`) returns only `keep` and `sm_keep`.
///
///   That version split is the whole content of the #259 reopen, and it is
///   ledgered: the logs e2e differential still scores against a 3.4.2 oracle,
///   so its shared corpus may carry no empty-valued attribute at all. See
///   docs/benchmarks/logs-differential-ledger.md
///   `empty-value-oracle-version-skew` for the measured three-store matrix
///   and the close condition. No rule on this path changed for it.
///
/// All three rules are now the shared canonicalization seam's own —
/// `pulsus_model::resolve_structured_metadata`, the reference's
/// `labels.Builder`, over keys this path has ALREADY renamed exactly as the
/// reference's `attributeToLabels` renames them (issue #381). This function
/// therefore only builds the ordered list and hands it over; it re-spells no
/// rule of its own.
///
/// That is a unification, not a carve-out. The reference's OTLP translation
/// runs `LabelNamer.Build` over every attribute key BEFORE the distributor
/// sees it (`pkg/loghttp/push/otlp.go:602-614 @ v3.7.4`, reached for scope
/// attributes from `:300-317`), so at the builder no OTLP pair is ever
/// renamed, `add` stays empty but for the U+FFFD rewrites, and the builder
/// degenerates to exactly the by-name delete + keep-last this function used to
/// spell inline. [`canonicalize_label_key`] is the same primitive
/// `LabelSet::from_normalized` uses, so the keys handed over are its fixed
/// points and the seam only sorts + JSON-encodes them (byte-identical to the
/// Loki-push representation). The surviving asymmetry — push hands the builder
/// RAW names, this path hands it renamed ones — is the reference's own
/// asymmetry, at the same place.
///
/// The resolution half of that seam, split out
/// (issue #483) so the level detector can read the scope's RESOLVED pairs —
/// canonical names, no empty values — and so the per-record splice can be
/// planned from them once per `ScopeLogs` instead of per record. The
/// rendering half is
/// [`render_structured_metadata`](crate::protocols::loki_push::render_structured_metadata),
/// which the pre-issue-#483 caller reached through one combined function.
///
fn build_scope_metadata_pairs(
    scope_logs: &ScopeLogs,
) -> Result<Vec<(String, String)>, LogsIngestError> {
    let Some(scope) = scope_logs.scope.as_ref() else {
        return Ok(Vec::new());
    };
    // Ordered (sanitized_key, value): attributes in wire order, then identity
    // appended last so it overwrites any colliding attribute (rule (a)); each
    // identity field empty-suppressed (#108).
    let mut ordered: Vec<(String, String)> = attr_pairs(&scope.attributes)?
        .into_iter()
        .map(|(key, value)| (canonicalize_label_key(&key), value))
        .collect();
    if !scope.name.is_empty() {
        // `scope_name`/`scope_version` are already canonicalize fixed points.
        ordered.push(("scope_name".to_string(), scope.name.clone()));
    }
    if !scope.version.is_empty() {
        ordered.push(("scope_version".to_string(), scope.version.clone()));
    }
    // Rules (a), (b) and (c) are all the seam's builder now, over this ONE
    // ordered list — every attribute AND both identity fields. Two halves of
    // that scope are load-bearing and each is pinned by a measurement against
    // `grafana/loki:3.7.4` and by a test below:
    //
    // - The empty-value delete is by NAME and runs over the unresolved set, so
    //   two attributes sanitizing to one key with either empty (`a.b=""` +
    //   `a_b="v"`) lose the key entirely, in both wire orders
    //   (`empty_valued_scope_attribute_drops_its_sanitized_twin_in_either_order`).
    //   That is `Reset`'s seeding, not a rule of this function.
    // - The identity fields go through the builder too, because an
    //   empty-valued attribute NAMED `scope_name` takes the real `scope_name`
    //   with it: a body with scope name `N`, version `1.0` and attribute
    //   `scope_name=""` comes back carrying `scope_version="1.0"` and no
    //   `scope_name` at all (`an_empty_valued_attribute_named_after_an_identity_field_deletes_it_too`).
    //   Appending them AFTER the attributes is what keeps rule (a) true for
    //   non-empty values: nothing is deleted, and among pairs the builder
    //   treats alike the last wins.
    Ok(pulsus_model::resolve_structured_metadata(ordered))
}

/// How one `ScopeLogs`' per-record structured-metadata string is produced.
enum ScopeMetadata {
    /// Level discovery off: one shared string, cloned onto every record —
    /// the pre-issue-#483 behaviour, byte-identical.
    Shared(String),
    /// Level discovery on: the scope's resolved pairs (what the rule reads)
    /// beside the splice plan (what the rule's answer is written into).
    Spliced {
        pairs: Vec<(String, String)>,
        splice: LevelSplice,
    },
}

/// The canonical structured-metadata JSON for one scope, rendered once with
/// a hole where the `detected_level` member belongs.
///
/// The stored string is `LabelSet::to_canonical_json` over the scope's
/// resolved pairs plus one more pair, and that rendering is sorted by key
/// with `serde_json` string escaping. Because the key is a constant, its
/// position in the sorted order is a constant too, so everything either side
/// of it can be built once per scope. `otlp_level_alloc.rs` asserts the
/// spliced string equals `render_structured_metadata` over the same pair
/// list — an identity between two of our own functions, not evidence of
/// reference agreement.
struct LevelSplice {
    /// `{` plus every member sorted before `detected_level`, comma-terminated.
    prefix: String,
    /// Every member sorted after `detected_level`, comma-prefixed, plus `}`.
    suffix: String,
}

impl LevelSplice {
    fn for_scope(pairs: &[(String, String)]) -> LevelSplice {
        // A scope pair already named `detected_level` is REPLACED, not
        // duplicated (case p6), so it is left out of both sides of the hole.
        let mut sorted: Vec<&(String, String)> = pairs
            .iter()
            .filter(|(name, _)| name != DETECTED_LEVEL)
            .collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        // Members before the hole are comma-TERMINATED, members after it
        // comma-PREFIXED, so the hole always sits against a brace or between
        // two commas and the result is one well-formed object either way.
        let mut prefix = String::from("{");
        let mut suffix = String::new();
        for (name, value) in sorted {
            let before = name.as_str() < DETECTED_LEVEL;
            let target = if before { &mut prefix } else { &mut suffix };
            if !before {
                target.push(',');
            }
            push_json_string(target, name);
            target.push(':');
            push_json_string(target, value);
            if before {
                target.push(',');
            }
        }
        suffix.push('}');
        LevelSplice { prefix, suffix }
    }

    /// The stored string for one record, given the rule's answer.
    fn render(&self, level: &str) -> String {
        let mut out = String::with_capacity(
            self.prefix.len() + self.suffix.len() + DETECTED_LEVEL.len() + level.len() + 4,
        );
        out.push_str(&self.prefix);
        push_json_string(&mut out, DETECTED_LEVEL);
        out.push(':');
        push_json_string(&mut out, level);
        out.push_str(&self.suffix);
        out
    }
}

/// Appends `value` as a JSON string, byte-identical to
/// `serde_json::to_string(value)` — which is what
/// `LabelSet::to_canonical_json` uses, and what
/// `otlp_level_alloc.rs`'s identity assertion compares against.
///
/// The fast path is taken exactly when `serde_json` would emit the bytes
/// verbatim between quotes: no `"`, no `\` and no control byte. `serde_json`
/// escapes nothing else — not `/`, not DEL, not any non-ASCII scalar.
fn push_json_string(out: &mut String, value: &str) {
    if value.bytes().all(|b| b != b'"' && b != b'\\' && b >= 0x20) {
        out.push('"');
        out.push_str(value);
        out.push('"');
        return;
    }
    out.push_str(
        &serde_json::to_string(value)
            .expect("a &str is always encodable as a JSON string: encoding cannot fail"),
    );
}

/// An [`AttributeLookup`] over an OTLP record's own attributes, read lazily:
/// a value is rendered only for the name the rule actually asks about, and
/// the rule stops at its first hit, so a record carrying many attributes
/// does not pay to render them all.
struct RecordAttributes<'a>(&'a [KeyValue]);

impl AttributeLookup for RecordAttributes<'_> {
    fn get(&self, canonical_name: &str) -> Option<std::borrow::Cow<'_, str>> {
        self.0
            .iter()
            .find(|kv| canonical_key_eq(&kv.key, canonical_name))
            .map(|kv| std::borrow::Cow::Owned(any_value_to_string(kv.value.as_ref())))
    }
}

/// `canonicalize_label_key(raw) == canonical`, without building the
/// canonical form. The reference reaches record attributes through the same
/// key sanitizer every other attribute goes through
/// (`pkg/loghttp/push/otlp.go:488-499 @ v3.7.4`), so the comparison has to
/// be on the canonical name — but the rule asks about at most fifteen names
/// per record, and allocating a canonical key per attribute per name to
/// answer them would be the whole cost of the feature.
///
/// `canonical_key_eq_matches_canonicalize_label_key` pins the two against
/// each other.
fn canonical_key_eq(raw: &str, canonical: &str) -> bool {
    let mut want = canonical.chars();
    for c in raw.chars() {
        let mapped = if c.is_ascii_alphanumeric() || c == '_' {
            c
        } else {
            '_'
        };
        if want.next() != Some(mapped) {
            return false;
        }
    }
    want.next().is_none()
}

/// Renders a `KeyValue` list to `(key, value)` label pairs, using the same
/// `AnyValue -> String` rendering as a log record's body
/// ([`any_value_to_string`]) for the value side — label values are always
/// strings, so a non-string attribute (bool/int/double/array/kvlist/bytes)
/// renders the same way a non-string body would.
/// Every RAW attribute key is validated first
/// ([`validate_otlp_attribute_names`](crate::protocols::label_name::validate_otlp_attribute_names),
/// issue #259), so this is the structural mirror of the reference's
/// `attributeToLabels` — the one function on its OTLP path that runs
/// `LabelNamer.Build` over every resource, scope and record attribute key
/// alike (`pkg/loghttp/push/otlp.go:603-614 @ v3.7.4`). A rejected key is a
/// whole-request 400, which is exactly what the reference answers there
/// (measured: an empty resource, scope or record attribute key returns
/// `400 symbolizer lookup: label name is empty` on `grafana/loki:3.7.4`).
/// It is the OTLP-flavoured validator because the `symbolizer lookup: `
/// prefix is added at this very seam on the reference (`otlp.go:613`) and
/// nowhere on its push path — same rule, different bytes on the wire.
/// Validating HERE rather than at the two call sites keeps that one-function
/// correspondence, and puts the check ahead of both the pair-wise stream-label
/// strip and the by-name structured-metadata strip.
///
/// A log record's attributes are the one OTLP attribute kind that does NOT
/// reach this function — issue #109's placement discards their values, so no
/// pair list is built for them — and their keys are therefore validated by
/// the same rule directly in [`parse`], where that note carries the
/// measurement.
fn attr_pairs(attrs: &[KeyValue]) -> Result<Vec<(String, String)>, LogsIngestError> {
    // Borrowed pass first: a rejected key costs no clone.
    validate_otlp_attribute_names(attrs.iter().map(|kv| kv.key.as_str()))?;
    Ok(attrs
        .iter()
        .map(|kv| (kv.key.clone(), any_value_to_string(kv.value.as_ref())))
        .collect())
}

/// Resolves a log record's `timestamp_ns`: `time_unix_nano` if non-zero,
/// else `observed_time_unix_nano` if non-zero, else `now_ns` (architect
/// plan). A `0` field value means "unknown or missing" per the OTLP wire
/// format's own doc comment, not a literal Unix-epoch instant.
///
/// `Err` if the wire value's top bit is set (unrepresentable as
/// [`UnixNano`]'s `i64`): timestamps are stored verbatim, never
/// rounded/truncated (architect plan), so an unrepresentable value cannot
/// be silently clamped — it is a per-record rejection (partial success),
/// not a whole-request failure, since the rest of the request is still
/// well-formed protobuf.
fn resolve_timestamp_ns(record: &LogRecord, now_ns: i64) -> Result<i64, String> {
    let raw = if record.time_unix_nano != 0 {
        record.time_unix_nano
    } else if record.observed_time_unix_nano != 0 {
        record.observed_time_unix_nano
    } else {
        return Ok(now_ns);
    };
    i64::try_from(raw).map_err(|_| {
        format!("log record timestamp {raw} exceeds the representable i64 nanosecond range")
    })
}

/// `severity = severity_number` if it falls in the valid `SeverityNumber`
/// range (`1..=24`), else `0` (architect plan).
fn resolve_severity(severity_number: i32) -> i8 {
    if VALID_SEVERITY_RANGE.contains(&severity_number) {
        // Infallible: `severity_number` is checked to be in `1..=24`,
        // which fits in `i8` without truncation.
        severity_number as i8
    } else {
        0
    }
}

/// Renders an `AnyValue` (a log record's `body`, or an attribute's value)
/// to its stored string form (architect plan): a string value verbatim;
/// a scalar (bool/int/double) via `Display`; an array/kvlist via
/// `serde_json`; bytes as base64 (task-manager resolution: base64,
/// matching the OTLP/JSON convention). Absent (`None`) or an entirely
/// unspecified `AnyValue` (empty `value` oneof) both render as `""`.
fn any_value_to_string(value: Option<&AnyValue>) -> String {
    let Some(value) = value.and_then(|v| v.value.as_ref()) else {
        return String::new();
    };
    match value {
        Value::StringValue(s) => s.clone(),
        Value::BoolValue(b) => b.to_string(),
        Value::IntValue(i) => i.to_string(),
        Value::DoubleValue(d) => d.to_string(),
        Value::ArrayValue(_) | Value::KvlistValue(_) => {
            serde_json::to_string(&any_value_to_json(value)).expect(
                "a JSON value tree built only from strings/numbers/bools/arrays/objects \
                 cannot fail to serialize",
            )
        }
        Value::BytesValue(bytes) => base64_encode(bytes),
        // Profiling-signal-only reference (into `ProfilesDictionary`); the
        // OTLP spec directs non-profiling receivers to treat its presence
        // as a non-fatal issue and process the value as absent/empty.
        Value::StringValueStrindex(_) => String::new(),
    }
}

/// Recursively renders an `AnyValue`'s `value` oneof to a `serde_json`
/// tree, used for the array/kvlist branch of [`any_value_to_string`].
fn any_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::StringValue(s) => serde_json::Value::String(s.clone()),
        Value::BoolValue(b) => serde_json::Value::Bool(*b),
        Value::IntValue(i) => serde_json::Value::Number((*i).into()),
        Value::DoubleValue(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            // NaN/±Infinity have no JSON number representation; `null` is
            // the closest lossless-enough fallback for this rare case.
            .unwrap_or(serde_json::Value::Null),
        Value::ArrayValue(array) => serde_json::Value::Array(
            array
                .values
                .iter()
                .map(|v| {
                    v.value
                        .as_ref()
                        .map(any_value_to_json)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        Value::KvlistValue(kvlist) => {
            let mut map = serde_json::Map::with_capacity(kvlist.values.len());
            for entry in &kvlist.values {
                let rendered = entry
                    .value
                    .as_ref()
                    .and_then(|v| v.value.as_ref())
                    .map(any_value_to_json)
                    .unwrap_or(serde_json::Value::Null);
                map.insert(entry.key.clone(), rendered);
            }
            serde_json::Value::Object(map)
        }
        Value::BytesValue(bytes) => serde_json::Value::String(base64_encode(bytes)),
        Value::StringValueStrindex(_) => serde_json::Value::Null,
    }
}

/// Minimal RFC 4648 standard base64 encoder (with padding) for
/// `bytes`-typed OTLP attribute/body values (task-manager open-question
/// resolution: base64, matching the OTLP/JSON convention). Hand-rolled to
/// avoid a new dependency — same alphabet and rationale as
/// `pulsus_server::middleware::base64_encode`, duplicated here because
/// `pulsus-write` does not depend on `pulsus-server`.
fn base64_encode(input: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied();
        let b2 = chunk.get(2).copied();
        let n =
            (u32::from(b0) << 16) | (u32::from(b1.unwrap_or(0)) << 8) | u32::from(b2.unwrap_or(0));
        out.push(CHARS[((n >> 18) & 0x3F) as usize] as char);
        out.push(CHARS[((n >> 12) & 0x3F) as usize] as char);
        out.push(if b1.is_some() {
            CHARS[((n >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        out.push(if b2.is_some() {
            CHARS[(n & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, InstrumentationScope, KeyValueList};
    use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
    use opentelemetry_proto::tonic::resource::v1::Resource;

    /// `super::parse` with level discovery OFF. Every assertion in this
    /// module predates ingest-time level detection (issue #483) and pins a
    /// stored structured-metadata string that carries no `detected_level`
    /// pair; routing them all through one off-path helper keeps those
    /// expectations exactly what they were.
    ///
    /// The cost is that this module then exercises the OFF path only. The ON
    /// path is covered by
    /// `crates/pulsus-write/tests/detected_level_reference_cases.rs`, which
    /// drives `super::parse` with discovery on over the whole captured OTLP
    /// table, and by `crates/pulsus-write/tests/otlp_level_alloc.rs`.
    fn parse_off(
        req: &ExportLogsServiceRequest,
        now_ns: i64,
    ) -> Result<ParsedLogs, LogsIngestError> {
        super::parse(
            req,
            now_ns,
            LogIngestSettings {
                discover_log_levels: false,
            },
        )
    }

    /// The `AnyValue` depth guard (finding #54) made `super::parse` fallible.
    /// Every legacy assertion below constructs shallow, in-bounds requests, so
    /// this shim unwraps the whole-request result to keep those cases reading
    /// against `ParsedLogs` unchanged; the dedicated depth tests call
    /// `super::parse` directly to observe the `Err`.
    fn parse(req: &ExportLogsServiceRequest, now_ns: i64) -> ParsedLogs {
        parse_off(req, now_ns).expect("test request is within the AnyValue depth cap")
    }

    fn kv(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
            key_strindex: 0,
        }
    }

    /// The attribute-key check (issue #259) made `attr_pairs` fallible. Every
    /// caller of this shim uses admissible keys; the dedicated rejection tests
    /// call `super::parse` and observe the `Err`. The `service_name` slot is
    /// resolved exactly as [`super::parse`] resolves it (issue #379), so a
    /// label set built here is the one the receiver would store.
    fn stream_labels(attributes: Vec<KeyValue>) -> (LabelSet, usize) {
        let raw = attr_pairs(&attributes).expect("test resource attribute keys are admissible");
        let slot = service_name::otlp_service_name(&raw);
        build_stream_labels(&raw, &slot)
    }

    fn string_body(s: &str) -> Option<AnyValue> {
        Some(AnyValue {
            value: Some(Value::StringValue(s.to_string())),
        })
    }

    fn request(resource_logs: Vec<ResourceLogs>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest { resource_logs }
    }

    fn simple_scope_logs(records: Vec<LogRecord>) -> ScopeLogs {
        ScopeLogs {
            scope: Some(InstrumentationScope {
                name: "my-scope".to_string(),
                version: "1.0.0".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            log_records: records,
            schema_url: String::new(),
        }
    }

    /// An empty request used to parse to empty output and answer `200`;
    /// issue #374 made it the reference's `422`. The rule and every shape of
    /// it are pinned in
    /// [`parse_rejects_a_request_with_no_log_records`]; this case stays as
    /// the one that used to assert the opposite, so the change is visible
    /// where the old contract was written down.
    #[test]
    fn parse_of_empty_request_is_a_stream_less_request() {
        let err = parse_off(&request(vec![]), 1_000).expect_err("no log records");
        assert!(matches!(err, LogsIngestError::MissingStreams), "{err:?}");
    }

    #[test]
    fn parse_derives_service_column_from_resource_service_name() {
        let resource = Resource {
            attributes: vec![kv(
                "service.name",
                Value::StringValue("checkout".to_string()),
            )],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("hello"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: Some(resource),
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].service, "checkout");
        assert_eq!(out.streams.len(), 1);
        assert_eq!(out.streams[0].service, "checkout");
        assert_eq!(out.streams[0].labels.service(), "checkout");
    }

    /// A resource with no attributes at all stores
    /// `service_name="unknown_service"` and puts that value in the physical
    /// `service` column (issue #379). This test asserted the opposite until
    /// discovery was implemented: its previous name,
    /// `parse_service_is_empty_string_when_absent_not_unknown_service`,
    /// named the divergence. Measured on stock `grafana/loki@sha256:87f0a067…`
    /// via `/loki/api/v1/series`: an attribute-less resource stores
    /// `{service_name="unknown_service"}`.
    #[test]
    fn parse_service_is_unknown_service_when_absent() {
        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("no resource"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rows[0].service, "unknown_service");
        assert_eq!(out.streams[0].service, "unknown_service");
        assert_eq!(
            out.streams[0].labels.to_canonical_json(),
            r#"{"service_name":"unknown_service"}"#
        );
    }

    /// Helper: builds a single-record request from a `ScopeLogs` and reads
    /// back the resolved structured-metadata JSON on its one row.
    fn scope_sm(scope: Option<InstrumentationScope>) -> String {
        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("x"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope,
                    log_records: vec![record],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }]),
            0,
        );
        out.rows[0].structured_metadata.clone()
    }

    fn scope(name: &str, version: &str, attributes: Vec<KeyValue>) -> InstrumentationScope {
        InstrumentationScope {
            name: name.to_string(),
            version: version.to_string(),
            attributes,
            dropped_attributes_count: 0,
        }
    }

    /// Runs `super::parse` over one record carrying the given resource and
    /// scope attributes, returning the whole-request rejection message.
    fn attribute_reject_message(
        resource_attrs: Vec<KeyValue>,
        scope_attrs: Vec<KeyValue>,
    ) -> String {
        let request = request(vec![ResourceLogs {
            resource: Some(Resource {
                attributes: resource_attrs,
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope("N", "1.0", scope_attrs)),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: string_body("x"),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }]);
        match parse_off(&request, 0) {
            Err(LogsIngestError::InvalidLabelName(message)) => message,
            other => panic!("expected InvalidLabelName, got {other:?}"),
        }
    }

    /// Issue #259: the OTLP receiver validates every RAW attribute key —
    /// resource (stream labels) and scope (structured metadata) alike, since
    /// both flow through `attr_pairs`, the structural mirror of the
    /// reference's `attributeToLabels` (`pkg/loghttp/push/otlp.go:603-614 @
    /// v3.7.4`, the one function on its OTLP path that runs `LabelNamer.Build`
    /// over every attribute key).
    ///
    /// Measured on `grafana/loki:3.7.4`, OTLP/JSON to `/otlp/v1/logs`: an
    /// empty resource, scope OR record attribute key returns
    /// `400 symbolizer lookup: label name is empty`; a `" "` or `"_"` key
    /// returns the same prefix in front of the normalization message. Before
    /// this change every one of those bodies was a `200` here.
    ///
    /// The prefix is the transport's, not the rule's: the identical name as
    /// push structured metadata carries no prefix at all (see
    /// `label_name::only_the_otlp_seam_carries_the_symbolizer_lookup_prefix`).
    #[test]
    fn an_inadmissible_otlp_attribute_key_rejects_the_whole_request() {
        let empty = kv("", Value::StringValue("v".to_string()));
        assert_eq!(
            attribute_reject_message(vec![empty.clone()], vec![]),
            "symbolizer lookup: label name is empty"
        );
        assert_eq!(
            attribute_reject_message(vec![], vec![empty]),
            "symbolizer lookup: label name is empty"
        );
        // An empty VALUE does not rescue an inadmissible name: the key check
        // runs ahead of both strips.
        assert_eq!(
            attribute_reject_message(vec![], vec![kv("", Value::StringValue(String::new()))]),
            "symbolizer lookup: label name is empty"
        );
        assert_eq!(
            attribute_reject_message(vec![kv(" ", Value::StringValue("v".to_string()))], vec![]),
            r#"symbolizer lookup: normalization for label name " " resulted in invalid name "_""#
        );
        assert_eq!(
            attribute_reject_message(vec![], vec![kv("_", Value::StringValue("v".to_string()))]),
            r#"symbolizer lookup: normalization for label name "_" resulted in invalid name "_""#
        );
        // A non-ASCII key the reference ACCEPTS stays accepted here, and the
        // two it refuses are refused with the same sentence (issue #259
        // re-review: `naïve` vs `µ` / `日本`, all three measured on the
        // container).
        assert_eq!(
            attribute_reject_message(vec![kv("µ", Value::StringValue("v".to_string()))], vec![]),
            r#"symbolizer lookup: normalization for label name "µ" resulted in invalid name "_""#
        );
        assert_eq!(
            attribute_reject_message(
                vec![],
                vec![kv("日本", Value::StringValue("v".to_string()))]
            ),
            r#"symbolizer lookup: normalization for label name "日本" resulted in invalid name "_""#
        );
    }

    /// Runs `super::parse` over one record carrying the given RECORD
    /// attributes (a resource and scope that are both admissible), returning
    /// the whole-request rejection message.
    fn record_attribute_reject_message(record_attrs: Vec<KeyValue>) -> String {
        match parse_off(&record_attribute_request(record_attrs), 0) {
            Err(LogsIngestError::InvalidLabelName(message)) => message,
            other => panic!("expected InvalidLabelName, got {other:?}"),
        }
    }

    fn record_attribute_request(record_attrs: Vec<KeyValue>) -> ExportLogsServiceRequest {
        request(vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", Value::StringValue("svc".to_string()))],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope("N", "1.0", vec![])),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: string_body("x"),
                    attributes: record_attrs,
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }])
    }

    /// Issue #259 re-review: a log RECORD's attribute keys are validated too,
    /// even though issue #109's placement stores none of their values. On the
    /// reference every non-dropped record attribute reaches the same
    /// `attributeToLabels`/`LabelNamer.Build` its resource and scope
    /// attributes do (`pkg/loghttp/push/otlp.go:488-499 -> :603-614 @
    /// v3.7.4`), the default action for a log attribute being
    /// `StructuredMetadata` (`otlp_config.go:90-116 @ v3.7.4`).
    ///
    /// Measured on `grafana/loki:3.7.4`, `POST /otlp/v1/logs`: a record
    /// attribute keyed `""` answers `400` with body
    /// `\x12&symbolizer lookup: label name is empty`, and one keyed `" "` the
    /// normalization message. PulsusDB answered `200` to both before this
    /// check — the values were discarded, so the key never reached a seam.
    #[test]
    fn an_inadmissible_otlp_record_attribute_key_rejects_the_whole_request() {
        assert_eq!(
            record_attribute_reject_message(vec![
                kv("ok", Value::StringValue("1".to_string())),
                kv("", Value::StringValue("v".to_string())),
            ]),
            "symbolizer lookup: label name is empty"
        );
        assert_eq!(
            record_attribute_reject_message(vec![kv(" ", Value::StringValue("v".to_string()))]),
            r#"symbolizer lookup: normalization for label name " " resulted in invalid name "_""#
        );
        // An empty VALUE does not rescue the name here either: the key check
        // is ahead of every strip, on this seam as on the other two.
        assert_eq!(
            record_attribute_reject_message(vec![kv("", Value::StringValue(String::new()))]),
            "symbolizer lookup: label name is empty"
        );
    }

    /// The accept half: an admissible record-attribute key is a 200, and its
    /// VALUE is still discarded — validating the key does not move issue
    /// #109's placement decision (record attributes are not stored).
    #[test]
    fn an_admissible_otlp_record_attribute_key_is_accepted_and_its_value_still_discarded() {
        let out = parse_off(
            &record_attribute_request(vec![
                kv("http.method", Value::StringValue("GET".to_string())),
                kv("naïve", Value::StringValue("yes".to_string())),
            ]),
            0,
        )
        .expect("admissible record attribute keys");
        assert_eq!(out.rows.len(), 1);
        assert_eq!(
            out.rows[0].structured_metadata, r#"{"scope_name":"N","scope_version":"1.0"}"#,
            "record attributes contribute nothing to storage (issue #109)"
        );
    }

    /// The accept half of the same seam: a dotted OTLP attribute key is what
    /// this receiver exists to carry, so it must stay a 200 on both sides.
    /// `naïve` rides along because the reference ADMITS it (measured — it
    /// keeps four ASCII letters), unlike `µ`/`日本`, which keep none.
    #[test]
    fn an_admissible_otlp_attribute_key_is_still_accepted_on_both_sides() {
        let request = request(vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("k8s.pod.name", Value::StringValue("pod-1".to_string()))],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope(
                    "N",
                    "",
                    vec![
                        kv("http.method", Value::StringValue("GET".to_string())),
                        kv("naïve", Value::StringValue("yes".to_string())),
                    ],
                )),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: string_body("x"),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }]);
        let out = parse_off(&request, 0).expect("admissible attribute keys");
        assert_eq!(
            out.rows[0].structured_metadata,
            r#"{"http_method":"GET","na_ve":"yes","scope_name":"N"}"#
        );
        // `k8s.pod.name` is an index attribute but is not one of the thirteen
        // discovery names, so the slot falls back (issue #379) — measured on
        // stock `grafana/loki@sha256:87f0a067…`: `{k8s_pod_name="p-379",
        // service_name="unknown_service"}`.
        assert_eq!(
            out.streams[0].labels.to_canonical_json(),
            r#"{"k8s_pod_name":"pod-1","service_name":"unknown_service"}"#
        );
    }

    #[test]
    fn parse_places_scope_identity_in_structured_metadata_not_stream_labels() {
        // AC-1 (issue #109): a non-empty scope yields per-entry structured
        // metadata keyed `scope_name`/`scope_version`, and the scope keys are
        // absent from the stream label set.
        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("x"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(
            out.rows[0].structured_metadata,
            r#"{"scope_name":"my-scope","scope_version":"1.0.0"}"#
        );
        // Scope is NOT a stream label (neither the new nor the old key names).
        assert_eq!(out.streams[0].labels.get("scope_name"), None);
        assert_eq!(out.streams[0].labels.get("scope_version"), None);
        assert_eq!(out.streams[0].labels.get("otel_scope_name"), None);
        assert_eq!(out.streams[0].labels.get("otel_scope_version"), None);
    }

    #[test]
    fn parse_places_scope_attributes_in_structured_metadata_under_sanitized_keys() {
        // Scope attributes -> SM under their sanitized attribute key
        // (`scope.attr.foo` -> `scope_attr_foo`), alongside identity.
        let sm = scope_sm(Some(scope(
            "my-scope",
            "1.0.0",
            vec![kv("scope.attr.foo", Value::StringValue("bar".to_string()))],
        )));
        assert_eq!(
            sm,
            r#"{"scope_attr_foo":"bar","scope_name":"my-scope","scope_version":"1.0.0"}"#
        );
    }

    #[test]
    fn parse_emits_no_scope_metadata_when_scope_is_present_but_empty() {
        // AC-2 (issue #109): the OTel Collector materializes a present-but-
        // empty `InstrumentationScope` (name/version `""`) on every re-export.
        // That must add NO structured metadata — matching Loki's per-field
        // empty-suppression (#108 parity, now in the SM surface).
        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("x"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv(
                        "service.name",
                        Value::StringValue("checkout".to_string()),
                    )],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(scope("", "", vec![])),
                    log_records: vec![record],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }]),
            0,
        );
        // Empty string (NOT "{}") keeps the read path on the zero-SM fast path.
        assert_eq!(out.rows[0].structured_metadata, "");
        // The real resource attribute remains a stream label.
        assert_eq!(out.streams[0].labels.get("service_name"), Some("checkout"));
    }

    #[test]
    fn parse_emits_only_the_non_empty_scope_identity_field() {
        // AC-2: a scope with a name but no version emits `scope_name` only —
        // the empty `scope_version` is suppressed independently.
        let sm = scope_sm(Some(scope("my-scope", "", vec![])));
        assert_eq!(sm, r#"{"scope_name":"my-scope"}"#);
    }

    #[test]
    fn parse_emits_no_scope_metadata_when_scope_is_absent() {
        assert_eq!(scope_sm(None), "");
    }

    // -- collision resolution (issue #109 v2, live-Loki-3.4.2-pinned) --------

    #[test]
    fn parse_scope_identity_wins_over_a_colliding_attribute_regardless_of_value_or_order() {
        // Rule (a): an attribute sanitizing onto a scope-identity key LOSES —
        // identity is appended last, so it wins irrespective of the
        // attribute's value or list position. Probed both dotted and literal,
        // with the attribute value lexically GREATER than the identity.
        let dotted = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![kv(
                "scope.name",
                Value::StringValue("ZZZ_greater".to_string()),
            )],
        )));
        assert_eq!(dotted, r#"{"scope_name":"N","scope_version":"1.0"}"#);
        assert!(!dotted.contains("ZZZ_greater"));

        let literal = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![kv(
                "scope_name",
                Value::StringValue("ZZZ_greater".to_string()),
            )],
        )));
        assert_eq!(literal, r#"{"scope_name":"N","scope_version":"1.0"}"#);

        let version_collision = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![kv("scope.version", Value::StringValue("9.9.9".to_string()))],
        )));
        assert_eq!(
            version_collision,
            r#"{"scope_name":"N","scope_version":"1.0"}"#
        );
    }

    #[test]
    fn parse_two_attributes_sanitizing_to_one_key_resolve_by_last_write_wins() {
        // Rule (b): two attributes sanitizing to the same key resolve to the
        // LAST in wire order — NOT key-based, NOT value-based. Order-flipping
        // flips the winner, the property `from_normalized`'s order-independent
        // rule CANNOT satisfy (the regression guard against reverting to it).
        let order1 = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![
                kv("a.b", Value::StringValue("Z_first".to_string())),
                kv("a_b", Value::StringValue("A_second".to_string())),
            ],
        )));
        assert_eq!(
            order1,
            r#"{"a_b":"A_second","scope_name":"N","scope_version":"1.0"}"#
        );

        let flipped = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![
                kv("a_b", Value::StringValue("A_first".to_string())),
                kv("a.b", Value::StringValue("Z_second".to_string())),
            ],
        )));
        assert_eq!(
            flipped,
            r#"{"a_b":"Z_second","scope_name":"N","scope_version":"1.0"}"#
        );
    }

    #[test]
    fn parse_drops_empty_valued_scope_attribute_and_suppresses_empty_identity() {
        // Rule (c) at the pinned v3.7.4 (issue #259): an empty-valued scope
        // ATTRIBUTE is dropped, exactly like an empty scope name/version. The
        // 3.4.2-era asymmetry (attribute kept, identity suppressed) is gone —
        // its distributor had no empty-value filter; v3.7.4's routes structured
        // metadata through Prometheus' `labels.Builder`, which deletes
        // empty-valued base labels by name.
        let dropped = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![kv("emptyattr", Value::StringValue(String::new()))],
        )));
        assert_eq!(dropped, r#"{"scope_name":"N","scope_version":"1.0"}"#);

        let empty_version = scope_sm(Some(scope(
            "N",
            "",
            vec![kv("emptyattr", Value::StringValue(String::new()))],
        )));
        // `scope_version` absent (suppressed), `emptyattr` absent (dropped).
        assert_eq!(empty_version, r#"{"scope_name":"N"}"#);

        // A non-empty neighbour survives, and a WHITESPACE-only value is NOT
        // empty — only an exactly-empty value is dropped (measured on
        // `grafana/loki:3.7.4`: `{"a":" "}` round-trips as `a=" "`).
        let mixed = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![
                kv("emptyattr", Value::StringValue(String::new())),
                kv("keep", Value::StringValue("1".to_string())),
                kv("ws", Value::StringValue(" ".to_string())),
            ],
        )));
        assert_eq!(
            mixed,
            r#"{"keep":"1","scope_name":"N","scope_version":"1.0","ws":" "}"#
        );
    }

    #[test]
    fn empty_valued_scope_attribute_drops_its_sanitized_twin_in_either_order() {
        // The strip runs BEFORE the last-write-wins resolution, so deletion is
        // by NAME over the whole raw set: two attributes sanitizing to the same
        // key, one empty, lose BOTH — whichever order they arrive in. Resolving
        // first would elect the non-empty value in one of the two orders
        // (issue #259; `labels.Builder.Reset` records the NAME in `del`).
        let empty_first = scope_sm(Some(scope(
            "N",
            "",
            vec![
                kv("a.b", Value::StringValue(String::new())),
                kv("a_b", Value::StringValue("v".to_string())),
            ],
        )));
        assert_eq!(empty_first, r#"{"scope_name":"N"}"#);

        let empty_last = scope_sm(Some(scope(
            "N",
            "",
            vec![
                kv("a_b", Value::StringValue("v".to_string())),
                kv("a.b", Value::StringValue(String::new())),
            ],
        )));
        assert_eq!(empty_last, r#"{"scope_name":"N"}"#);
    }

    /// An empty-valued scope attribute named after an IDENTITY field deletes
    /// that identity field too — the by-name strip covers `scope_name` and
    /// `scope_version`, not just the attributes.
    ///
    /// Measured on `grafana/loki:3.7.4`: an OTLP body with scope name `N`,
    /// version `1.0` and attribute `scope_name=""` comes back carrying
    /// `scope_version="1.0"` and no `scope_name`; the mirror case with
    /// attribute `scope_version=""` comes back with `scope_name="N"` alone.
    /// Rule (a) is unaffected — a NON-empty attribute of the same name still
    /// loses to the identity, asserted last.
    #[test]
    fn an_empty_valued_attribute_named_after_an_identity_field_deletes_it_too() {
        let kills_name = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![kv("scope_name", Value::StringValue(String::new()))],
        )));
        assert_eq!(kills_name, r#"{"scope_version":"1.0"}"#);

        let kills_version = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![kv("scope_version", Value::StringValue(String::new()))],
        )));
        assert_eq!(kills_version, r#"{"scope_name":"N"}"#);

        // Rule (a), unchanged: a non-empty colliding attribute loses to the
        // identity appended after it, and nothing is deleted.
        let identity_wins = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![kv("scope_name", Value::StringValue("attr".to_string()))],
        )));
        assert_eq!(identity_wins, r#"{"scope_name":"N","scope_version":"1.0"}"#);
    }

    /// …but "an empty-valued attribute takes the identity field with it" holds
    /// only while that identity field is never `Set`. `Reset` seeds the
    /// builder's `del` from the BASE scan, a U+FFFD value is a `Set` into
    /// `add`, and `Labels()` emits `add` whether or not `del` holds the name
    /// (`labels_common.go:163-200`, `labels_stringlabels.go:483-521 @
    /// v3.7.4`) — so a scope NAME carrying U+FFFD survives the same
    /// `scope_name=""` attribute that deletes a plain one, rewritten to a
    /// space.
    ///
    /// Measured on `grafana/loki:3.7.4` (`b318f282`) as the pair list this
    /// path builds: `{scope_name="", scope_name="N\u{FFFD}"}` stores
    /// `scope_name="N "` there, and the U+FFFD-free control
    /// `{scope_name="", scope_name="N"}` stores nothing. The rule is
    /// `pulsus_model`'s
    /// `the_builder_emits_a_set_name_even_when_reset_deleted_it`; this is the
    /// OTLP end of it, and the failure mode for the docs/schemas.md §3.1
    /// sentence that states it.
    #[test]
    fn a_u_fffd_bearing_scope_name_survives_an_empty_valued_attribute_of_the_same_name() {
        let survives = scope_sm(Some(scope(
            "N\u{FFFD}",
            "1.0",
            vec![kv("scope_name", Value::StringValue(String::new()))],
        )));
        assert_eq!(survives, r#"{"scope_name":"N ","scope_version":"1.0"}"#);

        // The discriminating control: identical but for the U+FFFD, and the
        // delete then stands (what the test above asserts).
        let deleted = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![kv("scope_name", Value::StringValue(String::new()))],
        )));
        assert_eq!(deleted, r#"{"scope_version":"1.0"}"#);

        // The same interaction on an ordinary attribute rather than an
        // identity field, so the rule is not read as special-casing identity.
        let attribute = scope_sm(Some(scope(
            "",
            "",
            vec![
                kv("a.b", Value::StringValue(String::new())),
                kv("a_b", Value::StringValue("p\u{FFFD}".to_string())),
            ],
        )));
        assert_eq!(attribute, r#"{"a_b":"p "}"#);
    }

    /// The REVERSE pair order — the `Set` first, the empty pair second — is
    /// reachable here too, and this test exists because an earlier revision
    /// claimed it was not. "Identity is appended after the attributes" rules
    /// out reaching that order **through the identity field**; it says
    /// nothing about reaching it through TWO ATTRIBUTES that canonicalize
    /// onto one name, with the scope's own name and version empty so nothing
    /// is appended after them at all.
    ///
    /// Measured on `grafana/loki:3.7.4` (`b318f282`) through the reference's
    /// OWN OTLP receiver (`POST /otlp/v1/logs`, read back with
    /// `categorize-labels`), not inferred from the push transport: a scope
    /// with empty name/version and attributes `scope.name="N\u{FFFD}"` then
    /// `scope_name=""` stores `scope_name="N "` (beside the reference's own
    /// `severity_number` entry, which PulsusDB does not add), the U+FFFD-free
    /// control `scope.name="N"` + `scope_name=""` stores neither, and the
    /// opposite attribute order stores `scope_name="N "` as well.
    ///
    /// Row `g06` of `pulsus_model`'s
    /// `the_builder_emits_a_set_name_even_when_reset_deleted_it` is this pair
    /// list; here it is driven through the real `parse`.
    #[test]
    fn two_attributes_canonicalizing_onto_one_name_reach_the_reverse_order_here_too() {
        for (first, second, expected, note) in [
            (
                kv("scope.name", Value::StringValue("N\u{FFFD}".to_string())),
                kv("scope_name", Value::StringValue(String::new())),
                r#"{"scope_name":"N "}"#,
                "the U+FFFD `Set` outranks the later empty pair's delete",
            ),
            (
                kv("scope_name", Value::StringValue(String::new())),
                kv("scope.name", Value::StringValue("N\u{FFFD}".to_string())),
                r#"{"scope_name":"N "}"#,
                "…and in the other attribute order",
            ),
            (
                kv("scope.name", Value::StringValue("N".to_string())),
                kv("scope_name", Value::StringValue(String::new())),
                "",
                "the discriminating control: without the U+FFFD the delete stands",
            ),
        ] {
            let stored = scope_sm(Some(scope("", "", vec![first, second])));
            assert_eq!(stored, expected, "{note}");
        }
    }

    #[test]
    fn empty_valued_resource_attribute_leaves_the_stream_label_set() {
        // Stream labels get a strip too (issue #259), the PAIR-WISE one: the
        // reference's OTLP path renders promoted labels back into a label-set
        // literal that the distributor re-parses through
        // `syntax.ParseLabels`/`WithoutEmpty`. Two resources differing only by
        // an empty-valued attribute must therefore fingerprint identically —
        // measured on `grafana/loki:3.7.4` with `deployment.environment`, one
        // of the attributes its default config actually promotes to an index
        // label.
        let with_empty = stream_labels(vec![
            kv("service.name", Value::StringValue("checkout".to_string())),
            kv("region", Value::StringValue(String::new())),
        ]);
        let without = stream_labels(vec![kv(
            "service.name",
            Value::StringValue("checkout".to_string()),
        )]);
        assert_eq!(
            with_empty.0.to_canonical_json(),
            r#"{"service_name":"checkout"}"#
        );
        assert_eq!(with_empty.0, without.0);
        assert_eq!(
            stream_fingerprint(&with_empty.0),
            stream_fingerprint(&without.0)
        );

        // Whitespace is not empty: it stays, and it changes the fingerprint.
        let whitespace = stream_labels(vec![
            kv("service.name", Value::StringValue("checkout".to_string())),
            kv("region", Value::StringValue(" ".to_string())),
        ]);
        assert_eq!(
            whitespace.0.to_canonical_json(),
            r#"{"region":" ","service_name":"checkout"}"#
        );
        assert_ne!(
            stream_fingerprint(&whitespace.0),
            stream_fingerprint(&without.0)
        );
    }

    /// A literally duplicated resource-attribute key, one occurrence empty:
    /// the pair-wise strip hands `from_normalized` the non-empty twin, so the
    /// stored label is what it was before #259 and the frozen issue-#4
    /// collision rule stays the only thing deciding duplicates. Pins the
    /// neutrality claim in `build_stream_labels`' doc — the by-name strip
    /// would drop both twins here and fail this test.
    #[test]
    fn a_duplicate_resource_attribute_with_one_empty_keeps_the_non_empty_twin() {
        for attrs in [
            vec![
                kv("region", Value::StringValue(String::new())),
                kv("region", Value::StringValue("eu".to_string())),
            ],
            vec![
                kv("region", Value::StringValue("eu".to_string())),
                kv("region", Value::StringValue(String::new())),
            ],
        ] {
            let (labels, _) = stream_labels(attrs);
            // `region` is neither indexed nor a discovery name, so the slot
            // falls back (issue #379); the duplicate rule under test is
            // unchanged by it.
            assert_eq!(
                labels.to_canonical_json(),
                r#"{"region":"eu","service_name":"unknown_service"}"#
            );
        }
    }

    #[test]
    fn parse_fingerprint_is_invariant_to_scope() {
        // AC-3: two ScopeLogs with identical resource but different-or-absent
        // scope produce the SAME fingerprint and one deduped StreamRow, with
        // per-row SM differing — scope has left the stream fingerprint.
        let resource = || {
            Some(Resource {
                attributes: vec![kv(
                    "service.name",
                    Value::StringValue("checkout".to_string()),
                )],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            })
        };
        let record = |body: &str| LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body(body),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: resource(),
                scope_logs: vec![
                    ScopeLogs {
                        scope: Some(scope("my-scope", "1.0.0", vec![])),
                        log_records: vec![record("with-scope")],
                        schema_url: String::new(),
                    },
                    ScopeLogs {
                        scope: None,
                        log_records: vec![record("no-scope")],
                        schema_url: String::new(),
                    },
                ],
                schema_url: String::new(),
            }]),
            7,
        );
        assert_eq!(out.rows.len(), 2);
        // Both records share one fingerprint / one deduped stream row.
        assert_eq!(out.rows[0].fingerprint, out.rows[1].fingerprint);
        assert_eq!(out.streams.len(), 1);
        // Per-row SM differs: scoped row carries scope metadata, the other none.
        assert_eq!(
            out.rows[0].structured_metadata,
            r#"{"scope_name":"my-scope","scope_version":"1.0.0"}"#
        );
        assert_eq!(out.rows[1].structured_metadata, "");
    }

    #[test]
    fn parse_normalizes_dotted_resource_attribute_keys() {
        let resource = Resource {
            attributes: vec![kv("k8s.pod.name", Value::StringValue("pod-1".to_string()))],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("x"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: Some(resource),
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.streams[0].labels.get("k8s_pod_name"), Some("pod-1"));
    }

    #[test]
    fn parse_counts_resource_label_collisions() {
        // Only RESOURCE attributes are stream labels now (issue #109), so the
        // collision metric counts collisions WITHIN the resource attribute set
        // (two keys sanitizing to `a_b`). A scope-attribute collision is a
        // structured-metadata concern and is NOT counted here.
        let resource = Resource {
            attributes: vec![
                kv("a.b", Value::StringValue("from_dot".to_string())),
                kv("a_b", Value::StringValue("from_underscore".to_string())),
            ],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("x"),
            ..Default::default()
        };
        let scope_logs = ScopeLogs {
            scope: Some(InstrumentationScope {
                name: String::new(),
                version: String::new(),
                // A scope-attribute collision must NOT bump `collisions`.
                attributes: vec![
                    kv("s.k", Value::StringValue("one".to_string())),
                    kv("s_k", Value::StringValue("two".to_string())),
                ],
                dropped_attributes_count: 0,
            }),
            log_records: vec![record],
            schema_url: String::new(),
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: Some(resource),
                scope_logs: vec![scope_logs],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.collisions, 1);
    }

    #[test]
    fn parse_body_string_value_is_verbatim() {
        let record = LogRecord {
            time_unix_nano: 1,
            body: string_body("plain text body"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rows[0].body, "plain text body");
    }

    #[test]
    fn parse_body_scalar_values_use_display() {
        for (value, expected) in [
            (Value::BoolValue(true), "true"),
            (Value::IntValue(42), "42"),
            (Value::DoubleValue(1.5), "1.5"),
        ] {
            let record = LogRecord {
                time_unix_nano: 1,
                body: Some(AnyValue { value: Some(value) }),
                ..Default::default()
            };
            let out = parse(
                &request(vec![ResourceLogs {
                    resource: None,
                    scope_logs: vec![simple_scope_logs(vec![record])],
                    schema_url: String::new(),
                }]),
                0,
            );
            assert_eq!(out.rows[0].body, expected);
        }
    }

    #[test]
    fn parse_body_array_value_renders_as_json() {
        let array = Value::ArrayValue(ArrayValue {
            values: vec![
                AnyValue {
                    value: Some(Value::IntValue(1)),
                },
                AnyValue {
                    value: Some(Value::StringValue("two".to_string())),
                },
            ],
        });
        let record = LogRecord {
            time_unix_nano: 1,
            body: Some(AnyValue { value: Some(array) }),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rows[0].body, r#"[1,"two"]"#);
    }

    #[test]
    fn parse_body_kvlist_value_renders_as_json_object() {
        let kvlist = Value::KvlistValue(KeyValueList {
            values: vec![kv("nested", Value::StringValue("val".to_string()))],
        });
        let record = LogRecord {
            time_unix_nano: 1,
            body: Some(AnyValue {
                value: Some(kvlist),
            }),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rows[0].body, r#"{"nested":"val"}"#);
    }

    #[test]
    fn parse_body_bytes_value_renders_as_base64() {
        let record = LogRecord {
            time_unix_nano: 1,
            body: Some(AnyValue {
                value: Some(Value::BytesValue(b"hi".to_vec())),
            }),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rows[0].body, "aGk=");
    }

    #[test]
    fn parse_body_absent_renders_as_empty_string() {
        let record = LogRecord {
            time_unix_nano: 1,
            body: None,
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rows[0].body, "");
    }

    #[test]
    fn parse_severity_in_valid_range_is_preserved() {
        let record = LogRecord {
            time_unix_nano: 1,
            severity_number: 17,
            body: string_body("x"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rows[0].severity, 17);
    }

    #[test]
    fn parse_severity_out_of_range_resolves_to_zero() {
        for severity_number in [0, -1, 25, 1000] {
            let record = LogRecord {
                time_unix_nano: 1,
                severity_number,
                body: string_body("x"),
                ..Default::default()
            };
            let out = parse(
                &request(vec![ResourceLogs {
                    resource: None,
                    scope_logs: vec![simple_scope_logs(vec![record])],
                    schema_url: String::new(),
                }]),
                0,
            );
            assert_eq!(out.rows[0].severity, 0, "severity_number {severity_number}");
        }
    }

    #[test]
    fn parse_timestamp_prefers_time_unix_nano() {
        let record = LogRecord {
            time_unix_nano: 111,
            observed_time_unix_nano: 222,
            body: string_body("x"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            999,
        );
        assert_eq!(out.rows[0].timestamp_ns.0, 111);
    }

    #[test]
    fn parse_timestamp_falls_back_to_observed_time_when_time_unix_nano_is_zero() {
        let record = LogRecord {
            time_unix_nano: 0,
            observed_time_unix_nano: 222,
            body: string_body("x"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            999,
        );
        assert_eq!(out.rows[0].timestamp_ns.0, 222);
    }

    #[test]
    fn parse_timestamp_falls_back_to_now_ns_when_both_are_zero() {
        let record = LogRecord {
            time_unix_nano: 0,
            observed_time_unix_nano: 0,
            body: string_body("x"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![record])],
                schema_url: String::new(),
            }]),
            999,
        );
        assert_eq!(out.rows[0].timestamp_ns.0, 999);
    }

    #[test]
    fn parse_rejects_a_record_with_an_unrepresentable_timestamp_as_partial_success() {
        let bad = LogRecord {
            time_unix_nano: u64::MAX, // top bit set: does not fit in i64
            body: string_body("bad"),
            ..Default::default()
        };
        let good = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("good"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![bad, good])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rejected, 1);
        assert!(out.rejected_message.is_some());
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].body, "good");
        // The rejected record contributes no stream row either.
        assert_eq!(out.streams.len(), 1);
    }

    #[test]
    fn parse_rejects_a_far_future_record_instead_of_orphaning_it_into_the_max_date_partition() {
        // Representable as i64 ns but ~year 2200 — past the 2149-06-06
        // ClickHouse `Date` cutoff (and past the tighter 2106-02-06
        // DateTime-safe cutoff, issue #137). Before #8's fix this saturated
        // the month to day 65535, silently orphaning the sample; now it is a
        // clean per-record rejection (partial success), contributing no
        // stream row.
        let far_future_ns: i64 = 86_400_000_000_000 * 84_000;
        let bad = LogRecord {
            time_unix_nano: far_future_ns as u64,
            body: string_body("far-future"),
            ..Default::default()
        };
        let good = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("good"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![bad, good])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rejected, 1);
        assert!(
            out.rejected_message.as_deref().unwrap().contains(
                "outside the supported storage time range (1970-01-01 to 2106-02-06 UTC)"
            )
        );
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].body, "good");
        assert_eq!(out.streams.len(), 1);
        // No stream row registered at the max-`Date` boundary.
        assert!(
            out.streams
                .iter()
                .all(|s| s.month.days_since_epoch() != u16::MAX)
        );
    }

    #[test]
    fn parse_accepts_the_last_datetime_safe_day_but_rejects_the_first_unsafe_one() {
        // Issue #137 (re-pointing #8's round-2 boundary pair from the `Date`
        // horizon to the DateTime-safe one): `log_samples` partitions by the
        // RAW sample day and its delete-TTL evaluates the row timestamp in
        // the 32-bit `DateTime` domain. Day 49_709 = 2106-02-06 is the last
        // UTC day fully inside that domain; day 49_710 = 2106-02-07 still
        // partitions correctly (inside the u16 `Date` range) but its TTL
        // seconds value exceeds u32::MAX — before #137 such a record was
        // accepted with a wrap-prone timestamp. The day-49_710 record must
        // now be rejected while the day-49_709 record stays accepted (no
        // over-rejection).
        const NANOS_PER_DAY: i64 = 86_400_000_000_000;
        let last_ok_ns = NANOS_PER_DAY * 49_709; // 2106-02-06 00:00 UTC
        let first_bad_ns = NANOS_PER_DAY * 49_710; // 2106-02-07 00:00 UTC
        let accepted = LogRecord {
            time_unix_nano: last_ok_ns as u64,
            body: string_body("last-datetime-safe-day"),
            ..Default::default()
        };
        let rejected = LogRecord {
            time_unix_nano: first_bad_ns as u64,
            body: string_body("first-datetime-unsafe-day"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![accepted, rejected])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rejected, 1);
        assert!(
            out.rejected_message.as_deref().unwrap().contains(
                "outside the supported storage time range (1970-01-01 to 2106-02-06 UTC)"
            )
        );
        // Only the in-range record survives, unchanged.
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].body, "last-datetime-safe-day");
        // Its stream registers exactly its month (2106-02-01 = day 49_704).
        assert_eq!(out.streams.len(), 1);
        assert_eq!(out.streams[0].month.days_since_epoch(), 49_704);
    }

    #[test]
    fn parse_dedups_streams_by_fingerprint_and_month_across_scopes() {
        // Two ScopeLogs with identical resource+scope (same fingerprint),
        // both in the same UTC month: exactly one StreamRow.
        let record_a = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("a"),
            ..Default::default()
        };
        let record_b = LogRecord {
            time_unix_nano: 1_700_000_100_000_000_000,
            body: string_body("b"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![
                    simple_scope_logs(vec![record_a]),
                    simple_scope_logs(vec![record_b]),
                ],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.streams.len(), 1);
    }

    #[test]
    fn parse_cross_month_request_yields_two_stream_rows() {
        // 2024-01-31T23:00:00Z and 2024-02-01T01:00:00Z: same stream,
        // straddling a UTC month boundary.
        let jan = LogRecord {
            time_unix_nano: 1_706_741_600_000_000_000,
            body: string_body("jan"),
            ..Default::default()
        };
        let feb = LogRecord {
            time_unix_nano: 1_706_756_400_000_000_000,
            body: string_body("feb"),
            ..Default::default()
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![jan, feb])],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.streams.len(), 2);
        let mut months: Vec<_> = out.streams.iter().map(|s| s.month).collect();
        months.sort();
        assert_ne!(months[0], months[1]);
        // Both stream rows share the same fingerprint (one logical stream).
        assert_eq!(out.streams[0].fingerprint, out.streams[1].fingerprint);
    }

    #[test]
    fn parse_backfilled_timestamp_registers_the_historical_month_not_now() {
        // Record timestamped in 2020, received "now" in 2024.
        let backfilled = LogRecord {
            time_unix_nano: 1_577_836_800_000_000_000, // 2020-01-01T00:00:00Z
            body: string_body("old"),
            ..Default::default()
        };
        let now_ns = 1_700_000_000_000_000_000; // ~2023-11-14
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![simple_scope_logs(vec![backfilled])],
                schema_url: String::new(),
            }]),
            now_ns,
        );
        assert_eq!(out.streams.len(), 1);
        let month_days = out.streams[0].month.days_since_epoch();
        let now_month_days = Date::start_of_month_utc(now_ns).unwrap().days_since_epoch();
        assert_ne!(month_days, now_month_days);
        // 2020-01-01 is day 18262 since the epoch.
        assert_eq!(month_days, 18_262);
        // `updated_ns` is still the receive time, not the record's month.
        assert_eq!(out.streams[0].updated_ns, now_ns);
    }

    #[test]
    fn parse_is_a_pure_function_of_its_arguments() {
        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            severity_number: 9,
            body: string_body("deterministic"),
            ..Default::default()
        };
        let req = request(vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", Value::StringValue("svc".to_string()))],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![simple_scope_logs(vec![record])],
            schema_url: String::new(),
        }]);
        let a = parse(&req, 42);
        let b = parse(&req, 42);
        assert_eq!(a, b);
    }

    #[test]
    fn decode_rejects_malformed_bytes() {
        let err = decode(b"\xFF\xFF\xFF not a protobuf message").unwrap_err();
        assert!(matches!(err, LogsIngestError::Decode(_)));
    }

    #[test]
    fn decode_round_trips_an_encoded_request() {
        let req = request(vec![ResourceLogs {
            resource: None,
            scope_logs: vec![simple_scope_logs(vec![LogRecord {
                time_unix_nano: 1,
                body: string_body("x"),
                ..Default::default()
            }])],
            schema_url: String::new(),
        }]);
        let bytes = req.encode_to_vec();
        let decoded = decode(&bytes).expect("valid protobuf decodes");
        assert_eq!(decoded, req);
    }

    #[test]
    fn base64_encode_matches_the_rfc_7617_worked_example() {
        assert_eq!(
            base64_encode(b"Aladdin:open sesame"),
            "QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );
    }

    #[test]
    fn base64_encode_pads_single_and_double_byte_remainders() {
        assert_eq!(base64_encode(b"a"), "YQ==");
        assert_eq!(base64_encode(b"ab"), "YWI=");
        assert_eq!(base64_encode(b"abc"), "YWJj");
    }

    // -- AnyValue recursion-depth guard (finding #54) --------------------

    /// A log record `body` nested `levels` `AnyValue` nodes deep (a scalar
    /// leaf wrapped in `levels - 1` `ArrayValue` containers). Built
    /// iteratively; `levels <= MAX_ANYVALUE_DEPTH + 1` here, so its `Drop`
    /// recursion is trivially safe.
    fn nested_body(levels: usize) -> AnyValue {
        let mut value = AnyValue {
            value: Some(Value::StringValue("leaf".to_string())),
        };
        for _ in 1..levels {
            value = AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![value],
                })),
            };
        }
        value
    }

    fn request_with_body(body: AnyValue) -> ExportLogsServiceRequest {
        request(vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: Some(body),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }])
    }

    #[test]
    fn parse_accepts_body_anyvalue_nesting_at_the_depth_cap() {
        let req = request_with_body(nested_body(
            crate::protocols::otlp_depth::MAX_ANYVALUE_DEPTH,
        ));
        // Calls the real fallible `parse` (not the unwrap shim): an at-cap
        // body renders and yields exactly one row, unchanged by the guard.
        let out = parse_off(&req, 0).expect("at-cap body is within the depth guard");
        assert_eq!(out.rows.len(), 1);
    }

    #[test]
    fn parse_rejects_body_anyvalue_nesting_past_the_depth_cap() {
        // One container level deeper than the accepted case above — WITHOUT
        // the guard this parses identically (renders to a JSON string and
        // yields one row); the guard makes it a whole-request reject before
        // any row is materialized, proving the reject is non-vacuous.
        let req = request_with_body(nested_body(
            crate::protocols::otlp_depth::MAX_ANYVALUE_DEPTH + 1,
        ));
        let err = parse_off(&req, 0).expect_err("over-depth body is rejected whole-request");
        assert!(matches!(err, LogsIngestError::OversizeMessage { .. }));
    }

    #[test]
    fn parse_rejects_attribute_anyvalue_nesting_past_the_depth_cap() {
        // The reject also covers resource attribute values, not just bodies.
        let req = request(vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "deep".to_string(),
                    value: Some(nested_body(
                        crate::protocols::otlp_depth::MAX_ANYVALUE_DEPTH + 1,
                    )),
                    key_strindex: 0,
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![simple_scope_logs(vec![LogRecord {
                time_unix_nano: 1_700_000_000_000_000_000,
                body: string_body("x"),
                ..Default::default()
            }])],
            schema_url: String::new(),
        }]);
        let err = parse_off(&req, 0).expect_err("over-depth resource attribute is rejected");
        assert!(matches!(err, LogsIngestError::OversizeMessage { .. }));
    }

    // -- per-stream label rules (issue #374) -------------------------------
    //
    // The OTLP logs path reaches the same validator upstream — its
    // `/otlp/v1/logs` handler funnels into `Distributor.PushWithResolver` ->
    // `parseStreamLabels` -> `ValidateLabels` like `/loki/api/v1/push`
    // (`pkg/distributor/http.go:28-33 @ v3.7.4`) — but not with the same data:
    // only the 18 attributes in
    // `distributor.otlp.default_resource_attributes_as_index_labels` become
    // stream labels there, and the rest become structured metadata
    // (`pkg/loghttp/push/otlp_config.go:56-73`, `otlp.go:180-212 @ v3.7.4`).
    // PulsusDB indexes them all (#109), so the bounds are charged on the subset
    // the reference indexes; these cases pin both halves of that, each measured
    // against `grafana/loki@sha256:87f0a067…`.

    fn logs_with_resource_attrs(attrs: Vec<KeyValue>) -> ExportLogsServiceRequest {
        request(vec![ResourceLogs {
            resource: Some(Resource {
                attributes: attrs,
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![simple_scope_logs(vec![LogRecord {
                time_unix_nano: 1_700_000_000_000_000_000,
                body: string_body("hello"),
                ..Default::default()
            }])],
            schema_url: String::new(),
        }])
    }

    fn attr(key: &str, value: &str) -> KeyValue {
        kv(key, Value::StringValue(value.to_string()))
    }

    fn only_stream_error(out: &ParsedLogs) -> &str {
        assert!(out.rows.is_empty(), "a rejected stream contributes no rows");
        assert_eq!(out.stream_errors.len(), 1, "{:?}", out.stream_errors);
        &out.stream_errors[0]
    }

    /// The 18 OTel keys the reference indexes, minus `service.name` (which the
    /// count bound discounts) — enough to build a 15/16 edge.
    const INDEXED: [&str; 17] = [
        "service.namespace",
        "service.instance.id",
        "deployment.environment",
        "deployment.environment.name",
        "cloud.region",
        "cloud.availability_zone",
        "k8s.cluster.name",
        "k8s.namespace.name",
        "k8s.pod.name",
        "k8s.container.name",
        "container.name",
        "k8s.replicaset.name",
        "k8s.deployment.name",
        "k8s.statefulset.name",
        "k8s.daemonset.name",
        "k8s.cronjob.name",
        "k8s.job.name",
    ];

    fn indexed(n: usize) -> Vec<KeyValue> {
        INDEXED[..n].iter().map(|k| attr(k, "v")).collect()
    }

    /// Issue #374: a request carrying no log records converts to an EMPTY
    /// push request upstream (`ld.LogRecordCount() == 0`,
    /// `pkg/loghttp/push/otlp.go:144-146 @ v3.7.4`), which the distributor
    /// then refuses with `422` (`distributor.go:579-581 @ v3.7.4`). All four
    /// shapes of "no records" measure `422` on
    /// `grafana/loki@sha256:87f0a067…`, including the one the review found —
    /// a resource with attributes and an empty `logRecords`.
    #[test]
    fn parse_rejects_a_request_with_no_log_records() {
        let empty_scope = ResourceLogs {
            resource: Some(Resource {
                attributes: vec![attr("service.name", "probe")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![simple_scope_logs(vec![])],
            schema_url: String::new(),
        };
        let no_scopes = ResourceLogs {
            scope_logs: vec![],
            ..empty_scope.clone()
        };
        for req in [
            request(vec![]),
            request(vec![no_scopes]),
            request(vec![empty_scope.clone()]),
            request(vec![empty_scope.clone(), empty_scope]),
        ] {
            let err = parse_off(&req, 0).expect_err("no log records");
            assert!(matches!(err, LogsIngestError::MissingStreams), "{err:?}");
            assert_eq!(
                err.to_string(),
                "error at least one valid stream is required for ingestion"
            );
        }
    }

    /// The discriminating neighbour: one record anywhere in the request is
    /// enough, even in a resource that carries no attributes at all (measured
    /// `204` upstream — its own `service_name` discovery gives that resource a
    /// label), and even when a record-less resource sits beside it.
    #[test]
    fn parse_accepts_a_request_whose_records_are_all_in_one_resource() {
        let empty = ResourceLogs {
            resource: Some(Resource {
                attributes: vec![attr("service.name", "empty")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![simple_scope_logs(vec![])],
            schema_url: String::new(),
        };
        let full = ResourceLogs {
            resource: None,
            scope_logs: vec![simple_scope_logs(vec![LogRecord {
                time_unix_nano: 1_700_000_000_000_000_000,
                body: string_body("hello"),
                ..Default::default()
            }])],
            schema_url: String::new(),
        };
        let out = parse_off(&request(vec![empty, full]), 0).unwrap();
        assert_eq!(out.rows.len(), 1);
    }

    /// Issue #374, ledger residual 6: the `AnyValue` depth cap is charged
    /// BEFORE the stream-less check, so a request that is both record-less
    /// and over-deep is the depth `400`, not the `422`. The reference has no
    /// depth cap and answers `422` for this body — measured on both
    /// transports against `grafana/loki@sha256:87f0a067…`.
    ///
    /// This pins `parse`'s OWN order only. On the wire the cap is charged
    /// earlier still, inside decode, so `parse`'s guard never gets the chance
    /// — the wire order is pinned by
    /// `ingest::http::tests::a_record_less_over_deep_request_is_400_not_the_stream_less_422`
    /// and by the harness's `*-over-deep-attr` cases. Both layers are worth
    /// pinning: this one keeps a direct library caller of `parse` on the same
    /// rule the wire has.
    ///
    /// The at-cap neighbour below is the discriminator: one level shallower,
    /// the same record-less shape, and it *does* reach the `422` — so this
    /// test cannot pass merely because the request is record-less.
    #[test]
    fn the_depth_cap_outranks_the_stream_less_check() {
        let over_deep = |levels| {
            request(vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "deep".to_string(),
                        value: Some(nested_body(levels)),
                        key_strindex: 0,
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                // No records anywhere: on its own this is `MissingStreams`.
                scope_logs: vec![simple_scope_logs(vec![])],
                schema_url: String::new(),
            }])
        };
        let err = parse_off(
            &over_deep(crate::protocols::otlp_depth::MAX_ANYVALUE_DEPTH + 1),
            0,
        )
        .expect_err("record-less AND over-deep");
        assert!(
            matches!(err, LogsIngestError::OversizeMessage { .. }),
            "depth cap must win over MissingStreams, got {err:?}"
        );
        let err = parse_off(
            &over_deep(crate::protocols::otlp_depth::MAX_ANYVALUE_DEPTH),
            0,
        )
        .expect_err("record-less, within the depth cap");
        assert!(matches!(err, LogsIngestError::MissingStreams), "{err:?}");
    }

    #[test]
    fn parse_rejects_an_indexed_attribute_value_over_2048_bytes() {
        let value = "b".repeat(2049);
        let req = logs_with_resource_attrs(vec![attr("k8s.pod.name", &value)]);
        let out = parse_off(&req, 0).unwrap();
        // The rendered set is the POST-synthesis one, on this transport too:
        // the reference validates the `streamLabels` map after writing the
        // `service_name` slot into it (`otlp.go:174-244` ->
        // `distributor.go:1370 @ v3.7.4`). Measured on stock
        // `grafana/loki@sha256:87f0a067…`, byte for byte.
        assert_eq!(
            only_stream_error(&out),
            format!(
                "stream '{{k8s_pod_name=\"{value}\", service_name=\"unknown_service\"}}' \
                 has label value too long: '{value}'"
            )
        );
    }

    #[test]
    fn parse_accepts_an_indexed_attribute_value_at_2048_bytes() {
        let req = logs_with_resource_attrs(vec![attr("k8s.pod.name", &"b".repeat(2048))]);
        assert_eq!(parse_off(&req, 0).unwrap().rows.len(), 1);
    }

    /// The other half, and the one that made the previous implementation
    /// narrower than the reference: an attribute the reference does not index
    /// is structured metadata there, so no bound applies to it. Measured on
    /// `grafana/loki@sha256:87f0a067…`: a resource carrying `app` with a
    /// 2049-byte value answers `204` and stores it as structured metadata.
    #[test]
    fn a_non_indexed_attribute_is_outside_every_bound() {
        for (key, value) in [
            ("app".to_string(), "b".repeat(2049)),
            ("a".repeat(1025), "v".to_string()),
        ] {
            let req = logs_with_resource_attrs(vec![attr(&key, &value)]);
            let out = parse_off(&req, 0).unwrap();
            assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
            assert_eq!(out.rows.len(), 1);
        }
        // ...and 16 arbitrary attributes is a `204` upstream, not a `400`.
        let attrs: Vec<KeyValue> = (0..16).map(|i| attr(&format!("l{i}"), "v")).collect();
        let out = parse_off(&logs_with_resource_attrs(attrs), 0).unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        assert_eq!(out.rows.len(), 1);
    }

    /// ...but a non-indexed attribute is still STORED as a stream label here
    /// (issue #109), which is what makes the subset the bound is charged on
    /// different from the set that is stored.
    #[test]
    fn a_non_indexed_attribute_is_still_stored_as_a_stream_label() {
        let req = logs_with_resource_attrs(vec![attr("app", "checkout")]);
        let out = parse_off(&req, 0).unwrap();
        assert_eq!(out.streams[0].labels.get("app"), Some("checkout"));
    }

    #[test]
    fn parse_rejects_sixteen_indexed_attributes_and_accepts_fifteen() {
        assert_eq!(
            parse_off(&logs_with_resource_attrs(indexed(15)), 0)
                .unwrap()
                .rows
                .len(),
            1
        );
        let out = parse_off(&logs_with_resource_attrs(indexed(16)), 0).unwrap();
        assert!(only_stream_error(&out).ends_with("' has 16 label names; limit 15"));
        // `service.name` canonicalizes to `service_name`, which is indexed but
        // not counted (`validator.go:169-174 @ v3.7.4`).
        let mut with_service = indexed(15);
        with_service.push(attr("service.name", "checkout"));
        assert_eq!(
            parse_off(&logs_with_resource_attrs(with_service), 0)
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    /// Non-indexed attributes do not count towards the 15 either, so 15
    /// indexed plus any number of others is accepted — the case an ordinary
    /// OTLP shipper actually sends.
    #[test]
    fn non_indexed_attributes_do_not_count_towards_the_label_bound() {
        let mut attrs = indexed(15);
        attrs.extend((0..40).map(|i| attr(&format!("extra{i}"), "v")));
        let out = parse_off(&logs_with_resource_attrs(attrs), 0).unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        // 15 indexed + 40 others + the discovered `service_name` (issue #379,
        // from `container.name` — one of the 15 indexed here).
        assert_eq!(out.streams[0].labels.len(), 56);
    }

    /// `pkg/distributor/distributor.go:639-641 @ v3.7.4` skips an entry-less
    /// stream before validating it. The over-wide resource needs a
    /// record-carrying sibling to be observed at all: alone it would make the
    /// whole request stream-less, which is the `422` above, not this rule.
    /// Measured on `grafana/loki@sha256:87f0a067…` as `204` (case
    /// `otlp/record-less-over-wide-resource+good`).
    #[test]
    fn a_resource_with_no_records_skips_the_label_bounds() {
        let req = request(vec![
            ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![attr("k8s.pod.name", &"b".repeat(4096))],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![simple_scope_logs(vec![])],
                schema_url: String::new(),
            },
            ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![attr("service.name", "good")],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![simple_scope_logs(vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: string_body("hello"),
                    ..Default::default()
                }])],
                schema_url: String::new(),
            },
        ]);
        let out = parse_off(&req, 0).unwrap();
        assert_eq!(out.rows.len(), 1);
        assert!(out.stream_errors.is_empty());
    }

    /// Scope attributes are structured metadata, not stream labels (#109), so
    /// they are outside the bound — matching upstream, whose OTLP translation
    /// puts them in structured metadata too.
    #[test]
    fn scope_attributes_are_not_subject_to_the_stream_label_bounds() {
        let req = request(vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "s".to_string(),
                    version: String::new(),
                    attributes: vec![attr("wide", &"b".repeat(4096))],
                    dropped_attributes_count: 0,
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: string_body("hello"),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }]);
        assert_eq!(parse_off(&req, 0).unwrap().rows.len(), 1);
    }

    /// `WithoutEmpty` reaches this transport too — the reference's OTLP
    /// translation renders a label literal that `parseStreamLabels` re-parses
    /// (`pkg/loghttp/push/otlp.go:244`, `distributor.go:1370 @ v3.7.4`) — so an
    /// empty-valued resource attribute must not reach the stored label set or
    /// the fingerprint. Applies to indexed and non-indexed attributes alike,
    /// since the drop is about storage, not about the bounds.
    #[test]
    fn an_empty_valued_resource_attribute_is_absent_from_the_stored_stream() {
        let with_empty = parse_off(
            &logs_with_resource_attrs(vec![
                attr("service.name", "checkout"),
                attr("ignored", ""),
                attr("k8s.pod.name", ""),
            ]),
            0,
        )
        .unwrap();
        let without = parse_off(
            &logs_with_resource_attrs(vec![attr("service.name", "checkout")]),
            0,
        )
        .unwrap();
        assert_eq!(with_empty.streams[0].labels.get("ignored"), None);
        assert_eq!(with_empty.streams[0].labels.get("k8s_pod_name"), None);
        assert_eq!(with_empty.streams[0].labels, without.streams[0].labels);
        assert_eq!(
            with_empty.streams[0].fingerprint,
            without.streams[0].fingerprint
        );
    }

    /// ...and an empty-valued indexed attribute is neither counted nor
    /// length-checked, so 15 indexed attributes plus empty-valued ones are
    /// accepted.
    #[test]
    fn empty_valued_indexed_attributes_are_not_counted() {
        let mut attrs = indexed(15);
        attrs.push(attr("k8s.job.name", ""));
        attrs.push(attr("service.name", ""));
        let out = parse_off(&logs_with_resource_attrs(attrs), 0).unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        assert_eq!(out.streams[0].labels.len(), 15);
    }

    /// `validator.go:164-167 @ v3.7.4` exempts a stream whose *index labels*
    /// carry `__pattern__`/`__aggregated_metric__`. An OTLP resource attribute
    /// of that name is not indexed upstream — it is structured metadata — so
    /// the exemption is unreachable from this transport on both sides, and 16
    /// indexed attributes are still refused when one is present. Measured on
    /// `grafana/loki@sha256:87f0a067…`.
    #[test]
    fn an_internal_label_from_a_resource_attribute_does_not_exempt_the_stream() {
        for internal in ["__aggregated_metric__", "__pattern__"] {
            let mut attrs = indexed(16);
            attrs.push(attr(internal, "x"));
            let out = parse_off(&logs_with_resource_attrs(attrs), 0).unwrap();
            assert!(
                only_stream_error(&out).ends_with("' has 16 label names; limit 15"),
                "{internal}"
            );
        }
    }

    /// The reference writes the streams that passed and answers `400` after
    /// them (`distributor.go:645-655, 780-790, 929 @ v3.7.4`), so one
    /// malformed resource must not cost a client the rest of its batch.
    #[test]
    fn a_bad_resource_costs_only_itself() {
        let resource = |value: &str| ResourceLogs {
            resource: Some(Resource {
                attributes: vec![attr("k8s.pod.name", value)],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![simple_scope_logs(vec![LogRecord {
                time_unix_nano: 1_700_000_000_000_000_000,
                body: string_body("hello"),
                ..Default::default()
            }])],
            schema_url: String::new(),
        };
        let req = request(vec![
            resource("good"),
            resource(&"b".repeat(2049)),
            resource("also_good"),
        ]);
        let out = parse_off(&req, 0).unwrap();
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.streams.len(), 2);
        assert_eq!(out.stream_errors.len(), 1);
        assert!(out.stream_errors[0].contains("has label value too long"));
    }

    /// The subset is selected on the **raw** attribute name, which is what
    /// `ActionForResourceAttribute` matches (`otlp.go:193 @ v3.7.4`, exact
    /// string equality at `otlp_config.go:88-99`) before `attributeToLabels`
    /// canonicalizes it. `service_name` is not one of the 18 raw names — only
    /// `service.name` is — so upstream routes it to structured metadata and no
    /// bound applies. Measured on `grafana/loki@sha256:87f0a067…`: a resource
    /// carrying `{service_name: "x"*2049}` answers `204` there.
    #[test]
    fn a_raw_attribute_that_only_canonicalizes_into_an_index_name_is_not_bounded() {
        let value = "x".repeat(2049);
        for spelling in [
            "service_name",
            "service-name",
            "k8s_pod_name",
            "cloud-region",
        ] {
            let req = logs_with_resource_attrs(vec![attr(spelling, &value)]);
            let out = parse_off(&req, 0).unwrap();
            assert!(
                out.stream_errors.is_empty(),
                "{spelling}: {:?}",
                out.stream_errors
            );
            assert_eq!(out.rows.len(), 1, "{spelling}");
        }
    }

    /// The other direction of the same rule, over the reference's whole list
    /// rather than one sample: every raw index name bounds, and the same name
    /// with its separators already canonicalized does not.
    #[test]
    fn every_raw_index_name_bounds_and_its_canonical_spelling_does_not() {
        let value = "x".repeat(2049);
        let mut raw_index = INDEXED.to_vec();
        raw_index.push("service.name");
        for raw in raw_index {
            let out = parse_off(&logs_with_resource_attrs(vec![attr(raw, &value)]), 0).unwrap();
            assert_eq!(out.stream_errors.len(), 1, "{raw} must be bounded");
            assert!(
                out.stream_errors[0].contains("has label value too long"),
                "{raw}"
            );

            let canonical = raw.replace('.', "_");
            let out =
                parse_off(&logs_with_resource_attrs(vec![attr(&canonical, &value)]), 0).unwrap();
            assert!(
                out.stream_errors.is_empty(),
                "{canonical} must not be bounded: {:?}",
                out.stream_errors
            );
        }
    }

    /// A near-miss spelling is not counted towards the 15 either, so 15 raw
    /// index attributes alongside a full set of underscored look-alikes is
    /// accepted — and all of them are still stored as stream labels (#109).
    #[test]
    fn near_miss_spellings_are_stored_but_not_counted() {
        let mut attrs = indexed(15);
        attrs.extend(
            INDEXED
                .iter()
                .map(|k| attr(&k.replace('.', "_"), "v"))
                .collect::<Vec<_>>(),
        );
        let out = parse_off(&logs_with_resource_attrs(attrs), 0).unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        // The 15 raw and the 17 underscored spellings canonicalize onto the
        // same 17 label names, plus the discovered `service_name` (issue
        // #379, from `container.name`), so the stored set is 18 wide.
        assert_eq!(out.streams[0].labels.len(), 18);
        assert_eq!(out.streams[0].labels.get("service_name"), Some("v"));
    }

    /// The gap the bounds do not cover, pinned on **stored state** rather than
    /// on the wire verdict (issue #374 round-3 review; adjudicated to #109) —
    /// and, since issue #379, the one name it no longer reaches.
    ///
    /// The wire verdict agrees with the reference: an index attribute
    /// alongside its underscored near-miss is accepted by both, because
    /// upstream matches the raw dotted name and routes the underscored one to
    /// structured metadata, which no bound reaches.
    ///
    /// Storage still disagrees for seventeen of the eighteen: we index every
    /// resource attribute (#109), both spellings canonicalize onto one label,
    /// and `from_normalized`'s frozen collision rule (#4) keeps the greatest
    /// *original* key — `_` (0x5F) sorts after `.` (0x2E) — so the
    /// **unvalidated** near-miss wins and a 2049-byte value is stored under a
    /// label the validator passed at two bytes.
    ///
    /// `service_name` is the exception: its slot is resolved from the raw
    /// attributes and written last, exactly as the reference's map assignment
    /// is, so the near-miss cannot win it. Measured on stock
    /// `grafana/loki@sha256:87f0a067…` via `/loki/api/v1/series`:
    /// `{service.name: "ok379", service_name: <2049 B>}` stores
    /// `{service_name="ok379"}` there, and now here.
    ///
    /// Both halves are asserted side by side so neither can drift into the
    /// other: unifying them would fail one assertion or the other.
    #[test]
    fn an_index_attribute_and_its_near_miss_collide_on_the_unvalidated_value() {
        let wide = "x".repeat(2049);

        // -- the surviving gap: `k8s.pod.name`, which the slot does not
        // govern. The near-miss value is stored and fixes the identity.
        let out = parse_off(
            &logs_with_resource_attrs(vec![
                attr("k8s.pod.name", "ok"),
                attr("k8s_pod_name", &wide),
            ]),
            0,
        )
        .unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        let stored = &out.streams[0].labels;
        assert_eq!(stored.get("k8s_pod_name"), Some(wide.as_str()));
        assert!(
            stored.get("k8s_pod_name").map(str::len)
                > Some(log_label_limits::MAX_LABEL_VALUE_BYTES),
            "an indexed, stored label wider than the bound this module introduces"
        );
        let near_miss_alone = parse_off(
            &logs_with_resource_attrs(vec![attr("k8s_pod_name", &wide)]),
            0,
        )
        .unwrap();
        assert_eq!(
            out.streams[0].fingerprint, near_miss_alone.streams[0].fingerprint,
            "the unvalidated value fixes the stream's identity"
        );

        // -- the closed case: `service_name`. The slot wins, so the stored
        // value is the one the bound was charged on and the identity follows
        // the VALIDATED attribute instead.
        let out = parse_off(
            &logs_with_resource_attrs(vec![
                attr("service.name", "ok"),
                attr("service_name", &wide),
            ]),
            0,
        )
        .unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        let stored = &out.streams[0].labels;
        assert_eq!(stored.len(), 1, "the near-miss is not stored at all");
        assert_eq!(stored.get("service_name"), Some("ok"));

        let validated = parse_off(
            &logs_with_resource_attrs(vec![attr("service.name", "ok")]),
            0,
        )
        .unwrap();
        assert_eq!(
            out.streams[0].fingerprint, validated.streams[0].fingerprint,
            "the validated value fixes the identity now"
        );
        // ...and NOT the stream a bare over-wide `service_name` produces,
        // which is the assertion that discriminates: were `from_normalized`
        // still deciding this name, these two would be one stream.
        let near_miss_alone = parse_off(
            &logs_with_resource_attrs(vec![attr("service_name", &wide)]),
            0,
        )
        .unwrap();
        assert_ne!(
            out.streams[0].fingerprint, near_miss_alone.streams[0].fingerprint,
            "the unvalidated near-miss no longer decides `service_name`"
        );
        assert_eq!(
            near_miss_alone.streams[0].labels.to_canonical_json(),
            r#"{"service_name":"unknown_service"}"#,
            "a near-miss alone leaves the slot at its fallback"
        );
    }

    /// **The #379 and #381 seams do not see each other** (merge check). Two
    /// resolvers now run over one `ResourceLogs`: `service_name` discovery and
    /// the stream-label build read the RESOURCE attributes, while the
    /// structured-metadata builder reads the SCOPE attributes. They are
    /// separate lists on the wire, so neither can observe the other's output —
    /// asserted here rather than argued, because both landed in this file in
    /// the same week and a shared list would be invisible until it changed a
    /// stored value.
    ///
    /// The discriminating case is the second one: a SCOPE attribute named
    /// `service.name` canonicalizes onto the label `service_name`, so if the
    /// two seams shared a list it would either capture the stream's identity
    /// or be captured by it. It does neither — it is stored as structured
    /// metadata and the stream keeps the synthesized `unknown_service`.
    #[test]
    fn the_service_name_slot_and_the_scope_metadata_builder_read_different_lists() {
        // (1) A resource attribute reaches the stream labels and NOT the
        // scope's structured metadata.
        let mut request = logs_with_resource_attrs(vec![
            attr("service.name", "checkout"),
            attr("k8s.container.name", "c"),
        ]);
        request.resource_logs[0].scope_logs[0].scope = Some(scope(
            "",
            "",
            vec![kv("team", Value::StringValue("pay".to_string()))],
        ));
        let out = parse_off(&request, 0).unwrap();
        assert_eq!(out.rows[0].structured_metadata, r#"{"team":"pay"}"#);
        assert_eq!(out.streams[0].service, "checkout");
        assert_eq!(
            out.streams[0].labels.get("k8s_container_name"),
            Some("c"),
            "labels: {:?}",
            out.streams[0].labels
        );

        // (2) …and a SCOPE attribute canonicalizing onto `service_name`
        // reaches the structured metadata and NOT the stream's identity, which
        // keeps the synthesized fallback.
        let mut request = logs_with_resource_attrs(vec![attr("zzz", "v")]);
        request.resource_logs[0].scope_logs[0].scope = Some(scope(
            "",
            "",
            vec![kv(
                "service.name",
                Value::StringValue("from-scope".to_string()),
            )],
        ));
        let out = parse_off(&request, 0).unwrap();
        assert_eq!(
            out.rows[0].structured_metadata,
            r#"{"service_name":"from-scope"}"#
        );
        assert_eq!(
            out.streams[0].service,
            crate::protocols::service_name::UNKNOWN_SERVICE,
            "a scope attribute must not reach the stream's identity"
        );
    }

    /// **An entry whose structured metadata resolves to NOTHING must not trip
    /// the stream-level empty-label rejection #379 made reachable** (merge
    /// check). The two "empty" rules live at different seams — the builder
    /// deletes per ENTRY, `MissingLabelsErrorMsg` fires on the STREAM's label
    /// set — and only the second is a rejection.
    #[test]
    fn structured_metadata_resolving_to_nothing_does_not_empty_the_stream() {
        let mut request = logs_with_resource_attrs(vec![attr("service.name", "checkout")]);
        request.resource_logs[0].scope_logs[0].scope = Some(scope(
            "",
            "",
            vec![
                kv("a.b", Value::StringValue(String::new())),
                kv("a_b", Value::StringValue("keep".to_string())),
            ],
        ));
        let out = parse_off(&request, 0).unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        assert_eq!(out.rows.len(), 1, "the entry is still stored");
        assert_eq!(
            out.rows[0].structured_metadata, "",
            "the builder deletes both pairs, which is not a stream-level emptiness"
        );
        assert_eq!(out.streams[0].service, "checkout");
    }

    /// AC7 (issue #379): `container.name=""` is an index attribute AND a
    /// discovery name, so it writes `service_name=""` with no non-empty guard
    /// and suppresses the `unknown_service` fallback; both labels then strip
    /// away and the stream is refused as empty. Measured on stock
    /// `grafana/loki@sha256:87f0a067…`: `400 error at least one label pair is
    /// required per stream`.
    #[test]
    fn an_empty_index_attribute_value_empties_the_stream() {
        let out = parse_off(
            &logs_with_resource_attrs(vec![attr("container.name", "")]),
            0,
        )
        .unwrap();
        assert_eq!(
            only_stream_error(&out),
            "error at least one label pair is required per stream"
        );
        assert!(out.rows.is_empty(), "nothing is stored");
    }

    /// AC7's other direction, and the one that catches the likely defect: a
    /// resource with NO index attributes at all still validates, because the
    /// subset the bounds are charged on carries the synthesized slot. Charging
    /// them on the index attributes alone would newly refuse this, where the
    /// reference answers `204` (measured: `{zzz: "v"}` stores
    /// `{service_name="unknown_service"}` there, with `zzz` as structured
    /// metadata).
    #[test]
    fn a_resource_with_no_index_attributes_still_validates() {
        let out = parse_off(&logs_with_resource_attrs(vec![attr("zzz", "v")]), 0).unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        assert_eq!(
            out.streams[0].labels.to_canonical_json(),
            r#"{"service_name":"unknown_service","zzz":"v"}"#
        );
        assert_eq!(out.rows[0].service, "unknown_service");
    }

    /// The measured OTLP rows that are only visible in STORED state: wire
    /// order decides the slot, and an empty `service.name` after a discovery
    /// hit leaves a stream with no `service_name` at all — which is not a
    /// rejection, because `container_name` survives the strip.
    #[test]
    fn the_stored_otlp_stream_follows_the_slot_including_when_it_is_empty() {
        let stored = |attrs: Vec<KeyValue>| {
            let out = parse_off(&logs_with_resource_attrs(attrs), 0).unwrap();
            assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
            out.streams[0].labels.to_canonical_json()
        };
        assert_eq!(
            stored(vec![attr("container.name", "c4"), attr("service.name", "")]),
            r#"{"container_name":"c4"}"#,
            "measured: the reference stores this stream with no `service_name`"
        );
        assert_eq!(
            stored(vec![
                attr("k8s.container.name", "kc"),
                attr("container.name", "c")
            ]),
            r#"{"container_name":"c","k8s_container_name":"kc","service_name":"kc"}"#
        );
        assert_eq!(
            stored(vec![
                attr("container.name", "c2"),
                attr("k8s.container.name", "kc2")
            ]),
            r#"{"container_name":"c2","k8s_container_name":"kc2","service_name":"c2"}"#,
            "wire order decides, so the same two names answer differently"
        );
    }

    /// The same shape reaches the other two bounds, so the gap is not specific
    /// to the value length: a near-miss spelling carries an over-long label
    /// *name* into storage, and near-miss spellings that do not collide take
    /// the stored label count past `MAX_LABEL_NAMES_PER_STREAM`.
    #[test]
    fn near_miss_spellings_store_labels_past_the_name_and_count_bounds() {
        let long_name = format!("k8s.{}", "n".repeat(1025));
        let out = parse_off(
            &logs_with_resource_attrs(vec![attr("service.name", "ok"), attr(&long_name, "v")]),
            0,
        )
        .unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        let stored_name = long_name.replace('.', "_");
        assert!(
            out.streams[0].labels.get(&stored_name).is_some()
                && stored_name.len() > log_label_limits::MAX_LABEL_NAME_BYTES,
            "an indexed, stored label name longer than the bound"
        );

        // 15 raw index attributes pass the count bound; the 17 underscored
        // look-alikes are not counted, and 15 of them do not collide with the
        // 15 raw ones either — the stored set is 17 wide.
        let mut attrs = indexed(15);
        attrs.extend(INDEXED.iter().map(|k| attr(&k.replace('.', "_"), "v")));
        let out = parse_off(&logs_with_resource_attrs(attrs), 0).unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        assert!(
            out.streams[0].labels.len() > log_label_limits::MAX_LABEL_NAMES_PER_STREAM,
            "stored label count {} is past the bound",
            out.streams[0].labels.len()
        );
    }

    /// One label set per `ResourceLogs`, so a resource with several scopes
    /// reports its breach once, not once per scope — the reference builds one
    /// `logproto.Stream` per resource label set too.
    #[test]
    fn a_breach_is_reported_once_per_resource_not_once_per_scope() {
        let record = || LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("hello"),
            ..Default::default()
        };
        let req = request(vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![attr("k8s.pod.name", &"b".repeat(2049))],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![
                simple_scope_logs(vec![record()]),
                simple_scope_logs(vec![record()]),
                simple_scope_logs(vec![record()]),
            ],
            schema_url: String::new(),
        }]);
        let out = parse_off(&req, 0).unwrap();
        assert_eq!(out.stream_errors.len(), 1);
        assert!(out.rows.is_empty());
    }
}
