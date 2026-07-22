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
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use pulsus_model::{
    Date, Fingerprint, LabelSet, UnixNano, canonicalize_label_key, stream_fingerprint,
};

use crate::error::LogsIngestError;
use crate::protocols::loki_push::structured_metadata_json;

/// A `SeverityNumber` outside this range (including the `0`/unset default)
/// resolves to severity `0` (architect plan: `severity = severity_number`
/// if `1..=24` else `0`) — the valid `SeverityNumber` enum range per the
/// OTLP logs data model (`TRACE`=1 .. `FATAL4`=24).
const VALID_SEVERITY_RANGE: std::ops::RangeInclusive<i32> = 1..=24;

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
/// like a decode error; malformed per-record timestamps stay per-record
/// partial-success rejections inside the `Ok`.
pub fn parse(req: &ExportLogsServiceRequest, now_ns: i64) -> Result<ParsedLogs, LogsIngestError> {
    // Whole-request `AnyValue` recursion-depth guard (finding #54): reject a
    // maliciously deep body/attribute tree before any value is rendered or a
    // row materialized, so the recursive `any_value_to_string` render below
    // can never overflow the stack. This makes `parse` fallible (it was
    // previously infallible) — a whole-request, atomic 400/`code = 3` reject,
    // the same class as a decode failure.
    crate::protocols::otlp_depth::ensure_logs_anyvalue_depth(req)?;

    let mut out = ParsedLogs::default();
    // Dedups stream registration within this request by `(fingerprint,
    // month)` (architect plan amendment) — a fingerprint-only key would
    // suppress a needed monthly row for a cross-month/backfilled request.
    let mut seen_streams: HashSet<(Fingerprint, Date)> = HashSet::new();

    for resource_logs in &req.resource_logs {
        for scope_logs in &resource_logs.scope_logs {
            // Stream labels + fingerprint computed once per ScopeLogs
            // (resource attributes ONLY — scope is structured metadata, not
            // a stream label; issue #109), reused across every record in it —
            // never re-derived per record.
            let (labels, collisions) = build_stream_labels(resource_logs.resource.as_ref());
            out.collisions += collisions as u64;
            let fingerprint = stream_fingerprint(&labels);
            let service = labels.service().to_string();
            // The scope's per-entry structured metadata, computed once per
            // ScopeLogs and cloned onto every record it contains.
            let structured_metadata = build_scope_structured_metadata(scope_logs);

            for record in &scope_logs.log_records {
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

                out.rows.push(LogRow {
                    service: service.clone(),
                    fingerprint,
                    timestamp_ns: UnixNano(timestamp_ns),
                    severity: resolve_severity(record.severity_number),
                    body: any_value_to_string(record.body.as_ref()),
                    // The scope's structured metadata (issue #109), shared by
                    // every record in this ScopeLogs.
                    structured_metadata: structured_metadata.clone(),
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
fn build_stream_labels(resource: Option<&Resource>) -> (LabelSet, usize) {
    let resource_attrs = resource.map(|r| r.attributes.as_slice()).unwrap_or(&[]);
    LabelSet::from_normalized(attr_pairs(resource_attrs))
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
/// - **(c)** an empty-valued scope *attribute* is KEPT; only scope
///   *name*/*version* are empty-suppressed (#108).
///
/// The resolution is done explicitly HERE, before the [`structured_metadata_json`]
/// seam, because `from_normalized` mis-resolves (a)/(b). Keys are sanitized with
/// the same [`canonicalize_label_key`] primitive `from_normalized` uses, so the
/// post-resolution pairs are unique canonicalize fixed points — the seam then
/// re-canonicalizes idempotently, finds no collision, and only sorts +
/// JSON-encodes (byte-identical to the Loki-push SM representation).
fn build_scope_structured_metadata(scope_logs: &ScopeLogs) -> String {
    let Some(scope) = scope_logs.scope.as_ref() else {
        return String::new();
    };
    // Ordered (sanitized_key, value): attributes in wire order (no empty-value
    // filter — rule (c)), then identity appended last so it overwrites any
    // colliding attribute (rule (a)); each identity field empty-suppressed (#108).
    let mut ordered: Vec<(String, String)> = attr_pairs(&scope.attributes)
        .map(|(key, value)| (canonicalize_label_key(&key), value))
        .collect();
    if !scope.name.is_empty() {
        // `scope_name`/`scope_version` are already canonicalize fixed points.
        ordered.push(("scope_name".to_string(), scope.name.clone()));
    }
    if !scope.version.is_empty() {
        ordered.push(("scope_version".to_string(), scope.version.clone()));
    }
    // Last-write-wins per sanitized key (Loki's rule). Cardinality is tiny;
    // a linear replace-in-place over insertion order needs no new dependency.
    let mut resolved: Vec<(String, String)> = Vec::with_capacity(ordered.len());
    for (key, value) in ordered {
        if let Some(slot) = resolved.iter_mut().find(|(k, _)| *k == key) {
            slot.1 = value;
        } else {
            resolved.push((key, value));
        }
    }
    structured_metadata_json(resolved)
}

/// Renders a `KeyValue` list to `(key, value)` label pairs, using the same
/// `AnyValue -> String` rendering as a log record's body
/// ([`any_value_to_string`]) for the value side — label values are always
/// strings, so a non-string attribute (bool/int/double/array/kvlist/bytes)
/// renders the same way a non-string body would.
fn attr_pairs(attrs: &[KeyValue]) -> impl Iterator<Item = (String, String)> + '_ {
    attrs
        .iter()
        .map(|kv| (kv.key.clone(), any_value_to_string(kv.value.as_ref())))
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

    /// The `AnyValue` depth guard (finding #54) made `super::parse` fallible.
    /// Every legacy assertion below constructs shallow, in-bounds requests, so
    /// this shim unwraps the whole-request result to keep those cases reading
    /// against `ParsedLogs` unchanged; the dedicated depth tests call
    /// `super::parse` directly to observe the `Err`.
    fn parse(req: &ExportLogsServiceRequest, now_ns: i64) -> ParsedLogs {
        super::parse(req, now_ns).expect("test request is within the AnyValue depth cap")
    }

    fn kv(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
            key_strindex: 0,
        }
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

    #[test]
    fn parse_of_empty_request_returns_empty_output() {
        let out = parse(&request(vec![]), 1_000);
        assert_eq!(out, ParsedLogs::default());
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

    #[test]
    fn parse_service_is_empty_string_when_absent_not_unknown_service() {
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
        assert_eq!(out.rows[0].service, "");
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
    fn parse_keeps_empty_valued_scope_attribute_while_suppressing_empty_identity() {
        // Rule (c): an empty-valued scope ATTRIBUTE is retained, while empty
        // scope name/version stay suppressed — the deliberate asymmetry.
        let kept = scope_sm(Some(scope(
            "N",
            "1.0",
            vec![kv("emptyattr", Value::StringValue(String::new()))],
        )));
        assert_eq!(
            kept,
            r#"{"emptyattr":"","scope_name":"N","scope_version":"1.0"}"#
        );

        let empty_version = scope_sm(Some(scope(
            "N",
            "",
            vec![kv("emptyattr", Value::StringValue(String::new()))],
        )));
        // `scope_version` absent (suppressed), `emptyattr` present (kept).
        assert_eq!(empty_version, r#"{"emptyattr":"","scope_name":"N"}"#);
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
        let out = super::parse(&req, 0).expect("at-cap body is within the depth guard");
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
        let err = super::parse(&req, 0).expect_err("over-depth body is rejected whole-request");
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
        let err = super::parse(&req, 0).expect_err("over-depth resource attribute is rejected");
        assert!(matches!(err, LogsIngestError::OversizeMessage { .. }));
    }
}
