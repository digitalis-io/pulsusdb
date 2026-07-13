//! OTLP logs parser (issue #8 architect plan, docs/architecture.md §4): a
//! pure `bytes -> ExportLogsServiceRequest -> ParsedLogs` pipeline with no
//! I/O. Resource + scope attributes flatten through the frozen canonical
//! label model (`pulsus_model::LabelSet::from_normalized` ->
//! `stream_fingerprint`, issue #4) — fingerprints and the `service` column
//! derive *only* via `pulsus-model`, never re-derived here.

use std::collections::HashSet;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use pulsus_model::{Date, Fingerprint, LabelSet, UnixNano, stream_fingerprint};

use crate::error::LogsIngestError;

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
    Ok(ExportLogsServiceRequest::decode(body)?)
}

/// Parses a decoded `ExportLogsServiceRequest` into normalized rows. Pure:
/// a function of `req` and `now_ns` only, no I/O, no clock reads — the
/// caller (the ingest handler) is the only clock/IO boundary, so `parse`
/// itself is trivially unit-testable and deterministic across calls with
/// identical arguments.
pub fn parse(req: &ExportLogsServiceRequest, now_ns: i64) -> ParsedLogs {
    let mut out = ParsedLogs::default();
    // Dedups stream registration within this request by `(fingerprint,
    // month)` (architect plan amendment) — a fingerprint-only key would
    // suppress a needed monthly row for a cross-month/backfilled request.
    let mut seen_streams: HashSet<(Fingerprint, Date)> = HashSet::new();

    for resource_logs in &req.resource_logs {
        for scope_logs in &resource_logs.scope_logs {
            // Label set + fingerprint computed once per ScopeLogs
            // (resource ⊕ scope), reused across every record in it —
            // never re-derived per record.
            let (labels, collisions) =
                build_scope_labels(resource_logs.resource.as_ref(), scope_logs);
            out.collisions += collisions as u64;
            let fingerprint = stream_fingerprint(&labels);
            let service = labels.service().to_string();

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

                let month = Date::start_of_month_utc(timestamp_ns);
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
                });
            }
        }
    }

    out
}

/// Flattens `resource.attributes ⊕ otel_scope_name/version ⊕
/// scope.attributes` as ONE iterator into
/// [`LabelSet::from_normalized`] — no source precedence override, a
/// collision between e.g. a resource attribute and a scope attribute
/// resolves by `from_normalized`'s frozen deterministic rule (issue #4)
/// and is counted, never swapped. `otel_scope_name`/`otel_scope_version`
/// are emitted only when `scope_logs.scope` is present (task-manager
/// resolution: "absent scopes emit nothing").
fn build_scope_labels(resource: Option<&Resource>, scope_logs: &ScopeLogs) -> (LabelSet, usize) {
    let resource_attrs = resource.map(|r| r.attributes.as_slice()).unwrap_or(&[]);
    let scope = scope_logs.scope.as_ref();
    let scope_identity = scope.into_iter().flat_map(|s| {
        [
            ("otel_scope_name".to_string(), s.name.clone()),
            ("otel_scope_version".to_string(), s.version.clone()),
        ]
    });
    let scope_attrs = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);

    let pairs = attr_pairs(resource_attrs)
        .chain(scope_identity)
        .chain(attr_pairs(scope_attrs));
    LabelSet::from_normalized(pairs)
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

    #[test]
    fn parse_emits_scope_identity_labels_when_scope_is_present() {
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
            out.streams[0].labels.get("otel_scope_name"),
            Some("my-scope")
        );
        assert_eq!(
            out.streams[0].labels.get("otel_scope_version"),
            Some("1.0.0")
        );
    }

    #[test]
    fn parse_emits_no_scope_labels_when_scope_is_absent() {
        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            body: string_body("x"),
            ..Default::default()
        };
        let scope_logs = ScopeLogs {
            scope: None,
            log_records: vec![record],
            schema_url: String::new(),
        };
        let out = parse(
            &request(vec![ResourceLogs {
                resource: None,
                scope_logs: vec![scope_logs],
                schema_url: String::new(),
            }]),
            0,
        );
        assert_eq!(out.streams[0].labels.get("otel_scope_name"), None);
        assert_eq!(out.streams[0].labels.get("otel_scope_version"), None);
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
    fn parse_counts_resource_scope_label_collisions() {
        let resource = Resource {
            attributes: vec![kv("env", Value::StringValue("from_resource".to_string()))],
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
                attributes: vec![kv("env", Value::StringValue("from_scope".to_string()))],
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
        let now_month_days = Date::start_of_month_utc(now_ns).days_since_epoch();
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
}
