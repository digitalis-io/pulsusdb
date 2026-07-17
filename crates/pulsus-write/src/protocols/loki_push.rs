//! Loki push receiver parser (issue #77 architect plan, docs/api.md §8.2): a
//! pure `bytes -> PushRequest -> ParsedLogs` pipeline with no I/O — the
//! structural analog of [`crate::protocols::remote_write`], but feeding the
//! **log** storage path. A pushed stream's label set flattens through the
//! *identical* frozen canonical model the OTLP logs path uses
//! (`pulsus_model::LabelSet::from_normalized` -> `stream_fingerprint`), so a
//! stream pushed here fingerprints byte-for-byte the same as the same
//! logical stream ingested via `otlp_logs::parse` — the load-bearing
//! correctness gate (AC-3): pushed logs are queryable via LogQL (#72/#73)
//! and appear in tail (#74) with no read-path change.
//!
//! ## Wire types: hand-rolled `logproto` prost structs
//!
//! The message set below is grafana/loki **3.4.2**'s `pkg/push/push.proto`
//! (the digest-pinned differential oracle, docs/benchmarks/
//! logs-differential-ledger.md:7), hand-rolled as `#[derive(::prost::
//! Message)]` structs at their exact field tags — the same no-protoc/no-
//! build-dep approach as [`crate::protocols::remote_write`] and the hand-
//! rolled `google.rpc.Status` in `ingest/http.rs`.
//!
//! Two wire fields are **intentionally undeclared** — `prost` silently skips
//! unknown fields on decode (the remote-write exemplars/native-histogram
//! precedent, `remote_write.rs:16-20`), so an undeclared field is never
//! materialized, never allocated:
//!
//! - `StreamAdapter` tag 3 (`uint64 hash`) — an intra-Loki routing hash, of
//!   no interest to a receiver.
//! - `EntryAdapter` tag 3 (`repeated LabelPairAdapter structuredMetadata`) —
//!   structured metadata (issue #77 delta 1 / task-manager Q1 adjudication:
//!   accept-and-drop, `log_samples` has no per-entry column and no LogQL read
//!   path surfaces it; a follow-up is filed for the schema column). Not
//!   declaring the tag is the *skip-without-allocate* implementation: the
//!   field costs only prost's byte-skip, never a `Vec<LabelPairAdapter>`
//!   materialized-then-dropped.
//!
//! Tag layout is cross-checked against a real capture from the
//! OpenTelemetry Collector's `loki` exporter (`tests/fixtures/loki-push/
//! README.md`) — a self-consistent wrong tag would decode without error but
//! silently corrupt every following field, which only a real-wire fixture
//! (not a synthetic round-trip through the same structs) can catch.

use std::collections::HashSet;

use prost::Message;
use pulsus_model::{Date, Fingerprint, LabelSet, UnixNano, stream_fingerprint};

use crate::error::LogsIngestError;
use crate::protocols::otlp_logs::{LogRow, ParsedLogs, StreamRow};

/// `logproto.PushRequest`: `streams` at tag 1.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    pub streams: Vec<StreamAdapter>,
}

/// `logproto.StreamAdapter`: `labels` (a Prometheus label-set literal
/// `{k="v",...}`) at tag 1, `entries` at tag 2. Tag 3 (`uint64 hash`) is
/// intentionally undeclared — see this module's doc comment.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamAdapter {
    #[prost(string, tag = "1")]
    pub labels: String,
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<EntryAdapter>,
}

/// `logproto.EntryAdapter`: `timestamp` (`google.protobuf.Timestamp`) at tag
/// 1, `line` at tag 2. Tag 3 (`repeated LabelPairAdapter
/// structuredMetadata`) is intentionally undeclared — the skip-without-
/// allocate accept-and-drop of structured metadata (this module's doc
/// comment).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntryAdapter {
    #[prost(message, optional, tag = "1")]
    pub timestamp: Option<Timestamp>,
    #[prost(string, tag = "2")]
    pub line: String,
}

/// `google.protobuf.Timestamp`: `seconds` at tag 1, `nanos` at tag 2.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}

/// Decode-time structural DoS guards — siblings of [`crate::protocols::
/// remote_write`]'s `MAX_*` family, same rationale/values: generous, per-
/// request bounds no legitimate push ever approaches, checked immediately
/// after decode (before any per-element allocation) so a body within the
/// 64 MiB decompressed cap cannot unpack into a far larger in-memory
/// structure via many minimal-length repeated submessages.
pub const MAX_STREAMS_PER_REQUEST: usize = 1_000_000;
/// See [`MAX_STREAMS_PER_REQUEST`].
pub const MAX_ENTRIES_PER_STREAM: usize = 100_000;
/// See [`MAX_STREAMS_PER_REQUEST`]. Bounds the label count parsed out of one
/// stream's label-set literal (protobuf) or JSON `stream` map, checked
/// before the label `Vec` is handed to `LabelSet::from_normalized`.
pub const MAX_LABELS_PER_STREAM: usize = 256;
/// The **aggregate** entry budget across all streams (issue #77 delta 1,
/// review [high] finding): the per-dimension product
/// `MAX_STREAMS_PER_REQUEST × MAX_ENTRIES_PER_STREAM` (1M × 100k) far
/// exceeds anything a 64 MiB body can encode, so it does not bound the
/// materialized `Vec<LogRow>`. This aggregate sum, charged at the
/// `decode -> validate_bounds -> parse` seam (before `parse` allocates any
/// row), bounds that second amplification. Total *line bytes* need no
/// separate budget: Σ line lengths ≤ the decompressed body ≤ 64 MiB by
/// construction.
pub const MAX_TOTAL_ENTRIES_PER_REQUEST: usize = 5_000_000;

/// Decodes a (decompressed) snappy-protobuf `POST /loki/api/v1/push` body,
/// then applies the [`MAX_STREAMS_PER_REQUEST`]-family structural bounds.
/// The sole decode boundary: a malformed/truncated protobuf, or a message
/// exceeding one of those bounds, is a whole-request atomic failure — Loki
/// has no partial-success channel (all-or-nothing), so this never partially
/// applies.
pub fn decode_protobuf(body: &[u8]) -> Result<PushRequest, LogsIngestError> {
    let req = PushRequest::decode(body)?;
    validate_bounds(
        req.streams.len(),
        req.streams.iter().map(|s| s.entries.len()),
    )?;
    Ok(req)
}

/// Enforces the [`MAX_STREAMS_PER_REQUEST`]-family bounds over a request's
/// stream count and per-stream entry counts (message-level fields before
/// the aggregate, so an over-count of streams is rejected before summing
/// entries), failing fast on the first breach. Shared verbatim by the
/// protobuf ([`decode_protobuf`]) and JSON ([`parse_json`]) paths so the
/// same aggregate `Vec<LogRow>` amplification is bounded identically before
/// either materializes a row.
fn validate_bounds(
    num_streams: usize,
    entries_per_stream: impl Iterator<Item = usize>,
) -> Result<(), LogsIngestError> {
    if num_streams > MAX_STREAMS_PER_REQUEST {
        return Err(LogsIngestError::OversizeMessage {
            field: "streams",
            limit: MAX_STREAMS_PER_REQUEST,
            actual: num_streams,
        });
    }
    let mut total = 0usize;
    for count in entries_per_stream {
        if count > MAX_ENTRIES_PER_STREAM {
            return Err(LogsIngestError::OversizeMessage {
                field: "entries",
                limit: MAX_ENTRIES_PER_STREAM,
                actual: count,
            });
        }
        total = total.saturating_add(count);
    }
    if total > MAX_TOTAL_ENTRIES_PER_REQUEST {
        return Err(LogsIngestError::OversizeMessage {
            field: "total_entries",
            limit: MAX_TOTAL_ENTRIES_PER_REQUEST,
            actual: total,
        });
    }
    Ok(())
}

/// Parses a decoded [`PushRequest`] into normalized rows. Pure: a function
/// of `req` and `now_ns` only, no I/O, no clock reads (the caller is the
/// only clock boundary). Fallible only on a per-entry timestamp overflow —
/// which, unlike OTLP's per-record partial-success drop, is a whole-request
/// `LokiDecode` failure here (Loki is all-or-nothing).
pub fn parse_protobuf(req: &PushRequest, now_ns: i64) -> Result<ParsedLogs, LogsIngestError> {
    let mut out = ParsedLogs::default();
    let mut seen_streams: HashSet<(Fingerprint, Date)> = HashSet::new();
    for stream in &req.streams {
        let (labels, collisions) = parse_label_set(&stream.labels)?;
        let entries = stream.entries.iter().map(|entry| {
            let timestamp_ns = match entry.timestamp.as_ref() {
                Some(ts) => resolve_pb_timestamp(ts)?,
                None => now_ns,
            };
            Ok((timestamp_ns, entry.line.clone()))
        });
        append_stream(
            &mut out,
            &mut seen_streams,
            labels,
            collisions,
            entries,
            now_ns,
        )?;
    }
    Ok(out)
}

/// Parses a Loki JSON push body (`{"streams":[{"stream":{...},"values":[[ts,
/// line],...]}]}`) into normalized rows — the JSON analog of
/// [`parse_protobuf`], funneling through the same [`append_stream`] seam so
/// a JSON stream and its equivalent protobuf stream produce byte-identical
/// `ParsedLogs`. Each `values` entry deserializes as exactly `(ts, line)`;
/// any trailing array element (the optional structured-metadata object) is
/// discarded without being materialized ([`JsonEntry`]'s `Deserialize`).
pub fn parse_json(body: &[u8], now_ns: i64) -> Result<ParsedLogs, LogsIngestError> {
    let push: JsonPush =
        serde_json::from_slice(body).map_err(|e| LogsIngestError::LokiDecode(e.to_string()))?;
    // Aggregate-budget charge at the same seam as the protobuf path, before
    // any `LogRow` is materialized (issue #77 delta 1).
    validate_bounds(
        push.streams.len(),
        push.streams.iter().map(|s| s.values.len()),
    )?;

    let mut out = ParsedLogs::default();
    let mut seen_streams: HashSet<(Fingerprint, Date)> = HashSet::new();
    for stream in &push.streams {
        if stream.stream.len() > MAX_LABELS_PER_STREAM {
            return Err(LogsIngestError::OversizeMessage {
                field: "labels",
                limit: MAX_LABELS_PER_STREAM,
                actual: stream.stream.len(),
            });
        }
        let pairs = stream
            .stream
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        let (labels, collisions) = LabelSet::from_normalized(pairs);
        let entries = stream.values.iter().map(|entry| {
            let timestamp_ns = entry.timestamp.parse::<i64>().map_err(|_| {
                LogsIngestError::LokiDecode(format!(
                    "log entry timestamp {:?} is not a base-10 nanosecond integer",
                    entry.timestamp
                ))
            })?;
            Ok((timestamp_ns, entry.line.clone()))
        });
        append_stream(
            &mut out,
            &mut seen_streams,
            labels,
            collisions,
            entries,
            now_ns,
        )?;
    }
    Ok(out)
}

/// The one seam both `parse_*` funnel through — mirrors `otlp_logs::parse`
/// exactly: `stream_fingerprint` computed **once per stream** and reused
/// across every entry (never per-row), `StreamRow` deduped by `(fingerprint,
/// month)`, one [`LogRow`] per entry (`severity = 0`, `service =
/// labels.service()`, `body = line`), collisions accumulated. `entries`
/// yields `(timestamp_ns, line)` fallibly (a per-entry timestamp overflow
/// aborts the whole request).
fn append_stream(
    out: &mut ParsedLogs,
    seen_streams: &mut HashSet<(Fingerprint, Date)>,
    labels: LabelSet,
    collisions: usize,
    entries: impl Iterator<Item = Result<(i64, String), LogsIngestError>>,
    now_ns: i64,
) -> Result<(), LogsIngestError> {
    out.collisions += collisions as u64;
    let fingerprint = stream_fingerprint(&labels);
    let service = labels.service().to_string();
    for entry in entries {
        let (timestamp_ns, line) = entry?;
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
            severity: 0,
            body: line,
        });
    }
    Ok(())
}

/// `seconds * 1e9 + nanos`, checked — an overflow of the representable i64
/// nanosecond range is a whole-request `LokiDecode` failure (timestamps are
/// stored verbatim, never clamped).
///
/// `nanos` is first range-validated to the `google.protobuf.Timestamp`
/// contract's `[0, 1_000_000_000)` window. An out-of-range `nanos` (e.g. a
/// negative value, or one ≥ 1e9) would otherwise fold silently into a
/// *different* wall-clock instant than the wire encoded — a corrupt
/// timestamp masquerading as valid. Reject-don't-corrupt: an out-of-range
/// `nanos` is a whole-request `LokiDecode` failure (a 400), never a silently
/// normalized stamp.
fn resolve_pb_timestamp(ts: &Timestamp) -> Result<i64, LogsIngestError> {
    if !(0..1_000_000_000).contains(&ts.nanos) {
        return Err(LogsIngestError::LokiDecode(format!(
            "log entry timestamp nanos={} is outside the google.protobuf.Timestamp range \
             [0, 1_000_000_000)",
            ts.nanos
        )));
    }
    ts.seconds
        .checked_mul(1_000_000_000)
        .and_then(|s| s.checked_add(i64::from(ts.nanos)))
        .ok_or_else(|| {
            LogsIngestError::LokiDecode(format!(
                "log entry timestamp seconds={} nanos={} overflows the representable i64 \
                 nanosecond range",
                ts.seconds, ts.nanos
            ))
        })
}

// ---------------------------------------------------------------------
// Prometheus label-set literal parser (protobuf `labels` field)
// ---------------------------------------------------------------------

/// Parses a Loki `StreamAdapter.labels` string — a Prometheus label-set
/// literal `{key="value", key2="value2"}` — into a [`LabelSet`] via the
/// same `LabelSet::from_normalized` seam every other path uses. Rejects a
/// missing/unbalanced brace, a missing `=`, an unterminated/ malformed
/// quoted value, or more than [`MAX_LABELS_PER_STREAM`] pairs as
/// [`LogsIngestError::LokiDecode`] (a whole-request 400). Prometheus value
/// escaping (`\\`, `\"`, `\n`, `\t`, `\r`) is unescaped; the empty set `{}`
/// yields an empty `LabelSet`.
fn parse_label_set(input: &str) -> Result<(LabelSet, usize), LogsIngestError> {
    let trimmed = input.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| {
            LogsIngestError::LokiDecode(format!(
                "stream labels {input:?} are not a brace-enclosed Prometheus label set"
            ))
        })?;

    let mut pairs: Vec<(String, String)> = Vec::new();
    let bytes = inner.as_bytes();
    let mut i = 0usize;
    skip_ws(bytes, &mut i);
    if i >= bytes.len() {
        // Empty set `{}` (or `{  }`).
        return Ok(LabelSet::from_normalized(pairs));
    }
    loop {
        if pairs.len() >= MAX_LABELS_PER_STREAM {
            return Err(LogsIngestError::OversizeMessage {
                field: "labels",
                limit: MAX_LABELS_PER_STREAM,
                actual: pairs.len() + 1,
            });
        }
        let key = read_key(bytes, &mut i, input)?;
        skip_ws(bytes, &mut i);
        expect_byte(bytes, &mut i, b'=', input)?;
        skip_ws(bytes, &mut i);
        let value = read_quoted(bytes, &mut i, input)?;
        pairs.push((key, value));
        skip_ws(bytes, &mut i);
        if i >= bytes.len() {
            break;
        }
        expect_byte(bytes, &mut i, b',', input)?;
        skip_ws(bytes, &mut i);
        // A trailing comma before `}` (`{a="b",}`) is tolerated.
        if i >= bytes.len() {
            break;
        }
    }
    Ok(LabelSet::from_normalized(pairs))
}

fn skip_ws(bytes: &[u8], i: &mut usize) {
    while *i < bytes.len() && bytes[*i].is_ascii_whitespace() {
        *i += 1;
    }
}

fn expect_byte(bytes: &[u8], i: &mut usize, want: u8, input: &str) -> Result<(), LogsIngestError> {
    if *i < bytes.len() && bytes[*i] == want {
        *i += 1;
        Ok(())
    } else {
        Err(LogsIngestError::LokiDecode(format!(
            "stream labels {input:?}: expected {:?} at byte {i}",
            want as char
        )))
    }
}

/// Reads a Prometheus label name: `[a-zA-Z_][a-zA-Z0-9_]*`. `from_normalized`
/// canonicalizes it afterward regardless, but a genuinely empty/absent key
/// is a malformed literal.
fn read_key(bytes: &[u8], i: &mut usize, input: &str) -> Result<String, LogsIngestError> {
    let start = *i;
    while *i < bytes.len() {
        let b = bytes[*i];
        if b == b'=' || b == b',' || b.is_ascii_whitespace() {
            break;
        }
        *i += 1;
    }
    if *i == start {
        return Err(LogsIngestError::LokiDecode(format!(
            "stream labels {input:?}: empty label name at byte {start}"
        )));
    }
    // `inner` is a `&str` slice of `input`; a key byte range never splits a
    // UTF-8 codepoint (the delimiters are all ASCII), so this is safe UTF-8.
    Ok(String::from_utf8_lossy(&bytes[start..*i]).into_owned())
}

/// Reads a double-quoted, Prometheus-escaped value starting at `bytes[*i]`
/// (which must be `"`), consuming through the closing quote. Rejects an
/// unterminated quote or a dangling escape.
fn read_quoted(bytes: &[u8], i: &mut usize, input: &str) -> Result<String, LogsIngestError> {
    expect_byte(bytes, i, b'"', input)?;
    let mut value: Vec<u8> = Vec::new();
    loop {
        let Some(&b) = bytes.get(*i) else {
            return Err(LogsIngestError::LokiDecode(format!(
                "stream labels {input:?}: unterminated quoted value"
            )));
        };
        *i += 1;
        match b {
            b'"' => break,
            b'\\' => {
                let Some(&esc) = bytes.get(*i) else {
                    return Err(LogsIngestError::LokiDecode(format!(
                        "stream labels {input:?}: dangling escape at end of value"
                    )));
                };
                *i += 1;
                match esc {
                    b'n' => value.push(b'\n'),
                    b't' => value.push(b'\t'),
                    b'r' => value.push(b'\r'),
                    b'\\' => value.push(b'\\'),
                    b'"' => value.push(b'"'),
                    other => {
                        // Unknown escape: keep the byte verbatim (lenient,
                        // matching Loki's tolerant label parsing).
                        value.push(other);
                    }
                }
            }
            other => value.push(other),
        }
    }
    Ok(String::from_utf8_lossy(&value).into_owned())
}

// ---------------------------------------------------------------------
// JSON body deserialization
// ---------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct JsonPush {
    #[serde(default)]
    streams: Vec<JsonStream>,
}

#[derive(serde::Deserialize)]
struct JsonStream {
    #[serde(default)]
    stream: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    values: Vec<JsonEntry>,
}

/// One `values` array entry: `["<unix_nano_string>", "<line>"]`. A trailing
/// third element (structured metadata) is **skipped without allocating**
/// (issue #77 delta 1) — `serde::de::IgnoredAny` discards any remaining
/// sequence elements rather than deserializing them into an owned value.
struct JsonEntry {
    timestamp: String,
    line: String,
}

impl<'de> serde::Deserialize<'de> for JsonEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct EntryVisitor;
        impl<'de> serde::de::Visitor<'de> for EntryVisitor {
            type Value = JsonEntry;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a [timestamp, line] Loki log entry array")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<JsonEntry, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let timestamp: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let line: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                // Drain any trailing element (structured metadata) without
                // materializing it.
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(JsonEntry { timestamp, line })
            }
        }
        deserializer.deserialize_seq(EntryVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(seconds: i64, nanos: i32) -> Timestamp {
        Timestamp { seconds, nanos }
    }

    fn entry(seconds: i64, nanos: i32, line: &str) -> EntryAdapter {
        EntryAdapter {
            timestamp: Some(ts(seconds, nanos)),
            line: line.to_string(),
        }
    }

    // -- label-set literal parser -----------------------------------------

    #[test]
    fn parses_a_basic_label_set() {
        let (labels, collisions) =
            parse_label_set(r#"{service_name="checkout", env="prod"}"#).unwrap();
        assert_eq!(collisions, 0);
        assert_eq!(labels.get("service_name"), Some("checkout"));
        assert_eq!(labels.get("env"), Some("prod"));
        assert_eq!(labels.service(), "checkout");
    }

    #[test]
    fn parses_the_empty_label_set() {
        let (labels, collisions) = parse_label_set("{}").unwrap();
        assert_eq!(collisions, 0);
        assert!(labels.is_empty());
    }

    #[test]
    fn parses_escaped_values() {
        let (labels, _) = parse_label_set(r#"{msg="a\"b\\c\nd"}"#).unwrap();
        assert_eq!(labels.get("msg"), Some("a\"b\\c\nd"));
    }

    #[test]
    fn tolerates_a_trailing_comma() {
        let (labels, _) = parse_label_set(r#"{a="1",}"#).unwrap();
        assert_eq!(labels.get("a"), Some("1"));
    }

    #[test]
    fn rejects_a_missing_brace() {
        let err = parse_label_set(r#"service_name="checkout""#).unwrap_err();
        assert!(matches!(err, LogsIngestError::LokiDecode(_)));
    }

    #[test]
    fn rejects_an_unterminated_quote() {
        let err = parse_label_set(r#"{a="unterminated}"#).unwrap_err();
        assert!(matches!(err, LogsIngestError::LokiDecode(_)));
    }

    #[test]
    fn rejects_a_missing_equals() {
        let err = parse_label_set(r#"{a"b"}"#).unwrap_err();
        assert!(matches!(err, LogsIngestError::LokiDecode(_)));
    }

    #[test]
    fn dotted_key_canonicalizes_like_every_other_path() {
        // A Loki label name is normally already dot-free, but the canonical
        // seam is the same one OTLP uses.
        let (labels, _) = parse_label_set(r#"{service_name="checkout"}"#).unwrap();
        assert_eq!(labels.get("service_name"), Some("checkout"));
    }

    // -- structural bounds -------------------------------------------------

    #[test]
    fn decode_rejects_malformed_bytes() {
        let err = decode_protobuf(b"\xFF\xFF\xFF not a protobuf message").unwrap_err();
        assert!(matches!(err, LogsIngestError::Decode(_)));
    }

    #[test]
    fn decode_round_trips_an_encoded_request() {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{service_name="checkout"}"#.to_string(),
                entries: vec![entry(1, 0, "hello")],
            }],
        };
        let bytes = req.encode_to_vec();
        assert_eq!(decode_protobuf(&bytes).unwrap(), req);
    }

    #[test]
    fn validate_bounds_rejects_too_many_streams() {
        let err = validate_bounds(MAX_STREAMS_PER_REQUEST + 1, std::iter::empty()).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "streams",
                ..
            }
        ));
    }

    #[test]
    fn validate_bounds_rejects_too_many_entries_in_one_stream() {
        let err = validate_bounds(1, std::iter::once(MAX_ENTRIES_PER_STREAM + 1)).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "entries",
                ..
            }
        ));
    }

    #[test]
    fn validate_bounds_rejects_too_many_total_entries_across_streams() {
        // Each stream is within MAX_ENTRIES_PER_STREAM, but the aggregate
        // exceeds MAX_TOTAL_ENTRIES_PER_REQUEST — the second-amplification
        // budget the per-dimension bounds cannot catch (delta 1).
        let per = MAX_ENTRIES_PER_STREAM; // 100_000
        let streams = MAX_TOTAL_ENTRIES_PER_REQUEST / per + 1; // 51 streams -> 5.1M
        let err = validate_bounds(streams, std::iter::repeat_n(per, streams)).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "total_entries",
                ..
            }
        ));
    }

    // -- parse -------------------------------------------------------------

    #[test]
    fn parse_protobuf_emits_one_row_per_entry_and_one_stream_row() {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{service_name="checkout"}"#.to_string(),
                entries: vec![entry(1_700_000_000, 0, "a"), entry(1_700_000_001, 0, "b")],
            }],
        };
        let out = parse_protobuf(&req, 0).unwrap();
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.streams.len(), 1);
        assert_eq!(out.rows[0].body, "a");
        assert_eq!(out.rows[0].service, "checkout");
        assert_eq!(out.rows[0].severity, 0);
        assert_eq!(out.rows[0].timestamp_ns.0, 1_700_000_000_000_000_000);
    }

    #[test]
    fn parse_protobuf_missing_timestamp_falls_back_to_now_ns() {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{a="b"}"#.to_string(),
                entries: vec![EntryAdapter {
                    timestamp: None,
                    line: "x".to_string(),
                }],
            }],
        };
        let out = parse_protobuf(&req, 999).unwrap();
        assert_eq!(out.rows[0].timestamp_ns.0, 999);
    }

    #[test]
    fn parse_protobuf_timestamp_overflow_is_a_whole_request_error() {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{a="b"}"#.to_string(),
                entries: vec![entry(i64::MAX, 0, "x")],
            }],
        };
        let err = parse_protobuf(&req, 0).unwrap_err();
        assert!(matches!(err, LogsIngestError::LokiDecode(_)));
    }

    #[test]
    fn parse_protobuf_out_of_range_nanos_is_a_whole_request_error() {
        // `nanos` outside `[0, 1_000_000_000)` must reject the whole request
        // (a 400), never silently normalize into a different instant.
        for bad_nanos in [1_000_000_000, i32::MAX, -1, i32::MIN] {
            let req = PushRequest {
                streams: vec![StreamAdapter {
                    labels: r#"{a="b"}"#.to_string(),
                    entries: vec![entry(1_700_000_000, bad_nanos, "x")],
                }],
            };
            let err = parse_protobuf(&req, 0).unwrap_err();
            assert!(
                matches!(err, LogsIngestError::LokiDecode(_)),
                "nanos={bad_nanos} must be a LokiDecode error, got {err:?}"
            );
        }
    }

    #[test]
    fn parse_protobuf_boundary_nanos_are_accepted() {
        // The inclusive lower / exclusive upper bounds: 0 is valid,
        // 999_999_999 is the largest valid nanos.
        for good_nanos in [0, 999_999_999] {
            let req = PushRequest {
                streams: vec![StreamAdapter {
                    labels: r#"{a="b"}"#.to_string(),
                    entries: vec![entry(1_700_000_000, good_nanos, "x")],
                }],
            };
            let out = parse_protobuf(&req, 0).unwrap();
            assert_eq!(
                out.rows[0].timestamp_ns.0,
                1_700_000_000_000_000_000 + i64::from(good_nanos)
            );
        }
    }

    #[test]
    fn parse_protobuf_bad_label_string_is_a_whole_request_error() {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: "not a label set".to_string(),
                entries: vec![entry(1, 0, "x")],
            }],
        };
        let err = parse_protobuf(&req, 0).unwrap_err();
        assert!(matches!(err, LogsIngestError::LokiDecode(_)));
    }

    #[test]
    fn parse_protobuf_is_pure() {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{service_name="checkout",env="prod"}"#.to_string(),
                entries: vec![entry(1_700_000_000, 0, "a")],
            }],
        };
        assert_eq!(
            parse_protobuf(&req, 42).unwrap(),
            parse_protobuf(&req, 42).unwrap()
        );
    }

    // -- JSON --------------------------------------------------------------

    #[test]
    fn parse_json_basic() {
        let body = br#"{"streams":[{"stream":{"service_name":"checkout"},
            "values":[["1700000000000000000","hello"],["1700000001000000000","world"]]}]}"#;
        let out = parse_json(body, 0).unwrap();
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.streams.len(), 1);
        assert_eq!(out.rows[0].body, "hello");
        assert_eq!(out.rows[0].service, "checkout");
        assert_eq!(out.rows[0].timestamp_ns.0, 1_700_000_000_000_000_000);
    }

    #[test]
    fn parse_json_ignores_trailing_structured_metadata_without_erroring() {
        // A 3-element values entry: ts, line, metadata object — the third
        // element is accepted and dropped (never materialized).
        let body = br#"{"streams":[{"stream":{"a":"b"},
            "values":[["1700000000000000000","hello",{"trace_id":"abc"}]]}]}"#;
        let out = parse_json(body, 0).unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].body, "hello");
    }

    #[test]
    fn parse_json_bad_timestamp_is_a_whole_request_error() {
        let body = br#"{"streams":[{"stream":{"a":"b"},
            "values":[["not-a-number","hello"]]}]}"#;
        let err = parse_json(body, 0).unwrap_err();
        assert!(matches!(err, LogsIngestError::LokiDecode(_)));
    }

    #[test]
    fn parse_json_malformed_is_a_whole_request_error() {
        let err = parse_json(b"{not json", 0).unwrap_err();
        assert!(matches!(err, LogsIngestError::LokiDecode(_)));
    }

    // -- dual-encoding equivalence (AC-1) ---------------------------------

    #[test]
    fn json_and_protobuf_bodies_parse_to_byte_identical_parsed_logs() {
        let json = br#"{"streams":[{"stream":{"service_name":"checkout","env":"prod"},
            "values":[["1700000000000000000","line one"],["1700000001000000000","line two"]]}]}"#;
        let proto = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{service_name="checkout", env="prod"}"#.to_string(),
                entries: vec![
                    entry(1_700_000_000, 0, "line one"),
                    entry(1_700_000_001, 0, "line two"),
                ],
            }],
        };
        let from_json = parse_json(json, 7).unwrap();
        let from_proto = parse_protobuf(&proto, 7).unwrap();
        assert_eq!(from_json, from_proto);
    }
}
