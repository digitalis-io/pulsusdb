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
//! One wire field is **intentionally undeclared** — `prost` silently skips
//! unknown fields on decode (the remote-write exemplars/native-histogram
//! precedent, `remote_write.rs:16-20`), so an undeclared field is never
//! materialized, never allocated:
//!
//! - `StreamAdapter` tag 3 (`uint64 hash`) — an intra-Loki routing hash, of
//!   no interest to a receiver.
//!
//! `EntryAdapter` tag 3 (`repeated LabelPairAdapter structuredMetadata`) is
//! **declared and decoded** (issue #97): per-entry structured metadata is now
//! stored in `log_samples.structured_metadata` (a canonical JSON String) and
//! surfaced in the LogQL read/tail label set. Two per-entry bounds guard it:
//! a cardinality bound ([`MAX_STRUCTURED_METADATA_PER_ENTRY`]) enforced
//! **during decode** by `EntryAdapter`'s hand-written [`prost::Message`] impl
//! (which caps tag-3 materialization at `MAX + 1` and drains the rest without
//! allocating — charge-before-allocate), and a total byte budget
//! ([`MAX_STRUCTURED_METADATA_BYTES_PER_ENTRY`]) charged on borrowed data
//! before any clone / canonical-JSON construction. Structured metadata is
//! per-ENTRY and never enters `stream_fingerprint` / `StreamRow`.
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
/// 1, `line` at tag 2, `structuredMetadata` (`repeated LabelPairAdapter`) at
/// tag 3 (issue #97 — decoded into `log_samples.structured_metadata`).
///
/// Unlike its sibling adapters, `EntryAdapter` does **not** derive
/// `::prost::Message`; it carries a hand-written [`prost::Message`] impl (below)
/// so tag-3 (`structured_metadata`) materialization is capped **inside the
/// decoder** at [`MAX_STRUCTURED_METADATA_PER_ENTRY`]` + 1` (issue #97): a
/// derived impl fully materializes the wire `Vec` before any cardinality check
/// runs, so an attacker's many-empty-submessage tag-3 payload could unpack far
/// past the cap before rejection. The manual impl drains excess tag-3 records
/// without allocating (charge-before-allocate), matching the JSON path's
/// [`BoundedStructuredMetadata`]. Because the derive is gone, the field-level
/// `#[prost(...)]` helper attributes are removed too (they have no registering
/// derive macro) — tags 1/2/3 and their wire kinds are hardcoded in the impl.
#[derive(Clone, PartialEq, Default, Debug)]
pub struct EntryAdapter {
    pub timestamp: Option<Timestamp>,
    pub line: String,
    pub structured_metadata: Vec<LabelPairAdapter>,
}

impl prost::Message for EntryAdapter {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        // proto3 encoding, byte-identical to the derived impl (skips defaults):
        // `None` timestamp and empty `line` emit nothing; tag-3 is repeated.
        if let Some(ts) = &self.timestamp {
            prost::encoding::message::encode(1u32, ts, buf);
        }
        if !self.line.is_empty() {
            prost::encoding::string::encode(2u32, &self.line, buf);
        }
        prost::encoding::message::encode_repeated(3u32, &self.structured_metadata, buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        match tag {
            1u32 => prost::encoding::message::merge(
                wire_type,
                self.timestamp.get_or_insert_with(Default::default),
                buf,
                ctx,
            ),
            2u32 => prost::encoding::string::merge(wire_type, &mut self.line, buf, ctx),
            3u32 => {
                if self.structured_metadata.len() > MAX_STRUCTURED_METADATA_PER_ENTRY {
                    // Cap reached: drain the excess record WITHOUT materializing,
                    // but still enforce the wire-type contract the derived
                    // `merge_repeated` would — a non-length-delimited tag-3 is a
                    // malformed submessage and must FAIL the decode (a
                    // `DecodeError`), never be silently skipped. The vec is
                    // allowed to reach `MAX + 1` (not capped at `MAX`) so the
                    // unchanged `canonical_structured_metadata(len > MAX)` check
                    // still rejects an over-limit entry as `OversizeMessage`.
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    prost::encoding::message::merge_repeated(
                        wire_type,
                        &mut self.structured_metadata,
                        buf,
                        ctx,
                    )
                }
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        self.timestamp
            .as_ref()
            .map_or(0, |ts| prost::encoding::message::encoded_len(1u32, ts))
            + if self.line.is_empty() {
                0
            } else {
                prost::encoding::string::encoded_len(2u32, &self.line)
            }
            + prost::encoding::message::encoded_len_repeated(3u32, &self.structured_metadata)
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

/// `logproto.LabelPairAdapter`: one structured-metadata `name`/`value` pair
/// (`name` at tag 1, `value` at tag 2). Issue #97.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LabelPairAdapter {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
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
/// Per-entry structured-metadata cardinality bound (issue #97), mirroring
/// [`MAX_LABELS_PER_STREAM`]. Enforced during decode by `EntryAdapter`'s
/// hand-written [`prost::Message`] impl (protobuf) and by
/// [`BoundedStructuredMetadata`] (JSON) — both charge-before-allocate — so an
/// entry carrying more than this is rejected before the excess is materialized.
/// The protobuf decoder lets the vec reach `MAX + 1` so the unchanged
/// [`canonical_structured_metadata`] cardinality check still fires a
/// whole-request [`LogsIngestError::OversizeMessage`] (Loki is all-or-nothing),
/// never a silent truncation.
pub const MAX_STRUCTURED_METADATA_PER_ENTRY: usize = 256;
/// Per-entry structured-metadata *byte* budget (issue #97): the sum of
/// `name.len() + value.len()` across an entry's SM pairs, charged on borrowed
/// data **before** any clone / canonical-JSON construction so an oversize
/// name/value cannot be cloned and JSON-escaped (up to ~6× for `\uXXXX`
/// escaping) into hundreds of MiB — a single body-cap-sized string would
/// otherwise amplify one 64 MiB request accordingly. 64 KiB is orders of
/// magnitude above any legitimate per-entry metadata (trace/span/user IDs) yet
/// caps worst-case canonical expansion to a few hundred KiB per entry. An entry
/// exceeding it is a whole-request [`LogsIngestError::OversizeMessage`] with
/// field `structured_metadata_bytes`, applied to both the protobuf and JSON
/// paths (the amplification is identical once strings are materialized 1:1 from
/// the wire/JSON).
pub const MAX_STRUCTURED_METADATA_BYTES_PER_ENTRY: usize = 64 * 1024;

/// The infallible canonicalization/serialization core shared by every
/// structured-metadata producer — the Loki-push receiver
/// ([`canonical_structured_metadata`]) and the OTLP-logs scope path
/// (`otlp_logs::build_scope_structured_metadata`, issue #109). Both funnel
/// through this one seam so the stored `log_samples.structured_metadata`
/// String is byte-identical across transports by construction.
///
/// - The **empty** set yields `""` (an empty string, NOT `"{}"`) so the read
///   path's `structured_metadata.is_empty()` fast-path branch stays on the
///   zero-structured-metadata path for entries that carry none — the common
///   case, and the byte-identity invariant for pre-#97 data.
/// - A non-empty set is normalized through the same `LabelSet::from_normalized`
///   then `to_canonical_json` seam stream labels use, so a structured-metadata
///   JSON string is byte-identical in shape to a stream-labels JSON string.
///   The normalization collision count is intentionally discarded: SM is
///   per-entry and never contributes to the stream-label collision metric.
///
/// This core carries **no** cardinality cap — the Loki-push cap check lives in
/// [`canonical_structured_metadata`] (charge-before-allocate, before this is
/// reached), and the OTLP path is intentionally uncapped (matching OTLP
/// `parse`'s existing unbounded-label, infallible behaviour). The OTLP path
/// pre-resolves its own last-write-wins collisions (Loki's rule) *before*
/// calling this, so `from_normalized` here only ever sees already-unique
/// sanitized keys and its own collision path is not exercised there.
pub(crate) fn structured_metadata_json(
    pairs: impl IntoIterator<Item = (String, String)>,
) -> String {
    let mut iter = pairs.into_iter().peekable();
    if iter.peek().is_none() {
        return String::new();
    }
    let (labels, _collisions) = LabelSet::from_normalized(iter);
    labels.to_canonical_json()
}

/// Canonicalizes one Loki-push entry's structured-metadata pairs into the
/// stored `log_samples.structured_metadata` JSON String (issue #97). Charges
/// two per-entry bounds **before** the `LabelSet`/JSON is built
/// (charge-before-allocate) — the [`MAX_STRUCTURED_METADATA_PER_ENTRY`]
/// cardinality bound and the [`MAX_STRUCTURED_METADATA_BYTES_PER_ENTRY`] total
/// byte budget (`byte_count`, computed by the caller with `.len()` on borrowed
/// strings, so the reject path performs zero clones) — an entry breaching
/// either is a whole-request [`LogsIngestError::OversizeMessage`] (Loki is
/// all-or-nothing), never a silent truncation — then delegates to the shared
/// [`structured_metadata_json`] core (where the clone/escape happens, past both
/// checks).
fn canonical_structured_metadata(
    pair_count: usize,
    byte_count: usize,
    pairs: impl IntoIterator<Item = (String, String)>,
) -> Result<String, LogsIngestError> {
    if pair_count == 0 {
        return Ok(String::new());
    }
    if pair_count > MAX_STRUCTURED_METADATA_PER_ENTRY {
        return Err(LogsIngestError::OversizeMessage {
            field: "structured_metadata",
            limit: MAX_STRUCTURED_METADATA_PER_ENTRY,
            actual: pair_count,
        });
    }
    if byte_count > MAX_STRUCTURED_METADATA_BYTES_PER_ENTRY {
        return Err(LogsIngestError::OversizeMessage {
            field: "structured_metadata_bytes",
            limit: MAX_STRUCTURED_METADATA_BYTES_PER_ENTRY,
            actual: byte_count,
        });
    }
    Ok(structured_metadata_json(pairs))
}

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
            let sm = &entry.structured_metadata;
            // Byte budget charged on borrowed data before the cloning iterator
            // below is consumed — the reject path performs zero clones.
            let byte_count = sm.iter().map(|p| p.name.len() + p.value.len()).sum();
            let structured_metadata = canonical_structured_metadata(
                sm.len(),
                byte_count,
                sm.iter().map(|p| (p.name.clone(), p.value.clone())),
            )?;
            Ok((timestamp_ns, entry.line.clone(), structured_metadata))
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
/// `ParsedLogs`. Each `values` entry deserializes as `(ts, line)` plus an
/// optional third structured-metadata object, decoded into
/// `structured_metadata` ([`JsonEntry`]'s `Deserialize`, issue #97); only a
/// fourth+ element is drained without being materialized.
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
            let sm = &entry.structured_metadata;
            // Byte budget charged on borrowed data before the cloning iterator
            // below is consumed — the reject path performs zero clones. Both
            // paths get the budget: amplification is identical once strings are
            // materialized 1:1 from the wire/JSON.
            let byte_count = sm.iter().map(|(k, v)| k.len() + v.len()).sum();
            let structured_metadata = canonical_structured_metadata(
                sm.len(),
                byte_count,
                sm.iter().map(|(k, v)| (k.clone(), v.clone())),
            )?;
            Ok((timestamp_ns, entry.line.clone(), structured_metadata))
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
    entries: impl Iterator<Item = Result<(i64, String, String), LogsIngestError>>,
    now_ns: i64,
) -> Result<(), LogsIngestError> {
    out.collisions += collisions as u64;
    let fingerprint = stream_fingerprint(&labels);
    let service = labels.service().to_string();
    for entry in entries {
        let (timestamp_ns, line, structured_metadata) = entry?;
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
            structured_metadata,
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

/// One `values` array entry: `["<unix_nano_string>", "<line>"]` or, with
/// per-entry structured metadata, `["<ts>", "<line>", {"k":"v", ...}]` (issue
/// #97). The optional third element is decoded into `structured_metadata` as
/// RAW `(key, value)` pairs (pre-dedup) by [`BoundedStructuredMetadata`],
/// whose visitor charges [`MAX_STRUCTURED_METADATA_PER_ENTRY`] DURING decode
/// and aborts before the object is fully materialized — mirroring the protobuf
/// path, which charges `entry.structured_metadata.len()` (prost's already-raw
/// repeated field) in [`canonical_structured_metadata`] *before*
/// `LabelSet::from_normalized` allocates. Counting RAW pairs (not a
/// dedup-collapsing `BTreeMap`) means duplicate JSON keys cannot evade the
/// bound. A present-but-non-object third element is a deserialization error (a
/// whole-request 400 — Loki is all-or-nothing), never a silent drop. Any
/// fourth+ element is drained without materializing.
struct JsonEntry {
    timestamp: String,
    line: String,
    structured_metadata: Vec<(String, String)>,
}

/// The optional third `values` element (`{"k":"v", ...}`), decoded into RAW
/// `(key, value)` pairs with the [`MAX_STRUCTURED_METADATA_PER_ENTRY`] bound
/// enforced DURING deserialization (charge-before-allocate): the visitor
/// aborts at pair 257 before materializing the rest of the object. A dedup-
/// collapsing `BTreeMap` would (a) allocate every key before any bound check
/// and (b) fold duplicate keys, letting a duplicate-key object evade the
/// per-entry cardinality bound — so raw pairs are counted instead. Downstream
/// dedup/canonicalization is left to [`canonical_structured_metadata`]'s
/// `LabelSet::from_normalized`, exactly as the protobuf path does.
struct BoundedStructuredMetadata(Vec<(String, String)>);

impl<'de> serde::Deserialize<'de> for BoundedStructuredMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MapVisitor;
        impl<'de> serde::de::Visitor<'de> for MapVisitor {
            type Value = Vec<(String, String)>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a structured-metadata object of string values")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut pairs: Vec<(String, String)> = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, String>()? {
                    if pairs.len() >= MAX_STRUCTURED_METADATA_PER_ENTRY {
                        // Charge-before-allocate: reject at the 257th raw pair
                        // (pre-dedup) without materializing the remainder.
                        return Err(serde::de::Error::custom(format!(
                            "structured_metadata exceeds the {MAX_STRUCTURED_METADATA_PER_ENTRY}-pair per-entry bound"
                        )));
                    }
                    pairs.push((key, value));
                }
                Ok(pairs)
            }
        }
        deserializer.deserialize_map(MapVisitor).map(Self)
    }
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
                let structured_metadata: Vec<(String, String)> = seq
                    .next_element::<BoundedStructuredMetadata>()?
                    .map(|b| b.0)
                    .unwrap_or_default();
                // Drain any trailing element beyond the third without
                // materializing it.
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(JsonEntry {
                    timestamp,
                    line,
                    structured_metadata,
                })
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
            structured_metadata: Vec::new(),
        }
    }

    fn label_pair(name: &str, value: &str) -> LabelPairAdapter {
        LabelPairAdapter {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn entry_with_sm(seconds: i64, line: &str, sm: Vec<LabelPairAdapter>) -> EntryAdapter {
        EntryAdapter {
            timestamp: Some(ts(seconds, 0)),
            line: line.to_string(),
            structured_metadata: sm,
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
                    structured_metadata: Vec::new(),
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
    fn parse_json_captures_structured_metadata_as_canonical_json() {
        // A 3-element values entry: ts, line, metadata object — the third
        // element is decoded into the canonical JSON String column (issue #97).
        let body = br#"{"streams":[{"stream":{"a":"b"},
            "values":[["1700000000000000000","hello",{"user_id":"42","trace_id":"abc"}]]}]}"#;
        let out = parse_json(body, 0).unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].body, "hello");
        // Sorted keys, byte-identical shape to a stream-labels JSON string.
        assert_eq!(
            out.rows[0].structured_metadata,
            r#"{"trace_id":"abc","user_id":"42"}"#
        );
    }

    #[test]
    fn parse_json_two_element_entry_has_empty_structured_metadata() {
        let body = br#"{"streams":[{"stream":{"a":"b"},
            "values":[["1700000000000000000","hello"]]}]}"#;
        let out = parse_json(body, 0).unwrap();
        // Empty string (NOT "{}") keeps the read path on the zero-SM fast path.
        assert_eq!(out.rows[0].structured_metadata, "");
    }

    #[test]
    fn parse_json_non_object_structured_metadata_is_a_whole_request_error() {
        // A present-but-non-object third element is a 400, not a silent drop.
        let body = br#"{"streams":[{"stream":{"a":"b"},
            "values":[["1700000000000000000","hello","not-an-object"]]}]}"#;
        let err = parse_json(body, 0).unwrap_err();
        assert!(matches!(err, LogsIngestError::LokiDecode(_)));
    }

    #[test]
    fn parse_protobuf_decodes_structured_metadata_into_canonical_json() {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{service_name="checkout"}"#.to_string(),
                entries: vec![entry_with_sm(
                    1_700_000_000,
                    "hello",
                    vec![label_pair("user_id", "42"), label_pair("trace_id", "abc")],
                )],
            }],
        };
        let out = parse_protobuf(&req, 0).unwrap();
        assert_eq!(
            out.rows[0].structured_metadata,
            r#"{"trace_id":"abc","user_id":"42"}"#
        );
    }

    #[test]
    fn structured_metadata_out_of_range_is_a_whole_request_error_before_allocation() {
        let sm: Vec<LabelPairAdapter> = (0..=MAX_STRUCTURED_METADATA_PER_ENTRY)
            .map(|i| label_pair(&format!("k{i}"), "v"))
            .collect();
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{a="b"}"#.to_string(),
                entries: vec![entry_with_sm(1_700_000_000, "x", sm)],
            }],
        };
        let err = parse_protobuf(&req, 0).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "structured_metadata",
                ..
            }
        ));
    }

    #[test]
    fn parse_protobuf_accepts_exactly_max_structured_metadata_pairs() {
        // Count boundary (AC3): exactly MAX (256) pairs is the largest accepted
        // cardinality — no off-by-one regression against the 257-rejection test.
        let sm: Vec<LabelPairAdapter> = (0..MAX_STRUCTURED_METADATA_PER_ENTRY)
            .map(|i| label_pair(&format!("k{i:03}"), "v"))
            .collect();
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{a="b"}"#.to_string(),
                entries: vec![entry_with_sm(1_700_000_000, "x", sm)],
            }],
        };
        let out = parse_protobuf(&req, 0).unwrap();
        assert_eq!(out.rows.len(), 1);
        // All 256 pairs are canonicalized (distinct keys, so no collision drop).
        let json = &out.rows[0].structured_metadata;
        assert!(json.starts_with('{') && json.ends_with('}'));
        assert_eq!(
            json.matches(':').count(),
            MAX_STRUCTURED_METADATA_PER_ENTRY,
            "all 256 pairs must be present in the canonical JSON"
        );
    }

    /// A minimal length-delimited protobuf field: key byte `(tag << 3) | 2`
    /// followed by a base-128 varint length and the payload. Used to hand-build
    /// wire bytes the derived encoder cannot produce (an over-cap malformed
    /// tag-3), so the decoder's post-cap wire-type check is exercised directly.
    fn field_ld(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![(tag << 3) | 2];
        let mut len = payload.len();
        loop {
            let mut b = (len & 0x7f) as u8;
            len >>= 7;
            if len != 0 {
                b |= 0x80;
            }
            out.push(b);
            if len == 0 {
                break;
            }
        }
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn decode_protobuf_caps_structured_metadata_materialization() {
        // AC2: an entry carrying a million empty tag-3 submessages must NOT
        // materialize all N — the hand-written decoder caps the vec at MAX+1
        // (257) and drains the rest without allocating. Decode succeeds (the
        // excess is length-delimited, drained cleanly); the deferred
        // canonical_structured_metadata(len > MAX) check then rejects in parse.
        let n = 1_000_000usize;
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{a="b"}"#.to_string(),
                entries: vec![EntryAdapter {
                    timestamp: Some(ts(1_700_000_000, 0)),
                    line: "x".to_string(),
                    structured_metadata: vec![LabelPairAdapter::default(); n],
                }],
            }],
        };
        let bytes = req.encode_to_vec();
        let decoded = decode_protobuf(&bytes).unwrap();
        assert_eq!(
            decoded.streams[0].entries[0].structured_metadata.len(),
            MAX_STRUCTURED_METADATA_PER_ENTRY + 1,
            "the decoder must cap materialization at MAX + 1, not materialize all N"
        );
        let err = parse_protobuf(&decoded, 0).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "structured_metadata",
                ..
            }
        ));
    }

    #[test]
    fn decode_protobuf_rejects_non_length_delimited_tag3_after_cap() {
        // AC4 (finding 1): after the 257th pair the decoder drains excess tag-3
        // records WITHOUT materializing, but must still enforce the wire-type
        // contract the derived merge_repeated would — a non-length-delimited
        // tag-3 (varint wire type) after the cap must FAIL decode, never be
        // silently skipped. With an unconditional skip_field (the pre-fix shape)
        // decode would succeed and this unwrap_err would panic.
        let mut entry_bytes = Vec::new();
        // 257 valid empty tag-3 records: `0x1a 0x00` (tag 3, length-delimited,
        // zero-length submessage). 0..=MAX == 257 records → drives the vec to
        // MAX + 1 so the next record hits the drain path.
        for _ in 0..=MAX_STRUCTURED_METADATA_PER_ENTRY {
            entry_bytes.extend_from_slice(&[0x1a, 0x00]);
        }
        // A malformed 258th tag-3 with varint wire type: `0x18 0x01`
        // ((3 << 3) | 0 = 0x18, value 1).
        entry_bytes.extend_from_slice(&[0x18, 0x01]);
        // Wrap: StreamAdapter.entries (tag 2) -> PushRequest.streams (tag 1).
        let stream_bytes = field_ld(2, &entry_bytes);
        let request_bytes = field_ld(1, &stream_bytes);
        let err = decode_protobuf(&request_bytes).unwrap_err();
        assert!(
            matches!(err, LogsIngestError::Decode(_)),
            "a non-length-delimited tag-3 after the cap must fail decode, got {err:?}"
        );
    }

    #[test]
    fn parse_protobuf_rejects_oversize_structured_metadata_bytes() {
        // AC5 (finding 2): a single over-budget pair must reject on the byte
        // budget BEFORE any clone/canonical-JSON construction.
        let big = "a".repeat(MAX_STRUCTURED_METADATA_BYTES_PER_ENTRY + 1);
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{a="b"}"#.to_string(),
                entries: vec![entry_with_sm(
                    1_700_000_000,
                    "x",
                    vec![label_pair("k", &big)],
                )],
            }],
        };
        let err = parse_protobuf(&req, 0).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "structured_metadata_bytes",
                ..
            }
        ));
    }

    #[test]
    fn parse_protobuf_accepts_at_budget_structured_metadata_bytes() {
        // AC5: a payload whose Σ(name.len()+value.len()) is exactly the budget
        // is accepted — no behaviour change for legitimate at-budget input.
        let value = "a".repeat(MAX_STRUCTURED_METADATA_BYTES_PER_ENTRY - 1);
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{a="b"}"#.to_string(),
                entries: vec![entry_with_sm(
                    1_700_000_000,
                    "x",
                    vec![label_pair("k", &value)],
                )],
            }],
        };
        let out = parse_protobuf(&req, 0).unwrap();
        assert_eq!(out.rows.len(), 1);
        assert!(!out.rows[0].structured_metadata.is_empty());
    }

    #[test]
    fn parse_json_rejects_oversize_structured_metadata_bytes() {
        // AC5: the byte budget applies to the JSON path too (amplification is
        // identical once strings are materialized 1:1 from the JSON).
        let big = "a".repeat(MAX_STRUCTURED_METADATA_BYTES_PER_ENTRY + 1);
        let body = format!(
            r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[["1700000000000000000","x",{{"k":"{big}"}}]]}}]}}"#
        );
        let err = parse_json(body.as_bytes(), 0).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "structured_metadata_bytes",
                ..
            }
        ));
    }

    #[test]
    fn parse_json_accepts_at_budget_structured_metadata_bytes() {
        // AC5: an at-budget JSON payload is accepted (no behaviour change).
        let value = "a".repeat(MAX_STRUCTURED_METADATA_BYTES_PER_ENTRY - 1);
        let body = format!(
            r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[["1700000000000000000","x",{{"k":"{value}"}}]]}}]}}"#
        );
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert_eq!(out.rows.len(), 1);
        assert!(!out.rows[0].structured_metadata.is_empty());
    }

    #[test]
    fn parse_json_structured_metadata_out_of_range_is_a_whole_request_error() {
        // The JSON path enforces the per-entry bound DURING serde decode (the
        // `BoundedStructuredMetadata` visitor aborts at pair 257 via
        // `serde::de::Error::custom`), which `parse_json` maps to a whole-request
        // `LokiDecode` — NOT the `OversizeMessage` the protobuf path raises from
        // `canonical_structured_metadata`, which the 257th-pair decode abort
        // never reaches. Asserting the exact variant AND that the message names
        // the per-entry bound proves it was the bound that fired, not an
        // incidental decode error.
        let mut obj = String::from("{");
        for i in 0..=MAX_STRUCTURED_METADATA_PER_ENTRY {
            if i > 0 {
                obj.push(',');
            }
            obj.push_str(&format!(r#""k{i}":"v""#));
        }
        obj.push('}');
        let body = format!(
            r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[["1700000000000000000","x",{obj}]]}}]}}"#
        );
        let err = parse_json(body.as_bytes(), 0).unwrap_err();
        let LogsIngestError::LokiDecode(msg) = err else {
            panic!("expected LokiDecode, got {err:?}");
        };
        assert!(
            msg.contains("structured_metadata exceeds") && msg.contains("per-entry bound"),
            "the decode error must name the per-entry bound: {msg:?}"
        );
    }

    #[test]
    fn parse_json_duplicate_structured_metadata_keys_cannot_evade_the_bound() {
        // A duplicate-key object would collapse to ONE entry in a `BTreeMap`,
        // evading the cardinality bound; counting RAW pairs during decode
        // rejects it. 257 repetitions of one key abort at pair 257 with the same
        // whole-request `LokiDecode` (the serde visitor's per-entry-bound custom
        // error) the distinct-key case raises — asserted precisely so the test
        // names what it checks.
        let mut obj = String::from("{");
        for i in 0..=MAX_STRUCTURED_METADATA_PER_ENTRY {
            if i > 0 {
                obj.push(',');
            }
            obj.push_str(r#""dup":"v""#);
        }
        obj.push('}');
        let body = format!(
            r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[["1700000000000000000","x",{obj}]]}}]}}"#
        );
        let err = parse_json(body.as_bytes(), 0).unwrap_err();
        let LogsIngestError::LokiDecode(msg) = err else {
            panic!("expected LokiDecode, got {err:?}");
        };
        assert!(
            msg.contains("structured_metadata exceeds") && msg.contains("per-entry bound"),
            "the decode error must name the per-entry bound: {msg:?}"
        );
    }

    #[test]
    fn structured_metadata_does_not_change_the_stream_fingerprint() {
        // AC-5: SM is per-entry — the stream fingerprint and StreamRow are
        // identical with and without it.
        let without = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{service_name="checkout"}"#.to_string(),
                entries: vec![entry(1_700_000_000, 0, "x")],
            }],
        };
        let with = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{service_name="checkout"}"#.to_string(),
                entries: vec![entry_with_sm(
                    1_700_000_000,
                    "x",
                    vec![label_pair("trace_id", "abc")],
                )],
            }],
        };
        let a = parse_protobuf(&without, 7).unwrap();
        let b = parse_protobuf(&with, 7).unwrap();
        assert_eq!(a.rows[0].fingerprint, b.rows[0].fingerprint);
        assert_eq!(a.streams, b.streams);
        assert_eq!(a.rows[0].structured_metadata, "");
        assert_eq!(b.rows[0].structured_metadata, r#"{"trace_id":"abc"}"#);
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

    /// AC-4: a protobuf tag-3 body and a JSON third-element body of one
    /// logical entry produce byte-identical `structured_metadata`.
    #[test]
    fn json_and_protobuf_structured_metadata_are_byte_identical() {
        let json = br#"{"streams":[{"stream":{"service_name":"checkout"},
            "values":[["1700000000000000000","line",{"user_id":"42","trace_id":"abc"}]]}]}"#;
        let proto = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{service_name="checkout"}"#.to_string(),
                entries: vec![entry_with_sm(
                    1_700_000_000,
                    "line",
                    vec![label_pair("user_id", "42"), label_pair("trace_id", "abc")],
                )],
            }],
        };
        let from_json = parse_json(json, 7).unwrap();
        let from_proto = parse_protobuf(&proto, 7).unwrap();
        assert_eq!(from_json, from_proto);
        assert_eq!(
            from_json.rows[0].structured_metadata,
            r#"{"trace_id":"abc","user_id":"42"}"#
        );
    }
}
