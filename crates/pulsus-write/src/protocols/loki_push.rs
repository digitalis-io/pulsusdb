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
use crate::protocols::otlp_prescan::MAX_DECODED_BYTES;

/// `logproto.PushRequest`: `streams` at tag 1.
///
/// This is the **domain / value** type: encode + a byte-identical round-trip
/// with derived [`PartialEq`], so a hand-built request and its encode/decode
/// round-trip compare equal by construction. It deliberately does **not**
/// derive `::prost::Message`: a derived decoder exposes a `pub`
/// `PushRequest::decode` that would materialize an unbounded stream/aggregate
/// fan-out when called directly — bypassing the ingest path's
/// [`BoundedPushRequest`] caps entirely (issue #115). Instead a hand-written
/// [`prost::Message`] impl (below) bounds **every** decode entry:
///
/// - `merge_field` caps `streams` (tag 1) at [`MAX_STREAMS_PER_REQUEST`]` + 1`
///   during merge (draining the excess, wire-type-checked, without allocating)
///   and delegates per-stream entry caps to [`StreamAdapter`].
/// - **Every** public decode/merge entry point — `decode`,
///   `decode_length_delimited`, `merge` AND `merge_length_delimited` — routes
///   through [`BoundedPushRequest`], whose `merge_field` is the single enforcing
///   chokepoint: it additionally drains streams once the cross-stream aggregate
///   exceeds [`MAX_TOTAL_ENTRIES_PER_REQUEST`], giving identical materialization
///   bounds to [`decode_protobuf`]. `prost`'s default `Message::merge` /
///   `merge_length_delimited` call `PushRequest::merge_field` directly (which
///   caps stream *count* only), so a raw `PushRequest::default().merge(buf)`
///   would otherwise bypass the aggregate cap (issue #115 round 2) — these two
///   overrides close that last gap so no public entry is an uncapped bypass.
///
/// The whole-request [`LogsIngestError::OversizeMessage`] reject still lives in
/// [`decode_protobuf`]'s [`validate_bounds`] (Loki is all-or-nothing). `encode`
/// and the derived [`PartialEq`] are unchanged, and no decode-scratch field is
/// added to the value type, so the struct literals and cross-crate encoders
/// keep working.
#[derive(Clone, PartialEq, Default, Debug)]
pub struct PushRequest {
    pub streams: Vec<StreamAdapter>,
}

impl prost::Message for PushRequest {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        prost::encoding::message::encode_repeated(1u32, &self.streams, buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        match tag {
            1u32 => {
                if self.streams.len() > MAX_STREAMS_PER_REQUEST {
                    // Cap reached: drain the excess stream WITHOUT materializing
                    // it, wire-type-checked exactly as `BoundedPushRequest`'s
                    // tag-1 drain — a non-length-delimited tag-1 is a malformed
                    // submessage and must FAIL the decode, never be silently
                    // skipped. This is belt-and-suspenders: every public
                    // decode/merge entry point below routes through
                    // [`BoundedPushRequest`], whose `merge_field` adds the
                    // cross-stream aggregate drain this one lacks.
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    prost::encoding::message::merge_repeated(wire_type, &mut self.streams, buf, ctx)
                }
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        prost::encoding::message::encoded_len_repeated(1u32, &self.streams)
    }

    fn clear(&mut self) {
        self.streams.clear();
    }

    fn decode(buf: impl bytes::Buf) -> Result<Self, prost::DecodeError>
    where
        Self: Default,
    {
        // The most-direct public decode entry (issue #115): route through the
        // fully-bounded twin so stream-count, per-stream entry AND cross-stream
        // aggregate fan-out are all bounded DURING decode — a direct
        // `PushRequest::decode` is no longer an uncapped bypass of the caps the
        // ingest path enforces.
        let bounded = BoundedPushRequest::decode(buf)?;
        Ok(Self {
            streams: bounded.streams,
        })
    }

    fn decode_length_delimited(buf: impl bytes::Buf) -> Result<Self, prost::DecodeError>
    where
        Self: Default,
    {
        let bounded = BoundedPushRequest::decode_length_delimited(buf)?;
        Ok(Self {
            streams: bounded.streams,
        })
    }

    fn merge(&mut self, buf: impl bytes::Buf) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        // Issue #115 round 2: `prost`'s default `Message::merge` calls
        // `PushRequest::merge_field` directly, which caps only stream COUNT — so
        // a raw `PushRequest::default().merge(buf)` would fan out past the
        // cross-stream aggregate cap. Route the merge through the fully-bounded
        // twin (the single enforcing chokepoint) instead. Seed the twin with
        // self's current streams so merge-INTO-existing semantics are preserved,
        // then move the aggregate-bounded result back. The one-shot re-sum is
        // O(existing streams) (zero for the common fresh-default `decode` path).
        //
        // Issue #115 round 3: restore `bounded.streams` into `self` on BOTH the
        // Ok AND Err paths — do NOT `?` while self's streams are moved out. A
        // decode error otherwise returns with `self.streams` left empty, dropping
        // the caller's pre-existing streams (data loss). Restoring first gives
        // prost-consistent partial-merge semantics: on error, self keeps its
        // pre-existing streams plus whatever decoded before the failure point.
        let streams = std::mem::take(&mut self.streams);
        // Seed `decoded_bytes` with the SAME shared re-sum the deferred
        // `decode_protobuf` byte check uses (issue #168), so a merge INTO an
        // existing request charges the pre-existing materialization too — no
        // budget bypass through repeated raw merges.
        let mut bounded = BoundedPushRequest {
            total_entries: streams.iter().map(|s| s.entries.len()).sum(),
            decoded_bytes: decoded_push_request_bytes(&streams),
            streams,
        };
        let result = bounded.merge(buf);
        self.streams = bounded.streams;
        result
    }

    fn merge_length_delimited(&mut self, buf: impl bytes::Buf) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        // `merge_length_delimited` likewise loops through `merge_field` directly
        // (it does not funnel through `merge`), so it needs the same bounded-twin
        // routing as `merge` above to enforce the cross-stream aggregate cap, and
        // the same round-3 error-path restoration: restore `bounded.streams` into
        // `self` on BOTH Ok and Err before propagating, so a decode error never
        // empties the caller's pre-existing streams (prost partial-merge
        // semantics).
        let streams = std::mem::take(&mut self.streams);
        let mut bounded = BoundedPushRequest {
            total_entries: streams.iter().map(|s| s.entries.len()).sum(),
            decoded_bytes: decoded_push_request_bytes(&streams),
            streams,
        };
        let result = bounded.merge_length_delimited(buf);
        self.streams = bounded.streams;
        result
    }
}

/// The **decode-time twin** of [`PushRequest`] (issue #77): a hand-written
/// [`prost::Message`] that bounds materialization **during** `decode` so a body
/// within the 64 MiB decompressed cap cannot unpack into a far larger in-memory
/// fan-out before the count checks run. Two decode-time guards, both mirroring
/// [`EntryAdapter`]'s landed #97 drain-past-cap-then-reject pattern:
///
/// 1. `streams` (tag 1) is capped at [`MAX_STREAMS_PER_REQUEST`]` + 1` — once
///    the vec would exceed the cap, the excess tag-1 record is drained (wire-
///    type-checked, no allocation) rather than materialized.
/// 2. A **transient, non-wire** `total_entries` accumulator sums every merged
///    stream's `entries.len()`. prost 0.14's `DecodeError::new` is deprecated,
///    so `merge_field` cannot abort mid-decode with a custom error; instead,
///    once the running total exceeds [`MAX_TOTAL_ENTRIES_PER_REQUEST`], further
///    streams are drained without materializing (bounding the aggregate fan-out
///    to `≤ MAX_TOTAL + one stream's cap`), and the deferred [`validate_bounds`]
///    re-sum in [`decode_protobuf`] then rejects the whole request. This closes
///    the second-amplification the per-dimension caps cannot catch: many streams
///    each under [`MAX_ENTRIES_PER_STREAM`] but collectively over the aggregate.
/// 3. A transient `decoded_bytes` accumulator (issue #168) estimates the BYTES
///    the materialized elements cost — `size_of::<StreamAdapter>()` per stream
///    (charged at the tag-1 boundary) plus `size_of::<EntryAdapter>()` +
///    `structured_metadata.len() × size_of::<LabelPairAdapter>()` per entry
///    (charged incrementally per entry DURING each stream's decode via
///    [`Self::merge_one_stream`]). The element-COUNT caps bound how many
///    elements decode, not how much memory: a minimal 2-wire-byte empty
///    structured-metadata pair materializes ~48 heap bytes, so one crafted
///    stream's 100k-entry × 257-pair fan-out (~1.2 GiB) would materialize inside
///    ONE tag-1 field before any between-stream boundary check ran (the #140
///    geometry) — hence the per-entry interposer, not a stream-boundary-only
///    charge. Once the estimate exceeds the shared
///    [`crate::protocols::otlp_prescan::MAX_DECODED_BYTES`] budget (256 MiB),
///    further entries / streams are drained without materializing, and the
///    deferred byte re-sum in [`decode_protobuf`] rejects the whole request with
///    the family-wide `"decoded bytes (estimated)"` field.
///
/// Kept separate from [`PushRequest`] so the value type carries no decode-scratch
/// field and preserves derived round-trip equality — the sanctioned alternative
/// to a transient field + manual `PartialEq` on the value type.
#[derive(Default)]
struct BoundedPushRequest {
    streams: Vec<StreamAdapter>,
    total_entries: usize,
    decoded_bytes: usize,
}

/// Estimated decoded bytes of ONE entry (issue #168): the `EntryAdapter` struct
/// itself plus its retained structured-metadata pairs — `size_of`-derived, no
/// magic numbers, exactly what the decoder materializes. The `Option<Timestamp>`
/// and `line` `String` are inline in the entry's `size_of`; the string CONTENT
/// is uncharged (bounded by the 64 MiB decompressed body cap, the #127 scalar
/// ruling). The containing stream's shell is charged separately (at the tag-1
/// boundary), so the two never double count.
fn decoded_entry_bytes(entry: &EntryAdapter) -> usize {
    std::mem::size_of::<EntryAdapter>().saturating_add(
        entry
            .structured_metadata
            .len()
            .saturating_mul(std::mem::size_of::<LabelPairAdapter>()),
    )
}

/// Re-sums the whole request's decoded-byte estimate from materialized data —
/// the SAME function of the materialized content as the incremental
/// `decoded_bytes` charges, so the deferred [`decode_protobuf`] re-check and the
/// decode-time drain can never disagree (a drained request always re-sums past
/// the budget), and a merge INTO an existing request seeds the pre-existing
/// fan-out too (issue #168, no budget bypass via repeated raw merges).
fn decoded_push_request_bytes(streams: &[StreamAdapter]) -> usize {
    let mut total = 0usize;
    for stream in streams {
        total = total.saturating_add(std::mem::size_of::<StreamAdapter>());
        for entry in &stream.entries {
            total = total.saturating_add(decoded_entry_bytes(entry));
        }
    }
    total
}

impl BoundedPushRequest {
    /// Decodes ONE `StreamAdapter` submessage (a `PushRequest` tag-1 field
    /// occurrence) while charging the request-wide `decoded_bytes` estimate
    /// **incrementally, per decoded entry** (issue #168) — the byte analog of
    /// [`crate::protocols::remote_write::BoundedWriteRequest`]'s
    /// `merge_one_time_series`: one crafted stream's per-entry fan-out
    /// (100k entries × 257 structured-metadata pairs ≈ 1.2 GiB of structs)
    /// exceeds the 256 MiB budget on its own, so a stream-boundary-only charge
    /// would let that ONE stream fully materialize before any check ran.
    ///
    /// Structurally this replicates `prost::encoding::message::merge` for the
    /// submessage (a [`prost::encoding::merge_loop`] over `decode_key` +
    /// `merge_field`), but interposes on tag 2: once the running `decoded_bytes`
    /// (or the per-stream entry count) exceeds its cap, further entries in THIS
    /// stream are drained without materializing — bounding the over-step to one
    /// entry (`≈ 12.4 KiB`) — and the deferred [`decode_protobuf`] byte re-sum
    /// then rejects the whole request. All other tags delegate to
    /// [`StreamAdapter::merge_field`] (which keeps the per-stream entry-count
    /// drain). The scratch total commits back to `self` only on `Ok`; on a
    /// decode error the whole request fails anyway.
    fn merge_one_stream(
        &mut self,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        prost::encoding::check_wire_type(prost::encoding::WireType::LengthDelimited, wire_type)?;
        // (StreamAdapter under construction, decoded_bytes) — one tuple so
        // `merge_loop` can thread the running byte total through its single
        // `&mut T`.
        let mut scratch = (StreamAdapter::default(), self.decoded_bytes);
        prost::encoding::merge_loop(
            &mut scratch,
            buf,
            ctx,
            |(stream, decoded_bytes), buf, ctx| {
                let (tag, wire_type) = prost::encoding::decode_key(buf)?;
                if tag == 2u32 {
                    if stream.entries.len() > MAX_ENTRIES_PER_STREAM
                        || *decoded_bytes > MAX_DECODED_BYTES
                    {
                        // Cap reached (per-stream count OR the request-wide byte
                        // budget): drain this entry WITHOUT materializing it,
                        // wire-type-checked exactly like every other drain arm. The
                        // vec is allowed to reach the `+ 1` over-cap state so
                        // `validate_bounds` still rejects the request.
                        prost::encoding::check_wire_type(
                            prost::encoding::WireType::LengthDelimited,
                            wire_type,
                        )?;
                        prost::encoding::skip_field(wire_type, tag, buf, ctx)
                    } else {
                        prost::encoding::message::merge_repeated(
                            wire_type,
                            &mut stream.entries,
                            buf,
                            ctx,
                        )?;
                        // Charge the just-merged entry immediately: its own
                        // structured-metadata fan-out is already capped at
                        // `MAX_STRUCTURED_METADATA_PER_ENTRY + 1` by
                        // `EntryAdapter::merge_field`, so one over-budget step grows
                        // the fan-out by at most one entry's bytes.
                        if let Some(entry) = stream.entries.last() {
                            *decoded_bytes =
                                decoded_bytes.saturating_add(decoded_entry_bytes(entry));
                        }
                        Ok(())
                    }
                } else {
                    stream.merge_field(tag, wire_type, buf, ctx)
                }
            },
        )?;
        let (stream, decoded_bytes) = scratch;
        self.decoded_bytes = decoded_bytes;
        self.streams.push(stream);
        Ok(())
    }
}

impl prost::Message for BoundedPushRequest {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        // Decode-only helper, but a complete impl is required by the trait; the
        // transient counters are never encoded, so this is byte-identical to
        // `PushRequest`'s wire form.
        prost::encoding::message::encode_repeated(1u32, &self.streams, buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        match tag {
            1u32 => {
                if self.streams.len() > MAX_STREAMS_PER_REQUEST
                    || self.total_entries > MAX_TOTAL_ENTRIES_PER_REQUEST
                    || self.decoded_bytes > MAX_DECODED_BYTES
                {
                    // Cap reached (stream count OR aggregate entries OR the byte
                    // budget): drain the excess stream WITHOUT materializing it,
                    // while still enforcing the wire-type contract the derived
                    // `merge_repeated` would — a non-length-delimited tag-1 is a
                    // malformed submessage and must FAIL the decode, never be
                    // silently skipped. The vec is allowed to reach `MAX + 1`
                    // (not capped at `MAX`) so the deferred `validate_bounds`
                    // stream-count check still rejects an over-limit request.
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    // Decode this ONE stream through the interposing
                    // [`Self::merge_one_stream`], which charges `decoded_bytes`
                    // INCREMENTALLY per entry DURING the stream's own decode
                    // (issue #168) — a single crafted stream of many
                    // individually-legal entries must not fully materialize
                    // before a between-stream boundary check runs.
                    self.merge_one_stream(wire_type, buf, ctx)?;
                    // Charge the just-merged stream's entry count into the
                    // aggregate (its entry BYTES were already charged
                    // incrementally above), plus the stream's own shell bytes.
                    // Its entry vec is already capped at `MAX_ENTRIES + 1` by
                    // `StreamAdapter::merge_field`, so one over-aggregate step
                    // grows the fan-out by at most one stream's cap.
                    if let Some(last) = self.streams.last() {
                        self.total_entries = self.total_entries.saturating_add(last.entries.len());
                        self.decoded_bytes = self
                            .decoded_bytes
                            .saturating_add(std::mem::size_of::<StreamAdapter>());
                    }
                    Ok(())
                }
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        prost::encoding::message::encoded_len_repeated(1u32, &self.streams)
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

/// `logproto.StreamAdapter`: `labels` (a Prometheus label-set literal
/// `{k="v",...}`) at tag 1, `entries` at tag 2. Tag 3 (`uint64 hash`) is
/// intentionally undeclared — see this module's doc comment.
///
/// Like [`EntryAdapter`] (and [`PushRequest`]) it does **not** derive
/// `::prost::Message`; a hand-written impl (below) caps the repeated `entries`
/// field **inside the decoder** at [`MAX_ENTRIES_PER_STREAM`]` + 1` (issue #77),
/// draining excess tag-2 records without allocating — so a single stream
/// carrying millions of minimal entries cannot unpack past the cap. The cap
/// therefore holds whether a stream decodes via [`BoundedPushRequest`] (the
/// ingest path) or via [`PushRequest`]'s hand-written `merge` (both call this
/// impl per stream).
#[derive(Clone, PartialEq, Default, Debug)]
pub struct StreamAdapter {
    pub labels: String,
    pub entries: Vec<EntryAdapter>,
}

impl prost::Message for StreamAdapter {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        // proto3 encoding, byte-identical to the derived impl (skips defaults):
        // empty `labels` emits nothing; `entries` is a repeated message.
        if !self.labels.is_empty() {
            prost::encoding::string::encode(1u32, &self.labels, buf);
        }
        prost::encoding::message::encode_repeated(2u32, &self.entries, buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        match tag {
            1u32 => prost::encoding::string::merge(wire_type, &mut self.labels, buf, ctx),
            2u32 => {
                if self.entries.len() > MAX_ENTRIES_PER_STREAM {
                    // Cap reached: drain the excess entry without materializing,
                    // wire-type-checked exactly as `PushRequest`'s tag-1 drain
                    // (mirrors `EntryAdapter`'s tag-3 handling). Reaches `MAX + 1`
                    // so the deferred `validate_bounds` entries check rejects.
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    prost::encoding::message::merge_repeated(wire_type, &mut self.entries, buf, ctx)
                }
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        (if self.labels.is_empty() {
            0
        } else {
            prost::encoding::string::encoded_len(1u32, &self.labels)
        }) + prost::encoding::message::encoded_len_repeated(2u32, &self.entries)
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
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
///
/// Decode goes through [`BoundedPushRequest`], whose hand-written
/// [`prost::Message`] (with [`StreamAdapter`]'s) bounds materialization
/// **during** `decode` — streams cap at `MAX_STREAMS_PER_REQUEST + 1`,
/// per-stream entries at `MAX_ENTRIES_PER_STREAM + 1`, and the transient
/// cross-stream accumulator drains streams once the aggregate exceeds
/// [`MAX_TOTAL_ENTRIES_PER_REQUEST`] (so the fan-out never grows unbounded
/// before this reject). This [`validate_bounds`] re-sum then converts those
/// `+1` over-cap sentinels into the whole-request
/// [`LogsIngestError::OversizeMessage`] failure — Loki has no partial-success
/// channel (all-or-nothing), so this never partially applies. A
/// malformed/truncated protobuf is likewise a whole-request atomic failure.
pub fn decode_protobuf(body: &[u8]) -> Result<PushRequest, LogsIngestError> {
    let bounded = BoundedPushRequest::decode(body)?;
    validate_bounds(
        bounded.streams.len(),
        bounded.streams.iter().map(|s| s.entries.len()),
    )?;
    // Decode-time byte budget (issue #168), re-summed from the materialized
    // request with the SAME function the incremental drain charges — the
    // deferred whole-request reject for a decode the twin drained past
    // MAX_DECODED_BYTES (bytes, complementing every element-COUNT cap above).
    // Deferred here (not in `validate_bounds`, which the JSON path shares and
    // which rejects in-seed) so the count caps stay byte-free.
    let decoded_bytes = decoded_push_request_bytes(&bounded.streams);
    if decoded_bytes > MAX_DECODED_BYTES {
        return Err(LogsIngestError::OversizeMessage {
            field: "decoded bytes (estimated)",
            limit: MAX_DECODED_BYTES,
            actual: decoded_bytes,
        });
    }
    Ok(PushRequest {
        streams: bounded.streams,
    })
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
///
/// [`JsonPush`]/[`JsonStream`] use bounded [`serde::de::DeserializeSeed`]
/// visitors (issue #77) that cap the `streams` array
/// ([`MAX_STREAMS_PER_REQUEST`]), each stream's `values` array
/// ([`MAX_ENTRIES_PER_STREAM`]) plus a **shared cross-stream** aggregate
/// ([`MAX_TOTAL_ENTRIES_PER_REQUEST`], threaded through a single
/// [`Cell`](std::cell::Cell) counter), and the per-stream `stream` label map
/// ([`MAX_LABELS_PER_STREAM`]) — all **during** deserialization, so
/// `serde_json` cannot grow those `Vec`s/map unbounded before the count checks.
/// A shared `decoded_bytes` [`Cell`](std::cell::Cell) additionally charges the
/// `size_of`-estimated BYTES each stream/entry/label-map materializes against
/// the [`crate::protocols::otlp_prescan::MAX_DECODED_BYTES`] budget (issue
/// #168), rejecting once the running estimate crosses it — the JSON twin of the
/// protobuf [`decode_protobuf`] byte re-sum (count caps bound element counts,
/// not the materialized heap fan-out). The excess is rejected as
/// [`LogsIngestError::LokiDecode`] mid-parse; the post-decode [`validate_bounds`]
/// re-sum below is a harmless secondary guard for in-bounds input. Each stream's label **names** are validated against the
/// same strict [`is_valid_label_name`] grammar the protobuf path enforces
/// (issue #115) before canonicalization, so an invalid name (`9bad`, `a.b`,
/// non-ASCII) is a whole-request reject on both transports, not a silent
/// canonicalization on the JSON one.
pub fn parse_json(body: &[u8], now_ns: i64) -> Result<ParsedLogs, LogsIngestError> {
    let push: JsonPush =
        serde_json::from_slice(body).map_err(|e| LogsIngestError::LokiDecode(e.to_string()))?;
    // Aggregate-budget charge at the same seam as the protobuf path, before
    // any `LogRow` is materialized (issue #77 delta 1). Redundant with the
    // bounded seed above (which rejects during deserialize) but kept as a cheap
    // secondary guard.
    validate_bounds(
        push.streams.len(),
        push.streams.iter().map(|s| s.values.len()),
    )?;

    let mut out = ParsedLogs::default();
    let mut seen_streams: HashSet<(Fingerprint, Date)> = HashSet::new();
    for stream in &push.streams {
        // Route JSON label keys through the SAME strict label-name grammar the
        // protobuf literal path enforces (issue #115) — before the infallible
        // `from_normalized` canonicalizes them — so a name that is invalid on
        // the wire (`9bad`, `a.b`, non-ASCII) is rejected here too rather than
        // silently reinterpreted. Whole-request reject (Loki all-or-nothing).
        for name in stream.stream.keys() {
            if !is_valid_label_name(name.as_bytes()) {
                return Err(LogsIngestError::LokiDecode(format!(
                    "stream label name {name:?} is invalid (must match [a-zA-Z_][a-zA-Z0-9_]*)"
                )));
            }
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
        // `log_samples` is partitioned by the RAW sample day
        // (`toDate(fromUnixTimestamp64Nano(timestamp_ns))`) and its
        // delete-TTL evaluates `intDiv(timestamp_ns, 1000000000)` in the
        // 32-bit `DateTime` domain (issue #137, mirroring #131's trace fix),
        // so an entry is storage-safe only when its day lies in `0..=49_709`
        // (1970-01-01 to 2106-02-06): a day in `49_710..=65_535` partitions
        // correctly but exceeds `u32::MAX` in the TTL seconds arithmetic,
        // and a later day falls outside the `Date` range entirely — even
        // when its month-start still fits (e.g. 2149-06-07 = day 65536 has
        // month-start 2149-06-01 = day 65530). Gate on the DAY, then derive
        // the month for the stream registration (guaranteed `Some` once the
        // day is in range, but kept fallible — no `.unwrap()` on untrusted
        // input). Saturating would orphan or silently early-expire the
        // sample; like a timestamp overflow above, this aborts the whole
        // request (Loki is all-or-nothing).
        if Date::start_of_day_utc_datetime_safe(timestamp_ns).is_none() {
            return Err(LogsIngestError::LokiDecode(format!(
                "log entry timestamp {timestamp_ns} is outside the supported \
                 storage time range (1970-01-01 to 2106-02-06 UTC)"
            )));
        }
        let month = Date::start_of_month_utc(timestamp_ns).ok_or_else(|| {
            LogsIngestError::LokiDecode(format!(
                "log entry timestamp {timestamp_ns} is outside the supported \
                 storage time range (1970-01-01 to 2106-02-06 UTC)"
            ))
        })?;
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

/// The strict Prometheus/Loki label-name grammar predicate
/// `[a-zA-Z_][a-zA-Z0-9_]*` (issue #77): the first byte must be `[A-Za-z_]` and
/// every subsequent byte `[A-Za-z0-9_]`; an empty name is invalid. This is the
/// **single** grammar check shared by both receiver paths — the protobuf
/// label-set literal ([`read_key`]) and the JSON `stream` label map
/// ([`parse_json`]) — so a name that is rejected on one transport is rejected
/// identically on the other (issue #115): a name starting with a digit
/// (`9bad`), containing a non-identifier byte (`a.b`), or carrying a non-ASCII
/// byte (`naïve`) fails on both.
fn is_valid_label_name(name: &[u8]) -> bool {
    let Some((first, rest)) = name.split_first() else {
        return false;
    };
    matches!(first, b'A'..=b'Z' | b'a'..=b'z' | b'_')
        && rest
            .iter()
            .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

/// Reads and **strictly validates** a Prometheus label name against the
/// documented grammar `[a-zA-Z_][a-zA-Z0-9_]*` (issue #77), via the shared
/// [`is_valid_label_name`] predicate the JSON path uses too (issue #115). A
/// genuinely empty/absent key, a name starting with a digit (`9bad`), a name
/// containing a non-identifier byte (`a.b`), or a non-ASCII name (`naïve`) is a
/// malformed literal — rejected as [`LogsIngestError::LokiDecode`] (a
/// whole-request 400). Prior behaviour was lenient (accepted any run of bytes
/// up to the delimiter and let `from_normalized` canonicalize), contradicting
/// this doc-comment; the receiver now enforces the grammar it documents rather
/// than silently reinterpreting malformed untrusted input.
fn read_key(bytes: &[u8], i: &mut usize, input: &str) -> Result<String, LogsIngestError> {
    let start = *i;
    while *i < bytes.len() {
        let b = bytes[*i];
        if b == b'=' || b == b',' || b.is_ascii_whitespace() {
            break;
        }
        *i += 1;
    }
    let name = &bytes[start..*i];
    if name.is_empty() {
        return Err(LogsIngestError::LokiDecode(format!(
            "stream labels {input:?}: empty label name at byte {start}"
        )));
    }
    if !is_valid_label_name(name) {
        return Err(LogsIngestError::LokiDecode(format!(
            "stream labels {input:?}: invalid label name {:?} at byte {start} \
             (must match [a-zA-Z_][a-zA-Z0-9_]*)",
            String::from_utf8_lossy(name)
        )));
    }
    // Every byte is now validated ASCII `[A-Za-z0-9_]`, so this is exact UTF-8
    // (no replacement characters are possible).
    Ok(String::from_utf8_lossy(name).into_owned())
}

/// Reads a double-quoted, Prometheus-escaped value starting at `bytes[*i]`
/// (which must be `"`), consuming through the closing quote. **Strictly**
/// validates the escape grammar (issue #77): only `\\`, `\"`, `\n`, `\t`, `\r`
/// are recognized; an unterminated quote, a dangling escape at end of value, or
/// an unknown escape (`\q`) is rejected as [`LogsIngestError::LokiDecode`] (a
/// whole-request 400). Prior behaviour kept an unknown escape's byte verbatim —
/// lenient, contradicting the surrounding doc-comments; the receiver now
/// rejects malformed escapes rather than silently reinterpreting them.
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
                        return Err(LogsIngestError::LokiDecode(format!(
                            "stream labels {input:?}: invalid escape sequence \\{} in value \
                             (only \\\\, \\\", \\n, \\t, \\r are recognized)",
                            other as char
                        )));
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

/// The Loki JSON push envelope (`{"streams":[...]}`). Hand-written
/// [`serde::Deserialize`] (issue #77): the `streams` array is bounded at
/// [`MAX_STREAMS_PER_REQUEST`] **during** deserialization, and every stream is
/// seeded with one **shared** cross-stream [`Cell`](std::cell::Cell) entry
/// counter so the per-stream `values` arrays cannot collectively exceed
/// [`MAX_TOTAL_ENTRIES_PER_REQUEST`] before the count check runs — the JSON
/// analog of [`PushRequest`]'s transient `total_entries` accumulator, closing
/// the same decode-before-limit amplification. A missing `streams` key yields
/// an empty request (the prior `#[serde(default)]` behaviour).
struct JsonPush {
    streams: Vec<JsonStream>,
}

/// One Loki stream: a `stream` label map (bounded at [`MAX_LABELS_PER_STREAM`]
/// **during** deserialization by [`BoundedLabelMap`], counting RAW pairs so a
/// duplicate JSON key cannot evade the cap — the same anti-evasion posture as
/// [`BoundedStructuredMetadata`]) and a `values` array (bounded per-stream at
/// [`MAX_ENTRIES_PER_STREAM`] and across streams via the shared aggregate
/// counter). Deserialized only through [`StreamSeed`], which threads that
/// shared counter in; a missing key yields the prior `#[serde(default)]` empty.
struct JsonStream {
    stream: std::collections::BTreeMap<String, String>,
    values: Vec<JsonEntry>,
}

impl<'de> serde::Deserialize<'de> for JsonPush {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use std::cell::Cell;

        struct PushVisitor;
        impl<'de> serde::de::Visitor<'de> for PushVisitor {
            type Value = JsonPush;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a Loki push object with a `streams` array")
            }

            fn visit_map<A>(self, mut map: A) -> Result<JsonPush, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                // One shared counter for the whole request — every stream's
                // `values` visitor increments it, so the aggregate is enforced
                // across streams, not merely per stream.
                let total_entries = Cell::new(0usize);
                // Shared decode-time byte estimate for the whole request (issue
                // #168), threaded alongside `total_entries` so every stream /
                // entry / label-map charge accumulates across streams.
                let decoded_bytes = Cell::new(0usize);
                let mut streams: Option<Vec<JsonStream>> = None;
                while let Some(key) = map.next_key::<std::borrow::Cow<str>>()? {
                    if key == "streams" {
                        if streams.is_some() {
                            return Err(serde::de::Error::duplicate_field("streams"));
                        }
                        streams = Some(map.next_value_seed(StreamsSeed {
                            total_entries: &total_entries,
                            decoded_bytes: &decoded_bytes,
                        })?);
                    } else {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }
                Ok(JsonPush {
                    streams: streams.unwrap_or_default(),
                })
            }
        }

        deserializer.deserialize_map(PushVisitor)
    }
}

/// Charges `weight` estimated decoded bytes to the shared `decoded_bytes`
/// counter (issue #168), rejecting (strictly-greater, exactly-at admits) once
/// the running total would exceed [`MAX_DECODED_BYTES`]. The reject surfaces
/// through [`LogsIngestError::LokiDecode`] (serde has no `OversizeMessage`
/// channel without materializing past the budget — the #127 JSON-side rationale)
/// and pins the family-wide `"decoded bytes (estimated)"` field text, the
/// running total at the crossing (so a test can read the reported estimate and
/// prove the one-element overshoot bound), and the budget value.
fn charge_json_decoded_bytes<E: serde::de::Error>(
    decoded_bytes: &std::cell::Cell<usize>,
    weight: usize,
) -> Result<(), E> {
    let new_total = decoded_bytes.get().saturating_add(weight);
    if new_total > MAX_DECODED_BYTES {
        return Err(serde::de::Error::custom(format!(
            "decoded bytes (estimated) {new_total} exceed the request decode budget of \
             {MAX_DECODED_BYTES}"
        )));
    }
    decoded_bytes.set(new_total);
    Ok(())
}

/// Bounded [`DeserializeSeed`](serde::de::DeserializeSeed) for the `streams`
/// array: caps element count at [`MAX_STREAMS_PER_REQUEST`] and seeds each
/// element with the shared aggregate counter. Mirrors
/// [`BoundedStructuredMetadata`]'s abort-before-materializing-the-remainder.
struct StreamsSeed<'c> {
    total_entries: &'c std::cell::Cell<usize>,
    decoded_bytes: &'c std::cell::Cell<usize>,
}

impl<'de> serde::de::DeserializeSeed<'de> for StreamsSeed<'_> {
    type Value = Vec<JsonStream>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StreamsVisitor<'c> {
            total_entries: &'c std::cell::Cell<usize>,
            decoded_bytes: &'c std::cell::Cell<usize>,
        }
        impl<'de> serde::de::Visitor<'de> for StreamsVisitor<'_> {
            type Value = Vec<JsonStream>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an array of Loki streams")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut streams: Vec<JsonStream> = Vec::new();
                while let Some(stream) = seq.next_element_seed(StreamSeed {
                    total_entries: self.total_entries,
                    decoded_bytes: self.decoded_bytes,
                })? {
                    if streams.len() >= MAX_STREAMS_PER_REQUEST {
                        // Charge-before-allocate: reject the over-cap stream
                        // without retaining the remainder of the array.
                        return Err(serde::de::Error::custom(format!(
                            "streams exceeds the {MAX_STREAMS_PER_REQUEST} per-request bound"
                        )));
                    }
                    // Charge this stream's shell bytes (issue #168) before
                    // retaining it — its entries and label pairs were charged
                    // during their own deserialization inside `StreamSeed`.
                    charge_json_decoded_bytes(
                        self.decoded_bytes,
                        std::mem::size_of::<JsonStream>(),
                    )?;
                    streams.push(stream);
                }
                Ok(streams)
            }
        }
        deserializer.deserialize_seq(StreamsVisitor {
            total_entries: self.total_entries,
            decoded_bytes: self.decoded_bytes,
        })
    }
}

/// Bounded [`DeserializeSeed`](serde::de::DeserializeSeed) for one
/// [`JsonStream`], threading the shared cross-stream aggregate counter into its
/// `values` visitor.
struct StreamSeed<'c> {
    total_entries: &'c std::cell::Cell<usize>,
    decoded_bytes: &'c std::cell::Cell<usize>,
}

impl<'de> serde::de::DeserializeSeed<'de> for StreamSeed<'_> {
    type Value = JsonStream;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StreamVisitor<'c> {
            total_entries: &'c std::cell::Cell<usize>,
            decoded_bytes: &'c std::cell::Cell<usize>,
        }
        impl<'de> serde::de::Visitor<'de> for StreamVisitor<'_> {
            type Value = JsonStream;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a Loki stream object with `stream` and `values`")
            }

            fn visit_map<A>(self, mut map: A) -> Result<JsonStream, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut stream: Option<std::collections::BTreeMap<String, String>> = None;
                let mut values: Option<Vec<JsonEntry>> = None;
                while let Some(key) = map.next_key::<std::borrow::Cow<str>>()? {
                    match key.as_ref() {
                        "stream" => {
                            if stream.is_some() {
                                return Err(serde::de::Error::duplicate_field("stream"));
                            }
                            let labels = map.next_value::<BoundedLabelMap>()?.0;
                            // Charge the RETAINED (post-dedup) label pairs (issue
                            // #168): the raw-pair count is already capped at
                            // MAX_LABELS_PER_STREAM by `BoundedLabelMap`, so the
                            // over-step is bounded to one map.
                            charge_json_decoded_bytes(
                                self.decoded_bytes,
                                labels
                                    .len()
                                    .saturating_mul(std::mem::size_of::<(String, String)>()),
                            )?;
                            stream = Some(labels);
                        }
                        "values" => {
                            if values.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values = Some(map.next_value_seed(ValuesSeed {
                                total_entries: self.total_entries,
                                decoded_bytes: self.decoded_bytes,
                            })?);
                        }
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(JsonStream {
                    stream: stream.unwrap_or_default(),
                    values: values.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_map(StreamVisitor {
            total_entries: self.total_entries,
            decoded_bytes: self.decoded_bytes,
        })
    }
}

/// The per-stream `stream` label map, bounded at [`MAX_LABELS_PER_STREAM`]
/// **during** deserialization. Counts RAW `next_entry` pairs (not the
/// dedup-collapsing `BTreeMap` length) so a duplicate JSON key cannot evade the
/// cap — the same rationale as [`BoundedStructuredMetadata`]. Last-write-wins
/// dedup (the prior `BTreeMap` semantics) is preserved for the retained value.
struct BoundedLabelMap(std::collections::BTreeMap<String, String>);

impl<'de> serde::Deserialize<'de> for BoundedLabelMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LabelMapVisitor;
        impl<'de> serde::de::Visitor<'de> for LabelMapVisitor {
            type Value = std::collections::BTreeMap<String, String>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a Loki stream label map of string values")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut labels = std::collections::BTreeMap::new();
                let mut seen = 0usize;
                while let Some((k, v)) = map.next_entry::<String, String>()? {
                    if seen >= MAX_LABELS_PER_STREAM {
                        // Charge-before-allocate on RAW pair count, so duplicate
                        // keys cannot collapse under the cap.
                        return Err(serde::de::Error::custom(format!(
                            "stream labels exceed the {MAX_LABELS_PER_STREAM} per-stream bound"
                        )));
                    }
                    seen += 1;
                    labels.insert(k, v);
                }
                Ok(labels)
            }
        }
        deserializer.deserialize_map(LabelMapVisitor).map(Self)
    }
}

/// Bounded [`DeserializeSeed`](serde::de::DeserializeSeed) for a stream's
/// `values` array: caps element count per stream at [`MAX_ENTRIES_PER_STREAM`]
/// and charges each entry into the shared cross-stream aggregate counter,
/// rejecting once it exceeds [`MAX_TOTAL_ENTRIES_PER_REQUEST`] — both **during**
/// deserialization, before the `Vec<JsonEntry>` grows past the cap.
struct ValuesSeed<'c> {
    total_entries: &'c std::cell::Cell<usize>,
    decoded_bytes: &'c std::cell::Cell<usize>,
}

impl<'de> serde::de::DeserializeSeed<'de> for ValuesSeed<'_> {
    type Value = Vec<JsonEntry>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValuesVisitor<'c> {
            total_entries: &'c std::cell::Cell<usize>,
            decoded_bytes: &'c std::cell::Cell<usize>,
        }
        impl<'de> serde::de::Visitor<'de> for ValuesVisitor<'_> {
            type Value = Vec<JsonEntry>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an array of Loki log entries")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut values: Vec<JsonEntry> = Vec::new();
                while let Some(entry) = seq.next_element::<JsonEntry>()? {
                    if values.len() >= MAX_ENTRIES_PER_STREAM {
                        return Err(serde::de::Error::custom(format!(
                            "entries exceeds the {MAX_ENTRIES_PER_STREAM} per-stream bound"
                        )));
                    }
                    let new_total = self.total_entries.get().saturating_add(1);
                    if new_total > MAX_TOTAL_ENTRIES_PER_REQUEST {
                        return Err(serde::de::Error::custom(format!(
                            "total_entries exceeds the {MAX_TOTAL_ENTRIES_PER_REQUEST} \
                             per-request aggregate bound"
                        )));
                    }
                    self.total_entries.set(new_total);
                    // Charge this entry's bytes (issue #168) after it
                    // deserializes but before it is retained — the entry's own
                    // structured-metadata fan-out is capped at
                    // MAX_STRUCTURED_METADATA_PER_ENTRY by
                    // `BoundedStructuredMetadata`, so the over-step is bounded to
                    // one entry (≈ 12 KiB).
                    charge_json_decoded_bytes(
                        self.decoded_bytes,
                        std::mem::size_of::<JsonEntry>().saturating_add(
                            entry
                                .structured_metadata
                                .len()
                                .saturating_mul(std::mem::size_of::<(String, String)>()),
                        ),
                    )?;
                    values.push(entry);
                }
                Ok(values)
            }
        }
        deserializer.deserialize_seq(ValuesVisitor {
            total_entries: self.total_entries,
            decoded_bytes: self.decoded_bytes,
        })
    }
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

    // -- decode-time DoS bounds (issue #77) --------------------------------
    //
    // These prove rejection happens BEFORE full materialization, not merely
    // that the request is rejected. The protobuf arms decode into the bounded
    // decode struct and inspect the materialized length (a length-cap the
    // derived decode would blow past — the non-vacuity property); the JSON arms
    // assert the bounded serde visitor's own `LokiDecode` message fired, which
    // the derived-then-`validate_bounds` path (an `OversizeMessage` AFTER full
    // materialization) never produces.

    /// One empty `StreamAdapter` wire record (`PushRequest.streams`, tag 1,
    /// length-delimited, zero-length payload): `0x0a 0x00`.
    fn empty_stream_record() -> [u8; 2] {
        [0x0a, 0x00]
    }

    /// One empty `EntryAdapter` wire record (`StreamAdapter.entries`, tag 2,
    /// length-delimited, zero-length payload): `0x12 0x00`.
    fn empty_entry_record() -> [u8; 2] {
        [0x12, 0x00]
    }

    #[test]
    fn decode_caps_stream_materialization_and_rejects_too_many_streams() {
        // AC (too many streams / protobuf): a body encoding more than
        // MAX_STREAMS_PER_REQUEST streams must NOT materialize them all — the
        // hand-written decoder caps the vec at MAX + 1 and drains the rest
        // without allocating. Non-vacuous: the derived decode would materialize
        // every encoded stream, so this length assertion would fail against it.
        let encoded = MAX_STREAMS_PER_REQUEST + 8;
        let mut body = Vec::with_capacity(encoded * 2);
        for _ in 0..encoded {
            body.extend_from_slice(&empty_stream_record());
        }
        let bounded = BoundedPushRequest::decode(body.as_slice()).expect("empty streams decode");
        assert_eq!(
            bounded.streams.len(),
            MAX_STREAMS_PER_REQUEST + 1,
            "the decoder must cap materialization at MAX + 1, not materialize all encoded streams"
        );
        let err = decode_protobuf(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "streams",
                ..
            }
        ));
    }

    #[test]
    fn decode_caps_entry_materialization_and_rejects_too_many_entries() {
        // AC (too many entries-per-stream / protobuf): one stream carrying more
        // than MAX_ENTRIES_PER_STREAM entries caps at MAX + 1 during decode.
        let encoded = MAX_ENTRIES_PER_STREAM + 8;
        let mut stream_payload = Vec::with_capacity(encoded * 2);
        for _ in 0..encoded {
            stream_payload.extend_from_slice(&empty_entry_record());
        }
        let body = field_ld(1, &stream_payload);
        let bounded = BoundedPushRequest::decode(body.as_slice()).expect("one-stream decode");
        assert_eq!(bounded.streams.len(), 1);
        assert_eq!(
            bounded.streams[0].entries.len(),
            MAX_ENTRIES_PER_STREAM + 1,
            "the decoder must cap per-stream entry materialization at MAX + 1"
        );
        let err = decode_protobuf(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "entries",
                ..
            }
        ));
    }

    #[test]
    fn decode_drains_streams_once_the_cross_stream_aggregate_is_exceeded() {
        // AC-9 anti-evasion (aggregate / protobuf): every stream stays UNDER
        // MAX_ENTRIES_PER_STREAM, but their entry counts SUM past
        // MAX_TOTAL_ENTRIES_PER_REQUEST. The transient accumulators stop
        // materializing streams/entries once a running total is exceeded, so
        // fewer streams are materialized than encoded (the derived decode would
        // materialize them all — the non-vacuity property).
        //
        // Issue #168: 5.2M empty entries at size_of::<EntryAdapter>() (~72 B)
        // each ≈ 374 MB, so the decode-time BYTE budget (256 MiB) drains at
        // ~3.7M entries — BEFORE the 5M total_entries count cap is reached. The
        // deferred reject is therefore `"decoded bytes (estimated)"`, not
        // `"total_entries"`; the count cap stays an enforced backstop (the
        // structural drain assertions below still hold).
        let per = MAX_ENTRIES_PER_STREAM; // 100_000, each stream in-bounds
        let encoded_streams = MAX_TOTAL_ENTRIES_PER_REQUEST / per + 2; // 52 -> 5.2M > 5M
        let mut stream_payload = Vec::with_capacity(per * 2);
        for _ in 0..per {
            stream_payload.extend_from_slice(&empty_entry_record());
        }
        let stream_record = field_ld(1, &stream_payload);
        let mut body = Vec::with_capacity(stream_record.len() * encoded_streams);
        for _ in 0..encoded_streams {
            body.extend_from_slice(&stream_record);
        }

        let bounded = BoundedPushRequest::decode(body.as_slice()).expect("aggregate decode");
        let materialized: usize = bounded.streams.iter().map(|s| s.entries.len()).sum();
        assert!(
            bounded.streams.len() < encoded_streams,
            "the decoder must drain streams once a budget is exceeded \
             (materialized {} of {encoded_streams} encoded streams)",
            bounded.streams.len()
        );
        assert!(
            materialized <= MAX_TOTAL_ENTRIES_PER_REQUEST + MAX_ENTRIES_PER_STREAM,
            "aggregate fan-out must be bounded to MAX_TOTAL + one stream's cap, got {materialized}"
        );

        let err = decode_protobuf(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "decoded bytes (estimated)",
                ..
            }
        ));
    }

    #[test]
    fn push_request_decode_is_no_longer_an_uncapped_bypass() {
        // Finding #115: a direct `PushRequest::decode` (the public wire type's
        // own decoder) must NOT materialize an unbounded stream fan-out the
        // ingest path's caps would reject. The hand-written impl (no derive)
        // routes decode through the bounded twin, capping `streams` at MAX + 1
        // — the derived decoder would materialize every encoded stream (the
        // non-vacuity property: this length assertion would fail against it).
        let encoded = MAX_STREAMS_PER_REQUEST + 8;
        let mut body = Vec::with_capacity(encoded * 2);
        for _ in 0..encoded {
            body.extend_from_slice(&empty_stream_record());
        }
        let decoded = PushRequest::decode(body.as_slice()).expect("bounded PushRequest::decode");
        assert_eq!(
            decoded.streams.len(),
            MAX_STREAMS_PER_REQUEST + 1,
            "PushRequest::decode must cap materialization at MAX + 1, not materialize all \
             encoded streams"
        );
        // The public Loki-push decode entry still converts the +1 sentinel into
        // a whole-request reject (Loki all-or-nothing).
        let err = decode_protobuf(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "streams",
                ..
            }
        ));
    }

    #[test]
    fn push_request_decode_drains_the_cross_stream_aggregate() {
        // Finding #115: `PushRequest::decode` routes through the bounded twin,
        // so it also drains streams once the cross-stream aggregate exceeds
        // MAX_TOTAL_ENTRIES_PER_REQUEST — the derived decoder would materialize
        // every encoded stream (non-vacuity).
        let per = MAX_ENTRIES_PER_STREAM; // each stream in-bounds
        let encoded_streams = MAX_TOTAL_ENTRIES_PER_REQUEST / per + 2; // sum > aggregate
        let mut stream_payload = Vec::with_capacity(per * 2);
        for _ in 0..per {
            stream_payload.extend_from_slice(&empty_entry_record());
        }
        let stream_record = field_ld(1, &stream_payload);
        let mut body = Vec::with_capacity(stream_record.len() * encoded_streams);
        for _ in 0..encoded_streams {
            body.extend_from_slice(&stream_record);
        }
        let decoded = PushRequest::decode(body.as_slice()).expect("bounded PushRequest::decode");
        assert!(
            decoded.streams.len() < encoded_streams,
            "PushRequest::decode must drain streams once the aggregate is exceeded \
             (materialized {} of {encoded_streams} encoded)",
            decoded.streams.len()
        );
    }

    /// A bare protobuf length-delimited prefix (a message-length varint, no tag)
    /// followed by the payload — the framing `Message::merge_length_delimited`
    /// consumes.
    fn length_delimited(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(payload.len() + 5);
        prost::encoding::encode_varint(payload.len() as u64, &mut out);
        out.extend_from_slice(payload);
        out
    }

    /// Encodes `encoded_streams` in-bounds streams (each `per` empty entries)
    /// whose entry counts SUM past MAX_TOTAL_ENTRIES_PER_REQUEST — the raw-merge
    /// analog of the aggregate-drain decode fixtures.
    fn cross_stream_aggregate_body(per: usize, encoded_streams: usize) -> Vec<u8> {
        let mut stream_payload = Vec::with_capacity(per * 2);
        for _ in 0..per {
            stream_payload.extend_from_slice(&empty_entry_record());
        }
        let stream_record = field_ld(1, &stream_payload);
        let mut body = Vec::with_capacity(stream_record.len() * encoded_streams);
        for _ in 0..encoded_streams {
            body.extend_from_slice(&stream_record);
        }
        body
    }

    /// Asserts that a raw-`merge`-decoded request bounded its cross-stream fan-out
    /// (drained streams once the aggregate was exceeded) rather than retaining the
    /// full encoded set. Non-vacuous: the pre-fix `PushRequest::merge_field`
    /// capped only stream count, so it would retain all `encoded_streams` (here
    /// `52 * 100_000 = 5.2M > 5M + 100k`), failing this bound.
    fn assert_aggregate_bounded(streams: &[StreamAdapter], encoded_streams: usize) {
        let materialized: usize = streams.iter().map(|s| s.entries.len()).sum();
        assert!(
            streams.len() < encoded_streams,
            "the raw merge path must drain streams once the aggregate is exceeded \
             (retained {} of {encoded_streams} encoded)",
            streams.len()
        );
        assert!(
            materialized <= MAX_TOTAL_ENTRIES_PER_REQUEST + MAX_ENTRIES_PER_STREAM,
            "the raw merge path must bound aggregate fan-out to MAX_TOTAL + one \
             stream's cap, got {materialized}"
        );
    }

    #[test]
    fn push_request_merge_enforces_the_cross_stream_aggregate() {
        // Finding #115 round 2: `prost`'s default `Message::merge` calls
        // `PushRequest::merge_field` directly (stream-count cap only), so a raw
        // `PushRequest::default().merge(buf)` must NOT retain a > MAX_TOTAL
        // fan-out the ingest path would reject. The override routes it through
        // the bounded twin, draining streams once the aggregate is exceeded.
        let per = MAX_ENTRIES_PER_STREAM; // each stream in-bounds
        let encoded_streams = MAX_TOTAL_ENTRIES_PER_REQUEST / per + 2; // sum > aggregate
        let body = cross_stream_aggregate_body(per, encoded_streams);

        let mut req = PushRequest::default();
        req.merge(body.as_slice()).expect("bounded raw merge");
        assert_aggregate_bounded(&req.streams, encoded_streams);
    }

    #[test]
    fn push_request_merge_length_delimited_enforces_the_cross_stream_aggregate() {
        // The `merge_length_delimited` sibling entry point loops through
        // `merge_field` directly too, so it gets the identical bounded-twin
        // routing — a length-delimited over-aggregate payload is bounded, never
        // retained in full.
        let per = MAX_ENTRIES_PER_STREAM;
        let encoded_streams = MAX_TOTAL_ENTRIES_PER_REQUEST / per + 2;
        let framed = length_delimited(&cross_stream_aggregate_body(per, encoded_streams));

        let mut req = PushRequest::default();
        req.merge_length_delimited(framed.as_slice())
            .expect("bounded raw merge_length_delimited");
        assert_aggregate_bounded(&req.streams, encoded_streams);
    }

    /// A pre-existing request to merge malformed input INTO — one real stream,
    /// so the retention assertions below have something to lose.
    fn request_with_one_stream() -> PushRequest {
        PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{service_name="checkout"}"#.to_string(),
                entries: vec![entry(1, 0, "hello")],
            }],
        }
    }

    #[test]
    fn merge_of_malformed_bytes_retains_pre_existing_streams() {
        // Finding #115 round 3: a failed raw `merge` must NOT drop the caller's
        // pre-existing streams. The override moves self's streams into the
        // bounded twin, so an early `?` on decode error would leave self EMPTY
        // (data loss). The fix restores the twin's streams on BOTH paths, giving
        // prost partial-merge semantics. Non-vacuous: against the pre-fix
        // `mem::take(...); bounded.merge(buf)?` code, `req.streams` is empty here.
        let original = request_with_one_stream();
        let mut req = original.clone();
        // The returned error is statically a `prost::DecodeError` (the merge
        // signature), so `expect_err` alone proves the decode failed.
        req.merge(b"\xff\xff\xff not a protobuf message".as_slice())
            .expect_err("malformed merge must fail");
        assert_eq!(
            req, original,
            "a failed merge must retain the pre-existing streams, not empty them"
        );
    }

    #[test]
    fn merge_length_delimited_of_malformed_bytes_retains_pre_existing_streams() {
        // The `merge_length_delimited` sibling gets the identical round-3
        // error-path restoration — a malformed framed payload leaves the
        // caller's pre-existing streams intact.
        let original = request_with_one_stream();
        let mut req = original.clone();
        let framed = length_delimited(b"\xff\xff\xff not a protobuf message");
        req.merge_length_delimited(framed.as_slice())
            .expect_err("malformed merge_length_delimited must fail");
        assert_eq!(
            req, original,
            "a failed merge_length_delimited must retain the pre-existing streams"
        );
    }

    #[test]
    fn parse_label_set_rejects_too_many_labels() {
        // AC (too many labels / protobuf label-set literal): more than
        // MAX_LABELS_PER_STREAM pairs in the `{...}` literal is an OversizeMessage.
        let mut lit = String::from("{");
        for i in 0..=MAX_LABELS_PER_STREAM {
            if i > 0 {
                lit.push(',');
            }
            lit.push_str(&format!(r#"k{i}="v""#));
        }
        lit.push('}');
        let err = parse_label_set(&lit).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "labels",
                ..
            }
        ));
    }

    // -- strict label grammar (issue #77) ----------------------------------

    #[test]
    fn read_key_rejects_invalid_label_names() {
        // A leading digit, a dot, and a non-ASCII byte each violate
        // [a-zA-Z_][a-zA-Z0-9_]* and must reject (previously silently accepted).
        for bad in [r#"{9bad="v"}"#, r#"{a.b="v"}"#, "{naïve=\"v\"}"] {
            let err = parse_label_set(bad).unwrap_err();
            let LogsIngestError::LokiDecode(msg) = err else {
                panic!("expected LokiDecode for {bad:?}, got a different variant");
            };
            assert!(
                msg.contains("invalid label name"),
                "the reject must name the invalid-label-name grammar for {bad:?}: {msg:?}"
            );
        }
    }

    #[test]
    fn read_quoted_rejects_unknown_escape() {
        // `\q` is not one of \\ \" \n \t \r — previously kept verbatim, now a
        // whole-request reject.
        let err = parse_label_set(r#"{a="x\q"}"#).unwrap_err();
        let LogsIngestError::LokiDecode(msg) = err else {
            panic!("expected LokiDecode, got a different variant");
        };
        assert!(
            msg.contains("invalid escape sequence"),
            "the reject must name the invalid escape: {msg:?}"
        );
    }

    #[test]
    fn strict_grammar_still_accepts_valid_names_and_escapes() {
        // Positive (no false reject): a valid name with digits/underscore and a
        // recognized escape still parse unchanged.
        let (labels, _) = parse_label_set(r#"{a_1="x\n"}"#).unwrap();
        assert_eq!(labels.get("a_1"), Some("x\n"));
    }

    // -- JSON decode-time DoS bounds (issue #77) ---------------------------

    fn json_loki_decode_message(body: &str) -> String {
        match parse_json(body.as_bytes(), 0).unwrap_err() {
            LogsIngestError::LokiDecode(msg) => msg,
            other => panic!("expected LokiDecode, got {other:?}"),
        }
    }

    #[test]
    fn parse_json_rejects_too_many_streams_during_deserialize() {
        // AC (too many streams / JSON): more than MAX_STREAMS_PER_REQUEST empty
        // stream objects. The bounded seed rejects DURING deserialize with its
        // own message — the non-vacuity signal vs. the derived + validate_bounds
        // `OversizeMessage`.
        let mut body = String::with_capacity(4 * MAX_STREAMS_PER_REQUEST);
        body.push_str(r#"{"streams":["#);
        for i in 0..=MAX_STREAMS_PER_REQUEST {
            if i > 0 {
                body.push(',');
            }
            body.push_str("{}");
        }
        body.push_str("]}");
        let msg = json_loki_decode_message(&body);
        assert!(
            msg.contains("streams exceeds"),
            "the reject must be the bounded-seed streams message: {msg:?}"
        );
    }

    #[test]
    fn parse_json_rejects_too_many_entries_per_stream_during_deserialize() {
        // AC (too many entries-per-stream / JSON).
        let mut body = String::new();
        body.push_str(r#"{"streams":[{"stream":{"a":"b"},"values":["#);
        for i in 0..=MAX_ENTRIES_PER_STREAM {
            if i > 0 {
                body.push(',');
            }
            body.push_str(r#"["1700000000000000000","x"]"#);
        }
        body.push_str("]}]}");
        let msg = json_loki_decode_message(&body);
        assert!(
            msg.contains("entries exceeds"),
            "the reject must be the bounded-seed entries message: {msg:?}"
        );
    }

    #[test]
    fn parse_json_rejects_cross_stream_aggregate_during_deserialize() {
        // AC-9 anti-evasion (aggregate / JSON): each stream carries exactly
        // MAX_ENTRIES_PER_STREAM values (individually in-bounds) but their entry
        // counts SUM past MAX_TOTAL_ENTRIES_PER_REQUEST.
        //
        // Issue #168: 5.1M metadata-less entries at size_of::<JsonEntry>() each
        // (~285 MB) cross the decode-time BYTE budget (256 MiB) at ~4.8M
        // entries — BEFORE the 5M total_entries count cap — so the reject is the
        // byte message, not `"total_entries exceeds"`; the count cap stays an
        // enforced backstop.
        let per = MAX_ENTRIES_PER_STREAM;
        let streams = MAX_TOTAL_ENTRIES_PER_REQUEST / per + 1; // 51 -> 5.1M
        let one_stream = {
            let mut s = String::from(r#"{"stream":{"a":"b"},"values":["#);
            for i in 0..per {
                if i > 0 {
                    s.push(',');
                }
                s.push_str(r#"["1700000000000000000","x"]"#);
            }
            s.push_str("]}");
            s
        };
        let mut body = String::from(r#"{"streams":["#);
        for i in 0..streams {
            if i > 0 {
                body.push(',');
            }
            body.push_str(&one_stream);
        }
        body.push_str("]}");
        let msg = json_loki_decode_message(&body);
        assert!(
            msg.contains("decoded bytes (estimated)"),
            "the reject must be the decode-time byte-budget message: {msg:?}"
        );
    }

    #[test]
    fn parse_json_rejects_oversized_label_map_during_deserialize() {
        // AC (oversized label map / JSON): more than MAX_LABELS_PER_STREAM keys
        // in one stream's `stream` map, rejected during the MapAccess visit.
        let mut map = String::from("{");
        for i in 0..=MAX_LABELS_PER_STREAM {
            if i > 0 {
                map.push(',');
            }
            map.push_str(&format!(r#""k{i}":"v""#));
        }
        map.push('}');
        let body =
            format!(r#"{{"streams":[{{"stream":{map},"values":[["1700000000000000000","x"]]}}]}}"#);
        let msg = json_loki_decode_message(&body);
        assert!(
            msg.contains("stream labels exceed"),
            "the reject must be the bounded label-map message: {msg:?}"
        );
    }

    #[test]
    fn parse_json_duplicate_label_keys_cannot_evade_the_map_cap() {
        // AC-9 anti-evasion (labels / JSON): a label map whose keys are all the
        // SAME string would collapse to one entry in a BTreeMap, evading the
        // cap; counting RAW pairs during the visit rejects it.
        let mut map = String::from("{");
        for i in 0..=MAX_LABELS_PER_STREAM {
            if i > 0 {
                map.push(',');
            }
            map.push_str(r#""dup":"v""#);
        }
        map.push('}');
        let body =
            format!(r#"{{"streams":[{{"stream":{map},"values":[["1700000000000000000","x"]]}}]}}"#);
        let msg = json_loki_decode_message(&body);
        assert!(
            msg.contains("stream labels exceed"),
            "duplicate keys must still trip the RAW-pair label-map cap: {msg:?}"
        );
    }

    #[test]
    fn parse_json_accepts_at_cap_labels_and_entries() {
        // Positive (no false reject): exactly MAX_LABELS_PER_STREAM distinct
        // labels and a small in-bounds values array still parse.
        let mut map = String::from("{");
        for i in 0..MAX_LABELS_PER_STREAM {
            if i > 0 {
                map.push(',');
            }
            map.push_str(&format!(r#""k{i}":"v{i}""#));
        }
        map.push('}');
        let body = format!(
            r#"{{"streams":[{{"stream":{map},"values":[["1700000000000000000","hello"]]}}]}}"#
        );
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].body, "hello");
    }

    #[test]
    fn parse_json_rejects_invalid_label_names() {
        // Finding #115: JSON label keys must be validated against the SAME
        // strict grammar as the protobuf path. A leading digit, a dot, and a
        // non-ASCII byte each violate [a-zA-Z_][a-zA-Z0-9_]* and must reject —
        // previously they were silently canonicalized by `from_normalized`.
        // Non-vacuous: the reject must be the grammar message (not some other
        // JSON error), and the same body shape with a valid key parses (see
        // `parse_json_accepts_valid_label_names`).
        for bad_key in ["9bad", "a.b", "naïve"] {
            let body = format!(
                r#"{{"streams":[{{"stream":{{"{bad_key}":"v"}},"values":[["1700000000000000000","x"]]}}]}}"#
            );
            let err = parse_json(body.as_bytes(), 0).unwrap_err();
            let LogsIngestError::LokiDecode(msg) = err else {
                panic!("expected LokiDecode for key {bad_key:?}, got a different variant");
            };
            assert!(
                msg.contains("is invalid") && msg.contains("must match"),
                "the reject must name the invalid-label-name grammar for {bad_key:?}: {msg:?}"
            );
        }
    }

    #[test]
    fn parse_json_accepts_valid_label_names() {
        // Positive (no false reject): valid names with a leading underscore,
        // digits, and underscores still parse on the JSON path — the non-vacuity
        // counterpart to `parse_json_rejects_invalid_label_names`.
        let body = r#"{"streams":[{"stream":{"_a1":"x","service_name":"checkout"},"values":[["1700000000000000000","hello"]]}]}"#;
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].body, "hello");
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
    fn parse_protobuf_far_future_month_is_a_whole_request_error_not_a_saturated_row() {
        // ~year 2200 (84_000 days after the epoch) in seconds: representable
        // as i64 ns but past the 2149-06-06 ClickHouse `Date` cutoff (and
        // past the tighter 2106-02-06 DateTime-safe cutoff, issue #137).
        // Before #8's fix the month saturated to day 65535, silently
        // orphaning the sample; now it is a whole-request `LokiDecode`
        // failure (Loki is all-or-nothing on a bad timestamp), never a
        // stored row.
        let far_future_secs = 86_400i64 * 84_000;
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{a="b"}"#.to_string(),
                entries: vec![entry(far_future_secs, 0, "x")],
            }],
        };
        let err = parse_protobuf(&req, 0).unwrap_err();
        let LogsIngestError::LokiDecode(msg) = err else {
            panic!("expected LokiDecode, got {err:?}");
        };
        assert!(msg.contains("outside the supported storage time range"));
    }

    #[test]
    fn parse_protobuf_last_datetime_safe_day_accepted_first_unsafe_day_rejected() {
        // Issue #137 (re-pointing #8's round-2 boundary pair from the `Date`
        // horizon to the DateTime-safe one): `log_samples` partitions by the
        // RAW sample day and its delete-TTL evaluates the row timestamp in
        // the 32-bit `DateTime` domain. Day 49_709 = 2106-02-06 is the last
        // UTC day fully inside that domain; day 49_710 = 2106-02-07 still
        // partitions correctly (inside the u16 `Date` range) but its TTL
        // seconds value exceeds u32::MAX — accepted (wrap-prone) before
        // #137. Loki is all-or-nothing, so the day-49_710 entry fails the
        // whole request while the day-49_709 request still parses (no
        // over-rejection).
        const SECS_PER_DAY: i64 = 86_400;
        let last_ok = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{a="b"}"#.to_string(),
                entries: vec![entry(SECS_PER_DAY * 49_709, 0, "ok")],
            }],
        };
        let out = parse_protobuf(&last_ok, 0).expect("day 49_709 is storage-safe");
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.streams.len(), 1);
        // Registers exactly its month (2106-02-01 = day 49_704).
        assert_eq!(out.streams[0].month.days_since_epoch(), 49_704);

        let first_bad = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{a="b"}"#.to_string(),
                entries: vec![entry(SECS_PER_DAY * 49_710, 0, "bad")],
            }],
        };
        let err = parse_protobuf(&first_bad, 0).unwrap_err();
        let LogsIngestError::LokiDecode(msg) = err else {
            panic!("expected LokiDecode, got {err:?}");
        };
        assert!(
            msg.contains("outside the supported storage time range (1970-01-01 to 2106-02-06 UTC)")
        );
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

    // -- issue #168: decode-time byte ceiling --------------------------------

    /// One `EntryAdapter` wire record (`StreamAdapter.entries`, tag 2) carrying
    /// `pairs` empty structured-metadata submessages (`LabelPairAdapter`, tag 3,
    /// zero-length payload `0x1a 0x00`). The empty pair is 2 wire bytes yet
    /// materializes `size_of::<LabelPairAdapter>()` heap bytes — the 24×
    /// amplification the byte budget bounds.
    fn wire_entry_with_pairs(pairs: usize) -> Vec<u8> {
        let mut payload = Vec::with_capacity(pairs * 2);
        for _ in 0..pairs {
            payload.extend_from_slice(&[0x1a, 0x00]);
        }
        field_ld(2, &payload)
    }

    /// AC 2 (weight identity, protobuf): the private byte-estimate helpers are
    /// mechanically `size_of`-weighted — hand-built values re-sum to the inline
    /// arithmetic, no magic numbers.
    #[test]
    fn decoded_bytes_helpers_are_size_of_weighted() {
        let pair = std::mem::size_of::<LabelPairAdapter>();
        let entry_shell = std::mem::size_of::<EntryAdapter>();
        let stream_shell = std::mem::size_of::<StreamAdapter>();

        let e0 = entry(1, 0, "line");
        assert_eq!(decoded_entry_bytes(&e0), entry_shell);
        let e3 = entry_with_sm(
            1,
            "line",
            vec![
                label_pair("a", "b"),
                label_pair("c", "d"),
                label_pair("e", "f"),
            ],
        );
        assert_eq!(decoded_entry_bytes(&e3), entry_shell + 3 * pair);

        let req = PushRequest {
            streams: vec![
                StreamAdapter {
                    labels: r#"{a="b"}"#.to_string(),
                    entries: vec![e0.clone(), e3.clone()],
                },
                StreamAdapter {
                    labels: String::new(),
                    entries: vec![e0],
                },
            ],
        };
        // 2 stream shells + (e0 + e3) + (e0) = 2*shell + (entry_shell) +
        // (entry_shell + 3*pair) + (entry_shell).
        let expected = 2 * stream_shell + 3 * entry_shell + 3 * pair;
        assert_eq!(decoded_push_request_bytes(&req.streams), expected);
    }

    /// AC 3 (protobuf boundary, 2a): a fixture whose re-summed estimate lands
    /// within ONE `LabelPairAdapter` leaf of the budget decodes Ok; adding one
    /// more pair rejects with the byte-budget `OversizeMessage`. An exact hit is
    /// arithmetically impossible (every charge is a multiple of
    /// gcd(shells, pair) = 24, and 24 ∤ `MAX_DECODED_BYTES`), so a within-one-leaf
    /// boundary is the tightest achievable — the fixture self-asserts it.
    #[test]
    fn protobuf_byte_budget_boundary_admits_then_rejects_one_more_pair() {
        let pair = std::mem::size_of::<LabelPairAdapter>();
        let entry_shell = std::mem::size_of::<EntryAdapter>();
        let stream_shell = std::mem::size_of::<StreamAdapter>();
        let full_w = entry_shell + MAX_STRUCTURED_METADATA_PER_ENTRY * pair;

        // N full (256-pair) entries + one r-pair entry (r <= 255 so the +1-pair
        // reject stays within the 256 cap) with resum <= MAX < resum + pair.
        let mut chosen: Option<(usize, usize, usize)> = None;
        for r in 0..MAX_STRUCTURED_METADATA_PER_ENTRY {
            let base = stream_shell + entry_shell + r * pair;
            if base > MAX_DECODED_BYTES {
                continue;
            }
            let n = (MAX_DECODED_BYTES - base) / full_w;
            let resum = base + n * full_w;
            if resum <= MAX_DECODED_BYTES && MAX_DECODED_BYTES < resum + pair {
                chosen = Some((n, r, resum));
                break;
            }
        }
        let (n, r, resum) = chosen.expect("a within-one-pair boundary fixture must exist");
        assert!(resum <= MAX_DECODED_BYTES && MAX_DECODED_BYTES < resum + pair);
        assert!(n < MAX_ENTRIES_PER_STREAM, "entries fit one stream");

        let build = |last_pairs: usize| -> Vec<u8> {
            let mut entries = Vec::new();
            for _ in 0..n {
                entries
                    .extend_from_slice(&wire_entry_with_pairs(MAX_STRUCTURED_METADATA_PER_ENTRY));
            }
            entries.extend_from_slice(&wire_entry_with_pairs(last_pairs));
            field_ld(1, &entries)
        };

        // Admit side: exactly at the boundary, decodes Ok and re-sums to `resum`.
        let admit = decode_protobuf(&build(r)).expect("boundary fixture decodes Ok");
        assert_eq!(decoded_push_request_bytes(&admit.streams), resum);

        // Reject side: one more pair crosses the budget by a single leaf.
        let err = decode_protobuf(&build(r + 1)).unwrap_err();
        match err {
            LogsIngestError::OversizeMessage { field, actual, .. } => {
                assert_eq!(field, "decoded bytes (estimated)");
                assert_eq!(actual, resum + pair);
            }
            other => panic!("expected the byte-budget OversizeMessage, got {other:?}"),
        }
    }

    /// AC 4 + AC 13 (protobuf 2b + bounded overshoot): a body of uniform
    /// 256-pair entries crossing the budget rejects with the byte field, and the
    /// reported estimate `T` satisfies `MAX < T <= MAX + w` where `w` is ONE
    /// entry's maximum retained weight — `size_of::<EntryAdapter>() +
    /// (MAX_STRUCTURED_METADATA_PER_ENTRY + 1) * size_of::<LabelPairAdapter>()`
    /// (the `+ 1` because the tag-3 drain fires PAST the cap, so a retained entry
    /// reaches up to 257 pairs). A >= 2-entry overshoot would report
    /// `T > MAX + w` and fail.
    #[test]
    fn protobuf_over_budget_rejects_with_one_entry_bounded_overshoot() {
        let pair = std::mem::size_of::<LabelPairAdapter>();
        let entry_shell = std::mem::size_of::<EntryAdapter>();
        let full_w = entry_shell + MAX_STRUCTURED_METADATA_PER_ENTRY * pair;
        // One entry's MAXIMUM retained weight (drain fires past the cap -> 257).
        let w = entry_shell + (MAX_STRUCTURED_METADATA_PER_ENTRY + 1) * pair;

        // Enough 256-pair entries to cross, plus >= 2 more (self-asserted under
        // every count cap so the BYTE gate, not a count cap, fires).
        let n = MAX_DECODED_BYTES / full_w + 8;
        assert!(n <= MAX_ENTRIES_PER_STREAM, "entries fit one stream");
        assert!(n * full_w > MAX_DECODED_BYTES, "fixture crosses the budget");

        let mut entries = Vec::new();
        for _ in 0..n {
            entries.extend_from_slice(&wire_entry_with_pairs(MAX_STRUCTURED_METADATA_PER_ENTRY));
        }
        let body = field_ld(1, &entries);

        let err = decode_protobuf(&body).unwrap_err();
        match err {
            LogsIngestError::OversizeMessage {
                field,
                limit,
                actual,
            } => {
                assert_eq!(field, "decoded bytes (estimated)");
                assert_eq!(limit, MAX_DECODED_BYTES);
                assert!(
                    MAX_DECODED_BYTES < actual && actual <= MAX_DECODED_BYTES + w,
                    "one-entry bounded overshoot: {} < {actual} <= {}",
                    MAX_DECODED_BYTES,
                    MAX_DECODED_BYTES + w
                );
            }
            other => panic!("expected the byte-budget OversizeMessage, got {other:?}"),
        }
    }

    /// AC 6 (merge seeding): two sequential raw merges whose combined estimate
    /// exceeds the budget leave materialization within one entry of the budget —
    /// proving the twin seeds `decoded_bytes` with the shared re-sum (a
    /// no-seed regression would retain `2 * half` ≈ 1.2× the budget and fail the
    /// ceiling).
    #[test]
    fn protobuf_merge_seeds_the_byte_budget_across_raw_merges() {
        let pair = std::mem::size_of::<LabelPairAdapter>();
        let entry_shell = std::mem::size_of::<EntryAdapter>();
        let stream_shell = std::mem::size_of::<StreamAdapter>();
        let full_w = entry_shell + MAX_STRUCTURED_METADATA_PER_ENTRY * pair;
        let w = entry_shell + (MAX_STRUCTURED_METADATA_PER_ENTRY + 1) * pair;

        // Each half is ~0.6× the budget (under it alone); two combine to ~1.2×.
        let half_n = (MAX_DECODED_BYTES * 6 / 10) / full_w;
        let half_est = stream_shell + half_n * full_w;
        assert!(half_est < MAX_DECODED_BYTES, "one half stays under budget");
        assert!(
            2 * half_est > MAX_DECODED_BYTES + w,
            "combined must exceed the budget by more than one entry (discriminating)"
        );
        assert!(half_n <= MAX_ENTRIES_PER_STREAM);

        let mut entries = Vec::new();
        for _ in 0..half_n {
            entries.extend_from_slice(&wire_entry_with_pairs(MAX_STRUCTURED_METADATA_PER_ENTRY));
        }
        let body = field_ld(1, &entries);

        let mut req = PushRequest::default();
        req.merge(body.as_slice()).expect("first raw merge");
        req.merge(body.as_slice()).expect("second raw merge");
        let resum = decoded_push_request_bytes(&req.streams);
        assert!(
            MAX_DECODED_BYTES < resum && resum <= MAX_DECODED_BYTES + w,
            "the seed must charge the pre-existing merge's bytes so the second \
             merge drains within one entry of the budget: {} < {resum} <= {}",
            MAX_DECODED_BYTES,
            MAX_DECODED_BYTES + w
        );
    }

    /// AC 7 (malformed-wire precedence): an over-budget body whose drained tail
    /// is malformed fails as a prost `Decode` error, never the byte reject — the
    /// drains stay wire-type-checked (a non-length-delimited tag-1 is a
    /// malformed stream, not a silent skip).
    #[test]
    fn protobuf_malformed_tail_precedes_the_byte_reject() {
        let pair = std::mem::size_of::<LabelPairAdapter>();
        let entry_shell = std::mem::size_of::<EntryAdapter>();
        let full_w = entry_shell + MAX_STRUCTURED_METADATA_PER_ENTRY * pair;
        let n = MAX_DECODED_BYTES / full_w + 4;
        assert!(n <= MAX_ENTRIES_PER_STREAM);

        let mut entries = Vec::new();
        for _ in 0..n {
            entries.extend_from_slice(&wire_entry_with_pairs(MAX_STRUCTURED_METADATA_PER_ENTRY));
        }
        let mut body = field_ld(1, &entries);
        // A top-level tag-1 (streams) with wire type 0 (varint) — a malformed
        // stream record the post-budget drain must reject on the wire-type check.
        body.extend_from_slice(&[0x08, 0x01]);

        let err = decode_protobuf(&body).unwrap_err();
        assert!(
            matches!(err, LogsIngestError::Decode(_)),
            "a malformed drained tail must be a prost Decode error, got {err:?}"
        );
    }

    /// Extracts the running estimate `T` a JSON byte-budget reject reports (the
    /// digits after the pinned `"decoded bytes (estimated) "` marker; serde_json
    /// appends a trailing " at line/column" the take-while ignores).
    fn extract_json_estimate(msg: &str) -> usize {
        let after = msg
            .split("decoded bytes (estimated) ")
            .nth(1)
            .expect("reject message names the family field");
        after
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>()
            .parse()
            .expect("the marker is followed by the running estimate")
    }

    /// AC 8 (JSON boundary, 2a): the shared charge seam admits charges summing to
    /// exactly `MAX_DECODED_BYTES` and rejects the next byte (strictly-greater),
    /// naming the family field — the cheap seam-level twin of the protobuf
    /// boundary (a full-parse admit at the budget would build millions of rows).
    #[test]
    fn json_byte_budget_exact_boundary_at_the_charge_seam() {
        use std::cell::Cell;
        let cell = Cell::new(0usize);
        let entry_w = std::mem::size_of::<JsonEntry>();
        let full = MAX_DECODED_BYTES / entry_w;
        let rem = MAX_DECODED_BYTES - full * entry_w;
        charge_json_decoded_bytes::<serde_json::Error>(&cell, full * entry_w).unwrap();
        charge_json_decoded_bytes::<serde_json::Error>(&cell, rem).unwrap();
        assert_eq!(cell.get(), MAX_DECODED_BYTES);
        // Exactly at the budget admits; one more byte is strictly greater.
        charge_json_decoded_bytes::<serde_json::Error>(&cell, 0).unwrap();
        let err = charge_json_decoded_bytes::<serde_json::Error>(&cell, 1).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("decoded bytes (estimated)"), "{msg}");
        assert!(msg.contains(&MAX_DECODED_BYTES.to_string()), "{msg}");
    }

    /// AC 8 + AC 13 (JSON 2b + bounded overshoot): a body of uniform 256-pair
    /// entries crossing the budget rejects as `LokiDecode` naming the family
    /// field and the budget value, and the reported estimate `T` satisfies
    /// `MAX < T <= MAX + w` where `w = size_of::<JsonEntry>() +
    /// MAX_STRUCTURED_METADATA_PER_ENTRY * size_of::<(String, String)>()` — NO
    /// `+ 1`, because `BoundedStructuredMetadata` HARD-rejects at the 257th raw
    /// pair, so a charged entry retains exactly 256 pairs.
    #[test]
    fn json_over_budget_rejects_with_one_entry_bounded_overshoot() {
        let entry_shell = std::mem::size_of::<JsonEntry>();
        let sm_pair = std::mem::size_of::<(String, String)>();
        let w = entry_shell + MAX_STRUCTURED_METADATA_PER_ENTRY * sm_pair;
        let full_w = w; // each crossing entry retains exactly 256 pairs

        let n = MAX_DECODED_BYTES / full_w + 8;
        assert!(n <= MAX_ENTRIES_PER_STREAM, "entries fit one stream");

        // One 256-pair entry `["ts","x",{"k0":"", ...}]` with distinct keys.
        let mut sm = String::from("{");
        for i in 0..MAX_STRUCTURED_METADATA_PER_ENTRY {
            if i > 0 {
                sm.push(',');
            }
            sm.push_str(&format!(r#""k{i}":"""#));
        }
        sm.push('}');
        let one_entry = format!(r#"["1700000000000000000","x",{sm}]"#);

        let mut body = String::from(r#"{"streams":[{"stream":{},"values":["#);
        for i in 0..n {
            if i > 0 {
                body.push(',');
            }
            body.push_str(&one_entry);
        }
        body.push_str("]}]}");

        let msg = json_loki_decode_message(&body);
        assert!(
            msg.contains("decoded bytes (estimated)"),
            "reject must name the family field: {msg}"
        );
        assert!(msg.contains(&MAX_DECODED_BYTES.to_string()), "{msg}");
        let t = extract_json_estimate(&msg);
        assert!(
            MAX_DECODED_BYTES < t && t <= MAX_DECODED_BYTES + w,
            "one-entry bounded overshoot: {} < {t} <= {}",
            MAX_DECODED_BYTES,
            MAX_DECODED_BYTES + w
        );
    }
}
