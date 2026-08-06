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
use crate::protocols::log_label_limits;
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
/// See [`MAX_STREAMS_PER_REQUEST`]. Bounds the count of **retained**
/// (non-empty-valued) labels parsed out of one stream's label-set literal
/// (protobuf) or JSON `stream` map, checked before the label `Vec` is handed
/// to `LabelSet::from_normalized`.
///
/// Empty-valued labels do not count: they are dropped before anything is
/// validated or stored (`syntax.ParseLabels`' closing `ls.WithoutEmpty()`,
/// `pkg/logql/syntax/parser.go:296 @ v3.7.4`), so counting them here would
/// refuse streams the reference accepts — 15 real labels padded with 250
/// empty-valued ones is a `204` upstream.
///
/// Neither do superseded ones. The count is charged on the SURVIVING map —
/// [`parse_json`] for the JSON transport, [`parse_label_pairs`] for the
/// protobuf one — never on raw occurrences, because 257 repetitions of one JSON
/// key are one label upstream and a whole superseded `stream`/`streams` value is
/// no labels at all (measured `204` upstream against our `400`, issue #374
/// round 11).
///
/// The reference has no label-*count* guard of its own; a stream carrying
/// more than 256 non-empty labels is refused here with this cap's message and
/// upstream with `has N label names; limit 15`. Both are `400` — the wording
/// differs only for input more than 17x over the parity bound. Ledgered.
pub const MAX_LABELS_PER_STREAM: usize = 256;
/// `maxStreamLabelsSize` (`pkg/logql/syntax/parser.go:22 @ v3.7.4`): the
/// reference's own bound on one stream's label-set literal, charged in
/// `syntax.ParseLabels` and again in `ParseLokiRequest`
/// (`pkg/loghttp/push/push.go:420-422 @ v3.7.4`). Adopted verbatim so a wide
/// literal is refused at the same width on both sides.
pub const MAX_STREAM_LABELS_BYTES: usize = (1 << 24) - 1;
/// PulsusDB-only structural guard on the number of **raw** `(key, value)`
/// pairs in one JSON `stream` object, counted during deserialization (which is
/// where the materialization stops) and reported by [`parse_json`] off the
/// count [`JsonStream::raw_label_pairs`] carries, so a superseded `stream`
/// occurrence discards its breach along with its pairs.
///
/// The protobuf transport needs no equivalent — it drops empty values as it
/// reads the literal, so its allocation is bounded by
/// [`MAX_LABELS_PER_STREAM`] retained pairs regardless of how many empty ones
/// the literal carries. The JSON transport must keep them until
/// [`parse_json`] has checked every key against [`is_valid_label_name`] (an
/// empty-valued label with a malformed name is a reject upstream, because the
/// literal is parsed before `WithoutEmpty` runs) and until the map's
/// last-write-wins collapse has happened, so this bounds that intermediate.
/// The reference bounds the same intermediate by the rendered literal's size
/// (16 MiB, [`MAX_STREAM_LABELS_BYTES`]); ours fires earlier, on a stream
/// carrying more than 65,536 raw keys of which 15 or fewer are non-empty.
/// Ledgered.
pub const MAX_RAW_LABEL_PAIRS_PER_STREAM: usize = 65_536;
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
///
/// Also the seam for the reference's *lower* bound on the same field (issue
/// #374): a push carrying no streams is refused with `422`. That check is the
/// distributor's rather than a decode cap, but this is the one place both
/// Loki-push encodings reach with the request's stream count in hand, so
/// putting it here keeps one definition instead of one per encoding.
fn validate_bounds(
    num_streams: usize,
    entries_per_stream: impl Iterator<Item = usize>,
) -> Result<(), LogsIngestError> {
    // "Return early if request does not contain any streams" —
    // `PushWithResolver` answers `422` before it validates a single label
    // (`pkg/distributor/distributor.go:579-581 @ v3.7.4`). Counted on the
    // streams the request CARRIES, not on what survives: a stream with labels
    // and no entries still counts (measured `204`), and so does one whose
    // labels breach a bound (measured `400`, not `422`). Our own
    // `MAX_STREAMS_PER_REQUEST` ceiling below is the opposite end of the same
    // field, so the two live together.
    if num_streams == 0 {
        return Err(LogsIngestError::MissingStreams);
    }
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
/// only clock boundary). Fallible on a per-entry timestamp overflow —
/// which, unlike OTLP's per-record partial-success drop, is a whole-request
/// `LokiDecode` failure here (upstream a malformed JSON timestamp likewise
/// fails the whole body, in its `jsoniter` decoder).
///
/// A stream whose labels breach one of the four per-stream label bounds
/// (issue #374) is **not** a whole-request failure: it is dropped and its
/// message accumulated into [`ParsedLogs::stream_errors`], because upstream
/// `PushWithResolver` `continue`s past it, writes the remaining streams and
/// answers `400` afterwards (`pkg/distributor/distributor.go:645-655,
/// 780-790, 929 @ v3.7.4`).
///
/// The bounds are charged on the parsed pairs before `LabelSet::from_normalized`
/// collapses a repeat, because the duplicate-name bound would otherwise be
/// unobservable — the protobuf `labels` literal is the one log transport that
/// can carry `{foo="bar", foo="barf"}` to this point, exactly as it is upstream.
pub fn parse_protobuf(req: &PushRequest, now_ns: i64) -> Result<ParsedLogs, LogsIngestError> {
    let mut out = ParsedLogs::default();
    let mut seen_streams: HashSet<(Fingerprint, Date)> = HashSet::new();
    for stream in &req.streams {
        let stream_labels =
            log_label_limits::StreamLabels::from_pairs(parse_label_pairs(&stream.labels)?);
        // A stream with no entries is skipped before validation upstream
        // (`pkg/distributor/distributor.go:639-641 @ v3.7.4`), so an
        // entry-less stream with over-wide labels is accepted there and here.
        // A breach drops just this stream (`distributor.go:645-655`).
        if !stream.entries.is_empty()
            && let Err(err) = stream_labels.validate()
        {
            out.stream_errors.push(err.to_string());
            continue;
        }
        let (labels, collisions) = LabelSet::from_normalized(stream_labels.into_pairs());
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
/// `structured_metadata` ([`JsonEntry`]'s `Deserialize`, issue #97); a fourth+
/// element is parsed and discarded rather than retained.
///
/// [`JsonPush`]/[`JsonStream`] use bounded [`serde::de::DeserializeSeed`]
/// visitors (issue #77) that stop RETAINING the `streams` array
/// ([`MAX_STREAMS_PER_REQUEST`]), each stream's `values` array
/// ([`MAX_ENTRIES_PER_STREAM`]), the per-stream `stream` label map
/// ([`MAX_RAW_LABEL_PAIRS_PER_STREAM`]) and an entry's structured metadata
/// ([`MAX_STRUCTURED_METADATA_PER_ENTRY`]) one element past the cap — so
/// `serde_json` cannot grow those `Vec`s/maps unbounded. Each **reports**
/// through the `MAX + 1` over-cap sentinel it leaves behind
/// ([`validate_bounds`], [`JsonStream::raw_label_pairs`],
/// `canonical_structured_metadata`), which is read AFTER the envelope's
/// last-wins resolution — the same two-phase shape [`decode_protobuf`] has
/// always had. Charging them at the moment they trip instead refused a request
/// whose over-cap value a later occurrence of the same key superseded, which
/// the reference answers `204` (issue #374 round 11).
///
/// Past the cap the remainder is **parsed and discarded, not skipped**: the
/// same type rules, the same message text and the same 128-level depth ceiling
/// apply to it as to what was retained (the `Drained*` group at the bottom of
/// this module), so crossing a cap cannot turn checking off. What stops is
/// retention: a drained run's own peak is one element — one `JsonEntry`, one
/// `(key, value)` pair's worth of parse scratch, one nesting chain of at most
/// 128 `Deserialize` frames — dropped before the next is read. That peak is
/// bounded by [`MAX_STREAMS_PER_REQUEST`]` + 1` streams and, per stream,
/// [`MAX_ENTRIES_PER_STREAM`]` + 1` entries of RETAINED material plus one
/// in-flight element; the input itself is bounded before decode by the 64 MiB
/// decompressed body cap.
///
/// The two SHARED cross-request counters are the exception and are immediately
/// fatal: the aggregate entry count ([`MAX_TOTAL_ENTRIES_PER_REQUEST`]) and the
/// `size_of`-estimated BYTES each stream/entry/label-map materializes
/// ([`crate::protocols::otlp_prescan::MAX_DECODED_BYTES`], issue #168), both
/// threaded through one [`Cell`](std::cell::Cell). They measure memory this
/// request has already cost across every occurrence, which a supersession does
/// not refund while the superseding value is still decoding; deferring them
/// would mean decoding past the budget to find out. The residual is ledgered.
/// Each stream's label **names** are validated against the
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
        // The two per-stream label caps, charged HERE rather than inside the
        // decoder, because both are only meaningful once the JSON envelope's
        // last-wins resolution has run: a repeated `stream` key replaces the
        // whole map, and a repeated key WITHIN the map replaces just that
        // label. Upstream never sees a discarded occurrence at all — its
        // `LabelSet.UnmarshalJSON` assigns into a `map[string]string`
        // (`pkg/loghttp/labels.go:25-40 @ v3.7.4`) and its one-field envelope
        // decoder re-runs the field decoder per occurrence — so charging a cap
        // against a value it has already thrown away is a `400` against its
        // `204` (measured, issue #374 round 11). Whole-request rejects: these
        // are our structural guards, not the reference's per-stream bounds
        // below, and it has no equivalent to either.
        if stream.raw_label_pairs > MAX_RAW_LABEL_PAIRS_PER_STREAM {
            return Err(LogsIngestError::LokiDecode(format!(
                "stream label pairs exceed the {MAX_RAW_LABEL_PAIRS_PER_STREAM} per-stream bound"
            )));
        }
        // Counted on the labels that SURVIVE: distinct keys whose final value is
        // non-empty. Empty values are dropped before the stream is validated,
        // hashed or stored (`ls.WithoutEmpty()`, `pkg/logql/syntax/parser.go:296
        // @ v3.7.4`), so they cost nothing here either.
        let surviving_labels = stream.stream.values().filter(|v| !v.is_empty()).count();
        if surviving_labels > MAX_LABELS_PER_STREAM {
            return Err(LogsIngestError::LokiDecode(format!(
                "stream labels exceed the {MAX_LABELS_PER_STREAM} per-stream bound"
            )));
        }
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
        // `WithoutEmpty` + the four per-stream label bounds (issue #374),
        // charged before any row is materialized. The empty-value drop happens
        // here rather than in the decoder because the map's last-write-wins
        // collapse must come first: `{"foo":"bar","foo":""}` leaves `foo`
        // empty upstream too, so the stream ends up with no labels at all.
        // `stream.stream` is a `BTreeMap`, so a repeated JSON key has already
        // collapsed and the duplicate-name bound is vacuous here — which is
        // also what the reference does with a duplicate JSON key (measured:
        // `{"foo":"bar","foo":"barf"}` answers `204` on `grafana/loki:3.7.4`).
        // Skipped for an entry-less stream, as upstream skips it before
        // validating (`pkg/distributor/distributor.go:639-641 @ v3.7.4`); a
        // breach drops just this stream (`distributor.go:645-655`).
        let stream_labels = log_label_limits::StreamLabels::from_pairs(
            stream
                .stream
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>(),
        );
        if !stream.values.is_empty()
            && let Err(err) = stream_labels.validate()
        {
            out.stream_errors.push(err.to_string());
            continue;
        }
        let (labels, collisions) = LabelSet::from_normalized(stream_labels.into_pairs());
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
/// same `LabelSet::from_normalized` seam every other path uses. See
/// [`parse_label_pairs`] for the accepted grammar and the rejections; this
/// wrapper only adds the canonicalizing collapse, so it is used where the
/// **raw** pairs are not needed (the duplicate-name bound in
/// [`log_label_limits`] must see them before they collapse).
#[cfg(test)]
fn parse_label_set(input: &str) -> Result<(LabelSet, usize), LogsIngestError> {
    Ok(LabelSet::from_normalized(parse_label_pairs(input)?))
}

/// Parses a Loki `StreamAdapter.labels` string into the `(name, value)` pairs
/// of one stream, in wire order and **without deduplication** (so the
/// duplicate-name bound can see a repeat) but **with empty values dropped**,
/// which is what `syntax.ParseLabels` returns upstream — its closing
/// `ls.WithoutEmpty()` (`pkg/logql/syntax/parser.go:296 @ v3.7.4`) runs after
/// the literal is parsed and before anything is validated, hashed or stored.
/// Dropping them here rather than after collection also keeps an
/// empty-valued label from consuming [`MAX_LABELS_PER_STREAM`]: the reference
/// has no per-stream label *count* guard at all, only the 16 MiB literal-size
/// one adopted below, so a stream of 15 real labels padded with any number of
/// empty-valued ones is accepted there and here.
///
/// Rejects a missing/unbalanced brace, a missing `=`, an unterminated/
/// malformed quoted value, or a literal over [`MAX_STREAM_LABELS_BYTES`] as
/// [`LogsIngestError::LokiDecode`], and more than [`MAX_LABELS_PER_STREAM`]
/// retained pairs as [`LogsIngestError::OversizeMessage`] — all whole-request
/// 400s. Prometheus value escaping (`\\`, `\"`, `\n`, `\t`, `\r`) is
/// unescaped; the empty set `{}` yields no pairs.
fn parse_label_pairs(input: &str) -> Result<Vec<(String, String)>, LogsIngestError> {
    // `maxStreamLabelsSize`, charged on the raw literal exactly as
    // `syntax.ParseLabels` charges it (`pkg/logql/syntax/parser.go:280-281 @
    // v3.7.4`) and again at `pkg/loghttp/push/push.go:420-422 @ v3.7.4`. This
    // is the reference's only bound on how wide one stream's literal may be,
    // and with empty values dropped as they are read it bounds this
    // function's allocation too.
    if input.len() > MAX_STREAM_LABELS_BYTES {
        return Err(LogsIngestError::LokiDecode(format!(
            "stream labels size {} exceeds limit of {MAX_STREAM_LABELS_BYTES} bytes",
            input.len()
        )));
    }
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
        return Ok(pairs);
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
        // `WithoutEmpty`, applied after the name grammar has been checked —
        // upstream order too, since the promql metric parser inside
        // `ParseLabels` rejects a malformed name before `WithoutEmpty` runs
        // (measured: `{a.b=""}` is `400 couldn't parse labels` there).
        if !value.is_empty() {
            pairs.push((key, value));
        }
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
    Ok(pairs)
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

/// One Loki stream: a `stream` label map (decoded by [`BoundedLabelMap`], which
/// carries its RAW pair count out so [`parse_json`] can charge
/// [`MAX_RAW_LABEL_PAIRS_PER_STREAM`] against the map that SURVIVED a repeated
/// `stream` key) and a `values` array (capped per-stream at
/// [`MAX_ENTRIES_PER_STREAM`]` + 1` during decode and charged across streams
/// against the shared aggregate counter). Deserialized only through
/// [`StreamSeed`], which threads that shared counter in; a missing key yields
/// the prior `#[serde(default)]` empty.
struct JsonStream {
    stream: std::collections::BTreeMap<String, String>,
    /// RAW `stream` pairs behind the collapsed map above, saturated at
    /// [`MAX_RAW_LABEL_PAIRS_PER_STREAM`]` + 1`. Travels WITH the map (a
    /// repeated `stream` key replaces both together), so the count always
    /// describes the labels that survived last-wins resolution.
    raw_label_pairs: usize,
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
                    // ASCII-case-insensitive, and a repeat overwrites — both
                    // are the reference's, and both are observable (measured:
                    // `Streams`/`STREAMS`/`StReAmS`/`streamS` and an escaped
                    // `"Streams"` all store their lines upstream, and of
                    // two spellings the LAST one is what gets stored).
                    //
                    // `loghttp.PushRequest` is a one-field struct decoded by
                    // jsoniter reflection (`pkg/loghttp/query.go:91-93 @
                    // v3.7.4`), and `jsoniter.NewDecoder` uses `ConfigDefault`,
                    // whose `CaseSensitive` is false. The field map then gets a
                    // `strings.ToLower` alias per tag and the wire key is folded
                    // a byte at a time over `'A'..='Z'` only
                    // (`reflect_struct_decoder.go:36-41`, `iter_object.go:49-90`
                    // @ jsoniter v1.1.12, vendored) — so the rule is ASCII
                    // folding, which is exactly `eq_ignore_ascii_case`. (Upstream
                    // compares an FNV hash of the folded key rather than the key;
                    // a colliding spelling would decode there too. That is an
                    // artifact of its decoder, not a rule, and is not reproduced.)
                    //
                    // The overwrite is the same decoder's: `oneFieldStructDecoder`
                    // re-invokes the field decoder on every matching key and
                    // `sliceDecoder` re-grows from zero, so the last occurrence
                    // wins (`reflect_struct_decoder.go:574-590`,
                    // `reflect_slice.go:66-99`). Rejecting the repeat as a
                    // duplicate field — which is what this did — was a `400`
                    // against upstream's `204`.
                    if key.eq_ignore_ascii_case("streams") {
                        // Bytes and entries charged by a superseded occurrence
                        // stay charged: the shared counters bound what this
                        // request made us materialize, and we did materialize it.
                        //
                        // `null` overwrites with nil upstream — `sliceDecoder`
                        // takes `UnsafeSetNil` before it ever reaches the
                        // elements — so `{"streams":[one],"streams":null}` is a
                        // stream-less request, measured `422`, not `204`.
                        streams = Some(
                            map.next_value_seed(NullableSeed(StreamsSeed {
                                total_entries: &total_entries,
                                decoded_bytes: &decoded_bytes,
                            }))?
                            .unwrap_or_default(),
                        );
                    } else {
                        map.next_value::<DrainedAny>()?;
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

/// Wraps a [`DeserializeSeed`](serde::de::DeserializeSeed) so a JSON `null`
/// yields `None` instead of an "invalid type" failure — `serde_json`'s
/// `deserialize_seq` rejects `null` outright
/// (`peek_invalid_type`), while both of the reference's JSON envelope decoders
/// treat it as "no value here". What they do NEXT differs, so the two callers
/// differ too and each says why: the `streams` slice is overwritten with nil
/// (`reflect_slice.go:66-73 @ jsoniter v1.1.12`, vendored in the Loki tree),
/// whereas a stream's `values` is left untouched (`case "values": if ty ==
/// jsonparser.Null { return nil }`, `pkg/loghttp/query.go:110-112 @ v3.7.4`).
struct NullableSeed<S>(S);

impl<'de, S> serde::de::DeserializeSeed<'de> for NullableSeed<S>
where
    S: serde::de::DeserializeSeed<'de>,
{
    type Value = Option<S::Value>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct OptVisitor<S>(S);
        impl<'de, S> serde::de::Visitor<'de> for OptVisitor<S>
        where
            S: serde::de::DeserializeSeed<'de>,
        {
            type Value = Option<S::Value>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON value or null")
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                self.0.deserialize(deserializer).map(Some)
            }
        }
        deserializer.deserialize_option(OptVisitor(self.0))
    }
}

/// Bounded [`DeserializeSeed`](serde::de::DeserializeSeed) for the `streams`
/// array: stops materializing at [`MAX_STREAMS_PER_REQUEST`]` + 1` elements and
/// seeds each element with the shared aggregate counter. Mirrors
/// [`BoundedStructuredMetadata`]'s drain-the-remainder, and like it leaves the
/// over-cap sentinel for a post-resolution check ([`validate_bounds`]) rather
/// than rejecting a value a later `streams` occurrence may discard.
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
                loop {
                    if streams.len() > MAX_STREAMS_PER_REQUEST {
                        // Cap reached: drain the remainder WITHOUT materializing
                        // it, leaving the vec at the `MAX + 1` over-cap sentinel
                        // the deferred [`validate_bounds`] rejects — the same
                        // shape `PushRequest::merge_field`'s tag-1 drain leaves
                        // on the protobuf path. Raising here instead would charge
                        // the cap against a `streams` occurrence a LATER one may
                        // still supersede, which the reference never sees (issue
                        // #374 round 11: last-wins resolves before any bound).
                        //
                        // Drained is not unchecked: [`DrainedStream`] applies
                        // the same key switch and the same type rules as the
                        // retained arm below and keeps none of the result, so a
                        // malformed element past the cap is the 400 it is
                        // before the cap (issue #374 round 12).
                        while seq.next_element::<DrainedStream>()?.is_some() {}
                        break;
                    }
                    let Some(stream) = seq.next_element_seed(StreamSeed {
                        total_entries: self.total_entries,
                        decoded_bytes: self.decoded_bytes,
                    })?
                    else {
                        break;
                    };
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
                let mut stream: Option<BoundedLabelMap> = None;
                let mut values: Option<Vec<JsonEntry>> = None;
                // Exact-match keys, unlike the envelope's `streams` above: a
                // stream object is decoded by a hand-written
                // `LogProtoStream.UnmarshalJSON` that switches on
                // `string(key)` (`pkg/loghttp/query.go:99-121 @ v3.7.4`), so
                // upstream ignores `Stream`/`Values` too — measured, both
                // sides `204` storing nothing. A repeat, though, overwrites
                // there (the switch simply runs again), so a duplicate key is
                // last-wins here as well and not a `400`.
                while let Some(key) = map.next_key::<std::borrow::Cow<str>>()? {
                    match key.as_ref() {
                        "stream" => {
                            let labels = map.next_value::<BoundedLabelMap>()?;
                            // Charge the RETAINED (post-dedup) label pairs (issue
                            // #168): the raw-pair count is already capped at
                            // MAX_RAW_LABEL_PAIRS_PER_STREAM by
                            // `BoundedLabelMap`'s drain, so the over-step is
                            // bounded to one map.
                            charge_json_decoded_bytes(
                                self.decoded_bytes,
                                labels.labels.len().saturating_mul(std::mem::size_of::<(
                                    String,
                                    String,
                                )>(
                                )),
                            )?;
                            stream = Some(labels);
                        }
                        "values" => {
                            // `null` is the one place the reference does NOT
                            // overwrite: its decoder returns from the callback
                            // before touching `s.Entries`, so a `null` after a
                            // populated `values` keeps the entries (measured
                            // `204`, line stored).
                            if let Some(entries) =
                                map.next_value_seed(NullableSeed(ValuesSeed {
                                    total_entries: self.total_entries,
                                    decoded_bytes: self.decoded_bytes,
                                }))?
                            {
                                values = Some(entries);
                            }
                        }
                        _ => {
                            map.next_value::<DrainedAny>()?;
                        }
                    }
                }
                let stream = stream.unwrap_or(BoundedLabelMap {
                    labels: std::collections::BTreeMap::new(),
                    raw_pairs: 0,
                });
                Ok(JsonStream {
                    stream: stream.labels,
                    raw_label_pairs: stream.raw_pairs,
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

/// The per-stream `stream` label map, decoded with its RAW pair count carried
/// alongside the dedup-collapsing `BTreeMap`.
///
/// Two counts, because the reference has two different reasons to refuse a wide
/// label map and only one of them is about this map's own growth:
///
/// - [`MAX_RAW_LABEL_PAIRS_PER_STREAM`] on RAW `next_entry` pairs — the
///   structural memory guard. Enforced here in the sense that matters (past the
///   cap nothing further is materialized), but *reported* by [`parse_json`] off
///   the carried `raw_pairs` count, so a `stream` occurrence a later one
///   supersedes takes its breach with it.
/// - [`MAX_LABELS_PER_STREAM`] on the labels that actually survive. That count
///   is not knowable inside this visitor: `{"a":"1","a":"2"}` is ONE label
///   upstream (`LabelSet.UnmarshalJSON` assigns into a `map[string]string`,
///   `pkg/loghttp/labels.go:25-40 @ v3.7.4`, and has no count bound of its own),
///   and `{"a":"1","a":""}` is none at all, because empty values are dropped
///   before the stream is validated, hashed or stored (`ls.WithoutEmpty()`,
///   `pkg/logql/syntax/parser.go:296 @ v3.7.4`). Charging RAW pairs here refused
///   257 repetitions of one key that upstream stores as a single label
///   (measured `204` vs our `400`), so the cap moved to [`parse_json`], where
///   the map has collapsed and the survivor is in hand.
///
/// Empty-valued pairs are still *retained*: [`parse_json`] checks their names
/// against [`is_valid_label_name`] (upstream parses the literal before
/// `WithoutEmpty` runs, so `{"a.b":""}` is a reject there) and the map's
/// last-write-wins collapse must happen before the drop, since
/// `{"foo":"bar","foo":""}` leaves `foo` empty upstream and must here too.
struct BoundedLabelMap {
    labels: std::collections::BTreeMap<String, String>,
    /// RAW `next_entry` pairs, saturated at [`MAX_RAW_LABEL_PAIRS_PER_STREAM`]`
    /// + 1` — the over-cap sentinel, exactly like the `+ 1` element counts the
    /// `streams`/`values`/`structured_metadata` drains leave behind.
    raw_pairs: usize,
}

impl<'de> serde::Deserialize<'de> for BoundedLabelMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LabelMapVisitor;
        impl<'de> serde::de::Visitor<'de> for LabelMapVisitor {
            type Value = BoundedLabelMap;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a Loki stream label map of string values")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut labels = std::collections::BTreeMap::new();
                let mut raw_pairs = 0usize;
                loop {
                    if raw_pairs > MAX_RAW_LABEL_PAIRS_PER_STREAM {
                        // Cap reached: drain the remaining pairs WITHOUT
                        // retaining them, leaving `raw_pairs` at the `MAX + 1`
                        // sentinel `parse_json` rejects. Still string-typed:
                        // [`DrainedString`] is `String`'s accept surface and
                        // `String`'s message, so a non-string value past the
                        // cap fails as it does before it (issue #374 round 12).
                        while map.next_entry::<DrainedString, DrainedString>()?.is_some() {}
                        break;
                    }
                    let Some((k, v)) = map.next_entry::<String, String>()? else {
                        break;
                    };
                    raw_pairs += 1;
                    labels.insert(k, v);
                }
                Ok(BoundedLabelMap { labels, raw_pairs })
            }
        }
        deserializer.deserialize_map(LabelMapVisitor)
    }
}

/// Bounded [`DeserializeSeed`](serde::de::DeserializeSeed) for a stream's
/// `values` array: stops materializing at [`MAX_ENTRIES_PER_STREAM`]` + 1`
/// elements per stream (the sentinel [`validate_bounds`] rejects after the
/// envelope resolves) and charges each RETAINED entry into the shared
/// cross-stream aggregate counter, rejecting on the spot once it exceeds
/// [`MAX_TOTAL_ENTRIES_PER_REQUEST`] — both **during** deserialization, so the
/// `Vec<JsonEntry>` never grows past the cap.
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
                loop {
                    if values.len() > MAX_ENTRIES_PER_STREAM {
                        // Cap reached: drain the remainder WITHOUT retaining it
                        // (and without charging the shared counters for what is
                        // never retained), leaving the vec at the `MAX + 1`
                        // over-cap sentinel [`validate_bounds`] rejects — the
                        // JSON twin of `StreamAdapter::merge_field`'s tag-2
                        // drain. Deferred rather than raised because a LATER
                        // `values` (or `streams`) occurrence can supersede this
                        // whole array, and the reference charges nothing against
                        // a value it has discarded (issue #374 round 11).
                        //
                        // Each drained element is still the real [`JsonEntry`],
                        // dropped as soon as it is read: one entry's worth of
                        // peak retention, and one implementation of the entry
                        // type rule for both sides of the cap (round 12).
                        while seq.next_element::<JsonEntry>()?.is_some() {}
                        break;
                    }
                    let Some(entry) = seq.next_element::<JsonEntry>()? else {
                        break;
                    };
                    // The two SHARED counters below stay immediately fatal, and
                    // deliberately so: unlike the per-occurrence caps they
                    // measure what this request has ALREADY made us materialize
                    // across every occurrence, and a supersession does not give
                    // that memory back while the superseding value is still
                    // decoding. Deferring them would mean materializing past the
                    // budget to find out whether the occurrence survives — a
                    // resource divergence traded for a rejection one. The
                    // residual (a superseded occurrence carrying more than
                    // MAX_TOTAL_ENTRIES_PER_REQUEST entries, which needs ≈46 MB
                    // of body, is `400` here and `204` upstream) is recorded in
                    // docs/benchmarks/logs-differential-ledger.md.
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
/// bounding materialization DURING deserialization (charge-before-allocate):
/// the visitor stops retaining at pair 257 and drains the rest of the object,
/// leaving the `MAX + 1` sentinel that `canonical_structured_metadata` — which
/// runs after every last-wins resolution — turns into the reject. A dedup-
/// collapsing `BTreeMap` would (a) allocate every key before any bound check
/// and (b) fold duplicate keys, letting a duplicate-key object evade the
/// per-entry cardinality bound — so raw pairs are counted instead, which is
/// also how the reference accumulates them (`unmarshalHTTPToLogProtoEntry`
/// appends every pair, `pkg/loghttp/query.go:181-196 @ v3.7.4`). Downstream
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
                loop {
                    if pairs.len() > MAX_STRUCTURED_METADATA_PER_ENTRY {
                        // Cap reached: drain the remainder WITHOUT retaining it,
                        // leaving the `MAX + 1` over-cap sentinel
                        // `canonical_structured_metadata` rejects — exactly what
                        // `EntryAdapter::merge_field`'s tag-3 drain leaves on the
                        // protobuf path. Raising here would charge the cap against
                        // an entry a later `values` occurrence may supersede.
                        // Still string-typed past the cap ([`DrainedString`]):
                        // upstream types these too (`dataType != String` is
                        // `MalformedStringError`, `pkg/loghttp/query.go:186-188
                        // @ v3.7.4`), and a `"bad":[]` past pair 257 was `204`
                        // here against its `400` (issue #374 round 12).
                        while map.next_entry::<DrainedString, DrainedString>()?.is_some() {}
                        break;
                    }
                    let Some((key, value)) = map.next_entry::<String, String>()? else {
                        break;
                    };
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
                // Parse-and-discard any trailing element beyond the third:
                // checked like every other value, retained nowhere.
                while seq.next_element::<DrainedAny>()?.is_some() {}
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

// ---------------------------------------------------------------------------
// Parse-and-discard
// ---------------------------------------------------------------------------
//
// Every JSON value this decoder does not keep is still PARSED: its structure,
// its scalar types and its nesting depth are checked exactly as a retained
// value's are, and only the retention stops. Two kinds of value are discarded:
//
// - a value under a key this decoder does not read (an unknown envelope key, an
//   unknown key inside a stream object, a fourth+ element of an entry array) —
//   [`DrainedAny`], which accepts any shape, because the reference does not
//   type these either;
// - the remainder of a `streams` / `stream` / `values` / structured-metadata
//   run past its cap, which the cap defers on so that a later occurrence of the
//   same key can supersede it — [`DrainedStream`], [`DrainedLabelMap`],
//   [`DrainedValues`] and [`DrainedString`], each mirroring the type rule of
//   the retained arm beside it, message text included.
//
// `serde::de::IgnoredAny` is what these replace and is the wrong tool for
// either: `serde_json` implements it as `Deserializer::ignore_value`, a
// bracket-matching skip that checks no types and — being iterative over a
// scratch `Vec` rather than recursive — is not covered by the crate's
// `RECURSION_LIMIT` (`de.rs:1102` / `de.rs:63,1375` @ serde_json 1.0.150). Both
// gaps were observable: `{"m0".."m256"…,"bad":[]}` and `[…100001 entries…,0]`,
// each superseded by a valid occurrence, were `400` upstream and `204` here,
// while the SAME malformed tail below the cap was `400` on both — the cap was
// what switched the checking off (issue #374 round 12).
//
// The reference checks a discarded value for the same reason: jsoniter decodes
// every occurrence of a repeated field in full before the last one wins
// (`reflect_struct_decoder.go:574-590 @ jsoniter v1.1.12`), a stream object
// reaches its hand-written unmarshaler only after `iter.Skip()` has walked it,
// and that walk carries jsoniter's own depth bound (`maxDepth = 10000`,
// `iter.go:331-338`, error `exceeded max depth`; measured: depth 10,000 is
// `204` upstream, 10,001 is `400`).

/// Parse-and-discard for a JSON value of any shape.
///
/// Retains nothing at all — not even a scalar's bytes — while still failing on
/// malformed JSON and, through `serde_json`'s `RECURSION_LIMIT`, on nesting
/// past 128 levels. That ceiling is the same one every typed value in this
/// decoder already has, which is the point: it is now the body's ONE depth
/// rule rather than a rule with an ignored-value hole in it. It is tighter than
/// the reference's 10,000 (residual 8 of the `ingest-label-bounds` ledger row);
/// a legal Loki push nests six deep.
struct DrainedAny;

impl<'de> serde::Deserialize<'de> for DrainedAny {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct AnyVisitor;
        impl<'de> serde::de::Visitor<'de> for AnyVisitor {
            type Value = DrainedAny;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("any JSON value")
            }

            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Ok(DrainedAny)
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> {
                Ok(DrainedAny)
            }
            fn visit_i128<E: serde::de::Error>(self, _: i128) -> Result<Self::Value, E> {
                Ok(DrainedAny)
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> {
                Ok(DrainedAny)
            }
            fn visit_u128<E: serde::de::Error>(self, _: u128) -> Result<Self::Value, E> {
                Ok(DrainedAny)
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> {
                Ok(DrainedAny)
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Ok(DrainedAny)
            }
            fn visit_bytes<E: serde::de::Error>(self, _: &[u8]) -> Result<Self::Value, E> {
                Ok(DrainedAny)
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(DrainedAny)
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(DrainedAny)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                serde::Deserialize::deserialize(deserializer)
            }

            fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                serde::Deserialize::deserialize(deserializer)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                // Recursing through `Deserialize` rather than skipping is what
                // puts every level under `serde_json`'s depth counter.
                while seq.next_element::<DrainedAny>()?.is_some() {}
                Ok(DrainedAny)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                while map.next_entry::<DrainedString, DrainedAny>()?.is_some() {}
                Ok(DrainedAny)
            }
        }
        deserializer.deserialize_any(AnyVisitor)
    }
}

/// Parse-and-discard for a JSON string, with `String`'s accept surface and
/// `String`'s `expecting` text — so a drained label or structured-metadata
/// value fails exactly as the retained one beside it does, down to the message
/// (`invalid type: sequence, expected a string`), and allocates nothing.
struct DrainedString;

impl<'de> serde::Deserialize<'de> for DrainedString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StrVisitor;
        impl serde::de::Visitor<'_> for StrVisitor {
            type Value = DrainedString;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a string")
            }

            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Ok(DrainedString)
            }
        }
        deserializer.deserialize_str(StrVisitor)
    }
}

/// Parse-and-discard for one `streams` element past
/// [`MAX_STREAMS_PER_REQUEST`]. Mirrors [`StreamSeed`]'s key switch — the same
/// three arms, the same `expecting` — but keeps nothing and charges nothing:
/// the shared counters bound RETAINED materialization, and this element is
/// dropped as it is read.
struct DrainedStream;

impl<'de> serde::Deserialize<'de> for DrainedStream {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StreamVisitor;
        impl<'de> serde::de::Visitor<'de> for StreamVisitor {
            type Value = DrainedStream;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a Loki stream object with `stream` and `values`")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                while let Some(key) = map.next_key::<std::borrow::Cow<str>>()? {
                    match key.as_ref() {
                        "stream" => {
                            map.next_value::<DrainedLabelMap>()?;
                        }
                        // `Option` for the same reason [`NullableSeed`] exists
                        // on the retained arm: `"values":null` is not a type
                        // error there and must not be one here.
                        "values" => {
                            map.next_value::<Option<DrainedValues>>()?;
                        }
                        _ => {
                            map.next_value::<DrainedAny>()?;
                        }
                    }
                }
                Ok(DrainedStream)
            }
        }
        deserializer.deserialize_map(StreamVisitor)
    }
}

/// Parse-and-discard for a `stream` label map inside a drained stream: the
/// string-typed pairs [`BoundedLabelMap`] would keep, kept nowhere. No cap —
/// the map it describes is already discarded, so there is no count for one to
/// report through.
struct DrainedLabelMap;

impl<'de> serde::Deserialize<'de> for DrainedLabelMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MapVisitor;
        impl<'de> serde::de::Visitor<'de> for MapVisitor {
            type Value = DrainedLabelMap;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a Loki stream label map of string values")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                while map.next_entry::<DrainedString, DrainedString>()?.is_some() {}
                Ok(DrainedLabelMap)
            }
        }
        deserializer.deserialize_map(MapVisitor)
    }
}

/// Parse-and-discard for a `values` array inside a drained stream. Each element
/// is deserialized as the real [`JsonEntry`] and dropped immediately — the
/// entry type rule has exactly one implementation, so a drained entry and a
/// retained one cannot drift apart. Peak retention is one entry.
struct DrainedValues;

impl<'de> serde::Deserialize<'de> for DrainedValues {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SeqVisitor;
        impl<'de> serde::de::Visitor<'de> for SeqVisitor {
            type Value = DrainedValues;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an array of Loki log entries")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                while seq.next_element::<JsonEntry>()?.is_some() {}
                Ok(DrainedValues)
            }
        }
        deserializer.deserialize_seq(SeqVisitor)
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

    /// The `(field, actual)` of the [`LogsIngestError::OversizeMessage`] a body
    /// rejects with. `actual` is the count the decoder actually MATERIALIZED,
    /// so asserting it is `MAX + 1` for a body carrying far more proves the
    /// drain ran — the JSON twin of the protobuf `+ 1` sentinel assertions.
    fn json_oversize(body: &str) -> (&'static str, usize) {
        match parse_json(body.as_bytes(), 0).unwrap_err() {
            LogsIngestError::OversizeMessage { field, actual, .. } => (field, actual),
            other => panic!("expected OversizeMessage, got {other:?}"),
        }
    }

    #[test]
    fn parse_json_rejects_too_many_streams_during_deserialize() {
        // AC (too many streams / JSON): more than MAX_STREAMS_PER_REQUEST empty
        // stream objects. The seed stops materializing at MAX + 1 and drains the
        // rest; `validate_bounds` — which runs after the envelope's last-wins
        // resolution — turns that sentinel into the reject. `actual` is the
        // materialized count, so `MAX + 1` out of `MAX + 8` encoded proves the
        // drain rather than a full unpack.
        let mut body = String::with_capacity(4 * MAX_STREAMS_PER_REQUEST);
        body.push_str(r#"{"streams":["#);
        for i in 0..MAX_STREAMS_PER_REQUEST + 8 {
            if i > 0 {
                body.push(',');
            }
            body.push_str("{}");
        }
        body.push_str("]}");
        assert_eq!(
            json_oversize(&body),
            ("streams", MAX_STREAMS_PER_REQUEST + 1)
        );
    }

    #[test]
    fn parse_json_rejects_too_many_entries_per_stream_during_deserialize() {
        // AC (too many entries-per-stream / JSON), same shape as the streams
        // case above: MAX + 1 materialized out of MAX + 8 encoded.
        let mut body = String::new();
        body.push_str(r#"{"streams":[{"stream":{"a":"b"},"values":["#);
        for i in 0..MAX_ENTRIES_PER_STREAM + 8 {
            if i > 0 {
                body.push(',');
            }
            body.push_str(r#"["1700000000000000000","x"]"#);
        }
        body.push_str("]}]}");
        assert_eq!(
            json_oversize(&body),
            ("entries", MAX_ENTRIES_PER_STREAM + 1)
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
    fn parse_json_duplicate_label_keys_collapse_before_the_label_cap_is_charged() {
        // Issue #374 round 11: a label map whose keys are all the SAME string is
        // ONE label upstream — `LabelSet.UnmarshalJSON` assigns into a
        // `map[string]string` (`pkg/loghttp/labels.go:25-40 @ v3.7.4`) and has
        // no count bound at all. Counting RAW pairs against
        // MAX_LABELS_PER_STREAM refused this at 257 repetitions where the
        // reference answers 204 and stores the line (measured). The cap is now
        // charged on the surviving map, so the collapse comes first.
        let mut map = String::from("{");
        for i in 0..=MAX_LABELS_PER_STREAM {
            if i > 0 {
                map.push(',');
            }
            map.push_str(&format!(r#""dup":"v{i}""#));
        }
        map.push_str(r#","app":"x"}"#);
        let body =
            format!(r#"{{"streams":[{{"stream":{map},"values":[["1700000000000000000","x"]]}}]}}"#);
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert_eq!(out.rows.len(), 1);
        // Last-write-wins, exactly as the reference's map assignment is.
        assert_eq!(
            out.streams[0].labels.iter().collect::<Vec<_>>(),
            [
                ("app", "x"),
                ("dup", format!("v{MAX_LABELS_PER_STREAM}").as_str()),
            ]
        );
    }

    #[test]
    fn parse_json_duplicate_label_keys_cannot_evade_the_raw_pair_guard() {
        // What the collapse above must NOT cost: the raw-pair count is still the
        // structural guard on the intermediate map, and repeating one key does
        // not evade it. 65,537 repetitions materialize 65,537 pairs and are
        // refused; the drain then stops, so nothing past the sentinel is built.
        let mut map = String::with_capacity(16 * MAX_RAW_LABEL_PAIRS_PER_STREAM);
        map.push('{');
        for i in 0..MAX_RAW_LABEL_PAIRS_PER_STREAM + 8 {
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
            msg.contains("stream label pairs exceed"),
            "duplicate keys must still trip the RAW-pair guard: {msg:?}"
        );
    }

    #[test]
    fn parse_json_accepts_at_cap_labels_and_entries() {
        // Positive (no false reject): exactly MAX_LABEL_NAMES_PER_STREAM
        // distinct labels and a small in-bounds values array still parse.
        //
        // This test used to build MAX_LABELS_PER_STREAM (256) labels — that is
        // the decode-time `BoundedLabelMap` materialization cap, and since
        // issue #374 it is no longer the binding one: the ingest bound (15
        // names) rejects first. `parse_json_at_cap_label_map_is_rejected_by_
        // the_ingest_bound_not_the_map_cap` below keeps the 256-label case and
        // pins WHICH bound answers, so the map cap's positive edge is still
        // covered.
        let mut map = String::from("{");
        for i in 0..log_label_limits::MAX_LABEL_NAMES_PER_STREAM {
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
    fn parse_json_at_cap_label_map_is_rejected_by_the_ingest_bound_not_the_map_cap() {
        // Exactly MAX_LABELS_PER_STREAM (256) distinct labels: the decode-time
        // `BoundedLabelMap` cap still admits it (that cap fires at 257), and
        // the issue #374 ingest bound is what rejects. Asserting the message
        // discriminates the two — a regression that moved the map cap down to
        // 256 would produce "stream labels exceed" instead.
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
        assert!(out.rows.is_empty());
        assert_eq!(out.stream_errors.len(), 1);
        assert!(
            out.stream_errors[0].ends_with("' has 256 label names; limit 15"),
            "{}",
            out.stream_errors[0]
        );
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

    // -- per-stream label rules (issue #374) -------------------------------
    //
    // Reference: `pkg/distributor/validator.go:157-199 @ v3.7.4`, reached from
    // `parseStreamLabels` (`distributor.go:1370-1387`). The rules and their
    // edges live in `log_label_limits`; these cases pin that BOTH Loki-push
    // transports reach them, that the empty-value drop reaches the stored
    // identity and not just the validator, that a breach costs only its own
    // stream, and that the reachability of the duplicate-name bound differs
    // between the transports exactly as it does upstream.

    fn json_body_with_labels(map: &str) -> String {
        format!(r#"{{"streams":[{{"stream":{map},"values":[["1700000000000000000","hi"]]}}]}}"#)
    }

    /// The single-stream shape a client sees: no rows admitted, one message.
    fn only_stream_error(out: &ParsedLogs) -> &str {
        assert!(out.rows.is_empty(), "a rejected stream contributes no rows");
        assert_eq!(out.stream_errors.len(), 1, "{:?}", out.stream_errors);
        &out.stream_errors[0]
    }

    #[test]
    fn parse_json_rejects_a_label_value_over_2048_bytes() {
        let body = json_body_with_labels(&format!(r#"{{"app":"{}"}}"#, "b".repeat(2049)));
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert!(only_stream_error(&out).contains("has label value too long"));
    }

    #[test]
    fn parse_json_accepts_a_label_value_at_2048_bytes() {
        let body = json_body_with_labels(&format!(r#"{{"app":"{}"}}"#, "b".repeat(2048)));
        assert_eq!(parse_json(body.as_bytes(), 0).unwrap().rows.len(), 1);
    }

    #[test]
    fn parse_json_rejects_a_label_name_over_1024_bytes() {
        let body = json_body_with_labels(&format!(r#"{{"{}":"v"}}"#, "a".repeat(1025)));
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert!(only_stream_error(&out).contains("has label name too long"));
    }

    #[test]
    fn parse_json_rejects_sixteen_labels_and_accepts_fifteen() {
        let map = |n: usize| {
            let inner = (0..n)
                .map(|i| format!(r#""l{i}":"v""#))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{inner}}}")
        };
        assert_eq!(
            parse_json(json_body_with_labels(&map(15)).as_bytes(), 0)
                .unwrap()
                .rows
                .len(),
            1
        );
        let out = parse_json(json_body_with_labels(&map(16)).as_bytes(), 0).unwrap();
        assert!(only_stream_error(&out).ends_with("' has 16 label names; limit 15"));
    }

    #[test]
    fn parse_json_duplicate_label_key_collapses_and_is_accepted() {
        // `BoundedLabelMap` is a `BTreeMap`, so a repeated JSON key is
        // last-write-wins before any bound sees it. The reference behaves the
        // same way (measured on `grafana/loki:3.7.4`:
        // `{"foo":"bar","foo":"barf"}` -> `204`), so the duplicate-name bound
        // is unreachable on this transport by construction, not by omission.
        let body = json_body_with_labels(r#"{"foo":"bar","foo":"barf"}"#);
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert_eq!(out.streams.len(), 1);
        assert_eq!(out.streams[0].labels.get("foo"), Some("barf"));
    }

    /// The envelope's `streams` key is matched with ASCII case folding, so
    /// every case variant carries its lines through to storage. Upstream
    /// `loghttp.PushRequest` is decoded by jsoniter reflection under
    /// `ConfigDefault` (`CaseSensitive: false`), which folds `'A'..='Z'` in the
    /// wire key before hashing it (`iter_object.go:85-87`,
    /// `reflect_struct_decoder.go:36-41 @ jsoniter v1.1.12`). Measured on
    /// `grafana/loki@sha256:87f0a067…`: all four spellings `204`, and each
    /// line reads back out of `/loki/api/v1/query_range`.
    ///
    /// The escaped spelling is here because upstream folds the *unescaped*
    /// key (`readStringSlowPath` then the same fold), as `serde_json` hands us
    /// the unescaped key — so the two agree for a reason, not by luck.
    #[test]
    fn parse_json_matches_the_streams_key_case_insensitively() {
        for spelling in ["streams", "Streams", "STREAMS", "StReAmS", "streamS"] {
            let body = format!(
                r#"{{"{spelling}":[{{"stream":{{"app":"a"}},"values":[["1700000000000000000","hi"]]}}]}}"#
            );
            let out = parse_json(body.as_bytes(), 0).unwrap_or_else(|e| panic!("{spelling}: {e}"));
            assert_eq!(out.rows.len(), 1, "{spelling}");
            assert_eq!(out.rows[0].body, "hi", "{spelling}");
            assert_eq!(out.streams.len(), 1, "{spelling}");
        }
        // `\u0053treams`: the key on the wire is not the bytes `Streams`,
        // but both decoders unescape before folding.
        let escaped = br#"{"\u0053treams":[{"stream":{"app":"a"},
             "values":[["1700000000000000000","hi"]]}]}"#;
        let out = parse_json(escaped, 0).expect("escaped uppercase spelling");
        assert_eq!(out.rows.len(), 1);
    }

    /// The neighbour that keeps the fold ASCII-only and anchored: a key that
    /// merely CONTAINS the field name, or differs outside `[A-Za-z]`, is an
    /// unknown key and the request is stream-less. Upstream compares the whole
    /// folded key, so `"streams "` measures `422` there too.
    #[test]
    fn parse_json_case_folding_does_not_widen_the_streams_key() {
        for spelling in ["streams ", " streams", "stream", "streamss", "xstreams"] {
            let body = format!(
                r#"{{"{spelling}":[{{"stream":{{"app":"a"}},"values":[["1700000000000000000","hi"]]}}]}}"#
            );
            let err = parse_json(body.as_bytes(), 0).expect_err(spelling);
            assert!(
                matches!(err, LogsIngestError::MissingStreams),
                "{spelling}: {err:?}"
            );
        }
    }

    /// A repeated `streams` key — same spelling or a different case of it —
    /// is last-wins, not a duplicate-field error. `oneFieldStructDecoder`
    /// re-runs the field decoder on every match and `sliceDecoder` re-grows
    /// from index zero (`reflect_struct_decoder.go:574-590`,
    /// `reflect_slice.go:66-99 @ jsoniter v1.1.12`). Measured: `204` with only
    /// the LAST occurrence's line stored, where we used to answer `400`.
    #[test]
    fn parse_json_a_repeated_streams_key_is_last_wins() {
        let two = |first: &str, second: &str| {
            format!(
                r#"{{"{first}":[{{"stream":{{"app":"a"}},"values":[["1700000000000000000","first"]]}}],"{second}":[{{"stream":{{"app":"b"}},"values":[["1700000000000000000","second"]]}}]}}"#
            )
        };
        for (first, second) in [
            ("streams", "streams"),
            ("streams", "Streams"),
            ("Streams", "streams"),
        ] {
            let body = two(first, second);
            let out =
                parse_json(body.as_bytes(), 0).unwrap_or_else(|e| panic!("{first}/{second}: {e}"));
            assert_eq!(out.rows.len(), 1, "{first}/{second}");
            assert_eq!(out.rows[0].body, "second", "{first}/{second}");
        }
    }

    /// The same last-wins rule reaches the stream-less `422`: an empty or
    /// `null` last occurrence discards a populated earlier one. `null`
    /// overwrites because `sliceDecoder` takes `UnsafeSetNil` before it looks
    /// at any element (`reflect_slice.go:69-72`). Measured `422` for both, and
    /// nothing of the first occurrence is stored.
    #[test]
    fn parse_json_a_trailing_empty_or_null_streams_key_empties_the_request() {
        let populated =
            r#""Streams":[{"stream":{"app":"a"},"values":[["1700000000000000000","first"]]}]"#;
        for trailing in [r#""streams":[]"#, r#""streams":null"#] {
            let body = format!("{{{populated},{trailing}}}");
            let err = parse_json(body.as_bytes(), 0).expect_err(&body);
            assert!(matches!(err, LogsIngestError::MissingStreams), "{err:?}");
        }
        // ...and the reverse order keeps the populated one.
        let body = format!(r#"{{"streams":null,{populated}}}"#);
        let out = parse_json(body.as_bytes(), 0).expect("null then populated");
        assert_eq!(out.rows.len(), 1);
    }

    /// A stream object's own keys are NOT case-folded: upstream decodes it
    /// with a hand-written `UnmarshalJSON` that switches on `string(key)`
    /// (`pkg/loghttp/query.go:99-121 @ v3.7.4`), so `Stream`/`Values` are
    /// unknown keys on both sides — measured `204` on Loki with nothing
    /// stored. This is the discriminating neighbour of the envelope test
    /// above: the fold is one field's, not the decoder's.
    #[test]
    fn parse_json_a_stream_objects_keys_are_case_sensitive() {
        let body =
            br#"{"streams":[{"Stream":{"app":"a"},"Values":[["1700000000000000000","hi"]]}]}"#;
        let out = parse_json(body, 0).expect("unknown stream keys are ignored, not rejected");
        assert!(out.rows.is_empty());
        assert!(out.streams.is_empty());
        assert!(out.stream_errors.is_empty());
    }

    /// Repeated `stream` / `values` keys inside one stream object are
    /// last-wins for the same reason — the reference's switch simply runs
    /// again — except that a `null` `values` returns from the callback before
    /// assigning, so it leaves the entries alone (`query.go:110-112`).
    /// Measured: all four `204`, storing `nd-second` / `line-nv-second` /
    /// `line-vn`, where we used to answer `400`.
    #[test]
    fn parse_json_repeated_stream_object_keys_are_last_wins() {
        let out = parse_json(
            br#"{"streams":[{"stream":{"app":"first"},"stream":{"app":"second"},
                 "values":[["1700000000000000000","hi"]]}]}"#,
            0,
        )
        .expect("repeated stream key");
        assert_eq!(out.streams.len(), 1);
        assert_eq!(out.streams[0].labels.get("app"), Some("second"));

        let out = parse_json(
            br#"{"streams":[{"stream":{"app":"a"},"values":[["1700000000000000000","first"]],
                 "values":[["1700000000000000000","second"]]}]}"#,
            0,
        )
        .expect("repeated values key");
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].body, "second");

        // `null` after a populated `values` keeps the entries...
        let out = parse_json(
            br#"{"streams":[{"stream":{"app":"a"},"values":[["1700000000000000000","kept"]],
                 "values":null}]}"#,
            0,
        )
        .expect("null values after a populated one");
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].body, "kept");

        // ...and `null` alone is an entry-less stream, which is still a stream.
        let out = parse_json(br#"{"streams":[{"stream":{"app":"a"},"values":null}]}"#, 0)
            .expect("null values alone");
        assert!(out.rows.is_empty());
        assert!(out.stream_errors.is_empty());
    }

    /// Issue #374 round 11: last-wins resolves BEFORE any of our structural
    /// caps is charged, so an occurrence the reference has already discarded
    /// cannot decide the request.
    ///
    /// The reference never looks at a superseded value at all — its one-field
    /// envelope decoder re-runs the field decoder per occurrence
    /// (`reflect_struct_decoder.go:574-590 @ jsoniter v1.1.12`) and a stream
    /// object's hand-written switch re-runs per key (`pkg/loghttp/query.go:99-121
    /// @ v3.7.4`) — so a cap-breaking first occurrence followed by a valid last
    /// one is `204` there, and the last one's line is stored. Measured against
    /// `grafana/loki@sha256:87f0a067…` for every row below; each was `400` here
    /// before this change.
    ///
    /// This is the whole enumeration of "a cap chargeable before a resolution
    /// point", not the three that were probed: three resolution points
    /// (`streams`, `stream`, `values`) crossed with the four per-occurrence caps
    /// that can be reached inside each. The fourth resolution point — duplicate
    /// keys collapsing inside one `stream` map — is
    /// `parse_json_duplicate_label_keys_collapse_before_the_label_cap_is_charged`.
    /// The two SHARED counters (`MAX_TOTAL_ENTRIES_PER_REQUEST`,
    /// `MAX_DECODED_BYTES`) are deliberately NOT in this set; see
    /// `parse_json_a_superseded_value_still_charges_the_shared_budget`.
    ///
    /// Each row is a PAIR: the over-cap value superseded (accepted, final line
    /// stored) and the same value last (rejected, naming its own cap). Without
    /// the second half, deleting the cap outright would pass.
    #[test]
    fn parse_json_a_superseded_over_cap_value_does_not_decide_the_request() {
        let entry = r#"["1700000000000000000","final"]"#;
        let good_values = format!("[{entry}]");
        let good_stream = format!(r#"{{"stream":{{"app":"win"}},"values":{good_values}}}"#);

        let repeat = |unit: &str, n: usize| {
            let mut s = String::with_capacity(unit.len() * n + n);
            for i in 0..n {
                if i > 0 {
                    s.push(',');
                }
                s.push_str(unit);
            }
            s
        };
        // One element past each cap, so the pairs below are cap/cap+1 edges.
        let over_values = format!(
            "[{}]",
            repeat(r#"["1700000000000000000","x"]"#, MAX_ENTRIES_PER_STREAM + 1)
        );
        let over_labels = {
            let mut m = String::from("{");
            for i in 0..=MAX_LABELS_PER_STREAM {
                if i > 0 {
                    m.push(',');
                }
                m.push_str(&format!(r#""k{i}":"v""#));
            }
            m.push('}');
            m
        };
        let over_raw_pairs = format!(
            "{{{}}}",
            repeat(r#""dup":"v""#, MAX_RAW_LABEL_PAIRS_PER_STREAM + 1)
        );
        let over_sm = format!(
            r#"[["1700000000000000000","x",{{{}}}]]"#,
            repeat(r#""dup":"v""#, MAX_STRUCTURED_METADATA_PER_ENTRY + 1)
        );
        let over_streams = format!("[{}]", repeat("{}", MAX_STREAMS_PER_REQUEST + 1));

        // (case, superseded-first body, same-value-last body, the cap's message)
        let cases = [
            (
                "streams / entries cap",
                format!(
                    r#"{{"streams":[{{"stream":{{"app":"a"}},"values":{over_values}}}],"StReAmS":[{good_stream}]}}"#
                ),
                format!(
                    r#"{{"StReAmS":[{good_stream}],"streams":[{{"stream":{{"app":"a"}},"values":{over_values}}}]}}"#
                ),
                "entries count",
            ),
            (
                "streams / label count",
                format!(
                    r#"{{"streams":[{{"stream":{over_labels},"values":{good_values}}}],"Streams":[{good_stream}]}}"#
                ),
                format!(
                    r#"{{"Streams":[{good_stream}],"streams":[{{"stream":{over_labels},"values":{good_values}}}]}}"#
                ),
                "stream labels exceed",
            ),
            (
                "streams / raw label pairs",
                format!(
                    r#"{{"streams":[{{"stream":{over_raw_pairs},"values":{good_values}}}],"Streams":[{good_stream}]}}"#
                ),
                format!(
                    r#"{{"Streams":[{good_stream}],"streams":[{{"stream":{over_raw_pairs},"values":{good_values}}}]}}"#
                ),
                "stream label pairs exceed",
            ),
            (
                "streams / structured metadata",
                format!(
                    r#"{{"streams":[{{"stream":{{"app":"a"}},"values":{over_sm}}}],"Streams":[{good_stream}]}}"#
                ),
                format!(
                    r#"{{"Streams":[{good_stream}],"streams":[{{"stream":{{"app":"a"}},"values":{over_sm}}}]}}"#
                ),
                "structured_metadata count",
            ),
            (
                "streams / stream count",
                format!(r#"{{"streams":{over_streams},"Streams":[{good_stream}]}}"#),
                format!(r#"{{"Streams":[{good_stream}],"streams":{over_streams}}}"#),
                "streams count",
            ),
            (
                "stream / label count",
                format!(
                    r#"{{"streams":[{{"stream":{over_labels},"stream":{{"app":"win"}},"values":{good_values}}}]}}"#
                ),
                format!(
                    r#"{{"streams":[{{"stream":{{"app":"win"}},"stream":{over_labels},"values":{good_values}}}]}}"#
                ),
                "stream labels exceed",
            ),
            (
                "stream / raw label pairs",
                format!(
                    r#"{{"streams":[{{"stream":{over_raw_pairs},"stream":{{"app":"win"}},"values":{good_values}}}]}}"#
                ),
                format!(
                    r#"{{"streams":[{{"stream":{{"app":"win"}},"stream":{over_raw_pairs},"values":{good_values}}}]}}"#
                ),
                "stream label pairs exceed",
            ),
            (
                "values / entries cap",
                format!(
                    r#"{{"streams":[{{"stream":{{"app":"win"}},"values":{over_values},"values":{good_values}}}]}}"#
                ),
                format!(
                    r#"{{"streams":[{{"stream":{{"app":"win"}},"values":{good_values},"values":{over_values}}}]}}"#
                ),
                "entries count",
            ),
            (
                "values / structured metadata",
                format!(
                    r#"{{"streams":[{{"stream":{{"app":"win"}},"values":{over_sm},"values":{good_values}}}]}}"#
                ),
                format!(
                    r#"{{"streams":[{{"stream":{{"app":"win"}},"values":{good_values},"values":{over_sm}}}]}}"#
                ),
                "structured_metadata count",
            ),
        ];

        for (case, superseded, surviving, cap_message) in cases {
            let out = parse_json(superseded.as_bytes(), 0)
                .unwrap_or_else(|e| panic!("{case}: superseded value still decided it: {e}"));
            assert_eq!(out.rows.len(), 1, "{case}");
            assert_eq!(out.rows[0].body, "final", "{case}");
            assert!(
                out.stream_errors.is_empty(),
                "{case}: {:?}",
                out.stream_errors
            );

            let err = parse_json(surviving.as_bytes(), 0)
                .err()
                .unwrap_or_else(|| panic!("{case}: the SURVIVING over-cap value was admitted"));
            assert!(
                err.to_string().contains(cap_message),
                "{case}: expected the cap {cap_message:?}, got {err}"
            );
        }
    }

    /// The two counters that stay immediately fatal, and why the row above stops
    /// where it does. `MAX_TOTAL_ENTRIES_PER_REQUEST` and `MAX_DECODED_BYTES`
    /// are charged across every occurrence and are NOT given back when one is
    /// superseded: they measure memory this request has already cost, and the
    /// superseding value decodes while the superseded one is still alive, so
    /// deferring them would mean decoding past the budget to find out whether it
    /// mattered. The reference has no equivalent — its only bound on the JSON
    /// push path is the 100 MiB compressed body (`distributor.max-recv-msg-size`,
    /// `pkg/distributor/distributor.go:124 @ v3.7.4`, applied by
    /// `io.LimitReader` in `parsePushRequestBody`, `pkg/loghttp/push/push.go:322-325`)
    /// — so this is a stated divergence, ledgered, not an oversight.
    ///
    /// Cheap to pin without a 46 MB body: two streams whose entry counts sum
    /// past the aggregate inside ONE superseded occurrence reject exactly as
    /// they do without the second occurrence.
    #[test]
    fn parse_json_a_superseded_value_still_charges_the_shared_budget() {
        let per = MAX_ENTRIES_PER_STREAM;
        let streams = MAX_TOTAL_ENTRIES_PER_REQUEST / per + 1; // 51 -> 5.1M
        let mut one = String::from(r#"{"stream":{"a":"b"},"values":["#);
        for i in 0..per {
            if i > 0 {
                one.push(',');
            }
            one.push_str(r#"["1700000000000000000","x"]"#);
        }
        one.push_str("]}");
        let mut big = String::from("[");
        for i in 0..streams {
            if i > 0 {
                big.push(',');
            }
            big.push_str(&one);
        }
        big.push(']');
        let body = format!(
            r#"{{"streams":{big},"Streams":[{{"stream":{{"app":"win"}},"values":[["1700000000000000000","final"]]}}]}}"#
        );
        let err = parse_json(body.as_bytes(), 0)
            .expect_err("the shared budget is charged across occurrences");
        // #168's byte budget crosses first at ~4.8M entries; the count cap is the
        // backstop behind it. Either way the request is refused, which is the
        // divergence being recorded.
        assert!(
            err.to_string().contains("decoded bytes (estimated)")
                || err.to_string().contains("total_entries"),
            "{err}"
        );
    }

    /// Issue #374 round 12: crossing a cap must stop RETENTION, not checking.
    ///
    /// Deferring the caps (round 11) left the remainder of an over-cap run to
    /// `serde::de::IgnoredAny`, which in `serde_json` is a bracket-matching skip
    /// (`Deserializer::ignore_value`, `de.rs:1102 @ 1.0.150`) that validates no
    /// types — so a malformed tail past the cap was accepted where the same tail
    /// before the cap was refused, and the reference refuses both (it decodes
    /// every occurrence of a repeated field in full,
    /// `reflect_struct_decoder.go:574-590 @ jsoniter v1.1.12`). Measured on
    /// `grafana/loki@sha256:87f0a067…`: 257 metadata pairs then `"bad":[]`
    /// superseded was `400` there and `204` here, as was 100,001 entries then a
    /// bare `0`; the SAME tails one element BELOW the cap were `400` on both.
    ///
    /// Each row is a TRIPLE — the malformed tail below the cap (the typed path),
    /// past the cap (the drained path) and past the cap inside a superseded
    /// occurrence — and all three must produce the SAME message. Equality is
    /// what makes this an anti-drift pin rather than three "it 400s" assertions:
    /// the drained arm has its own `Deserialize` impl, and a divergence in its
    /// `expecting` text or its accept surface fails here.
    #[test]
    fn parse_json_a_drained_value_is_checked_exactly_like_a_retained_one() {
        // Everything after " at line" is the byte offset, which necessarily
        // differs between a tail at element 3 and the same tail at element
        // 100,002.
        fn rule(body: &str) -> String {
            let msg = json_loki_decode_message(body);
            msg.split(" at line").next().unwrap_or(&msg).to_string()
        }
        let repeat = |unit: &str, n: usize| {
            let mut s = String::with_capacity((unit.len() + 1) * n);
            for i in 0..n {
                if i > 0 {
                    s.push(',');
                }
                s.push_str(unit);
            }
            s
        };
        let final_values = r#"[["1700000000000000000","final"]]"#;
        let win = format!(r#"{{"stream":{{"app":"w"}},"values":{final_values}}}"#);

        let empty_streams = repeat("{}", MAX_STREAMS_PER_REQUEST + 1);
        let entries = repeat(r#"["1700000000000000000","x"]"#, MAX_ENTRIES_PER_STREAM + 1);
        let raw_pairs = repeat(r#""dup":"v""#, MAX_RAW_LABEL_PAIRS_PER_STREAM + 1);
        let sm_pairs = repeat(r#""dup":"v""#, MAX_STRUCTURED_METADATA_PER_ENTRY + 1);

        // (drain, below the cap, past the cap, past the cap and superseded)
        let cases = [
            (
                "streams: a non-object element",
                r#"{"streams":[{},{},0]}"#.to_string(),
                format!(r#"{{"streams":[{empty_streams},0]}}"#),
                format!(r#"{{"streams":[{empty_streams},0],"Streams":[{win}]}}"#),
                "invalid type: integer `0`, expected a Loki stream object with `stream` and `values`",
            ),
            (
                "stream map: a non-string label value",
                format!(
                    r#"{{"streams":[{{"stream":{{"a":"b","bad":[]}},"values":{final_values}}}]}}"#
                ),
                format!(
                    r#"{{"streams":[{{"stream":{{{raw_pairs},"bad":[]}},"values":{final_values}}}]}}"#
                ),
                format!(
                    r#"{{"streams":[{{"stream":{{{raw_pairs},"bad":[]}},"stream":{{"app":"w"}},"values":{final_values}}}]}}"#
                ),
                "invalid type: sequence, expected a string",
            ),
            (
                "values: a non-array element",
                r#"{"streams":[{"stream":{"a":"b"},"values":[["1700000000000000000","x"],0]}]}"#
                    .to_string(),
                format!(r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[{entries},0]}}]}}"#),
                format!(
                    r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[{entries},0],"values":{final_values}}}]}}"#
                ),
                "invalid type: integer `0`, expected a [timestamp, line] Loki log entry array",
            ),
            (
                "structured metadata: a non-string value",
                r#"{"streams":[{"stream":{"a":"b"},"values":[["1700000000000000000","x",{"m":"v","bad":[]}]]}]}"#
                    .to_string(),
                format!(
                    r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[["1700000000000000000","x",{{{sm_pairs},"bad":[]}}]]}}]}}"#
                ),
                format!(
                    r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[["1700000000000000000","x",{{{sm_pairs},"bad":[]}}]]}}],"Streams":[{win}]}}"#
                ),
                "invalid type: sequence, expected a string",
            ),
        ];

        for (drain, below, past, superseded, expected) in cases {
            let below_rule = rule(&below);
            assert_eq!(below_rule, expected, "{drain}: below the cap");
            assert_eq!(rule(&past), below_rule, "{drain}: past the cap");
            assert_eq!(
                rule(&superseded),
                below_rule,
                "{drain}: past the cap, superseded"
            );
        }
    }

    /// One nesting ceiling for the whole body, with no ignored-value hole in it.
    ///
    /// `serde_json` bounds typed deserialization at 128 levels
    /// (`RECURSION_LIMIT`, `de.rs:63,1375 @ 1.0.150`) but implements
    /// `IgnoredAny` as an ITERATIVE skip that the counter never sees, so before
    /// round 12 a value under a key this decoder does not read — or past a cap —
    /// could nest arbitrarily deep. The reference bounds exactly these values
    /// too: jsoniter walks a stream object with `iter.Skip()` before its
    /// unmarshaler ever runs, under `maxDepth = 10000` (`iter.go:331-338 @
    /// jsoniter v1.1.12`, error `exceeded max depth`). Measured: a 200,000-level
    /// nest under an unknown envelope key, under an unknown key inside a stream,
    /// and as an entry's fourth element was `400` upstream and `204` here.
    ///
    /// Ours is the tighter ceiling (128 against 10,000; a legal push nests six),
    /// so 129..10,000 is a recorded divergence — residual 8 of the
    /// `ingest-label-bounds` ledger row — not a claim of parity.
    #[test]
    fn parse_json_bounds_nesting_depth_in_ignored_and_drained_values() {
        let nest = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
        let entry = r#"["1700000000000000000","x"]"#;
        let win = r#"{"stream":{"app":"w"},"values":[["1700000000000000000","final"]]}"#;
        let mut entries = String::new();
        for i in 0..MAX_ENTRIES_PER_STREAM + 1 {
            if i > 0 {
                entries.push(',');
            }
            entries.push_str(entry);
        }

        // Every position a value of arbitrary shape can legally reach: the two
        // unknown-key arms and an entry's fourth element — the last one both
        // inside a RETAINED entry and inside a drained one, which is the arm the
        // cap deferral introduced.
        let position = |depth: usize| {
            let deep = nest(depth);
            [
                (
                    "unknown envelope key",
                    format!(r#"{{"streams":[{win}],"junk":{deep}}}"#),
                ),
                (
                    "unknown key inside a stream",
                    format!(
                        r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[{entry}],"junk":{deep}}}]}}"#
                    ),
                ),
                (
                    "an entry's fourth element",
                    format!(
                        r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[["1700000000000000000","x",{{}},{deep}]]}}]}}"#
                    ),
                ),
                (
                    // Superseded, so that at 64 levels the request is accepted
                    // rather than refused by the entries cap itself — the depth
                    // ceiling is then the only thing that can refuse it at 200.
                    "an entry's fourth element, past the entries cap",
                    format!(
                        r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[{entries},["1700000000000000000","x",{{}},{deep}]],"values":[{entry}]}}]}}"#
                    ),
                ),
            ]
        };

        for (position, body) in position(64) {
            parse_json(body.as_bytes(), 0)
                .unwrap_or_else(|e| panic!("{position}: 64 levels must decode: {e}"));
        }
        for (position, body) in position(200) {
            let err = json_loki_decode_message(&body);
            assert!(
                err.contains("recursion limit exceeded"),
                "{position}: 200 levels must be refused by the depth ceiling, got {err:?}"
            );
        }
    }

    /// The case fold does not skip the per-stream label bounds: an over-wide
    /// value pushed under `Streams` is the same `400` it is under `streams`
    /// (measured — Loki answers `400 stream '{app="bbb…"}' has label value
    /// too long, whereas this branch used to answer `422` before the fold).
    #[test]
    fn parse_json_a_case_variant_streams_key_still_charges_the_label_bounds() {
        let body = format!(
            r#"{{"Streams":[{{"stream":{{"app":"{}"}},"values":[["1700000000000000000","x"]]}}]}}"#,
            "b".repeat(2049)
        );
        let out = parse_json(body.as_bytes(), 0).expect("bad stream is dropped, not a 422");
        assert!(out.rows.is_empty());
        assert!(only_stream_error(&out).contains("label value too long"));
    }

    /// Issue #374: `PushWithResolver` refuses a push with no streams at all
    /// before it validates anything (`distributor.go:579-581 @ v3.7.4`).
    /// Both JSON spellings of "no streams" — the empty array and the absent
    /// key — measure `422` on `grafana/loki@sha256:87f0a067…`.
    #[test]
    fn parse_json_rejects_a_request_with_no_streams() {
        for body in [r#"{"streams":[]}"#, "{}"] {
            let err = parse_json(body.as_bytes(), 0).expect_err(body);
            assert!(
                matches!(err, LogsIngestError::MissingStreams),
                "{body}: {err:?}"
            );
            assert_eq!(
                err.to_string(),
                "error at least one valid stream is required for ingestion"
            );
        }
    }

    /// The protobuf half: an empty, well-formed `PushRequest` is the same
    /// `422`, charged at [`decode_protobuf`]'s shared [`validate_bounds`]
    /// seam rather than at [`parse_protobuf`].
    #[test]
    fn decode_protobuf_rejects_a_request_with_no_streams() {
        let err = decode_protobuf(&[]).expect_err("empty push request");
        assert!(matches!(err, LogsIngestError::MissingStreams), "{err:?}");
    }

    /// The discriminating neighbour of the two tests above: a stream with no
    /// entries is still a stream, so the request is accepted. `len(streams)`
    /// is counted on the wire, before the per-stream loop skips it.
    #[test]
    fn parse_json_entry_less_stream_alone_is_not_a_stream_less_request() {
        let out = parse_json(br#"{"streams":[{"stream":{"app":"a"},"values":[]}]}"#, 0).unwrap();
        assert!(out.rows.is_empty());
        assert!(out.stream_errors.is_empty());
    }

    #[test]
    fn parse_json_entry_less_stream_skips_the_label_bounds() {
        // `pkg/distributor/distributor.go:639-641 @ v3.7.4` continues past a
        // stream with no entries before validating it.
        let body = format!(
            r#"{{"streams":[{{"stream":{{"app":"{}"}},"values":[]}}]}}"#,
            "b".repeat(4096)
        );
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert!(out.rows.is_empty());
        assert!(out.stream_errors.is_empty());
    }

    /// `WithoutEmpty` (`pkg/logql/syntax/parser.go:296 @ v3.7.4`) runs before
    /// the hash, so an empty-valued label must not reach the stored label set
    /// or the fingerprint — accepting it on both sides but storing it on one
    /// would silently split a stream in two.
    #[test]
    fn parse_json_an_empty_valued_label_is_absent_from_the_stored_stream() {
        let with_empty = parse_json(
            json_body_with_labels(r#"{"a":"1","ignored":""}"#).as_bytes(),
            0,
        )
        .unwrap();
        let without = parse_json(json_body_with_labels(r#"{"a":"1"}"#).as_bytes(), 0).unwrap();
        assert_eq!(with_empty.streams.len(), 1);
        assert_eq!(with_empty.streams[0].labels.get("ignored"), None);
        assert_eq!(with_empty.streams[0].labels, without.streams[0].labels);
        assert_eq!(
            with_empty.streams[0].fingerprint,
            without.streams[0].fingerprint
        );
        assert_eq!(with_empty.rows[0].fingerprint, without.rows[0].fingerprint);
    }

    /// The map's last-write-wins collapse happens BEFORE the empty-value drop,
    /// as it does upstream (`loghttp.LabelSet` is a map, rendered and only then
    /// parsed by `syntax.ParseLabels`), so a key whose last occurrence is empty
    /// leaves the stream with no labels at all rather than with the earlier
    /// value.
    #[test]
    fn parse_json_a_key_whose_last_occurrence_is_empty_leaves_no_label() {
        let body = json_body_with_labels(r#"{"foo":"bar","foo":""}"#);
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert_eq!(out.streams.len(), 1);
        assert_eq!(out.streams[0].labels.get("foo"), None);
    }

    /// Review finding: empty-valued labels must not consume the decode-time
    /// label cap either, or the reference's answer is pre-empted by ours at
    /// 16 non-empty labels rather than at 257. 16 + 241 empty is 257 raw pairs.
    #[test]
    fn parse_json_empty_labels_do_not_consume_the_decode_time_label_cap() {
        let mut pairs: Vec<String> = (0..16).map(|i| format!(r#""l{i}":"v""#)).collect();
        pairs.extend((0..241).map(|i| format!(r#""e{i}":"""#)));
        let body = json_body_with_labels(&format!("{{{}}}", pairs.join(",")));
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert!(
            only_stream_error(&out).ends_with("' has 16 label names; limit 15"),
            "{:?}",
            out.stream_errors
        );

        // ...and 15 non-empty padded the same way is accepted outright, which
        // is what `grafana/loki:3.7.4` answers (`204`).
        let mut ok: Vec<String> = (0..15).map(|i| format!(r#""l{i}":"v""#)).collect();
        ok.extend((0..250).map(|i| format!(r#""e{i}":"""#)));
        let body = json_body_with_labels(&format!("{{{}}}", ok.join(",")));
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        assert_eq!(out.streams[0].labels.len(), 15);
    }

    /// `validator.go:164-167 @ v3.7.4`: a stream carrying `__aggregated_metric__`
    /// or `__pattern__` returns before all four bounds. PulsusDB never
    /// generates such a stream, but a client can push one on either side.
    #[test]
    fn parse_json_an_internal_stream_is_exempt_from_the_bounds() {
        for internal in ["__aggregated_metric__", "__pattern__"] {
            let mut pairs: Vec<String> = (0..16).map(|i| format!(r#""l{i}":"v""#)).collect();
            pairs.push(format!(r#""{internal}":"x""#));
            let body = json_body_with_labels(&format!("{{{}}}", pairs.join(",")));
            let out = parse_json(body.as_bytes(), 0).unwrap();
            assert!(out.stream_errors.is_empty(), "{internal}");
            assert_eq!(out.rows.len(), 1, "{internal}");
        }
    }

    /// The reference writes the streams that passed and answers `400` after
    /// them (`distributor.go:645-655, 780-790, 929 @ v3.7.4`), so one
    /// malformed stream must not cost a client the rest of its batch.
    #[test]
    fn parse_json_a_bad_stream_costs_only_itself() {
        let body = format!(
            r#"{{"streams":[
                {{"stream":{{"app":"good"}},"values":[["1700000000000000000","a"]]}},
                {{"stream":{{"app":"{}"}},"values":[["1700000000000000000","b"]]}},
                {{"stream":{{"app":"also_good"}},"values":[["1700000000000000000","c"]]}}
            ]}}"#,
            "b".repeat(2049)
        );
        let out = parse_json(body.as_bytes(), 0).unwrap();
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.streams.len(), 2);
        assert_eq!(out.stream_errors.len(), 1);
        assert!(out.stream_errors[0].contains("has label value too long"));
        let stored: Vec<&str> = out
            .streams
            .iter()
            .map(|s| s.labels.get("app").unwrap_or_default())
            .collect();
        assert_eq!(stored, vec!["good", "also_good"]);
    }

    #[test]
    fn parse_protobuf_rejects_a_duplicate_label_name() {
        // The one log transport that can carry a repeat to the bound, here and
        // upstream (measured on `grafana/loki:3.7.4`: `{foo="bar",
        // foo="barf"}` -> `400 ... has duplicate label name: 'foo'`).
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{foo="bar", foo="barf"}"#.to_string(),
                entries: vec![entry(1_700_000_000, 0, "a")],
            }],
        };
        let out = parse_protobuf(&req, 0).unwrap();
        assert_eq!(
            only_stream_error(&out),
            "stream '{foo=\"bar\", foo=\"barf\"}' has duplicate label name: 'foo'"
        );
    }

    /// `WithoutEmpty` runs before the duplicate test upstream (the literal is
    /// parsed with both copies, then filtered), so a repeat whose other copy
    /// is empty-valued is not a duplicate.
    #[test]
    fn parse_protobuf_a_repeat_whose_other_copy_is_empty_is_not_a_duplicate() {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{foo="bar", foo=""}"#.to_string(),
                entries: vec![entry(1_700_000_000, 0, "a")],
            }],
        };
        let out = parse_protobuf(&req, 0).unwrap();
        assert!(out.stream_errors.is_empty(), "{:?}", out.stream_errors);
        assert_eq!(out.streams[0].labels.get("foo"), Some("bar"));
    }

    #[test]
    fn parse_protobuf_rejects_a_label_value_over_2048_bytes() {
        let value = "b".repeat(2049);
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: format!(r#"{{app="{value}"}}"#),
                entries: vec![entry(1_700_000_000, 0, "a")],
            }],
        };
        let out = parse_protobuf(&req, 0).unwrap();
        assert_eq!(
            only_stream_error(&out),
            format!("stream '{{app=\"{value}\"}}' has label value too long: '{value}'")
        );
    }

    #[test]
    fn parse_protobuf_rejects_sixteen_labels_and_accepts_fifteen_plus_service_name() {
        let literal = |n: usize| {
            let inner = (0..n)
                .map(|i| format!(r#"l{i}="v""#))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{inner}}}")
        };
        let req = |labels: String| PushRequest {
            streams: vec![StreamAdapter {
                labels,
                entries: vec![entry(1_700_000_000, 0, "a")],
            }],
        };
        assert_eq!(parse_protobuf(&req(literal(15)), 0).unwrap().rows.len(), 1);
        let out = parse_protobuf(&req(literal(16)), 0).unwrap();
        assert!(only_stream_error(&out).ends_with("' has 16 label names; limit 15"));
        // `service_name` is not counted (`validator.go:169-174 @ v3.7.4`), so
        // 15 labels plus one is still accepted.
        let with_service = format!(
            "{{{}, service_name=\"checkout\"}}",
            literal(15).trim_matches(|c| c == '{' || c == '}')
        );
        assert_eq!(parse_protobuf(&req(with_service), 0).unwrap().rows.len(), 1);
    }

    #[test]
    fn parse_protobuf_entry_less_stream_skips_the_label_bounds() {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: format!(r#"{{app="{}"}}"#, "b".repeat(4096)),
                entries: vec![],
            }],
        };
        let out = parse_protobuf(&req, 0).unwrap();
        assert!(out.rows.is_empty());
        assert!(out.stream_errors.is_empty());
    }

    #[test]
    fn parse_protobuf_an_empty_valued_label_is_absent_from_the_stored_stream() {
        let req = |labels: &str| PushRequest {
            streams: vec![StreamAdapter {
                labels: labels.to_string(),
                entries: vec![entry(1_700_000_000, 0, "a")],
            }],
        };
        let with_empty = parse_protobuf(&req(r#"{a="1", ignored=""}"#), 0).unwrap();
        let without = parse_protobuf(&req(r#"{a="1"}"#), 0).unwrap();
        assert_eq!(with_empty.streams[0].labels.get("ignored"), None);
        assert_eq!(
            with_empty.streams[0].fingerprint,
            without.streams[0].fingerprint
        );
    }

    /// Empty-valued labels do not consume [`MAX_LABELS_PER_STREAM`] on this
    /// transport either: the literal is parsed with 16 real labels and 241
    /// empty ones and still answers the reference's count message.
    #[test]
    fn parse_protobuf_empty_labels_do_not_consume_the_label_cap() {
        let mut pairs: Vec<String> = (0..16).map(|i| format!(r#"l{i}="v""#)).collect();
        pairs.extend((0..241).map(|i| format!(r#"e{i}="""#)));
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: format!("{{{}}}", pairs.join(", ")),
                entries: vec![entry(1_700_000_000, 0, "a")],
            }],
        };
        let out = parse_protobuf(&req, 0).unwrap();
        assert!(only_stream_error(&out).ends_with("' has 16 label names; limit 15"));
    }

    #[test]
    fn parse_protobuf_an_internal_stream_is_exempt_from_the_bounds() {
        for internal in ["__aggregated_metric__", "__pattern__"] {
            let mut pairs: Vec<String> = (0..16).map(|i| format!(r#"l{i}="v""#)).collect();
            pairs.push(format!(r#"{internal}="x""#));
            let req = PushRequest {
                streams: vec![StreamAdapter {
                    labels: format!("{{{}}}", pairs.join(", ")),
                    entries: vec![entry(1_700_000_000, 0, "a")],
                }],
            };
            let out = parse_protobuf(&req, 0).unwrap();
            assert!(out.stream_errors.is_empty(), "{internal}");
            assert_eq!(out.rows.len(), 1, "{internal}");
        }
    }

    #[test]
    fn parse_protobuf_a_bad_stream_costs_only_itself() {
        let req = PushRequest {
            streams: vec![
                StreamAdapter {
                    labels: r#"{app="good"}"#.to_string(),
                    entries: vec![entry(1_700_000_000, 0, "a")],
                },
                StreamAdapter {
                    labels: r#"{foo="bar", foo="barf"}"#.to_string(),
                    entries: vec![entry(1_700_000_000, 0, "b")],
                },
            ],
        };
        let out = parse_protobuf(&req, 0).unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.streams[0].labels.get("app"), Some("good"));
        assert_eq!(out.stream_errors.len(), 1);
        assert!(out.stream_errors[0].contains("has duplicate label name"));
    }

    /// `maxStreamLabelsSize` (`pkg/logql/syntax/parser.go:22,280-281 @
    /// v3.7.4`): the reference's own bound on one stream's label literal,
    /// adopted verbatim.
    #[test]
    fn parse_label_pairs_rejects_a_literal_over_sixteen_mebibytes() {
        let literal = format!(r#"{{app="{}"}}"#, "b".repeat(MAX_STREAM_LABELS_BYTES));
        let err = parse_label_set(&literal).unwrap_err();
        let LogsIngestError::LokiDecode(msg) = err else {
            panic!("expected LokiDecode, got a different variant");
        };
        assert!(msg.contains("exceeds limit of"), "{msg}");
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
        // The JSON visitor stops materializing at pair 257 and drains the rest;
        // `canonical_structured_metadata` — which runs after the envelope's
        // last-wins resolution — raises the SAME `OversizeMessage` the protobuf
        // path does from the same call site. `actual` is the materialized count,
        // so `MAX + 1` out of `MAX + 8` encoded proves the drain ran.
        let mut obj = String::from("{");
        for i in 0..MAX_STRUCTURED_METADATA_PER_ENTRY + 8 {
            if i > 0 {
                obj.push(',');
            }
            obj.push_str(&format!(r#""k{i}":"v""#));
        }
        obj.push('}');
        let body = format!(
            r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[["1700000000000000000","x",{obj}]]}}]}}"#
        );
        assert_eq!(
            json_oversize(&body),
            ("structured_metadata", MAX_STRUCTURED_METADATA_PER_ENTRY + 1)
        );
    }

    #[test]
    fn parse_json_duplicate_structured_metadata_keys_cannot_evade_the_bound() {
        // Unlike a stream's `stream` map, structured metadata is NOT collapsed
        // by the reference before it is counted: `unmarshalHTTPToLogProtoEntry`
        // appends every pair to a slice (`pkg/loghttp/query.go:181-196 @
        // v3.7.4`), so raw pairs are the right unit on both sides and a
        // duplicate-key object cannot evade the bound. 257 repetitions of one
        // key raise the same `OversizeMessage` the distinct-key case does.
        let mut obj = String::from("{");
        for i in 0..MAX_STRUCTURED_METADATA_PER_ENTRY + 8 {
            if i > 0 {
                obj.push(',');
            }
            obj.push_str(r#""dup":"v""#);
        }
        obj.push('}');
        let body = format!(
            r#"{{"streams":[{{"stream":{{"a":"b"}},"values":[["1700000000000000000","x",{obj}]]}}]}}"#
        );
        assert_eq!(
            json_oversize(&body),
            ("structured_metadata", MAX_STRUCTURED_METADATA_PER_ENTRY + 1)
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
