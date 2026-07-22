//! Prometheus remote-write parser (issue #28 architect plan, docs/
//! architecture.md §4): a pure `bytes -> WriteRequest -> ParsedMetrics`
//! pipeline with no I/O — structurally identical to `otlp_metrics`'s
//! decode/parse split, but simpler: remote-write arrives **pre-flattened**
//! (a histogram's `_bucket`/`_sum`/`_count` and a summary's quantile series
//! are already distinct `TimeSeries`, each carrying its own `__name__` and
//! `le`/`quantile` labels), so there is no per-type flattening, no
//! temporality, no exponential-bucket math — just `__name__` extraction,
//! label normalization through the frozen `LabelSet::from_normalized`,
//! `metric_fingerprint`, and verbatim `(ms, value)` samples.
//!
//! ## Wire types: hand-rolled prompb structs
//!
//! The prompb message set below is the RW-1.0 stable schema, hand-rolled at
//! its exact field tags — mirroring the hand-rolled `google.rpc.Status` in
//! `ingest/http.rs` — no protoc/build-dep, no new crate dependency
//! (`prost`/`snap` are already `pulsus-write` deps). The leaf messages
//! (`Label`, `Sample`, `MetricMetadataProto`) carry no repeated field and keep
//! their derived `#[derive(::prost::Message)]`, as does the repeated-free
//! [`BucketSpan`] leaf. The repeated-bearing
//! messages (`WriteRequest`, `TimeSeries`, `Histogram`) instead carry a
//! **hand-written**
//! `impl prost::Message` that caps their repeated fields **during**
//! `merge_field` (issue #115, finding #62) — see their doc comments and the
//! [`BoundedWriteRequest`] twin. Native histograms (`TimeSeries` tag 4) are
//! decoded since issue #140 — the RW-1.0 wire form IS the stored integer
//! `NativeHistogram` shape (spans + delta-encoded buckets), copied verbatim,
//! with the wire `ResetHint` mapped GAUGE→`Gauge`, everything else→`Unknown`.
//! `exemplars` (`TimeSeries` tag 3) and RW-2.0 stay intentionally
//! undeclared/out of scope: unknown fields are skipped on decode.
//!
//! Tag layout is pinned by the architect plan and cross-checked against a
//! real capture from the OpenTelemetry Collector's `prometheusremotewrite`
//! exporter (`tests/fixtures/remote-write/README.md`) — a self-consistent
//! wrong tag would decode without error but silently corrupt every field
//! after it, which only a real-wire fixture (not a synthetic round-trip
//! through the same structs) can catch.

use std::collections::HashSet;
use std::sync::Arc;

use prost::Message;
use pulsus_model::{
    CounterResetHint, Date, Fingerprint, LabelSet, METRIC_NAME_LABEL, NativeHistogram, Span,
    metric_fingerprint,
};

use crate::error::LogsIngestError;
use crate::ingest::metrics::{
    HistogramPoint, MetricMetadata, MetricPoint, ParsedMetrics, SeriesRef,
};
use crate::protocols::otlp_prescan::MAX_DECODED_BYTES;

/// `prompb.WriteRequest` (RW-1.0): `timeseries` at tag 1, `metadata` at tag
/// 3 (tag 2 is reserved on the wire for a Cortex-specific source marker,
/// never populated by a standard sender and never read here).
///
/// ## Why this does not derive `::prost::Message` (issue #115, finding #62)
///
/// A derived decoder exposes a `pub WriteRequest::decode` that materializes an
/// unbounded `timeseries`/`metadata` fan-out — and, worse, an unbounded
/// *aggregate* labels/samples fan-out across many individually-legal series —
/// charging only wire bytes before any cap runs. The hand-written
/// [`prost::Message`] impl (below) bounds **every** decode entry:
///
/// - `merge_field` caps `timeseries` (tag 1) at [`MAX_TIMESERIES_PER_REQUEST`]`
///   + 1` and `metadata` (tag 3) at [`MAX_METADATA_PER_REQUEST`]` + 1` during
///   merge (draining the excess, wire-type-checked, without allocating) and
///   delegates per-series `labels`/`samples` caps to [`TimeSeries`]'s own
///   hand-written `merge_field`.
/// - **Every** public decode/merge entry point — `decode`,
///   `decode_length_delimited`, `merge` AND `merge_length_delimited` — routes
///   through [`BoundedWriteRequest`], whose `merge_field` is the single
///   enforcing chokepoint: it additionally drains series once the cross-series
///   aggregate `total_labels`/`total_samples` exceeds
///   [`MAX_TOTAL_LABELS_PER_REQUEST`]/[`MAX_TOTAL_SAMPLES_PER_REQUEST`], so N
///   series each just under the per-series caps cannot sum past the aggregate
///   (the second-amplification the per-dimension caps alone cannot catch).
///   `prost`'s default `Message::merge` / `merge_length_delimited` call
///   `WriteRequest::merge_field` directly (which caps *counts* only), so a raw
///   `WriteRequest::default().merge(buf)` would otherwise bypass the aggregate
///   cap — these two overrides close that last gap so no public entry is an
///   uncapped bypass.
///
/// The whole-request [`LogsIngestError::OversizeMessage`] reject still lives in
/// [`decode`]'s [`validate_bounds`] (remote-write is all-or-nothing). `encode`
/// and the derived [`PartialEq`] are unchanged, and no decode-scratch field is
/// added to the value type, so the struct literals and cross-crate encoders
/// keep working.
#[derive(Clone, PartialEq, Default, Debug)]
pub struct WriteRequest {
    pub timeseries: Vec<TimeSeries>,
    pub metadata: Vec<MetricMetadataProto>,
}

impl prost::Message for WriteRequest {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        // proto3 encoding, byte-identical to the derived impl: tag 1 then tag 3
        // (declaration/tag order), tag 2 never emitted (no field).
        prost::encoding::message::encode_repeated(1u32, &self.timeseries, buf);
        prost::encoding::message::encode_repeated(3u32, &self.metadata, buf);
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
                if self.timeseries.len() > MAX_TIMESERIES_PER_REQUEST {
                    // Cap reached: drain the excess series WITHOUT materializing
                    // it, wire-type-checked exactly as `BoundedWriteRequest`'s
                    // tag-1 drain — a non-length-delimited tag-1 is a malformed
                    // submessage and must FAIL the decode, never be silently
                    // skipped. This is belt-and-suspenders: every public
                    // decode/merge entry point below routes through
                    // [`BoundedWriteRequest`], whose `merge_field` adds the
                    // cross-series aggregate drain this one lacks.
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    prost::encoding::message::merge_repeated(
                        wire_type,
                        &mut self.timeseries,
                        buf,
                        ctx,
                    )
                }
            }
            3u32 => {
                if self.metadata.len() > MAX_METADATA_PER_REQUEST {
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    prost::encoding::message::merge_repeated(
                        wire_type,
                        &mut self.metadata,
                        buf,
                        ctx,
                    )
                }
            }
            // Tag 2 (reserved) and any unknown field: skipped, as the derived
            // decoder would.
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        prost::encoding::message::encoded_len_repeated(1u32, &self.timeseries)
            + prost::encoding::message::encoded_len_repeated(3u32, &self.metadata)
    }

    fn clear(&mut self) {
        self.timeseries.clear();
        self.metadata.clear();
    }

    fn decode(buf: impl bytes::Buf) -> Result<Self, prost::DecodeError>
    where
        Self: Default,
    {
        // The most-direct public decode entry (issue #115): route through the
        // fully-bounded twin so series-count, metadata-count, per-series
        // labels/samples AND cross-series aggregate fan-out are all bounded
        // DURING decode — a direct `WriteRequest::decode` is no longer an
        // uncapped bypass of the caps the ingest path enforces.
        let bounded = BoundedWriteRequest::decode(buf)?;
        Ok(Self {
            timeseries: bounded.timeseries,
            metadata: bounded.metadata,
        })
    }

    fn decode_length_delimited(buf: impl bytes::Buf) -> Result<Self, prost::DecodeError>
    where
        Self: Default,
    {
        let bounded = BoundedWriteRequest::decode_length_delimited(buf)?;
        Ok(Self {
            timeseries: bounded.timeseries,
            metadata: bounded.metadata,
        })
    }

    fn merge(&mut self, buf: impl bytes::Buf) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        // `prost`'s default `Message::merge` calls `WriteRequest::merge_field`
        // directly, which caps only series/metadata COUNT — so a raw
        // `WriteRequest::default().merge(buf)` would fan out past the
        // cross-series aggregate caps. Route the merge through the fully-bounded
        // twin (the single enforcing chokepoint). Seed the twin with self's
        // current fields (and the aggregate re-sum, histograms included) so
        // merge-INTO-existing semantics are preserved, then move the
        // aggregate-bounded result back
        // on BOTH the Ok AND Err paths — do NOT `?` while self's fields are
        // moved out, or a decode error would leave the caller's request empty
        // (data-loss regression). Restoring first gives prost-consistent
        // partial-merge semantics.
        let mut bounded = BoundedWriteRequest::seeded_from(
            std::mem::take(&mut self.timeseries),
            std::mem::take(&mut self.metadata),
        );
        let result = bounded.merge(buf);
        self.timeseries = bounded.timeseries;
        self.metadata = bounded.metadata;
        result
    }

    fn merge_length_delimited(&mut self, buf: impl bytes::Buf) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        // `merge_length_delimited` likewise loops through `merge_field` directly
        // (it does not funnel through `merge`), so it needs the same bounded-twin
        // routing and the same both-paths field restoration as `merge` above.
        let mut bounded = BoundedWriteRequest::seeded_from(
            std::mem::take(&mut self.timeseries),
            std::mem::take(&mut self.metadata),
        );
        let result = bounded.merge_length_delimited(buf);
        self.timeseries = bounded.timeseries;
        self.metadata = bounded.metadata;
        result
    }
}

/// The **decode-time twin** of [`WriteRequest`] (issue #115): a hand-written
/// [`prost::Message`] that bounds materialization **during** `decode` so a body
/// within the 64 MiB decompressed cap cannot unpack into a far larger in-memory
/// fan-out before the count checks run. Guards, all mirroring the landed #97
/// [`crate::protocols::loki_push`] drain-past-cap-then-reject pattern:
///
/// 1. `timeseries` (tag 1) is capped at [`MAX_TIMESERIES_PER_REQUEST`]` + 1`
///    and `metadata` (tag 3) at [`MAX_METADATA_PER_REQUEST`]` + 1` — once a vec
///    would exceed its cap, the excess record is drained (wire-type-checked, no
///    allocation) rather than materialized.
/// 2. Two **transient, non-wire** accumulators, `total_labels` and
///    `total_samples`, sum every merged series' `labels.len()`/`samples.len()`.
///    prost 0.14's `DecodeError::new` is deprecated, so `merge_field` cannot
///    abort mid-decode with a custom error; instead, once either running total
///    exceeds its aggregate cap, further series are drained without
///    materializing (bounding the aggregate fan-out to `≤ aggregate cap + one
///    series' per-series cap`), and the deferred [`validate_bounds`] re-sum in
///    [`decode`] then rejects the whole request. This closes the
///    second-amplification the per-dimension caps cannot catch: many series each
///    under [`MAX_LABELS_PER_SERIES`]/[`MAX_SAMPLES_PER_SERIES`] but collectively
///    over the aggregate.
/// 3. Three more transient accumulators for native histograms (issue #140),
///    `total_histograms`/`total_hist_spans`/`total_hist_buckets` — charged
///    **incrementally per histogram DURING each series' decode** via
///    [`Self::merge_one_time_series`], not at the between-series boundary:
///    unlike labels/samples, one series' per-series histogram fan-out
///    ([`MAX_HISTOGRAMS_PER_SERIES`] × the per-histogram field caps) exceeds
///    the request-wide aggregates on its own, so a boundary-only charge would
///    let ONE crafted series fully materialize before any check ran (codex
///    review of issue #140, high finding). Once a total exceeds its cap,
///    further histograms in the same series — and then whole further series —
///    are drained.
/// 4. A transient `decoded_bytes` accumulator (issue #127) estimates the BYTES
///    the materialized elements cost — `size_of::<TimeSeries>()`/`Label`/
///    `Sample`/`MetricMetadataProto` per element (charged at the series /
///    metadata boundary) plus `size_of::<Histogram>()`/`BucketSpan`/8-byte
///    bucket elements (charged incrementally per histogram, alongside guard 3's
///    count charges). The element-COUNT caps above bound how many elements
///    decode, not how much memory: minimal 2-wire-byte empty labels are ~48
///    heap bytes each, so the counts alone still admit hundreds of MiB of
///    structs from a small body. Once the estimate exceeds the shared
///    [`crate::protocols::otlp_prescan::MAX_DECODED_BYTES`] budget (256 MiB),
///    further series / histograms / metadata are drained without
///    materializing, and the deferred [`validate_bounds`] re-sum rejects the
///    whole request with the family-wide `"decoded bytes (estimated)"` field.
///
/// Kept separate from [`WriteRequest`] so the value type carries no
/// decode-scratch field and preserves derived round-trip equality — the
/// sanctioned alternative to a transient field + manual `PartialEq` on the
/// value type (the struct is constructed by literal across several crates).
#[derive(Default)]
struct BoundedWriteRequest {
    timeseries: Vec<TimeSeries>,
    metadata: Vec<MetricMetadataProto>,
    total_labels: usize,
    total_samples: usize,
    total_histograms: usize,
    total_hist_spans: usize,
    total_hist_buckets: usize,
    decoded_bytes: usize,
}

/// Estimated decoded bytes of ONE native histogram (issue #127): the
/// `Histogram` struct itself plus its decoded `BucketSpan`s and 8-byte bucket
/// elements — `size_of`-derived, no magic numbers, exactly what the decoder
/// materializes.
fn decoded_histogram_bytes(h: &Histogram) -> usize {
    std::mem::size_of::<Histogram>()
        .saturating_add(
            h.span_count()
                .saturating_mul(std::mem::size_of::<BucketSpan>()),
        )
        .saturating_add(
            h.bucket_element_count()
                .saturating_mul(std::mem::size_of::<u64>()),
        )
}

/// Estimated decoded bytes of one series' NON-histogram structs (issue #127):
/// the `TimeSeries` struct plus its labels and samples. Histograms are charged
/// separately — incrementally per histogram DURING the series' decode (see
/// [`BoundedWriteRequest::merge_one_time_series`]) — so the two never double
/// count.
fn decoded_series_shell_bytes(ts: &TimeSeries) -> usize {
    std::mem::size_of::<TimeSeries>()
        .saturating_add(ts.labels.len().saturating_mul(std::mem::size_of::<Label>()))
        .saturating_add(
            ts.samples
                .len()
                .saturating_mul(std::mem::size_of::<Sample>()),
        )
}

/// Re-sums the whole request's decoded-byte estimate from materialized data —
/// the SAME function of the materialized content as the incremental
/// `decoded_bytes` charges, so the deferred [`validate_bounds`] re-check and
/// the decode-time drain can never disagree (a drained request always re-sums
/// past the budget).
fn decoded_request_bytes(timeseries: &[TimeSeries], metadata: &[MetricMetadataProto]) -> usize {
    let mut total = metadata
        .len()
        .saturating_mul(std::mem::size_of::<MetricMetadataProto>());
    for ts in timeseries {
        total = total.saturating_add(decoded_series_shell_bytes(ts));
        for h in &ts.histograms {
            total = total.saturating_add(decoded_histogram_bytes(h));
        }
    }
    total
}

impl BoundedWriteRequest {
    /// Re-sums every cross-series aggregate from already-materialized series
    /// — the seeding [`WriteRequest::merge`]/`merge_length_delimited` need so
    /// a merge INTO an existing request charges the pre-existing fan-out too
    /// (no aggregate-cap bypass through repeated raw merges).
    fn seeded_from(timeseries: Vec<TimeSeries>, metadata: Vec<MetricMetadataProto>) -> Self {
        let mut seed = Self {
            timeseries,
            metadata,
            ..Self::default()
        };
        for ts in &seed.timeseries {
            seed.total_labels = seed.total_labels.saturating_add(ts.labels.len());
            seed.total_samples = seed.total_samples.saturating_add(ts.samples.len());
            seed.total_histograms = seed.total_histograms.saturating_add(ts.histograms.len());
            for h in &ts.histograms {
                seed.total_hist_spans = seed.total_hist_spans.saturating_add(h.span_count());
                seed.total_hist_buckets = seed
                    .total_hist_buckets
                    .saturating_add(h.bucket_element_count());
            }
        }
        // The decoded-byte estimate is re-summed with the same shared function
        // the deferred validate_bounds re-check uses (issue #127), so a merge
        // INTO an existing request charges the pre-existing materialization
        // too — no budget bypass through repeated raw merges.
        seed.decoded_bytes = decoded_request_bytes(&seed.timeseries, &seed.metadata);
        seed
    }

    /// Decodes ONE `TimeSeries` submessage (a `WriteRequest` tag-1 field
    /// occurrence) while charging the request-wide histogram aggregates
    /// **incrementally, per decoded histogram** — the fix for the codex
    /// review's high finding on issue #140: the per-series histogram caps'
    /// product (up to [`MAX_HISTOGRAMS_PER_SERIES`] histograms × ~8k spans /
    /// ~330k bucket elements each) exceeds the request-wide aggregates by
    /// orders of magnitude, so charging them only at the BETWEEN-series
    /// boundary would let one crafted series fully materialize ~82M spans
    /// (or blow the bucket aggregate from a ~5 MB body) before any check
    /// ran.
    ///
    /// Structurally this replicates `prost::encoding::message::merge` for
    /// the submessage (a [`prost::encoding::merge_loop`] over
    /// `decode_key` + `merge_field`), but interposes on tag 4: once the
    /// running `total_histograms`/`total_hist_spans`/`total_hist_buckets`
    /// (or the per-series histogram count) exceeds its cap, further
    /// histograms in THIS series are drained without materializing —
    /// bounding the fan-out to `≤ aggregate cap + one histogram's per-field
    /// caps` — and the deferred [`validate_bounds`] re-sum then rejects the
    /// whole request. All other tags delegate to `TimeSeries::merge_field`
    /// (which keeps the per-series label/sample/histogram-count drains).
    /// The scratch totals commit back to `self` only on `Ok`; on a decode
    /// error the whole request fails anyway.
    fn merge_one_time_series(
        &mut self,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        prost::encoding::check_wire_type(prost::encoding::WireType::LengthDelimited, wire_type)?;
        // (TimeSeries under construction, total_histograms, total_hist_spans,
        // total_hist_buckets, decoded_bytes) — one tuple so `merge_loop` can
        // thread every running total through its single `&mut T`.
        let mut scratch = (
            TimeSeries::default(),
            self.total_histograms,
            self.total_hist_spans,
            self.total_hist_buckets,
            self.decoded_bytes,
        );
        prost::encoding::merge_loop(
            &mut scratch,
            buf,
            ctx,
            |(ts, total_histograms, total_hist_spans, total_hist_buckets, decoded_bytes),
             buf,
             ctx| {
                let (tag, wire_type) = prost::encoding::decode_key(buf)?;
                if tag == 4u32 {
                    if ts.histograms.len() > MAX_HISTOGRAMS_PER_SERIES
                        || *total_histograms > MAX_TOTAL_HISTOGRAMS_PER_REQUEST
                        || *total_hist_spans > MAX_TOTAL_HIST_SPANS_PER_REQUEST
                        || *total_hist_buckets > MAX_TOTAL_HIST_BUCKETS_PER_REQUEST
                        || *decoded_bytes > MAX_DECODED_BYTES
                    {
                        // Cap reached (per-series count OR a request-wide
                        // aggregate): drain this histogram WITHOUT
                        // materializing it, wire-type-checked exactly like
                        // every other drain arm. The vec is allowed to reach
                        // the `+ 1` over-cap state so `validate_bounds` still
                        // rejects the request.
                        prost::encoding::check_wire_type(
                            prost::encoding::WireType::LengthDelimited,
                            wire_type,
                        )?;
                        prost::encoding::skip_field(wire_type, tag, buf, ctx)
                    } else {
                        prost::encoding::message::merge_repeated(
                            wire_type,
                            &mut ts.histograms,
                            buf,
                            ctx,
                        )?;
                        // Charge the just-merged histogram immediately: its
                        // own fields are already capped per side/field by
                        // `Histogram::merge_field`, so one over-aggregate
                        // step grows the fan-out by at most one histogram's
                        // per-field caps. The byte estimate (issue #127) is
                        // charged at the same site with the same bounded
                        // over-step.
                        if let Some(h) = ts.histograms.last() {
                            *total_histograms = total_histograms.saturating_add(1);
                            *total_hist_spans = total_hist_spans.saturating_add(h.span_count());
                            *total_hist_buckets =
                                total_hist_buckets.saturating_add(h.bucket_element_count());
                            *decoded_bytes =
                                decoded_bytes.saturating_add(decoded_histogram_bytes(h));
                        }
                        Ok(())
                    }
                } else {
                    ts.merge_field(tag, wire_type, buf, ctx)
                }
            },
        )?;
        let (ts, total_histograms, total_hist_spans, total_hist_buckets, decoded_bytes) = scratch;
        self.total_histograms = total_histograms;
        self.total_hist_spans = total_hist_spans;
        self.total_hist_buckets = total_hist_buckets;
        self.decoded_bytes = decoded_bytes;
        self.timeseries.push(ts);
        Ok(())
    }
}

impl prost::Message for BoundedWriteRequest {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        // Decode-only helper, but a complete impl is required by the trait; the
        // transient counters are never encoded, so this is byte-identical to
        // `WriteRequest`'s wire form.
        prost::encoding::message::encode_repeated(1u32, &self.timeseries, buf);
        prost::encoding::message::encode_repeated(3u32, &self.metadata, buf);
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
                if self.timeseries.len() > MAX_TIMESERIES_PER_REQUEST
                    || self.total_labels > MAX_TOTAL_LABELS_PER_REQUEST
                    || self.total_samples > MAX_TOTAL_SAMPLES_PER_REQUEST
                    || self.total_histograms > MAX_TOTAL_HISTOGRAMS_PER_REQUEST
                    || self.total_hist_spans > MAX_TOTAL_HIST_SPANS_PER_REQUEST
                    || self.total_hist_buckets > MAX_TOTAL_HIST_BUCKETS_PER_REQUEST
                    || self.decoded_bytes > MAX_DECODED_BYTES
                {
                    // Cap reached (series count OR aggregate labels/samples):
                    // drain the excess series WITHOUT materializing it, while
                    // still enforcing the wire-type contract `merge_repeated`
                    // would. The vec is allowed to reach `MAX + 1` (not capped
                    // at `MAX`) so the deferred `validate_bounds` still rejects
                    // an over-limit request.
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    // Decode this ONE series through the interposing
                    // [`Self::merge_one_time_series`], which charges the
                    // request-wide histogram aggregates (`total_histograms`/
                    // `total_hist_spans`/`total_hist_buckets`) INCREMENTALLY
                    // per histogram DURING the series' own decode — a single
                    // crafted series of many individually-legal histograms
                    // (10k × ~8k spans / ~330k bucket elements each) must not
                    // fully materialize before a between-series boundary
                    // check runs (codex review of issue #140, high finding).
                    self.merge_one_time_series(wire_type, buf, ctx)?;
                    // Charge the just-merged series' labels/samples into the
                    // aggregates (histogram totals were already charged
                    // incrementally above). Its own vecs are capped at
                    // `MAX_*_PER_SERIES + 1` by the per-series drains, and a
                    // single series' label/sample fan-out (256 / 100k) sits
                    // far below the 5M aggregates, so charging THOSE at the
                    // series boundary bounds the over-aggregate step to one
                    // series' per-series cap. The byte estimate (issue #127)
                    // charges the series shell (struct + labels + samples) at
                    // the same boundary with the same bounded over-step (~1.6
                    // MiB worst case, far under the 256 MiB budget).
                    if let Some(last) = self.timeseries.last() {
                        self.total_labels = self.total_labels.saturating_add(last.labels.len());
                        self.total_samples = self.total_samples.saturating_add(last.samples.len());
                        self.decoded_bytes = self
                            .decoded_bytes
                            .saturating_add(decoded_series_shell_bytes(last));
                    }
                    Ok(())
                }
            }
            3u32 => {
                if self.metadata.len() > MAX_METADATA_PER_REQUEST
                    || self.decoded_bytes > MAX_DECODED_BYTES
                {
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    prost::encoding::message::merge_repeated(
                        wire_type,
                        &mut self.metadata,
                        buf,
                        ctx,
                    )?;
                    // One `merge_repeated` call materializes exactly one
                    // metadata record — charge it (issue #127).
                    self.decoded_bytes = self
                        .decoded_bytes
                        .saturating_add(std::mem::size_of::<MetricMetadataProto>());
                    Ok(())
                }
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        prost::encoding::message::encoded_len_repeated(1u32, &self.timeseries)
            + prost::encoding::message::encoded_len_repeated(3u32, &self.metadata)
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

/// `prompb.TimeSeries`: `labels` at tag 1, `samples` at tag 2, `histograms`
/// at tag 4 (issue #140; `exemplars` at tag 3 stays intentionally
/// undeclared/skipped — out of scope).
///
/// Like [`WriteRequest`] it does **not** derive `::prost::Message`; a
/// hand-written impl (below) caps the repeated `labels` field at
/// [`MAX_LABELS_PER_SERIES`]` + 1`, `samples` at [`MAX_SAMPLES_PER_SERIES`]`
/// + 1`, and `histograms` at [`MAX_HISTOGRAMS_PER_SERIES`]` + 1` **inside the
/// decoder** (issue #115), draining excess records without allocating — so a
/// single series carrying millions of minimal labels/samples/histograms
/// cannot unpack past the cap. The caps therefore hold whether a series decodes
/// via [`BoundedWriteRequest`] (the ingest path) or via a direct
/// `TimeSeries::decode`/`merge` (all route through this `merge_field`).
#[derive(Clone, PartialEq, Default, Debug)]
pub struct TimeSeries {
    pub labels: Vec<Label>,
    pub samples: Vec<Sample>,
    pub histograms: Vec<Histogram>,
}

impl prost::Message for TimeSeries {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        prost::encoding::message::encode_repeated(1u32, &self.labels, buf);
        prost::encoding::message::encode_repeated(2u32, &self.samples, buf);
        prost::encoding::message::encode_repeated(4u32, &self.histograms, buf);
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
                if self.labels.len() > MAX_LABELS_PER_SERIES {
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    prost::encoding::message::merge_repeated(wire_type, &mut self.labels, buf, ctx)
                }
            }
            2u32 => {
                if self.samples.len() > MAX_SAMPLES_PER_SERIES {
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    prost::encoding::message::merge_repeated(wire_type, &mut self.samples, buf, ctx)
                }
            }
            4u32 => {
                if self.histograms.len() > MAX_HISTOGRAMS_PER_SERIES {
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    prost::encoding::message::merge_repeated(
                        wire_type,
                        &mut self.histograms,
                        buf,
                        ctx,
                    )
                }
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        prost::encoding::message::encoded_len_repeated(1u32, &self.labels)
            + prost::encoding::message::encoded_len_repeated(2u32, &self.samples)
            + prost::encoding::message::encoded_len_repeated(4u32, &self.histograms)
    }

    fn clear(&mut self) {
        self.labels.clear();
        self.samples.clear();
        self.histograms.clear();
    }
}

/// `prompb.Label`: `name` at tag 1, `value` at tag 2.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// `prompb.Sample`: `value` (a `double`) at tag 1, `timestamp` (milliseconds
/// since the Unix epoch) at tag 2.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

/// One alternative of the `prompb.Histogram` `count`/`zero_count` protobuf
/// `oneof` pairs (tags 1|2 and 6|7 — issue #140 plan v2 finding 2). Modeled
/// as a real oneof, NOT two independent optionals: a valid wire message may
/// repeat oneof alternatives with the **last occurrence winning**, so
/// retaining a stale float alternative after a later integer one would
/// misclassify an integer histogram as float-flavor. `merge_field` overwrites
/// the whole `Option` on every occurrence of either tag, and flavor
/// classification keys off the actually-decoded case.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum HistogramCount {
    /// The integer alternative (`count_int` tag 1 / `zero_count_int` tag 6).
    Int(u64),
    /// The float alternative (`count_float` tag 2 / `zero_count_float` tag
    /// 7) — presence ⇒ float-flavor histogram, structurally unstorable in the
    /// integer-delta `metric_hist_samples` columns and rejected per-point.
    Float(f64),
}

/// `prompb.BucketSpan`: `offset` at tag 1 (**sint32**, zigzag — a plain
/// int32 declaration would decode self-consistent garbage for negative
/// offsets), `length` at tag 2 (uint32). A leaf message with no repeated
/// field, so the derive is safe (the span-count caps live on the containing
/// [`Histogram`]'s hand-written `merge_field`).
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct BucketSpan {
    #[prost(sint32, tag = "1")]
    pub offset: i32,
    #[prost(uint32, tag = "2")]
    pub length: u32,
}

/// `prompb.Histogram` (RW-1.0 native histogram, `TimeSeries` tag 4 — issue
/// #140). Tag layout pinned against the published Prometheus remote-write
/// 1.0 protobuf schema (`prompb/types.proto`) and cross-checked by the real
/// capture `tests/fixtures/remote-write/native_histogram.bin`:
///
/// | tag | field             | wire type                    |
/// |-----|-------------------|------------------------------|
/// | 1   | `count_int`       | uint64 (oneof `count`)       |
/// | 2   | `count_float`     | double (oneof `count`)       |
/// | 3   | `sum`             | double                       |
/// | 4   | `schema`          | **sint32** (zigzag)          |
/// | 5   | `zero_threshold`  | double                       |
/// | 6   | `zero_count_int`  | uint64 (oneof `zero_count`)  |
/// | 7   | `zero_count_float`| double (oneof `zero_count`)  |
/// | 8   | `negative_spans`  | repeated [`BucketSpan`]      |
/// | 9   | `negative_deltas` | **sint64** packed (zigzag)   |
/// | 10  | `negative_counts` | double packed                |
/// | 11  | `positive_spans`  | repeated [`BucketSpan`]      |
/// | 12  | `positive_deltas` | **sint64** packed (zigzag)   |
/// | 13  | `positive_counts` | double packed                |
/// | 14  | `reset_hint`      | enum (int32 varint)          |
/// | 15  | `timestamp`       | int64 (milliseconds)         |
/// | 16  | `custom_values`   | double packed                |
///
/// Hand-written [`prost::Message`] impl (no derive) for two reasons:
/// 1. the `count`/`zero_count` [`HistogramCount`] oneofs (last occurrence
///    wins across both alternatives' tags), and
/// 2. decode-time DoS caps (issue #115 pattern): spans are capped at
///    [`MAX_SPANS_PER_HISTOGRAM_SIDE`]` + 1` per side and each delta/count/
///    `custom_values` array at [`MAX_BUCKETS_PER_HISTOGRAM_SIDE`]` + 1` per
///    field. Packed runs are decoded **element-by-element** so the cap holds
///    mid-run (one packed field can carry millions of ~1-wire-byte varints
///    that decode to 8 heap bytes each — the fan-out the byte cap alone does
///    not bound): elements materialize while under the cap, the rest of the
///    run is decoded-and-discarded, and the deferred [`validate_bounds`]
///    re-check turns the `+ 1` sentinel into a whole-request reject.
///
/// `encode_raw` emits proto3 default-skipping for plain fields (nonzero
/// only) and by-case for the oneofs (a set oneof alternative is always
/// emitted, even at its default value), so tests can construct both flavors.
#[derive(Clone, PartialEq, Default, Debug)]
pub struct Histogram {
    /// `count` oneof (tags 1|2), last occurrence wins; `None` = proto3
    /// absent ⇒ 0 observations.
    pub count: Option<HistogramCount>,
    /// Sum of observations; also the stale-marker carrier (may be NaN —
    /// never gate on it).
    pub sum: f64,
    /// Bucket schema (sint32 on the wire).
    pub schema: i32,
    /// Width of the zero bucket.
    pub zero_threshold: f64,
    /// `zero_count` oneof (tags 6|7), last occurrence wins.
    pub zero_count: Option<HistogramCount>,
    /// Spans for negative buckets.
    pub negative_spans: Vec<BucketSpan>,
    /// Delta-encoded negative bucket counts (integer flavor).
    pub negative_deltas: Vec<i64>,
    /// Absolute negative bucket counts (float flavor — non-empty ⇒ reject).
    pub negative_counts: Vec<f64>,
    /// Spans for positive buckets.
    pub positive_spans: Vec<BucketSpan>,
    /// Delta-encoded positive bucket counts (integer flavor).
    pub positive_deltas: Vec<i64>,
    /// Absolute positive bucket counts (float flavor — non-empty ⇒ reject).
    pub positive_counts: Vec<f64>,
    /// `Histogram.ResetHint` enum: UNKNOWN=0 / YES=1 / NO=2 / GAUGE=3.
    pub reset_hint: i32,
    /// Milliseconds since the Unix epoch — independent of any
    /// `Sample.timestamp` on the same series.
    pub timestamp: i64,
    /// NHCB custom bounds (schema −53).
    pub custom_values: Vec<f64>,
}

impl Histogram {
    /// Total decoded spans across both sides — the unit the request-wide
    /// [`MAX_TOTAL_HIST_SPANS_PER_REQUEST`] aggregate counts (plan v2
    /// finding 1).
    fn span_count(&self) -> usize {
        self.negative_spans.len() + self.positive_spans.len()
    }

    /// Total decoded bucket-array elements (deltas + float counts +
    /// `custom_values`) — the unit [`MAX_TOTAL_HIST_BUCKETS_PER_REQUEST`]
    /// counts. All five arrays hold 8-byte elements, so this is the whole
    /// packed fan-out in one number.
    fn bucket_element_count(&self) -> usize {
        self.negative_deltas.len()
            + self.negative_counts.len()
            + self.positive_deltas.len()
            + self.positive_counts.len()
            + self.custom_values.len()
    }
}

/// Merges one wire occurrence of a packed-capable **sint64** field into
/// `values`, capped at `cap + 1` materialized elements. A length-delimited
/// occurrence is a packed run decoded element-by-element (materialize while
/// under the cap, decode-and-discard the rest of the run — the cap holds
/// MID-run); any other wire type is a single unpacked varint. Mirrors
/// `prost::encoding::sint64::merge_repeated` plus the cap.
fn merge_capped_sint64(
    values: &mut Vec<i64>,
    cap: usize,
    wire_type: prost::encoding::WireType,
    buf: &mut impl bytes::Buf,
    ctx: prost::encoding::DecodeContext,
) -> Result<(), prost::DecodeError> {
    if wire_type == prost::encoding::WireType::LengthDelimited {
        prost::encoding::merge_loop(values, buf, ctx, |values, buf, ctx| {
            let mut value = 0i64;
            prost::encoding::sint64::merge(
                prost::encoding::WireType::Varint,
                &mut value,
                buf,
                ctx,
            )?;
            if values.len() <= cap {
                values.push(value);
            }
            Ok(())
        })
    } else {
        let mut value = 0i64;
        prost::encoding::sint64::merge(wire_type, &mut value, buf, ctx)?;
        if values.len() <= cap {
            values.push(value);
        }
        Ok(())
    }
}

/// [`merge_capped_sint64`]'s **double** analog (packed fixed64 runs or a
/// single unpacked value), same `cap + 1` mid-run materialization bound.
fn merge_capped_double(
    values: &mut Vec<f64>,
    cap: usize,
    wire_type: prost::encoding::WireType,
    buf: &mut impl bytes::Buf,
    ctx: prost::encoding::DecodeContext,
) -> Result<(), prost::DecodeError> {
    if wire_type == prost::encoding::WireType::LengthDelimited {
        prost::encoding::merge_loop(values, buf, ctx, |values, buf, ctx| {
            let mut value = 0f64;
            prost::encoding::double::merge(
                prost::encoding::WireType::SixtyFourBit,
                &mut value,
                buf,
                ctx,
            )?;
            if values.len() <= cap {
                values.push(value);
            }
            Ok(())
        })
    } else {
        let mut value = 0f64;
        prost::encoding::double::merge(wire_type, &mut value, buf, ctx)?;
        if values.len() <= cap {
            values.push(value);
        }
        Ok(())
    }
}

impl prost::Message for Histogram {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        match self.count {
            Some(HistogramCount::Int(v)) => prost::encoding::uint64::encode(1u32, &v, buf),
            Some(HistogramCount::Float(v)) => prost::encoding::double::encode(2u32, &v, buf),
            None => {}
        }
        if self.sum != 0.0 {
            prost::encoding::double::encode(3u32, &self.sum, buf);
        }
        if self.schema != 0 {
            prost::encoding::sint32::encode(4u32, &self.schema, buf);
        }
        if self.zero_threshold != 0.0 {
            prost::encoding::double::encode(5u32, &self.zero_threshold, buf);
        }
        match self.zero_count {
            Some(HistogramCount::Int(v)) => prost::encoding::uint64::encode(6u32, &v, buf),
            Some(HistogramCount::Float(v)) => prost::encoding::double::encode(7u32, &v, buf),
            None => {}
        }
        prost::encoding::message::encode_repeated(8u32, &self.negative_spans, buf);
        prost::encoding::sint64::encode_packed(9u32, &self.negative_deltas, buf);
        prost::encoding::double::encode_packed(10u32, &self.negative_counts, buf);
        prost::encoding::message::encode_repeated(11u32, &self.positive_spans, buf);
        prost::encoding::sint64::encode_packed(12u32, &self.positive_deltas, buf);
        prost::encoding::double::encode_packed(13u32, &self.positive_counts, buf);
        if self.reset_hint != 0 {
            prost::encoding::int32::encode(14u32, &self.reset_hint, buf);
        }
        if self.timestamp != 0 {
            prost::encoding::int64::encode(15u32, &self.timestamp, buf);
        }
        prost::encoding::double::encode_packed(16u32, &self.custom_values, buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        match tag {
            // oneof `count` (tags 1|2): every occurrence of either tag
            // overwrites the whole Option — last occurrence wins.
            1u32 => {
                let mut v = 0u64;
                prost::encoding::uint64::merge(wire_type, &mut v, buf, ctx)?;
                self.count = Some(HistogramCount::Int(v));
                Ok(())
            }
            2u32 => {
                let mut v = 0f64;
                prost::encoding::double::merge(wire_type, &mut v, buf, ctx)?;
                self.count = Some(HistogramCount::Float(v));
                Ok(())
            }
            3u32 => prost::encoding::double::merge(wire_type, &mut self.sum, buf, ctx),
            4u32 => prost::encoding::sint32::merge(wire_type, &mut self.schema, buf, ctx),
            5u32 => prost::encoding::double::merge(wire_type, &mut self.zero_threshold, buf, ctx),
            // oneof `zero_count` (tags 6|7), same last-wins rule.
            6u32 => {
                let mut v = 0u64;
                prost::encoding::uint64::merge(wire_type, &mut v, buf, ctx)?;
                self.zero_count = Some(HistogramCount::Int(v));
                Ok(())
            }
            7u32 => {
                let mut v = 0f64;
                prost::encoding::double::merge(wire_type, &mut v, buf, ctx)?;
                self.zero_count = Some(HistogramCount::Float(v));
                Ok(())
            }
            8u32 => {
                if self.negative_spans.len() > MAX_SPANS_PER_HISTOGRAM_SIDE {
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    prost::encoding::message::merge_repeated(
                        wire_type,
                        &mut self.negative_spans,
                        buf,
                        ctx,
                    )
                }
            }
            9u32 => merge_capped_sint64(
                &mut self.negative_deltas,
                MAX_BUCKETS_PER_HISTOGRAM_SIDE,
                wire_type,
                buf,
                ctx,
            ),
            10u32 => merge_capped_double(
                &mut self.negative_counts,
                MAX_BUCKETS_PER_HISTOGRAM_SIDE,
                wire_type,
                buf,
                ctx,
            ),
            11u32 => {
                if self.positive_spans.len() > MAX_SPANS_PER_HISTOGRAM_SIDE {
                    prost::encoding::check_wire_type(
                        prost::encoding::WireType::LengthDelimited,
                        wire_type,
                    )?;
                    prost::encoding::skip_field(wire_type, tag, buf, ctx)
                } else {
                    prost::encoding::message::merge_repeated(
                        wire_type,
                        &mut self.positive_spans,
                        buf,
                        ctx,
                    )
                }
            }
            12u32 => merge_capped_sint64(
                &mut self.positive_deltas,
                MAX_BUCKETS_PER_HISTOGRAM_SIDE,
                wire_type,
                buf,
                ctx,
            ),
            13u32 => merge_capped_double(
                &mut self.positive_counts,
                MAX_BUCKETS_PER_HISTOGRAM_SIDE,
                wire_type,
                buf,
                ctx,
            ),
            14u32 => prost::encoding::int32::merge(wire_type, &mut self.reset_hint, buf, ctx),
            15u32 => prost::encoding::int64::merge(wire_type, &mut self.timestamp, buf, ctx),
            16u32 => merge_capped_double(
                &mut self.custom_values,
                MAX_BUCKETS_PER_HISTOGRAM_SIDE,
                wire_type,
                buf,
                ctx,
            ),
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0usize;
        match self.count {
            Some(HistogramCount::Int(v)) => len += prost::encoding::uint64::encoded_len(1u32, &v),
            Some(HistogramCount::Float(v)) => len += prost::encoding::double::encoded_len(2u32, &v),
            None => {}
        }
        if self.sum != 0.0 {
            len += prost::encoding::double::encoded_len(3u32, &self.sum);
        }
        if self.schema != 0 {
            len += prost::encoding::sint32::encoded_len(4u32, &self.schema);
        }
        if self.zero_threshold != 0.0 {
            len += prost::encoding::double::encoded_len(5u32, &self.zero_threshold);
        }
        match self.zero_count {
            Some(HistogramCount::Int(v)) => len += prost::encoding::uint64::encoded_len(6u32, &v),
            Some(HistogramCount::Float(v)) => len += prost::encoding::double::encoded_len(7u32, &v),
            None => {}
        }
        len += prost::encoding::message::encoded_len_repeated(8u32, &self.negative_spans);
        len += prost::encoding::sint64::encoded_len_packed(9u32, &self.negative_deltas);
        len += prost::encoding::double::encoded_len_packed(10u32, &self.negative_counts);
        len += prost::encoding::message::encoded_len_repeated(11u32, &self.positive_spans);
        len += prost::encoding::sint64::encoded_len_packed(12u32, &self.positive_deltas);
        len += prost::encoding::double::encoded_len_packed(13u32, &self.positive_counts);
        if self.reset_hint != 0 {
            len += prost::encoding::int32::encoded_len(14u32, &self.reset_hint);
        }
        if self.timestamp != 0 {
            len += prost::encoding::int64::encoded_len(15u32, &self.timestamp);
        }
        len += prost::encoding::double::encoded_len_packed(16u32, &self.custom_values);
        len
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

/// `prompb.MetricMetadata`: `type` at tag 1, `metric_family_name` at tag 2,
/// `help` at tag 4, `unit` at tag 5 (tag 3 is a gap in the upstream schema —
/// no field was ever assigned it). Named `MetricMetadataProto` (not
/// `MetricMetadata`) to avoid colliding with `crate::ingest::metrics::
/// MetricMetadata`, the seam type [`parse`] produces from this wire type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetricMetadataProto {
    #[prost(int32, tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub metric_family_name: String,
    #[prost(string, tag = "4")]
    pub help: String,
    #[prost(string, tag = "5")]
    pub unit: String,
}

/// Decode-time structural DoS guards (issue #28 code review hardening
/// finding, extended to enforce **during** decode in issue #115 finding #62):
/// generous, documented per-request bounds on repeated-field counts, sized so
/// no legitimate remote-write batch ever approaches them. A raw body is
/// already capped at 64 MiB decompressed
/// (`crate::ingest::decompress::MAX_DECOMPRESSED_BYTES`), but that byte cap
/// alone does not bound the *decoded* structure's size: many minimal-length
/// repeated submessages (e.g. a `TimeSeries` with no labels/samples costs
/// only a couple of wire bytes but ~50+ heap-adjacent bytes once decoded
/// into a `Vec<TimeSeries>` entry) let a 64 MiB body unpack into a far
/// larger in-memory structure. Enforced **during** decode by the hand-written
/// [`WriteRequest`]/[`BoundedWriteRequest`]/[`TimeSeries`] decoders (drain past
/// `MAX + 1` without materializing), then re-checked by [`validate_bounds`] in
/// [`decode`] — before [`parse`] performs any further per-element allocation
/// (label-set construction, fingerprinting, output row materialization).
/// These are element-COUNT caps; the residual decoded-MEMORY amplification
/// they leave (many minimal elements, each cheap on the wire but tens-to-
/// hundreds of heap bytes decoded) is bounded by the complementary decode-time
/// byte budget [`crate::protocols::otlp_prescan::MAX_DECODED_BYTES`], charged
/// per materialized element in the same drains (issue #127).
pub const MAX_TIMESERIES_PER_REQUEST: usize = 1_000_000;
/// See [`MAX_TIMESERIES_PER_REQUEST`]'s doc comment.
pub const MAX_LABELS_PER_SERIES: usize = 256;
/// See [`MAX_TIMESERIES_PER_REQUEST`]'s doc comment.
pub const MAX_SAMPLES_PER_SERIES: usize = 100_000;
/// See [`MAX_TIMESERIES_PER_REQUEST`]'s doc comment.
pub const MAX_METADATA_PER_REQUEST: usize = 10_000;
/// Per-series cap on decoded native histograms (`TimeSeries` tag 4, issue
/// #140) — same doctrine as [`MAX_SAMPLES_PER_SERIES`]: generous (Prometheus'
/// `max_samples_per_send` default is 2,000 for the whole request), enforced
/// during decode by [`TimeSeries::merge_field`]'s drain.
pub const MAX_HISTOGRAMS_PER_SERIES: usize = 10_000;
/// Cross-series **aggregate** cap on total decoded histograms (issue #140),
/// analogous to [`MAX_TOTAL_SAMPLES_PER_REQUEST`]: N series each under
/// [`MAX_HISTOGRAMS_PER_SERIES`] cannot sum past this ceiling during decode
/// (an empty wire `Histogram` is 2 bytes but a decoded entry is hundreds of
/// heap bytes). Enforced by [`BoundedWriteRequest`]'s drain + the deferred
/// [`validate_bounds`] re-sum.
pub const MAX_TOTAL_HISTOGRAMS_PER_REQUEST: usize = 500_000;
/// Per-side cap on one histogram's decoded `BucketSpan`s (issue #140) — a
/// span is 2 minimal wire bytes but 8 decoded bytes plus vec bookkeeping;
/// 4,096 spans per side is orders of magnitude beyond any real exporter's
/// bucket layout.
pub const MAX_SPANS_PER_HISTOGRAM_SIDE: usize = 4_096;
/// Per-field cap on one histogram's decoded bucket-array elements (each of
/// `negative_deltas`/`negative_counts`/`positive_deltas`/`positive_counts`/
/// `custom_values` independently, issue #140). Packed varint deltas are the
/// second-amplification: ~1 wire byte → 8 heap bytes, so the cap must hold
/// MID-run ([`merge_capped_sint64`]/[`merge_capped_double`] decode
/// element-by-element and discard past the cap).
pub const MAX_BUCKETS_PER_HISTOGRAM_SIDE: usize = 65_536;
/// Cross-histogram aggregate span cap (issue #140 plan v2 finding 1): many
/// individually-legal histograms (each under
/// [`MAX_SPANS_PER_HISTOGRAM_SIDE`]) must not sum to millions of decoded
/// `BucketSpan` structs (a minimal empty `BucketSpan` is 2 wire bytes).
/// ≈40 MiB of span structs worst case — same ceiling class as the label
/// aggregate. Element-COUNT cap; complements (does not duplicate) the
/// decode-time byte budget.
pub const MAX_TOTAL_HIST_SPANS_PER_REQUEST: usize = 5_000_000;
/// Cross-histogram aggregate bucket-element cap (issue #140): bounds the sum
/// of every histogram's delta/count/`custom_values` element counts across the
/// whole request (≈40 MiB of i64/f64 worst case) — the packed-run fan-out a
/// 64 MiB body could otherwise decode into ~67M i64s ≈ 536 MiB. Enforced by
/// [`BoundedWriteRequest`]'s drain + the deferred [`validate_bounds`] re-sum.
pub const MAX_TOTAL_HIST_BUCKETS_PER_REQUEST: usize = 5_000_000;

/// Cross-series **aggregate** cap on total decoded labels (issue #115, finding
/// #62). The per-dimension caps bound each series in isolation
/// ([`MAX_LABELS_PER_SERIES`]) and the series count
/// ([`MAX_TIMESERIES_PER_REQUEST`]), but their *product* (1M × 256 = 256M
/// `Label` structs) is a decode-time fan-out a 64 MiB body of minimal-length
/// empty labels can reach — each empty label is ~2 wire bytes but ~48 heap
/// bytes once decoded, and the parse-time expansion budget
/// ([`MAX_EXPANDED_BYTES`]) charges only label *bytes* (zero for empty labels),
/// so it does not catch this count-based fan-out. This aggregate bounds the
/// total decoded labels across all series to a generous ceiling (≈ 240 MiB of
/// `Label` structs worst case) — orders of magnitude above any legitimate
/// remote-write batch. Enforced **during** decode by [`BoundedWriteRequest`]
/// (drain past the cap) and re-checked by the deferred [`validate_bounds`].
pub const MAX_TOTAL_LABELS_PER_REQUEST: usize = 5_000_000;
/// Cross-series **aggregate** cap on total decoded samples (issue #115, finding
/// #62), analogous to [`MAX_TOTAL_LABELS_PER_REQUEST`]: bounds the sum of every
/// series' `samples.len()` so N series each just under
/// [`MAX_SAMPLES_PER_SERIES`] cannot sum past this ceiling during decode. Sized
/// like the Loki push analog's cross-stream aggregate; sits above the ≈ 4.2M
/// samples the parse-time [`MAX_EXPANDED_BYTES`] byte budget admits, so that
/// tighter output-expansion budget remains the effective secondary bound.
pub const MAX_TOTAL_SAMPLES_PER_REQUEST: usize = 5_000_000;

/// The per-request cap on [`parse`]'s **estimated expanded output bytes**
/// (issue #62). Own constant, same value and derivation as
/// `otlp_metrics::MAX_EXPANDED_BYTES` / `otlp_traces::MAX_EXPANDED_BYTES`
/// (4× the 64 MiB decompressed body cap = 256 MiB). The
/// [`MAX_TIMESERIES_PER_REQUEST`]-family caps bound each *dimension*
/// (series × labels × samples-per-series) but NOT aggregate output: a
/// minimal wire `Sample` is 2 bytes (empty body — `value`/`timestamp` are
/// proto3 defaults) yet decodes to one ~40-byte `MetricPoint`, so a 64 MiB
/// body of ~33.5M such samples packs into ≈ 336 series (each ≤ 100k) —
/// far under the 1M-timeseries cap — while materializing ≈ 1.25 GiB of
/// output. This byte budget bounds the total: it admits ≤
/// `MAX_EXPANDED_BYTES / SAMPLE_ROW_OVERHEAD` ≈ 4.2M samples (≈ 256 MiB),
/// far above Prometheus's `max_samples_per_send` default of 2,000 — an
/// order-of-magnitude DoS guard, not a tight quota.
pub const MAX_EXPANDED_BYTES: usize = 4 * crate::ingest::decompress::MAX_DECOMPRESSED_BYTES;

/// Estimated fixed heap cost of one emitted [`MetricPoint`]: `metric_name`
/// `Arc<str>` (shared per series, not per sample) + fingerprint +
/// `unix_milli` + `value` ≈ 40 bytes, floored to a round constant. The
/// dominant multiplicative term (one per wire sample).
const SAMPLE_ROW_OVERHEAD: usize = 64;
/// Estimated fixed heap cost of one [`SeriesRef`] beyond its label bytes.
const SERIES_ROW_OVERHEAD: usize = 64;
/// Fixed per-label heap floor charged for every materialized `(name, value)`
/// label pair (issue #115, finding #62). A wire label can be ~2 bytes (both
/// strings empty) yet, once `parse_time_series` clones it into `rest` and
/// `LabelSet::from_normalized` builds its sorted map, it costs two `String`
/// headers (48 B) plus the normalized-map node/container overhead — a fixed
/// heap cost the raw name+value byte charge undercounts to near zero. Without
/// this floor an attacker fans ≤ [`MAX_TOTAL_LABELS_PER_REQUEST`] near-empty
/// labels across many series (each under [`MAX_LABELS_PER_SERIES`]), staying
/// far below [`MAX_EXPANDED_BYTES`] while forcing millions of real
/// `(String, String)`/map allocations. Charging ≥128 B per label makes such a
/// fan-out trip the byte budget at ~`MAX_EXPANDED_BYTES / 128` labels — before
/// materialization — while legitimate few-labels-per-series batches stay well
/// under budget. Mirrors `otlp_traces`/`otlp_metrics`'s `ATTR_ROW_OVERHEAD`.
const LABEL_ROW_OVERHEAD: usize = 128;
/// Estimated fixed heap cost of one [`MetricMetadata`] beyond its
/// name/help/unit bytes.
const META_ROW_OVERHEAD: usize = 64;
/// Estimated fixed heap cost of one emitted [`HistogramPoint`] beyond its
/// per-element arrays: identity triplet + the `NativeHistogram` scalar
/// fields + five vec headers, floored to a round constant (issue #140,
/// mirroring `otlp_metrics`' native-path charge shape).
const HIST_ROW_OVERHEAD: usize = 256;
/// Per-element heap floor for a histogram's variable-length arrays: a
/// decoded span (`i32`+`u32`), bucket delta (`i64`), or custom value
/// (`f64`) each cost 8 bytes once materialized into the
/// [`NativeHistogram`]. Charged BEFORE the copy (issue #62).
const HIST_ELEMENT_BYTES: usize = 8;
/// Estimated per-entry heap cost of the transient histogram-wins dedup key
/// set [`dedup_histogram_wins`] builds — one `(&str, Fingerprint, i64)` slot
/// per native-histogram sample plus `HashSet` bookkeeping. Same constant and
/// rationale as `otlp_metrics::HIST_DEDUP_KEY_BYTES` (the dedup itself is a
/// private mirror, not a shared helper — issue #140 plan).
const HIST_DEDUP_KEY_BYTES: usize = 48;

/// Adds `amount` to the running expansion estimate and fails the whole
/// request the moment it exceeds [`MAX_EXPANDED_BYTES`] (issue #62) — the
/// single charge/check point every materialization site reserves through
/// before allocating. Identical body to `otlp_metrics::charge_budget`
/// (remote-write labels are already `String`s, charged 1× — no `AnyValue`
/// expansion factors).
fn charge_budget(expanded_bytes: &mut usize, amount: usize) -> Result<(), LogsIngestError> {
    *expanded_bytes = expanded_bytes.saturating_add(amount);
    if *expanded_bytes > MAX_EXPANDED_BYTES {
        return Err(LogsIngestError::OversizeMessage {
            field: "expanded metric row bytes (estimated)",
            limit: MAX_EXPANDED_BYTES,
            actual: *expanded_bytes,
        });
    }
    Ok(())
}

/// Decodes a (decompressed) `POST /api/v1/write` request body under the
/// [`MAX_TIMESERIES_PER_REQUEST`]-family structural bounds. `WriteRequest::
/// decode` routes through the [`BoundedWriteRequest`] twin, so the repeated
/// fields and the cross-series aggregate are capped **during** decode (no
/// over-cap materialization); [`validate_bounds`] then turns the drained
/// `+ 1` over-cap into the whole-request error. The sole decode boundary: a
/// malformed/truncated protobuf, or a message exceeding one of those bounds,
/// is a whole-request, atomic failure (mirrors `otlp_metrics::decode`) — never
/// partially applied.
pub fn decode(body: &[u8]) -> Result<WriteRequest, LogsIngestError> {
    let req = WriteRequest::decode(body)?;
    validate_bounds(&req)?;
    Ok(req)
}

/// Enforces the [`MAX_TIMESERIES_PER_REQUEST`]-family bounds, failing fast
/// on the first field that exceeds its limit (message-level fields before
/// per-series fields, so a request with too many series is rejected before
/// this function ever inspects any individual series' labels/samples).
///
/// The hand-written decoders ([`WriteRequest`]/[`BoundedWriteRequest`]/
/// [`TimeSeries`]) already cap each dimension at `MAX + 1` and drain the
/// cross-series aggregate during decode, so this deferred re-check is where the
/// `+ 1` over-cap (and the re-summed aggregate the transient twin counters do
/// not survive into the value type) becomes a whole-request
/// [`LogsIngestError::OversizeMessage`].
fn validate_bounds(req: &WriteRequest) -> Result<(), LogsIngestError> {
    if req.timeseries.len() > MAX_TIMESERIES_PER_REQUEST {
        return Err(LogsIngestError::OversizeMessage {
            field: "timeseries",
            limit: MAX_TIMESERIES_PER_REQUEST,
            actual: req.timeseries.len(),
        });
    }
    if req.metadata.len() > MAX_METADATA_PER_REQUEST {
        return Err(LogsIngestError::OversizeMessage {
            field: "metadata",
            limit: MAX_METADATA_PER_REQUEST,
            actual: req.metadata.len(),
        });
    }
    let mut total_labels: usize = 0;
    let mut total_samples: usize = 0;
    let mut total_histograms: usize = 0;
    let mut total_hist_spans: usize = 0;
    let mut total_hist_buckets: usize = 0;
    for ts in &req.timeseries {
        if ts.labels.len() > MAX_LABELS_PER_SERIES {
            return Err(LogsIngestError::OversizeMessage {
                field: "labels",
                limit: MAX_LABELS_PER_SERIES,
                actual: ts.labels.len(),
            });
        }
        if ts.samples.len() > MAX_SAMPLES_PER_SERIES {
            return Err(LogsIngestError::OversizeMessage {
                field: "samples",
                limit: MAX_SAMPLES_PER_SERIES,
                actual: ts.samples.len(),
            });
        }
        if ts.histograms.len() > MAX_HISTOGRAMS_PER_SERIES {
            return Err(LogsIngestError::OversizeMessage {
                field: "histograms",
                limit: MAX_HISTOGRAMS_PER_SERIES,
                actual: ts.histograms.len(),
            });
        }
        for h in &ts.histograms {
            let max_side_spans = h.negative_spans.len().max(h.positive_spans.len());
            if max_side_spans > MAX_SPANS_PER_HISTOGRAM_SIDE {
                return Err(LogsIngestError::OversizeMessage {
                    field: "histogram spans",
                    limit: MAX_SPANS_PER_HISTOGRAM_SIDE,
                    actual: max_side_spans,
                });
            }
            let max_bucket_field = h
                .negative_deltas
                .len()
                .max(h.negative_counts.len())
                .max(h.positive_deltas.len())
                .max(h.positive_counts.len())
                .max(h.custom_values.len());
            if max_bucket_field > MAX_BUCKETS_PER_HISTOGRAM_SIDE {
                return Err(LogsIngestError::OversizeMessage {
                    field: "histogram buckets",
                    limit: MAX_BUCKETS_PER_HISTOGRAM_SIDE,
                    actual: max_bucket_field,
                });
            }
            total_hist_spans = total_hist_spans.saturating_add(h.span_count());
            total_hist_buckets = total_hist_buckets.saturating_add(h.bucket_element_count());
        }
        total_labels = total_labels.saturating_add(ts.labels.len());
        total_samples = total_samples.saturating_add(ts.samples.len());
        total_histograms = total_histograms.saturating_add(ts.histograms.len());
    }
    // Cross-series aggregates last: a request whose series are each individually
    // in-bounds can still sum past these ceilings (the second-amplification the
    // per-series caps cannot catch).
    if total_labels > MAX_TOTAL_LABELS_PER_REQUEST {
        return Err(LogsIngestError::OversizeMessage {
            field: "total_labels",
            limit: MAX_TOTAL_LABELS_PER_REQUEST,
            actual: total_labels,
        });
    }
    if total_samples > MAX_TOTAL_SAMPLES_PER_REQUEST {
        return Err(LogsIngestError::OversizeMessage {
            field: "total_samples",
            limit: MAX_TOTAL_SAMPLES_PER_REQUEST,
            actual: total_samples,
        });
    }
    if total_histograms > MAX_TOTAL_HISTOGRAMS_PER_REQUEST {
        return Err(LogsIngestError::OversizeMessage {
            field: "total_histograms",
            limit: MAX_TOTAL_HISTOGRAMS_PER_REQUEST,
            actual: total_histograms,
        });
    }
    if total_hist_spans > MAX_TOTAL_HIST_SPANS_PER_REQUEST {
        return Err(LogsIngestError::OversizeMessage {
            field: "total_hist_spans",
            limit: MAX_TOTAL_HIST_SPANS_PER_REQUEST,
            actual: total_hist_spans,
        });
    }
    if total_hist_buckets > MAX_TOTAL_HIST_BUCKETS_PER_REQUEST {
        return Err(LogsIngestError::OversizeMessage {
            field: "total_hist_buckets",
            limit: MAX_TOTAL_HIST_BUCKETS_PER_REQUEST,
            actual: total_hist_buckets,
        });
    }
    // Decode-time byte budget (issue #127), re-summed from the materialized
    // request with the SAME function the incremental drain charges — the
    // deferred whole-request reject for a decode the twin drained past
    // MAX_DECODED_BYTES (bytes, complementing every element-COUNT cap above).
    let decoded_bytes = decoded_request_bytes(&req.timeseries, &req.metadata);
    if decoded_bytes > MAX_DECODED_BYTES {
        return Err(LogsIngestError::OversizeMessage {
            field: "decoded bytes (estimated)",
            limit: MAX_DECODED_BYTES,
            actual: decoded_bytes,
        });
    }
    Ok(())
}

/// Maps a `prompb.MetricMetadata.type` wire value to the same lowercase
/// Prometheus exposition-format type string `otlp_metrics::parse` emits
/// (architect plan's pinned table) — cross-transport `metric_metadata.
/// metric_type` parity is a hard invariant (docs/schemas.md §2.1; the
/// planner keys counter-function legality off these strings). An
/// out-of-range value (outside the eight defined `prompb.MetricType`
/// values) degrades to `"unknown"` rather than a decode error — a forward-
/// compatible unknown type on the wire must not fail the whole request.
fn metric_type_name(t: i32) -> &'static str {
    match t {
        1 => "counter",
        2 => "gauge",
        3 => "histogram",
        4 => "gaugehistogram",
        5 => "summary",
        6 => "info",
        7 => "stateset",
        _ => "unknown",
    }
}

/// Maps a wire `Histogram.ResetHint` to the stored [`CounterResetHint`]
/// (issue #140; the issue body pins the mapping: "Gauge for gauge
/// histograms; Unknown otherwise"). YES(1)/NO(2) are deliberately degraded
/// to `Unknown` — RW per-sample reset hints are not reliable across sender
/// resharding, and the read side re-detects counter resets; only the
/// series-level GAUGE property is safe to persist. Out-of-range values →
/// `Unknown` (forward-compatible, mirrors [`metric_type_name`]).
fn reset_hint_to_counter_reset_hint(v: i32) -> CounterResetHint {
    match v {
        3 => CounterResetHint::Gauge,
        _ => CounterResetHint::Unknown,
    }
}

/// The integer value of a decoded `count`/`zero_count` oneof: `None` is
/// proto3 absent ⇒ 0; the `Float` alternative never reaches this (the
/// float-flavor per-point reject fires first in [`parse_time_series`]), and
/// degrades to 0 defensively rather than panicking if it ever did.
fn histogram_count_int(count: Option<HistogramCount>) -> u64 {
    match count {
        Some(HistogramCount::Int(v)) => v,
        Some(HistogramCount::Float(_)) | None => 0,
    }
}

/// Parses a decoded `WriteRequest` into normalized rows. Pure: a function
/// of `req` and `now_ns` only, no I/O, no clock reads — the caller (the
/// ingest handler) is the only clock/IO boundary. `now_ns` becomes every
/// metadata row's `updated_ns` (the `ReplacingMergeTree` version column,
/// issue #26 amendment).
///
/// `Err` iff the request's estimated expanded output exceeds
/// [`MAX_EXPANDED_BYTES`] (issue #62) — a whole-request, atomic structural
/// failure, exactly like a decode/bounds error; everything else (a series
/// missing `__name__`) stays a per-series drop counted in `rejected` inside
/// the `Ok`.
pub fn parse(req: &WriteRequest, now_ns: i64) -> Result<ParsedMetrics, LogsIngestError> {
    let mut out = ParsedMetrics::default();
    let mut expanded_bytes: usize = 0;
    // Dedups `SeriesRef` registration within this request by `(metric_name,
    // fingerprint)` — a labels carrier, not a per-sample registration
    // (mirrors `otlp_metrics::parse`).
    let mut seen_series: HashSet<(Arc<str>, Fingerprint)> = HashSet::new();

    for ts in &req.timeseries {
        parse_time_series(&mut out, &mut expanded_bytes, &mut seen_series, ts)?;
    }

    // Within-request histogram-wins dedup (issue #140, mirroring
    // `otlp_metrics::parse`'s issue-#120 rule): a native histogram and a
    // float sample at the same `(metric_name, fingerprint, unix_milli)` must
    // never both be written — the histogram wins, matching the read-side
    // tie-break. RW wire allows both on one series; cross-request collisions
    // are resolved by the read path, not here.
    dedup_histogram_wins(&mut out, &mut expanded_bytes)?;

    // Metadata dedup within-request by family name, last-wins (architect
    // plan) — a later entry for the same name overwrites an earlier one
    // rather than both being emitted; `metric_family_name` is used verbatim
    // as `metric_name` (RW carries the base family name explicitly, unlike
    // OTLP where a suffix must never be stripped either — there is simply
    // no suffix to strip here).
    let mut by_name: std::collections::HashMap<Arc<str>, usize> = std::collections::HashMap::new();
    for meta in &req.metadata {
        // Charge the metadata row BEFORE building it (issue #62).
        charge_budget(
            &mut expanded_bytes,
            META_ROW_OVERHEAD + meta.metric_family_name.len() + meta.help.len() + meta.unit.len(),
        )?;
        let name: Arc<str> = Arc::from(meta.metric_family_name.as_str());
        let row = MetricMetadata {
            metric_name: Arc::clone(&name),
            metric_type: metric_type_name(meta.r#type).to_string(),
            help: meta.help.clone(),
            unit: meta.unit.clone(),
            updated_ns: now_ns,
        };
        match by_name.get(&name) {
            Some(&idx) => out.metadata[idx] = row,
            None => {
                by_name.insert(name, out.metadata.len());
                out.metadata.push(row);
            }
        }
    }

    Ok(out)
}

/// Parses one `TimeSeries`: extracts `__name__` (missing/empty -> drop the
/// whole series, `rejected += sample_count` — the only semantic per-series
/// violation remote-write has, architect plan's reject-boundary rule),
/// normalizes the remaining labels, fingerprints them, and emits one
/// [`MetricPoint`] per sample plus (if it has >=1 accepted sample) one
/// [`SeriesRef`] for the series.
fn parse_time_series(
    out: &mut ParsedMetrics,
    expanded_bytes: &mut usize,
    seen_series: &mut HashSet<(Arc<str>, Fingerprint)>,
    ts: &TimeSeries,
) -> Result<(), LogsIngestError> {
    // Charge this series' label/`SeriesRef` materialization BEFORE building
    // `rest`/`from_normalized` (issue #62). Allocation-free: sums wire
    // string lengths plus a fixed [`LABEL_ROW_OVERHEAD`] per label, so a
    // near-empty-label fan-out trips [`MAX_EXPANDED_BYTES`] before any
    // `(String, String)`/label-set materialization (issue #115, finding #62).
    let label_charge = ts.labels.iter().fold(SERIES_ROW_OVERHEAD, |acc, l| {
        acc.saturating_add(LABEL_ROW_OVERHEAD)
            .saturating_add(l.name.len())
            .saturating_add(l.value.len())
    });
    charge_budget(expanded_bytes, label_charge)?;

    let mut name: Option<&str> = None;
    let mut rest: Vec<(String, String)> = Vec::with_capacity(ts.labels.len());
    for label in &ts.labels {
        if label.name == METRIC_NAME_LABEL {
            name = Some(label.value.as_str());
        } else {
            rest.push((label.name.clone(), label.value.clone()));
        }
    }

    let Some(name) = name.filter(|n| !n.is_empty()) else {
        out.rejected += ts.samples.len() as u64;
        if out.rejected_message.is_none() {
            out.rejected_message = Some(
                "time series has no __name__ label (or it is empty): series dropped".to_string(),
            );
        }
        return Ok(());
    };
    let metric_name: Arc<str> = Arc::from(name);

    let (labels, collisions) = LabelSet::from_normalized(rest);
    out.collisions += collisions as u64;
    let fingerprint = metric_fingerprint(&labels);

    // A sampleless series (legal on the wire, e.g. a metadata-only push)
    // registers no `SeriesRef` — the writer derives `metric_series` rows
    // from `ParsedMetrics::samples`'/`hist_samples`' timestamps, so a series
    // with zero accepted samples would yield no row anyway (architect plan;
    // a histograms-only series must register too, issue #140).
    if (!ts.samples.is_empty() || !ts.histograms.is_empty())
        && seen_series.insert((Arc::clone(&metric_name), fingerprint))
    {
        out.series.push(SeriesRef {
            metric_name: Arc::clone(&metric_name),
            fingerprint,
            labels,
        });
    }

    for sample in &ts.samples {
        // `metric_samples` partitions on
        // `toDate(fromUnixTimestamp64Milli(unix_milli))` (issue #126,
        // mirroring #8's log/trace-path fix) and its delete-TTL evaluates
        // `intDiv(unix_milli, 1000)` in the 32-bit `DateTime` domain
        // (issue #137, mirroring #131's trace fix): a sample whose UTC day
        // falls outside the supported storage range (before 1970-01-01 or
        // after 2106-02-06, day 49_709) either cannot be stored in a valid
        // partition or would wrap in the TTL seconds arithmetic, so it is
        // dropped here rather than accepted verbatim. Zero (1970-01-01, no
        // sentinel meaning) still passes; a negative timestamp is rejected
        // too, per #8's pre-1970 `None` contract.
        if Date::start_of_day_utc_ms_datetime_safe(sample.timestamp).is_none() {
            out.rejected += 1;
            if out.rejected_message.is_none() {
                out.rejected_message = Some(format!(
                    "sample timestamp {}ms is outside the supported storage time range \
                     (1970-01-01 to 2106-02-06 UTC)",
                    sample.timestamp
                ));
            }
            continue;
        }
        // Charge each sample BEFORE pushing it (issue #62): the dominant
        // multiplicative term (a 2-byte wire sample → one ~40-byte
        // `MetricPoint`), so a 33.5M-sample fan-out aborts here before mass
        // materialization.
        charge_budget(expanded_bytes, SAMPLE_ROW_OVERHEAD)?;
        out.samples.push(MetricPoint {
            metric_name: Arc::clone(&metric_name),
            fingerprint,
            // Verbatim (once past the `Date`-range gate above): remote-write
            // timestamps are already milliseconds, with no `0`-is-unset
            // sentinel (unlike OTLP's nanosecond `time_unix_nano`, architect
            // plan) — `0` is a literal 1970 timestamp here, not a rejection
            // trigger.
            unix_milli: sample.timestamp,
            value: sample.value,
        });
    }

    for h in &ts.histograms {
        // Charge this histogram's expanded output BEFORE materializing
        // anything (issue #62): the fixed row floor plus a per-element floor
        // for every span/delta/count/custom_value array the verbatim copy
        // allocates. Allocation-free; aborts here before the
        // `NativeHistogram` is built (mirrors `otlp_metrics`' native-path
        // charge shape).
        let hist_charge = HIST_ROW_OVERHEAD.saturating_add(
            h.span_count()
                .saturating_add(h.bucket_element_count())
                .saturating_mul(HIST_ELEMENT_BYTES),
        );
        charge_budget(expanded_bytes, hist_charge)?;

        // Float-flavor histograms (`count_float`/`zero_count_float`/
        // `*_counts`) are structurally unstorable — `metric_hist_samples`'
        // bucket columns are integer deltas — so the point is rejected, never
        // lossily converted. Classification keys off the actually-decoded
        // oneof case (plan v2 finding 2): a reordered float-then-int oneof
        // decodes as `Int` and is accepted.
        let float_flavor = matches!(h.count, Some(HistogramCount::Float(_)))
            || matches!(h.zero_count, Some(HistogramCount::Float(_)))
            || !h.positive_counts.is_empty()
            || !h.negative_counts.is_empty();
        if float_flavor {
            out.rejected += 1;
            if out.rejected_message.is_none() {
                out.rejected_message = Some(
                    "float-flavor native histogram is not storable (integer bucket-delta \
                     columns): point dropped"
                        .to_string(),
                );
            }
            continue;
        }

        // Same `Date`-range gate as float samples above (issues #126/#137):
        // per-histogram timestamps are independent of `Sample.timestamp`;
        // out-of-range drops are per-point, never a whole-request error.
        if Date::start_of_day_utc_ms_datetime_safe(h.timestamp).is_none() {
            out.rejected += 1;
            if out.rejected_message.is_none() {
                out.rejected_message = Some(format!(
                    "histogram timestamp {}ms is outside the supported storage time range \
                     (1970-01-01 to 2106-02-06 UTC)",
                    h.timestamp
                ));
            }
            continue;
        }

        // Verbatim field copy: the wire form and the stored form share the
        // delta encoding (spans + delta-encoded buckets + custom_values), so
        // any transformation here would be a bug — no bucket math, unlike
        // the OTLP exponential path.
        let histogram = NativeHistogram {
            counter_reset_hint: reset_hint_to_counter_reset_hint(h.reset_hint),
            schema: h.schema,
            zero_threshold: h.zero_threshold,
            zero_count: histogram_count_int(h.zero_count),
            count: histogram_count_int(h.count),
            sum: h.sum,
            positive_spans: h
                .positive_spans
                .iter()
                .map(|s| Span {
                    offset: s.offset,
                    length: s.length,
                })
                .collect(),
            negative_spans: h
                .negative_spans
                .iter()
                .map(|s| Span {
                    offset: s.offset,
                    length: s.length,
                })
                .collect(),
            positive_buckets: h.positive_deltas.clone(),
            negative_buckets: h.negative_deltas.clone(),
            custom_values: h.custom_values.clone(),
        };

        // A3 `validate()` at the seam (mirrors `otlp_metrics`' native path):
        // a structurally-invalid histogram is a per-point drop, never a
        // whole-request error. No NaN gate on `sum` — a native-histogram
        // stale marker arrives as the stale NaN there and must pass through
        // bit-exact.
        if let Err(err) = histogram.validate() {
            out.rejected += 1;
            if out.rejected_message.is_none() {
                out.rejected_message = Some(format!("invalid native histogram: {err}"));
            }
            continue;
        }

        out.hist_samples.push(HistogramPoint {
            metric_name: Arc::clone(&metric_name),
            fingerprint,
            unix_milli: h.timestamp,
            histogram,
        });
    }
    Ok(())
}

/// Drops any float sample whose `(metric_name, fingerprint, unix_milli)`
/// coincides with a native-histogram sample in the same request, counting
/// each drop in `rejected` (histogram-wins). A private mirror of
/// `otlp_metrics::dedup_histogram_wins` — deliberately NOT hoisted into a
/// shared helper (issue #140 plan: no new abstraction). The transient dedup
/// key set is charged against the expansion budget BEFORE it is
/// materialized: one [`HIST_DEDUP_KEY_BYTES`] slot per native-histogram
/// sample; the keys borrow `metric_name` as `&str` (no per-sample refcount
/// churn).
fn dedup_histogram_wins(
    out: &mut ParsedMetrics,
    expanded_bytes: &mut usize,
) -> Result<(), LogsIngestError> {
    if out.hist_samples.is_empty() {
        return Ok(());
    }
    charge_budget(
        expanded_bytes,
        out.hist_samples.len().saturating_mul(HIST_DEDUP_KEY_BYTES),
    )?;
    // Borrow the two disjoint fields separately so the borrow checker permits
    // the immutable `hist_keys` borrow of `hist_samples` to coexist with the
    // `retain` mutation of `samples`.
    let hist_samples = &out.hist_samples;
    let samples = &mut out.samples;
    let hist_keys: HashSet<(&str, Fingerprint, i64)> = hist_samples
        .iter()
        .map(|h| (h.metric_name.as_ref(), h.fingerprint, h.unix_milli))
        .collect();
    let before = samples.len();
    samples.retain(|s| !hist_keys.contains(&(s.metric_name.as_ref(), s.fingerprint, s.unix_milli)));
    let dropped = (before - samples.len()) as u64;
    if dropped > 0 {
        out.rejected += dropped;
        if out.rejected_message.is_none() {
            out.rejected_message = Some(
                "float sample dropped: native histogram present at the same series and timestamp"
                    .to_string(),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_model::STALE_NAN_BITS;

    fn label(name: &str, value: &str) -> Label {
        Label {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn sample(value: f64, timestamp: i64) -> Sample {
        Sample { value, timestamp }
    }

    // -- decode -----------------------------------------------------------

    #[test]
    fn decode_rejects_malformed_bytes() {
        let err = decode(b"\xFF\xFF\xFF not a protobuf message").unwrap_err();
        assert!(matches!(err, LogsIngestError::Decode(_)));
    }

    #[test]
    fn decode_round_trips_an_encoded_request() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, 1)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let bytes = req.encode_to_vec();
        let decoded = decode(&bytes).expect("valid protobuf decodes");
        assert_eq!(decoded, req);
    }

    // -- decode-time structural bounds (issue #28 code review hardening) --

    #[test]
    fn validate_bounds_accepts_a_request_within_every_limit() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, 1)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        assert!(validate_bounds(&req).is_ok());
    }

    #[test]
    fn validate_bounds_rejects_too_many_timeseries() {
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![],
                    samples: vec![],
                    histograms: vec![],
                };
                MAX_TIMESERIES_PER_REQUEST + 1
            ],
            metadata: vec![],
        };
        let err = validate_bounds(&req).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "timeseries",
                limit: MAX_TIMESERIES_PER_REQUEST,
                actual,
            } if actual == MAX_TIMESERIES_PER_REQUEST + 1
        ));
    }

    #[test]
    fn validate_bounds_rejects_too_many_labels_in_one_series() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("k", "v"); MAX_LABELS_PER_SERIES + 1],
                samples: vec![],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let err = validate_bounds(&req).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "labels",
                ..
            }
        ));
    }

    #[test]
    fn validate_bounds_rejects_too_many_samples_in_one_series() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![],
                samples: vec![sample(1.0, 1); MAX_SAMPLES_PER_SERIES + 1],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let err = validate_bounds(&req).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "samples",
                ..
            }
        ));
    }

    #[test]
    fn validate_bounds_rejects_too_much_metadata() {
        let entry = MetricMetadataProto {
            r#type: 0,
            metric_family_name: String::new(),
            help: String::new(),
            unit: String::new(),
        };
        let req = WriteRequest {
            timeseries: vec![],
            metadata: vec![entry; MAX_METADATA_PER_REQUEST + 1],
        };
        let err = validate_bounds(&req).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "metadata",
                ..
            }
        ));
    }

    /// Proves the bound is actually wired into the public [`decode`]
    /// boundary (not just callable directly, same guard `LogsIngestError`
    /// classifies as a whole-request `400`), by round-tripping a too-large
    /// request through real protobuf encode/decode.
    #[test]
    fn decode_enforces_the_timeseries_bound_end_to_end() {
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![],
                    samples: vec![],
                    histograms: vec![],
                };
                MAX_TIMESERIES_PER_REQUEST + 1
            ],
            metadata: vec![],
        };
        let bytes = req.encode_to_vec();
        let err = decode(&bytes).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "timeseries",
                ..
            }
        ));
    }

    // -- decode-time DoS bounds (issue #115, finding #62) -----------------
    //
    // These prove rejection happens BEFORE full materialization, not merely
    // that the request is rejected. Each arm decodes a hand-encoded body via
    // the public `WriteRequest::decode` (which routes through the bounded twin)
    // and inspects the materialized length — a length-cap the *derived* decode
    // would blow past (materializing every encoded element). That length
    // assertion is the non-vacuity property: it fails against the pre-fix
    // derived decoder, and each arm additionally confirms the public [`decode`]
    // turns the drained `+ 1` sentinel into a whole-request `OversizeMessage`.

    /// One length-delimited protobuf field: key (tag, wire-type 2) + length
    /// varint + payload. An empty payload (`&[]`) is a zero-length submessage.
    fn field_ld(tag: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(payload.len() + 6);
        prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, &mut out);
        prost::encoding::encode_varint(payload.len() as u64, &mut out);
        out.extend_from_slice(payload);
        out
    }

    /// A bare length-delimited prefix (a message-length varint, no tag) +
    /// payload — the framing `Message::merge_length_delimited` consumes.
    fn length_delimited(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(payload.len() + 5);
        prost::encoding::encode_varint(payload.len() as u64, &mut out);
        out.extend_from_slice(payload);
        out
    }

    /// A body encoding `count` empty `TimeSeries` records (`WriteRequest`
    /// tag 1). Each is two bytes: `0x0a 0x00`.
    fn empty_timeseries_body(count: usize) -> Vec<u8> {
        let mut body = Vec::with_capacity(count * 2);
        for _ in 0..count {
            body.extend_from_slice(&field_ld(1, &[]));
        }
        body
    }

    #[test]
    fn decode_caps_timeseries_materialization_and_rejects_too_many_timeseries() {
        // AC (too many timeseries): a body encoding more than
        // MAX_TIMESERIES_PER_REQUEST series must NOT materialize them all — the
        // hand-written decoder caps the vec at MAX + 1 and drains the rest
        // without allocating.
        let encoded = MAX_TIMESERIES_PER_REQUEST + 8;
        let body = empty_timeseries_body(encoded);
        let decoded = WriteRequest::decode(body.as_slice()).expect("empty series decode");
        assert_eq!(
            decoded.timeseries.len(),
            MAX_TIMESERIES_PER_REQUEST + 1,
            "the decoder must cap materialization at MAX + 1, not materialize all encoded series"
        );
        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "timeseries",
                ..
            }
        ));
    }

    #[test]
    fn decode_caps_label_materialization_and_rejects_too_many_labels() {
        // AC (too many labels-per-series): one series carrying more than
        // MAX_LABELS_PER_SERIES labels caps at MAX + 1 during decode.
        let encoded = MAX_LABELS_PER_SERIES + 8;
        let mut ts_payload = Vec::with_capacity(encoded * 2);
        for _ in 0..encoded {
            ts_payload.extend_from_slice(&field_ld(1, &[])); // empty Label
        }
        let body = field_ld(1, &ts_payload); // one TimeSeries
        let decoded = WriteRequest::decode(body.as_slice()).expect("one-series decode");
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(
            decoded.timeseries[0].labels.len(),
            MAX_LABELS_PER_SERIES + 1,
            "the decoder must cap per-series label materialization at MAX + 1"
        );
        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "labels",
                ..
            }
        ));
    }

    #[test]
    fn decode_caps_sample_materialization_and_rejects_too_many_samples() {
        // AC (too many samples-per-series): one series carrying more than
        // MAX_SAMPLES_PER_SERIES samples caps at MAX + 1 during decode.
        let encoded = MAX_SAMPLES_PER_SERIES + 8;
        let mut ts_payload = Vec::with_capacity(encoded * 2);
        for _ in 0..encoded {
            ts_payload.extend_from_slice(&field_ld(2, &[])); // empty Sample
        }
        let body = field_ld(1, &ts_payload);
        let decoded = WriteRequest::decode(body.as_slice()).expect("one-series decode");
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(
            decoded.timeseries[0].samples.len(),
            MAX_SAMPLES_PER_SERIES + 1,
            "the decoder must cap per-series sample materialization at MAX + 1"
        );
        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "samples",
                ..
            }
        ));
    }

    #[test]
    fn decode_caps_metadata_materialization_and_rejects_too_much_metadata() {
        // AC (too much metadata): more than MAX_METADATA_PER_REQUEST metadata
        // records cap at MAX + 1 during decode (WriteRequest tag 3).
        let encoded = MAX_METADATA_PER_REQUEST + 8;
        let mut body = Vec::with_capacity(encoded * 2);
        for _ in 0..encoded {
            body.extend_from_slice(&field_ld(3, &[])); // empty MetricMetadata
        }
        let decoded = WriteRequest::decode(body.as_slice()).expect("empty metadata decode");
        assert_eq!(
            decoded.metadata.len(),
            MAX_METADATA_PER_REQUEST + 1,
            "the decoder must cap metadata materialization at MAX + 1"
        );
        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "metadata",
                ..
            }
        ));
    }

    /// A body of `series` in-bounds `TimeSeries`, each carrying `labels_each`
    /// empty labels (tag 1) — used to drive the cross-series LABEL aggregate
    /// past its cap while every series stays under [`MAX_LABELS_PER_SERIES`].
    fn label_aggregate_body(series: usize, labels_each: usize) -> Vec<u8> {
        let mut ts_payload = Vec::with_capacity(labels_each * 2);
        for _ in 0..labels_each {
            ts_payload.extend_from_slice(&field_ld(1, &[]));
        }
        let ts_record = field_ld(1, &ts_payload);
        let mut body = Vec::with_capacity(ts_record.len() * series);
        for _ in 0..series {
            body.extend_from_slice(&ts_record);
        }
        body
    }

    /// A body of `series` in-bounds `TimeSeries`, each carrying `samples_each`
    /// empty samples (tag 2) — drives the cross-series SAMPLE aggregate past its
    /// cap while every series stays within [`MAX_SAMPLES_PER_SERIES`].
    fn sample_aggregate_body(series: usize, samples_each: usize) -> Vec<u8> {
        let mut ts_payload = Vec::with_capacity(samples_each * 2);
        for _ in 0..samples_each {
            ts_payload.extend_from_slice(&field_ld(2, &[]));
        }
        let ts_record = field_ld(1, &ts_payload);
        let mut body = Vec::with_capacity(ts_record.len() * series);
        for _ in 0..series {
            body.extend_from_slice(&ts_record);
        }
        body
    }

    #[test]
    fn decode_drains_series_once_the_cross_series_label_aggregate_is_exceeded() {
        // AC (cross-series aggregate labels): every series stays UNDER
        // MAX_LABELS_PER_SERIES, but their label counts SUM past
        // MAX_TOTAL_LABELS_PER_REQUEST. The transient cross-series accumulator
        // stops materializing series once the running total exceeds the
        // aggregate, so fewer labels are materialized than encoded (the derived
        // decode would materialize them all — the non-vacuity property).
        let labels_each = MAX_LABELS_PER_SERIES; // 256, each series in-bounds
        let series = MAX_TOTAL_LABELS_PER_REQUEST / labels_each + 2;
        let body = label_aggregate_body(series, labels_each);

        let decoded = WriteRequest::decode(body.as_slice()).expect("aggregate decode");
        let materialized: usize = decoded.timeseries.iter().map(|ts| ts.labels.len()).sum();
        assert!(
            decoded.timeseries.len() < series,
            "the decoder must drain series once the label aggregate is exceeded \
             (materialized {} of {series} encoded series)",
            decoded.timeseries.len()
        );
        assert!(
            materialized <= MAX_TOTAL_LABELS_PER_REQUEST + MAX_LABELS_PER_SERIES,
            "aggregate label fan-out must be bounded to MAX_TOTAL + one series' cap, got {materialized}"
        );

        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "total_labels",
                ..
            }
        ));
    }

    #[test]
    fn decode_drains_series_once_the_cross_series_sample_aggregate_is_exceeded() {
        // AC (cross-series aggregate samples): every series stays WITHIN
        // MAX_SAMPLES_PER_SERIES, but their sample counts SUM past
        // MAX_TOTAL_SAMPLES_PER_REQUEST — drained during decode.
        let samples_each = MAX_SAMPLES_PER_SERIES; // 100_000, each series in-bounds
        let series = MAX_TOTAL_SAMPLES_PER_REQUEST / samples_each + 2;
        let body = sample_aggregate_body(series, samples_each);

        let decoded = WriteRequest::decode(body.as_slice()).expect("aggregate decode");
        let materialized: usize = decoded.timeseries.iter().map(|ts| ts.samples.len()).sum();
        assert!(
            decoded.timeseries.len() < series,
            "the decoder must drain series once the sample aggregate is exceeded \
             (materialized {} of {series} encoded series)",
            decoded.timeseries.len()
        );
        assert!(
            materialized <= MAX_TOTAL_SAMPLES_PER_REQUEST + MAX_SAMPLES_PER_SERIES,
            "aggregate sample fan-out must be bounded to MAX_TOTAL + one series' cap, got {materialized}"
        );

        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "total_samples",
                ..
            }
        ));
    }

    // -- raw merge / merge_length_delimited entry points are ALSO bounded --
    //
    // `prost`'s default `Message::merge`/`merge_length_delimited` call
    // `WriteRequest::merge_field` directly (which caps only element COUNTS), so
    // a raw `WriteRequest::default().merge(buf)` would otherwise bypass the
    // cross-series aggregate cap. The hand-written overrides route both raw
    // entry points through the bounded twin (issue #115 lesson 1).

    fn assert_label_aggregate_bounded(req: &WriteRequest, encoded_series: usize) {
        let materialized: usize = req.timeseries.iter().map(|ts| ts.labels.len()).sum();
        assert!(
            req.timeseries.len() < encoded_series,
            "the raw merge path must drain series once the label aggregate is exceeded \
             (retained {} of {encoded_series} encoded)",
            req.timeseries.len()
        );
        assert!(
            materialized <= MAX_TOTAL_LABELS_PER_REQUEST + MAX_LABELS_PER_SERIES,
            "the raw merge path must bound aggregate label fan-out to MAX_TOTAL + one \
             series' cap, got {materialized}"
        );
    }

    #[test]
    fn write_request_merge_enforces_the_cross_series_aggregate() {
        let labels_each = MAX_LABELS_PER_SERIES;
        let encoded_series = MAX_TOTAL_LABELS_PER_REQUEST / labels_each + 2;
        let body = label_aggregate_body(encoded_series, labels_each);

        let mut req = WriteRequest::default();
        req.merge(body.as_slice()).expect("bounded raw merge");
        assert_label_aggregate_bounded(&req, encoded_series);
    }

    #[test]
    fn write_request_merge_length_delimited_enforces_the_cross_series_aggregate() {
        let labels_each = MAX_LABELS_PER_SERIES;
        let encoded_series = MAX_TOTAL_LABELS_PER_REQUEST / labels_each + 2;
        let framed = length_delimited(&label_aggregate_body(encoded_series, labels_each));

        let mut req = WriteRequest::default();
        req.merge_length_delimited(framed.as_slice())
            .expect("bounded raw merge_length_delimited");
        assert_label_aggregate_bounded(&req, encoded_series);
    }

    // -- merge-into-existing preserves state on a decode error ------------

    /// A pre-existing request to merge malformed input INTO — one real series
    /// and one metadata entry, so the retention assertions have something to
    /// lose.
    fn request_with_one_series() -> WriteRequest {
        WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, 1)],
                histograms: vec![],
            }],
            metadata: vec![MetricMetadataProto {
                r#type: 1,
                metric_family_name: "up".to_string(),
                help: "total".to_string(),
                unit: String::new(),
            }],
        }
    }

    #[test]
    fn merge_of_malformed_bytes_retains_pre_existing_state() {
        // Issue #115 lesson 2: a failed raw `merge` must NOT drop the caller's
        // pre-existing fields. The override moves self's fields into the bounded
        // twin, so an early `?` on decode error would leave self EMPTY (data
        // loss). The fix restores the twin's fields on BOTH paths, giving prost
        // partial-merge semantics. Non-vacuous: against a `mem::take(...);
        // bounded.merge(buf)?` shape, `req` would be empty here.
        let original = request_with_one_series();
        let mut req = original.clone();
        req.merge(b"\xff\xff\xff not a protobuf message".as_slice())
            .expect_err("malformed merge must fail");
        assert_eq!(
            req, original,
            "a failed merge must retain the pre-existing timeseries/metadata, not empty them"
        );
    }

    #[test]
    fn merge_length_delimited_of_malformed_bytes_retains_pre_existing_state() {
        let original = request_with_one_series();
        let mut req = original.clone();
        let framed = length_delimited(b"\xff\xff\xff not a protobuf message");
        req.merge_length_delimited(framed.as_slice())
            .expect_err("malformed merge_length_delimited must fail");
        assert_eq!(
            req, original,
            "a failed merge_length_delimited must retain the pre-existing state"
        );
    }

    // -- positive: legitimate in-bounds requests decode unchanged ---------

    #[test]
    fn decode_admits_an_ordinary_multi_series_request_unchanged() {
        // A legitimate batch (multiple series, labels, samples, metadata) — all
        // dimensions and both aggregates well under their caps — round-trips
        // through real encode/decode byte-identically (the caps never reject
        // real traffic).
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![label("__name__", "up"), label("job", "checkout")],
                    samples: vec![sample(1.0, 1), sample(2.0, 2)],
                    histograms: vec![],
                },
                TimeSeries {
                    labels: vec![label("__name__", "latency_bucket"), label("le", "0.5")],
                    samples: vec![sample(3.0, 1)],
                    histograms: vec![],
                },
            ],
            metadata: vec![MetricMetadataProto {
                r#type: 1,
                metric_family_name: "up".to_string(),
                help: "total".to_string(),
                unit: String::new(),
            }],
        };
        let bytes = req.encode_to_vec();
        let decoded = decode(&bytes).expect("an ordinary in-bounds request decodes");
        assert_eq!(decoded, req);
    }

    // -- parse: basic series ----------------------------------------------

    #[test]
    fn parse_of_empty_request_returns_empty_output() {
        let out = parse(
            &WriteRequest {
                timeseries: vec![],
                metadata: vec![],
            },
            1_000,
        )
        .expect("within the expansion budget");
        assert_eq!(out, ParsedMetrics::default());
    }

    #[test]
    fn parse_is_a_pure_function_of_its_arguments() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up"), label("job", "checkout")],
                samples: vec![sample(1.0, 1_700_000_000_000)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let a = parse(&req, 42).expect("within the expansion budget");
        let b = parse(&req, 42).expect("within the expansion budget");
        assert_eq!(a, b);
    }

    #[test]
    fn time_series_extracts_name_and_fingerprints_remaining_labels() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    label("__name__", "http_requests_total"),
                    label("job", "checkout"),
                    label("method", "GET"),
                ],
                samples: vec![sample(42.0, 1_700_000_000_000)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.samples.len(), 1);
        assert_eq!(&*out.samples[0].metric_name, "http_requests_total");
        assert_eq!(out.samples[0].value, 42.0);
        assert_eq!(out.samples[0].unix_milli, 1_700_000_000_000);
        assert_eq!(out.series.len(), 1);
        assert_eq!(out.series[0].labels.get("job"), Some("checkout"));
        assert_eq!(out.series[0].labels.get("method"), Some("GET"));
        // `__name__` never enters the LabelSet (architect plan).
        assert_eq!(out.series[0].labels.get("__name__"), None);
    }

    #[test]
    fn multiple_samples_on_one_series_share_one_series_ref() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, 1), sample(2.0, 2), sample(3.0, 3)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.samples.len(), 3);
        assert_eq!(out.series.len(), 1);
    }

    #[test]
    fn a_sampleless_series_emits_no_series_ref() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert!(out.samples.is_empty());
        assert!(out.series.is_empty());
    }

    // -- reject boundary: missing/empty __name__ ---------------------------

    #[test]
    fn missing_name_label_drops_the_series_and_counts_rejected_samples() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("job", "checkout")],
                samples: vec![sample(1.0, 1), sample(2.0, 2)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 2);
        assert!(out.samples.is_empty());
        assert!(out.series.is_empty());
        assert!(out.rejected_message.is_some());
    }

    #[test]
    fn empty_name_label_value_drops_the_series() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "")],
                samples: vec![sample(1.0, 1)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert!(out.samples.is_empty());
    }

    #[test]
    fn one_bad_series_does_not_reject_the_rest_of_the_request() {
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![label("job", "checkout")],
                    samples: vec![sample(1.0, 1)],
                    histograms: vec![],
                },
                TimeSeries {
                    labels: vec![label("__name__", "up")],
                    samples: vec![sample(1.0, 1)],
                    histograms: vec![],
                },
            ],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert_eq!(out.samples.len(), 1);
        assert_eq!(&*out.samples[0].metric_name, "up");
    }

    // -- timestamps verbatim, no sentinel -------------------------------
    // -- `Date`-range gate (issue #126) ----------------------------------

    #[test]
    fn zero_timestamp_is_accepted_verbatim_no_sentinel_rule() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, 0)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 0);
        assert_eq!(out.samples[0].unix_milli, 0);
    }

    #[test]
    fn negative_timestamp_is_rejected_not_accepted_verbatim() {
        // #8's pre-1970 `None` contract: `metric_samples` partitions on the
        // raw sample day, which cannot represent a pre-epoch date, so it is
        // dropped rather than accepted verbatim (was pinned the other way
        // before issue #126).
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, -1_000)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert!(out.samples.is_empty());
        assert!(
            out.rejected_message
                .as_deref()
                .unwrap()
                .contains("outside the supported storage time range")
        );
    }

    #[test]
    fn sample_at_the_last_datetime_safe_day_is_accepted_verbatim() {
        // Day 49_709 = 2106-02-06, the last UTC day fully inside the
        // 32-bit DateTime domain the metric delete-TTL evaluates in
        // (issue #137); its last millisecond.
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, 4_294_943_999_999)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 0);
        assert_eq!(out.samples.len(), 1);
        assert_eq!(out.samples[0].unix_milli, 4_294_943_999_999);
    }

    #[test]
    fn sample_at_the_first_datetime_unsafe_day_is_rejected() {
        // Day 49_710 = 2106-02-07: inside the u16 `Date` range but its TTL
        // seconds value exceeds u32::MAX — accepted (wrap-prone) before
        // issue #137, a per-sample drop now.
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(1.0, 4_294_944_000_000)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert!(out.samples.is_empty());
        assert!(
            out.rejected_message.as_deref().unwrap().contains(
                "outside the supported storage time range (1970-01-01 to 2106-02-06 UTC)"
            )
        );
    }

    #[test]
    fn mixed_series_keeps_the_good_sample_and_rejects_the_far_future_one() {
        // One series, two samples: the in-range one survives, the
        // far-future one is dropped — proving per-sample (not per-series)
        // rejection semantics (issue #126 edge case).
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![
                    sample(1.0, 1_700_000_000_000),
                    sample(2.0, 5_662_310_400_000),
                ],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert_eq!(out.samples.len(), 1);
        assert_eq!(out.samples[0].unix_milli, 1_700_000_000_000);
        assert_eq!(out.samples[0].value, 1.0);
        // The series is still registered: it has at least one accepted
        // sample (writer/metric.rs derives `metric_series` rows from
        // accepted samples only, so this is harmless either way).
        assert_eq!(out.series.len(), 1);
    }

    // -- stale marker --------------------------------------------------------

    #[test]
    fn stale_nan_sample_survives_bit_exact() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![sample(f64::from_bits(STALE_NAN_BITS), 1)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.samples[0].value.to_bits(), STALE_NAN_BITS);
    }

    // -- label normalization / fingerprint identity --------------------------

    #[test]
    fn unsorted_wire_labels_are_accepted_and_resorted_deterministically() {
        let req_a = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    label("__name__", "up"),
                    label("z_label", "1"),
                    label("a_label", "2"),
                ],
                samples: vec![sample(1.0, 1)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let req_b = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    label("a_label", "2"),
                    label("__name__", "up"),
                    label("z_label", "1"),
                ],
                samples: vec![sample(1.0, 1)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out_a = parse(&req_a, 0).expect("within the expansion budget");
        let out_b = parse(&req_b, 0).expect("within the expansion budget");
        assert_eq!(out_a.samples[0].fingerprint, out_b.samples[0].fingerprint);
        assert_eq!(
            out_a.series[0].labels.iter().collect::<Vec<_>>(),
            vec![("a_label", "2"), ("z_label", "1")]
        );
    }

    #[test]
    fn dotted_and_underscored_labels_fingerprint_identically_cross_transport_identity() {
        let req_dot = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up"), label("service.name", "checkout")],
                samples: vec![sample(1.0, 1)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let req_underscore = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up"), label("service_name", "checkout")],
                samples: vec![sample(1.0, 1)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out_dot = parse(&req_dot, 0).expect("within the expansion budget");
        let out_underscore = parse(&req_underscore, 0).expect("within the expansion budget");
        assert_eq!(
            out_dot.samples[0].fingerprint,
            out_underscore.samples[0].fingerprint
        );
    }

    #[test]
    fn le_and_quantile_remain_ordinary_labels_in_the_fingerprint() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "latency_bucket"), label("le", "0.5")],
                samples: vec![sample(3.0, 1)],
                histograms: vec![],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.series[0].labels.get("le"), Some("0.5"));
    }

    // -- metadata ------------------------------------------------------------

    #[test]
    fn metadata_maps_every_documented_type_string() {
        let cases: &[(i32, &str)] = &[
            (0, "unknown"),
            (1, "counter"),
            (2, "gauge"),
            (3, "histogram"),
            (4, "gaugehistogram"),
            (5, "summary"),
            (6, "info"),
            (7, "stateset"),
            (99, "unknown"),
        ];
        for &(wire_type, expected) in cases {
            assert_eq!(metric_type_name(wire_type), expected);
        }
    }

    #[test]
    fn metadata_entry_maps_to_the_seam_type_with_injected_updated_ns() {
        let req = WriteRequest {
            timeseries: vec![],
            metadata: vec![MetricMetadataProto {
                r#type: 1,
                metric_family_name: "http_requests_total".to_string(),
                help: "total requests".to_string(),
                unit: "".to_string(),
            }],
        };
        let out = parse(&req, 123).expect("within the expansion budget");
        assert_eq!(out.metadata.len(), 1);
        assert_eq!(&*out.metadata[0].metric_name, "http_requests_total");
        assert_eq!(out.metadata[0].metric_type, "counter");
        assert_eq!(out.metadata[0].help, "total requests");
        assert_eq!(out.metadata[0].updated_ns, 123);
    }

    #[test]
    fn metadata_family_name_is_used_verbatim_no_suffix_stripping() {
        let req = WriteRequest {
            timeseries: vec![],
            metadata: vec![MetricMetadataProto {
                r#type: 3,
                metric_family_name: "latency".to_string(),
                help: String::new(),
                unit: String::new(),
            }],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(&*out.metadata[0].metric_name, "latency");
        assert_eq!(out.metadata[0].metric_type, "histogram");
    }

    #[test]
    fn duplicate_metadata_family_name_dedups_last_wins() {
        let req = WriteRequest {
            timeseries: vec![],
            metadata: vec![
                MetricMetadataProto {
                    r#type: 2,
                    metric_family_name: "up".to_string(),
                    help: "first".to_string(),
                    unit: String::new(),
                },
                MetricMetadataProto {
                    r#type: 2,
                    metric_family_name: "up".to_string(),
                    help: "second".to_string(),
                    unit: String::new(),
                },
            ],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.metadata.len(), 1);
        assert_eq!(out.metadata[0].help, "second");
    }

    // -- expansion budget (issue #62) -------------------------------------

    /// A single named series carrying more than the admissible ~4.2M-sample
    /// ceiling trips [`MAX_EXPANDED_BYTES`] (issue #62 Δ1) — the per-sample
    /// caps (per-series bounds) do not stop it, only this cumulative byte
    /// budget does. The `actual <= limit + SAMPLE_ROW_OVERHEAD` bound proves
    /// charge-before-materialize: each sample is charged (and the abort
    /// fires) BEFORE its `MetricPoint` is pushed, so materialization stops at
    /// the tipping sample rather than after the whole fan-out. Sample count
    /// derives from the constants so a retune cannot silently weaken it.
    #[test]
    fn expansion_budget_rejects_sample_fan_out() {
        let sample_count = MAX_EXPANDED_BYTES / SAMPLE_ROW_OVERHEAD + 2;
        let samples: Vec<Sample> = (0..sample_count as i64).map(|i| sample(0.0, i)).collect();
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "up")],
                samples,
                histograms: vec![],
            }],
            metadata: vec![],
        };

        let err = parse(&req, 0).expect_err("sample fan-out must trip the expansion budget");
        let LogsIngestError::OversizeMessage { limit, actual, .. } = err else {
            panic!("unexpected error: {err}");
        };
        assert_eq!(limit, MAX_EXPANDED_BYTES);
        assert!(actual > MAX_EXPANDED_BYTES);
        assert!(
            actual <= MAX_EXPANDED_BYTES + SAMPLE_ROW_OVERHEAD,
            "abort must fire at the tipping sample charge (charge-before-materialize): \
             actual={actual}"
        );
    }

    /// A near-empty-label fan-out trips [`MAX_EXPANDED_BYTES`] on the
    /// [`LABEL_ROW_OVERHEAD`] floor alone (issue #115, finding #62) — the
    /// undercharge the decode-time COUNT caps do not close. The construction
    /// stays UNDER every count cap: each series holds exactly
    /// [`MAX_LABELS_PER_SERIES`] labels, the ~2.1M total labels are under
    /// [`MAX_TOTAL_LABELS_PER_REQUEST`] (5M), and ~8.2k series are under
    /// [`MAX_TIMESERIES_PER_REQUEST`] — so [`validate_bounds`] (the count
    /// caps) ADMITS it, and only `parse`'s cumulative byte budget rejects it.
    ///
    /// NON-VACUOUS by construction: every label is `("", "")` — zero raw wire
    /// bytes — so the entire estimate comes from the per-label floor. Were
    /// `LABEL_ROW_OVERHEAD` 0, the label charge would be 0 and the request
    /// would stay ~0.5 MiB (series overhead only), well under the 256 MiB
    /// budget, and `parse` would ACCEPT it. The rejection is therefore
    /// attributable solely to the 128 B floor. Series count derives from the
    /// constants so a retune cannot silently weaken it. Full-trip form (not a
    /// focused charge probe) mirrors `expansion_budget_rejects_sample_fan_out`
    /// and the established ~4M-element hermetic tier, exercising the real
    /// top-level `parse` rejection with its exact `field`; ~2.1M empty-string
    /// `Label`s cost only ~100 MiB of headers (empty `String`s never heap-
    /// allocate), comparable to the sibling sample-fan-out test.
    #[test]
    fn expansion_budget_rejects_near_empty_label_fan_out() {
        // Enough full-width (256-label) series that the per-label floor alone
        // overshoots the budget: MAX_EXPANDED_BYTES / (128 * 256) series + 2.
        let num_series = MAX_EXPANDED_BYTES / (LABEL_ROW_OVERHEAD * MAX_LABELS_PER_SERIES) + 2;
        let timeseries: Vec<TimeSeries> = (0..num_series)
            .map(|_| TimeSeries {
                labels: vec![label("", ""); MAX_LABELS_PER_SERIES],
                samples: vec![],
                histograms: vec![],
            })
            .collect();
        let req = WriteRequest {
            timeseries,
            metadata: vec![],
        };

        // The count caps admit the fan-out: only the byte floor stops it.
        validate_bounds(&req).expect("count caps admit the near-empty-label fan-out");

        let err =
            parse(&req, 0).expect_err("near-empty-label fan-out must trip the expansion budget");
        assert!(
            matches!(
                err,
                LogsIngestError::OversizeMessage {
                    field: "expanded metric row bytes (estimated)",
                    limit: MAX_EXPANDED_BYTES,
                    ..
                }
            ),
            "unexpected error: {err}"
        );
    }

    /// The budget is a whole-request bound, not a per-series truncation: an
    /// ordinary request (multiple series, samples, metadata) parses `Ok`.
    #[test]
    fn expansion_budget_admits_ordinary_request() {
        let req = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![label("__name__", "up"), label("job", "checkout")],
                    samples: vec![sample(1.0, 1), sample(2.0, 2)],
                    histograms: vec![],
                },
                TimeSeries {
                    labels: vec![label("__name__", "latency_bucket"), label("le", "0.5")],
                    samples: vec![sample(3.0, 1)],
                    histograms: vec![],
                },
            ],
            metadata: vec![MetricMetadataProto {
                r#type: 1,
                metric_family_name: "up".to_string(),
                help: "total".to_string(),
                unit: String::new(),
            }],
        };
        let out = parse(&req, 0).expect("ordinary request is within the budget");
        assert_eq!(out.samples.len(), 3);
        assert_eq!(out.series.len(), 2);
        assert_eq!(out.metadata.len(), 1);
    }

    // -- native histograms (issue #140) ------------------------------------

    /// A valid integer native histogram carrying both bucket sides:
    /// positive absolute counts [1, 0, 2] (deltas [1, -1, 2]), negative
    /// absolute [2] (delta [2]), zero_count 1 ⇒ count 6.
    fn wire_hist(reset_hint: i32, timestamp: i64) -> Histogram {
        Histogram {
            count: Some(HistogramCount::Int(6)),
            sum: 10.5,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: Some(HistogramCount::Int(1)),
            negative_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            negative_deltas: vec![2],
            positive_spans: vec![BucketSpan {
                offset: 1,
                length: 3,
            }],
            positive_deltas: vec![1, -1, 2],
            reset_hint,
            timestamp,
            ..Default::default()
        }
    }

    fn one_hist_series(histograms: Vec<Histogram>) -> WriteRequest {
        WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "latency"), label("job", "checkout")],
                samples: vec![],
                histograms,
            }],
            metadata: vec![],
        }
    }

    // AC 1: the wire ResetHint → stored CounterResetHint mapping.
    #[test]
    fn reset_hint_maps_gauge_to_gauge_and_everything_else_to_unknown() {
        assert_eq!(
            reset_hint_to_counter_reset_hint(0),
            CounterResetHint::Unknown
        );
        assert_eq!(
            reset_hint_to_counter_reset_hint(1),
            CounterResetHint::Unknown,
            "YES is deliberately degraded to Unknown (issue body pins Gauge-or-Unknown)"
        );
        assert_eq!(
            reset_hint_to_counter_reset_hint(2),
            CounterResetHint::Unknown,
            "NO is deliberately degraded to Unknown"
        );
        assert_eq!(reset_hint_to_counter_reset_hint(3), CounterResetHint::Gauge);
        assert_eq!(
            reset_hint_to_counter_reset_hint(4),
            CounterResetHint::Unknown,
            "forward-compatible out-of-range value degrades to Unknown"
        );
        assert_eq!(
            reset_hint_to_counter_reset_hint(-1),
            CounterResetHint::Unknown
        );
    }

    // AC 2: verbatim field copy + Gauge hint + SeriesRef registration for a
    // histograms-only series.
    #[test]
    fn gauge_hint_histogram_parses_verbatim_and_registers_its_series_ref() {
        let req = one_hist_series(vec![wire_hist(3, 1_700_000_000_000)]);
        let out = parse(&req, 0).expect("within the expansion budget");

        assert_eq!(out.rejected, 0);
        assert!(out.samples.is_empty());
        assert_eq!(out.hist_samples.len(), 1);
        let point = &out.hist_samples[0];
        assert_eq!(&*point.metric_name, "latency");
        assert_eq!(point.unix_milli, 1_700_000_000_000);
        let h = &point.histogram;
        assert_eq!(h.counter_reset_hint, CounterResetHint::Gauge);
        assert_eq!(h.schema, 0);
        assert_eq!(h.zero_threshold.to_bits(), 0.001f64.to_bits());
        assert_eq!(h.zero_count, 1);
        assert_eq!(h.count, 6);
        assert_eq!(h.sum.to_bits(), 10.5f64.to_bits());
        assert_eq!(
            h.positive_spans,
            vec![pulsus_model::Span {
                offset: 1,
                length: 3
            }]
        );
        assert_eq!(
            h.negative_spans,
            vec![pulsus_model::Span {
                offset: 0,
                length: 1
            }]
        );
        assert_eq!(h.positive_buckets, vec![1, -1, 2]);
        assert_eq!(h.negative_buckets, vec![2]);
        assert!(h.custom_values.is_empty());

        // A histograms-only series (no float samples) still registers its
        // SeriesRef, with `__name__` excluded and the fingerprint
        // independently recomputable.
        assert_eq!(out.series.len(), 1);
        assert_eq!(out.series[0].labels.get("job"), Some("checkout"));
        assert_eq!(out.series[0].labels.get("__name__"), None);
        assert_eq!(
            pulsus_model::metric_fingerprint(&out.series[0].labels),
            point.fingerprint
        );
    }

    #[test]
    fn unknown_hint_histogram_parses_with_unknown_reset_hint() {
        let req = one_hist_series(vec![wire_hist(0, 1_700_000_000_000)]);
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(
            out.hist_samples[0].histogram.counter_reset_hint,
            CounterResetHint::Unknown
        );
    }

    // AC 3: float-flavor histograms are per-point rejects; integer siblings
    // in the same request are still accepted.
    #[test]
    fn float_flavor_histogram_is_rejected_per_point_integer_sibling_kept() {
        let float_count = Histogram {
            count: Some(HistogramCount::Float(6.0)),
            ..wire_hist(0, 1_700_000_000_000)
        };
        let float_zero_count = Histogram {
            zero_count: Some(HistogramCount::Float(1.0)),
            ..wire_hist(0, 1_700_000_000_000)
        };
        let float_buckets = Histogram {
            positive_spans: vec![BucketSpan {
                offset: 1,
                length: 1,
            }],
            positive_deltas: vec![],
            positive_counts: vec![5.0],
            ..wire_hist(0, 1_700_000_000_000)
        };
        let req = one_hist_series(vec![
            float_count,
            float_zero_count,
            float_buckets,
            wire_hist(0, 1_700_000_000_000),
        ]);
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 3, "every float-flavor variant is dropped");
        assert_eq!(
            out.hist_samples.len(),
            1,
            "the integer sibling in the same request is still emitted"
        );
        assert!(
            out.rejected_message
                .as_deref()
                .unwrap()
                .contains("float-flavor native histogram")
        );
    }

    // AC 4: the per-histogram Date-range timestamp gate.
    #[test]
    fn out_of_range_histogram_timestamp_is_rejected_in_range_sibling_kept() {
        // Day 49_710 = 2106-02-07: first DateTime-unsafe day (issue #137).
        let req = one_hist_series(vec![
            wire_hist(3, 4_294_944_000_000),
            wire_hist(3, 1_700_000_000_000),
        ]);
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert_eq!(out.hist_samples.len(), 1);
        assert_eq!(out.hist_samples[0].unix_milli, 1_700_000_000_000);
        assert!(
            out.rejected_message
                .as_deref()
                .unwrap()
                .contains("outside the supported storage time range")
        );
    }

    #[test]
    fn negative_histogram_timestamp_is_rejected() {
        let req = one_hist_series(vec![wire_hist(0, -1_000)]);
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert!(out.hist_samples.is_empty());
    }

    // AC 5: histogram-wins dedup at the same `(name, fp, ms)`.
    #[test]
    fn float_sample_at_the_same_series_and_timestamp_loses_to_the_histogram() {
        let ms = 1_700_000_000_000;
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![label("__name__", "latency"), label("job", "checkout")],
                samples: vec![sample(1.0, ms), sample(2.0, ms + 1)],
                histograms: vec![wire_hist(0, ms)],
            }],
            metadata: vec![],
        };
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 1, "the colliding float sample is dropped");
        assert_eq!(out.hist_samples.len(), 1, "the histogram wins");
        assert_eq!(
            out.samples.len(),
            1,
            "the non-colliding float sample survives"
        );
        assert_eq!(out.samples[0].unix_milli, ms + 1);
        assert!(
            out.rejected_message
                .as_deref()
                .unwrap()
                .contains("native histogram present at the same series and timestamp")
        );
    }

    // Seam validation: a structurally-invalid histogram is a per-point drop.
    #[test]
    fn histogram_failing_seam_validation_is_rejected_per_point() {
        let bad_count = Histogram {
            count: Some(HistogramCount::Int(99)), // buckets sum to 6
            ..wire_hist(0, 1_700_000_000_000)
        };
        let req = one_hist_series(vec![bad_count, wire_hist(0, 1_700_000_000_000)]);
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 1);
        assert_eq!(out.hist_samples.len(), 1);
        assert!(
            out.rejected_message
                .as_deref()
                .unwrap()
                .contains("invalid native histogram")
        );
    }

    // Edge case 2 (plan): a native-histogram stale marker (stale NaN in
    // `sum`) passes validation and survives bit-exact — no NaN gate on sum.
    #[test]
    fn stale_nan_histogram_sum_survives_bit_exact() {
        let stale = Histogram {
            sum: f64::from_bits(STALE_NAN_BITS),
            ..wire_hist(0, 1_700_000_000_000)
        };
        let req = one_hist_series(vec![stale]);
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 0);
        assert_eq!(
            out.hist_samples[0].histogram.sum.to_bits(),
            STALE_NAN_BITS,
            "the stale marker must round-trip bit-for-bit"
        );
    }

    // NHCB (schema −53 + custom_values) is accepted verbatim.
    #[test]
    fn nhcb_histogram_with_custom_values_is_accepted_verbatim() {
        let nhcb = Histogram {
            count: Some(HistogramCount::Int(4)),
            sum: 5.0,
            schema: -53,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_deltas: vec![1, 1, -1],
            custom_values: vec![5.0, 10.0],
            reset_hint: 3,
            timestamp: 1_700_000_000_000,
            ..Default::default()
        };
        let req = one_hist_series(vec![nhcb]);
        let out = parse(&req, 0).expect("within the expansion budget");
        assert_eq!(out.rejected, 0);
        let h = &out.hist_samples[0].histogram;
        assert_eq!(h.schema, -53);
        assert_eq!(h.custom_values, vec![5.0, 10.0]);
        assert_eq!(h.counter_reset_hint, CounterResetHint::Gauge);
    }

    // Wire round-trip through the hand-written encode/decode, including the
    // zigzag fields at their extremes (negative schema/offset/deltas).
    #[test]
    fn histogram_round_trips_through_encode_and_the_public_decode() {
        let req = one_hist_series(vec![Histogram {
            count: Some(HistogramCount::Int(6)),
            sum: -2.5,
            schema: -4,
            zero_threshold: 1e-128,
            zero_count: Some(HistogramCount::Int(1)),
            negative_spans: vec![BucketSpan {
                offset: -3,
                length: 1,
            }],
            negative_deltas: vec![-2, 4],
            positive_spans: vec![BucketSpan {
                offset: 2,
                length: 2,
            }],
            positive_deltas: vec![3, -3],
            reset_hint: 3,
            timestamp: 1_700_000_000_000,
            ..Default::default()
        }]);
        let bytes = req.encode_to_vec();
        let decoded = decode(&bytes).expect("valid protobuf decodes");
        assert_eq!(decoded, req);
    }

    // AC 12 (plan v2 finding 2): a reordered/repeated oneof — the float
    // alternative emitted BEFORE the int alternative — decodes with the int
    // case winning (proto3 last-occurrence-wins) and parses as an ACCEPTED
    // integer histogram, never misclassified as float-flavor.
    #[test]
    fn reordered_oneof_float_then_int_decodes_as_integer_and_is_accepted() {
        let mut hist_payload = Vec::new();
        // count oneof: tag 2 (float) then tag 1 (int) — int wins.
        prost::encoding::double::encode(2u32, &99.5f64, &mut hist_payload);
        prost::encoding::uint64::encode(1u32, &6u64, &mut hist_payload);
        // zero_count oneof: tag 7 (float) then tag 6 (int) — int wins.
        prost::encoding::double::encode(7u32, &1.5f64, &mut hist_payload);
        prost::encoding::uint64::encode(6u32, &1u64, &mut hist_payload);
        prost::encoding::double::encode(3u32, &10.5f64, &mut hist_payload);
        let neg_span = BucketSpan {
            offset: 0,
            length: 1,
        };
        prost::encoding::message::encode(8u32, &neg_span, &mut hist_payload);
        prost::encoding::sint64::encode_packed(9u32, &[2i64], &mut hist_payload);
        let pos_span = BucketSpan {
            offset: 1,
            length: 3,
        };
        prost::encoding::message::encode(11u32, &pos_span, &mut hist_payload);
        prost::encoding::sint64::encode_packed(12u32, &[1i64, -1, 2], &mut hist_payload);
        prost::encoding::int64::encode(15u32, &1_700_000_000_000i64, &mut hist_payload);

        let mut ts_payload = Vec::new();
        let name = label("__name__", "latency");
        prost::encoding::message::encode(1u32, &name, &mut ts_payload);
        ts_payload.extend_from_slice(&field_ld(4, &hist_payload));
        let body = field_ld(1, &ts_payload);

        let decoded = decode(&body).expect("reordered oneof still decodes");
        let h = &decoded.timeseries[0].histograms[0];
        assert_eq!(
            h.count,
            Some(HistogramCount::Int(6)),
            "the LAST count oneof occurrence (int) must win"
        );
        assert_eq!(
            h.zero_count,
            Some(HistogramCount::Int(1)),
            "the LAST zero_count oneof occurrence (int) must win"
        );

        let out = parse(&decoded, 0).expect("within the expansion budget");
        assert_eq!(
            out.rejected, 0,
            "a reordered oneof must never misfire the float-flavor reject"
        );
        assert_eq!(out.hist_samples.len(), 1);
        assert_eq!(out.hist_samples[0].histogram.count, 6);
        assert_eq!(out.hist_samples[0].histogram.zero_count, 1);
    }

    // -- decode-time histogram DoS caps (issue #140, #115 pattern) ---------

    #[test]
    fn decode_caps_histogram_materialization_and_rejects_too_many_histograms() {
        // One series carrying more than MAX_HISTOGRAMS_PER_SERIES empty
        // histograms caps at MAX + 1 during decode.
        let encoded = MAX_HISTOGRAMS_PER_SERIES + 8;
        let mut ts_payload = Vec::with_capacity(encoded * 2);
        for _ in 0..encoded {
            ts_payload.extend_from_slice(&field_ld(4, &[])); // empty Histogram
        }
        let body = field_ld(1, &ts_payload);
        let decoded = WriteRequest::decode(body.as_slice()).expect("one-series decode");
        assert_eq!(
            decoded.timeseries[0].histograms.len(),
            MAX_HISTOGRAMS_PER_SERIES + 1,
            "the decoder must cap per-series histogram materialization at MAX + 1"
        );
        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "histograms",
                ..
            }
        ));
    }

    #[test]
    fn decode_caps_span_materialization_per_side_and_rejects_too_many_spans() {
        // One histogram carrying more than MAX_SPANS_PER_HISTOGRAM_SIDE
        // spans on one side caps at MAX + 1 during decode.
        let encoded = MAX_SPANS_PER_HISTOGRAM_SIDE + 8;
        let mut hist_payload = Vec::with_capacity(encoded * 2);
        for _ in 0..encoded {
            hist_payload.extend_from_slice(&field_ld(11, &[])); // empty BucketSpan
        }
        let ts_payload = field_ld(4, &hist_payload);
        let body = field_ld(1, &ts_payload);
        let decoded = WriteRequest::decode(body.as_slice()).expect("one-series decode");
        assert_eq!(
            decoded.timeseries[0].histograms[0].positive_spans.len(),
            MAX_SPANS_PER_HISTOGRAM_SIDE + 1,
            "the decoder must cap per-side span materialization at MAX + 1"
        );
        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "histogram spans",
                ..
            }
        ));
    }

    #[test]
    fn decode_caps_a_single_over_cap_packed_delta_run_mid_run() {
        // ONE packed sint64 run carrying more than
        // MAX_BUCKETS_PER_HISTOGRAM_SIDE elements (each zigzag zero is a
        // single wire byte) must cap MID-run: materialize MAX + 1, then
        // decode-and-discard the rest of the run.
        let encoded = MAX_BUCKETS_PER_HISTOGRAM_SIDE + 8;
        let run = vec![0u8; encoded]; // sint64 zigzag(0) == one 0x00 varint each
        let hist_payload = field_ld(12, &run); // positive_deltas, packed
        let ts_payload = field_ld(4, &hist_payload);
        let body = field_ld(1, &ts_payload);
        let decoded = WriteRequest::decode(body.as_slice()).expect("one-series decode");
        assert_eq!(
            decoded.timeseries[0].histograms[0].positive_deltas.len(),
            MAX_BUCKETS_PER_HISTOGRAM_SIDE + 1,
            "a single packed run must be capped mid-run at MAX + 1"
        );
        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "histogram buckets",
                ..
            }
        ));
    }

    /// A body of `series` in-bounds `TimeSeries`, each carrying one
    /// histogram with `spans_each` empty positive spans — drives the
    /// request-wide span aggregate past its cap while every histogram stays
    /// within [`MAX_SPANS_PER_HISTOGRAM_SIDE`].
    fn hist_span_aggregate_body(series: usize, spans_each: usize) -> Vec<u8> {
        let mut hist_payload = Vec::with_capacity(spans_each * 2);
        for _ in 0..spans_each {
            hist_payload.extend_from_slice(&field_ld(11, &[]));
        }
        let ts_record = field_ld(1, &field_ld(4, &hist_payload));
        let mut body = Vec::with_capacity(ts_record.len() * series);
        for _ in 0..series {
            body.extend_from_slice(&ts_record);
        }
        body
    }

    fn materialized_spans(req: &WriteRequest) -> usize {
        req.timeseries
            .iter()
            .flat_map(|ts| &ts.histograms)
            .map(|h| h.span_count())
            .sum()
    }

    // AC 11 (plan v2 finding 1): the request-wide aggregate span cap.
    #[test]
    fn decode_drains_series_once_the_cross_histogram_span_aggregate_is_exceeded() {
        // Every histogram stays within MAX_SPANS_PER_HISTOGRAM_SIDE, but the
        // span counts SUM past MAX_TOTAL_HIST_SPANS_PER_REQUEST — drained
        // during decode (strictly fewer spans materialized than encoded),
        // then rejected whole-request by the deferred re-sum.
        let spans_each = MAX_SPANS_PER_HISTOGRAM_SIDE; // 4_096, in-bounds
        let series = MAX_TOTAL_HIST_SPANS_PER_REQUEST / spans_each + 2;
        let body = hist_span_aggregate_body(series, spans_each);

        let decoded = WriteRequest::decode(body.as_slice()).expect("aggregate decode");
        assert!(
            decoded.timeseries.len() < series,
            "the decoder must drain series once the span aggregate is exceeded \
             (materialized {} of {series} encoded series)",
            decoded.timeseries.len()
        );
        let materialized = materialized_spans(&decoded);
        assert!(
            materialized <= MAX_TOTAL_HIST_SPANS_PER_REQUEST + spans_each,
            "aggregate span fan-out must be bounded to MAX_TOTAL + one series' worth, \
             got {materialized}"
        );

        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "total_hist_spans",
                ..
            }
        ));
    }

    #[test]
    fn write_request_merge_enforces_the_cross_histogram_span_aggregate() {
        let spans_each = MAX_SPANS_PER_HISTOGRAM_SIDE;
        let encoded_series = MAX_TOTAL_HIST_SPANS_PER_REQUEST / spans_each + 2;
        let body = hist_span_aggregate_body(encoded_series, spans_each);

        let mut req = WriteRequest::default();
        req.merge(body.as_slice()).expect("bounded raw merge");
        assert!(
            req.timeseries.len() < encoded_series,
            "the raw merge path must drain series once the span aggregate is exceeded"
        );
        assert!(materialized_spans(&req) <= MAX_TOTAL_HIST_SPANS_PER_REQUEST + spans_each);
    }

    #[test]
    fn write_request_merge_length_delimited_enforces_the_cross_histogram_span_aggregate() {
        let spans_each = MAX_SPANS_PER_HISTOGRAM_SIDE;
        let encoded_series = MAX_TOTAL_HIST_SPANS_PER_REQUEST / spans_each + 2;
        let framed = length_delimited(&hist_span_aggregate_body(encoded_series, spans_each));

        let mut req = WriteRequest::default();
        req.merge_length_delimited(framed.as_slice())
            .expect("bounded raw merge_length_delimited");
        assert!(
            req.timeseries.len() < encoded_series,
            "the raw merge_length_delimited path must drain series once the span aggregate \
             is exceeded"
        );
        assert!(
            materialized_spans(&req)
                <= MAX_TOTAL_HIST_SPANS_PER_REQUEST + MAX_SPANS_PER_HISTOGRAM_SIDE
        );
    }

    #[test]
    fn raw_merge_seeding_counts_pre_existing_histogram_spans() {
        // Merge INTO a request whose pre-existing series already exceed the
        // span aggregate: the seeding re-sum must count them, so every newly
        // merged series is drained. Non-vacuous: were the seed ignored, all 8
        // encoded series would materialize.
        let mut req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![],
                samples: vec![],
                histograms: vec![Histogram {
                    positive_spans: vec![
                        BucketSpan::default();
                        MAX_TOTAL_HIST_SPANS_PER_REQUEST + 1
                    ],
                    ..Default::default()
                }],
            }],
            metadata: vec![],
        };
        let body = hist_span_aggregate_body(8, MAX_SPANS_PER_HISTOGRAM_SIDE);
        req.merge(body.as_slice()).expect("bounded raw merge");
        assert_eq!(
            req.timeseries.len(),
            1,
            "with the seed re-sum already over the aggregate, every merged series is drained"
        );
    }

    // Codex review (high): the request-wide aggregates must trip WITHIN one
    // series, not only at the between-series boundary — one crafted series
    // (each histogram individually legal) could otherwise fully materialize
    // its entire span/bucket fan-out before any cross-series check ran.

    #[test]
    fn decode_drains_histograms_mid_series_once_the_span_aggregate_is_exceeded() {
        // A SINGLE TimeSeries whose histograms (each at exactly
        // MAX_SPANS_PER_HISTOGRAM_SIDE spans, in-bounds) sum past
        // MAX_TOTAL_HIST_SPANS_PER_REQUEST on their own: the incremental
        // per-histogram charge must drain the excess histograms DURING the
        // series' decode (strictly fewer spans materialized than encoded),
        // then the deferred re-sum rejects the whole request.
        let spans_each = MAX_SPANS_PER_HISTOGRAM_SIDE; // 4_096, in-bounds
        let hists = MAX_TOTAL_HIST_SPANS_PER_REQUEST / spans_each + 2;
        let mut hist_payload = Vec::with_capacity(spans_each * 2);
        for _ in 0..spans_each {
            hist_payload.extend_from_slice(&field_ld(11, &[]));
        }
        let hist_record = field_ld(4, &hist_payload);
        let mut ts_payload = Vec::with_capacity(hist_record.len() * hists);
        for _ in 0..hists {
            ts_payload.extend_from_slice(&hist_record);
        }
        let body = field_ld(1, &ts_payload); // ONE TimeSeries

        let decoded = WriteRequest::decode(body.as_slice()).expect("single-series decode");
        assert_eq!(decoded.timeseries.len(), 1);
        let materialized = materialized_spans(&decoded);
        assert!(
            materialized < hists * spans_each,
            "the decoder must drain histograms mid-series once the span aggregate is \
             exceeded (materialized all {materialized} encoded spans)"
        );
        assert!(
            materialized <= MAX_TOTAL_HIST_SPANS_PER_REQUEST + spans_each,
            "single-series span fan-out must be bounded to MAX_TOTAL + one histogram's \
             worth, got {materialized}"
        );

        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "total_hist_spans",
                ..
            }
        ));
    }

    #[test]
    fn decode_drains_histograms_mid_series_once_the_bucket_aggregate_is_exceeded() {
        // The bucket analog: ONE series of histograms each carrying a
        // MAX_BUCKETS_PER_HISTOGRAM_SIDE packed delta run (in-bounds per
        // field) whose element counts sum past
        // MAX_TOTAL_HIST_BUCKETS_PER_REQUEST — ~5 MB of wire bytes that
        // would otherwise materialize the whole ~40 MiB fan-out before the
        // between-series boundary.
        let buckets_each = MAX_BUCKETS_PER_HISTOGRAM_SIDE; // 65_536, in-bounds
        let hists = MAX_TOTAL_HIST_BUCKETS_PER_REQUEST / buckets_each + 2;
        let run = vec![0u8; buckets_each];
        let hist_record = field_ld(4, &field_ld(12, &run));
        let mut ts_payload = Vec::with_capacity(hist_record.len() * hists);
        for _ in 0..hists {
            ts_payload.extend_from_slice(&hist_record);
        }
        let body = field_ld(1, &ts_payload); // ONE TimeSeries

        let decoded = WriteRequest::decode(body.as_slice()).expect("single-series decode");
        assert_eq!(decoded.timeseries.len(), 1);
        let materialized: usize = decoded.timeseries[0]
            .histograms
            .iter()
            .map(|h| h.bucket_element_count())
            .sum();
        assert!(
            materialized < hists * buckets_each,
            "the decoder must drain histograms mid-series once the bucket aggregate is \
             exceeded (materialized all {materialized} encoded bucket elements)"
        );
        assert!(
            materialized <= MAX_TOTAL_HIST_BUCKETS_PER_REQUEST + buckets_each,
            "single-series bucket fan-out must be bounded to MAX_TOTAL + one histogram's \
             worth, got {materialized}"
        );

        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "total_hist_buckets",
                ..
            }
        ));
    }

    #[test]
    fn decode_drains_series_once_the_cross_series_histogram_aggregate_is_exceeded() {
        // Every series stays within MAX_HISTOGRAMS_PER_SERIES, but the
        // histogram counts SUM past MAX_TOTAL_HISTOGRAMS_PER_REQUEST.
        let hists_each = MAX_HISTOGRAMS_PER_SERIES; // 10_000, in-bounds
        let series = MAX_TOTAL_HISTOGRAMS_PER_REQUEST / hists_each + 2;
        let mut ts_payload = Vec::with_capacity(hists_each * 2);
        for _ in 0..hists_each {
            ts_payload.extend_from_slice(&field_ld(4, &[]));
        }
        let ts_record = field_ld(1, &ts_payload);
        let mut body = Vec::with_capacity(ts_record.len() * series);
        for _ in 0..series {
            body.extend_from_slice(&ts_record);
        }

        let decoded = WriteRequest::decode(body.as_slice()).expect("aggregate decode");
        let materialized: usize = decoded
            .timeseries
            .iter()
            .map(|ts| ts.histograms.len())
            .sum();
        assert!(
            decoded.timeseries.len() < series,
            "the decoder must drain series once the histogram aggregate is exceeded"
        );
        assert!(
            materialized <= MAX_TOTAL_HISTOGRAMS_PER_REQUEST + MAX_HISTOGRAMS_PER_SERIES,
            "aggregate histogram fan-out must be bounded to MAX_TOTAL + one series' cap, \
             got {materialized}"
        );

        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "total_histograms",
                ..
            }
        ));
    }

    #[test]
    fn decode_drains_series_once_the_cross_histogram_bucket_aggregate_is_exceeded() {
        // Every packed run stays within MAX_BUCKETS_PER_HISTOGRAM_SIDE, but
        // the bucket-element counts SUM past
        // MAX_TOTAL_HIST_BUCKETS_PER_REQUEST.
        let buckets_each = MAX_BUCKETS_PER_HISTOGRAM_SIDE; // 65_536, in-bounds
        let series = MAX_TOTAL_HIST_BUCKETS_PER_REQUEST / buckets_each + 2;
        let run = vec![0u8; buckets_each];
        let ts_record = field_ld(1, &field_ld(4, &field_ld(12, &run)));
        let mut body = Vec::with_capacity(ts_record.len() * series);
        for _ in 0..series {
            body.extend_from_slice(&ts_record);
        }

        let decoded = WriteRequest::decode(body.as_slice()).expect("aggregate decode");
        let materialized: usize = decoded
            .timeseries
            .iter()
            .flat_map(|ts| &ts.histograms)
            .map(|h| h.bucket_element_count())
            .sum();
        assert!(
            decoded.timeseries.len() < series,
            "the decoder must drain series once the bucket aggregate is exceeded"
        );
        assert!(
            materialized <= MAX_TOTAL_HIST_BUCKETS_PER_REQUEST + buckets_each,
            "aggregate bucket fan-out must be bounded to MAX_TOTAL + one series' worth, \
             got {materialized}"
        );

        let err = decode(&body).unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::OversizeMessage {
                field: "total_hist_buckets",
                ..
            }
        ));
    }
}
