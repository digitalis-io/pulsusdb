//! The streaming JSON envelope encoder for `/api/logs/v1`'s five endpoints
//! (docs/api.md §2). Builds the response body incrementally from the
//! already-materialized `QueryResult`/`PlanExplain`/`Vec<String>` — never a
//! `serde_json::Value` DOM over the whole result set, never a second full
//! copy (issue #13 architect plan: "the streaming encoder writes the
//! response body incrementally ... no serde_json DOM for result sets").
//!
//! [`stream_array`] is the one low-level primitive every response shape
//! below is built from: it yields `prefix`, then one rendered chunk per
//! item (comma-separated), then `suffix`, via `futures::stream::unfold` —
//! at most one item's rendered bytes are ever alive between successive
//! `poll_next` calls (the encoder-memory AC amendment 1 exists to satisfy;
//! see this module's tests for the chunk-boundedness proof).
//!
//! **Poll-after-end (issue #24):** the raw `unfold` stream is `.fuse()`d
//! before it is handed to `Body::from_stream`. `Unfold`'s documented
//! invariant is that it must never be polled again once it has returned
//! `Poll::Ready(None)` — it panics otherwise. Under identity encoding,
//! axum/hyper never poll a body again after `None`, so the bug lay
//! dormant; `tower_http::compression::CompressionLayer`'s gzip encoder
//! polls the wrapped body once more past its final `None` to observe
//! EOF/flush, which re-polled the bare `Unfold` and panicked the request
//! task on every gzip-negotiated request. `Fuse` makes the extra poll a
//! safe no-op (`Poll::Ready(None)` forever) without buffering or changing
//! any frame this encoder yields.
//!
//! **Ordering (edge case #1):** the engine's results arrive in
//! `HashMap`-iteration order (unstable). Every response shape here sorts
//! its items by label set (streams: `(labels_json, fingerprint)`; matrix/
//! vector/series: the label vector itself) before framing, so wire output
//! is deterministic and golden fixtures are byte-exact.

use std::borrow::Cow;

use axum::body::{Body, Bytes};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde::Serialize;

use pulsus_read::{
    DetectedFieldOut, DetectedFields, DetectedLabelOut, EntryCategories, ExplainStage, LogStats,
    MatrixSeries, PatternSeries, PlanExplain, QueryResult, RouteChoice, StreamResult, VectorSample,
    VolumeEntry, Warnings, WireArity,
};

/// Builds a streaming JSON body: `prefix`, then `render(item)` for each
/// item in `items` (comma-separated), then `suffix`. `items` (the already-
/// materialized, O(limit)-bounded domain data) is moved into the stream's
/// state and lives for the whole drain — only the *current* item's
/// `render()` output is additional, temporary encoder memory.
///
/// The `unfold` stream is `.fuse()`d before reaching `Body::from_stream`
/// (issue #24): `Fuse` adds no buffering, it only turns a poll after the
/// stream's first `None` into another safe `None` instead of a panic —
/// load-bearing for `tower_http::compression::CompressionLayer`'s gzip
/// encoder, which polls once past EOF.
fn stream_array<T, R>(prefix: Vec<u8>, items: Vec<T>, render: R, suffix: Vec<u8>) -> Body
where
    T: Send + 'static,
    R: Fn(&T) -> Vec<u8> + Send + 'static,
{
    enum Step {
        Prefix,
        /// The `,` between items — its own zero-copy static chunk (issue
        /// #312), so an item is never copied into a separator-bearing Vec.
        Sep(usize),
        Item(usize),
        Suffix,
        Done,
    }

    struct State<T, R> {
        items: Vec<T>,
        render: R,
        step: Step,
        prefix: Vec<u8>,
        suffix: Vec<u8>,
    }

    let state = State {
        items,
        render,
        step: Step::Prefix,
        prefix,
        suffix,
    };

    let stream = futures::stream::unfold(state, |mut state| async move {
        match state.step {
            Step::Prefix => {
                let bytes = std::mem::take(&mut state.prefix);
                state.step = if state.items.is_empty() {
                    Step::Suffix
                } else {
                    Step::Item(0)
                };
                Some((Ok::<_, std::io::Error>(Bytes::from(bytes)), state))
            }
            Step::Sep(i) => {
                state.step = Step::Item(i);
                Some((Ok(Bytes::from_static(b",")), state))
            }
            Step::Item(i) => {
                let chunk = (state.render)(&state.items[i]);
                let next = i + 1;
                state.step = if next < state.items.len() {
                    Step::Sep(next)
                } else {
                    Step::Suffix
                };
                Some((Ok(Bytes::from(chunk)), state))
            }
            Step::Suffix => {
                let bytes = std::mem::take(&mut state.suffix);
                state.step = Step::Done;
                Some((Ok(Bytes::from(bytes)), state))
            }
            Step::Done => None,
        }
    });

    Body::from_stream(stream.fuse())
}

fn json_response(body: Body) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

// ---------------------------------------------------------------------
// Small, fixed-size metadata blocks (stats/explain): `serde_json` is fine
// here — these are never the (potentially large) result array itself.
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct StreamStats {
    streams: usize,
    entries: usize,
    bytes: usize,
    /// Issue #90 signaled partial: `true` iff a fetch-until-limit
    /// filtering query stopped early because the byte scan budget was
    /// exhausted (distinguishable from genuine exhaustion). Skipped when
    /// `false` so ordinary responses stay byte-identical to pre-#90.
    #[serde(skip_serializing_if = "is_false")]
    pulsus_partial: bool,
}

/// `serde` `skip_serializing_if` predicate for a defaulted-`false` flag.
fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Serialize)]
struct SeriesStats {
    series: usize,
}

#[derive(Serialize)]
struct ExplainWire<'a> {
    result_type: &'a str,
    routing: Option<RoutingWire<'a>>,
    stages: Vec<StageWire<'a>>,
}

#[derive(Serialize)]
struct RoutingWire<'a> {
    chosen: &'static str,
    reason: &'a str,
}

#[derive(Serialize)]
struct StageWire<'a> {
    name: &'a str,
    sql: &'a str,
    note: Option<&'a str>,
}

fn explain_json(e: &PlanExplain) -> String {
    let wire = ExplainWire {
        result_type: e.result_type,
        routing: e.routing.as_ref().map(|r| RoutingWire {
            chosen: match r.chosen {
                RouteChoice::Rollup => "rollup",
                RouteChoice::Raw => "raw",
            },
            reason: &r.reason,
        }),
        stages: e
            .stages
            .iter()
            .map(|s: &ExplainStage| StageWire {
                name: s.name,
                sql: &s.sql,
                note: s.note.as_deref(),
            })
            .collect(),
    };
    serde_json::to_string(&wire).unwrap_or_else(|_| "{}".to_string())
}

fn labels_object_json(labels: &[(String, String)]) -> String {
    let map: serde_json::Map<String, serde_json::Value> = labels
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_else(|_| "{}".to_string())
}

fn json_string(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
}

/// Prometheus-style `unix_seconds.millis` timestamp (a bare JSON number
/// literal, embedded unquoted).
fn format_unix_seconds(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let millis = ns.rem_euclid(1_000_000_000) / 1_000_000;
    format!("{secs}.{millis:03}")
}

/// Prometheus-style sample value formatting: `NaN`/`+Inf`/`-Inf` as
/// strings, everything else via Rust's round-trip `f64` `Display` — always
/// returned as a **quoted** JSON string (docs/api.md §3.1's convention,
/// applied consistently here).
fn format_value_json(v: f64) -> String {
    let text = if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        format!("{v}")
    };
    json_string(&text)
}

/// The UNESCAPED rendered size of one stream item — an exact reservation
/// for the common case (no `\u00XX` expansion), computed in `O(entries)`
/// with no per-byte work.
fn stream_item_estimate(s: &StreamResult) -> usize {
    23 + s.labels_json.len()
        + s.entries
            .iter()
            .map(|(_, line)| 28 + line.len())
            .sum::<usize>()
}

/// The streams-response substitution (issue #455): every U+FFFD rune in
/// a stream's rendered label JSON becomes ONE space (`0x20`).
///
/// The reference does this in its streams marshaller —
/// `pkg/util/marshal/query.go:25-32 @ v3.7.4` defines
/// `removeInvalidUtf` ("The rune error replacement is rejected by
/// Prometheus hence replacing them with space"), applied by `bytes.Map`
/// over `stream.Labels` in `NewStreams` at `:92-93`. Measured on
/// `docker.io/grafana/loki@sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`
/// (`/status/buildinfo` -> `3.7.4`/`b318f282`) at
/// `/loki/api/v1/query_range`: a `bytes` unit error whose argument holds
/// `\xe0\xa0` comes back with `20 20` after `unhandled size name: `, and
/// an ordinary `label_format`-minted U+FFFD comes back as `20` too — the
/// rule is not special to the error pair.
///
/// **Log LINE values are deliberately NOT touched**: the reference maps
/// `stream.Labels` only, and a line carrying a genuine U+FFFD comes back
/// as `ef bf bd` on both sides.
///
/// Raw invalid BYTES cannot occur here — `StreamResult::labels_json` is a
/// `String` — so mapping the rune is the whole rule for us.
///
/// **Scoped to the QUERY response.** `render_stream_item_into` is shared
/// with [`tail_frame`], so the caller states which rule it wants
/// ([`LabelBytes`]) rather than inheriting one. `/api/logs/v1/tail` and
/// its `/loki/api/v1/tail` alias keep `LabelBytes::Verbatim`: every
/// measurement on issue #455 — nine plan revisions, four review rounds —
/// was taken at `/loki/api/v1/query_range`, nothing about the tail
/// surface was ever established, and matching it needs its own
/// differential, its own ledger row and its own reachability question.
/// Unchanged bytes are reversible; a divergence changed on an unmeasured
/// route is not. Held still by
/// `tail_frames_keep_their_stream_label_bytes_verbatim` below.
///
/// **Placement is load-bearing, and the two ways of getting it wrong are
/// caught by different tests — established by running them, not by
/// reading.** This runs at RENDER time, downstream of
/// `query_response_warned`'s `(labels_json, fingerprint)` sort, so object
/// order follows the PRE-substitution label set, which is what the
/// reference's unsplit branch does.
///
/// * Moved to the GROUPING key (`push_fanout_entry`'s
///   `render_labels_json_sorted`) the two colliding streams merge into
///   one object and the seam interleaves. Caught live by
///   `logs_utf8_substitution_live::colliding_streams_and_the_split_divergence_agree_with_the_reference`
///   (measured: `left: 1  right: 2` for Q9 at `h=15m`).
/// * Moved before the SORT but after grouping, the ordering key collapses.
///   Caught by `colliding_stream_labels_order_by_the_pre_substitution_label_set`
///   below, and **not** by the live suite: a fan-out group's fingerprint is
///   `fnv1a64` of its own pre-substitution `labels_json`
///   (`logql/detected_probe.rs:85`), so on the real pipeline the tiebreak
///   carries the same information the key just lost and the order survives.
///   The hermetic test builds `StreamResult`s with chosen fingerprints,
///   which is what lets it see the difference.
///
/// **Two readings of a collision that cannot be told apart, so neither is
/// claimed.** When two label sets differ only in a U+FFFD against a
/// literal space they become identical here, and "first in
/// pre-substitution sort order wins" and "the substituted value loses"
/// are *indistinguishable by construction*: U+FFFD begins `0xEF` and a
/// space is `0x20`, so a substituted value always sorts AFTER the
/// literal-space one it collides with. The other reading is untested
/// rather than wrong, and nothing here implements it. Ledgered as
/// `streams-split-merge`.
///
/// Borrows when the input holds no U+FFFD, which is every ordinary
/// response: an unconditional `replace` adds an allocation per rendered
/// stream and moves `ac19_render_path_peak_and_allocation_profile`'s
/// benign profile from `(1, 108045)` to `(2, 108067)`.
fn space_for_replacement_chars(labels_json: &str) -> Cow<'_, str> {
    if labels_json.contains(char::REPLACEMENT_CHARACTER) {
        Cow::Owned(labels_json.replace(char::REPLACEMENT_CHARACTER, " "))
    } else {
        Cow::Borrowed(labels_json)
    }
}

/// The bytes one entry's third element renders as when both category
/// objects are empty — the reference emits `{}` rather than omitting the
/// element (`pkg/util/marshal/query.go:404-470 @ grafana/loki v3.7.4
/// b318f282`).
const THIRD_ELEMENT_EMPTY: &str = ",{}";

/// The UNESCAPED rendered size of one entry's third element (issue #463),
/// computed in `O(pairs)` with no per-byte work — the
/// [`stream_item_estimate`] discipline.
fn third_element_estimate(cats: &EntryCategories) -> usize {
    let pairs = |v: &[(String, String)]| -> usize {
        // `"k":"v",` per pair, plus the object's own braces.
        v.iter()
            .map(|(k, val)| k.len() + val.len() + 6)
            .sum::<usize>()
            + 2
    };
    // `,{` + `}` plus, per present category, its key and object.
    let mut n = 3;
    if !cats.structured_metadata.is_empty() {
        n += 22 + pairs(&cats.structured_metadata);
    }
    if !cats.parsed.is_empty() {
        n += 10 + pairs(&cats.parsed);
    }
    n
}

/// Writes one entry's third element (issue #463): `structuredMetadata`
/// before `parsed`, each omitted when empty, `{}` when both are —
/// `pkg/util/marshal/query.go:404-470 @ grafana/loki v3.7.4 b318f282`,
/// where the two maps are Go `map[string]string`s marshalled by
/// `encoding/json`, which sorts its keys. Ours arrive sorted from the
/// engine.
fn write_third_element(out: &mut Vec<u8>, cats: &EntryCategories) {
    fn object(out: &mut Vec<u8>, key: &str, pairs: &[(String, String)]) {
        out.extend_from_slice(key.as_bytes());
        out.push(b'{');
        for (i, (k, v)) in pairs.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            if serde_json::to_writer(&mut *out, k).is_err() {
                out.extend_from_slice(b"\"\"");
            }
            out.push(b':');
            if serde_json::to_writer(&mut *out, v).is_err() {
                out.extend_from_slice(b"\"\"");
            }
        }
        out.push(b'}');
    }
    out.extend_from_slice(b",{");
    let mut wrote = false;
    if !cats.structured_metadata.is_empty() {
        object(out, "\"structuredMetadata\":", &cats.structured_metadata);
        wrote = true;
    }
    if !cats.parsed.is_empty() {
        if wrote {
            out.push(b',');
        }
        object(out, "\"parsed\":", &cats.parsed);
    }
    out.push(b'}');
}

/// Which rule a caller of [`render_stream_item_into`] wants applied to the
/// stream's label JSON.
///
/// An enum and not a flag, because the two callers are two WIRE SURFACES
/// with two evidence bases: the query response is measured against the
/// reference on issue #455, the tail frame is not. Naming the rule at
/// each call site is what stops the next edit to the shared renderer from
/// silently moving a surface nobody measured — which is exactly what
/// happened here, and twice before in the same issue at
/// `render_labels_json_sorted`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LabelBytes {
    /// Map every U+FFFD to one space — the query-response rule
    /// ([`space_for_replacement_chars`], issue #455).
    SpaceForReplacementChars,
    /// Splice the label JSON exactly as the engine produced it.
    Verbatim,
}

/// Renders one stream item into ONE buffer (issue #312): entry framing,
/// timestamps and JSON-escaped lines are written in place, so no
/// per-entry `String` and no whole-item second copy is ever live.
///
/// Two production callers, and they ask for different label bytes —
/// [`render_stream_item`] (the query response) and [`tail_frame`] (the
/// WebSocket frame). See [`LabelBytes`].
fn render_stream_item_into(
    out: &mut Vec<u8>,
    s: &StreamResult,
    labels: LabelBytes,
    categorize: categorize::Categorize,
) {
    use std::io::Write;
    out.extend_from_slice(b"{\"stream\":");
    match labels {
        LabelBytes::SpaceForReplacementChars => {
            out.extend_from_slice(space_for_replacement_chars(&s.labels_json).as_bytes());
        }
        LabelBytes::Verbatim => out.extend_from_slice(s.labels_json.as_bytes()),
    }
    out.extend_from_slice(b",\"values\":[");
    for (i, (ts, line)) in s.entries.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(b"[\"");
        let _ = write!(&mut *out, "{ts}");
        out.extend_from_slice(b"\",");
        if serde_json::to_writer(&mut *out, line).is_err() {
            out.extend_from_slice(b"\"\"");
        }
        if categorize.is_on() {
            // `decide` only returns `on` when EVERY stream reports
            // `WireArity::Three`, so the index is in range — but a
            // missing element would still render `,{}` rather than a
            // two-element entry inside a body that advertised three.
            match s.categories.get(i) {
                Some(cats) => write_third_element(out, cats),
                None => out.extend_from_slice(THIRD_ELEMENT_EMPTY.as_bytes()),
            }
        }
        out.push(b']');
    }
    out.extend_from_slice(b"]}");
}

/// The chunk one stream contributes to a streamed body — the ONE place
/// the QUERY path allocates a per-item buffer, and byte- and
/// allocation-identical to the pre-#463 `render_stream_item`.
///
/// The `Streams` arm's `stream_array` closure is exactly this call and so
/// is `ac19_render_path_peak_and_allocation_profile`, so the gate drives
/// the shipped body rather than a transcription of it (the
/// `StreamsFastPathProbe` pattern). Exactly two call sites, asserted by
/// the source-scan gate.
fn item_chunk(w: categorize::ItemWriter, s: &StreamResult) -> Vec<u8> {
    let mut chunk: Vec<u8> = Vec::with_capacity(w.estimate(s));
    w.write(&mut chunk, s);
    chunk
}

/// The issue #463 wire-shape decision, and the only way to obtain one.
///
/// The categorised `values` shape is all-or-nothing: a three-element
/// entry in a body that does not advertise `categorize-labels`
/// desynchronises the datasource's streaming decoder, and so does a
/// two-element entry in one that does. So the decision is taken **once,
/// from the data**, and both the advertisement and the per-item renderer
/// are derived from that one value.
mod categorize {
    use super::{
        EntryCategories, LabelBytes, StreamResult, WireArity, render_stream_item_into,
        stream_item_estimate, third_element_estimate,
    };

    /// The token the reference switches on
    /// (`pkg/util/httpreq/encoding_flags.go @ grafana/loki v3.7.4
    /// b318f282`).
    pub(super) const CATEGORIZE_LABELS: &str = "categorize-labels";

    /// Opaque outside this module: the field is private, so no other code
    /// in `encode.rs` can mint one. A renderer that emits a third element
    /// must be HANDED a `Categorize`, and [`decide`] is its only source —
    /// so the predicate has exactly one implementation by construction
    /// rather than by convention.
    ///
    /// **What that proves, and what it does not.** It proves every
    /// `Categorize` value was built by `decide`. It does NOT prove the
    /// effective decision is unique: a caller could compute its own
    /// predicate and use it to doctor `flags` or the stream slice on the
    /// way in. The resulting body is still self-consistent — it
    /// advertises iff it renders three elements — so that residual can
    /// produce a wrongly-flagged body, never one a client cannot parse.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct Categorize(bool);

    impl Categorize {
        pub(super) fn is_on(self) -> bool {
            self.0
        }
    }

    /// `on` iff the request asked for `categorize-labels` **and** every
    /// stream can serve a third element — so an assembly bug downgrades
    /// the whole body to the two-element shape and never desynchronises a
    /// parser.
    ///
    /// `all()` over an empty slice is `true`, so an empty `result` with
    /// the flag advertises it and renders no entries — which is what the
    /// reference does.
    pub(super) fn decide(flags: &[String], streams: &[StreamResult]) -> Categorize {
        Categorize(
            flags.iter().any(|f| f == CATEGORIZE_LABELS)
                && streams.iter().all(|s| s.wire_arity() == WireArity::Three),
        )
    }

    /// The tokens the response echoes, in first-occurrence request order.
    ///
    /// **`categorize-labels` is removed iff the decision came out off.**
    /// Echoing it beside two-element values is exactly the
    /// desynchronisation the whole design exists to prevent, produced by
    /// the downgrade meant to prevent it. Unknown tokens are untouched
    /// and still echoed verbatim, which is what the reference does with
    /// them. The reference cannot enter the state that triggers the
    /// removal, because its decision is the flag alone — a divergence in
    /// a state it has no answer for, and in the safe direction. Ledgered
    /// as `encoding-flags-echo-order`.
    pub(super) fn echoed(
        flags: &[String],
        decision: Categorize,
    ) -> impl Iterator<Item = &str> + '_ {
        flags
            .iter()
            .map(String::as_str)
            .filter(move |f| decision.is_on() || *f != CATEGORIZE_LABELS)
    }

    /// The per-item rendering decision: `Copy`, allocation-free, and
    /// constructible only by [`super::streams_render`], so it cannot
    /// carry a decision the envelope bytes were not built from.
    #[derive(Debug, Clone, Copy)]
    pub(super) struct ItemWriter {
        labels: LabelBytes,
        categorize: Categorize,
    }

    impl ItemWriter {
        pub(super) fn new(labels: LabelBytes, categorize: Categorize) -> Self {
            ItemWriter { labels, categorize }
        }

        pub(super) fn estimate(self, s: &StreamResult) -> usize {
            stream_item_estimate(s) + self.third_element_bytes(s)
        }

        /// What this stream's THIRD ELEMENTS add to the rendered size —
        /// `0` when the decision is off, so a caller that reserves with
        /// its own base formula stays exactly as it was on the plain
        /// path.
        pub(super) fn third_element_bytes(self, s: &StreamResult) -> usize {
            if !self.categorize.is_on() {
                return 0;
            }
            s.entries
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    s.categories
                        .get(i)
                        .map_or(super::THIRD_ELEMENT_EMPTY.len(), third_element_estimate)
                })
                .sum::<usize>()
        }

        pub(super) fn write(self, out: &mut Vec<u8>, s: &StreamResult) {
            render_stream_item_into(out, s, self.labels, self.categorize);
        }
    }

    /// One entry lifted out of its stream, for the categorised tail's
    /// per-entry stream objects.
    pub(super) struct LiftedEntry {
        pub(super) timestamp_ns: i64,
        pub(super) labels_json: String,
        pub(super) fingerprint: u64,
        pub(super) line: String,
        pub(super) categories: EntryCategories,
    }
}

/// Which envelope a streams body is spliced into. The reference places
/// the advertisement differently in each: `resultType, encodingFlags,
/// result, stats` for the query response
/// (`pkg/util/marshal/query.go:301-322 @ grafana/loki v3.7.4 b318f282`)
/// and LAST — after `streams`/`dropped_entries` — for the tail frame
/// (`:231-257`).
///
/// Two variants, and the count is asserted by the source-scan gate: a
/// third response shape carrying log lines has to come through here.
enum StreamsEnvelope<'a> {
    Query {
        stats_json: &'a str,
        explain: Option<&'a PlanExplain>,
        warnings: &'a str,
    },
    Tail {
        dropped: &'a [super::tail::Dropped],
        dropped_total: u64,
    },
}

/// Everything a streams body needs, built in ONE call from ONE decision.
///
/// `prefix` and `suffix` are complete envelope bytes with the
/// advertisement already spliced at the position the envelope dictates —
/// the caller never sees an advertisement it could place, drop or
/// reorder, and the item writer it gets back carries the same decision
/// those bytes were built from.
///
/// **Streaming is preserved (issue #312):** `item` is a `Copy` value, not
/// a boxed closure and not bytes. The query body is still rendered
/// item-by-item by `stream_array`, and nothing here materialises the
/// response.
struct StreamsRender {
    prefix: Vec<u8>,
    suffix: Vec<u8>,
    item: categorize::ItemWriter,
}

/// Builds both halves of a streams body from one [`categorize::decide`].
///
/// Exactly two production call sites, one per [`StreamsEnvelope`]
/// variant — the `Streams` arm of [`query_response_warned`] and
/// [`tail_frame`].
fn streams_render(
    env: StreamsEnvelope<'_>,
    flags: &[String],
    items: &[StreamResult],
    label_bytes: LabelBytes,
) -> StreamsRender {
    use std::io::Write;
    let decision = categorize::decide(flags, items);
    // The echo is an ITERATOR, not a collected list: `categorize-labels`
    // is dropped from it exactly when the decision came out off, and
    // every other token passes through verbatim. See
    // `categorize::echoed`.
    let echoed = || categorize::echoed(flags, decision);
    let advertised = echoed().next().is_some();
    // Exact, not an allowance: `"encodingFlags":[]` is 18 bytes, each
    // token adds its own bytes plus two quotes and a separator comma, and
    // an escape can only shrink the reservation's accuracy in the safe
    // direction (`serde_json` never emits fewer bytes than the input).
    let advertisement_len: usize = if advertised {
        18 + echoed().map(|f| f.len() + 3).sum::<usize>()
    } else {
        0
    };
    /// Writes `"encodingFlags":[…]` — the ONE place the echo is
    /// rendered, so the two envelopes cannot disagree about its bytes.
    fn write_advertisement<'a>(out: &mut Vec<u8>, tokens: impl Iterator<Item = &'a str>) {
        out.extend_from_slice(b"\"encodingFlags\":[");
        for (i, f) in tokens.enumerate() {
            if i > 0 {
                out.push(b',');
            }
            if serde_json::to_writer(&mut *out, f).is_err() {
                out.extend_from_slice(b"\"\"");
            }
        }
        out.push(b']');
    }
    let (prefix, suffix) = match env {
        StreamsEnvelope::Query {
            stats_json,
            explain,
            warnings,
        } => {
            const HEAD: &[u8] = b"{\"status\":\"success\",\"data\":{\"resultType\":\"streams\",";
            let mut prefix: Vec<u8> = Vec::with_capacity(HEAD.len() + advertisement_len + 12);
            prefix.extend_from_slice(HEAD);
            if advertised {
                write_advertisement(&mut prefix, echoed());
                prefix.push(b',');
            }
            prefix.extend_from_slice(b"\"result\":[");
            let suffix = explain_suffix(format!("],\"stats\":{stats_json}"), explain);
            (prefix, format!("{suffix}}}{warnings}}}").into_bytes())
        }
        StreamsEnvelope::Tail {
            dropped,
            dropped_total,
        } => {
            let drops: usize = dropped.iter().map(|d| d.labels_json.len() + 48).sum();
            let mut suffix: Vec<u8> = Vec::with_capacity(64 + drops + advertisement_len);
            suffix.extend_from_slice(b"],\"dropped_entries\":[");
            for (i, d) in dropped.iter().enumerate() {
                if i > 0 {
                    suffix.push(b',');
                }
                suffix.extend_from_slice(b"{\"labels\":");
                suffix.extend_from_slice(d.labels_json.as_bytes());
                suffix.extend_from_slice(b",\"timestamp\":\"");
                let _ = write!(&mut suffix, "{}", d.timestamp_ns);
                suffix.extend_from_slice(b"\"}");
            }
            suffix.extend_from_slice(b"],\"dropped_total\":");
            let _ = write!(&mut suffix, "{dropped_total}");
            if advertised {
                suffix.push(b',');
                write_advertisement(&mut suffix, echoed());
            }
            suffix.push(b'}');
            (b"{\"streams\":[".to_vec(), suffix)
        }
    };
    StreamsRender {
        prefix,
        suffix,
        item: categorize::ItemWriter::new(label_bytes, decision),
    }
}

/// Encodes a `/api/logs/v1/stats` result (issue #74, docs/api.md §2.5):
/// the bare `{"streams","chunks","entries","bytes"}` object (no
/// status/data envelope — the reference wire shape), with `explain`
/// added as a sibling key when requested. Fixed-size — no streaming
/// needed.
pub(crate) fn stats_response(stats: LogStats, explain: Option<PlanExplain>) -> Response {
    let mut body = format!(
        "{{\"streams\":{},\"chunks\":{},\"entries\":{},\"bytes\":{}",
        stats.streams, stats.chunks, stats.entries, stats.bytes
    );
    if let Some(e) = &explain {
        body.push_str(",\"explain\":");
        body.push_str(&explain_json(e));
    }
    body.push('}');
    json_response(Body::from(body))
}

/// Encodes a `/api/logs/v1/volume` result (issue #169, docs/api.md §2.6):
/// the §2.2 vector envelope evaluated at `end_ns`, **order-preserving** —
/// the engine's entries arrive already sorted `(bytes desc, labels asc)`
/// and truncated to `limit`, and that top-N presentation IS the contract,
/// so this deliberately does NOT route through [`query_response`]'s
/// `Vector` arm (which re-sorts by label set and would scramble it).
/// `stats.series` is the PulsusDB-additive key (same clients-ignore-extras
/// precedent as §2.4's `dropped_total`); `explain` joins it inside `data`
/// when requested. `bytes` converts u64 → f64 only here at render (values
/// past 2^53 lose precision exactly as the oracle's own `float64` cast).
pub(crate) fn volume_response(
    entries: Vec<VolumeEntry>,
    end_ns: i64,
    explain: Option<PlanExplain>,
) -> Response {
    let stats = SeriesStats {
        series: entries.len(),
    };
    let stats_json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
    let prefix =
        b"{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[".to_vec();
    let suffix = explain_suffix(format!("],\"stats\":{stats_json}"), explain.as_ref());
    let suffix = format!("{suffix}}}}}").into_bytes();
    json_response(stream_array(
        prefix,
        entries,
        move |e: &VolumeEntry| {
            // Per-item adapter to reuse `render_vector_item` verbatim —
            // the clone is O(limit)-bounded label pairs, never row data.
            let sample = VectorSample {
                labels: e.labels.clone(),
                value: e.bytes as f64,
            };
            render_vector_item(&sample, end_ns)
        },
        suffix,
    ))
}

/// Encodes a `/api/logs/v1/detected_labels` result (issue #170,
/// docs/api.md §2.6): the bare `{"detectedLabels":[...]}` object (the
/// reference wire shape; no status/data envelope), entries already sorted
/// by key (the SQL's `ORDER BY key`), `explain` as a sibling key when
/// requested. Top-level key always present (deterministic-shape
/// divergence from the reference's `omitempty`); no `sketch` (we have no
/// HLL sketch — valid under the reference's own `omitempty` tag).
pub(crate) fn detected_labels_response(
    labels: Vec<DetectedLabelOut>,
    explain: Option<PlanExplain>,
) -> Response {
    let prefix = b"{\"detectedLabels\":[".to_vec();
    let suffix = explain_suffix("]".to_string(), explain.as_ref());
    let suffix = format!("{suffix}}}").into_bytes();
    json_response(stream_array(
        prefix,
        labels,
        |l: &DetectedLabelOut| {
            format!(
                "{{\"label\":{},\"cardinality\":{}}}",
                json_string(&l.label),
                l.cardinality
            )
            .into_bytes()
        },
        suffix,
    ))
}

/// Encodes a `/api/logs/v1/detected_fields` result (issue #170,
/// docs/api.md §2.6): the bare `{"fields":[...],"limit":N}` object,
/// fields already label-sorted by the engine.
///
/// **Scope of the byte-exactness claim**, with both exceptions named so
/// the sentence cannot drift wider than the code:
///  * the zero-field body IS byte-exact;
///  * each per-field OBJECT is byte-exact against the reference's
///    SINGLE-RESPONSE path only — its sharded merge rebuilds fields
///    without `JsonPath` (`pkg/storage/detected/fields.go:92-99 @
///    v3.7.4`), which we deliberately decline to reproduce (registered
///    exception `detected-fields-jsonpath-survives-merge`);
///  * a populated body AS A WHOLE is not, because of ARRAY ORDER: the
///    reference builds its `fields` slice by ranging a Go map at both
///    build sites and never sorts before marshaling
///    (`detected_fields.go:57-75`, `fields.go:54-101`,
///    `pkg/util/marshal/marshal.go:182-188`), and the Go spec withholds
///    any guarantee that order repeats between iterations, so there is
///    no order guaranteed to exist to mirror. We pin label-ascending
///    (registered `detected-fields-array-order-pinned`).
///
/// Both ledger rows are in docs/benchmarks/logs-differential-ledger.md.
/// Nothing below claims order parity.
///
/// Three shapes are the reference's, byte for byte (all captured from
/// `grafana/loki:3.7.4` and recorded on issue #258):
///  * **zero fields is bare `{}`** (issue #258) — `fields` carries
///    `[(gogoproto.jsontag) = "fields,omitempty"]` so a nil slice
///    disappears, and `limit` is only ever assigned inside
///    `if len(fields) > 0 || len(values) > 0`
///    (`pkg/logproto/logproto.proto:470-472`,
///    `pkg/querier/queryrange/detected_fields.go:85-87 @ v3.7.4`). `limit`
///    is CORRECT alongside a populated `fields`, so only the empty case
///    changes;
///  * **`parsers` is `null`, not `[]`, when unattributed** — the jsontag
///    is a bare `"parsers"` (NO `omitempty`, so the key is always
///    present) and the handler explicitly maps the empty slice to nil
///    before marshaling (`logproto.proto:481`,
///    `detected_fields.go:64-66`);
///  * **`jsonPath`** (issue #254) is emitted for a json-flattened field
///    and OMITTED otherwise — `[(gogoproto.jsontag) =
///    "jsonPath,omitempty"]` (`logproto.proto:483`).
///
/// `pulsus_partial: true` is the additive #90-convention
/// not-the-complete-answer signal — budget-truncated sampling OR (issue
/// #244) a retention-capped cardinality — **omitted when false** so
/// complete responses stay byte-identical to the reference shape;
/// `explain` joins as a sibling key when requested. Both additive keys
/// survive into the zero-field body, which is otherwise `{}`.
pub(crate) fn detected_fields_response(
    out: DetectedFields,
    limit: u32,
    explain: Option<PlanExplain>,
) -> Response {
    let partial = out.truncated || out.retention_capped;
    if out.fields.is_empty() {
        // The reference's zero-field body carries NEITHER key; ours adds
        // only the two documented additive ones, in the same order the
        // populated body places them.
        let mut body = String::from("{");
        if partial {
            body.push_str("\"pulsus_partial\":true");
        }
        if let Some(e) = explain.as_ref() {
            if body.len() > 1 {
                body.push(',');
            }
            body.push_str("\"explain\":");
            body.push_str(&explain_json(e));
        }
        body.push('}');
        return json_response(Body::from(body));
    }
    let prefix = b"{\"fields\":[".to_vec();
    let mut tail = format!("],\"limit\":{limit}");
    if partial {
        tail.push_str(",\"pulsus_partial\":true");
    }
    let suffix = explain_suffix(tail, explain.as_ref());
    let suffix = format!("{suffix}}}").into_bytes();
    json_response(stream_array(
        prefix,
        out.fields,
        |f: &DetectedFieldOut| {
            let mut item = String::new();
            item.push_str("{\"label\":");
            item.push_str(&json_string(&f.label));
            item.push_str(",\"type\":");
            item.push_str(&json_string(f.field_type));
            item.push_str(&format!(",\"cardinality\":{}", f.cardinality));
            item.push_str(",\"parsers\":");
            push_json_string_array(&mut item, &f.parsers, JsonStringArray::NullWhenEmpty);
            if let Some(path) = f.json_path.as_deref().filter(|p| !p.is_empty()) {
                item.push_str(",\"jsonPath\":");
                push_json_string_array(&mut item, path, JsonStringArray::AlwaysArray);
            }
            item.push('}');
            item.into_bytes()
        },
        suffix,
    ))
}

/// How [`push_json_string_array`] renders the EMPTY case — the reference
/// distinguishes a nil slice from an empty one on the wire, and
/// `/detected_fields` needs both spellings in the same object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonStringArray {
    /// Go's nil-slice marshaling: `null` rather than `[]`.
    NullWhenEmpty,
    /// Always the array (never reached for an `omitempty` key, whose
    /// caller omits it instead).
    AlwaysArray,
}

fn push_json_string_array<S: AsRef<str>>(out: &mut String, items: &[S], empty: JsonStringArray) {
    if items.is_empty() && empty == JsonStringArray::NullWhenEmpty {
        out.push_str("null");
        return;
    }
    out.push('[');
    for (i, s) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&json_string(s.as_ref()));
    }
    out.push(']');
}

/// Encodes a `/api/logs/v1/patterns` result (M7-C3, issue #171, docs/api.md
/// §2.6): the Loki-interop envelope `{"status":"success","data":[{"pattern":
/// "<_> ...","samples":[[<unix_seconds>,<count>],...]}]}`. `data` is
/// **order-preserving** — the engine returns series already ordered
/// total-count-desc then pattern-asc (the pushed-down top-1000), and that
/// presentation IS the contract, so this deliberately does NOT re-sort.
/// `samples` are ascending by second, zero-count steps already omitted; both
/// elements are bare JSON integers. `explain` joins as a sibling of `data`
/// when requested.
pub(crate) fn patterns_response(
    series: Vec<PatternSeries>,
    explain: Option<PlanExplain>,
) -> Response {
    let prefix = b"{\"status\":\"success\",\"data\":[".to_vec();
    let suffix = explain_suffix("]".to_string(), explain.as_ref());
    let suffix = format!("{suffix}}}").into_bytes();
    json_response(stream_array(
        prefix,
        series,
        |s: &PatternSeries| {
            let mut samples = String::new();
            for (i, (secs, count)) in s.samples.iter().enumerate() {
                if i > 0 {
                    samples.push(',');
                }
                samples.push_str(&format!("[{secs},{count}]"));
            }
            format!(
                "{{\"pattern\":{},\"samples\":[{samples}]}}",
                json_string(&s.pattern)
            )
            .into_bytes()
        },
        suffix,
    ))
}

/// Encodes one live-tail WebSocket frame (issue #74, docs/api.md §2.4):
/// `{"streams":[...],"dropped_entries":[...],"dropped_total":N}`.
/// `dropped_entries` is the bounded representative sample of evicted
/// rows (`{"labels":{...},"timestamp":"<ns>"}` — labels spliced verbatim
/// from the stream's canonical JSON, ns as a string like stream values);
/// `dropped_total` is the EXACT cumulative drop count since the last
/// emitted frame — the documented additive field, so counts are never
/// lost while the array never grows unbounded. Streams sort by
/// `(labels_json, fingerprint)` for a deterministic wire order,
/// mirroring [`query_response`].
pub(crate) fn tail_frame(
    streams: Vec<StreamResult>,
    dropped: &[super::tail::Dropped],
    dropped_total: u64,
    encoding_flags: &[String],
) -> String {
    // Issue #463: under `categorize-labels` the tail emits ONE stream
    // object per entry, in timestamp order — see [`split_tail_entries`].
    // Splitting preserves every stream's `Three` arity (one entry, one
    // category), so the decision `streams_render` re-takes below is the
    // same one taken here.
    let mut streams = streams;
    if categorize::decide(encoding_flags, &streams).is_on() {
        streams = split_tail_entries(streams);
    } else {
        streams
            .sort_by(|a, b| (&a.labels_json, a.fingerprint).cmp(&(&b.labels_json, b.fingerprint)));
    }
    let render = streams_render(
        StreamsEnvelope::Tail {
            dropped,
            dropped_total,
        },
        encoding_flags,
        &streams,
        // `LabelBytes::Verbatim` (issue #455): the tail frame's label
        // bytes are UNCHANGED by that issue. Its evidence is entirely
        // `/loki/api/v1/query_range`; this route was never measured
        // against the reference, so it keeps what it served before.
        LabelBytes::Verbatim,
    );
    // Pre-size the output (review round 1, low): labels + entry bodies
    // dominate; ~48 bytes covers per-entry/per-drop JSON scaffolding.
    //
    // **The per-stream term is the pre-#463 allowance, unchanged**, so
    // the plain frame reserves exactly what it always did and its
    // per-stream peak slope does not move (criterion 16(a)). The two
    // envelope halves are already rendered, so their exact lengths
    // replace the old fixed 64 plus per-drop allowance — an INTERCEPT
    // change, which that criterion permits — and the categorised third
    // elements are added on top, which is what keeps the flagged frame
    // from regrowing on bytes it knew it was going to write.
    let capacity: usize = render.prefix.len()
        + render.suffix.len()
        + streams
            .iter()
            .map(|s| {
                s.labels_json.len()
                    + 32
                    + s.entries
                        .iter()
                        .map(|(_, line)| line.len() + 48)
                        .sum::<usize>()
                    + render.item.third_element_bytes(s)
            })
            .sum::<usize>();
    let mut buf: Vec<u8> = Vec::with_capacity(capacity);
    buf.extend_from_slice(&render.prefix);
    for (i, s) in streams.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        // Rendered IN PLACE (issue #312): no per-item `String` copy, and
        // no whole-item temporary — the tail cannot stream, so it writes
        // straight into the frame buffer.
        render.item.write(&mut buf, s);
    }
    buf.extend_from_slice(&render.suffix);
    String::from_utf8(buf).expect("rendered JSON is UTF-8")
}

/// The categorised tail's per-entry stream objects (issue #463).
///
/// **Measured against the reference, header on AND off:** two streams
/// differing in one label with entries interleaved in time — prod@t,
/// staging@t+1, prod@t+2 — come back as THREE stream objects in strict
/// timestamp order, the prod map appearing twice with staging between.
/// The mechanism is `pkg/querier/tail/tail.go:114-125 @ grafana/loki
/// v3.7.4 b318f282`, which appends one `logproto.Stream` per entry as it
/// pops the oldest from the merge iterator.
///
/// A renderer that grouped by label set would emit two objects and the
/// consumer — object loop outside, value loop inside — would render the
/// rows `A1, A3, B2`. Log lines out of order in a tail view is a defect,
/// not a verbosity trade, so the categorised path matches the reference
/// exactly.
///
/// **The non-categorised tail is deliberately NOT changed here**: it
/// carries the same pre-existing divergence, it is byte-frozen by every
/// pre-#463 tail expectation, and it is owned by issue #469. Ledgered as
/// `tail-stream-object-granularity-unflagged`.
///
/// The sort key is `(timestamp_ns, labels_json, fingerprint,
/// entry_index)`. The timestamp is the reference's own order; the rest is
/// our deterministic tiebreak, because the reference's tie order is its
/// merge tree's arrival order and is not reproducible from the data.
/// `entry_index` is carried by `sort_by`'s STABILITY — entries are lifted
/// in per-stream order, so a full three-way tie keeps it.
fn split_tail_entries(streams: Vec<StreamResult>) -> Vec<StreamResult> {
    // Already one entry per object — the shape a tail poll usually
    // produces, and the shape the split's own output has. Reorder in
    // place: no lift, no rebuild, and no per-entry allocation, so the
    // categorised frame's allocation profile equals the plain one's
    // (criterion 16(b-i)). `sort_by` is stable, so a full key tie keeps
    // the caller's order exactly as the general path's `entry_index`
    // tiebreak does.
    if streams.iter().all(|s| s.entries.len() == 1) {
        let mut streams = streams;
        streams.sort_by(|a, b| {
            (a.entries[0].0, &a.labels_json, a.fingerprint).cmp(&(
                b.entries[0].0,
                &b.labels_json,
                b.fingerprint,
            ))
        });
        return streams;
    }
    let mut lifted: Vec<categorize::LiftedEntry> = Vec::new();
    for s in streams {
        let StreamResult {
            fingerprint,
            service: _,
            labels_json,
            entries,
            categories,
        } = s;
        let mut cats = categories.into_iter();
        for (ts, line) in entries {
            lifted.push(categorize::LiftedEntry {
                timestamp_ns: ts,
                labels_json: labels_json.clone(),
                fingerprint,
                line,
                categories: cats.next().unwrap_or_default(),
            });
        }
    }
    lifted.sort_by(|a, b| {
        (a.timestamp_ns, &a.labels_json, a.fingerprint).cmp(&(
            b.timestamp_ns,
            &b.labels_json,
            b.fingerprint,
        ))
    });
    lifted
        .into_iter()
        .map(|e| StreamResult {
            fingerprint: e.fingerprint,
            // Not rendered on this surface; the frame carries labels and
            // values only.
            service: String::new(),
            labels_json: e.labels_json,
            entries: vec![(e.timestamp_ns, e.line)],
            categories: vec![e.categories],
        })
        .collect()
}

fn render_matrix_item(s: &MatrixSeries) -> Vec<u8> {
    let mut points = String::new();
    for (i, (step_ns, value)) in s.points.iter().enumerate() {
        if i > 0 {
            points.push(',');
        }
        points.push_str(&format!(
            "[{},{}]",
            format_unix_seconds(*step_ns),
            format_value_json(*value)
        ));
    }
    format!(
        "{{\"metric\":{},\"values\":[{}]}}",
        labels_object_json(&s.labels),
        points
    )
    .into_bytes()
}

fn render_vector_item(s: &VectorSample, at_ns: i64) -> Vec<u8> {
    format!(
        "{{\"metric\":{},\"value\":[{},{}]}}",
        labels_object_json(&s.labels),
        format_unix_seconds(at_ns),
        format_value_json(s.value)
    )
    .into_bytes()
}

fn explain_suffix(mut suffix: String, explain: Option<&PlanExplain>) -> String {
    if let Some(e) = explain {
        suffix.push_str(",\"explain\":");
        suffix.push_str(&explain_json(e));
    }
    suffix
}

/// Issue #277: the `warnings` envelope suffix — a TOP-LEVEL sibling of
/// `data`, appended AFTER `data`'s own closing brace and never nested
/// inside it (unlike [`explain_suffix`], whose `explain` key lives
/// *inside* `data`).
///
/// **Warnings come LAST, after `data`** — captured from the pinned
/// reference, not read off its encoder. `pkg/util/marshal/query.go:201-228`
/// (`EncodeResult`) writes `status`, `warnings`, `data`, and that is NOT
/// the encoder a user's query reaches: the query-frontend path serialises
/// `queryrangebase.PrometheusResponse`
/// (`pkg/querier/queryrange/queryrangebase/queryrange.proto:36-46` —
/// `Status`=1, `Data`=2, … `Warnings`=6 `json:"warnings,omitempty"`), so
/// the captured body puts `warnings` after `data`:
///
/// ```text
/// {"status":"success","data":{…},"warnings":["maximum of series (500) reached for variant (0)"]}
/// ```
///
/// Empty ⇒ zero bytes (`omitempty`), so every non-variants response stays
/// byte-identical.
fn warnings_suffix(warnings: &Warnings) -> String {
    if warnings.is_empty() {
        return String::new();
    }
    let mut s = String::from(",\"warnings\":[");
    for (i, w) in warnings.as_strings().iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&json_string(w));
    }
    s.push(']');
    s
}

/// [`query_response_warned`] with no warnings.
///
// Test-only since issue #277: the production caller
// (`handlers::run_query`) passes the engine's accumulated warnings
// through `query_response_warned`. Kept as a thin convenience so every
// pre-existing byte-exact envelope test is untouched — an empty
// `Warnings` renders zero extra bytes, which is exactly the property
// `empty_warnings_leaves_every_query_response_arm_byte_identical` pins.
#[cfg(test)]
pub(crate) fn query_response(
    result: QueryResult,
    explain: Option<PlanExplain>,
    at_ns: i64,
    preserve_vector_order: bool,
) -> Response {
    query_response_warned(
        result,
        explain,
        at_ns,
        preserve_vector_order,
        &Warnings::new(),
        &[],
    )
}

/// Encodes a `query`/`query_range` result: `data.resultType`/`result`/
/// `stats`(/`explain`) per docs/api.md §2.1/§2.2, plus the top-level
/// `warnings` array (issue #277) **after** `data`. `at_ns` is the instant
/// evaluation time (`/query`'s `time` param) — only read when `result` is
/// [`QueryResult::Vector`] (never produced by a `Range` spec, so
/// `query_range` callers may pass any placeholder).
///
/// `preserve_vector_order` (issue M8-LQ3, mirrors the PromQL `ordered`
/// gate at `prom_api::encode::query_response`): `true` for a terminal
/// `sort`/`sort_desc` **instant** query, so the engine's value order
/// survives on the wire instead of being clobbered by the default label
/// re-sort. It applies to the Vector arm ONLY — the Matrix/Streams arms
/// keep their deterministic label-sort (a range `sort(...)` yields a
/// matrix with no per-series value order, so no HashMap nondeterminism
/// reaches the wire).
///
/// `warnings` renders zero bytes when empty, so every non-variants
/// response is byte-identical to the pre-#277 encoder.
pub(crate) fn query_response_warned(
    result: QueryResult,
    explain: Option<PlanExplain>,
    at_ns: i64,
    preserve_vector_order: bool,
    warnings: &Warnings,
    encoding_flags: &[String],
) -> Response {
    let warns = warnings_suffix(warnings);
    match result {
        QueryResult::Streams { mut items, partial } => {
            items.sort_by(|a, b| {
                (&a.labels_json, a.fingerprint).cmp(&(&b.labels_json, b.fingerprint))
            });
            let stats = StreamStats {
                streams: items.len(),
                entries: items.iter().map(|s| s.entries.len()).sum(),
                bytes: items
                    .iter()
                    .flat_map(|s| s.entries.iter())
                    .map(|(_, line)| line.len())
                    .sum(),
                pulsus_partial: partial,
            };
            let stats_json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
            let StreamsRender {
                prefix,
                suffix,
                item,
            } = streams_render(
                StreamsEnvelope::Query {
                    stats_json: &stats_json,
                    explain: explain.as_ref(),
                    warnings: &warns,
                },
                encoding_flags,
                &items,
                // The QUERY response is the surface issue #455 measured,
                // so the one that substitutes.
                LabelBytes::SpaceForReplacementChars,
            );
            json_response(stream_array(
                prefix,
                items,
                move |s: &StreamResult| item_chunk(item, s),
                suffix,
            ))
        }
        QueryResult::Matrix(mut items) => {
            items.sort_by(|a, b| a.labels.cmp(&b.labels));
            let stats = SeriesStats {
                series: items.len(),
            };
            let stats_json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
            let prefix =
                b"{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":["
                    .to_vec();
            let suffix = explain_suffix(format!("],\"stats\":{stats_json}"), explain.as_ref());
            let suffix = format!("{suffix}}}{warns}}}").into_bytes();
            json_response(stream_array(prefix, items, render_matrix_item, suffix))
        }
        QueryResult::Vector(mut items) => {
            // Skip the label re-sort for a terminal sort/sort_desc instant
            // query so the engine's value order reaches the client.
            if !preserve_vector_order {
                items.sort_by(|a, b| a.labels.cmp(&b.labels));
            }
            let stats = SeriesStats {
                series: items.len(),
            };
            let stats_json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
            let prefix =
                b"{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":["
                    .to_vec();
            let suffix = explain_suffix(format!("],\"stats\":{stats_json}"), explain.as_ref());
            let suffix = format!("{suffix}}}{warns}}}").into_bytes();
            json_response(stream_array(
                prefix,
                items,
                move |s: &VectorSample| render_vector_item(s, at_ns),
                suffix,
            ))
        }
        // Issue #31: `pulsus_promql::QueryValue::Scalar` (a bare-number
        // PromQL expression, e.g. `1 + 1`) — docs/api.md §2.1's documented
        // `"resultType":"scalar"` shape. No streaming needed (a single
        // value, unlike the O(series) result arrays above); `pulsus-server`
        // does not yet wire `MetricsEngine` into a route (that is #32), so
        // this arm is unreachable from any request today, but keeps
        // `QueryResult` matches exhaustive and correct for when it lands.
        QueryResult::Scalar(v) => {
            let body = explain_suffix(
                format!(
                    "{{\"status\":\"success\",\"data\":{{\"resultType\":\"scalar\",\"result\":[{},{}]}}",
                    format_unix_seconds(at_ns),
                    format_value_json(v)
                ),
                explain.as_ref(),
            );
            json_response(Body::from(format!("{body}{warns}}}")))
        }
        // Unreachable: `QueryResult::String` is a PromQL-only variant of
        // the shared enum (issue #86; a top-level string-literal metrics
        // query) — LogQL has no string result type at all. Kept as a
        // well-formed error response rather than a panic, mirroring
        // `prom_api::encode`'s own handling of the LogQL-only
        // `QueryResult::Streams` variant.
        // Issue #264: rendered through the same plain-text writer as every
        // other logs-surface error, so the surface never speaks two error
        // containers.
        QueryResult::String(_) => super::error::plain_text_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "unexpected string result from a logs query".to_string(),
        ),
        // Unreachable: `QueryResult::VectorHist`/`MatrixHist` (M7-A5b-i)
        // are PromQL metrics-only variants (native-histogram results) —
        // LogQL never fetches `metric_hist_samples` and so never produces
        // one. Mirrors the `String` arm's well-formed-error-over-panic
        // precedent.
        QueryResult::VectorHist(_) | QueryResult::MatrixHist(_) => super::error::plain_text_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "unexpected histogram result from a logs query".to_string(),
        ),
    }
}

/// Encodes a `labels`/`label/{name}/values` result: `{"status":"success",
/// "data":["name1",...]}`, `explain` as a top-level sibling of `data` when
/// requested (docs/api.md §2.3).
pub(crate) fn string_array_response(items: Vec<String>, explain: Option<PlanExplain>) -> Response {
    let prefix = b"{\"status\":\"success\",\"data\":[".to_vec();
    let suffix = explain_suffix("]".to_string(), explain.as_ref());
    let suffix = format!("{suffix}}}").into_bytes();
    json_response(stream_array(
        prefix,
        items,
        |s: &String| json_string(s).into_bytes(),
        suffix,
    ))
}

/// Encodes a `series` result: `{"status":"success","data":[{k:v...},...]}`.
/// `items` are already-canonical label-set JSON object strings (from
/// `LogQlEngine::series`) — spliced verbatim, never re-parsed/re-encoded
/// (matches `pulsus-read::exec`'s own "never re-encode a response" design
/// note).
pub(crate) fn json_array_response(items: Vec<String>, explain: Option<PlanExplain>) -> Response {
    let prefix = b"{\"status\":\"success\",\"data\":[".to_vec();
    let suffix = explain_suffix("]".to_string(), explain.as_ref());
    let suffix = format!("{suffix}}}").into_bytes();
    json_response(stream_array(
        prefix,
        items,
        |s: &String| s.clone().into_bytes(),
        suffix,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Read;

    use axum::Router;
    use axum::body::to_bytes;
    use axum::http::Request;
    use axum::routing::get;
    use flate2::read::GzDecoder;
    use pulsus_read::RoutingDecision;
    use tower::ServiceExt;
    use tower_http::compression::CompressionLayer;

    async fn body_string(res: Response) -> String {
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        String::from_utf8(bytes.to_vec()).expect("utf8")
    }

    fn stream(fp: u64, labels_json: &str, entries: Vec<(i64, &str)>) -> StreamResult {
        StreamResult {
            fingerprint: fp,
            service: "checkout".to_string(),
            labels_json: labels_json.to_string(),
            entries: entries
                .into_iter()
                .map(|(ts, line)| (ts, line.to_string()))
                .collect(),
            categories: Vec::new(),
        }
    }

    fn probe312_item(n: usize, line_len: usize, ctrl: bool) -> StreamResult {
        let ch = if ctrl { '\u{1}' } else { 'a' };
        let line: String = std::iter::repeat_n(ch, line_len).collect();
        StreamResult {
            fingerprint: 1,
            service: "svc".to_string(),
            labels_json: r#"{"service_name":"svc"}"#.to_string(),
            entries: (0..n)
                .map(|i| (1_700_000_000_000_000_000i64 + i as i64, line.clone()))
                .collect(),
            categories: Vec::new(),
        }
    }

    /// The QUERY path's production [`categorize::ItemWriter`], obtained
    /// the way the shipped `Streams` arm obtains it — through a real
    /// [`streams_render`] call — so a gate that measures through it
    /// measures the decision production makes, not a transcription of it
    /// (issue #463).
    fn query_item_writer(flags: &[String], items: &[StreamResult]) -> categorize::ItemWriter {
        streams_render(
            StreamsEnvelope::Query {
                stats_json: "{}",
                explain: None,
                warnings: "",
            },
            flags,
            items,
            LabelBytes::SpaceForReplacementChars,
        )
        .item
    }

    /// AC-19 (issue #312): the render path's PEAK LIVE BYTES and its
    /// allocation profile, both pinned, both measured over the whole call.
    /// Peak bytes is the quantity #312 guarantees; the count is a companion
    /// that is blind to allocation SIZE (round 6's compensating pair).
    #[test]
    fn ac19_render_path_peak_and_allocation_profile() {
        let benign = probe312_item(200, 512, false);
        let est = stream_item_estimate(&benign);
        // Issue #463 repoints this from the deleted `render_stream_item`
        // onto `item_chunk`, whose operations are the same ones in the
        // same order — and the writer comes from a real `streams_render`,
        // so this measures the shipped decision.
        let w = query_item_writer(&[], std::slice::from_ref(&benign));
        let (out, allocs, peak) = crate::probe_alloc::measure(|| item_chunk(w, &benign));
        // PEAK BYTES is the load-bearing figure: #312 guarantees a memory
        // bound, and peak live bytes is that quantity in its own unit. The
        // allocation COUNT is a companion — cheap and structural, but blind
        // to SIZE, so a compensating pair (add an allocation, remove a
        // doubling) preserves it while moving the peak.
        //
        // benign: one allocation of exactly the reservation, nothing else
        // live, so peak == reservation.
        assert_eq!(
            (allocs, peak, out.len(), out.capacity()),
            (1, est as u64, 107_844, est),
            "the unescaped render's allocation profile moved"
        );

        let adversarial = probe312_item(200, 512, true);
        let est_a = stream_item_estimate(&adversarial);
        let w_a = query_item_writer(&[], std::slice::from_ref(&adversarial));
        let (out_a, allocs_a, peak_a) =
            crate::probe_alloc::measure(|| item_chunk(w_a, &adversarial));

        // adversarial: reservation is the UNESCAPED size and each 0x01 byte
        // renders as a six-character escape, so the buffer doubles until it
        // fits: 108,045 -> 216,090 -> 432,180 -> 864,360 >= 619,844. Three
        // doublings, four allocations, final capacity 8x the reservation,
        // and a peak of 432,180 + 864,360 = 1,296,540 at the last realloc,
        // where both blocks are live for the copy.
        assert_eq!(
            (allocs_a, peak_a, out_a.len(), out_a.capacity()),
            (4, 1_296_540, 619_844, est_a * 8),
            "the escaped-line render's allocation profile moved"
        );

        eprintln!("AC19 benign=({allocs},peak={peak}) adversarial=({allocs_a},peak={peak_a})");
    }

    /// Drives a REAL `StreamAccumulator` over `bodies` and returns
    /// `(items, charged)` — the shipped ledger's own figure, never one
    /// recomputed by the test.
    fn probe312_accumulate(bodies: &[String]) -> (Vec<StreamResult>, u64) {
        use pulsus_read::logql::exec::StreamAccumulator;
        use pulsus_read::logql::pipeline::CompiledPipeline;
        use pulsus_read::logql::rows::{SampleRow, StreamMetaRow};

        let meta = std::collections::HashMap::from([(
            1u64,
            StreamMetaRow {
                fingerprint: 1,
                service: "svc".to_string(),
                labels: r#"{"service_name":"svc"}"#.to_string(),
            },
        )]);
        let pulsus_logql::Expr::Log(log) = pulsus_logql::parse(r#"{a="b"}"#).expect("parse") else {
            panic!("expected a log query");
        };
        let compiled = CompiledPipeline::compile(&log.pipeline).expect("compile");
        let mut acc = StreamAccumulator::new(&meta, u32::MAX);
        for (i, body) in bodies.iter().enumerate() {
            acc.push_row(
                SampleRow {
                    fingerprint: 1,
                    timestamp_ns: 1_700_000_000_000_000_000i64 + i as i64,
                    body: body.clone(),
                    structured_metadata: String::new(),
                },
                &compiled,
            )
            .expect("well inside the shipped cap");
        }
        acc.flush_chunk(&compiled).expect("well inside the cap");
        let charged = acc.charged();
        (acc.into_streams(), charged)
    }

    /// AC-4 (issue #312) — the internal ledger and the WIRE cannot drift
    /// apart: `stats.bytes <= charged <= MAX_STREAMS_RESULT_BYTES` for a
    /// served streams response.
    ///
    /// A break test, not a formula check: the corpus goes through a real
    /// `StreamAccumulator`, the response through the PRODUCTION encoder
    /// (`query_response_warned` — `query_response` has been `#[cfg(test)]`
    /// since #277), and `data.stats.bytes` is parsed back off the
    /// serialized body. Bodies straddle the 32-byte `MIN_ALLOC_BYTES`
    /// floor in both directions, so a charge that stopped pricing the line
    /// is visible.
    #[tokio::test]
    async fn wire_stats_bytes_never_exceeds_the_charged_ledger() {
        let bodies: Vec<String> = (0..400)
            .map(|i| "x".repeat(if i % 4 == 0 { 3 } else { 40 + i }))
            .collect();
        let (items, charged) = probe312_accumulate(&bodies);
        assert!(!items.is_empty(), "the fixture must produce output");

        let res = query_response_warned(
            QueryResult::Streams {
                items,
                partial: false,
            },
            None,
            0,
            false,
            &Warnings::new(),
            &[],
        );
        let body = body_string(res).await;
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON body");
        let stats_bytes = parsed["data"]["stats"]["bytes"]
            .as_u64()
            .expect("data.stats.bytes is a number");

        assert!(
            stats_bytes > 0 && stats_bytes <= charged,
            "stats.bytes = {stats_bytes} exceeds the charged ledger {charged} — the internal \
             ledger and the wire have drifted apart"
        );
        assert!(
            charged <= pulsus_read::logql::MAX_STREAMS_RESULT_BYTES,
            "a served response charged {charged}, past the shipped cap"
        );
    }

    /// AC-17 (issue #312) — the DOCUMENTED encoded-body factor is the one
    /// the renderer produces.
    ///
    /// The factor is parsed out of the committed ledger at RUN TIME (the
    /// `the_docs_quote_the_shipped_bound` precedent), so breaking the
    /// DOCUMENT alone fails this test with no recompilation. Two-sided:
    /// the documented factor must not be too small (the renderer stays
    /// under it) and must not be too large (this corpus is still the worst
    /// case, so one less multiple would not hold).
    #[test]
    fn the_encoder_body_factor_matches_what_the_renderer_produces() {
        let item = probe312_item(200, 512, true);
        // `charged` comes from the real accumulator, not from a formula
        // written here.
        let bodies: Vec<String> = item.entries.iter().map(|(_, l)| l.clone()).collect();
        let (_items, charged) = probe312_accumulate(&bodies);
        let rendered =
            item_chunk(query_item_writer(&[], std::slice::from_ref(&item)), &item).len() as u64;

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let ledger =
            std::fs::read_to_string(root.join("docs/benchmarks/logs-differential-ledger.md"))
                .expect("ledger readable");
        // The anchor line, verbatim:
        //   - **Encoded-body factor:** a rendered stream item is at most
        //     `3 ×` its charged bytes.
        const ANCHOR: &str = "- **Encoded-body factor:** a rendered stream item is at most `";
        let tail = ledger
            .split(ANCHOR)
            .nth(1)
            .expect("the ledger's streams-result-budget entry carries the encoded-body anchor");
        let factor: u64 = tail
            .split(" ×`")
            .next()
            .expect("the anchor names a factor")
            .trim()
            .parse()
            .expect("the documented factor is an integer");

        assert!(
            rendered <= factor * charged,
            "the renderer emitted {rendered} B for {charged} charged B — MORE than the \
             documented factor {factor}x ({} B). The documented bound is too small.",
            factor * charged
        );
        assert!(
            rendered > (factor - 1) * charged,
            "the renderer emitted only {rendered} B for {charged} charged B — under {}x. The \
             documented factor {factor}x is not tight, or this corpus is no longer the worst \
             case.",
            factor - 1
        );
        eprintln!(
            "AC17 rendered={rendered} charged={charged} ratio={:.3} documented_factor={factor}",
            rendered as f64 / charged as f64
        );
    }

    #[tokio::test]
    async fn streams_envelope_is_byte_exact_for_a_single_stream() {
        let result = QueryResult::Streams {
            items: vec![stream(
                1,
                r#"{"env":"prod","service_name":"checkout"}"#,
                vec![(100, "hello"), (200, "world")],
            )],
            partial: false,
        };
        let res = query_response(result, None, 0, false);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"streams","result":[{"stream":{"env":"prod","service_name":"checkout"},"values":[["100","hello"],["200","world"]]}],"stats":{"streams":1,"entries":2,"bytes":10}}}"#
        );
    }

    #[tokio::test]
    async fn streams_stats_omit_pulsus_partial_when_not_budget_truncated() {
        // Issue #90 Delta 2: an ordinary (complete) streams result carries
        // NO `pulsus_partial` field — byte-identical to pre-#90.
        let result = QueryResult::Streams {
            items: vec![stream(1, r#"{"service_name":"a"}"#, vec![(1, "x")])],
            partial: false,
        };
        let body = body_string(query_response(result, None, 0, false)).await;
        assert!(
            !body.contains("pulsus_partial"),
            "complete result must not carry the partial signal: {body}"
        );
    }

    #[tokio::test]
    async fn streams_stats_carry_pulsus_partial_true_on_budget_truncation() {
        // Issue #90 Delta 2: a budget-truncated fetch-until-limit result
        // signals incompleteness via `stats.pulsus_partial=true`,
        // distinguishable from a genuinely-exhausted complete result.
        let result = QueryResult::Streams {
            items: vec![stream(1, r#"{"service_name":"a"}"#, vec![(1, "x")])],
            partial: true,
        };
        let body = body_string(query_response(result, None, 0, false)).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["stats"]["pulsus_partial"], true);
    }

    /// Issue #455 — a U+FFFD in a stream LABEL becomes one space on the
    /// wire; a U+FFFD in a LOG LINE does not.
    ///
    /// Both halves in one response, because a fix that maps the
    /// replacement character everywhere satisfies the first half alone.
    /// The reference leaves lines untouched (`NewStreams` maps
    /// `stream.Labels` only, `pkg/util/marshal/query.go:92-93 @ v3.7.4`),
    /// and `x<U+FFFD>y` comes back as `78 ef bf bd 79` from both sides.
    #[tokio::test]
    async fn stream_labels_map_replacement_chars_to_spaces_and_lines_do_not() {
        let result = QueryResult::Streams {
            items: vec![stream(
                1,
                "{\"k\":\"\u{FFFD}\u{FFFD}\",\"service_name\":\"a\"}",
                vec![(1, "x\u{FFFD}y")],
            )],
            partial: false,
        };
        let body = body_string(query_response(result, None, 0, false)).await;
        assert!(
            body.contains("\"stream\":{\"k\":\"  \",\"service_name\":\"a\"}"),
            "each U+FFFD in a label value is ONE space: {body}"
        );
        assert!(
            body.contains("[\"1\",\"x\u{FFFD}y\"]"),
            "the log line keeps its U+FFFD: {body}"
        );
        // Stated in bytes as well as in characters: two spaces, and the
        // line's three-byte encoding intact.
        let raw = body.as_bytes();
        assert!(
            raw.windows(2).any(|w| w == b"  "),
            "two 0x20 bytes in the label: {body}"
        );
        assert_eq!(
            body.matches('\u{FFFD}').count(),
            1,
            "exactly one U+FFFD survives, and it is the line's: {body}"
        );
    }

    /// Issue #455 — **the tail frame's stream label bytes are UNCHANGED
    /// by that issue**, and this is what keeps them that way.
    ///
    /// `render_stream_item_into` is shared between the query response and
    /// [`tail_frame`]. The substitution went in at the shared renderer and
    /// leaked onto `/api/logs/v1/tail` and its `/loki/api/v1/tail` alias —
    /// measured through a real WebSocket frame, `"k":"<U+FFFD>"` became
    /// `"k":" "`. Every measurement on #455 is a `query_range`
    /// measurement; nothing about the tail surface was established, so it
    /// keeps its own bytes until something measures it.
    ///
    /// The two halves are one claim: the SAME `StreamResult` must render
    /// with a space through the query response and with `ef bf bd`
    /// through the tail frame. Asserting only the second would pass on an
    /// encoder that substitutes nowhere.
    #[test]
    fn tail_frames_keep_their_stream_label_bytes_verbatim() {
        let item = stream(
            1,
            "{\"k\":\"\u{FFFD}\",\"service_name\":\"a\"}",
            vec![(1, "line")],
        );

        let frame = tail_frame(vec![item.clone()], &[], 0, &[]);
        assert!(
            frame.contains("\"stream\":{\"k\":\"\u{FFFD}\",\"service_name\":\"a\"}"),
            "the tail frame splices the label JSON verbatim: {frame}"
        );
        assert!(
            !frame.contains("\"k\":\" \""),
            "the query-response substitution must not reach the tail frame: {frame}"
        );

        // The same item through the query response DOES substitute, so a
        // renderer that simply stopped substituting cannot pass this.
        let rendered = String::from_utf8(item_chunk(
            query_item_writer(&[], std::slice::from_ref(&item)),
            &item,
        ))
        .expect("utf8");
        assert!(
            rendered.contains("\"stream\":{\"k\":\" \",\"service_name\":\"a\"}"),
            "the query response still substitutes: {rendered}"
        );
    }

    /// Issue #455 — the substitution BORROWS when there is nothing to
    /// substitute. Not an optimisation: an unconditional `replace` per
    /// rendered stream moves the benign profile in
    /// `ac19_render_path_peak_and_allocation_profile` from `(1, 108045)`
    /// to `(2, 108067)`.
    #[test]
    fn the_label_substitution_borrows_when_there_is_no_replacement_char() {
        let clean = r#"{"env":"prod","service_name":"checkout"}"#;
        let got = space_for_replacement_chars(clean);
        assert!(matches!(got, Cow::Borrowed(_)), "clean input must borrow");
        assert_eq!(got.as_ptr(), clean.as_ptr(), "and borrow the SAME bytes");

        let dirty = "{\"k\":\"\u{FFFD}\"}";
        let got = space_for_replacement_chars(dirty);
        assert!(matches!(got, Cow::Owned(_)), "dirty input must own");
        assert_eq!(got.as_ref(), r#"{"k":" "}"#);
    }

    /// Issue #455 — **object order follows the PRE-substitution label
    /// set.** Two streams whose labels differ only in a U+FFFD against a
    /// literal space collide after substitution; the reference's unsplit
    /// branch emits both, ordered by the labels it grouped on, and so do
    /// we because the substitution runs at render time, downstream of
    /// `query_response_warned`'s `(labels_json, fingerprint)` sort.
    ///
    /// The two halves are the hermetic form of the live Q9/Q9s pair:
    /// swapping which stream holds the U+FFFD reverses the objects.
    ///
    /// **This is the ONLY test that reddens when the substitution moves
    /// before the sort**, and the fingerprints here are what make that
    /// possible. On the real pipeline a fan-out group's fingerprint is
    /// `fnv1a64` of its own pre-substitution `labels_json`
    /// (`logql/detected_probe.rs:85`), so collapsing the labels leaves the
    /// tiebreak carrying the same information and the live order does not
    /// move — measured, not assumed. Choosing fingerprints whose order
    /// CONTRADICTS the label order is what exposes the collapse:
    /// with the substitution hoisted above the sort this half reports
    /// `the literal space sorts before the U+FFFD (0x20 < 0xEF)`.
    #[tokio::test]
    async fn colliding_stream_labels_order_by_the_pre_substitution_label_set() {
        let space = r#"{"k":" "}"#;
        let repl = "{\"k\":\"\u{FFFD}\"}";

        let order_of = |first_holds_space: bool| {
            let (a, b) = if first_holds_space {
                (space, repl)
            } else {
                (repl, space)
            };
            QueryResult::Streams {
                items: vec![
                    stream(2, a, vec![(1, "line-from-c1")]),
                    stream(1, b, vec![(2, "line-from-c2")]),
                ],
                partial: false,
            }
        };

        let body = body_string(query_response(order_of(true), None, 0, false)).await;
        assert_eq!(
            body.matches(r#"{"k":" "}"#).count(),
            2,
            "both objects are emitted, both labelled with a space: {body}"
        );
        let c1 = body.find("line-from-c1").expect("c1 present");
        let c2 = body.find("line-from-c2").expect("c2 present");
        assert!(
            c1 < c2,
            "the literal space sorts before the U+FFFD (0x20 < 0xEF): {body}"
        );

        let body = body_string(query_response(order_of(false), None, 0, false)).await;
        let c1 = body.find("line-from-c1").expect("c1 present");
        let c2 = body.find("line-from-c2").expect("c2 present");
        assert!(
            c2 < c1,
            "swapping which stream holds the U+FFFD REVERSES the objects: {body}"
        );
    }

    #[tokio::test]
    async fn streams_envelope_sorts_multiple_streams_by_label_set_deterministically() {
        let result = QueryResult::Streams {
            items: vec![
                stream(2, r#"{"service_name":"zeta"}"#, vec![(1, "z")]),
                stream(1, r#"{"service_name":"alpha"}"#, vec![(1, "a")]),
            ],
            partial: false,
        };
        let res = query_response(result, None, 0, false);
        let body = body_string(res).await;
        // "alpha" sorts before "zeta" lexicographically.
        let alpha_pos = body.find("alpha").expect("alpha present");
        let zeta_pos = body.find("zeta").expect("zeta present");
        assert!(alpha_pos < zeta_pos);
    }

    #[tokio::test]
    async fn streams_envelope_respects_the_global_limit_across_multiple_streams() {
        // Amendment 2's semantic pin: `limit` bounds total entries across
        // the whole response, not per stream. This fixture proves the
        // encoder faithfully reports whatever total the engine already
        // capped to (2 entries total across 2 streams), not a per-stream
        // count.
        let result = QueryResult::Streams {
            items: vec![
                stream(1, r#"{"service_name":"a"}"#, vec![(1, "x")]),
                stream(2, r#"{"service_name":"b"}"#, vec![(1, "y")]),
            ],
            partial: false,
        };
        let res = query_response(result, None, 0, false);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["stats"]["entries"], 2);
        assert_eq!(json["data"]["stats"]["streams"], 2);
    }

    #[tokio::test]
    async fn streams_envelope_carries_data_explain_when_requested() {
        let mut explain = PlanExplain::new("streams");
        explain.push("stage1_stream_resolution", "SELECT 1", None);
        let result = QueryResult::Streams {
            items: vec![],
            partial: false,
        };
        let res = query_response(result, Some(explain), 0, false);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["explain"]["result_type"], "streams");
        assert_eq!(
            json["data"]["explain"]["stages"][0]["name"],
            "stage1_stream_resolution"
        );
        assert!(json["data"]["explain"]["routing"].is_null());
    }

    #[tokio::test]
    async fn matrix_envelope_renders_points_and_series_stats() {
        let series = MatrixSeries {
            labels: vec![("service_name".to_string(), "checkout".to_string())],
            points: vec![(0, 1.0), (1_000_000_000, 2.5)],
        };
        let mut explain = PlanExplain::new("matrix");
        explain.set_routing(RoutingDecision {
            chosen: RouteChoice::Rollup,
            reason: "rollup: step divisible by resolution".to_string(),
        });
        let res = query_response(QueryResult::Matrix(vec![series]), Some(explain), 0, false);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["resultType"], "matrix");
        assert_eq!(json["data"]["stats"]["series"], 1);
        assert_eq!(
            json["data"]["result"][0]["metric"]["service_name"],
            "checkout"
        );
        assert_eq!(json["data"]["result"][0]["values"][0][0], 0.0);
        assert_eq!(json["data"]["result"][0]["values"][0][1], "1");
        assert_eq!(json["data"]["explain"]["routing"]["chosen"], "rollup");
    }

    #[tokio::test]
    async fn vector_envelope_uses_the_instant_evaluation_time() {
        let sample = VectorSample {
            labels: vec![("service_name".to_string(), "checkout".to_string())],
            value: 42.0,
        };
        let res = query_response(
            QueryResult::Vector(vec![sample]),
            None,
            5_500_000_000,
            false,
        );
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["resultType"], "vector");
        assert_eq!(json["data"]["result"][0]["value"][0], 5.500);
        assert_eq!(json["data"]["result"][0]["value"][1], "42");
    }

    /// Byte-exact matrix golden (round-1 code-review finding 4a; finding 3
    /// — "matrix timestamps should be ns-strings" — was reviewed and
    /// rejected by architect plan amendment 3 §3: api.md §2.1 pins
    /// Prometheus-style `[<unix_seconds>, "<value>"]` matrix/vector points,
    /// distinct from streams' ns-string log-line timestamps. This fixture
    /// locks that exact wire shape, including the millisecond-resolution
    /// `.000`/`.500` formatting `format_unix_seconds` produces.
    #[tokio::test]
    async fn matrix_envelope_is_byte_exact_for_a_single_series() {
        let series = MatrixSeries {
            labels: vec![("service_name".to_string(), "checkout".to_string())],
            points: vec![(0, 1.0), (1_000_000_000, 2.5)],
        };
        let res = query_response(QueryResult::Matrix(vec![series]), None, 0, false);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"service_name":"checkout"},"values":[[0.000,"1"],[1.000,"2.5"]]}],"stats":{"series":1}}}"#
        );
    }

    /// Byte-exact vector golden (round-1 code-review finding 4b) — same
    /// Prometheus-style `[<unix_seconds>, "<value>"]` point shape as
    /// matrix, at the single instant-evaluation timestamp.
    #[tokio::test]
    async fn vector_envelope_is_byte_exact_for_a_single_sample() {
        let sample = VectorSample {
            labels: vec![("service_name".to_string(), "checkout".to_string())],
            value: 42.0,
        };
        let res = query_response(
            QueryResult::Vector(vec![sample]),
            None,
            5_500_000_000,
            false,
        );
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"service_name":"checkout"},"value":[5.500,"42"]}],"stats":{"series":1}}}"#
        );
    }

    /// Issue M8-LQ3, direction 1: `preserve_vector_order = true` (a
    /// terminal `sort`/`sort_desc` instant query) keeps the engine's value
    /// order on the wire. The input Vec is deliberately in the OPPOSITE of
    /// label-sorted order (`z` before `a`); with the flag set it must
    /// serialise in that engine order, never re-sorted by label.
    #[tokio::test]
    async fn preserve_vector_order_keeps_the_engine_value_order_on_the_wire() {
        let items = vec![
            VectorSample {
                labels: vec![("app".to_string(), "z".to_string())],
                value: 9.0,
            },
            VectorSample {
                labels: vec![("app".to_string(), "a".to_string())],
                value: 1.0,
            },
        ];
        let res = query_response(QueryResult::Vector(items), None, 0, true);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["result"][0]["metric"]["app"], "z");
        assert_eq!(json["data"]["result"][1]["metric"]["app"], "a");
    }

    /// Issue M8-LQ3, direction 2 (the regression pin proving the flag is
    /// load-bearing): with `preserve_vector_order = false` the SAME input
    /// label-sorts (`a` before `z`).
    #[tokio::test]
    async fn without_preserve_vector_order_the_vector_label_sorts() {
        let items = vec![
            VectorSample {
                labels: vec![("app".to_string(), "z".to_string())],
                value: 9.0,
            },
            VectorSample {
                labels: vec![("app".to_string(), "a".to_string())],
                value: 1.0,
            },
        ];
        let res = query_response(QueryResult::Vector(items), None, 0, false);
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["result"][0]["metric"]["app"], "a");
        assert_eq!(json["data"]["result"][1]["metric"]["app"], "z");
    }

    /// Issue #169 byte-exact volume golden: the vector envelope at
    /// timestamp `end` (3dp seconds), entries in the ENGINE's bytes-desc
    /// order — "zeta" (9 bytes) stays FIRST despite sorting after "alpha"
    /// lexically, proving the encoder never re-sorts by label set.
    #[tokio::test]
    async fn volume_envelope_is_byte_exact_and_preserves_bytes_desc_order() {
        let entries = vec![
            VolumeEntry {
                labels: vec![("env".to_string(), "zeta".to_string())],
                bytes: 9,
            },
            VolumeEntry {
                labels: vec![("env".to_string(), "alpha".to_string())],
                bytes: 4,
            },
        ];
        let res = volume_response(entries, 5_500_000_000, None);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"env":"zeta"},"value":[5.500,"9"]},{"metric":{"env":"alpha"},"value":[5.500,"4"]}],"stats":{"series":2}}}"#
        );
    }

    /// Issue #169: labels-mode entries render the oracle's
    /// `{"<name>":""}` empty-value metric object.
    #[tokio::test]
    async fn volume_envelope_renders_the_labels_mode_empty_value_metric() {
        let entries = vec![VolumeEntry {
            labels: vec![("env".to_string(), String::new())],
            bytes: 7,
        }];
        let res = volume_response(entries, 0, None);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"env":""},"value":[0.000,"7"]}],"stats":{"series":1}}}"#
        );
    }

    #[tokio::test]
    async fn volume_envelope_carries_data_explain_when_requested() {
        let mut explain = PlanExplain::new("volume");
        explain.push("volume_read", "SELECT 1", None);
        let res = volume_response(Vec::new(), 0, Some(explain));
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["result"], serde_json::json!([]));
        assert_eq!(json["data"]["stats"]["series"], 0);
        assert_eq!(json["data"]["explain"]["result_type"], "volume");
        assert_eq!(json["data"]["explain"]["stages"][0]["name"], "volume_read");
    }

    /// Issue #170 byte-exact detected_labels golden: the bare reference
    /// wire shape, no envelope, entries verbatim in engine (key-sorted)
    /// order.
    #[tokio::test]
    async fn detected_labels_envelope_is_byte_exact() {
        let labels = vec![
            DetectedLabelOut {
                label: "env".to_string(),
                cardinality: 3,
            },
            DetectedLabelOut {
                label: "namespace".to_string(),
                cardinality: 1,
            },
        ];
        let res = detected_labels_response(labels, None);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"detectedLabels":[{"label":"env","cardinality":3},{"label":"namespace","cardinality":1}]}"#
        );
    }

    #[tokio::test]
    async fn detected_labels_empty_result_keeps_the_top_level_key() {
        let res = detected_labels_response(Vec::new(), None);
        let body = body_string(res).await;
        assert_eq!(body, r#"{"detectedLabels":[]}"#);
    }

    #[tokio::test]
    async fn detected_labels_envelope_carries_explain_as_a_sibling_key() {
        let mut explain = PlanExplain::new("detected_labels");
        explain.push("detected_labels", "SELECT 1", None);
        let res = detected_labels_response(Vec::new(), Some(explain));
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["detectedLabels"], serde_json::json!([]));
        assert_eq!(json["explain"]["result_type"], "detected_labels");
    }

    /// Issues #170/#254/#258 detected_fields golden, pinned against the
    /// `grafana/loki:3.7.4` capture recorded on #258:
    /// label/type/cardinality/parsers/jsonPath per field (the proto field
    /// order), `parsers` as `null` when unattributed, `jsonPath` present
    /// only for a json-flattened field, `limit` as the trailing key — and
    /// NO `pulsus_partial` key on a complete result.
    ///
    /// The byte-exactness this pins is the ENVELOPE and each per-field
    /// OBJECT; the array ORDER is our deterministic pin over the
    /// reference's irreproducible Go map order, not parity (ledger row
    /// `detected-fields-array-order-pinned`), which is why the input here
    /// is already label-sorted.
    #[tokio::test]
    async fn detected_fields_field_objects_and_envelope_are_byte_exact_when_complete() {
        let out = DetectedFields {
            fields: vec![
                DetectedFieldOut {
                    label: "count".to_string(),
                    field_type: "int",
                    cardinality: 2,
                    parsers: vec!["json"],
                    json_path: Some(vec!["count".to_string()]),
                },
                DetectedFieldOut {
                    label: "trace_id".to_string(),
                    field_type: "string",
                    cardinality: 1,
                    parsers: Vec::new(),
                    json_path: None,
                },
            ],
            truncated: false,
            retention_capped: false,
        };
        let res = detected_fields_response(out, 1000, None);
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"fields":[{"label":"count","type":"int","cardinality":2,"parsers":["json"],"jsonPath":["count"]},{"label":"trace_id","type":"string","cardinality":1,"parsers":null}],"limit":1000}"#
        );
    }

    /// Issue #254: a NESTED json field carries every raw path component,
    /// in order — the reference's `buildJSONPathFromPrefixBuffer`
    /// (`pkg/logql/log/parser.go:234-248 @ v3.7.4`).
    #[tokio::test]
    async fn detected_fields_json_path_carries_every_nested_component() {
        let out = DetectedFields {
            fields: vec![DetectedFieldOut {
                label: "user_id".to_string(),
                field_type: "int",
                cardinality: 3,
                parsers: vec!["json"],
                json_path: Some(vec!["user".to_string(), "id".to_string()]),
            }],
            truncated: false,
            retention_capped: false,
        };
        let body = body_string(detected_fields_response(out, 1000, None)).await;
        assert_eq!(
            body,
            r#"{"fields":[{"label":"user_id","type":"int","cardinality":3,"parsers":["json"],"jsonPath":["user","id"]}],"limit":1000}"#
        );
    }

    /// Issue #258: the zero-field body is bare `{}` — `fields` is
    /// `omitempty` and `limit` is only assigned when `len(fields) > 0`
    /// (`pkg/querier/queryrange/detected_fields.go:85-87 @ v3.7.4`).
    /// Captured from the pinned container on #258.
    #[tokio::test]
    async fn detected_fields_zero_fields_is_the_bare_empty_object() {
        let out = DetectedFields {
            fields: Vec::new(),
            truncated: false,
            retention_capped: false,
        };
        let res = detected_fields_response(out, 1000, None);
        assert_eq!(body_string(res).await, "{}");
    }

    /// Issue #170 plan v2: a budget-truncated sample carries the additive
    /// `pulsus_partial: true` key (the #90 wire convention) — the ONLY
    /// key in an otherwise-empty body (issue #258).
    #[tokio::test]
    async fn detected_fields_envelope_carries_pulsus_partial_true_on_truncation() {
        let out = DetectedFields {
            fields: Vec::new(),
            truncated: true,
            retention_capped: false,
        };
        let res = detected_fields_response(out, 1000, None);
        let body = body_string(res).await;
        assert_eq!(body, r#"{"pulsus_partial":true}"#);
    }

    /// Issue #244: a retention-capped accumulation (the byte ceiling
    /// refused a distinct value/name) carries the SAME additive
    /// `pulsus_partial: true` key, without budget truncation.
    #[tokio::test]
    async fn detected_fields_envelope_carries_pulsus_partial_true_on_retention_cap_alone() {
        let out = DetectedFields {
            fields: Vec::new(),
            truncated: false,
            retention_capped: true,
        };
        let res = detected_fields_response(out, 1000, None);
        let body = body_string(res).await;
        assert_eq!(body, r#"{"pulsus_partial":true}"#);
    }

    /// A truncated non-empty result keeps `limit` AND `pulsus_partial`:
    /// #258 narrowed the omission to the zero-field case only.
    #[tokio::test]
    async fn detected_fields_non_empty_truncated_keeps_limit_and_pulsus_partial() {
        let out = DetectedFields {
            fields: vec![DetectedFieldOut {
                label: "lvl".to_string(),
                field_type: "string",
                cardinality: 1,
                parsers: vec!["logfmt"],
                json_path: None,
            }],
            truncated: true,
            retention_capped: false,
        };
        let body = body_string(detected_fields_response(out, 1000, None)).await;
        assert_eq!(
            body,
            r#"{"fields":[{"label":"lvl","type":"string","cardinality":1,"parsers":["logfmt"]}],"limit":1000,"pulsus_partial":true}"#
        );
    }

    /// The additive keys compose in the zero-field body in the same order
    /// the populated body places them.
    #[tokio::test]
    async fn detected_fields_zero_fields_composes_pulsus_partial_and_explain() {
        let mut explain = PlanExplain::new("detected_fields");
        explain.push("stage1_stream_resolution", "SELECT 1", None);
        let out = DetectedFields {
            fields: Vec::new(),
            truncated: true,
            retention_capped: false,
        };
        let body = body_string(detected_fields_response(out, 500, Some(explain))).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(json.get("fields").is_none(), "body {body}");
        assert!(json.get("limit").is_none(), "body {body}");
        assert_eq!(json["pulsus_partial"], true);
        assert_eq!(json["explain"]["result_type"], "detected_fields");
        assert!(
            body.starts_with(r#"{"pulsus_partial":true,"explain":"#),
            "body {body}"
        );
    }

    #[tokio::test]
    async fn detected_fields_envelope_carries_explain_as_a_sibling_key() {
        let mut explain = PlanExplain::new("detected_fields");
        explain.push("stage1_stream_resolution", "SELECT 1", None);
        let out = DetectedFields {
            fields: Vec::new(),
            truncated: false,
            retention_capped: false,
        };
        let res = detected_fields_response(out, 500, Some(explain));
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        // Issue #258: an empty result carries neither `fields` nor
        // `limit`; `explain` is the whole body.
        assert!(json.get("fields").is_none(), "body {body}");
        assert!(json.get("limit").is_none(), "body {body}");
        assert!(json.get("pulsus_partial").is_none());
        assert_eq!(json["explain"]["result_type"], "detected_fields");
    }

    #[tokio::test]
    async fn gzip_detected_labels_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            detected_labels_response(
                vec![DetectedLabelOut {
                    label: "env".to_string(),
                    cardinality: 3,
                }],
                None,
            )
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_detected_fields_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            detected_fields_response(
                DetectedFields {
                    fields: vec![DetectedFieldOut {
                        label: "level".to_string(),
                        field_type: "string",
                        cardinality: 4,
                        parsers: vec!["logfmt"],
                        json_path: None,
                    }],
                    truncated: false,
                    retention_capped: false,
                },
                1000,
                None,
            )
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_volume_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            volume_response(
                vec![VolumeEntry {
                    labels: vec![("service_name".to_string(), "checkout".to_string())],
                    bytes: 42,
                }],
                5_500_000_000,
                None,
            )
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn empty_streams_result_still_renders_a_well_formed_envelope() {
        let res = query_response(
            QueryResult::Streams {
                items: vec![],
                partial: false,
            },
            None,
            0,
            false,
        );
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["data"]["result"], serde_json::json!([]));
        assert_eq!(json["data"]["stats"]["streams"], 0);
    }

    #[tokio::test]
    async fn string_array_envelope_escapes_values_and_supports_explain() {
        let mut explain = PlanExplain::new("labels");
        explain.push("label_names", "SELECT DISTINCT key", None);
        let res = string_array_response(
            vec!["env".to_string(), "with \"quote\"".to_string()],
            Some(explain),
        );
        let body = body_string(res).await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"][0], "env");
        assert_eq!(json["data"][1], "with \"quote\"");
        assert_eq!(json["explain"]["result_type"], "labels");
    }

    #[tokio::test]
    async fn json_array_envelope_splices_canonical_labels_json_verbatim() {
        let res = json_array_response(
            vec![r#"{"env":"prod","service_name":"checkout"}"#.to_string()],
            None,
        );
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":[{"env":"prod","service_name":"checkout"}]}"#
        );
    }

    /// Byte-exact series golden (round-1 code-review finding 4c) — multiple
    /// already-canonical label-object JSON strings, comma-joined verbatim
    /// with no re-parse/re-encode.
    #[tokio::test]
    async fn series_envelope_is_byte_exact_for_multiple_label_sets() {
        let res = json_array_response(
            vec![
                r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
                r#"{"env":"staging","service_name":"checkout"}"#.to_string(),
            ],
            None,
        );
        let body = body_string(res).await;
        assert_eq!(
            body,
            r#"{"status":"success","data":[{"env":"prod","service_name":"checkout"},{"env":"staging","service_name":"checkout"}]}"#
        );
    }

    #[tokio::test]
    async fn empty_array_response_renders_an_empty_data_array() {
        let res = string_array_response(vec![], None);
        let body = body_string(res).await;
        assert_eq!(body, r#"{"status":"success","data":[]}"#);
    }

    /// Encoder memory bound (architect plan amendment 1, encoder unit test
    /// 2(a)): drives a synthetic 100k-entry streams result (spread across
    /// many small streams, the worst case for "one item at a time")
    /// through the raw chunk stream and asserts every individual yielded
    /// chunk stays near one stream's own size — never anywhere close to
    /// the full ~100k-entry aggregate. This is a stronger, more direct
    /// proof of "bounded intermediate buffering" than measuring process
    /// allocation would be: a chunk that is itself small **cannot** be
    /// the product of a whole-result `serde_json` DOM/second copy.
    #[tokio::test]
    async fn streams_encoder_yields_bounded_chunks_for_a_100k_entry_synthetic_result() {
        const NUM_STREAMS: usize = 1000;
        const ENTRIES_PER_STREAM: usize = 100; // 100_000 entries total.

        let items: Vec<StreamResult> = (0..NUM_STREAMS)
            .map(|i| {
                let labels_json = format!(r#"{{"service_name":"svc-{i:05}"}}"#);
                let entries = (0..ENTRIES_PER_STREAM)
                    .map(|j| {
                        (
                            (i * ENTRIES_PER_STREAM + j) as i64,
                            "a modestly sized log line for chunk-bound measurement purposes"
                                .to_string(),
                        )
                    })
                    .collect();
                StreamResult {
                    fingerprint: i as u64,
                    service: "checkout".to_string(),
                    labels_json,
                    entries,
                    categories: Vec::new(),
                }
            })
            .collect();

        let res = query_response(
            QueryResult::Streams {
                items,
                partial: false,
            },
            None,
            0,
            false,
        );
        let mut stream = res.into_body().into_data_stream();

        let mut chunk_count = 0usize;
        let mut max_chunk_len = 0usize;
        let mut total_len = 0usize;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("chunk");
            chunk_count += 1;
            max_chunk_len = max_chunk_len.max(chunk.len());
            total_len += chunk.len();
        }

        // One prefix chunk, one chunk per stream, one zero-copy `,` chunk
        // between consecutive streams (issue #312 — the separator is no
        // longer copied into the item's buffer), one suffix chunk.
        assert_eq!(chunk_count, 2 * NUM_STREAMS + 1);
        // Total output is large (~100k entries' worth of text) ...
        assert!(total_len > 5_000_000, "total_len = {total_len}");
        // ... but no single chunk is anywhere near that size: each stream
        // item's own chunk (100 entries of ~70 bytes) is a few KB, so a
        // generous 64KB ceiling is still two orders of magnitude below the
        // aggregate — proving the encoder never materializes the whole
        // result as one buffer.
        assert!(
            max_chunk_len < 64 * 1024,
            "max_chunk_len = {max_chunk_len} (aggregate would be ~{total_len})"
        );
    }

    /// Poll-after-end regression (issue #24): `futures::stream::Unfold`'s
    /// documented contract is that it must never be polled again once it
    /// has returned `Poll::Ready(None)` — it `panic!`s otherwise. This is
    /// exactly what `tower_http::compression::CompressionLayer`'s gzip
    /// encoder does (it polls the wrapped body once more past EOF), which
    /// used to abort the request task on every gzip-negotiated request.
    /// Drives the body's data stream to completion, then polls it once
    /// more, and asserts a second, safe `None` — the minimal reproduction
    /// of the defect, with no compression dependency needed. Fails (panics
    /// at `unfold.rs:108`) on `Body::from_stream(stream)` without `.fuse()`.
    #[tokio::test]
    async fn stream_array_body_yields_none_instead_of_panicking_when_polled_after_completion() {
        let result = QueryResult::Streams {
            items: vec![stream(
                1,
                r#"{"service_name":"checkout"}"#,
                vec![(100, "hello")],
            )],
            partial: false,
        };
        let res = query_response(result, None, 0, false);
        let mut body_stream = res.into_body().into_data_stream();

        while body_stream.next().await.is_some() {}

        assert!(
            body_stream.next().await.is_none(),
            "polling the body stream once more after completion must yield None, not panic"
        );
    }

    /// Runs `build` (a response-shape constructor) through a real
    /// `CompressionLayer`-wrapped router twice — once with no
    /// `Accept-Encoding` (identity) and once with `Accept-Encoding: gzip`
    /// — and asserts the gzip-decoded body is byte-identical to the
    /// identity body. Exercises the actual layer that triggers the
    /// poll-after-end panic (a synthetic `unfold`/`.fuse()` test cannot
    /// prove the *real* compression encoder is satisfied).
    async fn assert_gzip_response_is_byte_identical_to_identity(build: fn() -> Response) {
        let router = Router::new()
            .route("/x", get(move || async move { build() }))
            .layer(CompressionLayer::new());

        let identity_request = Request::builder().uri("/x").body(Body::empty()).unwrap();
        let identity_response = router
            .clone()
            .oneshot(identity_request)
            .await
            .expect("identity request must not panic the request task");
        let identity_body = to_bytes(identity_response.into_body(), usize::MAX)
            .await
            .expect("identity body");

        let gzip_request = Request::builder()
            .uri("/x")
            .header(header::ACCEPT_ENCODING, "gzip")
            .body(Body::empty())
            .unwrap();
        let gzip_response = router
            .oneshot(gzip_request)
            .await
            .expect("gzip request must not panic the request task (issue #24 regression)");
        assert_eq!(
            gzip_response
                .headers()
                .get(header::CONTENT_ENCODING)
                .and_then(|v| v.to_str().ok()),
            Some("gzip"),
            "response must actually be gzip-encoded for this assertion to be meaningful"
        );
        let gzip_body = to_bytes(gzip_response.into_body(), usize::MAX)
            .await
            .expect("gzip body");

        let mut decoder = GzDecoder::new(&gzip_body[..]);
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .expect("gzip body must decode as a valid gzip stream");

        assert_eq!(
            decoded, identity_body,
            "gzip-decoded body must be byte-identical to the identity-encoding body"
        );
    }

    #[tokio::test]
    async fn gzip_streams_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            let result = QueryResult::Streams {
                items: vec![
                    stream(
                        1,
                        r#"{"env":"prod","service_name":"checkout"}"#,
                        vec![(100, "hello"), (200, "world")],
                    ),
                    stream(
                        2,
                        r#"{"env":"staging","service_name":"checkout"}"#,
                        vec![(150, "another line")],
                    ),
                ],
                partial: false,
            };
            query_response(result, None, 0, false)
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_empty_streams_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            query_response(
                QueryResult::Streams {
                    items: vec![],
                    partial: false,
                },
                None,
                0,
                false,
            )
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_matrix_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            let series = MatrixSeries {
                labels: vec![("service_name".to_string(), "checkout".to_string())],
                points: vec![(0, 1.0), (1_000_000_000, 2.5)],
            };
            query_response(QueryResult::Matrix(vec![series]), None, 0, false)
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_vector_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            let sample = VectorSample {
                labels: vec![("service_name".to_string(), "checkout".to_string())],
                value: 42.0,
            };
            query_response(
                QueryResult::Vector(vec![sample]),
                None,
                5_500_000_000,
                false,
            )
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_string_array_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            string_array_response(vec!["env".to_string(), "service_name".to_string()], None)
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    #[tokio::test]
    async fn gzip_series_response_matches_identity_byte_for_byte() {
        fn build() -> Response {
            json_array_response(
                vec![
                    r#"{"env":"prod","service_name":"checkout"}"#.to_string(),
                    r#"{"env":"staging","service_name":"checkout"}"#.to_string(),
                ],
                None,
            )
        }
        assert_gzip_response_is_byte_identical_to_identity(build).await;
    }

    // -----------------------------------------------------------------
    // Issue #277: the `warnings` envelope key.
    // -----------------------------------------------------------------

    fn one_warning() -> Warnings {
        let mut w = Warnings::new();
        w.add("maximum of series (500) reached for variant (0)".to_string());
        w
    }

    /// AC 1 — **position and content, byte for byte.** `warnings` is the
    /// LAST top-level key, a sibling of `data` and appended AFTER its
    /// closing brace.
    ///
    /// The position was CAPTURED from the pinned reference, not read off
    /// its encoder: `pkg/util/marshal/query.go:201-228` (`EncodeResult`)
    /// writes `status`, `warnings`, `data`, and that is not the encoder a
    /// user's query reaches — the query-frontend path serialises
    /// `queryrangebase.PrometheusResponse`
    /// (`pkg/querier/queryrange/queryrangebase/queryrange.proto:36-46`,
    /// `Warnings`=6 `json:"warnings,omitempty"`). Had this been derived
    /// from `EncodeResult` the key would have shipped in the wrong place.
    #[tokio::test]
    async fn logs_query_envelope_places_warnings_after_data() {
        let sample = VectorSample {
            labels: vec![("__variant__".to_string(), "1".to_string())],
            value: 501.0,
        };
        let body = body_string(query_response_warned(
            QueryResult::Vector(vec![sample]),
            None,
            5_500_000_000,
            false,
            &one_warning(),
            &[],
        ))
        .await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__variant__":"1"},"value":[5.500,"501"]}],"stats":{"series":1}},"warnings":["maximum of series (500) reached for variant (0)"]}"#
        );
        // Stated as a relation as well as a literal, so a future edit to
        // the golden cannot quietly move the key inside `data`.
        let warn_at = body.find(r#""warnings""#).expect("the key is present");
        let data_end = body.rfind(r#"},"warnings""#).expect("data closes first");
        assert!(warn_at > data_end, "warnings must follow data's brace");
        assert!(
            body.ends_with(r#"]}"#),
            "warnings is the LAST top-level key"
        );
    }

    /// The matrix arm's twin, and the multi-warning wire order — byte
    /// lexicographic, so `variant (10)` precedes `variant (2)` (captured;
    /// `pkg/logqlmodel/metadata/context.go:80-92 @ grafana/loki v3.7.4
    /// b318f2829f0ae2094ab3a1e90780450e9e4b03be`).
    #[tokio::test]
    async fn logs_query_range_envelope_places_warnings_after_data() {
        let series = MatrixSeries {
            labels: vec![("__variant__".to_string(), "1".to_string())],
            points: vec![(0, 501.0)],
        };
        let mut w = Warnings::new();
        w.add("maximum of series (500) reached for variant (2)".to_string());
        w.add("maximum of series (500) reached for variant (10)".to_string());
        let body = body_string(query_response_warned(
            QueryResult::Matrix(vec![series]),
            None,
            0,
            false,
            &w,
            &[],
        ))
        .await;
        assert_eq!(
            body,
            r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__variant__":"1"},"values":[[0.000,"501"]]}],"stats":{"series":1}},"warnings":["maximum of series (500) reached for variant (10)","maximum of series (500) reached for variant (2)"]}"#
        );
    }

    /// `explain` lives INSIDE `data`; `warnings` lives outside it. The two
    /// are not siblings, and appending the warnings to `explain_suffix`
    /// would nest them under `data` — this is the assertion that catches
    /// that.
    #[tokio::test]
    async fn explain_stays_inside_data_while_warnings_stay_outside_it() {
        let mut explain = PlanExplain::new("vector");
        explain.push("metric_read", "SELECT 1".to_string(), None);
        let sample = VectorSample {
            labels: vec![("__variant__".to_string(), "1".to_string())],
            value: 1.0,
        };
        let body = body_string(query_response_warned(
            QueryResult::Vector(vec![sample]),
            Some(explain),
            0,
            false,
            &one_warning(),
            &[],
        ))
        .await;
        let explain_at = body.find(r#""explain""#).expect("explain is present");
        let warn_at = body.find(r#""warnings""#).expect("warnings are present");
        let data_end = body.rfind(r#"},"warnings""#).expect("data closes");
        assert!(explain_at < data_end, "explain must be inside data: {body}");
        assert!(warn_at > data_end, "warnings must be outside data: {body}");
    }

    /// AC 12 — **the empty-`Warnings` path is byte-identical on every arm
    /// of `query_response` as it exists today.** `warnings_suffix` of an
    /// empty accumulator is the empty string, and each of the seven
    /// `QueryResult` arms — `Streams`, `Matrix`, `Vector`, `Scalar`,
    /// `String`, `VectorHist`, `MatrixHist` — produces the same bytes
    /// with an empty accumulator as it does through the pre-change
    /// `query_response` wrapper.
    ///
    /// **What is compiler-closed and what is not.** `query_response`'s
    /// `match` is exhaustive with no `_` arm, so a new `QueryResult`
    /// variant cannot be added without adding an ENCODER arm. It does
    /// **not** force this test to gain a case: an implementer can add the
    /// arm and leave the seven below untouched, and nothing reddens. The
    /// encoder domain is compiler-closed; **this test's domain is a
    /// hand-written list.** That residue is accepted and recorded rather
    /// than policed by another test (issue #277 plan v3).
    #[tokio::test]
    async fn empty_warnings_leaves_every_query_response_arm_byte_identical() {
        assert_eq!(warnings_suffix(&Warnings::new()), "");

        let arms: Vec<(&str, QueryResult)> = vec![
            (
                "Streams",
                QueryResult::Streams {
                    items: vec![stream(1, r#"{"service_name":"a"}"#, vec![(1, "x")])],
                    partial: false,
                },
            ),
            (
                "Matrix",
                QueryResult::Matrix(vec![MatrixSeries {
                    labels: vec![("service_name".to_string(), "a".to_string())],
                    points: vec![(0, 1.0)],
                }]),
            ),
            (
                "Vector",
                QueryResult::Vector(vec![VectorSample {
                    labels: vec![("service_name".to_string(), "a".to_string())],
                    value: 1.0,
                }]),
            ),
            ("Scalar", QueryResult::Scalar(1.5)),
            ("String", QueryResult::String("x".to_string())),
            ("VectorHist", QueryResult::VectorHist(Vec::new())),
            ("MatrixHist", QueryResult::MatrixHist(Vec::new())),
        ];

        for (name, result) in arms {
            let with_empty = query_response_warned(
                result.clone(),
                None,
                5_500_000_000,
                false,
                &Warnings::new(),
                &[],
            );
            let plain = query_response(result, None, 5_500_000_000, false);
            assert_eq!(
                with_empty.status(),
                plain.status(),
                "{name}: status moved with an empty accumulator"
            );
            let a = body_string(with_empty).await;
            let b = body_string(plain).await;
            assert_eq!(a, b, "{name}: body moved with an empty accumulator");
            assert!(
                !a.contains("warnings"),
                "{name}: an empty accumulator must emit no key at all: {a}"
            );
        }
    }

    // -----------------------------------------------------------------
    // Issue #463 — `X-Loki-Response-Encoding-Flags: categorize-labels`
    // -----------------------------------------------------------------

    /// The one fixture every issue #463 hermetic gate below is built from:
    /// two streams, the first carrying an entry of each third-element
    /// framing (`structuredMetadata` + `parsed`, `structuredMetadata`
    /// only, `parsed` only, and both empty), the second a single plain
    /// entry. Written once so the plain and categorised bodies below are
    /// the same data seen two ways.
    fn cat_pair(k: &str, v: &str) -> (String, String) {
        (k.to_string(), v.to_string())
    }

    fn c463_items(categorize: bool) -> Vec<StreamResult> {
        let cats = |sm: &[(&str, &str)], parsed: &[(&str, &str)]| EntryCategories {
            structured_metadata: sm.iter().map(|(k, v)| cat_pair(k, v)).collect(),
            parsed: parsed.iter().map(|(k, v)| cat_pair(k, v)).collect(),
        };
        let mut a = StreamResult {
            fingerprint: 7,
            service: "checkout".to_string(),
            labels_json: r#"{"app":"checkout","service_name":"checkout"}"#.to_string(),
            entries: vec![
                (1_700_000_000_000_000_001, "both".to_string()),
                (1_700_000_000_000_000_002, "sm only".to_string()),
                (1_700_000_000_000_000_003, "parsed only".to_string()),
                (1_700_000_000_000_000_004, "neither".to_string()),
            ],
            categories: Vec::new(),
        };
        let mut b = StreamResult {
            fingerprint: 9,
            service: "billing".to_string(),
            labels_json: r#"{"app":"billing","service_name":"billing"}"#.to_string(),
            entries: vec![(1_700_000_000_000_000_005, "plain".to_string())],
            categories: Vec::new(),
        };
        if categorize {
            a.categories = vec![
                cats(&[("trace_id", "abc")], &[("lvl", "warn")]),
                cats(&[("user_id", "42")], &[]),
                cats(&[], &[("__error__", "JSONParserErr")]),
                cats(&[], &[]),
            ];
            b.categories = vec![cats(&[], &[])];
        }
        vec![a, b]
    }

    fn flags(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    /// Every `values` element in a rendered streams body, as arrays.
    fn value_arities(body: &str) -> Vec<usize> {
        let v: serde_json::Value = serde_json::from_str(body).expect("valid JSON body");
        let streams = v["data"]["result"]
            .as_array()
            .or_else(|| v["streams"].as_array())
            .expect("a streams array");
        streams
            .iter()
            .flat_map(|s| s["values"].as_array().expect("values").iter())
            .map(|e| e.as_array().expect("entry array").len())
            .collect()
    }

    fn encoding_flags_of(body: &str) -> Option<Vec<String>> {
        let v: serde_json::Value = serde_json::from_str(body).expect("valid JSON body");
        let node = if v["data"].is_object() {
            &v["data"]["encodingFlags"]
        } else {
            &v["encodingFlags"]
        };
        node.as_array().map(|a| {
            a.iter()
                .map(|s| s.as_str().expect("token").to_string())
                .collect()
        })
    }

    async fn c463_query_body(items: Vec<StreamResult>, flags: &[String]) -> String {
        body_string(query_response_warned(
            QueryResult::Streams {
                items,
                partial: false,
            },
            None,
            0,
            false,
            &Warnings::new(),
            flags,
        ))
        .await
    }

    /// **Criterion 3 — no header ⇒ byte-identical to the pre-#463 tree.**
    ///
    /// The literal below was captured by running THIS fixture through
    /// `query_response_warned` at `bf3a8f6` (the commit this branch
    /// forked from, before any of issue #463 existed), by checking that
    /// tree out in place and printing the body. It is not a
    /// transcription of what the current code emits.
    ///
    /// The companion half of this criterion is that
    /// `logql_pipeline_alloc.rs`, `logql_variants_alloc.rs`,
    /// `logql_streams_retention_inventory.rs` and
    /// [`ac19_render_path_peak_and_allocation_profile`] pass with no edit
    /// to their expected values.
    #[tokio::test]
    async fn no_header_renders_the_pre_change_bytes_exactly() {
        // Categorised data present on every stream, and NO header: the
        // decision must still come out off, so this also pins that the
        // arity is never taken from the data alone.
        let body = c463_query_body(c463_items(true), &[]).await;
        assert_eq!(body, C463_PLAIN_GOLDEN);
        assert!(
            value_arities(&body).iter().all(|n| *n == 2),
            "every value must stay two-element: {body}"
        );
        assert_eq!(encoding_flags_of(&body), None);
    }

    /// Captured at `bf3a8f6` — see
    /// [`no_header_renders_the_pre_change_bytes_exactly`]. Generated by
    /// checking that commit out in this worktree, printing the body from
    /// the same fixture data, and pasting the bytes; never retyped from
    /// what the current renderer emits.
    const C463_PLAIN_GOLDEN: &str = r#"{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"billing","service_name":"billing"},"values":[["1700000000000000005","plain"]]},{"stream":{"app":"checkout","service_name":"checkout"},"values":[["1700000000000000001","both"],["1700000000000000002","sm only"],["1700000000000000003","parsed only"],["1700000000000000004","neither"]]}],"stats":{"streams":2,"entries":5,"bytes":34}}}"#;

    /// **Criterion 5 — `encodingFlags` precedes `result`.**
    ///
    /// The datasource's decoder dispatches the moment it reaches
    /// `result`, so a flag emitted after it is silently ignored and the
    /// three-element entries then fail to parse. The tail's order is the
    /// other way round — the reference puts the key LAST there — and the
    /// consumer parses the whole frame before reading a key, so both
    /// orders are asserted where they belong.
    #[tokio::test]
    async fn the_advertisement_sits_where_each_envelope_puts_it() {
        let body = c463_query_body(c463_items(true), &flags(&["categorize-labels"])).await;
        let ef = body.find(r#""encodingFlags""#).expect("advertised");
        let result = body.find(r#""result""#).expect("result");
        let rt = body.find(r#""resultType""#).expect("resultType");
        assert!(rt < ef && ef < result, "query envelope order: {body}");

        let frame = tail_frame(c463_items(true), &[], 0, &flags(&["categorize-labels"]));
        let streams = frame.find(r#""streams""#).expect("streams");
        let total = frame.find(r#""dropped_total""#).expect("dropped_total");
        let ef = frame.find(r#""encodingFlags""#).expect("advertised");
        assert!(
            streams < total && total < ef,
            "tail envelope order: {frame}"
        );
    }

    /// **Criterion 7 — only a `streams` result carries the key.**
    ///
    /// The reference's frontend codec sends every other result type
    /// through an encoder that takes no flags at all, so the key must not
    /// appear on a matrix or a vector however the request was headed.
    #[tokio::test]
    async fn only_the_streams_arm_advertises_the_flag() {
        let f = flags(&["categorize-labels"]);
        let matrix = body_string(query_response_warned(
            QueryResult::Matrix(vec![MatrixSeries {
                labels: vec![("app".to_string(), "checkout".to_string())],
                points: vec![(0, 1.0)],
            }]),
            None,
            0,
            false,
            &Warnings::new(),
            &f,
        ))
        .await;
        let vector = body_string(query_response_warned(
            QueryResult::Vector(vec![VectorSample {
                labels: vec![("app".to_string(), "checkout".to_string())],
                value: 1.0,
            }]),
            None,
            0,
            false,
            &Warnings::new(),
            &f,
        ))
        .await;
        for (name, body) in [("matrix", &matrix), ("vector", &vector)] {
            assert!(
                !body.contains("encodingFlags"),
                "{name} must not advertise: {body}"
            );
        }
    }

    // --- Criterion 4 / 13: the advertisement and the arity are ONE
    // decision, on BOTH envelopes.

    /// The two wire surfaces a streams body can be spliced into. Every
    /// criterion-4 assertion runs once per variant, which is what lets a
    /// tail-only defect have a signature of its own.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Env463 {
        Query,
        Tail,
    }

    async fn c463_render(env: Env463, items: Vec<StreamResult>, flags: &[String]) -> String {
        match env {
            Env463::Query => c463_query_body(items, flags).await,
            Env463::Tail => tail_frame(items, &[], 0, flags),
        }
    }

    /// 4A's fixture: the second stream's `categories` is one element
    /// short, so it cannot serve a third element and the WHOLE body must
    /// downgrade — a mixed-arity body is a hard client parse failure, and
    /// losing a feature is the only safe direction.
    fn c463_short_by_one() -> Vec<StreamResult> {
        let mut items = c463_items(true);
        items[1].categories.pop();
        items
    }

    /// **Criteria 4 and 13(b)/13(c): the mutation matrix, run on both
    /// envelopes.**
    ///
    /// Each mutation named in the plan has a signature in these eight
    /// assertion ids, and the coder performs every one on the implemented
    /// tree and records which reddened — a criterion that only says a
    /// break "would" fire has never been run.
    ///
    /// * `A1` the echo does not contain `categorize-labels`
    /// * `A2` every value has two elements
    /// * `B1` no `encodingFlags` key at all
    /// * `B2` every value has two elements
    /// * `C1` the echo is exactly `["categorize-labels"]`
    /// * `C2` the result/streams array is empty
    /// * `D1` the echo is exactly `["foo"]`
    /// * `D2` every value has two elements
    #[tokio::test]
    async fn the_advertisement_and_the_arity_are_one_decision() {
        // COLLECTED, not asserted eagerly: a mutation's signature is the
        // SET of assertions it fails, and a test that stops at the first
        // one can only ever report a prefix of it. Two mutations that
        // both fail `A1` are then indistinguishable however many other
        // assertions separate them.
        let mut failed: Vec<String> = Vec::new();
        let mut check = |ok: bool, id: &str, env: Env463, body: &str| {
            if !ok {
                failed.push(format!("{id}({env:?})"));
                eprintln!("c463 matrix FAIL {id}({env:?}): {body}");
            }
        };
        for env in [Env463::Query, Env463::Tail] {
            // 4A — a construction bug downgrades the body; it never
            // advertises a shape it cannot serve.
            let body = c463_render(env, c463_short_by_one(), &flags(&["categorize-labels"])).await;
            let echo = encoding_flags_of(&body).unwrap_or_default();
            check(
                !echo.iter().any(|f| f == "categorize-labels"),
                "A1",
                env,
                &body,
            );
            check(
                value_arities(&body).iter().all(|n| *n == 2),
                "A2",
                env,
                &body,
            );

            // 4B — well-formed categories, NO header.
            let body = c463_render(env, c463_items(true), &[]).await;
            check(encoding_flags_of(&body).is_none(), "B1", env, &body);
            check(
                value_arities(&body).iter().all(|n| *n == 2),
                "B2",
                env,
                &body,
            );

            // 4C — an EMPTY result with the flag still advertises it.
            let body = c463_render(env, Vec::new(), &flags(&["categorize-labels"])).await;
            check(
                encoding_flags_of(&body).as_deref() == Some(&["categorize-labels".to_string()][..]),
                "C1",
                env,
                &body,
            );
            let parsed: serde_json::Value = serde_json::from_str(&body).expect("json");
            let arr = match env {
                Env463::Query => &parsed["data"]["result"],
                Env463::Tail => &parsed["streams"],
            };
            check(arr.as_array().map(Vec::len) == Some(0), "C2", env, &body);

            // 4D — an UNKNOWN flag is echoed verbatim and changes nothing.
            let body = c463_render(env, c463_items(true), &flags(&["foo"])).await;
            check(
                encoding_flags_of(&body).as_deref() == Some(&["foo".to_string()][..]),
                "D1",
                env,
                &body,
            );
            check(
                value_arities(&body).iter().all(|n| *n == 2),
                "D2",
                env,
                &body,
            );
        }
        assert!(
            failed.is_empty(),
            "the advertisement and the arity disagreed; signature: {failed:?}"
        );
    }

    /// **The categorised body, on both envelopes.** The positive half of
    /// the matrix above: with the flag and well-formed categories, every
    /// value is three-element and the third element carries
    /// `structuredMetadata` before `parsed`, each omitted when empty and
    /// `{}` when both are.
    #[tokio::test]
    async fn the_categorised_body_renders_the_reference_third_element() {
        let body = c463_query_body(c463_items(true), &flags(&["categorize-labels"])).await;
        assert!(
            value_arities(&body).iter().all(|n| *n == 3),
            "every value must be three-element: {body}"
        );
        let v: serde_json::Value = serde_json::from_str(&body).expect("json");
        let checkout = v["data"]["result"]
            .as_array()
            .expect("result")
            .iter()
            .find(|s| s["stream"]["app"] == "checkout")
            .expect("the checkout stream");
        let vals = checkout["values"].as_array().expect("values");
        assert_eq!(
            vals[0][2],
            serde_json::json!({
                "structuredMetadata": {"trace_id": "abc"},
                "parsed": {"lvl": "warn"}
            })
        );
        assert_eq!(
            vals[1][2],
            serde_json::json!({"structuredMetadata": {"user_id": "42"}})
        );
        assert_eq!(
            vals[2][2],
            serde_json::json!({"parsed": {"__error__": "JSONParserErr"}})
        );
        assert_eq!(vals[3][2], serde_json::json!({}));
        // Key ORDER inside the third element is load-bearing on the wire,
        // and `serde_json::Value` comparison cannot see it.
        let both =
            body.contains(r#""structuredMetadata":{"trace_id":"abc"},"parsed":{"lvl":"warn"}"#);
        assert!(both, "structuredMetadata must precede parsed: {body}");
    }

    // --- Criterion 16: the tail's allocation and peak profile.

    /// N single-entry streams, optionally categorised with an EMPTY
    /// third element — the shape whose flag-on/flag-off delta is exactly
    /// the two things issue #463 adds to this path.
    fn c463_tail_items(n: usize, categorize: bool) -> Vec<StreamResult> {
        (0..n)
            .map(|i| StreamResult {
                fingerprint: i as u64,
                service: "svc".to_string(),
                labels_json: format!(r#"{{"app":"a{i:04}","service_name":"svc"}}"#),
                entries: vec![(1_700_000_000_000_000_000i64 + i as i64, "line".to_string())],
                categories: if categorize {
                    vec![EntryCategories::default()]
                } else {
                    Vec::new()
                },
            })
            .collect()
    }

    /// **Criterion 16 — the tail path's cost, gated on the SLOPE with the
    /// intercept enumerated.**
    ///
    /// The tail cannot stream: it builds one `String` and must not grow a
    /// whole-item temporary beside it. What issue #463 costs here is two
    /// envelope `Vec`s that do not scale with streams or entries; there
    /// is no per-item cost and no boxed closure. So the per-stream cost
    /// may not move, and the fixed cost is enumerated rather than left as
    /// an allowance.
    ///
    /// The flag-off increments below are the pre-#463 tree's, measured
    /// at `bf3a8f6` by checking it out in this worktree and running the
    /// same fixture through the same probe: `n=1 allocs=2 peak=222`,
    /// `n=200 allocs=3 peak=24102` — increments `(1, 23 880)`.
    /// `tail_frame` therefore keeps the pre-#463 per-stream RESERVATION
    /// term verbatim; only the envelope intercept moved, which this
    /// clause permits.
    #[test]
    fn the_tail_frame_keeps_its_allocation_profile() {
        // The fixture is built OUTSIDE `measure`: this gates the
        // RENDERER's profile, and `StreamResult`'s own width is a
        // separate, per-stream question the streams-result budget owns.
        let off = |n: usize| {
            let items = c463_tail_items(n, false);
            crate::probe_alloc::measure(move || tail_frame(items, &[], 0, &[]))
        };
        let on = |n: usize| {
            let items = c463_tail_items(n, true);
            let f = flags(&["categorize-labels"]);
            crate::probe_alloc::measure(move || tail_frame(items, &[], 0, &f))
        };

        // 16(a) — the PER-STREAM cost may not move. The fixed cost may.
        let (_, a1, p1) = off(1);
        let (_, a200, p200) = off(200);
        assert_eq!(
            (a200 - a1, p200 - p1),
            (1, 23_880),
            "the flag-off tail's per-stream allocation/peak slope moved"
        );
        // Stated as a relation as well as a literal: whatever the
        // reservation, the tail may not allocate per stream.
        assert!(
            a200 - a1 <= 1,
            "the tail allocated {a200} buffers for 200 streams and {a1} for one — a per-item              allocation appeared"
        );

        // 16(b-i) — NO per-item allocation appears with the flag on,
        // and no fixed one either. This is the clause that catches a
        // whole-item temporary, and it is independent of any byte
        // accounting: `streams_render` reserves both envelope halves
        // exactly and writes the echo in place, so turning the flag on
        // costs no allocation at all.
        for n in [1usize, 200] {
            let (_, allocs_off, _) = off(n);
            let (_, allocs_on, _) = on(n);
            assert_eq!(
                allocs_on, allocs_off,
                "n={n}: the categorised tail allocated {allocs_on} where the plain one \
                 allocated {allocs_off} — a temporary appeared"
            );
        }

        // 16(b-ii) — the intercept is exactly the two things that
        // changed: one empty third element per entry, and the
        // advertisement. Both are COMPUTED from what production emits,
        // never asserted as a constant.
        let items = c463_tail_items(1, true);
        let advertisement = {
            let render = streams_render(
                StreamsEnvelope::Tail {
                    dropped: &[],
                    dropped_total: 0,
                },
                &flags(&["categorize-labels"]),
                &items,
                LabelBytes::Verbatim,
            );
            let plain = streams_render(
                StreamsEnvelope::Tail {
                    dropped: &[],
                    dropped_total: 0,
                },
                &[],
                &items,
                LabelBytes::Verbatim,
            );
            render.suffix.len() - plain.suffix.len()
        };
        let (body_off, _, _) = off(1);
        let (body_on, _, _) = on(1);
        assert_eq!(
            body_on.len() - body_off.len(),
            THIRD_ELEMENT_EMPTY.len() + advertisement,
            "the categorised frame grew by something other than one empty third element \
             plus the advertisement"
        );

        // 16(b-iii) — the peak delta is attributable to rendered bytes
        // and nothing else.
        //
        // Stated scale-invariantly, because the exact form is what a
        // staged envelope makes true: the advertisement is live twice
        // while the frame is assembled — once in `suffix`, once copied
        // into `buf` — and its `String` over-reserves geometrically
        // while it is built. Every one of those terms is a function of
        // the ADVERTISEMENT's length, none of the frame's. So the
        // property is that the peak overhead the flag adds, over and
        // above the bytes it renders, does not grow with the frame.
        //
        // Measured: 38 B at N = 1 and 38 B at N = 200 — exactly the
        // advertisement's own bytes, held in `suffix` while the frame
        // buffer holds its copy.
        let overhead = |n: usize| -> i64 {
            let (body_off, _, peak_off) = off(n);
            let (body_on, _, peak_on) = on(n);
            (peak_on as i64 - peak_off as i64) - (body_on.len() as i64 - body_off.len() as i64)
        };
        let (o1, o200) = (overhead(1), overhead(200));
        assert_eq!(
            o1, o200,
            "the categorised tail's non-rendered peak overhead grows with the frame: \
             {o1} B at N=1 against {o200} B at N=200"
        );
        assert_eq!(o1, 38, "the categorised tail's fixed peak overhead moved");

        // 16(c) — the fixed cost, enumerated. Wrapping ONLY the
        // `streams_render` call, exactly two allocations: `prefix` and
        // `suffix`. A third buffer, or a reintroduced boxed closure,
        // reds here.
        let f = flags(&["categorize-labels"]);
        let (_, allocs, _) = crate::probe_alloc::measure(|| {
            streams_render(
                StreamsEnvelope::Tail {
                    dropped: &[],
                    dropped_total: 0,
                },
                &f,
                &items,
                LabelBytes::Verbatim,
            )
        });
        eprintln!("c463 streams_render allocations: {allocs}");
        assert_eq!(
            allocs, 2,
            "streams_render must allocate exactly the two envelope buffers"
        );
    }

    // --- Criterion 10: the encoded-body factor for the CATEGORISED shape.

    /// Issue #463's categorised worst-case fixture, driven through the
    /// REAL fast-path accumulator so both halves of the ratio come from
    /// shipped code: 200 entries, each a 512-byte all-`\u{0001}` line,
    /// in the `variant` third-element framing.
    ///
    /// The categories are DERIVED from structured metadata by the
    /// accumulator, not injected: a reserved `__error__` pair is routed
    /// to the error slots and files under `parsed`, an ordinary pair
    /// files under `structuredMetadata`. That is what makes this measure
    /// the shipped split rather than a transcription of it.
    fn c463_factor_case(variant: usize) -> (u64, u64) {
        use pulsus_read::logql::MAX_STREAMS_RESULT_BYTES;
        use pulsus_read::logql::exec::StreamsFastPathProbe;
        use pulsus_read::logql::rows::{SampleRow, StreamMetaRow};

        let ctrl: String = std::iter::repeat_n('\u{1}', 512).collect();
        let meta = std::collections::HashMap::from([(
            1u64,
            StreamMetaRow {
                fingerprint: 1,
                service: "svc".to_string(),
                labels: r#"{"service_name":"svc"}"#.to_string(),
            },
        )]);
        let esc = serde_json::to_string(&ctrl).expect("escape");
        let sm = match variant {
            // structuredMetadata + parsed
            0 => format!(r#"{{"kk1":{esc},"__error__":{esc}}}"#),
            // structuredMetadata only
            1 => format!(r#"{{"kk1":{esc}}}"#),
            // parsed only
            2 => format!(r#"{{"__error__":{esc}}}"#),
            // neither — the `{}` framing
            _ => String::new(),
        };
        let mut probe = StreamsFastPathProbe::with_cap_categorized(MAX_STREAMS_RESULT_BYTES);
        for i in 0..200 {
            probe
                .push_row(
                    SampleRow {
                        fingerprint: 1,
                        timestamp_ns: 1_700_000_000_000_000_000i64 + i,
                        body: ctrl.clone(),
                        structured_metadata: sm.clone(),
                    },
                    &meta,
                )
                .expect("well inside the shipped cap");
        }
        let charged = probe.charged();
        let items = probe.into_streams();
        assert_eq!(items.len(), 1, "variant {variant}: one stream expected");
        let w = query_item_writer(&flags(&["categorize-labels"]), &items);
        let rendered: u64 = items.iter().map(|s| item_chunk(w, s).len() as u64).sum();
        (rendered, charged)
    }

    /// Reads one integer factor out of the `streams-result-budget`
    /// ledger entry's anchor line, so breaking the DOCUMENT alone fails
    /// with no recompilation.
    fn c463_ledger_factor(anchor: &str) -> u64 {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let ledger =
            std::fs::read_to_string(root.join("docs/benchmarks/logs-differential-ledger.md"))
                .expect("ledger readable");
        let tail = ledger
            .split(anchor)
            .nth(1)
            .unwrap_or_else(|| panic!("the ledger carries the anchor {anchor:?}"));
        tail.split(" ×`")
            .next()
            .expect("the anchor names a factor")
            .trim()
            .parse()
            .expect("the documented factor is an integer")
    }

    /// **Criterion 10 — the encoded-body factor holds for the
    /// CATEGORISED shape, on its own anchor.**
    ///
    /// A single anchor could not be made to bind here, and the reason is
    /// arithmetic rather than a choice: `alloc_block_bytes(n) = max(2n,
    /// 32)` and `grown_alloc_bytes(n) = 3·alloc_block_bytes(n)`, so every
    /// shape's rendered/charged tends to 3 from below — a C0 byte renders
    /// as six characters and is charged two — while the third element
    /// adds strictly more FIXED per-entry overhead than amplification. A
    /// clause requiring the categorised ratio to EXCEED the plain one is
    /// therefore unsatisfiable, and this is a second anchor instead.
    ///
    /// Two-sided on the categorised ratio alone: neither clause mentions
    /// the plain fixture, so neither can be satisfied by it.
    #[test]
    fn the_categorised_body_factor_matches_what_the_renderer_produces() {
        const ANCHOR: &str = "- **Categorised encoded-body factor (#463):** at most `";
        let f_cat = c463_ledger_factor(ANCHOR);
        let mut worst = 0.0f64;
        let mut worst_variant = 0usize;
        let mut worst_pair = (0u64, 0u64);
        for variant in 0..4 {
            let (rendered, charged) = c463_factor_case(variant);
            let ratio = rendered as f64 / charged as f64;
            eprintln!(
                "c463 F_cat variant={variant} rendered={rendered} charged={charged} \
                 ratio={ratio:.3}"
            );
            assert!(
                rendered <= f_cat * charged,
                "variant {variant}: the renderer emitted {rendered} B for {charged} charged B \
                 — MORE than the documented factor {f_cat}x. The documented bound is too small."
            );
            if ratio > worst {
                worst = ratio;
                worst_variant = variant;
                worst_pair = (rendered, charged);
            }
        }
        let (rendered, charged) = worst_pair;
        assert!(
            rendered > (f_cat - 1) * charged,
            "the widest categorised variant ({worst_variant}) emitted only {rendered} B for \
             {charged} charged B — under {}x. The documented factor {f_cat}x is not tight, or \
             this corpus is no longer the worst case.",
            f_cat - 1
        );
        eprintln!("c463 F_cat worst variant={worst_variant} ratio={worst:.3} documented={f_cat}");
    }

    /// **Criterion 10(d) — the ledger records the DERIVATION, not just
    /// the number.** The four ratios and the binding variant's two byte
    /// totals are parsed out of the ledger and compared against what the
    /// fixture actually produces, so the derivation cannot go stale
    /// beside a still-correct factor.
    #[test]
    fn the_categorised_factor_derivation_matches_the_fixture() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let ledger =
            std::fs::read_to_string(root.join("docs/benchmarks/logs-differential-ledger.md"))
                .expect("ledger readable");
        let (rendered, charged) = c463_factor_case(0);
        let ratio = rendered as f64 / charged as f64;
        // The recorded totals, in the row's own thousands-separated form.
        let sep = |n: u64| {
            let d = n.to_string();
            let mut out = String::new();
            for (i, c) in d.chars().enumerate() {
                if i > 0 && (d.len() - i).is_multiple_of(3) {
                    out.push(',');
                }
                out.push(c);
            }
            out
        };
        for (what, needle) in [
            (
                "the rendered total",
                format!("`rendered = {} B`", sep(rendered)),
            ),
            (
                "the charged total",
                format!("`charged = {} B`", sep(charged)),
            ),
            ("the binding ratio", format!("ratio **{ratio:.3}**")),
        ] {
            assert!(
                ledger.contains(&needle),
                "{what} the fixture produces ({needle}) is not what the \
                 streams-result-budget entry records"
            );
        }
        for variant in 1..4 {
            let (r, c) = c463_factor_case(variant);
            let recorded = format!("{:.3}", r as f64 / c as f64);
            assert!(
                ledger.contains(&recorded),
                "variant {variant}'s ratio {recorded} is not recorded in the ledger"
            );
        }
    }

    /// **Criterion 10(e) — the ledger's term list and the charge
    /// function's destructured bindings are the same set.**
    ///
    /// `entry_category_bytes` destructures `EntryCategories`, so a new
    /// field without a term is a build failure. This is the other half:
    /// a new field WITH a term but no ledger row would leave the
    /// published derivation describing a narrower charge than the one
    /// that ships.
    #[test]
    fn the_categorised_charge_terms_match_the_destructured_bindings() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let charge = std::fs::read_to_string(root.join("crates/pulsus-read/src/logql/charge.rs"))
            .expect("charge.rs readable");
        let at = charge
            .find("let super::exec::EntryCategories {")
            .expect("entry_category_bytes destructures EntryCategories");
        let body = &charge[at..at
            + charge[at..]
                .find("} = cats;")
                .expect("destructuring closes")];
        let mut bindings: Vec<String> = body
            .lines()
            .skip(1)
            .filter_map(|l| l.trim().strip_suffix(','))
            .map(str::to_string)
            .collect();
        bindings.sort();
        assert_eq!(
            bindings,
            vec!["parsed".to_string(), "structured_metadata".to_string()],
            "the destructured bindings moved"
        );

        let ledger =
            std::fs::read_to_string(root.join("docs/benchmarks/logs-differential-ledger.md"))
                .expect("ledger readable");
        let at = ledger
            .find("**The charge's term list**")
            .expect("the streams-result-budget entry carries the term list");
        let list = &ledger[at..at + 500];
        for binding in &bindings {
            assert!(
                list.contains(&format!("`{binding}`")),
                "the ledger's term list does not name `{binding}`"
            );
        }
        for term in [
            "`alloc_block_bytes`",
            "`grown_alloc_bytes`",
            "`size_of::<EntryCategories>()`",
        ] {
            assert!(
                list.contains(term),
                "the ledger's term list does not name {term}"
            );
        }
    }
}
