//! The seam between the OTLP traces receiver (`POST /v1/traces`, issue
//! #54) and the writer core: [`TraceSink`] plus the types an admitted
//! batch carries. Pure data + trait, no I/O — mirrors `ingest/metrics.rs`'s
//! `ParsedMetrics`/`MetricSink` split (issue #26/#27's precedent), minus
//! everything traces do not have: no fingerprints, no label sets (so no
//! `collisions`), no registration/metadata carriers (`trace_tag_catalog` is
//! MV-populated, never writer-written — issue #53).

use crate::ingest::{AdmitRefusal, FlushWait, PushHeaders};

/// One `trace_spans` row's source data (docs/schemas.md §4.1), produced by
/// the OTLP traces parser. IDs are raw wire bytes (`FixedString(16)`/
/// `FixedString(8)` columns); `payload` is a self-contained
/// single-`ResourceSpans` `TracesData` protobuf (this span + its resource +
/// its scope), the pinned T2/T3 contract — T3 decodes each span's payload
/// independently and concatenates into a valid `TracesData`.
#[derive(Debug, Clone, PartialEq)]
pub struct SpanRecord {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    /// `[0u8; 8]` for a root span (empty `parent_span_id` on the wire).
    pub parent_id: [u8; 8],
    pub name: String,
    /// String-rendering of resource attr `service.name`, verbatim (not
    /// normalized — docs/architecture.md §2.3), `""` when absent.
    pub service: String,
    pub timestamp_ns: i64,
    pub duration_ns: i64,
    pub status_code: i8,
    /// OTLP `Status.message` verbatim, `""` when absent (issue #184). Stored
    /// on `trace_spans` so the `statusMessage` / `span:statusMessage`
    /// intrinsic is queryable.
    pub status_message: String,
    pub kind: i8,
    /// `1` iff the span carried the `zipkin.shared = "true"` attribute at
    /// parse time (issue #173): a Zipkin shared span (both RPC sides stored
    /// under the same `span_id`), else `0`. Promoted onto `trace_spans` so
    /// the service-graph edge MV can key a shared server half by its own
    /// `span_id` rather than its inherited `parent_id` — the exact wire
    /// contract `zipkin.rs` emits, which an OTLP-native sender may also set.
    pub shared: u8,
    /// OTLP `InstrumentationScope.name`/`version` verbatim, `""` when the
    /// scope is absent (issue #192). Stored on `trace_spans` so the
    /// `instrumentation:name`/`instrumentation:version` intrinsics are
    /// queryable and `compare()` has a per-span source.
    pub scope_name: String,
    pub scope_version: String,
    /// Encoded single-`ResourceSpans` `TracesData` (see above).
    pub payload: Vec<u8>,
    /// This span's OWN attribute rows, projected element-wise into five
    /// ALIGNED arrays (issue #556, part of #537): `trace_spans.attr_key`,
    /// `attr_scope`, `attr_val`, `attr_type`, `attr_num`. Element `i` of
    /// each array describes one attribute, and the five lengths are equal
    /// — the `attr_arrays_aligned` CHECK (migration 59) refuses a row
    /// where they are not.
    ///
    /// They carry exactly the attributes that also become
    /// [`AttrRecord`]s for this span, in the same order, and the parser
    /// DERIVES them from those records rather than recomputing them: the
    /// two stores therefore cannot disagree about an element **within one
    /// parsed span**. That is a property of the parser and not of the
    /// system — a hand-built `SpanRecord` may carry empty arrays beside
    /// real index rows, and the writer commits the two tables as
    /// independent flush generations.
    ///
    /// EVERY phase-2 attribute read comes off these — the condition
    /// (issue #557) and the `select()` / aggregate / `by()` value reads
    /// and the event/link value sets (issue #558) — through projected
    /// slots on the batch hydration statement. `trace_attrs_idx` serves
    /// phase 1, the candidate generator, and nothing else on the search
    /// route.
    pub attr_key: Vec<String>,
    pub attr_scope: Vec<String>,
    pub attr_val: Vec<String>,
    /// The declared OTLP kind, the same closed four-variant enum
    /// [`AttrRecord::val_type`] carries — never a `String`, which would
    /// accept a spelling the wire has no meaning for. The row type renders
    /// it through [`AttrValueType::as_str`].
    pub attr_type: Vec<AttrValueType>,
    /// The numeric value, decided by the whole of (scope, key, value) and
    /// NOT by the value text: a link's `spanID` of `0000000000000001`
    /// parses as `1.0` and is stored `None`, because the `AttrRecord`
    /// that built this element stores `None`.
    pub attr_num: Vec<Option<f64>>,
}

/// The OTLP value kind an attribute arrived as, carried to
/// `trace_attrs_idx.val_type` and from there, through
/// `trace_tag_catalog_mv`, to the `type` field of
/// `GET /api/v2/search/tag/{tag}/values` (issue #476).
///
/// The point of the column is that the wire `type` is the type the sender
/// SENT, not a guess about how `val`'s text reads. Before it existed, a
/// string attribute whose value was `12345` was reported `int`, and a
/// client that quotes only `string`-typed values built
/// `{resource.service.name=12345}` from its own dropdown.
///
/// These four spellings ARE the stored values and the wire `type` — the
/// reference's complete attribute-type domain. Changing one is a wire
/// change and a stored-data change at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrValueType {
    String,
    Int,
    Float,
    Bool,
}

impl AttrValueType {
    /// The stored/wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            AttrValueType::String => "string",
            AttrValueType::Int => "int",
            AttrValueType::Float => "float",
            AttrValueType::Bool => "bool",
        }
    }
}

/// One `trace_attrs_idx` row's source data (docs/schemas.md §4.1): one
/// resource or span attribute of one span, key verbatim (never normalized —
/// docs/architecture.md §2.3), discriminated by `scope`.
#[derive(Debug, Clone, PartialEq)]
pub struct AttrRecord {
    /// The span's UTC **day** since the Unix epoch
    /// (`pulsus_model::Date::start_of_day_utc_datetime_safe` —
    /// `trace_attrs_idx` is `PARTITION BY date`, daily, unlike
    /// `log_streams.month`; capped at day 49_709 = 2106-02-06 so the trace
    /// tables' 32-bit-DateTime-domain delete-TTL input cannot wrap, issue
    /// #131).
    pub date: u16,
    pub key: String,
    /// The index's scope discriminator, so same-key attributes never collide
    /// across scopes (issue #54 plan v2 delta 1): `'resource'`, `'span'`, or
    /// `'instrumentation'` for the three attribute scopes; plus the span-event
    /// scopes `'event'` (event attributes, verbatim keys) and the dedicated
    /// `'event:intrinsic'` (reserved keys `name`/`timeSinceStart`) that the
    /// writer emits ONLY from intrinsic code — a hard namespace partition
    /// (issue #192 PR-B). Instrumentation-scope (`InstrumentationScope`)
    /// attributes are indexed under `scope='instrumentation'` (issue #192,
    /// superseding the #54 decision that dropped them). All of these also
    /// stay in the span payload.
    pub scope: String,
    pub val: String,
    /// The OTLP kind [`Self::val`] was rendered FROM (issue #476). Not
    /// derivable from `val`: the string `"1.5"` and the double `1.5`
    /// render to the same six bytes.
    pub val_type: AttrValueType,
    /// `val.parse::<f64>()` when finite, else `None` (`Nullable(Float64)`).
    ///
    /// A parse of the RENDERED TEXT, so it is not evidence of the sender's
    /// type — a string attribute reading `1.5` gets `Some(1.5)`. It serves
    /// TraceQL numeric filtering and aggregation; [`Self::val_type`] is
    /// what the tag-values `type` reports.
    pub val_num: Option<f64>,
    pub timestamp_ns: i64,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub duration_ns: i64,
}

/// The normalized output the OTLP traces parser hands a [`TraceSink`]:
/// rows destined for `trace_spans` and `trace_attrs_idx`, plus the
/// per-request partial-success accounting (`rejected`, `rejected_message`).
/// No `collisions` counter — traces have no `LabelSet` (verbatim keys, no
/// normalization, nothing to collide).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ParsedTraces {
    /// One per accepted span.
    pub spans: Vec<SpanRecord>,
    /// One per indexed row of every accepted span: its resource ⊕ span ⊕
    /// instrumentation-scope attributes, plus each span event's intrinsic
    /// rows (`event:intrinsic` name/timeSinceStart) and event attributes
    /// (`event` scope) — issue #192.
    pub attrs: Vec<AttrRecord>,
    /// Count of individual spans dropped during parsing (not requests — a
    /// malformed/truncated payload is a whole-request error, never a
    /// `rejected` count).
    pub rejected: u64,
    /// The first rejection's error message, surfaced verbatim as the OTLP
    /// `partial_success.error_message`.
    pub rejected_message: Option<String>,
}

/// The boundary the OTLP traces handler hands parsed batches across:
/// admission only, no batching/flush/ClickHouse-write logic lives on this
/// side. `Send + Sync` because a server holds an implementor behind
/// `axum::extract::State`, shared across concurrently-handled requests —
/// mirrors [`crate::ingest::LogSink`]/[`crate::ingest::metrics::MetricSink`]
/// exactly, including the reuse of [`FlushWait`] (whose `Output` is
/// `Result<(), LogsIngestError>` — see `MetricSink`'s doc comment for the
/// task-manager resolution deferring a neutral `IngestError` rename to M6).
/// **Three arguments, one more than the log and metric sinks take, and one
/// decode more than this seam used to carry** (issue #586). A trace push
/// feeds two paths: `batch` is the old two-table path's rows and `landing`
/// is the landing path's. Both are decoded in the handler, because the
/// landing decode's own refusal — a value carrying more stored JSON paths
/// than `format_binary_max_object_size` admits — is a decode-class failure
/// that leaves through each route's whole-request error writer rather than
/// through a refusal mapper, and an [`AdmitRefusal`] carries no decode class.
///
/// `push` carries the request's `Idempotency-Key`/`Retry-Attempt` headers.
/// A handler that forwards a default answers a client success over a push it
/// never stored: two bodies carrying identical content under different keys
/// resolve to one identity, and the second is suppressed before either
/// path's insert.
pub trait TraceSink: Send + Sync {
    /// Admits `batch` for async-mode requests: the caller responds
    /// immediately once this returns `Ok`, without waiting for the batch
    /// to be flushed.
    fn admit(
        &self,
        batch: ParsedTraces,
        landing: ParsedTraceLanding,
        push: PushHeaders,
    ) -> Result<(), AdmitRefusal>;

    /// Admits `batch` for sync-mode requests: the caller `.await`s the
    /// returned [`FlushWait`] before responding. For a suppressed push the
    /// returned wait resolves to **the original push's** outcome.
    fn admit_flush(
        &self,
        batch: ParsedTraces,
        landing: ParsedTraceLanding,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_record_stores_fields_verbatim() {
        let span = SpanRecord {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_id: [0; 8],
            name: "op-a".to_string(),
            service: "checkout".to_string(),
            timestamp_ns: 1_700_000_000_000_000_000,
            duration_ns: 42,
            status_code: 2,
            status_message: String::new(),
            kind: 3,
            shared: 0,
            scope_name: "io.otel.http".to_string(),
            scope_version: "1.4.2".to_string(),
            payload: vec![0xDE, 0xAD],
            // Hand-built: no attribute arrays (issue #556). Five EMPTY arrays
            // satisfy the `attr_arrays_aligned` CHECK — 0 = 0 = 0 = 0 = 0.
            attr_key: Vec::new(),
            attr_scope: Vec::new(),
            attr_val: Vec::new(),
            attr_type: Vec::new(),
            attr_num: Vec::new(),
        };
        assert_eq!(span.trace_id, [1; 16]);
        assert_eq!(span.span_id, [2; 8]);
        assert_eq!(span.parent_id, [0; 8]);
        assert_eq!(span.name, "op-a");
        assert_eq!(span.service, "checkout");
        assert_eq!(span.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(span.duration_ns, 42);
        assert_eq!(span.status_code, 2);
        assert_eq!(span.kind, 3);
        assert_eq!(span.shared, 0);
        assert_eq!(span.scope_name, "io.otel.http");
        assert_eq!(span.scope_version, "1.4.2");
        assert_eq!(span.payload, vec![0xDE, 0xAD]);
    }

    #[test]
    fn parsed_traces_default_is_empty() {
        let parsed = ParsedTraces::default();
        assert!(parsed.spans.is_empty());
        assert!(parsed.attrs.is_empty());
        assert_eq!(parsed.rejected, 0);
        assert_eq!(parsed.rejected_message, None);
    }
}

// === The landing-shaped decode (issues #584 to #586) ======================
//
// `SpanRecord`/`AttrRecord` above are the OLD two-table path's shape and are
// untouched: the reads that have not moved still answer from `trace_spans`
// and `trace_attrs_idx`. The types below are what the landing path decodes
// into, and they carry what that path stores and the old one does not —
// events, links, the scope's attributes, the resource's attributes, the
// trace state, the flags and the four dropped counts, all of which the old
// path keeps inside the span's protobuf `payload` blob instead.
//
// **The decode is a second walk over the same request, not a projection of
// the first.** A `SpanRecord` cannot be widened into one of these: it holds
// a rendered-text view of the attributes it indexed and no resource
// identity, and the two walks are kept independent so neither path's
// behaviour depends on the other's.

use pulsus_model::Fingerprint;

use crate::writer::trace_json::TraceJson;

/// One span event, as the `events` column's element tuple stores it.
#[derive(Debug, Clone, PartialEq)]
pub struct LandingEvent {
    /// The protocol's own `fixed64`, unsaturated: an event time above
    /// `i64::MAX` round-trips, which the reference's unsigned offset does
    /// too (issue #587 row 11).
    pub time_ns: u64,
    pub name: String,
    pub attrs: TraceJson,
    /// This event's own keys whose values no JSON path can hold, in
    /// [`LandingSpan::attrs_other`]'s carrier (issue #587 row 4).
    pub attrs_other: Vec<u8>,
    pub dropped_attrs: u32,
}

/// One span link, as the `links` column's element tuple stores it.
#[derive(Debug, Clone, PartialEq)]
pub struct LandingLink {
    /// The bytes the sender put on the wire, whatever their length: a link
    /// points at a span in another system and is never length-validated
    /// here (issue #587 row 7).
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub trace_state: String,
    pub flags: u32,
    pub attrs: TraceJson,
    /// This link's own keys whose values no JSON path can hold, in
    /// [`LandingSpan::attrs_other`]'s carrier (issue #587 row 4).
    pub attrs_other: Vec<u8>,
    pub dropped_attrs: u32,
}

/// One decoded span, in the shape the kind-0 landing row is built from.
#[derive(Debug, Clone, PartialEq)]
pub struct LandingSpan {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    /// `[0u8; 8]` for a root span, which is what `traces_mv`'s
    /// `parent_span_id = toFixedString('', 8)` tests.
    pub parent_span_id: [u8; 8],
    pub start_ns: i64,
    pub duration_ns: i64,
    /// The resource this span belongs to, which is also the key of the
    /// kind-1 row the push lands for it.
    pub resource_id: Fingerprint,
    pub name: String,
    /// The protocol's own signed value, stored as it arrived: the reference
    /// stores a signed integer kind and returns it verbatim (issue #587
    /// row 7).
    pub kind: i32,
    /// The protocol's own signed value, stored as it arrived (issue #587
    /// row 12). Narrowing it to a byte turns an out-of-range code into
    /// `STATUS_CODE_UNSET`, which is silent in a response.
    pub status_code: i32,
    pub status_message: String,
    pub trace_state: String,
    pub flags: u32,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attrs: TraceJson,
    /// `ScopeSpans.schema_url` (issue #587 row 1).
    pub scope_schema_url: String,
    /// `InstrumentationScope.dropped_attributes_count` (issue #587 row 2).
    pub scope_dropped_attrs: u32,
    /// The scope's own keys whose values no JSON path can hold, in
    /// [`Self::attrs_other`]'s carrier (issue #587 row 3).
    pub scope_attrs_other: Vec<u8>,
    /// The sender's `end_time_unix_nano`, verbatim — no clamp, no
    /// substitution, no rejection. `duration_ns` above keeps its clamped
    /// value (issue #587 row 9).
    pub end_ns: u64,
    pub events: Vec<LandingEvent>,
    pub dropped_events: u32,
    pub links: Vec<LandingLink>,
    pub dropped_links: u32,
    /// The resource's `service.name`, rendered verbatim, `""` when absent.
    pub service: String,
    pub attrs: TraceJson,
    /// The protobuf serialization of an
    /// `opentelemetry.proto.common.v1.KeyValueList` carrying the keys whose
    /// values no JSON path can hold, under their **original OTLP keys**
    /// rather than escaped paths — they are not JSON paths.
    pub attrs_other: Vec<u8>,
    pub dropped_attrs: u32,
}

/// One resource, as the kind-1 landing row stores it: one per distinct
/// `(resource_id, day)` **in the push**.
///
/// `day` is the UTC day of the spans that resource carried in this push, so
/// a push whose spans straddle midnight emits two rows for one resource.
/// That is what `resources`' "one row per distinct resource per day" means,
/// and a resource joined at read time by a day-bounded predicate needs a row
/// in each day it appears in.
#[derive(Debug, Clone, PartialEq)]
pub struct LandingResource {
    pub resource_id: Fingerprint,
    /// Days since the Unix epoch, the bare `u16` the `Date` column takes.
    pub day: u16,
    pub service: String,
    /// The resource's attributes **without** `service.name`: the span row
    /// carries it as a column. The identity above still covers it — see
    /// `otlp_traces`'s resource-identity note.
    pub attrs: TraceJson,
    pub attrs_other: Vec<u8>,
    pub dropped_attrs: u32,
    pub schema_url: String,
    /// `Resource.entity_refs`, as an
    /// `opentelemetry.proto.resource.v1.Resource` with only field 3
    /// populated — zero bytes when there are none (issue #587 row 8).
    pub entity_refs: Vec<u8>,
}

/// The five attribute scopes the tag catalogs discriminate on, which are the
/// five `docs/TraceQL/measure/schema.sql`'s own `tag_names` comment admits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TagScope {
    Span,
    Resource,
    Event,
    Link,
    Instrumentation,
}

impl TagScope {
    /// The stored spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            TagScope::Span => "span",
            TagScope::Resource => "resource",
            TagScope::Event => "event",
            TagScope::Link => "link",
            TagScope::Instrumentation => "instrumentation",
        }
    }
}

/// One tag name, as the kind-2 landing row stores it: one per distinct
/// `(scope, key)` **in the push**.
///
/// The key is the **original OTLP key**, not the escaped JSON path: the
/// catalog answers `GET /api/v2/search/tags`, whose values are the keys a
/// client writes in a query.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LandingTagName {
    pub scope: TagScope,
    pub key: String,
}

/// One tag value, as the kind-3 landing row stores it: one per distinct
/// `(scope, key, value, type)` **in the push**.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LandingTagValue {
    pub scope: TagScope,
    pub key: String,
    pub value: String,
    /// `string`, `int`, `float` or `bool` — the four values
    /// `tag_values`' own comment admits.
    pub val_type: &'static str,
}

/// The landing-shaped decode of one trace push: the four landed event
/// shapes, each already deduplicated **inside the push**, plus the
/// per-request partial-success accounting.
///
/// **There is no cache of anything already written.** Kinds 1, 2 and 3 are
/// deduplicated from this push's own decoded spans and nothing else: no
/// per-process set, no LRU, no cross-push state. A push that fails re-emits
/// everything next time. The two shipped signals can key a commit-promoted
/// LRU on a time bucket, so a registration lost to a fan-out failure
/// reappears at the next bucket; `tag_names` and `tag_values` are time-less
/// by contract (`docs/api.md` §4.3) and have no heal interval at all.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ParsedTraceLanding {
    pub spans: Vec<LandingSpan>,
    pub resources: Vec<LandingResource>,
    pub tag_names: Vec<LandingTagName>,
    pub tag_values: Vec<LandingTagValue>,
    /// Count of individual spans dropped during parsing.
    pub rejected: u64,
    /// The first rejection's error message, surfaced verbatim as the OTLP
    /// `partial_success.error_message`.
    pub rejected_message: Option<String>,
}

impl ParsedTraceLanding {
    /// The landed rows this push produces, over all four kinds. The per-push
    /// row ceiling is counted over this figure: a case counting kind 0 alone
    /// passes at a push the server would split.
    pub fn total_rows(&self) -> u64 {
        (self.spans.len() + self.resources.len() + self.tag_names.len() + self.tag_values.len())
            as u64
    }

    /// `true` when the push carried no span at all. Such a push makes no
    /// block and no insert and is charged for nothing.
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
            && self.resources.is_empty()
            && self.tag_names.is_empty()
            && self.tag_values.is_empty()
    }
}
