//! The seam between the OTLP traces receiver (`POST /v1/traces`, issue
//! #54) and the writer core: [`TraceSink`] plus the types an admitted
//! batch carries. Pure data + trait, no I/O — mirrors `ingest/metrics.rs`'s
//! `ParsedMetrics`/`MetricSink` split (issue #26/#27's precedent), minus
//! everything traces do not have: no fingerprints and no label sets (so no
//! `collisions`).

use crate::ingest::{AdmitRefusal, FlushWait, PushHeaders};

/// The boundary the OTLP traces handler hands a decoded push across:
/// admission only, no batching, flush or ClickHouse-write logic lives on this
/// side. `Send + Sync` because a server holds an implementor behind
/// `axum::extract::State`, shared across concurrently-handled requests —
/// mirrors [`crate::ingest::LogSink`]/[`crate::ingest::metrics::MetricSink`].
///
/// `landing` is decoded in the handler, because its own refusal — a value
/// carrying more stored JSON paths than `format_binary_max_object_size`
/// admits — is a decode-class failure that leaves through each route's
/// whole-request error writer, and an [`AdmitRefusal`] carries no decode
/// class. `push` carries the request's `Idempotency-Key`/`Retry-Attempt`
/// headers.
pub trait TraceSink: Send + Sync {
    /// Admits `landing` for async-mode requests: the caller responds
    /// immediately once this returns `Ok`.
    fn admit(&self, landing: ParsedTraceLanding, push: PushHeaders) -> Result<(), AdmitRefusal>;

    /// Admits `landing` for sync-mode requests: the caller `.await`s the
    /// returned [`FlushWait`], which resolves once the landing insert has
    /// committed or failed. For a suppressed push it resolves to **the
    /// original push's** outcome.
    fn admit_flush(
        &self,
        landing: ParsedTraceLanding,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal>;
}

// === The landing-shaped decode (issues #584 to #586) ======================
//
// What one push lands: span rows carrying their events, links and scope;
// resource rows; and the push's distinct tag names and tag values.

use pulsus_model::Fingerprint;

use pulsus_clickhouse::json_column::TraceJson;

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
    /// The `AnyValue` arm `service` was rendered from — `string`, `bool`,
    /// `int`, `double`, `array`, `kvlist`, `bytes` — or `""` for no key, no
    /// arm, or the profiling string reference (issue #589). `service`
    /// alone cannot tell the empty string from an absent key, or the
    /// integer `12345` from the string `"12345"`.
    pub service_type: &'static str,
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
    /// The latest start of a span in the push that carries this name, in
    /// Unix nanoseconds (issue #496): `tag_names` keeps the row until it is
    /// past retention, and a read lists it only while it is within. `0`
    /// while staged, set when the push's entries are collected.
    pub last_start_ns: i64,
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
    /// As [`LandingTagName::last_start_ns`], for this value.
    pub last_start_ns: i64,
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
