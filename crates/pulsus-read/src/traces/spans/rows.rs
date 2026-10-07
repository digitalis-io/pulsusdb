//! The trace fetch's result-row shapes (issue #587).
//!
//! **Field names are the projection's aliases and field order is the
//! projection's order.** RowBinary is positional, and the bench's
//! stage-name reader compares a statement's projection aliases against the
//! row's `COLUMN_NAMES`, so neither the names nor the order are free.
//!
//! ## Three row types, one per statement
//!
//! The three statements project **four**, **two** and **two** columns
//! (`fetch::indexed_fetch_sql`, `fetch::wide_fetch_sql`,
//! `fetch::fallback_fetch_sql`), so there are three row types rather than
//! one with `Option`s. [`WideFetchRow`] is identical in shape to
//! [`FallbackFetchRow`] and is kept separate so each builder has exactly
//! one row type: a row type whose column count does not match its
//! statement's is accepted on the way out and answers
//! `Code: 32 … Attempt to read after eof` on the way back
//! (`vendor/clickhouse/PATCHES.md` section 4).
//!
//! ## The outer rows derive, the nested elements must not
//!
//! The outer types are **rows**, which take `deserialize_struct`'s path
//! legitimately. The nested element types cannot:
//!
//! ```text
//!    vendor/clickhouse/src/rowbinary/de.rs   deserialize_tuple
//!        -> self.inner(SerdeType::Tuple(len))? , then visit_seq
//!    vendor/clickhouse/src/rowbinary/de.rs   deserialize_struct
//!        -> visit_seq directly, no inner(Tuple) at all
//! ```
//!
//! So a derived struct used as the element of a non-empty
//! `Array(Tuple(…))` has its **first field** validated against the
//! **tuple** type, the driver refuses the row, and the fetch fails for
//! every trace that has an event or a link. Each nested element type
//! therefore keeps its named fields for readability and implements
//! `Deserialize` by hand through `deserialize_tuple`, mirroring the write
//! side's hand-written `Serialize` in
//! `crates/pulsus-write/src/writer/rows.rs`.
//!
//! ## The eight binary fields
//!
//! `attrs_other` on the span, the scope, each event, each link and the
//! resource; a link's `trace_id` and `span_id`; and the resource's
//! `entity_refs`. Each is a binary blob in a `String` column and each is
//! read as [`serde_bytes::ByteBuf`], which reaches `SerdeType::ByteBuf` —
//! the arm `vendor/clickhouse/src/rowbinary/validation.rs` admits against a
//! `String` column, its own comment being *"allows to work with BLOB
//! strings as well"*. A plain `Vec<u8>` would deserialise through serde's
//! **sequence** path and so demand `Array(UInt8)`, and the fetch would be
//! refused with a `SchemaMismatch` naming the column.
//!
//! The span row's own `span_id`/`parent_span_id` are **not** in that set —
//! they stay `FixedString(8)`, read as `[u8; 8]`. Nor are `name`,
//! `service`, `status_message`, `trace_state`, `scope_name`,
//! `scope_version`, `scope_schema_url` or `schema_url`, which are `String`
//! columns holding text.

use pulsus_clickhouse::Row;
use pulsus_clickhouse::json_column::JsonColumn;
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::ByteBuf;

/// Statement 1's row — the indexed fetch's four projected columns.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct IndexedFetchRow {
    /// `count()` over the per-trace table's rows for this trace, read only
    /// as `!= 0`.
    pub index_rows: u64,
    /// `toUInt32(length(bk))` — the size of the stored bucket set. The cast
    /// is in the statement, not a widened field: `length` over an array
    /// returns `UInt64`, and the value's domain is `0..=4096`.
    pub bucket_count: u32,
    pub spans: Vec<FetchedSpanTuple>,
    pub resources: Vec<FetchedResourceTuple>,
}

/// Statement 1w's row — the complete-predicate read's two projected
/// columns.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct WideFetchRow {
    pub spans: Vec<FetchedSpanTuple>,
    pub resources: Vec<FetchedResourceTuple>,
}

/// Statement 2's row — the window fallback's two projected columns.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct FallbackFetchRow {
    pub spans: Vec<FetchedSpanTuple>,
    pub resources: Vec<FetchedResourceTuple>,
}

/// Which of the three statements answered a fetch.
///
/// **Not a boolean**, because two routes give `statements == 2`: the
/// truncated-set read and the window fallback. The branch is in this crate
/// and the operational counter is on the server's metric surface, so the
/// route travels on the returned value rather than being inferred from the
/// count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchRoute {
    /// Statement 1 answered.
    Indexed,
    /// The stored bucket set may have truncated, so statement 1w answered.
    TruncatedSet,
    /// The trace is not in the per-trace table, so statement 2 answered
    /// over the request's window.
    Fallback,
}

/// The request window a fetch may supply, used only by the fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// One `events` element, in the column's declared order.
#[derive(Debug, Clone, PartialEq)]
pub struct FetchedEventTuple {
    /// `UInt64` since the write side stopped saturating it.
    pub time_ns: u64,
    pub name: String,
    pub attrs: JsonColumn,
    pub attrs_other: ByteBuf,
    pub dropped_attrs: u32,
}

/// One `links` element, in the column's declared order. `trace_id` and
/// `span_id` are `String` columns holding whatever bytes the sender sent,
/// so both are byte buffers rather than fixed-width arrays.
#[derive(Debug, Clone, PartialEq)]
pub struct FetchedLinkTuple {
    pub trace_id: ByteBuf,
    pub span_id: ByteBuf,
    pub trace_state: String,
    pub flags: u32,
    pub attrs: JsonColumn,
    pub attrs_other: ByteBuf,
    pub dropped_attrs: u32,
}

/// One span, as the statements' 25-element span tuple.
///
/// Twenty-five elements: the span table's declared columns minus
/// `trace_id` (the caller supplies it) and minus `duration_ns`, with
/// `end_ns` projected in `duration_ns`'s place rather than appended last.
#[derive(Debug, Clone, PartialEq)]
pub struct FetchedSpanTuple {
    pub span_id: [u8; 8],
    pub parent_span_id: [u8; 8],
    pub start_ns: i64,
    /// The sender's `end_time_unix_nano`, verbatim.
    pub end_ns: u64,
    pub service: String,
    pub resource_id: u128,
    pub name: String,
    /// The protocol's own signed value.
    pub kind: i32,
    /// The protocol's own signed value.
    pub status_code: i32,
    pub status_message: String,
    pub trace_state: String,
    pub flags: u32,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attrs: JsonColumn,
    pub attrs: JsonColumn,
    pub attrs_other: ByteBuf,
    pub dropped_attrs: u32,
    pub events: Vec<FetchedEventTuple>,
    pub dropped_events: u32,
    pub links: Vec<FetchedLinkTuple>,
    pub dropped_links: u32,
    pub scope_schema_url: String,
    pub scope_dropped_attrs: u32,
    pub scope_attrs_other: ByteBuf,
}

/// One grouped resource, as the statements' 6-element resource tuple.
#[derive(Debug, Clone, PartialEq)]
pub struct FetchedResourceTuple {
    pub resource_id: u128,
    pub attrs: JsonColumn,
    pub attrs_other: ByteBuf,
    pub dropped_attrs: u32,
    pub schema_url: String,
    pub entity_refs: ByteBuf,
}

/// One fetched span, as the public read surface hands it over.
///
/// **The same type as the row's element**, because nothing among the 25 is
/// read-alignment-only: the caller consumes every one of them. The alias
/// is the name the public surface uses; a narrower copy would be a second
/// place for the field list to drift.
pub type FetchedSpan = FetchedSpanTuple;

/// One fetched resource, for [`FetchedSpan`]'s reason.
pub type FetchedResource = FetchedResourceTuple;

/// What a trace fetch hands back.
#[derive(Debug, Clone, PartialEq)]
pub struct FetchedTrace {
    pub spans: Vec<FetchedSpan>,
    pub resources: Vec<FetchedResource>,
    /// `1` or `2` — the statement count the measurement record publishes,
    /// exposed so a caller need not read `system.query_log`.
    pub statements: u8,
    /// Which route answered.
    ///
    /// Beside `statements` and not instead of it: the count cannot
    /// distinguish the truncated-set read from the window fallback,
    /// because both give `2`. The branch is in this crate and the
    /// operational counter is on the server's metric surface, so the route
    /// travels on the value rather than being inferred.
    pub route: FetchRoute,
}

impl FetchedTrace {
    /// The answer for a trace no statement found: no spans, no resources,
    /// one statement, the indexed route.
    pub fn empty_at_one_statement() -> Self {
        FetchedTrace {
            spans: Vec::new(),
            resources: Vec::new(),
            statements: 1,
            route: FetchRoute::Indexed,
        }
    }
}

/// Reads the next element of a tuple, naming the element that is missing
/// rather than reporting a bare length error.
fn element<'de, A: SeqAccess<'de>, T: Deserialize<'de>>(
    seq: &mut A,
    tuple: &'static str,
    field: &'static str,
) -> Result<T, A::Error> {
    seq.next_element::<T>()?.ok_or_else(|| {
        serde::de::Error::custom(format!(
            "the {tuple} tuple ended before its `{field}` element"
        ))
    })
}

/// The element counts the four nested tuple types declare, named once so
/// the deserializers, the serializers and the cases read the same numbers.
pub const SPAN_TUPLE_ELEMENTS: usize = 25;
/// See [`SPAN_TUPLE_ELEMENTS`].
pub const RESOURCE_TUPLE_ELEMENTS: usize = 6;
/// See [`SPAN_TUPLE_ELEMENTS`].
pub const EVENT_TUPLE_ELEMENTS: usize = 5;
/// See [`SPAN_TUPLE_ELEMENTS`].
pub const LINK_TUPLE_ELEMENTS: usize = 7;

// --- the events element -------------------------------------------------

impl Serialize for FetchedEventTuple {
    /// A tuple, for the reason the module doc gives. Never used by a
    /// production path — the fetch only reads — and present because
    /// `pulsus_clickhouse::ChRow` requires `Serialize` on the row that
    /// holds this.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut t = s.serialize_tuple(EVENT_TUPLE_ELEMENTS)?;
        t.serialize_element(&self.time_ns)?;
        t.serialize_element(self.name.as_str())?;
        t.serialize_element(&self.attrs)?;
        t.serialize_element(serde_bytes::Bytes::new(&self.attrs_other))?;
        t.serialize_element(&self.dropped_attrs)?;
        t.end()
    }
}

struct EventVisitor;

impl<'de> Visitor<'de> for EventVisitor {
    type Value = FetchedEventTuple;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a {EVENT_TUPLE_ELEMENTS}-element span-event tuple")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        const T: &str = "span-event";
        Ok(FetchedEventTuple {
            // `UInt64` since the write side stopped saturating it: a read
            // at the old signed width would turn a value above the signed
            // maximum into a negative one.
            time_ns: element(&mut seq, T, "time_ns")?,
            name: element(&mut seq, T, "name")?,
            attrs: element(&mut seq, T, "attrs")?,
            // The byte path: a plain byte vector would demand an array of
            // integers and the fetch would be refused.
            attrs_other: element::<_, ByteBuf>(&mut seq, T, "attrs_other")?,
            dropped_attrs: element(&mut seq, T, "dropped_attrs")?,
        })
    }
}

impl<'de> Deserialize<'de> for FetchedEventTuple {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_tuple(EVENT_TUPLE_ELEMENTS, EventVisitor)
    }
}

// --- the links element --------------------------------------------------

impl Serialize for FetchedLinkTuple {
    /// See [`FetchedEventTuple::serialize`].
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut t = s.serialize_tuple(LINK_TUPLE_ELEMENTS)?;
        t.serialize_element(serde_bytes::Bytes::new(&self.trace_id))?;
        t.serialize_element(serde_bytes::Bytes::new(&self.span_id))?;
        t.serialize_element(self.trace_state.as_str())?;
        t.serialize_element(&self.flags)?;
        t.serialize_element(&self.attrs)?;
        t.serialize_element(serde_bytes::Bytes::new(&self.attrs_other))?;
        t.serialize_element(&self.dropped_attrs)?;
        t.end()
    }
}

struct LinkVisitor;

impl<'de> Visitor<'de> for LinkVisitor {
    type Value = FetchedLinkTuple;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a {LINK_TUPLE_ELEMENTS}-element span-link tuple")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        const T: &str = "span-link";
        Ok(FetchedLinkTuple {
            // Both ids are `String` columns holding whatever bytes the
            // sender sent, so both take the byte path.
            trace_id: element::<_, ByteBuf>(&mut seq, T, "trace_id")?,
            span_id: element::<_, ByteBuf>(&mut seq, T, "span_id")?,
            trace_state: element(&mut seq, T, "trace_state")?,
            flags: element(&mut seq, T, "flags")?,
            attrs: element(&mut seq, T, "attrs")?,
            attrs_other: element::<_, ByteBuf>(&mut seq, T, "attrs_other")?,
            dropped_attrs: element(&mut seq, T, "dropped_attrs")?,
        })
    }
}

impl<'de> Deserialize<'de> for FetchedLinkTuple {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_tuple(LINK_TUPLE_ELEMENTS, LinkVisitor)
    }
}

// --- the span element ---------------------------------------------------

impl Serialize for FetchedSpanTuple {
    /// See [`FetchedEventTuple::serialize`].
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut t = s.serialize_tuple(SPAN_TUPLE_ELEMENTS)?;
        t.serialize_element(&self.span_id)?;
        t.serialize_element(&self.parent_span_id)?;
        t.serialize_element(&self.start_ns)?;
        t.serialize_element(&self.end_ns)?;
        t.serialize_element(self.service.as_str())?;
        t.serialize_element(&self.resource_id)?;
        t.serialize_element(self.name.as_str())?;
        t.serialize_element(&self.kind)?;
        t.serialize_element(&self.status_code)?;
        t.serialize_element(self.status_message.as_str())?;
        t.serialize_element(self.trace_state.as_str())?;
        t.serialize_element(&self.flags)?;
        t.serialize_element(self.scope_name.as_str())?;
        t.serialize_element(self.scope_version.as_str())?;
        t.serialize_element(&self.scope_attrs)?;
        t.serialize_element(&self.attrs)?;
        t.serialize_element(serde_bytes::Bytes::new(&self.attrs_other))?;
        t.serialize_element(&self.dropped_attrs)?;
        t.serialize_element(&self.events)?;
        t.serialize_element(&self.dropped_events)?;
        t.serialize_element(&self.links)?;
        t.serialize_element(&self.dropped_links)?;
        t.serialize_element(self.scope_schema_url.as_str())?;
        t.serialize_element(&self.scope_dropped_attrs)?;
        t.serialize_element(serde_bytes::Bytes::new(&self.scope_attrs_other))?;
        t.end()
    }
}

struct SpanVisitor;

impl<'de> Visitor<'de> for SpanVisitor {
    type Value = FetchedSpanTuple;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a {SPAN_TUPLE_ELEMENTS}-element span tuple")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        const T: &str = "span";
        Ok(FetchedSpanTuple {
            span_id: element(&mut seq, T, "span_id")?,
            parent_span_id: element(&mut seq, T, "parent_span_id")?,
            start_ns: element(&mut seq, T, "start_ns")?,
            end_ns: element(&mut seq, T, "end_ns")?,
            service: element(&mut seq, T, "service")?,
            resource_id: element(&mut seq, T, "resource_id")?,
            name: element(&mut seq, T, "name")?,
            // The protocol's own signed values. A read at the old
            // unsigned byte width would refuse the row, and a read at a
            // narrower signed one would truncate silently.
            kind: element(&mut seq, T, "kind")?,
            status_code: element(&mut seq, T, "status_code")?,
            status_message: element(&mut seq, T, "status_message")?,
            trace_state: element(&mut seq, T, "trace_state")?,
            flags: element(&mut seq, T, "flags")?,
            scope_name: element(&mut seq, T, "scope_name")?,
            scope_version: element(&mut seq, T, "scope_version")?,
            scope_attrs: element(&mut seq, T, "scope_attrs")?,
            attrs: element(&mut seq, T, "attrs")?,
            attrs_other: element::<_, ByteBuf>(&mut seq, T, "attrs_other")?,
            dropped_attrs: element(&mut seq, T, "dropped_attrs")?,
            events: element(&mut seq, T, "events")?,
            dropped_events: element(&mut seq, T, "dropped_events")?,
            links: element(&mut seq, T, "links")?,
            dropped_links: element(&mut seq, T, "dropped_links")?,
            scope_schema_url: element(&mut seq, T, "scope_schema_url")?,
            scope_dropped_attrs: element(&mut seq, T, "scope_dropped_attrs")?,
            scope_attrs_other: element::<_, ByteBuf>(&mut seq, T, "scope_attrs_other")?,
        })
    }
}

impl<'de> Deserialize<'de> for FetchedSpanTuple {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_tuple(SPAN_TUPLE_ELEMENTS, SpanVisitor)
    }
}

// --- the resources element ----------------------------------------------

impl Serialize for FetchedResourceTuple {
    /// See [`FetchedEventTuple::serialize`].
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut t = s.serialize_tuple(RESOURCE_TUPLE_ELEMENTS)?;
        t.serialize_element(&self.resource_id)?;
        t.serialize_element(&self.attrs)?;
        t.serialize_element(serde_bytes::Bytes::new(&self.attrs_other))?;
        t.serialize_element(&self.dropped_attrs)?;
        t.serialize_element(self.schema_url.as_str())?;
        t.serialize_element(serde_bytes::Bytes::new(&self.entity_refs))?;
        t.end()
    }
}

struct ResourceVisitor;

impl<'de> Visitor<'de> for ResourceVisitor {
    type Value = FetchedResourceTuple;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a {RESOURCE_TUPLE_ELEMENTS}-element resource tuple")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        const T: &str = "resource";
        Ok(FetchedResourceTuple {
            resource_id: element(&mut seq, T, "resource_id")?,
            attrs: element(&mut seq, T, "attrs")?,
            attrs_other: element::<_, ByteBuf>(&mut seq, T, "attrs_other")?,
            dropped_attrs: element(&mut seq, T, "dropped_attrs")?,
            schema_url: element(&mut seq, T, "schema_url")?,
            entity_refs: element::<_, ByteBuf>(&mut seq, T, "entity_refs")?,
        })
    }
}

impl<'de> Deserialize<'de> for FetchedResourceTuple {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_tuple(RESOURCE_TUPLE_ELEMENTS, ResourceVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three row types' column names and counts are the three
    /// statements' projection aliases, which is what the bench's
    /// stage-name reader compares against.
    #[test]
    fn each_row_types_column_names_are_its_statements_aliases() {
        assert_eq!(
            <IndexedFetchRow as Row>::COLUMN_NAMES,
            ["index_rows", "bucket_count", "spans", "resources"]
        );
        assert_eq!(<WideFetchRow as Row>::COLUMN_NAMES, ["spans", "resources"]);
        assert_eq!(
            <FallbackFetchRow as Row>::COLUMN_NAMES,
            ["spans", "resources"]
        );
    }

    /// The four nested tuple arities, beside the projection they have to
    /// agree with.
    #[test]
    fn the_nested_tuple_arities_are_the_projections() {
        assert_eq!(SPAN_TUPLE_ELEMENTS, 25);
        assert_eq!(RESOURCE_TUPLE_ELEMENTS, 6);
        assert_eq!(EVENT_TUPLE_ELEMENTS, 5);
        assert_eq!(LINK_TUPLE_ELEMENTS, 7);
    }
}

// --- the search statement's row (issue #590) ----------------------------

/// One row of [`super::search::search_sql`]: one returned trace, in the
/// statement's projection order and under its aliases.
///
/// The server reports the projected types as `FixedString(16)`, `String`,
/// `String`, `SimpleAggregateFunction(min, Int64)`, `Int64`, `Int64`,
/// `UInt64` and `Array(Tuple(FixedString(8), Int64, Int64, String,
/// Array(Tuple(UInt8, String, String))))` (`toTypeName` on 26.3.29.7). The driver's row decoder reads a
/// `SimpleAggregateFunction` as its inner type, so `start_ns` is an `i64`.
/// For a trace the per-trace table has not indexed, the left join gives
/// `root_service` and `root_name` empty and `start_ns` and `duration_ns`
/// zero.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct SearchTraceRow {
    pub trace_id: [u8; 16],
    pub root_service: String,
    pub root_name: String,
    pub start_ns: i64,
    pub duration_ns: i64,
    /// The newest matching span's start: the order's key.
    pub last: i64,
    /// The matching spans of the trace in the window, before the `spss`
    /// cap.
    pub matched: u64,
    /// The first `spss` matching spans by `(start_ns, span_id)`.
    pub spans: Vec<SearchSpanTuple>,
}

/// One element of [`SearchTraceRow::spans`], in the tuple's order. Its
/// `Deserialize` goes through `deserialize_tuple` for the reason the
/// module doc gives.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchSpanTuple {
    pub span_id: [u8; 8],
    pub start_ns: i64,
    pub duration_ns: i64,
    pub service: String,
    /// The span's projected values, one per projection group whose
    /// condition held for it, in group order (issue #591).
    pub projected: Vec<SearchProjected>,
}

/// See [`SPAN_TUPLE_ELEMENTS`].
pub const SEARCH_SPAN_TUPLE_ELEMENTS: usize = 5;

/// One projected value of a matched span (issue #591): its projection
/// group (1-based), the value as text, and the kind it decodes as — a
/// stored type name, `String` or `Bool`.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchProjected {
    pub group: u8,
    pub value: String,
    pub kind: String,
}

/// See [`SPAN_TUPLE_ELEMENTS`].
pub const SEARCH_PROJECTED_TUPLE_ELEMENTS: usize = 3;

impl Serialize for SearchProjected {
    /// See [`FetchedEventTuple::serialize`].
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut t = s.serialize_tuple(SEARCH_PROJECTED_TUPLE_ELEMENTS)?;
        t.serialize_element(&self.group)?;
        t.serialize_element(self.value.as_str())?;
        t.serialize_element(self.kind.as_str())?;
        t.end()
    }
}

struct SearchProjectedVisitor;

impl<'de> Visitor<'de> for SearchProjectedVisitor {
    type Value = SearchProjected;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a {SEARCH_PROJECTED_TUPLE_ELEMENTS}-element projected value tuple"
        )
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        const T: &str = "projected value";
        Ok(SearchProjected {
            group: element(&mut seq, T, "group")?,
            value: element(&mut seq, T, "value")?,
            kind: element(&mut seq, T, "kind")?,
        })
    }
}

impl<'de> Deserialize<'de> for SearchProjected {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_tuple(SEARCH_PROJECTED_TUPLE_ELEMENTS, SearchProjectedVisitor)
    }
}

impl Serialize for SearchSpanTuple {
    /// See [`FetchedEventTuple::serialize`].
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut t = s.serialize_tuple(SEARCH_SPAN_TUPLE_ELEMENTS)?;
        t.serialize_element(&self.span_id)?;
        t.serialize_element(&self.start_ns)?;
        t.serialize_element(&self.duration_ns)?;
        t.serialize_element(self.service.as_str())?;
        t.serialize_element(&self.projected)?;
        t.end()
    }
}

struct SearchSpanVisitor;

impl<'de> Visitor<'de> for SearchSpanVisitor {
    type Value = SearchSpanTuple;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a {SEARCH_SPAN_TUPLE_ELEMENTS}-element search span tuple"
        )
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        const T: &str = "search span";
        Ok(SearchSpanTuple {
            span_id: element(&mut seq, T, "span_id")?,
            start_ns: element(&mut seq, T, "start_ns")?,
            duration_ns: element(&mut seq, T, "duration_ns")?,
            service: element(&mut seq, T, "service")?,
            projected: element(&mut seq, T, "projected")?,
        })
    }
}

impl<'de> Deserialize<'de> for SearchSpanTuple {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_tuple(SEARCH_SPAN_TUPLE_ELEMENTS, SearchSpanVisitor)
    }
}
