//! Allocation-free, schema-aware OTLP protobuf **wire pre-scan** (issue #115,
//! track 5 of the coordinated ingest-DoS family fix).
//!
//! Run BEFORE `Export{Trace,Metrics,Logs}ServiceRequest::decode` on the three
//! OTLP protobuf parse paths, this pre-scan walks the raw protobuf wire bytes
//! — without decoding or materializing a single field into the vendored
//! `prost` structs — and enforces per-level and shared cross-request aggregate
//! element caps on **every repeated field reachable from the three decode
//! roots**, plus the `AnyValue` nesting-depth bound. An over-cap or over-deep
//! request is rejected whole-request ([`LogsIngestError::OversizeMessage`] →
//! HTTP 400 / `google.rpc.Status.code = 3`) before `prost` allocates the
//! amplified in-memory structure that a body of many minimal-length repeated
//! sub-messages would otherwise unpack into.
//!
//! # Why a wire pre-scan, not a post-decode check
//!
//! `Export*::decode` materializes the ENTIRE message before any post-decode
//! count check could fire — the very allocation this guards against. Counting
//! occurrences directly on the wire (each length-delimited sub-message is a
//! byte slice we *re-slice*, never copy or decode) lets us reject the request
//! having touched only O(depth) auxiliary memory and, on rejection, having read
//! only up to the offending field.
//!
//! # Allocation discipline
//!
//! The walk is **iterative**: an explicit stack of [`Frame`]s, each holding a
//! borrowed `&[u8]` slice (a `Copy` fat pointer, never a copy of the bytes) and
//! a fixed-size `[u32; SLOTS]` of per-level counters. The stack depth is bounded
//! by [`MAX_WIRE_DEPTH`] — the message-type graph is a finite DAG whose only
//! unbounded-nesting cycle is `AnyValue ↔ Array/Kvlist`, itself capped at
//! [`MAX_ANYVALUE_DEPTH`] — so the walk's auxiliary memory is
//! O(nesting depth), never O(width). It never recurses natively, so it cannot
//! stack-overflow while guarding untrusted depth.
//!
//! # Malformed wire is deferred, not rejected here
//!
//! The pre-scan enforces **counts and depth only**. Any wire anomaly
//! (truncated varint, length overrun, group wire type, bad packed length) is
//! NOT reported as an oversize error — the scan returns `Ok`, leaving the
//! subsequent `Export*::decode` to produce the canonical
//! [`LogsIngestError::Decode`] (`prost::DecodeError`). This keeps error
//! classification identical to the pre-track-5 behaviour for every malformed
//! body and guarantees the pre-scan can only ever *add* an oversize rejection,
//! never change how a structurally-valid-but-otherwise-bad body is classified.
//!
//! Malformed classification takes **precedence** over an oversize reject even
//! when a body is BOTH over-cap AND malformed: a cap/depth violation is
//! recorded but the (allocation-free, depth-bounded) structural walk continues
//! to the end of the body — INCLUDING descending into the length-validated
//! payloads of the over-cap children themselves — and if malformed wire appears
//! anywhere (inside an over-cap child or after the over-cap prefix) the request
//! defers to `prost` all the same. Cap accounting is DECOUPLED from the
//! structural scan on EVERY charge path uniformly (issue #115 review rounds 3 &
//! 4): a blown per-level / aggregate counter records the first violation but
//! never suppresses the structural validation of the length-validated payload it
//! guards — the [`Action::RepeatedMessage`] and [`Action::CountedMessage`] arms
//! still DESCEND into their over-cap child, and [`Action::PackedVarint`] still
//! walks its packed blob to the end — so malformed wire nested inside (or packed
//! within) an over-cap field is surfaced and defers to `prost`. The only
//! deliberately skip-descent charge paths are the DEPTH bounds — the `AnyValue`
//! nesting reject and the [`MAX_WIRE_DEPTH`] frame-stack backstop — where a wire
//! deeper than the bound IS the reject and descending further would defeat it.
//! The recorded oversize reject is surfaced only for a body that walks cleanly
//! to the end. See [`walk`] / [`step_length_delimited`] for the
//! record-and-continue control flow; crucially the over-cap structure is never
//! decoded either way, so DoS protection holds.
//!
//! # Duplicate singular messages accumulate (prost merge semantics)
//!
//! Protobuf permits a singular (`optional`) embedded-message field to appear
//! multiple times on the wire, and `prost` MERGES the occurrences —
//! CONCATENATING their repeated sub-fields into one struct. Scanning each
//! occurrence as an independent frame would let an attacker split an over-cap
//! repeated vector across `N` duplicate singular parents, each under the
//! per-level cap, that decode to one struct with `N×` the elements. For the
//! singular fields whose merged sub-tree carries a repeated vector without a
//! monotonic aggregate backstop — `ExponentialHistogramDataPoint.positive` /
//! `.negative` (`Buckets.bucket_counts`) and `Resource` via
//! `Resource{Spans,Metrics,Logs}.resource` (`entity_refs`, `attributes`) — the
//! pre-scan accumulates the child's per-level counts ACROSS all occurrences
//! within the same parent (see [`Action::MergeableMessage`] / [`ACC_SLOTS`]),
//! so the split is charged as `N×K` and rejected exactly as the merged decode
//! would be. Every OTHER merge-prone singular field (`InstrumentationScope`,
//! the `Metric.data` oneof arms, the `AnyValue` chain) reaches only repeated
//! vectors that DO carry a monotonic cross-request aggregate
//! (`total attributes` / `total data points` / `total AnyValue elements`) or
//! the nesting-depth bound, which already caps their merged total.
//!
//! # Scalar value fields are NOT capped here (v6 ruling)
//!
//! Scalar `bytes`/`string` fields (ids, names, `AnyValue.string_value`/
//! `bytes_value`) carry no per-field cap: each occurs at most once per an
//! already-capped repeated ancestor, and their aggregate size is bounded by the
//! 64 MiB `read_capped_body` backstop that runs before decode. The pre-scan
//! bounds *counts and depth*, not scalar sizes.

use crate::error::LogsIngestError;

/// Reuses track 1's `AnyValue` nesting cap so the wire pre-scan and the
/// post-decode [`otlp_depth::ensure_anyvalue_depth`] reject at the identical
/// depth with the identical error.
///
/// [`otlp_depth::ensure_anyvalue_depth`]: crate::protocols::otlp_depth::ensure_anyvalue_depth
pub use crate::protocols::otlp_depth::MAX_ANYVALUE_DEPTH;

// ---------------------------------------------------------------------------
// Caps
//
// Per-level caps bound a single parent message's repeated field; shared
// aggregate caps bound the running total across the whole request (the real
// amplification ceiling — a body of `MAX_RESOURCE_SPANS × MAX_SCOPE_SPANS ×
// MAX_SPANS` would be astronomically large, so the aggregate is what actually
// binds, exactly as the landed remote-write/loki tracks use a generous
// per-series cap plus a 5 000 000 cross-request aggregate). Values are chosen
// consistent with those landed tracks' scale: fan-out leaves (spans, metrics,
// data points, log records) at 1 000 000 per level with a 5 000 000 aggregate;
// groupings and secondary vectors at 65 536. Legitimate telemetry sits orders
// of magnitude below every cap; each bound exists solely to reject the
// decode-time heap amplification a hostile body would otherwise force.
// ---------------------------------------------------------------------------

/// Per-`ScopeSpans` span cap; the aggregate [`MAX_TOTAL_SPANS`] is the binding
/// cross-request bound.
pub const MAX_SPANS: usize = 1_000_000;
/// Cross-request aggregate span cap.
pub const MAX_TOTAL_SPANS: usize = 5_000_000;
/// Per-`Span` event cap.
pub const MAX_EVENTS_PER_SPAN: usize = 65_536;
/// Cross-request aggregate span-event cap.
pub const MAX_TOTAL_EVENTS: usize = 5_000_000;
/// Per-`Span` link cap.
pub const MAX_LINKS_PER_SPAN: usize = 65_536;
/// Cross-request aggregate span-link cap.
pub const MAX_TOTAL_LINKS: usize = 5_000_000;
/// Per-`ExportTraceServiceRequest` resource-spans cap.
pub const MAX_RESOURCE_SPANS: usize = 65_536;
/// Per-`ResourceSpans` scope-spans cap.
pub const MAX_SCOPE_SPANS: usize = 65_536;

/// Per-`ExportMetricsServiceRequest` resource-metrics cap.
pub const MAX_RESOURCE_METRICS: usize = 65_536;
/// Per-`ResourceMetrics` scope-metrics cap.
pub const MAX_SCOPE_METRICS: usize = 65_536;
/// Per-`ScopeMetrics` metric cap.
pub const MAX_METRICS: usize = 1_000_000;
/// Per-data-point-container data-point cap (all five `Metric.data` oneof arms
/// share the per-level cap and the aggregate below).
pub const MAX_DATA_POINTS: usize = 1_000_000;
/// Cross-request aggregate data-point cap.
pub const MAX_TOTAL_DATA_POINTS: usize = 5_000_000;
/// Per-data-point exemplar cap.
pub const MAX_EXEMPLARS: usize = 65_536;
/// Cross-request aggregate exemplar cap.
pub const MAX_TOTAL_EXEMPLARS: usize = 5_000_000;
/// Per-`HistogramDataPoint` bucket / bound cap and per-`Buckets` bucket-count
/// cap (histogram fan-out vectors).
pub const MAX_BUCKETS: usize = 65_536;
/// Per-`SummaryDataPoint` quantile cap.
pub const MAX_QUANTILES: usize = 65_536;

/// Per-`ExportLogsServiceRequest` resource-logs cap.
pub const MAX_RESOURCE_LOGS: usize = 65_536;
/// Per-`ResourceLogs` scope-logs cap.
pub const MAX_SCOPE_LOGS: usize = 65_536;
/// Per-`ScopeLogs` log-record cap.
pub const MAX_LOG_RECORDS: usize = 1_000_000;
/// Cross-request aggregate log-record cap.
pub const MAX_TOTAL_LOG_RECORDS: usize = 5_000_000;

/// Per-element attribute cap (every `KeyValue` repeated field: resource /
/// scope / span / event / link / data-point / exemplar / log-record
/// attributes and `Metric.metadata`).
pub const MAX_ATTRIBUTES_PER_ELEMENT: usize = 65_536;
/// Cross-request aggregate attribute cap.
pub const MAX_TOTAL_ATTRIBUTES: usize = 5_000_000;

/// Per-`Resource` entity-reference cap.
pub const MAX_ENTITY_REFS: usize = 65_536;
/// Per-`EntityRef` id-key / description-key cap.
pub const MAX_ENTITY_REF_KEYS: usize = 65_536;

/// Cross-request aggregate cap on `ArrayValue`/`KvlistValue` element count —
/// the fan-out width of a nested `AnyValue` tree (complements the
/// [`MAX_ANYVALUE_DEPTH`] nesting-depth bound).
pub const MAX_ANYVALUE_ELEMENTS: usize = 5_000_000;

/// Hard cap on the wire-walk frame stack, an explicit backstop that bounds the
/// pre-scan's own recursion independently of the semantic depth reject. The
/// message-type schema graph is a finite DAG whose deepest structural chain is
/// well under a dozen frames and whose only unbounded-nesting cycle
/// (`AnyValue ↔ Array/Kvlist`) is capped at [`MAX_ANYVALUE_DEPTH`], so a
/// legitimate or even an `AnyValue`-depth-attacking request never approaches
/// this limit — it exists purely so no crafted wire nesting can grow the stack
/// (heap) without bound before the semantic guard fires.
const MAX_WIRE_DEPTH: usize = MAX_ANYVALUE_DEPTH + 64;

/// Number of per-level counter slots a single frame carries. The message type
/// with the most distinct repeated fields is `HistogramDataPoint`
/// (`attributes`, `bucket_counts`, `explicit_bounds`, `exemplars`).
const SLOTS: usize = 4;

/// Number of cross-occurrence *merge* accumulator slots a frame carries for its
/// singular MERGEABLE sub-message children (see [`Action::MergeableMessage`]).
///
/// `prost` MERGES a singular (`optional`) embedded-message field that appears
/// multiple times on the wire, CONCATENATING its repeated sub-fields into one
/// struct — so `N` occurrences of a singular `Buckets` with `K` `bucket_counts`
/// each decode to one `Buckets` with `N*K` counts. Scanning each occurrence as
/// an independent frame with its own per-level counter would let that split
/// bypass the per-level cap (issue #115 code review, finding 1). To match
/// prost's merge semantics the parent frame holds a persistent accumulator per
/// mergeable child site (`SLOTS` wide, so a child's own slot indices map
/// directly): the child is SEEDED from it on descent and WRITES it back on pop,
/// so sibling occurrences within one parent accumulate and are checked against
/// the per-level cap as a running total. Two sites suffice for the widest case
/// (`ExponentialHistogramDataPoint.positive` + `.negative`), each `SLOTS` wide.
const ACC_SLOTS: usize = 2 * SLOTS;

// ---------------------------------------------------------------------------
// Message-type schema
// ---------------------------------------------------------------------------

/// Every OTLP message type the pre-scan descends into. The variant set IS the
/// schema's node set; [`field_action`] is the per-type field-number table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MsgType {
    // traces
    ExportTraceServiceRequest,
    ResourceSpans,
    ScopeSpans,
    Span,
    SpanEvent,
    SpanLink,
    // metrics
    ExportMetricsServiceRequest,
    ResourceMetrics,
    ScopeMetrics,
    Metric,
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
    NumberDataPoint,
    HistogramDataPoint,
    ExponentialHistogramDataPoint,
    ExponentialHistogramBuckets,
    SummaryDataPoint,
    ValueAtQuantile,
    Exemplar,
    // logs
    ExportLogsServiceRequest,
    ResourceLogs,
    ScopeLogs,
    LogRecord,
    // shared
    Resource,
    InstrumentationScope,
    EntityRef,
    KeyValue,
    AnyValue,
    ArrayValue,
    KeyValueList,
}

/// Which cross-request aggregate counter a repeated field charges. Each kind's
/// cap and error-field name are fixed by [`AggKind::spec`].
#[derive(Clone, Copy, Debug)]
enum AggKind {
    Spans,
    Events,
    Links,
    Attributes,
    DataPoints,
    Exemplars,
    AnyValueElements,
    LogRecords,
}

impl AggKind {
    /// Index into the [`Aggregates`] running-total array.
    fn index(self) -> usize {
        self as usize
    }

    /// The aggregate cap and the error `field` label reported when it is
    /// exceeded.
    fn spec(self) -> (usize, &'static str) {
        match self {
            AggKind::Spans => (MAX_TOTAL_SPANS, "total spans"),
            AggKind::Events => (MAX_TOTAL_EVENTS, "total span events"),
            AggKind::Links => (MAX_TOTAL_LINKS, "total span links"),
            AggKind::Attributes => (MAX_TOTAL_ATTRIBUTES, "total attributes"),
            AggKind::DataPoints => (MAX_TOTAL_DATA_POINTS, "total data points"),
            AggKind::Exemplars => (MAX_TOTAL_EXEMPLARS, "total exemplars"),
            AggKind::AnyValueElements => (MAX_ANYVALUE_ELEMENTS, "total AnyValue elements"),
            AggKind::LogRecords => (MAX_TOTAL_LOG_RECORDS, "total log records"),
        }
    }
}

/// Number of distinct [`AggKind`]s (array length for [`Aggregates`]).
const AGG_KINDS: usize = 8;

/// The counting rule for one repeated field: its per-frame slot, its optional
/// per-level cap (`None` for the `AnyValue` element vectors, which are bounded
/// only in aggregate + by depth), and its optional aggregate charge.
#[derive(Clone, Copy)]
struct Count {
    slot: usize,
    per_level: Option<(usize, &'static str)>,
    agg: Option<AggKind>,
}

/// What to do with one wire field, resolved by [`field_action`] from the
/// containing message type and the field number.
#[derive(Clone, Copy)]
enum Action {
    /// Ignore this field (scalar we do not cap, or a sub-message with nothing
    /// reachable worth capping) — advance past its value without descending.
    Skip,
    /// A singular (`optional`) sub-message: descend into `child`, no count.
    SingularMessage { child: MsgType },
    /// A singular (`optional`) sub-message that `prost` MERGES across duplicate
    /// wire occurrences (concatenating its repeated sub-fields into one struct).
    /// Descend into `child`, but SEED the child's per-level counters from — and
    /// on pop WRITE them back to — the parent frame's [`Frame::acc`] region at
    /// `acc_base`, so `N` duplicate occurrences accumulate their repeated
    /// sub-field element counts (matching prost's merge) and are checked against
    /// the per-level cap as one running total (issue #115 finding 1). Distinct
    /// sites within one parent (e.g. `positive` vs `negative`) use disjoint
    /// `acc_base`s so they accumulate separately.
    MergeableMessage { child: MsgType, acc_base: usize },
    /// A repeated sub-message: charge `count`, then descend into `child`.
    RepeatedMessage { child: MsgType, count: Count },
    /// A repeated sub-message whose `child` carries no cappable repeated field
    /// or nesting (e.g. `ValueAtQuantile`, all scalar) — we charge `count` but
    /// have nothing to *count* inside it. We STILL descend into the
    /// length-validated `child` so malformed inner wire in an over-cap occurrence
    /// is detected and defers to `prost` (issue #115 review round 4): a blown cap
    /// is recorded (not short-circuited) and the descent proceeds exactly like
    /// [`Action::RepeatedMessage`], keeping the structural malformed scan
    /// complete on every charge path.
    CountedMessage { child: MsgType, count: Count },
    /// A repeated `string` field (non-packed): +1 per length-delimited
    /// occurrence.
    RepeatedString { count: Count },
    /// A packed-or-unpacked repeated varint scalar (`uint64` bucket counts):
    /// element count = number of varints in the packed blob, or 1 per
    /// unpacked occurrence.
    PackedVarint { count: Count },
    /// A packed-or-unpacked repeated 64-bit scalar (`fixed64` bucket counts /
    /// `double` bounds): element count = blob length / 8, or 1 per unpacked
    /// occurrence.
    PackedFixed64 { count: Count },
}

/// Builds the attribute [`Count`] (per-element cap + shared aggregate) shared
/// by every `KeyValue` repeated field.
const fn attr_count() -> Count {
    Count {
        slot: 0,
        per_level: Some((MAX_ATTRIBUTES_PER_ELEMENT, "attributes")),
        agg: Some(AggKind::Attributes),
    }
}

/// The per-type field-number → [`Action`] table (the plan's "table per message
/// type"). Field numbers and wire kinds are verified against
/// `vendor/opentelemetry-proto/src/proto/tonic/*.rs`.
fn field_action(ty: MsgType, field: u32) -> Action {
    use MsgType::*;
    match ty {
        // ---- traces ----
        ExportTraceServiceRequest => match field {
            1 => repeated_msg(ResourceSpans, 0, MAX_RESOURCE_SPANS, "resource_spans", None),
            _ => Action::Skip,
        },
        ResourceSpans => match field {
            1 => Action::MergeableMessage {
                child: Resource,
                acc_base: 0,
            },
            2 => repeated_msg(ScopeSpans, 0, MAX_SCOPE_SPANS, "scope_spans", None),
            _ => Action::Skip,
        },
        ScopeSpans => match field {
            1 => Action::SingularMessage {
                child: InstrumentationScope,
            },
            2 => repeated_msg(Span, 0, MAX_SPANS, "spans", Some(AggKind::Spans)),
            _ => Action::Skip,
        },
        Span => match field {
            9 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            11 => repeated_msg(
                SpanEvent,
                1,
                MAX_EVENTS_PER_SPAN,
                "events",
                Some(AggKind::Events),
            ),
            13 => repeated_msg(
                SpanLink,
                2,
                MAX_LINKS_PER_SPAN,
                "links",
                Some(AggKind::Links),
            ),
            _ => Action::Skip,
        },
        SpanEvent => match field {
            3 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            _ => Action::Skip,
        },
        SpanLink => match field {
            4 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            _ => Action::Skip,
        },
        // ---- metrics ----
        ExportMetricsServiceRequest => match field {
            1 => repeated_msg(
                ResourceMetrics,
                0,
                MAX_RESOURCE_METRICS,
                "resource_metrics",
                None,
            ),
            _ => Action::Skip,
        },
        ResourceMetrics => match field {
            1 => Action::MergeableMessage {
                child: Resource,
                acc_base: 0,
            },
            2 => repeated_msg(ScopeMetrics, 0, MAX_SCOPE_METRICS, "scope_metrics", None),
            _ => Action::Skip,
        },
        ScopeMetrics => match field {
            1 => Action::SingularMessage {
                child: InstrumentationScope,
            },
            2 => repeated_msg(Metric, 0, MAX_METRICS, "metrics", None),
            _ => Action::Skip,
        },
        Metric => match field {
            5 => Action::SingularMessage { child: Gauge },
            7 => Action::SingularMessage { child: Sum },
            9 => Action::SingularMessage { child: Histogram },
            10 => Action::SingularMessage {
                child: ExponentialHistogram,
            },
            11 => Action::SingularMessage { child: Summary },
            12 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            _ => Action::Skip,
        },
        Gauge | Sum => match field {
            1 => data_points(NumberDataPoint),
            _ => Action::Skip,
        },
        Histogram => match field {
            1 => data_points(HistogramDataPoint),
            _ => Action::Skip,
        },
        ExponentialHistogram => match field {
            1 => data_points(ExponentialHistogramDataPoint),
            _ => Action::Skip,
        },
        Summary => match field {
            1 => data_points(SummaryDataPoint),
            _ => Action::Skip,
        },
        NumberDataPoint => match field {
            7 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            5 => exemplars(),
            _ => Action::Skip,
        },
        HistogramDataPoint => match field {
            9 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            6 => Action::PackedFixed64 {
                count: bucket_count(1, "bucket_counts"),
            },
            7 => Action::PackedFixed64 {
                count: bucket_count(2, "explicit_bounds"),
            },
            8 => exemplars_slot(3),
            _ => Action::Skip,
        },
        ExponentialHistogramDataPoint => match field {
            1 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            // `positive` (8) and `negative` (9) are SINGULAR `Buckets` that
            // prost merges across duplicate occurrences, concatenating their
            // `bucket_counts`. Accumulate each site into its own parent-frame
            // `acc` region so N split occurrences sum against the per-level
            // `bucket_counts` cap (issue #115 finding 1).
            8 => Action::MergeableMessage {
                child: ExponentialHistogramBuckets,
                acc_base: 0,
            },
            9 => Action::MergeableMessage {
                child: ExponentialHistogramBuckets,
                acc_base: SLOTS,
            },
            11 => exemplars(),
            _ => Action::Skip,
        },
        ExponentialHistogramBuckets => match field {
            2 => Action::PackedVarint {
                count: bucket_count(0, "bucket_counts"),
            },
            _ => Action::Skip,
        },
        SummaryDataPoint => match field {
            7 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            6 => Action::CountedMessage {
                child: ValueAtQuantile,
                count: Count {
                    slot: 1,
                    per_level: Some((MAX_QUANTILES, "quantile_values")),
                    agg: None,
                },
            },
            _ => Action::Skip,
        },
        // `ValueAtQuantile` is all-scalar (`quantile`, `value` doubles) — nothing
        // to cap, but we descend into it (see [`Action::CountedMessage`]) purely
        // to structurally validate its wire so malformed bytes defer to `prost`.
        ValueAtQuantile => Action::Skip,
        Exemplar => match field {
            7 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            _ => Action::Skip,
        },
        // ---- logs ----
        ExportLogsServiceRequest => match field {
            1 => repeated_msg(ResourceLogs, 0, MAX_RESOURCE_LOGS, "resource_logs", None),
            _ => Action::Skip,
        },
        ResourceLogs => match field {
            1 => Action::MergeableMessage {
                child: Resource,
                acc_base: 0,
            },
            2 => repeated_msg(ScopeLogs, 0, MAX_SCOPE_LOGS, "scope_logs", None),
            _ => Action::Skip,
        },
        ScopeLogs => match field {
            1 => Action::SingularMessage {
                child: InstrumentationScope,
            },
            2 => repeated_msg(
                LogRecord,
                0,
                MAX_LOG_RECORDS,
                "log_records",
                Some(AggKind::LogRecords),
            ),
            _ => Action::Skip,
        },
        LogRecord => match field {
            6 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            5 => Action::SingularMessage { child: AnyValue },
            _ => Action::Skip,
        },
        // ---- shared ----
        Resource => match field {
            1 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            3 => repeated_msg(EntityRef, 1, MAX_ENTITY_REFS, "entity_refs", None),
            _ => Action::Skip,
        },
        InstrumentationScope => match field {
            3 => Action::RepeatedMessage {
                child: KeyValue,
                count: attr_count(),
            },
            _ => Action::Skip,
        },
        EntityRef => match field {
            3 => Action::RepeatedString {
                count: entity_ref_keys(0),
            },
            4 => Action::RepeatedString {
                count: entity_ref_keys(1),
            },
            _ => Action::Skip,
        },
        KeyValue => match field {
            2 => Action::SingularMessage { child: AnyValue },
            _ => Action::Skip,
        },
        AnyValue => match field {
            5 => Action::SingularMessage { child: ArrayValue },
            6 => Action::SingularMessage {
                child: KeyValueList,
            },
            _ => Action::Skip,
        },
        ArrayValue => match field {
            1 => Action::RepeatedMessage {
                child: AnyValue,
                count: anyvalue_elements(),
            },
            _ => Action::Skip,
        },
        KeyValueList => match field {
            1 => Action::RepeatedMessage {
                child: KeyValue,
                count: anyvalue_elements(),
            },
            _ => Action::Skip,
        },
    }
}

/// Shorthand constructor for a repeated sub-message field with a per-level cap
/// and an optional aggregate — keeps [`field_action`] terse.
fn repeated_msg(
    child: MsgType,
    slot: usize,
    per_level: usize,
    field: &'static str,
    agg: Option<AggKind>,
) -> Action {
    Action::RepeatedMessage {
        child,
        count: Count {
            slot,
            per_level: Some((per_level, field)),
            agg,
        },
    }
}

/// The `data_points` field shared by all five `Metric.data` oneof arms.
fn data_points(child: MsgType) -> Action {
    repeated_msg(
        child,
        0,
        MAX_DATA_POINTS,
        "data_points",
        Some(AggKind::DataPoints),
    )
}

/// The `exemplars` field on `NumberDataPoint`/`ExponentialHistogramDataPoint`
/// (slot 1).
fn exemplars() -> Action {
    exemplars_slot(1)
}

/// The `exemplars` field with an explicit per-level slot (`HistogramDataPoint`
/// uses slot 3, its other slots being `attributes`/`bucket_counts`/
/// `explicit_bounds`).
fn exemplars_slot(slot: usize) -> Action {
    repeated_msg(
        MsgType::Exemplar,
        slot,
        MAX_EXEMPLARS,
        "exemplars",
        Some(AggKind::Exemplars),
    )
}

/// The bucket / bound [`Count`] (per-level [`MAX_BUCKETS`], no aggregate).
fn bucket_count(slot: usize, field: &'static str) -> Count {
    Count {
        slot,
        per_level: Some((MAX_BUCKETS, field)),
        agg: None,
    }
}

/// The `EntityRef` key [`Count`] (per-level [`MAX_ENTITY_REF_KEYS`], no
/// aggregate).
fn entity_ref_keys(slot: usize) -> Count {
    Count {
        slot,
        per_level: Some((MAX_ENTITY_REF_KEYS, "entity_ref_keys")),
        agg: None,
    }
}

/// The `ArrayValue`/`KvlistValue` element [`Count`]: no per-level cap, charged
/// only against the shared [`AggKind::AnyValueElements`] aggregate (depth is
/// the complementary bound).
fn anyvalue_elements() -> Count {
    Count {
        slot: 0,
        per_level: None,
        agg: Some(AggKind::AnyValueElements),
    }
}

// ---------------------------------------------------------------------------
// Walk
// ---------------------------------------------------------------------------

/// One open message level: its type, the wire slice it spans, the read cursor,
/// its `AnyValue`-nesting `depth`, and its per-level repeated-field counters.
struct Frame<'a> {
    ty: MsgType,
    buf: &'a [u8],
    pos: usize,
    /// `AnyValue` nesting bookkeeping (interpretation depends on `ty`; see
    /// [`child_depth`]). Zero and unused for structural frames.
    depth: usize,
    counts: [u32; SLOTS],
    /// Cross-occurrence merge accumulators for this frame's singular MERGEABLE
    /// children (see [`Action::MergeableMessage`] / [`ACC_SLOTS`]). A child at
    /// site `acc_base` seeds its `counts` from `acc[acc_base..acc_base + SLOTS]`
    /// on descent and writes them back on pop, so duplicate occurrences of a
    /// merged singular child accumulate their repeated sub-field counts.
    acc: [u32; ACC_SLOTS],
    /// When this frame IS a singular mergeable child, the parent-frame
    /// [`Frame::acc`] base its `counts` are written back to on pop. `None` for
    /// every non-mergeable (structural / repeated / root) frame.
    merge_base: Option<usize>,
}

impl<'a> Frame<'a> {
    fn new(ty: MsgType, buf: &'a [u8], depth: usize) -> Self {
        Frame {
            ty,
            buf,
            pos: 0,
            depth,
            counts: [0; SLOTS],
            acc: [0; ACC_SLOTS],
            merge_base: None,
        }
    }
}

/// Running cross-request aggregate totals, threaded (`&mut`) through the whole
/// walk and never reset — monotonic, so splitting a payload across siblings
/// cannot evade an aggregate cap.
#[derive(Default)]
struct Aggregates {
    totals: [u64; AGG_KINDS],
}

/// Internal walk failure: either a genuine cap/depth rejection to surface as a
/// 400, or a benign wire anomaly on which the scan bails and defers to
/// `prost`'s own decode error.
enum ScanErr {
    /// A per-level or aggregate cap, or the depth bound, was exceeded.
    Reject(LogsIngestError),
    /// The wire bytes are malformed; stop scanning and let `prost` classify.
    Malformed,
}

/// One step of the walk applied to the top frame.
enum Outcome<'a> {
    /// Consumed one scalar/skip/counted field; stay on the same frame.
    Advanced,
    /// The frame is exhausted; pop it.
    Done,
    /// Descend into this child sub-message.
    Descend(Frame<'a>),
}

/// The `AnyValue`-nesting depth a `child` reached from `parent` (at
/// `parent_depth`) is assigned. The value is the level of the `AnyValue` node a
/// `KeyValue`'s value will become, or of an `AnyValue`/container itself; it is
/// meaningful only along the `KeyValue → AnyValue → Array/Kvlist` chain and is
/// `0` for every structural edge.
fn child_depth(parent: MsgType, parent_depth: usize, child: MsgType) -> usize {
    use MsgType::*;
    match (parent, child) {
        // An array element `AnyValue` is one level below its container.
        (ArrayValue, AnyValue) => parent_depth + 1,
        // A `KeyValue`'s value `AnyValue` inherits the level stamped on the
        // `KeyValue` frame (1 for a top-level attribute; container+1 inside a
        // kvlist).
        (KeyValue, AnyValue) => parent_depth,
        // A log record body is a top-level (level-1) `AnyValue`.
        (LogRecord, AnyValue) => 1,
        // Array/Kvlist containers sit at the same level as their `AnyValue`.
        (AnyValue, ArrayValue) | (AnyValue, KeyValueList) => parent_depth,
        // A kvlist entry carries the level its value `AnyValue` will hold.
        (KeyValueList, KeyValue) => parent_depth + 1,
        // A structural attribute `KeyValue`'s value is a top-level `AnyValue`.
        (_, KeyValue) => 1,
        _ => 0,
    }
}

/// Records the FIRST cap/depth violation seen during the walk, ignoring every
/// later one (the first is the reject surfaced for a body that scans cleanly to
/// the end). Decouples cap accounting from control flow so a blown counter can
/// be noted without short-circuiting the structural malformed scan.
fn record(recorded: &mut Option<LogsIngestError>, err: LogsIngestError) {
    if recorded.is_none() {
        *recorded = Some(err);
    }
}

/// Charges `count` against the top frame's per-level slot and the shared
/// aggregate by `n` occurrences, rejecting on either overflow.
fn charge(
    count: &Count,
    n: usize,
    counts: &mut [u32; SLOTS],
    agg: &mut Aggregates,
) -> Result<(), ScanErr> {
    if let Some((limit, field)) = count.per_level {
        let slot = &mut counts[count.slot];
        let updated = (*slot as usize).saturating_add(n);
        if updated > limit {
            return Err(ScanErr::Reject(LogsIngestError::OversizeMessage {
                field,
                limit,
                actual: updated,
            }));
        }
        *slot = updated as u32;
    }
    if let Some(kind) = count.agg {
        let (limit, field) = kind.spec();
        let total = &mut agg.totals[kind.index()];
        *total = total.saturating_add(n as u64);
        if *total > limit as u64 {
            return Err(ScanErr::Reject(LogsIngestError::OversizeMessage {
                field,
                limit,
                actual: (*total).min(usize::MAX as u64) as usize,
            }));
        }
    }
    Ok(())
}

/// Reads a base-128 varint at `buf[pos..]`, returning its value and the
/// position after it, or `None` on truncation / >10-byte overflow.
fn read_varint(buf: &[u8], pos: usize) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    let mut i = pos;
    loop {
        let byte = *buf.get(i)?;
        i += 1;
        if shift >= 64 {
            return None;
        }
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i));
        }
        shift += 7;
    }
}

/// Counts the varints packed into `blob`, walking it to the end even after the
/// count passes `limit` (issue #115 review round 4). The blob's outer length is
/// already bounds-validated, so this is a single linear allocation-free pass
/// bounded by the blob size — and it is what upholds the malformed-wire
/// precedence contract on this charge path: an over-cap packed vector whose
/// trailing bytes are a truncated varint must DEFER to `prost`, not surface as
/// `OversizeMessage`. So we note the over-limit crossing but keep validating the
/// remaining varints: `None` (truncated varint anywhere) takes precedence and
/// defers; only a blob that is well-formed to the end reports [`CountOutcome::OverLimit`]
/// for the caller's [`charge`] to record.
fn count_packed_varints(blob: &[u8], limit: usize) -> Option<CountOutcome> {
    let mut pos = 0;
    let mut n = 0usize;
    let mut over_limit = false;
    while pos < blob.len() {
        let (_, next) = read_varint(blob, pos)?;
        pos = next;
        n += 1;
        if n > limit {
            over_limit = true;
        }
    }
    if over_limit {
        Some(CountOutcome::OverLimit(n))
    } else {
        Some(CountOutcome::Exact(n))
    }
}

/// The result of counting a packed vector's elements.
enum CountOutcome {
    /// The exact element count (`<= limit`).
    Exact(usize),
    /// Counting stopped early: at least this many elements, already over cap.
    OverLimit(usize),
}

/// Advances the top frame by one wire field, returning the next [`Outcome`].
///
/// `recorded` carries the walk's first cap/depth violation so a `RepeatedMessage`
/// whose per-level / aggregate counter is blown can record the reject yet STILL
/// descend into the length-validated child (issue #115 review round 3), keeping
/// the structural malformed scan complete inside over-cap children.
fn step<'a>(
    frame: &mut Frame<'a>,
    agg: &mut Aggregates,
    recorded: &mut Option<LogsIngestError>,
) -> Result<Outcome<'a>, ScanErr> {
    if frame.pos >= frame.buf.len() {
        return Ok(Outcome::Done);
    }
    let buf = frame.buf;
    let (tag, after_tag) = read_varint(buf, frame.pos).ok_or(ScanErr::Malformed)?;
    let field = (tag >> 3) as u32;
    let wire = (tag & 7) as u8;
    let action = field_action(frame.ty, field);

    match wire {
        // varint
        0 => {
            let (_, after) = read_varint(buf, after_tag).ok_or(ScanErr::Malformed)?;
            frame.pos = after;
            // A packed field may arrive unpacked: one varint = one element.
            if let Action::PackedVarint { count } = action {
                charge(&count, 1, &mut frame.counts, agg)?;
            }
            Ok(Outcome::Advanced)
        }
        // 64-bit
        1 => {
            let after = after_tag.checked_add(8).ok_or(ScanErr::Malformed)?;
            if after > buf.len() {
                return Err(ScanErr::Malformed);
            }
            frame.pos = after;
            if let Action::PackedFixed64 { count } = action {
                charge(&count, 1, &mut frame.counts, agg)?;
            }
            Ok(Outcome::Advanced)
        }
        // length-delimited
        2 => {
            let (len, after_len) = read_varint(buf, after_tag).ok_or(ScanErr::Malformed)?;
            let len = usize::try_from(len).map_err(|_| ScanErr::Malformed)?;
            let end = after_len.checked_add(len).ok_or(ScanErr::Malformed)?;
            if end > buf.len() {
                return Err(ScanErr::Malformed);
            }
            let value = &buf[after_len..end];
            frame.pos = end;
            step_length_delimited(frame, agg, recorded, action, value)
        }
        // 32-bit
        5 => {
            let after = after_tag.checked_add(4).ok_or(ScanErr::Malformed)?;
            if after > buf.len() {
                return Err(ScanErr::Malformed);
            }
            frame.pos = after;
            Ok(Outcome::Advanced)
        }
        // groups (3/4) and any other wire type: OTLP never uses them; defer to
        // prost, which will reject the body.
        _ => Err(ScanErr::Malformed),
    }
}

/// Handles a length-delimited field's `value` per its resolved [`Action`]:
/// counts packed scalars / repeated strings, or descends into sub-messages.
fn step_length_delimited<'a>(
    frame: &mut Frame<'a>,
    agg: &mut Aggregates,
    recorded: &mut Option<LogsIngestError>,
    action: Action,
    value: &'a [u8],
) -> Result<Outcome<'a>, ScanErr> {
    match action {
        Action::Skip => Ok(Outcome::Advanced),
        Action::RepeatedString { count } => {
            charge(&count, 1, &mut frame.counts, agg)?;
            Ok(Outcome::Advanced)
        }
        Action::PackedFixed64 { count } => {
            // Packed 64-bit vector: element count = byte length / 8. A length
            // not a multiple of 8 is malformed — defer to prost.
            if !value.len().is_multiple_of(8) {
                return Err(ScanErr::Malformed);
            }
            charge(&count, value.len() / 8, &mut frame.counts, agg)?;
            Ok(Outcome::Advanced)
        }
        Action::PackedVarint { count } => {
            // `count_packed_varints` walks the WHOLE blob (issue #115 review round
            // 4): a truncated trailing varint returns `None` here and DEFERS to
            // `prost`, taking precedence over the cap. `OverLimit` therefore means
            // the blob is well-formed to its end but over cap — `charge` rejects,
            // recorded (via the walk) and surfaced only if the rest of the body is
            // clean, exactly like every other charge path.
            match count_packed_varints(value, count.per_level.map_or(usize::MAX, |(l, _)| l)) {
                Some(CountOutcome::Exact(n)) => {
                    charge(&count, n, &mut frame.counts, agg)?;
                    Ok(Outcome::Advanced)
                }
                Some(CountOutcome::OverLimit(n)) => {
                    charge(&count, n, &mut frame.counts, agg)?;
                    // `OverLimit` implies a per-level cap was present, so `charge`
                    // rejected above; a capless field never reports `OverLimit`.
                    Ok(Outcome::Advanced)
                }
                None => Err(ScanErr::Malformed),
            }
        }
        Action::CountedMessage { child, count } => {
            // Decouple the cap charge from the structural scan (issue #115 review
            // round 4), mirroring `RepeatedMessage`: record the first violation
            // but ALWAYS descend into the length-validated `child` so malformed
            // inner wire in an over-cap counted message is detected and defers to
            // `prost`. The descent decodes nothing and stays within
            // `MAX_WIRE_DEPTH`, so the over-cap structure is walked, never
            // materialized.
            if let Err(ScanErr::Reject(err)) = charge(&count, 1, &mut frame.counts, agg) {
                record(recorded, err);
            }
            descend(frame, child, value)
        }
        Action::SingularMessage { child } => descend(frame, child, value),
        Action::MergeableMessage { child, acc_base } => {
            descend_merge(frame, child, acc_base, value)
        }
        Action::RepeatedMessage { child, count } => {
            // Charge the per-level / aggregate cap, but do NOT let a cap
            // violation short-circuit the descent (issue #115 review round 3):
            // record the first violation and ALWAYS descend into the
            // length-validated child so malformed wire inside an over-cap (or
            // over-aggregate) child is still detected and defers to `prost`.
            // The descent decodes nothing, is allocation-free, and stays within
            // `MAX_WIRE_DEPTH`, so completing it adds no asymptotic cost — the
            // over-cap structure is walked, never materialized.
            if let Err(ScanErr::Reject(err)) = charge(&count, 1, &mut frame.counts, agg) {
                record(recorded, err);
            }
            descend(frame, child, value)
        }
    }
}

/// Builds the child frame for a sub-message `value`, applying the
/// `AnyValue`-depth reject when the child is an `AnyValue` node.
fn descend<'a>(frame: &Frame<'a>, child: MsgType, value: &'a [u8]) -> Result<Outcome<'a>, ScanErr> {
    let depth = child_depth(frame.ty, frame.depth, child);
    if child == MsgType::AnyValue && depth > MAX_ANYVALUE_DEPTH {
        return Err(ScanErr::Reject(LogsIngestError::OversizeMessage {
            field: "AnyValue nesting depth",
            limit: MAX_ANYVALUE_DEPTH,
            actual: depth,
        }));
    }
    Ok(Outcome::Descend(Frame::new(child, value, depth)))
}

/// Builds the child frame for a singular MERGEABLE sub-message `value` (issue
/// #115 finding 1): its per-level counters are SEEDED from the parent's
/// [`Frame::acc`] region at `acc_base` and, on pop, written back there (see
/// [`walk`]), so duplicate occurrences of a `prost`-merged singular child
/// accumulate their repeated sub-field element counts against the per-level cap
/// as one running total. None of these children is an `AnyValue`, so no
/// depth reject applies; the generic `child_depth` (0 here) is retained for
/// uniformity.
fn descend_merge<'a>(
    frame: &Frame<'a>,
    child: MsgType,
    acc_base: usize,
    value: &'a [u8],
) -> Result<Outcome<'a>, ScanErr> {
    let depth = child_depth(frame.ty, frame.depth, child);
    let mut child_frame = Frame::new(child, value, depth);
    child_frame
        .counts
        .copy_from_slice(&frame.acc[acc_base..acc_base + SLOTS]);
    child_frame.merge_base = Some(acc_base);
    Ok(Outcome::Descend(child_frame))
}

/// Iteratively walks the wire bytes of `body` as a `root`-typed message,
/// enforcing every per-level / aggregate count cap and the `AnyValue`-depth
/// bound. Bails to `Ok` on any wire anomaly (deferring to `prost`), and rejects
/// with [`LogsIngestError::OversizeMessage`] on a genuine cap/depth violation.
///
/// # Malformed-wire precedence (issue #115 review rounds 2 & 3)
///
/// A cap/depth violation is NOT returned the instant it is detected: the walk
/// RECORDS the first violation (via [`record`]) and CONTINUES the (already
/// allocation-free, depth-bounded) structural scan to the end of the body. This
/// is what upholds the module's malformed-wire deferral contract for a body
/// that is BOTH over-cap AND malformed — if the continued scan meets malformed
/// wire anywhere, the whole request DEFERS to `prost`'s canonical decode error
/// (malformed classification takes precedence), rather than surfacing an
/// oversize reject `prost` would never have produced. Only when the walk
/// completes cleanly to the end is the recorded cap/depth reject surfaced.
///
/// Crucially, cap accounting is DECOUPLED from the structural scan (round 3): a
/// blown [`Action::RepeatedMessage`] counter records its reject but STILL
/// descends into the length-validated child (see [`step_length_delimited`]), so
/// malformed wire nested INSIDE an over-cap (or over-aggregate) child — and
/// inside every subsequent same-field child whose counter is already frozen — is
/// scanned and defers to `prost` all the same. Each wire byte is still visited
/// O(1) times (a child's bytes are disjoint from its parent's remaining bytes),
/// so the whole walk stays a single linear O(body) pass with no materialization
/// and decode still skipped — the DoS protection is preserved because the
/// over-cap structure is walked, never decoded.
fn walk(root: MsgType, body: &[u8]) -> Result<(), LogsIngestError> {
    let mut agg = Aggregates::default();
    let mut stack: Vec<Frame<'_>> = Vec::with_capacity(MAX_WIRE_DEPTH.min(64));
    stack.push(Frame::new(root, body, 0));
    // The FIRST cap/depth violation seen, deferred until the walk confirms the
    // rest of the body carries no malformed wire (see the fn doc). Recorded once.
    let mut recorded: Option<LogsIngestError> = None;
    while let Some(frame) = stack.last_mut() {
        match step(frame, &mut agg, &mut recorded) {
            Ok(Outcome::Advanced) => {}
            Ok(Outcome::Done) => {
                // Pop the finished frame; if it was a singular MERGEABLE child,
                // write its accumulated per-level counters back into the parent's
                // `acc` region so a later duplicate occurrence of the same
                // singular field resumes the running total (issue #115 finding
                // 1), matching prost's merge-concatenation across occurrences.
                let done = stack.pop().expect("stack non-empty in loop body");
                if let Some(base) = done.merge_base
                    && let Some(parent) = stack.last_mut()
                {
                    parent.acc[base..base + SLOTS].copy_from_slice(&done.counts);
                }
            }
            Ok(Outcome::Descend(child)) => {
                if stack.len() >= MAX_WIRE_DEPTH {
                    // Frame-stack backstop: record the reject ONCE and SKIP the
                    // descent (the child's bytes are already consumed from the
                    // parent cursor) so the walk stays depth-bounded while it
                    // continues scanning the remainder for malformed wire. Only
                    // reachable if the shallower `AnyValue` depth reject is
                    // somehow bypassed — it never is for `AnyValue` nodes.
                    record(
                        &mut recorded,
                        LogsIngestError::OversizeMessage {
                            field: "AnyValue nesting depth",
                            limit: MAX_ANYVALUE_DEPTH,
                            actual: MAX_WIRE_DEPTH,
                        },
                    );
                } else {
                    stack.push(child);
                }
            }
            // Wire anomaly: defer to prost's decode error (identical
            // classification to the pre-track-5 behaviour). Malformed wire takes
            // precedence over any recorded cap/depth reject.
            Err(ScanErr::Malformed) => return Ok(()),
            // A genuine cap/depth violation: record the FIRST one and CONTINUE
            // (the offending field is already consumed from the frame cursor),
            // surfacing it only if the rest of the body proves free of malformed
            // wire — preserving prost error precedence without materializing the
            // over-cap structure.
            Err(ScanErr::Reject(err)) => record(&mut recorded, err),
        }
    }
    match recorded {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Wire pre-scan for an OTLP `/v1/traces` protobuf body. Rejects an over-cap /
/// over-deep request before `ExportTraceServiceRequest::decode` materializes
/// it. `Ok` for any in-bounds or malformed body (the latter deferred to decode).
pub fn prescan_traces(body: &[u8]) -> Result<(), LogsIngestError> {
    walk(MsgType::ExportTraceServiceRequest, body)
}

/// Wire pre-scan for an OTLP `/v1/metrics` protobuf body — the metrics analog
/// of [`prescan_traces`].
pub fn prescan_metrics(body: &[u8]) -> Result<(), LogsIngestError> {
    walk(MsgType::ExportMetricsServiceRequest, body)
}

/// Wire pre-scan for an OTLP `/v1/logs` protobuf body — the logs analog of
/// [`prescan_traces`].
pub fn prescan_logs(body: &[u8]) -> Result<(), LogsIngestError> {
    walk(MsgType::ExportLogsServiceRequest, body)
}

#[cfg(test)]
mod tests;
