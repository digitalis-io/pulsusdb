//! The typed values a search response carries (issue #602 part 2c moved
//! them here from the deleted search engine): group keys, aggregate
//! values, projected attributes, span summaries, and the byte charges the
//! search statement's decode makes before it allocates each of them.

use std::collections::HashSet;

use pulsus_traceql::{ComparisonOp, Field};

use super::exec::ByteBudget;
use crate::logql::error::{ReadError, TooBroadReason};

/// The wire spelling of one projected field, and the ONLY way one can be
/// built.
///
/// Its own module so the tuple constructor is out of scope everywhere
/// else: this module's ENTIRE body is the type, its constructor and its
/// accessor. Outside it, [`WireKey::new`] is the only way to obtain one,
/// so no string in this workspace can become a wire attribute key without
/// passing through it (issue #479 AC1(a)).
mod wire_key {
    use pulsus_traceql::Field;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct WireKey(String);

    impl WireKey {
        /// The reference keys a matched span's projected attribute with the
        /// BARE field name and carries the scope as a separate field of its
        /// own attribute struct, so the wire key never carries our
        /// `span.`/`resource.` prefix (issue #479; measured against the
        /// reference: `{} | select(span.http.method)` comes back keyed
        /// `http.method`).
        pub fn new(field: &Field) -> Self {
            WireKey(match field {
                // `Intrinsic::as_str` is private to pulsus-traceql;
                // `Display` delegates to it, so this is byte-identical.
                Field::Intrinsic(i) => i.to_string(),
                // RAW key: no scope prefix, and NOT `render_attr_key`'s
                // bracket-quoted form.
                Field::Attribute { key, .. } => key.clone(),
            })
        }

        pub fn as_str(&self) -> &str {
            &self.0
        }
    }
}
pub use wire_key::WireKey;

/// One projected `attributes` entry (issue #479).
///
/// **Both fields are PRIVATE and there is exactly one constructor**, so no
/// string in this workspace can become a wire attribute key without
/// passing through [`WireKey::new`] — the boundary reaches the renderer,
/// not only the planner, because this is the type the renderer reads.
/// What the compiler does NOT establish is that the `Field` handed here is
/// the query's own field rather than one a caller transformed first: a
/// sole constructor proves a value was BUILT by that function, never that
/// it is the right value. (The check for that half,
/// `projection_identities_equal_the_parsed_query_fields`, went with the
/// previous search engine, #602 part 2c-2b-2.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedAttribute {
    key: WireKey,
    /// The TYPED value (issue #510). It used to be a `String`, which made
    /// the renderer hard-code `stringValue` for every projected attribute
    /// — an int came back `{"stringValue":"3"}` where the reference sends
    /// `{"intValue":"3"}`. Carrying a [`GroupValue`] routes this surface
    /// through the SAME `group_value_json` the group keys and the
    /// aggregates use, so the arm cannot be decided differently here.
    value: GroupValue,
}

impl ProjectedAttribute {
    pub fn new(field: &Field, value: GroupValue) -> Self {
        ProjectedAttribute {
            key: WireKey::new(field),
            value,
        }
    }

    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    pub fn value(&self) -> &GroupValue {
        &self.value
    }

    /// `key.len() + value.payload_bytes()` — the term
    /// [`SpanSummary::heap_payload_bytes`] adds and [`build_summary`]
    /// charges BEFORE the pair is built. A numeric or boolean value owns
    /// no heap payload, so its term is the key alone (issue #510).
    pub fn heap_bytes(&self) -> usize {
        self.key.as_str().len() + self.value.payload_bytes()
    }
}

/// One matched span's response summary (docs/api.md §4.2 `spanSets`
/// entry): summary fields plus the projected attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanSummary {
    pub span_id: [u8; 8],
    /// `Some` iff a name value was COLLECTED for this span (issue #479
    /// rule 1) — the query referenced `name` and the leaf that referenced
    /// it matched, or a `select(name)` stage named it.
    ///
    /// PRIVATE: every read outside this module goes through [`Self::name`],
    /// so a future reader cannot reach the field through a tuple position,
    /// an enum payload, a generic container or an inferred binding — and,
    /// decisively, `Option<String>` in a `json!` value position COMPILES
    /// and renders `null`, which is exactly the wrong answer the reference
    /// never gives. Privacy turns that silent null into a type error.
    /// `None` costs no retained bytes and writes no wire field.
    name: Option<String>,
    pub start_ns: i64,
    pub duration_ns: i64,
    /// The projected attributes, in the plan's projection-group order.
    pub attributes: Vec<ProjectedAttribute>,
}

impl SpanSummary {
    /// The only way to build one outside this module.
    pub fn new(
        span_id: [u8; 8],
        name: Option<String>,
        start_ns: i64,
        duration_ns: i64,
        attributes: Vec<ProjectedAttribute>,
    ) -> Self {
        SpanSummary {
            span_id,
            name,
            start_ns,
            duration_ns,
            attributes,
        }
    }

    /// `None` iff the query collected no name for this span. Callers that
    /// need a `String` must choose their own absent-value behaviour: the
    /// renderer writes no key at all, and the TTL-race fallback writes an
    /// empty root name, as it already does for the other two fallback
    /// fields it refuses to invent.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// The summary's heap payload beyond its own `size_of` slot in the
    /// parent trace's `spans` buffer (which the parent accounts):
    /// overhead envelope + name bytes (only when one was collected) + the
    /// attributes buffer at its **actual capacity** (code review round 2:
    /// unused preallocated capacity is retained memory too) + the
    /// attribute string bytes — what the search statement's decode
    /// charges (`spans/search.rs`'s `decode_search_charged`).
    #[cfg(test)]
    pub(crate) fn heap_payload_bytes(&self) -> usize {
        super::exec::RETAINED_ENTRY_OVERHEAD
            + self.name.as_ref().map_or(0, String::len)
            + self.attributes.capacity() * std::mem::size_of::<ProjectedAttribute>()
            + self
                .attributes
                .iter()
                .map(ProjectedAttribute::heap_bytes)
                .sum::<usize>()
    }
}

/// The canonical NaN bit pattern all NaN group keys collapse onto (issue
/// #193 R2-F2).
const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

/// The WIRE ARM one typed response value renders into, and that value as
/// text (issue #510).
///
/// **One decision, three surfaces, two crates.** The server's
/// `group_value_json` builds its JSON object from this arm, and the live
/// grouping differential — which reads [`GroupValue`]s straight out of
/// the engine, one crate below the encoder — builds its type-tagged
/// comparison token from the same call. Two spellings of "which arm" was
/// the defect issue #510 exists to remove; a differential carrying its
/// own copy would compare text the wire never sends.
///
/// The TEXT is the value's own rendering, not the JSON encoder's: a
/// finite double comes back as `5` where our encoder writes `5.0`. That
/// lexical difference is a ledger row
/// (`traceql-spanset-aggregate-double-lexical-form`), and keeping it out
/// of the token is what lets the differential compare arms and values
/// without failing on it.
///
/// `Nil` is `("stringValue", "nil")`: the reference groups a span
/// carrying no value for a `by()` key under that literal marker, not
/// under a null.
pub fn wire_arm(value: &GroupValue) -> (&'static str, String) {
    match value {
        GroupValue::Str(s) => ("stringValue", s.clone()),
        GroupValue::Int(i) => ("intValue", i.to_string()),
        GroupValue::Double(bits) => {
            let f = f64::from_bits(*bits);
            (
                "doubleValue",
                match non_finite_double_spelling(f) {
                    Some(spelling) => spelling.to_string(),
                    None => f.to_string(),
                },
            )
        }
        GroupValue::Bool(b) => ("boolValue", b.to_string()),
        GroupValue::Nil => ("stringValue", "nil".to_string()),
        GroupValue::Array(v) => (
            "arrayValue",
            serde_json::to_string(&v.iter().map(wire_arm).collect::<Vec<_>>())
                .expect("pairs of strings serialize"),
        ),
    }
}

/// The reference's spelling of a NON-FINITE double inside the
/// `doubleValue` arm (issue #510), or `None` when the value is finite.
///
/// protojson renders a `double` field's NaN and infinities as these three
/// literal JSON STRINGS, and the reference sends exactly them —
/// `{"doubleValue":"NaN"}`, `{"doubleValue":"Infinity"}`,
/// `{"doubleValue":"-Infinity"}`. Our own stored spellings are Rust's
/// (`NaN`, `inf`, `-inf`), so the normalisation happens here and `inf`
/// never reaches the wire.
///
/// **In `pulsus-read` rather than beside the renderer** so the response
/// encoder and the live differential — which reads [`GroupValue`]s
/// straight out of the engine, one crate below the encoder — spell a
/// non-finite value the same way. Two spellings would have made the
/// differential compare text the wire never carries.
pub fn non_finite_double_spelling(f: f64) -> Option<&'static str> {
    if f.is_finite() {
        None
    } else if f.is_nan() {
        Some("NaN")
    } else if f.is_sign_positive() {
        Some("Infinity")
    } else {
        Some("-Infinity")
    }
}

/// The bit pattern a double GROUP KEY is identified by (issue #510).
///
/// Group identity is the RAW bit pattern with NaN alone canonicalised.
/// `-0.0` and `+0.0` are DIFFERENT groups, which is what the reference
/// returns: measured on three spans carrying `-0.0`, `+0.0`, `-0.0`, it
/// answers two span sets (`{"doubleValue":0}` over span 02 and
/// `{"doubleValue":-0}` over spans 01 and 03) where the pre-#510 fold gave
/// one span set of three. The NaN fold stays, and it is UNOBSERVABLE end
/// to end — the wire format carries one NaN spelling, so no payload
/// difference can reach a response — which is why it is not claimed as
/// reference parity in either direction.
///
/// Only GROUPING calls this. An aggregate value is not a group key: it
/// keeps its raw bits, so a `-0.0` sum renders `-0` and not `0`.
pub(crate) fn group_double_bits(f: f64) -> u64 {
    if f.is_nan() {
        CANONICAL_NAN_BITS
    } else {
        f.to_bits()
    }
}

/// One typed response value — a `by()` group-key value, an aggregate's
/// own value, or a matched span's projected attribute (issues #193,
/// #510) — rendering to the reference's
/// `value:{stringValue|intValue|doubleValue|boolValue|arrayValue}` arms.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupValue {
    Str(String),
    Int(i64),
    /// A double as its `f64` bit pattern; render via `f64::from_bits`.
    /// A GROUP KEY's pattern comes from [`group_double_bits`] (NaN
    /// folded, sign of zero kept); an aggregate's and a projected
    /// attribute's are raw.
    Double(u64),
    Bool(bool),
    /// The span carried no value for this key (grouped into the nil bucket).
    Nil,
    /// An array (issue #601): its elements in stored order, each `Str`,
    /// `Int`, `Double` or `Bool` — never `Nil`, never `Array`.
    Array(Vec<GroupValue>),
}

impl GroupValue {
    /// The owned heap payload beyond the enum's own `size_of` slot: a
    /// `Str` its bytes (charged/released via `.len()`, the module-wide
    /// payload convention); an `Array` its buffer at its capacity, which is
    /// what is retained, plus each element's payload. Numeric/bool/nil add
    /// nothing.
    pub(crate) fn payload_bytes(&self) -> usize {
        match self {
            GroupValue::Str(s) => s.len(),
            GroupValue::Array(v) => {
                v.capacity() * std::mem::size_of::<GroupValue>()
                    + v.iter().map(GroupValue::payload_bytes).sum::<usize>()
            }
            GroupValue::Int(_) | GroupValue::Double(_) | GroupValue::Bool(_) | GroupValue::Nil => 0,
        }
    }
}

/// One response spanSet produced by a `by()` regroup (issue #193): the
/// resolved group-key `attributes` (in `by()` order), the total matched
/// span count IN THIS GROUP (pre-`spss`), and the `spss`-capped per-group
/// summaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanSetGroup {
    pub attributes: Vec<(String, GroupValue)>,
    pub matched: u32,
    pub spans: Vec<SpanSummary>,
}

impl SpanSetGroup {
    /// This group's retained heap — accounted the SAME way a returned
    /// trace accounts its own `spans`, plus the `attributes` (display + value
    /// payload via `.len()`, `.capacity()` for the enclosing `Vec` slot).
    /// The grouped decode (`spans/search.rs`'s
    /// `decode_search_grouped_charged`) charges exactly these amounts.
    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        super::exec::RETAINED_ENTRY_OVERHEAD
            + self.attributes.capacity() * std::mem::size_of::<(String, GroupValue)>()
            + self
                .attributes
                .iter()
                .map(|(display, value)| display.len() + value.payload_bytes())
                .sum::<usize>()
            + self.spans.capacity() * std::mem::size_of::<SpanSummary>()
            + self
                .spans
                .iter()
                .map(SpanSummary::heap_payload_bytes)
                .sum::<usize>()
    }
}

/// The retained heap of a `by()`-produced group vector (issue #193): the
/// enclosing `Vec` slot + overhead plus each group's [`SpanSetGroup::retained_bytes`].
/// This is the amount the grouped decode charges for a trace's groups.
#[cfg(test)]
pub(crate) fn groups_retained_bytes(groups: &[SpanSetGroup]) -> usize {
    // `len()` == the enclosing `Vec`'s capacity by construction (the
    // decode reserves `Vec::with_capacity(n)` and pushes exactly `n`), so
    // this matches the charge exactly.
    super::exec::RETAINED_ENTRY_OVERHEAD
        + std::mem::size_of_val(groups)
        + groups
            .iter()
            .map(SpanSetGroup::retained_bytes)
            .sum::<usize>()
}

/// A `by()` group-key value tuple (issue #193) — one [`GroupValue`] per
/// `by()` key, in query order.
pub(crate) type GroupTuple = Vec<GroupValue>;

/// The fixed per-tuple overhead of the [`GroupCardinalityCounter`]'s
/// `HashSet` — the set slot (the tuple `Vec` header) + the
/// container-overhead envelope. The variable string payload is charged on
/// top per tuple (issue #193 R4-F1: `.len()` for payload, `.capacity()`
/// for slots).
const GROUP_TUPLE_ENTRY_BYTES: usize =
    std::mem::size_of::<GroupTuple>() + super::exec::RETAINED_ENTRY_OVERHEAD;

/// The retained cost of one distinct group-key tuple in the cardinality
/// counter (issue #193 R4-F1): the fixed `HashSet` slot / per-element
/// overhead PLUS the owned `GroupValue::Str` payloads, via the SAME
/// `.len()`-based method [`SpanSetGroup::retained_bytes`] uses for its
/// value strings — so the two accounting sites provably cannot drift.
pub(crate) fn group_tuple_bytes(tuple: &GroupTuple) -> usize {
    GROUP_TUPLE_ENTRY_BYTES
        + tuple.capacity() * std::mem::size_of::<GroupValue>()
        + tuple.iter().map(GroupValue::payload_bytes).sum::<usize>()
}

/// The distinct-group cardinality cap (issue #193 R2-F1): a running
/// accumulator over every group the search statement's rows carry, fed
/// by the grouped decode before anything is built, so `by()|coalesce()`
/// is bounded by the SAME static `422 TraceSearchSeriesCap` as bare
/// `by()`. Each distinct tuple's ACTUAL heap ([`group_tuple_bytes`]) is
/// charged BEFORE it is retained; a repeat tuple charges nothing (dedup).
/// The counter persists for the whole request (it must, to keep enforcing
/// across all it decodes) and its charge is released on the success path by
/// [`GroupCardinalityCounter::release`].
pub(crate) struct GroupCardinalityCounter {
    seen: HashSet<GroupTuple>,
    cap: u64,
    charged: usize,
}

impl GroupCardinalityCounter {
    pub(crate) fn new(cap: u64) -> Self {
        GroupCardinalityCounter {
            seen: HashSet::new(),
            cap,
            charged: 0,
        }
    }

    /// Observes one distinct group tuple: charges its actual retained
    /// bytes BEFORE retaining it, then trips the `422` the moment the
    /// distinct count exceeds `cap`. A tuple already seen charges nothing.
    pub(crate) fn observe(
        &mut self,
        tuple: &GroupTuple,
        budget: &mut ByteBudget,
    ) -> Result<(), ReadError> {
        if self.seen.contains(tuple) {
            return Ok(());
        }
        let bytes = group_tuple_bytes(tuple);
        budget.charge(bytes)?;
        self.charged += bytes;
        self.seen.insert(tuple.clone());
        if self.seen.len() as u64 > self.cap {
            return Err(ReadError::QueryTooBroad(
                TooBroadReason::TraceSearchSeriesCap {
                    count: self.seen.len() as u64,
                    cap: self.cap,
                },
            ));
        }
        Ok(())
    }

    /// Releases the counter's whole retained charge (success-path only;
    /// on an error the request budget dies whole).
    pub(crate) fn release(&self, budget: &mut ByteBudget) {
        budget.release(self.charged);
    }
}

/// The engine's numeric comparator. It was shared with the previous metrics
/// filter compiler's `nestedSetParent` lowering (issue #458), which went
/// with the previous metrics SQL (#602 part 3a).
pub(crate) fn cmp_f64(op: ComparisonOp, lhs: f64, rhs: f64) -> bool {
    match op {
        ComparisonOp::Eq => lhs == rhs,
        ComparisonOp::Neq => lhs != rhs,
        ComparisonOp::Gt => lhs > rhs,
        ComparisonOp::Gte => lhs >= rhs,
        ComparisonOp::Lt => lhs < rhs,
        ComparisonOp::Lte => lhs <= rhs,
        ComparisonOp::Re | ComparisonOp::Nre => false,
    }
}

/// Charged before a span summary's attribute buffer is reserved at
/// `attr_capacity` slots (issue #591 part 3 shares it with the search
/// statement's decode).
pub(crate) fn summary_reserve_bytes(attr_capacity: usize) -> usize {
    super::exec::RETAINED_ENTRY_OVERHEAD + attr_capacity * std::mem::size_of::<ProjectedAttribute>()
}

/// Charged before a returned trace's summaries buffer is reserved at
/// `take` slots: one entry's overhead and the slots. The trace's own slot
/// is charged with the output buffer (`exec::output_reserve_bytes`).
pub(crate) fn match_reserve_bytes(take: usize) -> usize {
    super::exec::RETAINED_ENTRY_OVERHEAD + take * std::mem::size_of::<SpanSummary>()
}

/// Charged before a trace's `by()` group vector is reserved at `n`
/// groups (issue #592 part 2).
pub(crate) fn groups_reserve_bytes(n: usize) -> usize {
    super::exec::RETAINED_ENTRY_OVERHEAD + n * std::mem::size_of::<SpanSetGroup>()
}

/// Charged before one group's span buffer is reserved at `take` slots
/// (issue #592 part 2).
pub(crate) fn group_reserve_bytes(take: usize) -> usize {
    super::exec::RETAINED_ENTRY_OVERHEAD + take * std::mem::size_of::<SpanSummary>()
}

/// Go's `time.Duration.String()` fractional-part formatter: emits up to
/// `prec` fractional digits (trailing zeros trimmed) as `".xxx"` (empty if
/// all zero) and returns the integer part `u / 10^prec`.
fn go_duration_frac(mut u: u128, prec: u32) -> (String, u128) {
    let mut digits: Vec<u8> = Vec::new();
    let mut printing = false;
    for _ in 0..prec {
        let digit = (u % 10) as u8;
        printing = printing || digit != 0;
        if printing {
            digits.push(b'0' + digit);
        }
        u /= 10;
    }
    let frac = if digits.is_empty() {
        String::new()
    } else {
        digits.reverse();
        let mut s = String::from(".");
        // Safety: `digits` holds only ASCII '0'..='9' by construction.
        s.push_str(std::str::from_utf8(&digits).expect("ascii digits"));
        s
    };
    (frac, u)
}

/// Renders a nanosecond duration as Go's `time.Duration.String()` — the
/// form Tempo v3.0.2 uses for a TraceQL `duration`/`traceDuration` group
/// value's `stringValue` (verified live via the grouping differential):
/// sub-second uses `ns`/`µs`/`ms` with a trimmed fraction, `>= 1s` uses
/// `[h][m]s` with a trimmed fractional-seconds part (`1.5s`, `1m30s`,
/// `1h1m1s`, `500µs`, `0s`).
pub fn go_duration_string(nanos: i64) -> String {
    if nanos == 0 {
        return "0s".to_string();
    }
    let neg = nanos < 0;
    let u0: u128 = (nanos as i128).unsigned_abs();
    const SECOND: u128 = 1_000_000_000;
    let s = if u0 < SECOND {
        let (unit, prec): (&str, u32) = if u0 < 1_000 {
            ("ns", 0)
        } else if u0 < 1_000_000 {
            ("µs", 3)
        } else {
            ("ms", 6)
        };
        let (frac, int_part) = go_duration_frac(u0, prec);
        format!("{int_part}{frac}{unit}")
    } else {
        let (frac, secs) = go_duration_frac(u0, 9);
        let mut out = format!("{}{frac}s", secs % 60);
        let mins = secs / 60;
        if mins > 0 {
            out = format!("{}m{out}", mins % 60);
            let hours = mins / 60;
            if hours > 0 {
                out = format!("{hours}h{out}");
            }
        }
        out
    };
    if neg { format!("-{s}") } else { s }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T1 (issue #601 part 1): an array takes the `arrayValue` arm, its
    /// token the compact JSON of each element's own `[arm, text]` pair, and
    /// its payload is its buffer's capacity plus its elements' payloads.
    #[test]
    fn an_array_takes_the_array_arm_with_each_element_in_its_own() {
        let four = GroupValue::Array(vec![
            GroupValue::Str("x".to_string()),
            GroupValue::Int(1),
            GroupValue::Double(1.5f64.to_bits()),
            GroupValue::Bool(true),
        ]);
        assert_eq!(
            wire_arm(&four),
            (
                "arrayValue",
                r#"[["stringValue","x"],["intValue","1"],["doubleValue","1.5"],["boolValue","true"]]"#
                    .to_string()
            )
        );
        assert_eq!(
            wire_arm(&GroupValue::Array(vec![])),
            ("arrayValue", "[]".to_string())
        );
        let mut v = Vec::with_capacity(4);
        v.push(GroupValue::Str("ab".to_string()));
        assert_eq!(v.capacity(), 4);
        assert_eq!(
            GroupValue::Array(v).payload_bytes(),
            4 * std::mem::size_of::<GroupValue>() + 2
        );
    }

    /// Moved here with `go_duration_string` from the deleted
    /// `search_eval.rs` (#602 part 2c-2b-2): the Go runtime's
    /// `time.Duration.String()` format, which a `by(duration)` group key
    /// and a metrics duration label render in.
    #[test]
    fn go_duration_string_matches_the_go_runtime_format() {
        assert_eq!(go_duration_string(0), "0s");
        assert_eq!(go_duration_string(500), "500ns");
        assert_eq!(go_duration_string(1_500), "1.5µs");
        assert_eq!(go_duration_string(1_000_000), "1ms");
        assert_eq!(go_duration_string(1_500_000), "1.5ms");
        assert_eq!(go_duration_string(1_000_000_000), "1s");
        assert_eq!(go_duration_string(1_500_000_000), "1.5s");
        assert_eq!(go_duration_string(90_000_000_000), "1m30s");
        assert_eq!(go_duration_string(3_661_000_000_000), "1h1m1s");
        assert_eq!(go_duration_string(-1_500_000_000), "-1.5s");
        for (ns, want) in [
            (1, "1ns"),
            (500_000_000, "500ms"),
            (1_080_000_000, "1.08s"),
            (100_000_000_000, "1m40s"),
            (3_600_000_000_000, "1h0m0s"),
            (-900_000_000, "-900ms"),
            (250_000_000, "250ms"),
        ] {
            assert_eq!(go_duration_string(ns), want, "{ns} ns");
        }
    }

    /// Two durations never render alike across the unit boundaries, so
    /// grouping by the rendering never merges two distinct durations.
    #[test]
    fn go_duration_string_is_injective_over_the_unit_boundaries() {
        const BOUNDARIES: [i64; 9] = [
            0,
            999,
            1_000,
            1_001,
            999_999_999,
            1_000_000_000,
            60_000_000_000,
            i64::MIN,
            i64::MAX,
        ];
        let mut seen: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
        for ns in BOUNDARIES {
            let rendered = go_duration_string(ns);
            if let Some(other) = seen.insert(rendered.clone(), ns) {
                panic!("{ns} and {other} both render {rendered:?}");
            }
        }
        assert_eq!(seen.len(), BOUNDARIES.len());
    }

    /// AC1 — every projected attribute key is the BARE field spelling, for
    /// every field SHAPE the grammar admits. Moved here with `WireKey`
    /// from the deleted `search_plan.rs` (#602 part 2c-2b-2).
    ///
    /// The enumeration is generated, not transcribed: `Intrinsic::ALL` and
    /// `AttrScope::ALL` come from the same token list as the variants
    /// themselves, so a new variant joins this test automatically. It does
    /// not establish that the spelling is the reference's: a sole
    /// constructor proves a value was BUILT by `WireKey::new`, never that
    /// `WireKey::new` is right.
    #[test]
    fn wire_key_is_bare_for_every_field_shape() {
        use pulsus_traceql::Field;
        for i in pulsus_traceql::Intrinsic::ALL {
            let field = Field::Intrinsic(*i);
            assert_eq!(
                WireKey::new(&field).as_str(),
                i.to_string(),
                "{i} must render as its own canonical spelling"
            );
        }
        // Keys chosen for the shapes that make a rendering differ: a
        // dotted key, a nested one, one no identifier syntax admits, and
        // the empty key.
        for scope in pulsus_traceql::AttrScope::ALL {
            for key in ["http.method", "a.b.c", "odd key!", ""] {
                let field = Field::Attribute {
                    scope: *scope,
                    key: key.to_string(),
                };
                assert_eq!(
                    WireKey::new(&field).as_str(),
                    key,
                    "{scope}{key:?} must render as the RAW key — no scope prefix, and NOT the \
                     bracket-quoted Display form"
                );
            }
        }
        let span_http = Field::Attribute {
            scope: pulsus_traceql::AttrScope::Span,
            key: "http.method".to_string(),
        };
        assert_eq!(span_http.to_string(), "span.http.method");
        assert_eq!(WireKey::new(&span_http).as_str(), "http.method");
    }
}
