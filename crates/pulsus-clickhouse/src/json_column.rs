//! The `JSON` column's own RowBinary form, written and read by this crate
//! rather than by the driver (issues #585 and #587).
//!
//! **One home for both directions.** The encoder is the write path's
//! (issue #585); the decoder is the trace fetch's (issue #587). They share
//! the tag constants, the eleven captured frames below and the path escape
//! with its inverse, so the module lives in the crate both `pulsus-write`
//! and `pulsus-read` already depend on. The format's own record is
//! `vendor/clickhouse/PATCHES.md` section 3.
//!
//! **Why the binary form and not text.** Text JSON refuses a non-finite
//! double on this engine — four attempts, each
//! `SELECT toJSONString(CAST('<text>' AS JSON))` on ClickHouse 26.3.29.7:
//! `{"k":NaN}`, `{"k":Inf}`, `{"k":Infinity}` and `{"k":1e400}` each answer
//! `Code: 117. DB::Exception: Cannot parse JSON object here`. A span
//! attribute is a client-chosen `double`, so a path that cannot carry
//! `±Inf` or `NaN` cannot carry what a sender sends.
//!
//! **The form, captured.** A `JSON` column in RowBinary is a path count,
//! then per path a length-prefixed path string and the value as a
//! binary-encoded `Dynamic`: a type tag, then the value's own bytes. There
//! is **no length prefix on the column as a whole**. Captured off
//! ClickHouse 26.3.29.7 with `SELECT CAST('<text>' AS JSON) FORMAT
//! RowBinary`, piped through `xxd -p`:
//!
//! ```text
//! {"a":1}                              0101610a0100000000000000
//! {"a%2Eb":1,"c":"s","d":{"e":true}}   0303642e652d0101631501730561253245620a0100000000000000
//! {"k":1.5}                            01016b0e000000000000f83f
//! {"k":true}                           01016b2d01
//! {"k":["a","b"]}                      01016b1e231502000161000162
//! {"k":[1,2]}                          01016b1e230a02000100000000000000000200000000000000
//! {"k":[1.5,2.5]}                      01016b1e230e0200000000000000f83f000000000000000440
//! {"k":[true]}                         01016b1e232d010001
//! {"k":["a",1]}                        01016b1e2b20021501610a0100000000000000
//! {"k":[]}                             01016b1e231500
//! {"k":{}}                             00
//! ```
//!
//! Reading the second: `03` three paths; `03 "d.e"` `2d` `01`;
//! `01 "c"` `15` `01 "s"`; `05 "a%2Eb"` `0a` `0100000000000000`. **A nested
//! object is dotted leaf paths and nothing else** — there is no value at
//! the parent path. The last line is why an empty object cannot be stored:
//! the path disappears.
//!
//! **Path order is the encoder's, not the server's.** The captures above
//! are in the server's own output order; this encoder sorts, so one logical
//! attribute map has one encoding.

use std::fmt;

use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use serde::ser::{SerializeSeq, SerializeTuple};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The `Dynamic` type tag for a `String` value.
const TAG_STRING: u8 = 0x15;
/// The `Dynamic` type tag for a `Bool` value.
const TAG_BOOL: u8 = 0x2d;
/// The `Dynamic` type tag for an `Int64` value.
const TAG_INT64: u8 = 0x0a;
/// The `Dynamic` type tag for a `Float64` value.
const TAG_FLOAT64: u8 = 0x0e;
/// The `Dynamic` type tag prefix for an `Array(...)` value.
const TAG_ARRAY: u8 = 0x1e;
/// The element-type prefix for an `Array(Nullable(T))`.
const TAG_NULLABLE: u8 = 0x23;
/// The element-type prefix for an `Array(Dynamic(max_types=N))`, followed by
/// `N`.
const TAG_DYNAMIC: u8 = 0x2b;
/// `max_types` on the `Dynamic` a mixed array's elements are stored as.
const DYNAMIC_MAX_TYPES: u8 = 32;
/// The not-null marker one element of an `Array(Nullable(T))` carries.
const NOT_NULL: u8 = 0x00;

/// One scalar OTLP attribute value, in the four arms this engine stores
/// natively.
#[derive(Debug, Clone, PartialEq)]
pub enum TraceJsonScalar {
    Str(String),
    Bool(bool),
    Int(i64),
    Double(f64),
}

impl TraceJsonScalar {
    /// This arm's `Dynamic` type tag.
    fn tag(&self) -> u8 {
        match self {
            TraceJsonScalar::Str(_) => TAG_STRING,
            TraceJsonScalar::Bool(_) => TAG_BOOL,
            TraceJsonScalar::Int(_) => TAG_INT64,
            TraceJsonScalar::Double(_) => TAG_FLOAT64,
        }
    }

    /// The text the API renders this value as, which is what a `tag_values`
    /// row carries.
    pub fn render(&self) -> String {
        match self {
            TraceJsonScalar::Str(s) => s.clone(),
            TraceJsonScalar::Bool(b) => b.to_string(),
            TraceJsonScalar::Int(i) => i.to_string(),
            TraceJsonScalar::Double(d) => render_double(*d),
        }
    }

    /// The `tag_values.val_type` spelling for this arm — one of the four
    /// `docs/TraceQL/measure/schema.sql`'s own comment admits.
    pub fn val_type(&self) -> &'static str {
        match self {
            TraceJsonScalar::Str(_) => "string",
            TraceJsonScalar::Bool(_) => "bool",
            TraceJsonScalar::Int(_) => "int",
            TraceJsonScalar::Double(_) => "float",
        }
    }

    /// The bytes this value occupies after its tag.
    fn payload_len(&self) -> u64 {
        match self {
            TraceJsonScalar::Str(s) => leb128_len(s.len() as u64) + s.len() as u64,
            TraceJsonScalar::Bool(_) => 1,
            TraceJsonScalar::Int(_) | TraceJsonScalar::Double(_) => 8,
        }
    }

    /// The heap this value holds.
    fn allocated_bytes(&self) -> u64 {
        match self {
            TraceJsonScalar::Str(s) => s.capacity() as u64,
            _ => 0,
        }
    }
}

/// How a double is rendered as text. Finite values take Rust's shortest
/// round-tripping form; the three non-finite ones take the spellings the
/// trace API already uses.
fn render_double(d: f64) -> String {
    if d.is_nan() {
        return "NaN".to_string();
    }
    if d.is_infinite() {
        return if d.is_sign_positive() { "+Inf" } else { "-Inf" }.to_string();
    }
    d.to_string()
}

/// The value stored at one JSON path.
///
/// **A scalar array keeps its element type**, which is what lets a read ask
/// `attrs.`k`.:`Array(Nullable(Int64))``; a mixed one falls to
/// `Array(Dynamic)`; and an empty one is stored as
/// `Array(Nullable(String))`, which is a decision this encoder makes rather
/// than something the wire says — an empty array carries no element type.
#[derive(Debug, Clone, PartialEq)]
pub enum TraceJsonValue {
    Scalar(TraceJsonScalar),
    StrArray(Vec<String>),
    IntArray(Vec<i64>),
    DoubleArray(Vec<f64>),
    BoolArray(Vec<bool>),
    MixedArray(Vec<TraceJsonScalar>),
    EmptyArray,
}

impl TraceJsonValue {
    /// The bytes this value's tag and payload occupy together.
    fn encoded_len(&self) -> u64 {
        match self {
            TraceJsonValue::Scalar(s) => 1 + s.payload_len(),
            TraceJsonValue::StrArray(v) => {
                3 + leb128_len(v.len() as u64)
                    + v.iter()
                        .map(|s| 1 + leb128_len(s.len() as u64) + s.len() as u64)
                        .sum::<u64>()
            }
            TraceJsonValue::IntArray(v) => 3 + leb128_len(v.len() as u64) + 9 * v.len() as u64,
            TraceJsonValue::DoubleArray(v) => 3 + leb128_len(v.len() as u64) + 9 * v.len() as u64,
            TraceJsonValue::BoolArray(v) => 3 + leb128_len(v.len() as u64) + 2 * v.len() as u64,
            // `1e 2b 20`, the count, then each element's own tag and
            // payload — no not-null marker, because a `Dynamic` element
            // carries its type rather than inheriting one.
            TraceJsonValue::MixedArray(v) => {
                3 + leb128_len(v.len() as u64) + v.iter().map(|s| 1 + s.payload_len()).sum::<u64>()
            }
            // `1e 23 15` then one zero count byte.
            TraceJsonValue::EmptyArray => 4,
        }
    }

    /// The heap this value holds, vectors by **capacity** and strings by
    /// capacity: the queue holds the row until its block is encoded, and
    /// capacity is what the allocator is holding.
    fn allocated_bytes(&self) -> u64 {
        match self {
            TraceJsonValue::Scalar(s) => s.allocated_bytes(),
            TraceJsonValue::StrArray(v) => {
                v.capacity() as u64 * std::mem::size_of::<String>() as u64
                    + v.iter().map(|s| s.capacity() as u64).sum::<u64>()
            }
            TraceJsonValue::IntArray(v) => v.capacity() as u64 * 8,
            TraceJsonValue::DoubleArray(v) => v.capacity() as u64 * 8,
            TraceJsonValue::BoolArray(v) => v.capacity() as u64,
            TraceJsonValue::MixedArray(v) => {
                v.capacity() as u64 * std::mem::size_of::<TraceJsonScalar>() as u64
                    + v.iter().map(TraceJsonScalar::allocated_bytes).sum::<u64>()
            }
            TraceJsonValue::EmptyArray => 0,
        }
    }
}

impl Serialize for TraceJsonScalar {
    /// The tag byte then the payload in its native Rust type: RowBinary's
    /// `serialize_tuple` writes no framing, so these are the value's own
    /// bytes after the tag.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut t = s.serialize_tuple(2)?;
        t.serialize_element(&self.tag())?;
        match self {
            TraceJsonScalar::Str(v) => t.serialize_element(v.as_str())?,
            TraceJsonScalar::Bool(v) => t.serialize_element(&u8::from(*v))?,
            TraceJsonScalar::Int(v) => t.serialize_element(v)?,
            TraceJsonScalar::Double(v) => t.serialize_element(v)?,
        }
        t.end()
    }
}

/// One element of an `Array(Nullable(T))`: the not-null marker, then the
/// value with **no** tag of its own — the element type is declared once, in
/// the array's own three-byte prefix.
struct NullableElement<'a, T: ?Sized>(&'a T);

impl<T: Serialize + ?Sized> Serialize for NullableElement<'_, T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut t = s.serialize_tuple(2)?;
        t.serialize_element(&NOT_NULL)?;
        t.serialize_element(self.0)?;
        t.end()
    }
}

impl Serialize for TraceJsonValue {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            TraceJsonValue::Scalar(v) => v.serialize(s),
            TraceJsonValue::StrArray(v) => {
                let mut t = s.serialize_tuple(4)?;
                t.serialize_element(&TAG_ARRAY)?;
                t.serialize_element(&TAG_NULLABLE)?;
                t.serialize_element(&TAG_STRING)?;
                t.serialize_element(&NullableSeq(v))?;
                t.end()
            }
            TraceJsonValue::IntArray(v) => {
                let mut t = s.serialize_tuple(4)?;
                t.serialize_element(&TAG_ARRAY)?;
                t.serialize_element(&TAG_NULLABLE)?;
                t.serialize_element(&TAG_INT64)?;
                t.serialize_element(&NullableSeq(v))?;
                t.end()
            }
            TraceJsonValue::DoubleArray(v) => {
                let mut t = s.serialize_tuple(4)?;
                t.serialize_element(&TAG_ARRAY)?;
                t.serialize_element(&TAG_NULLABLE)?;
                t.serialize_element(&TAG_FLOAT64)?;
                t.serialize_element(&NullableSeq(v))?;
                t.end()
            }
            TraceJsonValue::BoolArray(v) => {
                let bytes: Vec<u8> = v.iter().map(|b| u8::from(*b)).collect();
                let mut t = s.serialize_tuple(4)?;
                t.serialize_element(&TAG_ARRAY)?;
                t.serialize_element(&TAG_NULLABLE)?;
                t.serialize_element(&TAG_BOOL)?;
                t.serialize_element(&NullableSeq(&bytes))?;
                t.end()
            }
            // Each element carries its own tag, so no not-null marker: the
            // captured frame for `{"k":["a",1]}` is
            // `1e 2b 20 02 15 01 61 0a 01…` — count, then tag/value pairs.
            TraceJsonValue::MixedArray(v) => {
                let mut t = s.serialize_tuple(4)?;
                t.serialize_element(&TAG_ARRAY)?;
                t.serialize_element(&TAG_DYNAMIC)?;
                t.serialize_element(&DYNAMIC_MAX_TYPES)?;
                t.serialize_element(&RawSeq(v))?;
                t.end()
            }
            // `Array(Nullable(String))` with no element: the element type
            // has to be declared even where no element carries it.
            TraceJsonValue::EmptyArray => {
                let empty: [&str; 0] = [];
                let mut t = s.serialize_tuple(4)?;
                t.serialize_element(&TAG_ARRAY)?;
                t.serialize_element(&TAG_NULLABLE)?;
                t.serialize_element(&TAG_STRING)?;
                t.serialize_element(&NullableSeq(&empty))?;
                t.end()
            }
        }
    }
}

/// A sequence whose LEB128 count is written by `serialize_seq`, each element
/// prefixed with the not-null marker.
struct NullableSeq<'a, T>(&'a [T]);

impl<T: Serialize> Serialize for NullableSeq<'_, T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for element in self.0 {
            seq.serialize_element(&NullableElement(element))?;
        }
        seq.end()
    }
}

/// The elements of a mixed array: the LEB128 count `serialize_seq` writes,
/// then each element's own tag and payload with **no** not-null marker — a
/// `Dynamic` element carries its type rather than inheriting one.
struct RawSeq<'a, T>(&'a [T]);

impl<T: Serialize> Serialize for RawSeq<'_, T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for element in self.0 {
            seq.serialize_element(element)?;
        }
        seq.end()
    }
}

/// One stored path and its value.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceJsonEntry {
    /// The **escaped** path, which is what the column stores — see
    /// [`escape_json_path`].
    pub path: String,
    pub value: TraceJsonValue,
}

impl Serialize for TraceJsonEntry {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut t = s.serialize_tuple(2)?;
        t.serialize_element(self.path.as_str())?;
        t.serialize_element(&self.value)?;
        t.end()
    }
}

/// One `JSON` column's value: the stored paths, sorted, each with its own
/// value.
///
/// **Sorted, because the encoding has to be a function of the content.** Two
/// pushes carrying the same attribute map in different wire order produce
/// the same bytes, so a resend of a block the server already accepted is
/// recognised by its deduplication token rather than defeated by a reordered
/// map.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TraceJson(Vec<TraceJsonEntry>);

impl TraceJson {
    /// An empty value: no path at all, which is what the eleventh captured
    /// frame shows an empty object stores.
    pub fn empty() -> Self {
        TraceJson(Vec::new())
    }

    /// Takes `entries`, sorts them by path and drops every repeat of a path
    /// after the first.
    ///
    /// **The deduplication is the writer's and has to be.**
    /// `type_json_skip_duplicated_paths` is `0` on this server, so a
    /// repeated path in one value is an exception rather than a silent drop;
    /// `QuerySettings::trace_landing_insert` pins it there so this stays the
    /// only mechanism.
    pub fn from_entries(mut entries: Vec<TraceJsonEntry>) -> Self {
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        entries.dedup_by(|a, b| a.path == b.path);
        TraceJson(entries)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn entries(&self) -> &[TraceJsonEntry] {
        &self.0
    }

    /// The bytes this value takes on the wire: the path count, then each
    /// path's length prefix, its text, its tag and its payload.
    ///
    /// Used by the per-push byte estimate, which has to price what the
    /// insert sends and not only what the row holds.
    pub fn encoded_len(&self) -> u64 {
        leb128_len(self.0.len() as u64)
            + self
                .0
                .iter()
                .map(|e| {
                    leb128_len(e.path.len() as u64) + e.path.len() as u64 + e.value.encoded_len()
                })
                .sum::<u64>()
    }

    /// The heap this value holds, by capacity.
    pub fn allocated_bytes(&self) -> u64 {
        self.0.capacity() as u64 * std::mem::size_of::<TraceJsonEntry>() as u64
            + self
                .0
                .iter()
                .map(|e| e.path.capacity() as u64 + e.value.allocated_bytes())
                .sum::<u64>()
    }
}

impl Serialize for TraceJson {
    /// `serialize_seq` writes the LEB128 path count and then the entries,
    /// which is the whole of the column's form: there is no length prefix on
    /// the column itself.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for entry in &self.0 {
            seq.serialize_element(entry)?;
        }
        seq.end()
    }
}

/// The decoder (issue #587), and the whole of the column's read side.
///
/// **Every level reads exactly two elements**, so no level under-reads a
/// declared tuple arity:
///
/// ```text
/// TraceJson        deserialize_seq       -> LEB128 path count, then N entries
/// TraceJsonEntry   deserialize_tuple(2)  -> (String path, TraceJsonValue)
/// TraceJsonValue   deserialize_tuple(2)  -> (u8 tag, body-by-seed)
///    tag 0x15 String   body = String
///    tag 0x2d Bool     body = u8, 0 or 1
///    tag 0x0a Int64    body = i64
///    tag 0x0e Float64  body = f64
///    tag 0x1e Array    body = deserialize_tuple(2) -> (u8 kind, elements-by-seed)
///         kind 0x23 Nullable  -> (u8 elem tag, Vec<(u8 NOT_NULL, T)>)
///         kind 0x2b Dynamic   -> (u8 max_types, Vec<TraceJsonValue>)
///    any other tag     -> Err
/// ```
///
/// The body is read through [`serde::de::DeserializeSeed`], which is what
/// lets the tag decide the next read inside one `visit_seq`. **Nothing
/// inside the column is validated by the driver**
/// (`vendor/clickhouse/PATCHES.md` section 3 states that as the patch's
/// limit), so this decoder owns every byte.
///
/// **The domain is the WRITER's range, not the column type's.** The tags
/// above are exactly the set [`TraceJsonValue`] and [`TraceJsonScalar`]
/// can produce. A `JSON` column can hold more and nothing in this
/// workspace writes it, so **a tag outside the set is an error, not a
/// default**: the message names the tag byte and the path.
///
/// **The order is the column's**, not sorted: reconstructing an OTLP key
/// order is the response assembler's, and this returns what the column
/// holds.
impl<'de> Deserialize<'de> for TraceJson {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_seq(ColumnVisitor)
    }
}

struct ColumnVisitor;

impl<'de> Visitor<'de> for ColumnVisitor {
    type Value = TraceJson;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON column: a path count then one (path, tagged value) per path")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut entries = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(entry) = seq.next_element::<TraceJsonEntry>()? {
            entries.push(entry);
        }
        Ok(TraceJson(entries))
    }
}

impl<'de> Deserialize<'de> for TraceJsonEntry {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_tuple(2, EntryVisitor)
    }
}

struct EntryVisitor;

impl<'de> Visitor<'de> for EntryVisitor {
    type Value = TraceJsonEntry;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a stored path and its tagged value")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let path: String = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::custom("a JSON column entry with no path"))?;
        // The path is read FIRST so a bad value can name it: a `500` that
        // says only "unknown tag" tells an operator nothing about which
        // attribute to look at.
        let value = seq
            .next_element::<TraceJsonValue>()
            .map_err(|e| serde::de::Error::custom(format!("json column path {path:?}: {e}")))?
            .ok_or_else(|| {
                serde::de::Error::custom(format!("json column path {path:?} carries no value"))
            })?;
        Ok(TraceJsonEntry { path, value })
    }
}

impl<'de> Deserialize<'de> for TraceJsonValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_tuple(2, ValueVisitor)
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = TraceJsonValue;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a Dynamic type tag and its value")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let tag: u8 = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::custom("a Dynamic value with no type tag"))?;
        seq.next_element_seed(ValueBody { tag })?
            .ok_or_else(|| serde::de::Error::custom("a Dynamic tag with no value after it"))
    }
}

/// The seed the tag chooses: it decides which read comes next, inside the
/// same `visit_seq`.
struct ValueBody {
    tag: u8,
}

impl<'de> DeserializeSeed<'de> for ValueBody {
    type Value = TraceJsonValue;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        match self.tag {
            TAG_STRING => Ok(TraceJsonValue::Scalar(TraceJsonScalar::Str(
                String::deserialize(d)?,
            ))),
            TAG_BOOL => Ok(TraceJsonValue::Scalar(TraceJsonScalar::Bool(read_bool(
                u8::deserialize(d)?,
            )?))),
            TAG_INT64 => Ok(TraceJsonValue::Scalar(TraceJsonScalar::Int(
                i64::deserialize(d)?,
            ))),
            TAG_FLOAT64 => Ok(TraceJsonValue::Scalar(TraceJsonScalar::Double(
                f64::deserialize(d)?,
            ))),
            TAG_ARRAY => d.deserialize_tuple(2, ArrayVisitor),
            other => Err(unknown_tag(other)),
        }
    }
}

/// A scalar body of the tag's own type, for an `Array(Nullable(T))`'s
/// declared element type.
struct NullableElements {
    elem_tag: u8,
}

impl<'de> DeserializeSeed<'de> for NullableElements {
    type Value = TraceJsonValue;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        match self.elem_tag {
            TAG_STRING => {
                let v = Vec::<NotNullElement<String>>::deserialize(d)?;
                // **An EMPTY `Array(Nullable(String))` is the empty-array
                // arm**, not an empty string array. The encoder stores
                // both as the same four bytes, because an empty array
                // carries no element type on the wire, and the arm the
                // captured frame was taken from is the empty one.
                if v.is_empty() {
                    return Ok(TraceJsonValue::EmptyArray);
                }
                Ok(TraceJsonValue::StrArray(
                    v.into_iter().map(|e| e.0).collect(),
                ))
            }
            TAG_INT64 => Ok(TraceJsonValue::IntArray(
                Vec::<NotNullElement<i64>>::deserialize(d)?
                    .into_iter()
                    .map(|e| e.0)
                    .collect(),
            )),
            TAG_FLOAT64 => Ok(TraceJsonValue::DoubleArray(
                Vec::<NotNullElement<f64>>::deserialize(d)?
                    .into_iter()
                    .map(|e| e.0)
                    .collect(),
            )),
            TAG_BOOL => {
                let raw = Vec::<NotNullElement<u8>>::deserialize(d)?;
                let mut out = Vec::with_capacity(raw.len());
                for e in raw {
                    out.push(read_bool(e.0)?);
                }
                Ok(TraceJsonValue::BoolArray(out))
            }
            other => Err(unknown_tag(other)),
        }
    }
}

/// The elements of an `Array(Dynamic)`: each carries its own tag, so each
/// is a whole [`TraceJsonValue`] read.
struct DynamicElements;

impl<'de> DeserializeSeed<'de> for DynamicElements {
    type Value = TraceJsonValue;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        let values = Vec::<TraceJsonValue>::deserialize(d)?;
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            match value {
                TraceJsonValue::Scalar(s) => out.push(s),
                other => {
                    return Err(serde::de::Error::custom(format!(
                        "a mixed array's element is not a scalar: {other:?}"
                    )));
                }
            }
        }
        Ok(TraceJsonValue::MixedArray(out))
    }
}

struct ArrayVisitor;

impl<'de> Visitor<'de> for ArrayVisitor {
    type Value = TraceJsonValue;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an array's element-type prefix and its elements")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let kind: u8 = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::custom("an array with no element-type prefix"))?;
        match kind {
            TAG_NULLABLE => seq
                .next_element_seed(NullableArray)?
                .ok_or_else(|| serde::de::Error::custom("a nullable array with no elements")),
            TAG_DYNAMIC => seq
                .next_element_seed(DynamicArray)?
                .ok_or_else(|| serde::de::Error::custom("a dynamic array with no elements")),
            other => Err(unknown_tag(other)),
        }
    }
}

/// `Array(Nullable(T))`: the element type's own tag, then the elements.
struct NullableArray;

impl<'de> DeserializeSeed<'de> for NullableArray {
    type Value = TraceJsonValue;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_tuple(2, NullableArrayVisitor)
    }
}

struct NullableArrayVisitor;

impl<'de> Visitor<'de> for NullableArrayVisitor {
    type Value = TraceJsonValue;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a nullable array's element tag and its elements")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let elem_tag: u8 = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::custom("a nullable array with no element tag"))?;
        seq.next_element_seed(NullableElements { elem_tag })?
            .ok_or_else(|| serde::de::Error::custom("a nullable array with no element sequence"))
    }
}

/// `Array(Dynamic(max_types=N))`: the declared `max_types`, then the
/// elements.
struct DynamicArray;

impl<'de> DeserializeSeed<'de> for DynamicArray {
    type Value = TraceJsonValue;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_tuple(2, DynamicArrayVisitor)
    }
}

struct DynamicArrayVisitor;

impl<'de> Visitor<'de> for DynamicArrayVisitor {
    type Value = TraceJsonValue;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a dynamic array's max_types and its elements")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        // `max_types` is read and discarded: it is the column's own
        // declaration, not a value, and the encoder writes one constant.
        let _max_types: u8 = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::custom("a dynamic array with no max_types"))?;
        seq.next_element_seed(DynamicElements)?
            .ok_or_else(|| serde::de::Error::custom("a dynamic array with no element sequence"))
    }
}

/// One element of an `Array(Nullable(T))` on the way back: the not-null
/// marker, then the value with no tag of its own.
///
/// The mirror of the encoder's own element type, and a `NULL` element is
/// refused rather than mapped to anything — this encoder writes none, so a
/// null here means the column was written by something else.
struct NotNullElement<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for NotNullElement<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_tuple(2, NotNullElementVisitor(std::marker::PhantomData))
    }
}

struct NotNullElementVisitor<T>(std::marker::PhantomData<T>);

impl<'de, T: Deserialize<'de>> Visitor<'de> for NotNullElementVisitor<T> {
    type Value = NotNullElement<T>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a not-null marker and a value")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let marker: u8 = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::custom("an array element with no null marker"))?;
        if marker != NOT_NULL {
            return Err(serde::de::Error::custom(format!(
                "a NULL array element (marker {marker:#04x}); this encoder writes none"
            )));
        }
        let value: T = seq
            .next_element()?
            .ok_or_else(|| serde::de::Error::custom("an array element with no value"))?;
        Ok(NotNullElement(value))
    }
}

/// A stored boolean is one byte and only `0` or `1` is a boolean.
fn read_bool<E: serde::de::Error>(raw: u8) -> Result<bool, E> {
    match raw {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(serde::de::Error::custom(format!(
            "a Bool value that is neither 0 nor 1: {other:#04x}"
        ))),
    }
}

/// A type tag outside the writer's range. **An error, not a default** —
/// the caller wraps it with the path.
fn unknown_tag<E: serde::de::Error>(tag: u8) -> E {
    serde::de::Error::custom(format!("unexpected JSON value type tag {tag:#04x}"))
}

impl fmt::Display for TraceJson {
    /// The paths, for a diagnostic. Not a JSON document and not what is
    /// stored.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        f.write_str("{")?;
        for entry in &self.0 {
            if !first {
                f.write_str(", ")?;
            }
            first = false;
            write!(f, "{}", entry.path)?;
        }
        f.write_str("}")
    }
}

/// What a `JSON` column decodes to on the read side.
///
/// The same type and the same wire form as the encoder's, under the name
/// the fetch's row types read it by: one column, the paths it holds, each
/// with its value. **One owning type for both directions** — a second type
/// over one wire form is two places for the format to drift.
pub type JsonColumn = TraceJson;

/// An OTLP attribute key as the stored JSON path: `%` becomes `%25`, then
/// `.` becomes `%2E`.
///
/// **The order matters.** Escaping `%` first means a key literally spelled
/// `a%2Eb` stores `a%252Eb` and does not collide with the key `a.b`, which
/// stores `a%2Eb`. Read back on ClickHouse 26.3.29.7: for a key spelled
/// `a%2Eb`, ``dynamicType(attrs.`a%252Eb`) != 'None'`` is `1` and
/// ``dynamicType(attrs.`a%2Eb`) != 'None'`` is `0`, and
/// `replaceAll(replaceAll('a%252Eb','%2E','.'),'%25','%')` is `a%2Eb`.
///
/// A dot in a stored path is the server's own nesting separator, which is
/// why a key carrying one has to be escaped: a nested object stores dotted
/// leaf paths and nothing else.
pub fn escape_json_path(key: &str) -> String {
    if !key.contains('%') && !key.contains('.') {
        return key.to_string();
    }
    let mut out = String::with_capacity(key.len() + 8);
    for c in key.chars() {
        match c {
            '%' => out.push_str("%25"),
            '.' => out.push_str("%2E"),
            other => out.push(other),
        }
    }
    out
}

/// The inverse of [`escape_json_path`]: `%2E` becomes `.`, then `%25`
/// becomes `%`.
///
/// **The order matters, and it is the mirror of the escape's.** Undoing the
/// dot first means a stored path of `a%252Eb` becomes `a%2Eb` — the key the
/// sender actually spelled — where undoing the percent first would make it
/// `a.b`, which is a different key.
///
/// **Split the stored path on `.` BEFORE calling this, never after.** A
/// stored `http%2Eroute` is one segment spelling the dotted key
/// `http.route`; unescaping it first produces `http.route` and a splitter
/// then reports a two-level kvlist the sender never sent.
pub fn unescape_json_path(segment: &str) -> String {
    if !segment.contains('%') {
        return segment.to_string();
    }
    segment.replace("%2E", ".").replace("%25", "%")
}

/// The bytes LEB128 takes for `n`.
fn leb128_len(mut n: u64) -> u64 {
    let mut bytes = 1;
    while n >= 0x80 {
        n >>= 7;
        bytes += 1;
    }
    bytes
}

/// The bloom-filter index expression over one span attribute key (issue
/// #595 part 2): every string the key holds on the span, as a scalar or
/// as an array element, a missing one as `''`. The schema renders it into
/// the index and the search compiler into the `indexHint` that reaches
/// it, so the two are one text. `key` is a validated
/// `traceql_indexed_attributes` key: no backtick can occur in it.
pub fn span_attr_index_expr(key: &str) -> String {
    let p = escape_json_path(key);
    format!(
        "arrayMap(x -> ifNull(x, ''), arrayConcat(attrs.`{p}`.:`Array(Nullable(String))`, [attrs.`{p}`.:String]))"
    )
}

/// [`span_attr_index_expr`] for a span-event attribute key: every string
/// any of the span's events holds under it.
pub fn event_attr_index_expr(key: &str) -> String {
    let p = escape_json_path(key);
    format!(
        "arrayMap(x -> ifNull(x, ''), arrayConcat(events.attrs.`{p}`.:String, arrayFlatten(events.attrs.`{p}`.:`Array(Nullable(String))`)))"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A row of exactly one `JSON` column, so the row's bytes **are** the
    /// column's bytes: `RowBinary` writes no framing around a row.
    #[derive(clickhouse::Row, serde::Serialize, serde::Deserialize)]
    struct OneJsonColumn {
        attrs: TraceJson,
    }

    /// The bytes one value serialises to, through the same RowBinary
    /// serializer the insert uses, with no column metadata — the validator
    /// is what the driver's own case covers, and this case is about the
    /// bytes.
    fn encode(value: &TraceJson) -> Vec<u8> {
        clickhouse::_priv::serialize_row_unvalidated(&OneJsonColumn {
            attrs: value.clone(),
        })
        .expect("serialize a JSON column")
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn entry(path: &str, value: TraceJsonValue) -> TraceJsonEntry {
        TraceJsonEntry {
            path: path.to_string(),
            value,
        }
    }

    fn one(path: &str, value: TraceJsonValue) -> TraceJson {
        TraceJson::from_entries(vec![entry(path, value)])
    }

    /// **Every value kind encodes the bytes the server emits**, compared
    /// against the frames captured off ClickHouse 26.3.29.7 and pasted in
    /// this module's own doc comment.
    ///
    /// Eleven sub-cases, one per row of the stored-type table. The captures'
    /// path order is the server's output order and is **not** the expected
    /// value; each capture's components are, assembled in this encoder's own
    /// sorted path order.
    #[test]
    fn every_value_kind_encodes_the_bytes_the_server_emits() {
        let cases: Vec<(&str, TraceJson, &str)> = vec![
            (
                r#"{"a":1}"#,
                one("a", TraceJsonValue::Scalar(TraceJsonScalar::Int(1))),
                "0101610a0100000000000000",
            ),
            (
                r#"{"k":"s"}"#,
                one(
                    "k",
                    TraceJsonValue::Scalar(TraceJsonScalar::Str("s".to_string())),
                ),
                "01016b150173",
            ),
            (
                r#"{"k":true}"#,
                one("k", TraceJsonValue::Scalar(TraceJsonScalar::Bool(true))),
                "01016b2d01",
            ),
            (
                r#"{"k":1.5}"#,
                one("k", TraceJsonValue::Scalar(TraceJsonScalar::Double(1.5))),
                "01016b0e000000000000f83f",
            ),
            (
                r#"{"k":["a","b"]}"#,
                one(
                    "k",
                    TraceJsonValue::StrArray(vec!["a".to_string(), "b".to_string()]),
                ),
                "01016b1e231502000161000162",
            ),
            (
                r#"{"k":[1,2]}"#,
                one("k", TraceJsonValue::IntArray(vec![1, 2])),
                "01016b1e230a02000100000000000000000200000000000000",
            ),
            (
                r#"{"k":[1.5,2.5]}"#,
                one("k", TraceJsonValue::DoubleArray(vec![1.5, 2.5])),
                "01016b1e230e0200000000000000f83f000000000000000440",
            ),
            (
                r#"{"k":[true]}"#,
                one("k", TraceJsonValue::BoolArray(vec![true])),
                "01016b1e232d010001",
            ),
            (
                r#"{"k":["a",1]}"#,
                one(
                    "k",
                    TraceJsonValue::MixedArray(vec![
                        TraceJsonScalar::Str("a".to_string()),
                        TraceJsonScalar::Int(1),
                    ]),
                ),
                "01016b1e2b20021501610a0100000000000000",
            ),
            (
                r#"{"k":[]}"#,
                one("k", TraceJsonValue::EmptyArray),
                "01016b1e231500",
            ),
            (r#"{"k":{}}"#, TraceJson::empty(), "00"),
        ];

        assert_eq!(cases.len(), 11, "one sub-case per captured frame");
        let mut wrong: Vec<String> = Vec::new();
        for (label, value, want) in &cases {
            let got = hex(&encode(value));
            if &got != want {
                wrong.push(format!("{label}: wanted {want}, got {got}"));
            }
            assert_eq!(
                value.encoded_len(),
                (want.len() / 2) as u64,
                "{label}: encoded_len must price the bytes the insert sends"
            );
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    /// The nested-object frame, whole: three paths, in this encoder's sorted
    /// order, each with the components the capture carries. **A nested
    /// object is dotted leaf paths and nothing else** — there is no entry at
    /// the parent path.
    #[test]
    fn a_nested_object_stores_dotted_leaf_paths_and_a_percent_key_escapes() {
        let value = TraceJson::from_entries(vec![
            entry(
                &escape_json_path("a.b"),
                TraceJsonValue::Scalar(TraceJsonScalar::Int(1)),
            ),
            entry(
                "c",
                TraceJsonValue::Scalar(TraceJsonScalar::Str("s".to_string())),
            ),
            entry("d.e", TraceJsonValue::Scalar(TraceJsonScalar::Bool(true))),
        ]);
        assert_eq!(
            value
                .entries()
                .iter()
                .map(|e| e.path.as_str())
                .collect::<Vec<_>>(),
            vec!["a%2Eb", "c", "d.e"],
            "the flat key `a.b` escapes; the nested leaf path `d.e` does not"
        );
        // The capture's own components, in this encoder's order: `03` three
        // paths, then `05 "a%2Eb" 0a <i64>`, `01 "c" 15 01 "s"`,
        // `03 "d.e" 2d 01`.
        assert_eq!(
            hex(&encode(&value)),
            "030561253245620a0100000000000000016315017303642e652d01"
        );
    }

    /// **A non-finite double encodes its bit pattern** and no text appears
    /// in the output. Text JSON refuses all three on this engine, which is
    /// why the binary form is used at all.
    #[test]
    fn a_non_finite_double_encodes_its_bit_pattern() {
        for (label, value, want) in [
            ("+Inf", f64::INFINITY, "01016b0e000000000000f07f"),
            ("-Inf", f64::NEG_INFINITY, "01016b0e000000000000f0ff"),
            ("NaN", f64::NAN, "01016b0e000000000000f87f"),
        ] {
            let got = hex(&encode(&one(
                "k",
                TraceJsonValue::Scalar(TraceJsonScalar::Double(value)),
            )));
            assert_eq!(got, want, "{label}");
            assert!(
                !got.contains("4e614e") && !got.contains("496e66"),
                "{label}: no `NaN` or `Inf` text may reach the wire: {got}"
            );
        }
    }

    /// The bytes of a lowercase hex frame — the inverse of [`hex`], for a
    /// case whose input is one of the captured frames.
    fn unhex(s: &str) -> Vec<u8> {
        assert!(s.len().is_multiple_of(2), "odd-length hex frame");
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex frame"))
            .collect()
    }

    /// One `JSON` column's value, decoded from the column's own bytes
    /// through the same RowBinary deserializer the fetch uses, with no
    /// column metadata — the exact mirror of [`encode`]
    /// (`vendor/clickhouse/PATCHES.md` section 3's test-support exports).
    ///
    /// Returns the driver's error rather than panicking, because
    /// [`a_dynamic_tag_outside_the_writers_range_is_an_error_naming_it`]
    /// is about the error.
    fn decode(bytes: &[u8]) -> Result<TraceJson, clickhouse::error::Error> {
        let mut cursor = bytes;
        clickhouse::_priv::deserialize_row_unvalidated::<OneJsonColumn>(&mut cursor)
            .map(|row| row.attrs)
    }

    /// One representative value per [`TraceJsonValue`] arm.
    ///
    /// The `match` below is **exhaustive over the enum and that is the
    /// point**: a new arm does not compile until it is handled here, and
    /// the count assertion then fails until it is given a representative.
    fn every_value_arm() -> Vec<TraceJsonValue> {
        let arms = vec![
            TraceJsonValue::Scalar(TraceJsonScalar::Int(1)),
            TraceJsonValue::StrArray(vec!["a".to_string(), "b".to_string()]),
            TraceJsonValue::IntArray(vec![1, -2]),
            TraceJsonValue::DoubleArray(vec![1.5, 2.5]),
            TraceJsonValue::BoolArray(vec![true, false]),
            TraceJsonValue::MixedArray(vec![
                TraceJsonScalar::Str("a".to_string()),
                TraceJsonScalar::Int(1),
            ]),
            TraceJsonValue::EmptyArray,
        ];
        for arm in &arms {
            match arm {
                TraceJsonValue::Scalar(_)
                | TraceJsonValue::StrArray(_)
                | TraceJsonValue::IntArray(_)
                | TraceJsonValue::DoubleArray(_)
                | TraceJsonValue::BoolArray(_)
                | TraceJsonValue::MixedArray(_)
                | TraceJsonValue::EmptyArray => {}
            }
        }
        assert_eq!(arms.len(), 7, "one representative per TraceJsonValue arm");
        arms
    }

    /// One representative per [`TraceJsonScalar`] arm, on the same rule as
    /// [`every_value_arm`].
    fn every_scalar_arm() -> Vec<TraceJsonScalar> {
        let arms = vec![
            TraceJsonScalar::Str("s".to_string()),
            TraceJsonScalar::Bool(true),
            TraceJsonScalar::Int(-7),
            TraceJsonScalar::Double(1.5),
        ];
        for arm in &arms {
            match arm {
                TraceJsonScalar::Str(_)
                | TraceJsonScalar::Bool(_)
                | TraceJsonScalar::Int(_)
                | TraceJsonScalar::Double(_) => {}
            }
        }
        assert_eq!(arms.len(), 4, "one representative per TraceJsonScalar arm");
        arms
    }

    /// `F-1`: **the eleven captured frames decode to the values they were
    /// captured from.** The same eleven rows
    /// `every_value_kind_encodes_the_bytes_the_server_emits` compares in the
    /// other direction, read off this module's own header, with the same
    /// `cases.len() == 11` guard before anything is compared — so a row
    /// deleted from the table cannot shrink the case into a green run.
    ///
    /// **The empty-array arm is where the two directions are not a
    /// bijection, and the decode rule is stated rather than implied.** The
    /// encoder stores both `EmptyArray` and an empty `StrArray` as
    /// `1e 23 15 00` — `Array(Nullable(String))` with no element — because
    /// an empty array carries no element type on the wire. The decoder
    /// therefore reads a zero-length nullable array back as `EmptyArray`,
    /// which is the arm the eleventh frame was captured from.
    #[test]
    fn every_captured_frame_decodes_to_the_value_it_was_captured_from() {
        let cases: Vec<(&str, &str, TraceJson)> = vec![
            (
                r#"{"a":1}"#,
                "0101610a0100000000000000",
                one("a", TraceJsonValue::Scalar(TraceJsonScalar::Int(1))),
            ),
            (
                r#"{"k":"s"}"#,
                "01016b150173",
                one(
                    "k",
                    TraceJsonValue::Scalar(TraceJsonScalar::Str("s".to_string())),
                ),
            ),
            (
                r#"{"k":true}"#,
                "01016b2d01",
                one("k", TraceJsonValue::Scalar(TraceJsonScalar::Bool(true))),
            ),
            (
                r#"{"k":1.5}"#,
                "01016b0e000000000000f83f",
                one("k", TraceJsonValue::Scalar(TraceJsonScalar::Double(1.5))),
            ),
            (
                r#"{"k":["a","b"]}"#,
                "01016b1e231502000161000162",
                one(
                    "k",
                    TraceJsonValue::StrArray(vec!["a".to_string(), "b".to_string()]),
                ),
            ),
            (
                r#"{"k":[1,2]}"#,
                "01016b1e230a02000100000000000000000200000000000000",
                one("k", TraceJsonValue::IntArray(vec![1, 2])),
            ),
            (
                r#"{"k":[1.5,2.5]}"#,
                "01016b1e230e0200000000000000f83f000000000000000440",
                one("k", TraceJsonValue::DoubleArray(vec![1.5, 2.5])),
            ),
            (
                r#"{"k":[true]}"#,
                "01016b1e232d010001",
                one("k", TraceJsonValue::BoolArray(vec![true])),
            ),
            (
                r#"{"k":["a",1]}"#,
                "01016b1e2b20021501610a0100000000000000",
                one(
                    "k",
                    TraceJsonValue::MixedArray(vec![
                        TraceJsonScalar::Str("a".to_string()),
                        TraceJsonScalar::Int(1),
                    ]),
                ),
            ),
            (
                r#"{"k":[]}"#,
                "01016b1e231500",
                one("k", TraceJsonValue::EmptyArray),
            ),
            (r#"{"k":{}}"#, "00", TraceJson::empty()),
        ];

        assert_eq!(cases.len(), 11, "one sub-case per captured frame");
        let mut wrong: Vec<String> = Vec::new();
        for (label, frame, want) in &cases {
            match decode(&unhex(frame)) {
                Ok(got) if &got == want => {}
                Ok(got) => wrong.push(format!("{label}: wanted {want:?}, got {got:?}")),
                Err(e) => wrong.push(format!("{label}: decode failed: {e}")),
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    /// `F-1`, second half: **every arm of both value enums survives a round
    /// trip through the column's own bytes.** The arms are enumerated by
    /// [`every_value_arm`] and [`every_scalar_arm`], whose `match`es are
    /// exhaustive, so a new arm does not compile until it is listed.
    #[test]
    fn every_value_and_scalar_arm_round_trips_through_the_column() {
        for arm in every_value_arm() {
            let value = one("k", arm.clone());
            let got =
                decode(&encode(&value)).unwrap_or_else(|e| panic!("decode {arm:?} back: {e}"));
            assert_eq!(got, value, "{arm:?} must survive encode then decode");
        }
        for scalar in every_scalar_arm() {
            let value = one("k", TraceJsonValue::Scalar(scalar.clone()));
            let got =
                decode(&encode(&value)).unwrap_or_else(|e| panic!("decode {scalar:?} back: {e}"));
            assert_eq!(got, value, "{scalar:?} must survive encode then decode");
        }
    }

    /// The three non-finite doubles round-trip as the **same** non-finite
    /// value, which is the whole reason the binary form exists: text JSON
    /// refuses all three on this engine.
    ///
    /// `NaN != NaN`, so that arm is compared on the bit pattern.
    #[test]
    fn a_non_finite_double_round_trips_as_itself() {
        for (label, value) in [
            ("+Inf", f64::INFINITY),
            ("-Inf", f64::NEG_INFINITY),
            ("NaN", f64::NAN),
        ] {
            let stored = one("k", TraceJsonValue::Scalar(TraceJsonScalar::Double(value)));
            let got = decode(&encode(&stored)).unwrap_or_else(|e| panic!("{label}: {e}"));
            let TraceJsonValue::Scalar(TraceJsonScalar::Double(back)) = got.entries()[0].value
            else {
                panic!(
                    "{label}: wanted a Double scalar, got {:?}",
                    got.entries()[0]
                );
            };
            assert_eq!(
                back.to_bits(),
                value.to_bits(),
                "{label}: the bit pattern must survive"
            );
        }
    }

    /// `F-2`: **`unescape_json_path` is `escape_json_path`'s inverse**, over
    /// the seven keys that exercise each rule, plus the asymmetry case that
    /// is the whole reason the two replacements are ordered.
    #[test]
    fn the_path_unescape_inverts_the_escape_and_the_order_is_what_makes_it_so() {
        for key in ["http.route", "a.b", "a%2Eb", "a%25b", "%", ".", ""] {
            assert_eq!(
                unescape_json_path(&escape_json_path(key)),
                key,
                "unescape(escape({key:?})) must be {key:?}"
            );
        }
        // The asymmetry, spelled out: the key literally spelled `a%2Eb`
        // stores `a%252Eb`, and undoing the dot BEFORE the percent is what
        // gives the key back. Undoing the percent first gives `a.b` — a
        // different key, and one the escape maps to a different path.
        assert_eq!(escape_json_path("a%2Eb"), "a%252Eb");
        assert_eq!(unescape_json_path("a%252Eb"), "a%2Eb");
        let percent_first = "a%252Eb".replace("%25", "%").replace("%2E", ".");
        assert_eq!(
            percent_first, "a.b",
            "the swapped order is what this ordering exists to avoid"
        );
        assert_ne!(
            unescape_json_path("a%252Eb"),
            percent_first,
            "the two orders must not agree, or the order would not matter"
        );
    }

    /// `F-12`: **a `Dynamic` type tag outside the writer's range is an
    /// error naming the tag byte and the path**, not a default and not a
    /// null.
    ///
    /// The frame is the one-path `{"k": <tag 0x11>}` shape — `01 016b 11`
    /// — where `0x11` is not one of the six tags `land_value` and
    /// `land_array` can produce. `docs/api.md` §4.1's `500` row is where
    /// this surfaces to a caller.
    #[test]
    fn a_dynamic_tag_outside_the_writers_range_is_an_error_naming_it() {
        let frame = unhex("01016b11");
        let err = decode(&frame).expect_err("an unknown Dynamic tag must not decode");
        let message = err.to_string();
        assert!(
            message.contains("0x11"),
            "the message must name the tag byte, got {message:?}"
        );
        assert!(
            message.contains('k'),
            "the message must name the path, got {message:?}"
        );
    }

    /// `F-13`: a repeated path keeps the **first** value — the write side's
    /// own rule — **and the survivor round-trips through the column.**
    /// `a_repeated_path_keeps_the_first_value` is the first half on its
    /// own; this is the half that says the decoder sees what was stored.
    #[test]
    fn a_repeated_path_keeps_the_first_value_and_the_survivor_round_trips() {
        let value = TraceJson::from_entries(vec![
            entry(
                "k",
                TraceJsonValue::Scalar(TraceJsonScalar::Str("first".to_string())),
            ),
            entry(
                "k",
                TraceJsonValue::Scalar(TraceJsonScalar::Str("second".to_string())),
            ),
        ]);
        assert_eq!(value.len(), 1);
        let got = decode(&encode(&value)).expect("decode the survivor");
        assert_eq!(got.len(), 1, "one path in, one path out");
        assert_eq!(got, value);
        assert_eq!(
            got.entries()[0].value,
            TraceJsonValue::Scalar(TraceJsonScalar::Str("first".to_string()))
        );
    }

    /// The multi-path nested frame decodes to the three leaf paths it was
    /// built from, in the stored order — and **the decoder does not sort**:
    /// reconstructing the OTLP key order is the response assembler's, and
    /// this module returns what the column holds.
    #[test]
    fn a_nested_frame_decodes_to_its_leaf_paths_in_stored_order() {
        let value = TraceJson::from_entries(vec![
            entry(
                &escape_json_path("a.b"),
                TraceJsonValue::Scalar(TraceJsonScalar::Int(1)),
            ),
            entry(
                "c",
                TraceJsonValue::Scalar(TraceJsonScalar::Str("s".to_string())),
            ),
            entry("d.e", TraceJsonValue::Scalar(TraceJsonScalar::Bool(true))),
        ]);
        let got = decode(&encode(&value)).expect("decode the nested frame");
        assert_eq!(
            got.entries()
                .iter()
                .map(|e| e.path.as_str())
                .collect::<Vec<_>>(),
            vec!["a%2Eb", "c", "d.e"]
        );
        assert_eq!(got, value);
    }

    /// Escaping `%` before `.` is what keeps a key literally spelled
    /// `a%2Eb` from colliding with the key `a.b`.
    #[test]
    fn the_path_escape_is_percent_then_dot() {
        assert_eq!(escape_json_path("plain"), "plain");
        assert_eq!(escape_json_path("a.b"), "a%2Eb");
        assert_eq!(escape_json_path("a%2Eb"), "a%252Eb");
        assert_eq!(escape_json_path("100%"), "100%25");
        assert_ne!(escape_json_path("a.b"), escape_json_path("a%2Eb"));
    }

    /// A repeated path keeps the first value and drops the rest, because
    /// `type_json_skip_duplicated_paths` is `0` on this server and a repeat
    /// in the block is an exception rather than a silent drop.
    #[test]
    fn a_repeated_path_keeps_the_first_value() {
        let value = TraceJson::from_entries(vec![
            entry(
                "k",
                TraceJsonValue::Scalar(TraceJsonScalar::Str("first".to_string())),
            ),
            entry(
                "k",
                TraceJsonValue::Scalar(TraceJsonScalar::Str("second".to_string())),
            ),
        ]);
        assert_eq!(value.len(), 1);
        assert_eq!(
            value.entries()[0].value,
            TraceJsonValue::Scalar(TraceJsonScalar::Str("first".to_string()))
        );
    }

    /// The encoding is a function of the content, not of arrival order: two
    /// maps with the same pairs in different order encode identically.
    #[test]
    fn the_encoding_does_not_depend_on_arrival_order() {
        let a = TraceJson::from_entries(vec![
            entry("z", TraceJsonValue::Scalar(TraceJsonScalar::Int(1))),
            entry("a", TraceJsonValue::Scalar(TraceJsonScalar::Int(2))),
        ]);
        let b = TraceJson::from_entries(vec![
            entry("a", TraceJsonValue::Scalar(TraceJsonScalar::Int(2))),
            entry("z", TraceJsonValue::Scalar(TraceJsonScalar::Int(1))),
        ]);
        assert_eq!(encode(&a), encode(&b));
    }

    /// The four `tag_values.val_type` spellings and the text each scalar
    /// renders as, which is what a kind-3 row carries.
    #[test]
    fn each_scalar_renders_its_api_text_and_its_declared_type() {
        for (scalar, text, val_type) in [
            (TraceJsonScalar::Str("s".to_string()), "s", "string"),
            (TraceJsonScalar::Bool(true), "true", "bool"),
            (TraceJsonScalar::Int(-7), "-7", "int"),
            (TraceJsonScalar::Double(1.5), "1.5", "float"),
            (TraceJsonScalar::Double(f64::INFINITY), "+Inf", "float"),
            (TraceJsonScalar::Double(f64::NEG_INFINITY), "-Inf", "float"),
            (TraceJsonScalar::Double(f64::NAN), "NaN", "float"),
        ] {
            assert_eq!(scalar.render(), text, "{scalar:?}");
            assert_eq!(scalar.val_type(), val_type, "{scalar:?}");
        }
    }
}
