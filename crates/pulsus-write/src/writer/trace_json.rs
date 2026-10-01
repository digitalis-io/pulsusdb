//! The `JSON` column's own RowBinary form, written by this crate rather
//! than by the driver (issue #585).
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

/// **Refuses, always.** `pulsus_clickhouse::ChRow` requires
/// `DeserializeOwned`, and nothing on this path reads a landing row back
/// through the driver: the landing table is written and the five targets are
/// read through their own column lists. An implementation that returned a
/// value would be a decoder nothing exercises, which is worse than one that
/// says so.
impl<'de> Deserialize<'de> for TraceJson {
    fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "a JSON column is written by this encoder and never read back through it",
        ))
    }
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

/// The bytes LEB128 takes for `n`.
fn leb128_len(mut n: u64) -> u64 {
    let mut bytes = 1;
    while n >= 0x80 {
        n >>= 7;
        bytes += 1;
    }
    bytes
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
