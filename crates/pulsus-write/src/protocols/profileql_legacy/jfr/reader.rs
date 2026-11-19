//! The JFR reader: a recording's chunks, each chunk's header, metadata and
//! constant pools, and the events the profile conversion reads (§3.2 of the
//! design for issue #819). A close port of the JFR library the reference
//! pins, at tag `v0.19.0` (`parser/parser.go`, `header.go`, `metadata.go`,
//! `cpool.go`, `symbols.go` and the generated field readers of
//! `parser/types/`), which is Apache-2.0. Every error text is the library's.

use std::borrow::Cow;
use std::cell::Cell;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::rc::Rc;
use std::sync::LazyLock;

use regex::Regex;

const CHUNK_HEADER_SIZE: usize = 68;
const CHUNK_MAGIC: u32 = 0x464c_5200;

/// The process's secret hash seed, drawn from the standard library's
/// random keys.
static SEED: LazyLock<u64> = LazyLock::new(|| RandomState::new().hash_one(0x819u64));

/// A hash for the ids and names the body chooses: each word is mixed with a
/// secret seed by one folded 128-bit multiply, whose high half reaches the
/// bucket bits, so a body cannot pick keys that share a bucket.
#[derive(Clone, Copy)]
pub(crate) struct IdState(u64);

impl Default for IdState {
    fn default() -> Self {
        IdState(*SEED)
    }
}

impl BuildHasher for IdState {
    type Hasher = IdHasher;

    fn build_hasher(&self) -> IdHasher {
        IdHasher(self.0)
    }
}

pub(crate) struct IdHasher(u64);

const MIX: u64 = 0x5851_f42d_4c95_7f2d;

impl IdHasher {
    #[inline]
    fn add(&mut self, n: u64) {
        let x = u128::from(self.0 ^ n) * u128::from(MIX);
        self.0 = (x as u64) ^ (x >> 64) as u64;
    }
}

impl Hasher for IdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            let mut w = [0u8; 8];
            w.copy_from_slice(c);
            self.add(u64::from_le_bytes(w));
        }
        let mut w = [0u8; 8];
        w[..chunks.remainder().len()].copy_from_slice(chunks.remainder());
        self.add(u64::from_le_bytes(w) ^ (bytes.len() as u64) << 56);
    }

    #[inline]
    fn write_u8(&mut self, n: u8) {
        self.add(u64::from(n));
    }

    #[inline]
    fn write_u32(&mut self, n: u32) {
        self.add(u64::from(n));
    }

    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.add(n);
    }

    #[inline]
    fn write_usize(&mut self, n: usize) {
        self.add(n as u64);
    }
}

/// A hash map keyed by ids or names the body chooses.
pub(crate) type IdMap<K, V> = HashMap<K, V, IdState>;

/// What a push's body allocates per byte of it, at most
/// ([`budget_bytes`]).
pub const BUDGET_PER_BODY_BYTE: u64 = 16;
/// What any body may allocate beside that.
pub const BUDGET_FLOOR: u64 = 1 << 20;
/// What any body may allocate at all: sixteen times the model's profile
/// size limit.
pub const BUDGET_CAP: u64 = 16 * 4 * 1024 * 1024;

/// The bytes a JFR body of `len` bytes may have the reader and the
/// builders allocate from it: [`BUDGET_PER_BODY_BYTE`] per byte plus
/// [`BUDGET_FLOOR`], and never more than [`BUDGET_CAP`].
pub fn budget_bytes(len: usize) -> u64 {
    (len as u64)
        .saturating_mul(BUDGET_PER_BODY_BYTE)
        .saturating_add(BUDGET_FLOOR)
        .min(BUDGET_CAP)
}

/// The allocation one push has left. Every allocation made from the body's
/// data is charged before it is made, at the size it may grow to; past the
/// budget the push is refused.
#[derive(Clone, Debug)]
pub(crate) struct Budget(Rc<Cell<u64>>);

impl Budget {
    pub(crate) fn new(bytes: u64) -> Budget {
        Budget(Rc::new(Cell::new(bytes)))
    }

    /// `true` when `bytes` fit, which are then spent.
    pub(crate) fn charge(&self, bytes: u64) -> bool {
        let left = self.0.get();
        if bytes > left {
            self.0.set(0);
            return false;
        }
        self.0.set(left - bytes);
        true
    }

    fn take(&self, bytes: u64) -> Read<()> {
        if self.charge(bytes) {
            Ok(())
        } else {
            Err(ReadError::Budget)
        }
    }
}

/// The text a push over its budget is refused with.
pub(crate) const OVER_BUDGET: &str = "the recording needs more memory than its size allows";

/// A map entry's charge: key and value, twice for the table's growth, and
/// its control bytes.
const fn entry<K, V>() -> u64 {
    2 * (std::mem::size_of::<K>() + std::mem::size_of::<V>()) as u64 + 2
}

/// A string's charge: what it copied from the body.
fn owned_len(s: &str, owned: bool) -> u64 {
    if owned { s.len() as u64 } else { 0 }
}

/// One frame of a stack trace: its method's pool id and its line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Frame {
    pub(crate) method: u64,
    pub(crate) line: u32,
}

/// A method: its class's and its name's pool ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Method {
    pub(crate) class: u64,
    pub(crate) name: u64,
}

/// The constant pools the conversion reads, emptied at each chunk. A later
/// entry of one id replaces the earlier one.
#[derive(Debug, Default)]
pub(crate) struct Pools<'a> {
    /// Stack trace id to its frames, top frame first.
    pub(crate) stack_traces: IdMap<u64, Vec<Frame>>,
    pub(crate) methods: IdMap<u64, Method>,
    /// Class id to its name's symbol id.
    pub(crate) classes: IdMap<u64, u64>,
    pub(crate) symbols: IdMap<u64, Cow<'a, str>>,
    /// Thread state id to its name.
    pub(crate) thread_states: IdMap<u64, Cow<'a, str>>,
}

impl Pools<'_> {
    /// The symbol's text, empty when the pool has no such symbol.
    pub(crate) fn symbol(&self, id: u64) -> &str {
        self.symbols.get(&id).map_or("", |s| s)
    }

    fn clear(&mut self) {
        self.stack_traces.clear();
        self.methods.clear();
        self.classes.clear();
        self.symbols.clear();
        self.thread_states.clear();
    }
}

/// The fields an event's samples are keyed by beside their stack.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) struct Correlation {
    pub(crate) context_id: u64,
    pub(crate) span_id: u64,
    pub(crate) span_name: u64,
    pub(crate) trace_id_hi: u64,
    pub(crate) trace_id_lo: u64,
}

/// One event the conversion reads (§3.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Event<'a> {
    ExecutionSample {
        stack: u64,
        state: u64,
        correlation: Correlation,
    },
    WallClockSample {
        stack: u64,
        state: u64,
        samples: u32,
        correlation: Correlation,
    },
    AllocInNewTlab {
        stack: u64,
        tlab_size: u64,
        correlation: Correlation,
    },
    AllocOutsideTlab {
        stack: u64,
        allocation_size: u64,
        correlation: Correlation,
    },
    MonitorEnter {
        stack: u64,
        duration: u64,
        correlation: Correlation,
    },
    ThreadPark {
        stack: u64,
        duration: u64,
    },
    LiveObject {
        stack: u64,
    },
    AllocSample {
        stack: u64,
        weight: u64,
    },
    Malloc {
        stack: u64,
        size: u64,
    },
    ActiveSetting {
        name: Cow<'a, str>,
        value: Cow<'a, str>,
    },
}

/// A read failure, rendered as the library renders it.
#[derive(Debug)]
enum ReadError {
    Eof,
    Overflow,
    Budget,
    Message(String),
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::Eof => f.write_str("unexpected EOF"),
            ReadError::Overflow => f.write_str("int overflow"),
            ReadError::Budget => f.write_str(OVER_BUDGET),
            ReadError::Message(m) => f.write_str(m),
        }
    }
}

type Read<T> = Result<T, ReadError>;

fn message(m: String) -> ReadError {
    ReadError::Message(m)
}

/// A position over the whole recording; every read is bounded by its end,
/// not by the chunk's.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    #[inline]
    fn byte(&mut self) -> Read<u8> {
        let b = *self.buf.get(self.pos).ok_or(ReadError::Eof)?;
        self.pos += 1;
        Ok(b)
    }

    #[inline]
    fn varint32(&mut self) -> Read<u32> {
        let mut v: u32 = 0;
        let mut shift = 0u32;
        loop {
            if shift >= 32 {
                return Err(ReadError::Overflow);
            }
            let b = self.byte()?;
            v |= u32::from(b & 0x7f) << shift;
            if b < 0x80 {
                return Ok(v);
            }
            shift += 7;
        }
    }

    #[inline]
    fn varint16(&mut self) -> Read<u16> {
        let mut v: u16 = 0;
        let mut shift = 0u32;
        loop {
            if shift >= 16 {
                return Err(ReadError::Overflow);
            }
            let b = self.byte()?;
            v |= u16::from(b & 0x7f) << shift;
            if b < 0x80 {
                return Ok(v);
            }
            shift += 7;
        }
    }

    /// Up to nine bytes, the ninth taken whole.
    #[inline]
    fn varlong(&mut self) -> Read<u64> {
        let mut v: u64 = 0;
        let mut shift = 0u32;
        loop {
            let b = self.byte()?;
            if shift == 56 {
                return Ok(v | u64::from(b) << 56);
            }
            v |= u64::from(b & 0x7f) << shift;
            if b < 0x80 {
                return Ok(v);
            }
            shift += 7;
        }
    }

    /// A count of things each at least one byte long: more than the bytes
    /// left is a read past the end, refused before anything is read or held.
    fn count(&mut self) -> Read<usize> {
        let n = self.varint32()? as usize;
        if n > self.buf.len().saturating_sub(self.pos) {
            return Err(ReadError::Eof);
        }
        Ok(n)
    }

    fn bytes(&mut self) -> Read<&'a [u8]> {
        let n = self.varint32()? as usize;
        if self.pos + n > self.buf.len() {
            return Err(ReadError::Eof);
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    /// A UTF-16 string: a count, then each code unit as a varint.
    fn chars(&mut self) -> Read<String> {
        let n = self.count()?;
        let mut out = String::with_capacity(n);
        for _ in 0..n {
            let c = self.varint32()?;
            out.push(char::from_u32(c).unwrap_or(char::REPLACEMENT_CHARACTER));
        }
        Ok(out)
    }

    /// A metadata string: encodings 0 and 1 are empty, 3 UTF-8, 4 UTF-16.
    fn meta_string(&mut self) -> Read<Cow<'a, str>> {
        match self.byte()? {
            0 | 1 => Ok(Cow::Borrowed("")),
            3 => Ok(utf8(self.bytes()?)),
            4 => Ok(Cow::Owned(self.chars()?)),
            b => Err(message(format!("unknown string type {b}"))),
        }
    }

    /// A field's string: as [`Self::meta_string`], and 5 Latin-1. An unknown
    /// encoding names its position from `base`.
    fn field_string(&mut self, base: usize) -> Read<Cow<'a, str>> {
        match self.byte()? {
            0 | 1 => Ok(Cow::Borrowed("")),
            3 => Ok(utf8(self.bytes()?)),
            5 => Ok(Cow::Owned(
                self.bytes()?.iter().map(|&b| b as char).collect(),
            )),
            4 => Ok(Cow::Owned(self.chars()?)),
            b => Err(message(format!(
                "unknown string type {b} at {}",
                self.pos - base
            ))),
        }
    }
}

fn utf8(bytes: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// A field: its name shares the metadata's copy.
struct FieldDef {
    name: Rc<str>,
    ty: i64,
    cp: bool,
    array: bool,
}

struct ClassDef {
    id: i64,
    name: Rc<str>,
    fields: Vec<FieldDef>,
}

/// The most bytes of one body-derived name an error text carries.
const ERROR_NAME_BYTES: usize = 256;
/// The most fields a class description in an error text lists.
const ERROR_FIELDS: usize = 32;

/// `s` as an error text may carry it: whole when short, else its first
/// [`ERROR_NAME_BYTES`] bytes and `...`.
fn short(s: &str) -> Cow<'_, str> {
    if s.len() <= ERROR_NAME_BYTES {
        return Cow::Borrowed(s);
    }
    Cow::Owned(format!(
        "{}...",
        &s[..s.floor_char_boundary(ERROR_NAME_BYTES)]
    ))
}

impl fmt::Display for ClassDef {
    /// `class{name: N, id: I, fields: [{Name:n Type:t ConstantPool:b Array:b} ...]}`,
    /// the library's text. Each name is cut at [`ERROR_NAME_BYTES`] bytes and
    /// the list at [`ERROR_FIELDS`] fields, so the text stays short whatever
    /// the body declares.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "class{{name: {}, id: {}, fields: [",
            short(&self.name),
            self.id
        )?;
        for (i, field) in self.fields.iter().take(ERROR_FIELDS).enumerate() {
            if i > 0 {
                f.write_str(" ")?;
            }
            write!(
                f,
                "{{Name:{} Type:{} ConstantPool:{} Array:{}}}",
                short(&field.name),
                field.ty,
                field.cp,
                field.array
            )?;
        }
        if self.fields.len() > ERROR_FIELDS {
            write!(f, " ... {} more", self.fields.len() - ERROR_FIELDS)?;
        }
        f.write_str("]}")
    }
}

/// `strconv.Atoi`.
fn atoi(s: &str) -> Read<i64> {
    // Twenty characters hold any `i64`: a longer text is refused without
    // reading it, so a long string named by many classes costs nothing.
    if s.len() > 20 {
        return Err(message(format!(
            "strconv.Atoi: parsing {:?}: invalid syntax",
            &s[..s.floor_char_boundary(20)]
        )));
    }
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(message(format!(
            "strconv.Atoi: parsing {s:?}: invalid syntax"
        )));
    }
    s.parse::<i64>()
        .map_err(|_| message(format!("strconv.Atoi: parsing {s:?}: value out of range")))
}

/// One metadata element's header: its name, its attributes when asked for,
/// and its child count, as indexes into the metadata's strings.
struct Element {
    name: usize,
    attrs: Vec<(usize, usize)>,
    children: u32,
}

impl Element {
    /// The attribute's last value, `""` when absent (a Go map's read).
    fn attr<'s>(&self, strings: &'s [Rc<str>], key: &str) -> &'s str {
        self.attr_index(strings, key).map_or("", |i| &strings[i])
    }

    fn attr_index(&self, strings: &[Rc<str>], key: &str) -> Option<usize> {
        self.attrs
            .iter()
            .rev()
            .find(|(k, _)| &*strings[*k] == key)
            .map(|(_, v)| *v)
    }
}

fn read_element(
    cur: &mut Cursor<'_>,
    strings: usize,
    with_attrs: bool,
    budget: &Budget,
) -> Read<Element> {
    let mut index = || -> Read<usize> {
        let i = cur.varint32()? as usize;
        if i >= strings {
            return Err(ReadError::Overflow);
        }
        Ok(i)
    };
    let name = index()?;
    let count = cur.count()?;
    if with_attrs {
        budget.take(2 * 16 * count as u64 + 64)?;
    }
    let mut attrs = Vec::new();
    for _ in 0..count {
        let k = {
            let i = cur.varint32()? as usize;
            if i >= strings {
                return Err(ReadError::Overflow);
            }
            i
        };
        let v = {
            let i = cur.varint32()? as usize;
            if i >= strings {
                return Err(ReadError::Overflow);
            }
            i
        };
        if with_attrs {
            attrs.push((k, v));
        }
    }
    let children = cur.count()? as u32;
    Ok(Element {
        name,
        attrs,
        children,
    })
}

/// The metadata event at `pos`, inside `buf` from `lo` and inside its own
/// declared length: every class of its `metadata` element.
fn read_metadata(buf: &[u8], lo: usize, pos: i64, budget: &Budget) -> Read<Vec<ClassDef>> {
    let at = seek(buf, lo, pos)?;
    let mut cur = Cursor { buf, pos: at };
    let size = cur.varint32()? as usize;
    cur.buf = &buf[..at.saturating_add(size).min(buf.len())];
    cur.varint32()?;
    cur.varlong()?;
    cur.varlong()?;
    cur.varlong()?;
    let n = cur.count()?;
    budget.take(n as u64 * 2 * std::mem::size_of::<Rc<str>>() as u64)?;
    let mut strings: Vec<Rc<str>> = Vec::with_capacity(n);
    for _ in 0..n {
        let text = cur.meta_string()?;
        budget.take(text.len() as u64 + 16)?;
        strings.push(Rc::from(text.as_ref()));
    }
    let s = strings.len();
    let mut classes = Vec::new();
    let root = read_element(&mut cur, s, false, budget)?;
    if &*strings[root.name] != "root" {
        return Err(message(format!(
            "expected root element, got {}",
            short(&strings[root.name])
        )));
    }
    for _ in 0..root.children {
        let meta = read_element(&mut cur, s, false, budget)?;
        match &*strings[meta.name] {
            "metadata" => {
                for _ in 0..meta.children {
                    let class = read_element(&mut cur, s, true, budget)?;
                    let id = atoi(class.attr(&strings, "id"))?;
                    let Some(name) = class
                        .attr_index(&strings, "name")
                        .filter(|&i| !strings[i].is_empty())
                    else {
                        return Err(message("class/field name is empty".to_string()));
                    };
                    let mut fields = Vec::new();
                    for _ in 0..class.children {
                        let child = read_element(&mut cur, s, true, budget)?;
                        if &*strings[child.name] == "field" {
                            budget.take(2 * std::mem::size_of::<FieldDef>() as u64)?;
                            fields.push(field_def(&child, &strings)?);
                        }
                        for _ in 0..child.children {
                            read_element(&mut cur, s, false, budget)?;
                        }
                    }
                    budget.take(2 * std::mem::size_of::<ClassDef>() as u64)?;
                    classes.push(ClassDef {
                        id,
                        name: Rc::clone(&strings[name]),
                        fields,
                    });
                }
            }
            "region" => {}
            other => return Err(message(format!("unexpected element {}", short(other)))),
        }
    }
    Ok(classes)
}

fn field_def(e: &Element, strings: &[Rc<str>]) -> Read<FieldDef> {
    let ty = atoi(e.attr(strings, "class"))?;
    let Some(name) = e
        .attr_index(strings, "name")
        .filter(|&i| !strings[i].is_empty())
    else {
        return Err(message("class/field name is empty".to_string()));
    };
    let array = match e.attr(strings, "dimension") {
        "" => false,
        "1" => true,
        d => return Err(message(format!("unsupported dimension {}", short(d)))),
    };
    Ok(FieldDef {
        name: Rc::clone(&strings[name]),
        ty,
        cp: e.attr(strings, "constantPool") == "true",
        array,
    })
}

/// `pos` when it lies in `buf` at or after `lo`, the chunk's start.
fn seek(buf: &[u8], lo: usize, pos: i64) -> Read<usize> {
    if pos >= lo as i64 && (pos as u64) < buf.len() as u64 {
        Ok(pos as usize)
    } else {
        Err(ReadError::Eof)
    }
}

// ---------------------------------------------------------------------------
// Field binding
// ---------------------------------------------------------------------------

/// The types a binding names, resolved per chunk.
#[derive(Clone, Copy)]
enum Ty {
    Int,
    Long,
    Str,
    ThreadState,
    StackTrace,
    StackFrame,
    Class,
    Method,
    Symbol,
}

/// Where a bound field's value goes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Slot {
    None,
    Num(u8),
    Text(u8),
    Frames,
}

/// How a field is read.
#[derive(Clone, Copy)]
enum Kind {
    Cp,
    Str,
    Int,
    Long,
    Short,
    Boolean,
    Float,
    /// A stack trace's frames, read field by field (the stack trace pool only).
    Frames,
    /// An object of the class at this index, skipped.
    Nested(usize),
    /// A type with no fields, or none at all: refused when met.
    Unknown {
        ty: i64,
        class: Option<usize>,
    },
}

#[derive(Clone, Copy)]
struct Plan {
    array: bool,
    kind: Kind,
    slot: Slot,
}

/// A binding: the field name, the type, constant pool and array flags it
/// must have, and the slot it fills.
type Spec = (&'static str, Ty, bool, bool, Slot);

const STACK: Slot = Slot::Num(0);
const VALUE: Slot = Slot::Num(1);
const SAMPLES: Slot = Slot::Num(7);

const fn correlation_specs() -> [Spec; 5] {
    [
        ("contextId", Ty::Long, false, false, Slot::Num(2)),
        ("spanId", Ty::Long, false, false, Slot::Num(3)),
        ("spanName", Ty::Long, false, false, Slot::Num(4)),
        ("traceIdHi", Ty::Long, false, false, Slot::Num(5)),
        ("traceIdLo", Ty::Long, false, false, Slot::Num(6)),
    ]
}

const STACK_SPEC: Spec = ("stackTrace", Ty::StackTrace, true, false, STACK);

/// The events read, by metadata name, in the library's dispatch order, with
/// the fields each binds.
const EVENT_NAMES: [&str; 10] = [
    "jdk.ExecutionSample",
    "profiler.WallClockSample",
    "profiler.Malloc",
    "jdk.ObjectAllocationInNewTLAB",
    "jdk.ObjectAllocationOutsideTLAB",
    "jdk.ObjectAllocationSample",
    "profiler.LiveObject",
    "jdk.JavaMonitorEnter",
    "jdk.ThreadPark",
    "jdk.ActiveSetting",
];

fn event_specs(i: usize) -> Vec<Spec> {
    let c = correlation_specs();
    let long = |name| (name, Ty::Long, false, false, VALUE);
    let mut v = match i {
        0 => vec![STACK_SPEC, ("state", Ty::ThreadState, true, false, VALUE)],
        1 => vec![
            STACK_SPEC,
            ("state", Ty::ThreadState, true, false, VALUE),
            ("samples", Ty::Int, false, false, SAMPLES),
        ],
        2 => vec![STACK_SPEC, long("size")],
        3 => vec![STACK_SPEC, long("tlabSize")],
        4 => vec![STACK_SPEC, long("allocationSize")],
        5 => vec![STACK_SPEC, long("weight")],
        6 => vec![STACK_SPEC],
        7 => vec![STACK_SPEC, long("duration")],
        8 => vec![STACK_SPEC, long("duration")],
        _ => vec![
            ("name", Ty::Str, false, false, Slot::Text(0)),
            ("value", Ty::Str, false, false, Slot::Text(1)),
        ],
    };
    if matches!(i, 0 | 1 | 3 | 4 | 7) {
        v.extend_from_slice(&c);
    }
    v
}

/// The pools kept, by class name, and the fields each binds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PoolKind {
    Symbol,
    ThreadState,
    Class,
    Method,
    StackTrace,
    /// `java.lang.String`: per entry an id and a string, no fields.
    Strings,
    /// `jdk.types.ChunkHeader`: 68 bytes.
    ChunkHeader,
    /// Read by its fields and dropped.
    Skip,
}

fn pool_kind(name: &str) -> PoolKind {
    match name {
        "jdk.types.Symbol" => PoolKind::Symbol,
        "jdk.types.ThreadState" => PoolKind::ThreadState,
        "java.lang.Class" => PoolKind::Class,
        "jdk.types.Method" => PoolKind::Method,
        "jdk.types.StackTrace" => PoolKind::StackTrace,
        "java.lang.String" => PoolKind::Strings,
        "jdk.types.ChunkHeader" => PoolKind::ChunkHeader,
        _ => PoolKind::Skip,
    }
}

fn pool_specs(kind: PoolKind) -> Vec<Spec> {
    match kind {
        PoolKind::Symbol => vec![("string", Ty::Str, false, false, Slot::Text(0))],
        PoolKind::ThreadState => vec![("name", Ty::Str, false, false, Slot::Text(0))],
        PoolKind::Class => vec![("name", Ty::Symbol, true, false, Slot::Num(0))],
        PoolKind::Method => vec![
            ("type", Ty::Class, true, false, Slot::Num(0)),
            ("name", Ty::Symbol, true, false, Slot::Num(1)),
        ],
        PoolKind::StackTrace => vec![("frames", Ty::StackFrame, false, true, Slot::Frames)],
        _ => Vec::new(),
    }
}

const FRAME_SPECS: [Spec; 2] = [
    ("method", Ty::Method, true, false, Slot::Num(0)),
    ("lineNumber", Ty::Int, false, false, Slot::Num(1)),
];

/// One field's values: the bound numbers, strings and frames.
#[derive(Default)]
struct Values<'a> {
    num: [u64; 8],
    text: [Cow<'a, str>; 2],
    frames: Vec<Frame>,
}

impl Values<'_> {
    fn reset(&mut self) {
        self.num = [0; 8];
        self.text = [Cow::Borrowed(""), Cow::Borrowed("")];
    }
}

/// The type ids a chunk's metadata gives the bound types.
#[derive(Clone, Copy)]
struct Types {
    int: i64,
    long: i64,
    short: i64,
    float: i64,
    boolean: i64,
    string: i64,
    thread_state: i64,
    class: i64,
    method: i64,
    symbol: i64,
    stack_trace: i64,
    stack_frame: i64,
}

impl Types {
    fn of(&self, ty: Ty) -> i64 {
        match ty {
            Ty::Int => self.int,
            Ty::Long => self.long,
            Ty::Str => self.string,
            Ty::ThreadState => self.thread_state,
            Ty::StackTrace => self.stack_trace,
            Ty::StackFrame => self.stack_frame,
            Ty::Class => self.class,
            Ty::Method => self.method,
            Ty::Symbol => self.symbol,
        }
    }
}

/// One chunk's metadata: its classes by id, the bound types and the plans.
struct Meta {
    classes: Vec<ClassDef>,
    by_id: IdMap<i64, usize>,
    types: Types,
    /// The event types read, by [`EVENT_NAMES`] position, with their plans;
    /// `None` for a type the metadata does not declare.
    events: Vec<(Option<i64>, Vec<Plan>)>,
    frame_plans: Vec<Plan>,
    /// Each class's pool kind and plan, built when its pool is first met.
    pools: Vec<Option<(PoolKind, Vec<Plan>)>>,
}

impl Meta {
    fn new(classes: Vec<ClassDef>, budget: &Budget) -> Read<Meta> {
        budget.take(
            classes.len() as u64
                * (entry::<i64, usize>()
                    + 2 * std::mem::size_of::<Option<(PoolKind, Vec<Plan>)>>() as u64),
        )?;
        let mut by_id = IdMap::default();
        for (i, c) in classes.iter().enumerate() {
            by_id.insert(c.id, i);
        }
        let last = |name: &str| classes.iter().rposition(|c| &*c.name == name);
        let need = |name: &str| {
            last(name)
                .map(|i| classes[i].id)
                .ok_or_else(|| message(format!("missing \"{name}\"")))
        };
        let int = need("int")?;
        let long = need("long")?;
        let short = need("short")?;
        let float = need("float")?;
        let boolean = need("boolean")?;
        let string = need("java.lang.String")?;
        need("jdk.types.FrameType")?;
        let thread_state = need("jdk.types.ThreadState")?;
        need("java.lang.Thread")?;
        let class = need("java.lang.Class")?;
        let method = need("jdk.types.Method")?;
        need("jdk.types.Package")?;
        let symbol = need("jdk.types.Symbol")?;
        let stack_trace = need("jdk.types.StackTrace")?;
        need("jdk.types.ClassLoader")?;
        let stack_frame = need("jdk.types.StackFrame")?;
        let types = Types {
            int,
            long,
            short,
            float,
            boolean,
            string,
            thread_state,
            class,
            method,
            symbol,
            stack_trace,
            stack_frame,
        };
        let mut meta = Meta {
            pools: (0..classes.len()).map(|_| None).collect(),
            classes,
            by_id,
            types,
            events: Vec::new(),
            frame_plans: Vec::new(),
        };
        let fields: usize = std::iter::once("jdk.types.StackFrame")
            .chain(EVENT_NAMES)
            .filter_map(|name| meta.class_index(name))
            .map(|c| meta.classes[c].fields.len())
            .sum();
        budget.take(fields as u64 * std::mem::size_of::<Plan>() as u64)?;
        meta.frame_plans = meta.plans_by_name("jdk.types.StackFrame", &FRAME_SPECS, false);
        for (i, name) in EVENT_NAMES.iter().enumerate() {
            let id = meta.class_index(name).map(|c| meta.classes[c].id);
            let plans = meta.plans_by_name(name, &event_specs(i), false);
            meta.events.push((id, plans));
        }
        Ok(meta)
    }

    /// The last class of this name, as the library's name map keeps it.
    fn class_index(&self, name: &str) -> Option<usize> {
        self.classes.iter().rposition(|c| &*c.name == name)
    }

    fn plans_by_name(&self, name: &str, specs: &[Spec], frames: bool) -> Vec<Plan> {
        match self.class_index(name) {
            Some(i) => self.plans(i, specs, frames),
            None => Vec::new(),
        }
    }

    /// How each field of the class is read, and where a bound one goes.
    fn plans(&self, class: usize, specs: &[Spec], frames: bool) -> Vec<Plan> {
        let t = &self.types;
        self.classes[class]
            .fields
            .iter()
            .map(|f| {
                let slot = specs
                    .iter()
                    .find(|(name, ty, cp, array, _)| {
                        &*f.name == *name && f.ty == t.of(*ty) && f.cp == *cp && f.array == *array
                    })
                    .map_or(Slot::None, |s| s.4);
                let kind = if f.cp {
                    Kind::Cp
                } else if f.ty == t.string {
                    Kind::Str
                } else if f.ty == t.int {
                    Kind::Int
                } else if f.ty == t.long {
                    Kind::Long
                } else if f.ty == t.short {
                    Kind::Short
                } else if f.ty == t.boolean {
                    Kind::Boolean
                } else if f.ty == t.float {
                    Kind::Float
                } else if frames && f.ty == t.stack_frame {
                    Kind::Frames
                } else {
                    match self.by_id.get(&f.ty) {
                        Some(&c) if !self.classes[c].fields.is_empty() => Kind::Nested(c),
                        c => Kind::Unknown {
                            ty: f.ty,
                            class: c.copied(),
                        },
                    }
                };
                Plan {
                    array: f.array,
                    kind,
                    slot,
                }
            })
            .collect()
    }

    /// Reads one object's fields by `plans` into `out`.
    fn read_object<'a>(
        &self,
        cur: &mut Cursor<'a>,
        base: usize,
        plans: &[Plan],
        out: &mut Values<'a>,
        budget: &Budget,
    ) -> Read<()> {
        for p in plans {
            let n = if p.array {
                let n = cur.count()?;
                if matches!(p.kind, Kind::Frames) && n > 0 && self.frame_plans.is_empty() {
                    return Err(message(
                        "a stack frame type with no field reads no byte".to_string(),
                    ));
                }
                if p.slot == Slot::Frames {
                    budget.take(n as u64 * std::mem::size_of::<Frame>() as u64)?;
                    out.frames = Vec::with_capacity(n);
                }
                n
            } else {
                1
            };
            for _ in 0..n {
                match p.kind {
                    Kind::Cp | Kind::Long => {
                        let v = cur.varlong()?;
                        if let Slot::Num(i) = p.slot {
                            out.num[i as usize] = v;
                        }
                    }
                    Kind::Str => {
                        let s = cur.field_string(base)?;
                        if let Slot::Text(i) = p.slot {
                            budget.take(owned_len(&s, matches!(s, Cow::Owned(_))))?;
                            out.text[i as usize] = s;
                        }
                    }
                    Kind::Int => {
                        let v = cur.varint32()?;
                        if let Slot::Num(i) = p.slot {
                            out.num[i as usize] = u64::from(v);
                        }
                    }
                    Kind::Short => {
                        cur.varint16()?;
                    }
                    Kind::Boolean => {
                        let b = cur.byte()?;
                        if let Slot::Num(i) = p.slot {
                            out.num[i as usize] = u64::from(b != 0);
                        }
                    }
                    Kind::Float => {
                        cur.varint32()?;
                    }
                    Kind::Frames => {
                        let frame = self.read_frame(cur, base)?;
                        if p.slot == Slot::Frames {
                            out.frames.push(frame);
                        }
                    }
                    Kind::Nested(c) => {
                        let k = if p.array { cur.count()? } else { 1 };
                        for _ in 0..k {
                            self.skip_nested(cur, base, c)?;
                        }
                    }
                    Kind::Unknown { ty, class } => {
                        let class =
                            class.map_or("class{nil}".to_string(), |c| self.classes[c].to_string());
                        return Err(message(format!("unknown type {ty} {class}")));
                    }
                }
            }
        }
        Ok(())
    }

    /// One stack frame by the frame plans: its method and its line. A frame
    /// binds numbers only, so its strings are read and dropped.
    fn read_frame(&self, cur: &mut Cursor<'_>, base: usize) -> Read<Frame> {
        let mut num = [0u64; 2];
        for p in &self.frame_plans {
            let n = if p.array { cur.count()? } else { 1 };
            for _ in 0..n {
                let v = match p.kind {
                    Kind::Cp | Kind::Long => cur.varlong()?,
                    Kind::Int => u64::from(cur.varint32()?),
                    Kind::Short => u64::from(cur.varint16()?),
                    Kind::Boolean => u64::from(cur.byte()? != 0),
                    Kind::Float => u64::from(cur.varint32()?),
                    Kind::Str => {
                        cur.field_string(base)?;
                        0
                    }
                    Kind::Nested(c) => {
                        let k = if p.array { cur.count()? } else { 1 };
                        for _ in 0..k {
                            self.skip_nested(cur, base, c)?;
                        }
                        0
                    }
                    Kind::Frames | Kind::Unknown { .. } => {
                        let mut v = Values::default();
                        let budget = Budget::new(0);
                        self.read_object(cur, base, std::slice::from_ref(p), &mut v, &budget)?;
                        0
                    }
                };
                if let Slot::Num(i) = p.slot
                    && (i as usize) < num.len()
                {
                    num[i as usize] = v;
                }
            }
        }
        Ok(Frame {
            method: num[0],
            line: num[1] as u32,
        })
    }

    /// An object inside an object: references as 32-bit varints, and no
    /// object inside it.
    fn skip_nested(&self, cur: &mut Cursor<'_>, base: usize, class: usize) -> Read<()> {
        let t = &self.types;
        for f in &self.classes[class].fields {
            if f.cp || f.ty == t.int || f.ty == t.float {
                cur.varint32()?;
            } else if f.ty == t.string {
                cur.field_string(base)?;
            } else if f.ty == t.long {
                cur.varlong()?;
            } else if f.ty == t.short {
                cur.varint16()?;
            } else if f.ty == t.boolean {
                cur.byte()?;
            } else {
                return Err(message("nested objects not implemented. ".to_string()));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Symbols
// ---------------------------------------------------------------------------

/// One rewrite of a generated class or a temporary library name to a stable
/// one, tried when its marker is present (`parser/symbols.go:19-62`). Go's
/// `\d` and `\w` are ASCII, so they are written as ASCII ranges here.
struct Rule {
    marker: &'static str,
    prefix: bool,
    regex: Regex,
    with: &'static str,
}

static RULES: LazyLock<Vec<Rule>> = LazyLock::new(|| {
    let rule = |marker, prefix, pattern: &str, with| Rule {
        marker,
        prefix,
        regex: Regex::new(pattern).expect("a valid symbol rule"),
        with,
    };
    vec![
        rule(
            "jdk/internal/reflect/GeneratedMethodAccessor",
            true,
            r"^(jdk/internal/reflect/GeneratedMethodAccessor)([0-9]+)$",
            "${1}_",
        ),
        rule(
            "$$Lambda",
            false,
            r"^(.+\$\$Lambda)(\$[a-zA-Z][0-9A-Za-z_$]*?)?((\$[0-9]+)?[./](0x)?[0-9a-f]+|\$?[0-9]+)$",
            "${1}${2}_",
        ),
        rule(
            "libzstd-jni-",
            false,
            r"^(\.?/tmp/)?(libzstd-jni-[0-9]+\.[0-9]+\.[0-9]+-)([0-9]+)(\.so)( \(deleted\))?$",
            "libzstd-jni-_.so",
        ),
        rule(
            "amazonCorrettoCryptoProvider",
            false,
            concat!(
                r"^(\.?/tmp/)?(lib)?(amazonCorrettoCryptoProvider)(NativeLibraries\.)?([0-9a-f]{16})",
                r"(/libcrypto|/libamazonCorrettoCryptoProvider)?(\.so)( \(deleted\))?$"
            ),
            "libamazonCorrettoCryptoProvider_.so",
        ),
        rule(
            "libasyncProfiler-",
            false,
            r"^(\.?/tmp/)?(libasyncProfiler)-(linux-arm64|linux-musl-x64|linux-x64|macos)-(17b9a1d8156277a98ccc871afa9a8f69215f92)(\.so)( \(deleted\))?$",
            "libasyncProfiler-_.so",
        ),
        rule(
            "$$EnhancerBySpringCGLIB$$",
            false,
            r"^(.+\$\$EnhancerBySpringCGLIB\$\$)(.*)$",
            "${1}_",
        ),
    ]
});

/// A symbol through every rule in turn (`parser/symbols.go:64-81`).
pub(crate) fn merge_generated_class(symbol: Cow<'_, str>) -> Cow<'_, str> {
    let mut s = symbol;
    for r in RULES.iter() {
        let present = if r.prefix {
            s.starts_with(r.marker)
        } else {
            s.contains(r.marker)
        };
        if present && let Cow::Owned(n) = r.regex.replace_all(&s, r.with) {
            s = Cow::Owned(n);
        }
    }
    s
}

// ---------------------------------------------------------------------------
// The reader
// ---------------------------------------------------------------------------

/// The recording, read event by event.
pub(crate) struct Reader<'a> {
    cur: Cursor<'a>,
    budget: Budget,
    chunk_end: i64,
    ticks_per_second: u64,
    meta: Option<Meta>,
    pools: Pools<'a>,
    values: Values<'a>,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(body: &'a [u8], budget: Budget) -> Self {
        Reader {
            cur: Cursor { buf: body, pos: 0 },
            budget,
            chunk_end: 0,
            ticks_per_second: 0,
            meta: None,
            pools: Pools::default(),
            values: Values::default(),
        }
    }

    /// The current chunk's pools.
    pub(crate) fn pools(&self) -> &Pools<'a> {
        &self.pools
    }

    /// The current chunk's ticks per second.
    pub(crate) fn ticks_per_second(&self) -> u64 {
        self.ticks_per_second
    }

    /// The next event read, `None` at the end of the recording. An error
    /// carries the text the reference answers with.
    pub(crate) fn next_event(&mut self) -> Result<Option<Event<'a>>, String> {
        self.next()
            .map_err(|e| format!("jfr parser ParseEvent error: {e}"))
    }

    fn next(&mut self) -> Read<Option<Event<'a>>> {
        loop {
            if self.cur.pos as i64 == self.chunk_end {
                if self.cur.pos == self.cur.buf.len() {
                    return Ok(None);
                }
                self.read_chunk(self.cur.pos)?;
            }
            let start = self.cur.pos;
            let size = self.cur.varlong()?;
            if size == 0 {
                return Err(ReadError::Overflow);
            }
            if (self.chunk_end.wrapping_sub(start as i64) as u64) < size {
                return Err(message(format!(
                    "invalid event size {size} at position {start}"
                )));
            }
            // Inside the declared chunk but past the body: the body is cut.
            if size > (self.cur.buf.len() - start) as u64 {
                return Err(ReadError::Eof);
            }
            let ty = self.cur.varlong()? as i64;
            let end = start.saturating_add(size as usize);
            let Some(meta) = &self.meta else {
                self.cur.pos = end;
                continue;
            };
            let Some(kind) = meta.events.iter().position(|(id, _)| *id == Some(ty)) else {
                self.cur.pos = end;
                continue;
            };
            // The event's fields are read inside the event.
            let base = self.cur.pos;
            let mut event = Cursor {
                buf: &self.cur.buf[..end.min(self.cur.buf.len())],
                pos: base,
            };
            let v = &mut self.values;
            v.reset();
            meta.read_object(&mut event, base, &meta.events[kind].1, v, &self.budget)?;
            self.cur.pos = end;
            let n = v.num;
            let correlation = Correlation {
                context_id: n[2],
                span_id: n[3],
                span_name: n[4],
                trace_id_hi: n[5],
                trace_id_lo: n[6],
            };
            let (stack, value) = (n[0], n[1]);
            return Ok(Some(match kind {
                0 => Event::ExecutionSample {
                    stack,
                    state: value,
                    correlation,
                },
                1 => Event::WallClockSample {
                    stack,
                    state: value,
                    samples: n[7] as u32,
                    correlation,
                },
                2 => Event::Malloc { stack, size: value },
                3 => Event::AllocInNewTlab {
                    stack,
                    tlab_size: value,
                    correlation,
                },
                4 => Event::AllocOutsideTlab {
                    stack,
                    allocation_size: value,
                    correlation,
                },
                5 => Event::AllocSample {
                    stack,
                    weight: value,
                },
                6 => Event::LiveObject { stack },
                7 => Event::MonitorEnter {
                    stack,
                    duration: value,
                    correlation,
                },
                8 => Event::ThreadPark {
                    stack,
                    duration: value,
                },
                _ => Event::ActiveSetting {
                    name: std::mem::take(&mut v.text[0]),
                    value: std::mem::take(&mut v.text[1]),
                },
            }));
        }
    }

    /// The chunk at `pos`: its header, metadata and constant pools; events
    /// follow its header.
    fn read_chunk(&mut self, pos: usize) -> Read<()> {
        let (size, cp, meta) = self
            .read_header(pos)
            .map_err(|e| message(format!("error reading chunk header: {e}")))?;
        // The metadata and the pools are read inside the chunk as the body
        // holds it, from its start to its declared end or the body's: no
        // chunk reads another's bytes, and none reads past the body. A
        // chunk the body cuts short is read until a read meets the cut, as
        // the reference reads it.
        let whole = self.cur.buf;
        let limit = (pos as u64)
            .saturating_add(size as u64)
            .min(whole.len() as u64) as usize;
        let chunk = &whole[..limit];
        let budget = self.budget.clone();
        let classes = read_metadata(chunk, pos, (pos as i64).wrapping_add(meta), &budget)
            .and_then(|c| Meta::new(c, &budget))
            .map_err(|e| message(format!("error reading metadata: {e}")))?;
        self.meta = Some(classes);
        self.pools.clear();
        let cp_pos = (pos as i64).wrapping_add(cp);
        self.cur.buf = chunk;
        let pools = self.read_pools(pos, cp_pos);
        self.cur.buf = whole;
        pools.map_err(|e| message(format!("error reading CP: {e} @ {cp_pos}")))?;
        for s in self.pools.symbols.values_mut() {
            if let Cow::Owned(n) = merge_generated_class(Cow::Borrowed(s.as_ref())) {
                budget.take(n.len() as u64)?;
                *s = Cow::Owned(n);
            }
        }
        self.chunk_end = (pos as i64).wrapping_add(size);
        self.cur.pos = pos + CHUNK_HEADER_SIZE;
        Ok(())
    }

    /// The chunk's size and its pool and metadata offsets.
    fn read_header(&mut self, pos: usize) -> Read<(i64, i64, i64)> {
        let buf = self.cur.buf;
        if pos + CHUNK_HEADER_SIZE > buf.len() {
            return Err(ReadError::Eof);
        }
        let h = &buf[pos..pos + CHUNK_HEADER_SIZE];
        let u32_at = |i: usize| u32::from_be_bytes([h[i], h[i + 1], h[i + 2], h[i + 3]]);
        let i64_at = |i: usize| {
            let mut b = [0u8; 8];
            b.copy_from_slice(&h[i..i + 8]);
            i64::from_be_bytes(b)
        };
        let magic = u32_at(0);
        let version = u32_at(4);
        let size = i64_at(8);
        let cp = i64_at(16);
        let meta = i64_at(24);
        if magic != CHUNK_MAGIC {
            return Err(message(format!("invalid chunk magic: {magic:x}")));
        }
        if !(0x20000..=0x2ffff).contains(&version) {
            return Err(message(format!("unknown version {version:x}")));
        }
        if cp <= 0 || meta <= 0 {
            return Err(message(format!("invalid offsets: cp {cp} meta {meta}")));
        }
        if size < CHUNK_HEADER_SIZE as i64 {
            return Err(message(format!("invalid size: {size}")));
        }
        self.ticks_per_second = i64_at(56) as u64;
        Ok((size, cp, meta))
    }

    /// The constant pool events, from the first at `pos` along their deltas,
    /// all inside the chunk that starts at `lo`, each inside its own declared
    /// length. An event is read once, and together they read no more bytes
    /// than the chunk holds.
    fn read_pools(&mut self, lo: usize, mut pos: i64) -> Read<()> {
        let chunk = self.cur.buf;
        let chunk_size = (chunk.len() - lo) as u64;
        let mut visited = std::collections::HashSet::new();
        let mut read: u64 = 0;
        loop {
            self.budget.take(entry::<i64, ()>())?;
            if !visited.insert(pos) {
                return Err(message(format!(
                    "constant pool event at {pos} is reached twice"
                )));
            }
            let start = seek(chunk, lo, pos)?;
            self.cur.buf = chunk;
            self.cur.pos = start;
            let size = self.cur.varlong()?;
            self.cur.buf =
                &chunk[..(start as u64).saturating_add(size).min(chunk.len() as u64) as usize];
            self.cur.varlong()?;
            self.cur.varlong()?;
            self.cur.varlong()?;
            let delta = self.cur.varlong()? as i64;
            self.cur.varint32()?;
            let n = self.cur.count()?;
            for _ in 0..n {
                let ty = self.cur.varlong()? as i64;
                let meta = self.meta.as_mut().ok_or(ReadError::Eof)?;
                let Some(&class) = meta.by_id.get(&ty) else {
                    return Err(message(format!("unknown type {ty}")));
                };
                if meta.pools[class].is_none() {
                    let kind = pool_kind(&meta.classes[class].name);
                    let frames = kind == PoolKind::StackTrace;
                    let fields = meta.classes[class].fields.len() as u64;
                    self.budget
                        .take(fields * std::mem::size_of::<Plan>() as u64)?;
                    let plans = meta.plans(class, &pool_specs(kind), frames);
                    meta.pools[class] = Some((kind, plans));
                }
                self.read_pool(class).map_err(|e| {
                    let meta = self.meta.as_ref().expect("read above");
                    message(format!("error reading {} {e}", meta.classes[class]))
                })?;
            }
            read += (self.cur.pos.min(chunk.len()) - start) as u64;
            if read > chunk_size {
                return Err(message(
                    "constant pool events read more than their chunk holds".to_string(),
                ));
            }
            if delta == 0 {
                return Ok(());
            }
            pos = pos.wrapping_add(delta);
            if pos <= 0 {
                return Ok(());
            }
        }
    }

    /// One pool's entries.
    fn read_pool(&mut self, class: usize) -> Read<()> {
        let meta = self.meta.as_ref().ok_or(ReadError::Eof)?;
        let (kind, plans) = meta.pools[class].as_ref().ok_or(ReadError::Eof)?;
        if *kind == PoolKind::ChunkHeader {
            if self.cur.pos + CHUNK_HEADER_SIZE > self.cur.buf.len() {
                return Err(ReadError::Eof);
            }
            self.cur.pos += CHUNK_HEADER_SIZE;
            return Ok(());
        }
        let cur = &mut self.cur;
        let base = cur.pos;
        let n = cur.count()?;
        let v = &mut self.values;
        for _ in 0..n {
            let id = cur.varlong()?;
            if *kind == PoolKind::Strings {
                cur.field_string(base)?;
                continue;
            }
            v.reset();
            meta.read_object(cur, base, plans, v, &self.budget)?;
            let p = &mut self.pools;
            self.budget.take(match kind {
                PoolKind::Symbol | PoolKind::ThreadState => entry::<u64, Cow<'_, str>>(),
                PoolKind::Class => entry::<u64, u64>(),
                PoolKind::Method => entry::<u64, Method>(),
                PoolKind::StackTrace => entry::<u64, Vec<Frame>>(),
                _ => 0,
            })?;
            match kind {
                PoolKind::Symbol => {
                    p.symbols.insert(id, std::mem::take(&mut v.text[0]));
                }
                PoolKind::ThreadState => {
                    p.thread_states.insert(id, std::mem::take(&mut v.text[0]));
                }
                PoolKind::Class => {
                    p.classes.insert(id, v.num[0]);
                }
                PoolKind::Method => {
                    p.methods.insert(
                        id,
                        Method {
                            class: v.num[0],
                            name: v.num[1],
                        },
                    );
                }
                PoolKind::StackTrace => {
                    p.stack_traces.insert(id, std::mem::take(&mut v.frames));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::super::test_support::unzipped;
    use super::super::{LabelsSnapshot, parse_jfr};
    use super::merge_generated_class;

    fn profiles(body: &[u8]) -> Result<usize, String> {
        parse_jfr(body, &LabelsSnapshot::default(), 0, 0, 100).map(|o| o.profiles.len())
    }

    #[test]
    fn j4_a_broken_recording_is_refused_with_the_librarys_text() {
        let prefix = "jfr parser ParseEvent error: ";
        assert_eq!(
            profiles(b"JFR"),
            Err(format!(
                "{prefix}error reading chunk header: unexpected EOF"
            ))
        );
        let example = unzipped("example.jfr.gz");
        assert_eq!(
            profiles(&example[..1000]),
            Err(format!("{prefix}error reading metadata: unexpected EOF"))
        );
        let mut zeroed = example.clone();
        zeroed[0] = 0;
        assert_eq!(
            profiles(&zeroed),
            Err(format!(
                "{prefix}error reading chunk header: invalid chunk magic: 4c5200"
            ))
        );
        let e = profiles(&example[..120_000]).expect_err("cut inside the constant pool");
        assert!(
            e.starts_with(&format!("{prefix}error reading CP: ")) && e.ends_with(" @ 55825"),
            "{e}"
        );
        assert_eq!(profiles(b""), Ok(0));
    }

    #[test]
    fn j7_generated_class_names_merge_as_the_library_merges_them() {
        let cases = [
            (
                "org/example/rideshare/EnclosingClass$$Lambda$4/1283928880",
                "org/example/rideshare/EnclosingClass$$Lambda_",
            ),
            (
                "org/example/rideshare/EnclosingClass$$Lambda$8/0x0000000800c01220",
                "org/example/rideshare/EnclosingClass$$Lambda_",
            ),
            ("Fib$$Lambda.0x00007ffa600c4da0", "Fib$$Lambda_"),
            (
                "io/opentelemetry/context/Context$$Lambda$lambda$wrap$2$2630016632.0x00000000000100dd",
                "io/opentelemetry/context/Context$$Lambda$lambda$wrap$2_",
            ),
            (
                "org/example/rideshare/EnclosingClass$$Lambda$lambda$doWork$1$1283928880",
                "org/example/rideshare/EnclosingClass$$Lambda$lambda$doWork$1_",
            ),
            (
                "org/example/rideshare/EnclosingClass$$Lambda",
                "org/example/rideshare/EnclosingClass$$Lambda",
            ),
            (
                "org/example/rideshare/EnclosingClass$$LambdaHelper",
                "org/example/rideshare/EnclosingClass$$LambdaHelper",
            ),
            (
                "java/util/concurrent/Executors$RunnableAdapter",
                "java/util/concurrent/Executors$RunnableAdapter",
            ),
            (
                "jdk/internal/reflect/GeneratedMethodAccessor31",
                "jdk/internal/reflect/GeneratedMethodAccessor_",
            ),
            (
                "foo/bar/Baz$$EnhancerBySpringCGLIB$$1234567890",
                "foo/bar/Baz$$EnhancerBySpringCGLIB$$_",
            ),
        ];
        for (src, want) in cases {
            assert_eq!(merge_generated_class(Cow::Borrowed(src)), want, "{src}");
        }
    }
}
