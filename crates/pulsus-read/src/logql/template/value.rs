//! The template value model (issue #230) — the closure of Go types
//! reachable from LogQL `line_format`/`label_format` templates at
//! reference v3.7.4 (plan v2 §Δ1b, v6 §A): `string`, `int`-kinded
//! integers, `float64`, `bool`, nil, the `fromJson` containers, `[]byte`,
//! and the `time` closure (`Time`/`Duration`/`Month`/`Weekday`/
//! `*Location`).
//!
//! **Strings are byte strings** (`Cow<[u8]>`), NOT `String`: sprig's
//! `trunc`/`substr` slice bytes and can split a UTF-8 rune
//! (`sprig/strings.go:189-197`, `:228-236`); the invalid-UTF-8 residue is
//! carried through the render and substituted with U+FFFD only at the
//! pipeline boundary (owner-ratified byte-model + lossy-boundary design,
//! issue #230 adjudication 2).

use std::borrow::Cow;
use std::rc::Rc;

use super::timefns::GoTime;

/// The Go integer kind a [`Value::Int`] carries — observable through
/// `printf "%T"` and `%#v` only (arithmetic and printing are otherwise
/// kind-blind). `int` comes from number literals (including character
/// constants — `idealConstant` yields plain `int` for runes), `len`,
/// `count`, sprig `int`; `int64` from sprig `add`/`sub`/`mul`/`div`/
/// `mod`/`max`/`min` and `Time.Unix*`; `time.Duration`/`Month`/`Weekday`
/// are named integer kinds with their own [`Value`] variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntKind {
    Int,
    Int64,
}

impl IntKind {
    /// The reflect type name `%T` prints.
    pub fn type_name(self) -> &'static str {
        match self {
            IntKind::Int => "int",
            IntKind::Int64 => "int64",
        }
    }
}

/// The Go unsigned kind a [`Value::Uint`] carries: `uint8` from indexing
/// a `[]byte` (a `time.Time` marshal result), `uint64` from an integer
/// literal too large for `int64` (`parse.NumberNode.IsUint`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UintKind {
    Uint8,
    Uint64,
}

impl UintKind {
    pub fn type_name(self) -> &'static str {
        match self {
            UintKind::Uint8 => "uint8",
            UintKind::Uint64 => "uint64",
        }
    }
}

/// A `map[string]any` / `[]any` tree — what `fromJson` yields
/// (`encoding/json.Unmarshal` into `any`: every number is `float64`,
/// objects are string-keyed maps, arrays are `[]any`, `null` is nil).
/// `Rc` because values are cloned freely (variable assignment, argument
/// binding) and the containers are immutable once built.
#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    /// The nil/missing class: a nil `any` (a failed `fromJson`, a JSON
    /// `null`, a missing `map[string]any` key under `missingkey=zero`).
    /// Go's pipeline unwrap (`value.Elem()` on an empty interface,
    /// `exec.go` `evalPipeline`) turns a nil interface into the INVALID
    /// `reflect.Value`, so by the time any observable operation sees it
    /// the two are one state: the template write path prints
    /// `<no value>`, `printf %v` prints `<nil>` (plan v1 trap table).
    Nil,
    /// A Go `string` as raw bytes.
    Str(Cow<'a, [u8]>),
    Int(i64, IntKind),
    Uint(u64, UintKind),
    Float(f64),
    /// `complex128` — reachable only through `i`-suffixed number
    /// literals (Go template constants support them; the plan's value
    /// domain missed this literal-only entry, flagged in the notes).
    Complex(f64, f64),
    Bool(bool),
    /// A `[]byte` (only reachable from `time.Time` marshal/append
    /// methods). Prints as a bracketed decimal list under `%v`/`%d` and
    /// as a byte string under `%s`/`%q`/`%x`/`%X` (plan v3 Δ1).
    Bytes(Cow<'a, [u8]>),
    /// `[]any` from `fromJson` / `slice`.
    List(Rc<Vec<Value<'a>>>),
    /// `map[string]any` from `fromJson`. Kept SORTED by key (byte order
    /// == Go string order) — Go's template `range` and `fmt` both sort
    /// string map keys, so sorted storage loses nothing and makes both
    /// paths trivial.
    Map(Rc<Vec<(Vec<u8>, Value<'a>)>>),
    /// The per-line label data map (`map[string]string`, the template
    /// dot) — a MARKER resolved against the render context, so the map
    /// is never materialised unless the template actually consumes it as
    /// a value (`range .`, `index .`, printing `.`, `$x := .`). Includes
    /// the out-of-band `__error__`/`__error_details__` pair when the
    /// caller's #238 visibility gate is open.
    LabelMap,
    Time(GoTime),
    /// `time.Duration` in nanoseconds (only from `Time.Sub`).
    Duration(i64),
    /// `time.Month` (1-12 from `Time.Month`, but any integer is
    /// representable — Go lets arithmetic produce out-of-range months).
    Month(i64),
    /// `time.Weekday` (0-6 from `Time.Weekday`).
    Weekday(i64),
    /// `*time.Location` (from `Time.Location`).
    Location(GoLoc),
}

/// The `*time.Location` model. `Utc` is Go's `time.UTC`; `Local` is
/// `time.Local` (whose IANA rules the [`super::TemplateEnv`] carries —
/// on the stock reference container `initLocal` degenerates it to the
/// all-nil "UTC" form, the PROVENANCE capture precondition); `Named` is
/// a `toDateInZone`/`In`-loaded IANA zone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoLoc {
    Utc,
    Local,
    Named(chrono_tz::Tz),
    /// Go `time.FixedZone` — only reachable from layout PARSING of a
    /// numeric offset / unknown abbreviation (`time.Parse`'s fabricated
    /// location).
    Fixed {
        name: String,
        offset: i32,
    },
}

impl<'a> Value<'a> {
    /// Go `text/template`'s `isTrue` (`exec.go`): the truth of a value
    /// for `if`/`with`/`and`/`or`/`not`. Returns `(truth, ok)`. The
    /// INVALID value is `(false, true)` ("a form of nil", `exec.go
    /// isTrue`); `ok=false` needs a chan/func kind, unreachable in this
    /// model — `if/with can't use` is therefore never emitted here.
    pub fn is_true(&self) -> (bool, bool) {
        match self {
            Value::Nil => (false, true),
            Value::Str(s) => (!s.is_empty(), true),
            Value::Int(n, _) => (*n != 0, true),
            Value::Uint(n, _) => (*n != 0, true),
            Value::Float(f) => (*f != 0.0, true),
            Value::Complex(re, im) => (*re != 0.0 || *im != 0.0, true),
            Value::Bool(b) => (*b, true),
            Value::Bytes(b) => (!b.is_empty(), true),
            Value::List(l) => (!l.is_empty(), true),
            Value::Map(m) => (!m.is_empty(), true),
            // Resolved by the evaluator (needs the label context); this
            // arm is unreachable through `eval` but kept total.
            Value::LabelMap => (true, true),
            // Structs are always true; named integer kinds by value.
            Value::Time(_) => (true, true),
            Value::Duration(n) | Value::Month(n) | Value::Weekday(n) => (*n != 0, true),
            Value::Location(_) => (true, true),
        }
    }

    /// The reflect type name `printf "%T"` renders.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Nil => "<nil>",
            Value::Str(_) => "string",
            Value::Int(_, k) => k.type_name(),
            Value::Uint(_, k) => k.type_name(),
            Value::Float(_) => "float64",
            Value::Complex(..) => "complex128",
            Value::Bool(_) => "bool",
            Value::Bytes(_) => "[]uint8",
            Value::List(_) => "[]interface {}",
            Value::Map(_) => "map[string]interface {}",
            Value::LabelMap => "map[string]string",
            Value::Time(_) => "time.Time",
            Value::Duration(_) => "time.Duration",
            Value::Month(_) => "time.Month",
            Value::Weekday(_) => "time.Weekday",
            Value::Location(_) => "*time.Location",
        }
    }

    /// Converts a borrowed value into one that owns all its data (used
    /// when a value must outlive the borrow it was built from — never on
    /// the per-row hot path).
    pub fn into_owned(self) -> Value<'static> {
        match self {
            Value::Nil => Value::Nil,
            Value::Str(s) => Value::Str(Cow::Owned(s.into_owned())),
            Value::Int(n, k) => Value::Int(n, k),
            Value::Uint(n, k) => Value::Uint(n, k),
            Value::Float(f) => Value::Float(f),
            Value::Complex(re, im) => Value::Complex(re, im),
            Value::Bool(b) => Value::Bool(b),
            Value::Bytes(b) => Value::Bytes(Cow::Owned(b.into_owned())),
            Value::List(l) => Value::List(Rc::new(
                Rc::try_unwrap(l)
                    .unwrap_or_else(|rc| (*rc).clone())
                    .into_iter()
                    .map(Value::into_owned)
                    .collect(),
            )),
            Value::Map(m) => Value::Map(Rc::new(
                Rc::try_unwrap(m)
                    .unwrap_or_else(|rc| (*rc).clone())
                    .into_iter()
                    .map(|(k, v)| (k, v.into_owned()))
                    .collect(),
            )),
            Value::LabelMap => Value::LabelMap,
            Value::Time(t) => Value::Time(t),
            Value::Duration(n) => Value::Duration(n),
            Value::Month(n) => Value::Month(n),
            Value::Weekday(n) => Value::Weekday(n),
            Value::Location(l) => Value::Location(l),
        }
    }

    /// Convenience: an owned string value.
    pub fn str_owned(s: impl Into<Vec<u8>>) -> Value<'a> {
        Value::Str(Cow::Owned(s.into()))
    }

    /// Convenience: `int` (the Go builtin kind).
    pub fn int(n: i64) -> Value<'a> {
        Value::Int(n, IntKind::Int)
    }

    /// Convenience: `int64` (the sprig arithmetic kind).
    pub fn int64(n: i64) -> Value<'a> {
        Value::Int(n, IntKind::Int64)
    }
}
