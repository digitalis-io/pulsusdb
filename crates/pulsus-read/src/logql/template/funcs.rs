//! The template function registry (issue #230): exactly the 67
//! non-builtin names the reference registers — the 23-entry
//! `functionMap` literal + the 2 per-line injected functions
//! (`pkg/logql/log/fmt.go:29-73`, `:122-134`) + the 42-name sprig subset
//! (`fmt.go:75-119`) — plus the non-special `text/template` builtins
//! (`and`/`or` short-circuit in `eval`; `print`/`printf`/`println` need
//! the full print environment and live here via `FuncCtx`).
//!
//! Function-call failures return `Err(msg)`; the evaluator wraps them as
//! `error calling <name>: <msg>` exactly like Go's `evalCall`+`safeCall`
//! (panics recovered into the same shape).

use std::borrow::Cow;
use std::collections::HashMap;
use std::rc::Rc;

use super::decimal::Dec;
use super::gofmt::{self, PrintEnv};
use super::timefns::{self, GoTime, TemplateEnv};
use super::value::{GoLoc, IntKind, UintKind, Value};

/// Parameter types for argument coercion (Go `evalArg`/`validateType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamTy {
    /// `interface{}`.
    Any,
    /// `string`.
    Str,
    /// `int` (any Go integer kind converts — `intLike`).
    Int,
    /// `float64`.
    Float,
    /// `time.Time`.
    Time,
    /// `time.Duration` (int-like conversion, Duration-typed result).
    Dur,
    /// `*time.Location` (nil-able).
    Loc,
    /// `[]byte` (only `Bytes` values assign).
    BytesTy,
}

impl ParamTy {
    /// The Go type-name text `validateType` errors embed.
    pub fn go_name(self) -> &'static str {
        match self {
            ParamTy::Any => "interface {}",
            ParamTy::Str => "string",
            ParamTy::Int => "int",
            ParamTy::Float => "float64",
            ParamTy::Time => "time.Time",
            ParamTy::Dur => "time.Duration",
            ParamTy::Loc => "*time.Location",
            ParamTy::BytesTy => "[]uint8",
        }
    }
}

/// What a function implementation can see (plan v1 §5: the per-line
/// bindings behind `__line__`/`__timestamp__`, the print environment,
/// and the compile-time regex cache).
pub struct FuncCtx<'c, 'a> {
    pub print_env: &'c dyn PrintEnv,
    pub line: &'c [u8],
    pub ts_ns: i64,
    /// Patterns pre-compiled at template-compile time (literal args
    /// only — plan v1 §6); anything else compiles per call, like the
    /// reference.
    pub regex_cache: &'c HashMap<String, regex::Regex>,
    /// The per-render output budget (issue #230 follow-up): every
    /// function whose output size a template argument multiplies
    /// charges here BEFORE allocating.
    pub budget: &'c super::RenderBudget,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

impl<'c, 'a> FuncCtx<'c, 'a> {
    pub fn env(&self) -> &TemplateEnv {
        self.print_env.env()
    }

    /// Charge-before-allocate for template-controlled output sizes.
    fn charge(&self, bytes: usize) -> Result<(), String> {
        self.budget.charge(bytes)
    }
}

pub type FuncImpl = for<'c, 'a> fn(&FuncCtx<'c, 'a>, &[Value<'a>]) -> Result<Value<'a>, String>;

#[derive(Clone, Copy)]
pub struct FuncDef {
    pub name: &'static str,
    pub params: &'static [ParamTy],
    pub variadic: Option<ParamTy>,
    pub call: FuncImpl,
}

/// Every function name callable from a LogQL template (used by the
/// parser's `hasFunction` check): the 67 non-builtin names + the 19
/// builtins = 86 (plan v2 Δ2).
pub fn all_callable_names() -> &'static [&'static str] {
    &ALL_NAMES
}

/// The non-builtin registry names (67).
pub fn registry_names() -> Vec<&'static str> {
    REGISTRY.iter().map(|f| f.name).collect()
}

pub fn lookup(name: &str) -> Option<&'static FuncDef> {
    REGISTRY.iter().find(|f| f.name == name)
}

static ALL_NAMES: [&str; 86] = [
    // text/template builtins (19).
    "and",
    "call",
    "html",
    "index",
    "slice",
    "js",
    "len",
    "not",
    "or",
    "print",
    "printf",
    "println",
    "urlquery",
    "eq",
    "ge",
    "gt",
    "le",
    "lt",
    "ne",
    // functionMap literal (23).
    "ToLower",
    "ToUpper",
    "Replace",
    "Trim",
    "TrimLeft",
    "TrimRight",
    "TrimPrefix",
    "TrimSuffix",
    "TrimSpace",
    "regexReplaceAll",
    "regexReplaceAllLiteral",
    "count",
    "urldecode",
    "urlencode",
    "bytes",
    "duration",
    "duration_seconds",
    "unixEpochMillis",
    "unixEpochNanos",
    "toDateInZone",
    "unixToTime",
    "alignLeft",
    "alignRight",
    // injected per line (2).
    "__line__",
    "__timestamp__",
    // sprig subset (42).
    "b64enc",
    "b64dec",
    "lower",
    "upper",
    "title",
    "trunc",
    "substr",
    "contains",
    "hasPrefix",
    "hasSuffix",
    "indent",
    "nindent",
    "replace",
    "repeat",
    "trim",
    "trimAll",
    "trimSuffix",
    "trimPrefix",
    "int",
    "float64",
    "add",
    "sub",
    "mul",
    "div",
    "mod",
    "addf",
    "subf",
    "mulf",
    "divf",
    "max",
    "min",
    "maxf",
    "minf",
    "ceil",
    "floor",
    "round",
    "fromJson",
    "date",
    "toDate",
    "now",
    "unixEpoch",
    "default",
];

// ---------------------------------------------------------------------
// Coercion helpers (spf13/cast v1.10.0 ports)
// ---------------------------------------------------------------------

fn bytes_of<'v>(v: &'v Value<'_>) -> &'v [u8] {
    match v {
        Value::Str(s) => s,
        _ => b"",
    }
}

fn lossy(v: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(v)
}

/// [`lossy`]'s charged twin (review rounds 4+6): invalid UTF-8 forces
/// the owned branch, whose EXACT repaired length is precomputed,
/// charged, and then allocated ONCE — never `from_utf8_lossy`'s
/// grow-doubling buffer, so there is no growth worst case to bound.
/// Valid input borrows and charges nothing, so valid-UTF-8 budget
/// arithmetic (and every pinned boundary) is unchanged.
fn lossy_charged<'v>(ctx: &FuncCtx<'_, '_>, v: &'v [u8]) -> Result<Cow<'v, str>, String> {
    match std::str::from_utf8(v) {
        Ok(s) => Ok(Cow::Borrowed(s)),
        Err(_) => {
            // Round 6: the repaired length is PRECOMPUTED and the buffer
            // allocated ONCE at exactly that size, after charging it — a
            // `from_utf8_lossy` call may start at `len` capacity and
            // grow-double to 4× (cumulatively requesting up to 7×), so
            // no constant ceiling is a provable bound; a single
            // allocation of a computed size has no worst case.
            let need = lossy_repaired_len(v);
            ctx.charge(need)?;
            Ok(Cow::Owned(lossy_repaired(v, need)))
        }
    }
}

/// The exact byte length of `String::from_utf8_lossy(v)` — same
/// substitution granularity as std (one U+FFFD per maximal invalid
/// sequence, per `Utf8Error::error_len`), computed without allocating.
fn lossy_repaired_len(v: &[u8]) -> usize {
    let mut rest = v;
    let mut len = 0usize;
    loop {
        match std::str::from_utf8(rest) {
            Ok(s) => return len + s.len(),
            Err(e) => {
                len += e.valid_up_to() + '\u{FFFD}'.len_utf8();
                match e.error_len() {
                    Some(bad) => rest = &rest[e.valid_up_to() + bad..],
                    // Truncated trailing sequence: one replacement, done.
                    None => return len,
                }
            }
        }
    }
}

/// Builds the lossy repair in ONE allocation of exactly `cap` bytes
/// (byte-identical to `String::from_utf8_lossy(v)` — pinned by a unit
/// test below, including the truncated-tail and interleaved cases).
fn lossy_repaired(v: &[u8], cap: usize) -> String {
    let mut out = String::with_capacity(cap);
    let mut rest = v;
    loop {
        match std::str::from_utf8(rest) {
            Ok(s) => {
                out.push_str(s);
                break;
            }
            Err(e) => {
                let (valid, tail) = rest.split_at(e.valid_up_to());
                out.push_str(std::str::from_utf8(valid).expect("validated by valid_up_to"));
                out.push('\u{FFFD}');
                match e.error_len() {
                    Some(bad) => rest = &tail[bad..],
                    None => break,
                }
            }
        }
    }
    // Length only — an equality debug_assert against
    // `String::from_utf8_lossy` would re-run the grow-doubling path in
    // debug builds and re-appear in the allocation gate; byte equality
    // is pinned by the unit test below instead.
    debug_assert_eq!(out.len(), cap);
    out
}

/// Go float→int64 conversion semantics (amd64 `cvttsd2si`): truncate
/// toward zero; NaN/±Inf/out-of-range yield the INDEFINITE value
/// `i64::MIN`.
fn go_f64_to_i64(f: f64) -> i64 {
    if f.is_nan() || f <= (i64::MIN as f64) - 1.0 || f >= (i64::MAX as f64) + 1.0 {
        return i64::MIN;
    }
    let t = f.trunc();
    if t < i64::MIN as f64 || t > i64::MAX as f64 {
        i64::MIN
    } else {
        t as i64
    }
}

/// `cast.trimDecimal`.
fn trim_decimal(s: &str) -> Cow<'_, str> {
    if !s.contains('.') {
        return Cow::Borrowed(s);
    }
    // ^([-+]?\d*)(\.\d*)?$
    let bytes = s.as_bytes();
    let mut i = 0;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        i += 1;
    }
    let int_end = {
        let mut j = i;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
        j
    };
    let rest = &bytes[int_end..];
    let frac_ok = rest.first() == Some(&b'.') && rest[1..].iter().all(|b| b.is_ascii_digit());
    if !frac_ok {
        return Cow::Borrowed(s);
    }
    let int_part = &s[..int_end];
    match int_part {
        "-" | "+" => Cow::Owned(format!("{int_part}0")),
        "" => Cow::Borrowed("0"),
        _ => Cow::Borrowed(int_part),
    }
}

/// `strconv.ParseInt(s, 0, 0)` (base-0 prefixes, underscores).
fn go_parse_int_base0(s: &str) -> Option<i64> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    if body.is_empty() {
        return None;
    }
    // Underscore validation is looser than Go's exact rule; the
    // reachable corpus shapes have none. Only allocate the stripped
    // copy when underscores are present (round-4 sweep: the
    // unconditional copy was a ~2× transient on every big cast).
    let cleaned: Cow<'_, str> = if body.contains('_') {
        Cow::Owned(body.chars().filter(|&c| c != '_').collect())
    } else {
        Cow::Borrowed(body)
    };
    if cleaned.is_empty() || (body.starts_with('_') || body.ends_with('_') || body.contains("__")) {
        return None;
    }
    let (base, digits): (u32, &str) = if let Some(r) = cleaned
        .strip_prefix("0x")
        .or_else(|| cleaned.strip_prefix("0X"))
    {
        (16, r)
    } else if let Some(r) = cleaned
        .strip_prefix("0o")
        .or_else(|| cleaned.strip_prefix("0O"))
    {
        (8, r)
    } else if let Some(r) = cleaned
        .strip_prefix("0b")
        .or_else(|| cleaned.strip_prefix("0B"))
    {
        (2, r)
    } else if cleaned.len() > 1 && cleaned.starts_with('0') {
        (8, &cleaned[1..])
    } else {
        (10, &cleaned)
    };
    if digits.is_empty() {
        return None;
    }
    let mag = u64::from_str_radix(digits, base).ok()?;
    if neg {
        if mag > (i64::MAX as u64) + 1 {
            return None;
        }
        Some((mag as i64).wrapping_neg())
    } else {
        i64::try_from(mag).ok()
    }
}

/// `strconv.ParseFloat` (underscore-tolerant like Go's base-10 rule).
fn go_parse_float(s: &str) -> Option<f64> {
    let cleaned: Cow<'_, str> = if s.contains('_') {
        if s.starts_with('_') || s.ends_with('_') || s.contains("__") {
            return None;
        }
        Cow::Owned(s.chars().filter(|&c| c != '_').collect())
    } else {
        Cow::Borrowed(s)
    };
    let t = cleaned.trim();
    if t != cleaned.as_ref() {
        // Go does not trim whitespace.
        return None;
    }
    cleaned.parse::<f64>().ok()
}

/// `cast.ToInt64` (errors swallowed to 0, sprig `toInt64`).
pub fn cast_to_i64(v: &Value<'_>) -> i64 {
    match v {
        Value::Int(n, _) => *n,
        Value::Uint(n, _) => *n as i64,
        Value::Float(f) => go_f64_to_i64(*f),
        Value::Bool(b) => {
            if *b {
                1
            } else {
                0
            }
        }
        Value::Nil => 0,
        Value::Month(n) | Value::Weekday(n) | Value::Duration(n) => *n,
        Value::Str(s) => {
            let text = lossy(s);
            if text.is_empty() {
                return 0;
            }
            go_parse_int_base0(&trim_decimal(&text)).unwrap_or(0)
        }
        _ => 0,
    }
}

/// `cast.ToFloat64`.
pub fn cast_to_f64(v: &Value<'_>) -> f64 {
    match v {
        Value::Int(n, _) => *n as f64,
        Value::Uint(n, _) => *n as f64,
        Value::Float(f) => *f,
        Value::Bool(b) => {
            if *b {
                1.0
            } else {
                0.0
            }
        }
        Value::Nil => 0.0,
        Value::Month(n) | Value::Weekday(n) | Value::Duration(n) => *n as f64,
        Value::Str(s) => {
            let text = lossy(s);
            if text.is_empty() {
                return 0.0;
            }
            go_parse_float(&text).unwrap_or(0.0)
        }
        _ => 0.0,
    }
}

/// `cast.ToInt` — same as ToInt64 on 64-bit.
fn cast_to_int(v: &Value<'_>) -> i64 {
    cast_to_i64(v)
}

fn str_val<'a>(bytes: Vec<u8>) -> Value<'a> {
    Value::Str(Cow::Owned(bytes))
}

fn string_val<'a>(s: String) -> Value<'a> {
    Value::Str(Cow::Owned(s.into_bytes()))
}

fn time_arg<'v, 'a>(args: &'v [Value<'a>], i: usize) -> &'v GoTime {
    match &args[i] {
        Value::Time(t) => t,
        // Unreachable: the eval layer coerced ParamTy::Time already.
        _ => unreachable!("ParamTy::Time coercion guarantees a Time value"),
    }
}

// ---------------------------------------------------------------------
// The registry
// ---------------------------------------------------------------------

macro_rules! f {
    ($name:literal, [$($p:expr),*], $variadic:expr, $call:expr) => {
        FuncDef {
            name: $name,
            params: &[$($p),*],
            variadic: $variadic,
            call: $call,
        }
    };
}

use ParamTy::{Any, Float, Int, Str, Time};

pub static REGISTRY: [FuncDef; 67] = [
    // -- functionMap literal (23) --------------------------------------
    f!("ToLower", [Str], None, |c, a| {
        c.charge(4 * bytes_of(&a[0]).len() + 4)?;
        Ok(str_val(go_to_lower(bytes_of(&a[0]))))
    }),
    f!("ToUpper", [Str], None, |c, a| {
        c.charge(4 * bytes_of(&a[0]).len() + 4)?;
        Ok(str_val(go_to_upper(bytes_of(&a[0]))))
    }),
    f!("Replace", [Str, Str, Str, Int], None, |c, a| {
        let n = int_of(&a[3]);
        Ok(str_val(go_replace(
            c,
            bytes_of(&a[0]),
            bytes_of(&a[1]),
            bytes_of(&a[2]),
            n,
        )?))
    }),
    f!("Trim", [Str, Str], None, |c, a| Ok(str_val(go_trim(
        c,
        bytes_of(&a[0]),
        bytes_of(&a[1]),
        true,
        true
    )?))),
    f!("TrimLeft", [Str, Str], None, |c, a| Ok(str_val(go_trim(
        c,
        bytes_of(&a[0]),
        bytes_of(&a[1]),
        true,
        false
    )?))),
    f!("TrimRight", [Str, Str], None, |c, a| Ok(str_val(go_trim(
        c,
        bytes_of(&a[0]),
        bytes_of(&a[1]),
        false,
        true
    )?))),
    f!("TrimPrefix", [Str, Str], None, |c, a| {
        let (s, p) = (bytes_of(&a[0]), bytes_of(&a[1]));
        let out = s.strip_prefix(p).unwrap_or(s);
        c.charge(out.len())?;
        Ok(str_val(out.to_vec()))
    }),
    f!("TrimSuffix", [Str, Str], None, |c, a| {
        let (s, p) = (bytes_of(&a[0]), bytes_of(&a[1]));
        let out = s.strip_suffix(p).unwrap_or(s);
        c.charge(out.len())?;
        Ok(str_val(out.to_vec()))
    }),
    f!("TrimSpace", [Str], None, |c, a| Ok(str_val(go_trim_space(
        c,
        bytes_of(&a[0])
    )?))),
    f!("regexReplaceAll", [Str, Str, Str], None, |c, a| {
        let re = compile_regex(c, bytes_of(&a[0]))?;
        // Borrowed Cow args (round 3); an INVALID-UTF-8 input's ≤3×
        // owned substitution buffer is charged before converting
        // (round 4 — the shape the ordering leg had not covered).
        let s = lossy_charged(c, bytes_of(&a[1]))?;
        let repl = lossy_charged(c, bytes_of(&a[2]))?;
        // ≤ (matches+1)×len(repl) + len(s); matches ≤ len(s)+1.
        c.charge(
            (s.len() + 2)
                .saturating_mul(repl.len().max(1))
                .saturating_add(s.len()),
        )?;
        Ok(string_val(
            re.replace_all(s.as_ref(), repl.as_ref()).into_owned(),
        ))
    }),
    f!("regexReplaceAllLiteral", [Str, Str, Str], None, |c, a| {
        let re = compile_regex(c, bytes_of(&a[0]))?;
        let s = lossy_charged(c, bytes_of(&a[1]))?;
        let repl = lossy_charged(c, bytes_of(&a[2]))?;
        c.charge(
            (s.len() + 2)
                .saturating_mul(repl.len().max(1))
                .saturating_add(s.len()),
        )?;
        Ok(string_val(
            re.replace_all(s.as_ref(), regex::NoExpand(repl.as_ref()))
                .into_owned(),
        ))
    }),
    f!("count", [Str, Str], None, |c, a| {
        let re = compile_regex(c, bytes_of(&a[0]))?;
        let s = lossy_charged(c, bytes_of(&a[1]))?;
        Ok(Value::int(re.find_iter(s.as_ref()).count() as i64))
    }),
    f!("urldecode", [Str], None, |c, a| {
        let s = bytes_of(&a[0]);
        query_unescape(c, s).map(str_val)
    }),
    f!("urlencode", [Str], None, |c, a| {
        c.charge(3 * bytes_of(&a[0]).len() + 4)?;
        Ok(str_val(query_escape(bytes_of(&a[0]))))
    }),
    f!("bytes", [Str], None, |_c, a| {
        let raw = lossy(bytes_of(&a[0])).into_owned();
        crate::logql::pipeline::convert_bytes_value(&raw)
            .map(Value::Float)
            .map_err(|e| e.to_string())
    }),
    f!("duration", [Str], None, |_c, a| {
        let raw = lossy(bytes_of(&a[0])).into_owned();
        go_parse_duration(&raw).map(|d| Value::Float(duration_seconds_f64(d)))
    }),
    f!("duration_seconds", [Str], None, |_c, a| {
        let raw = lossy(bytes_of(&a[0])).into_owned();
        go_parse_duration(&raw).map(|d| Value::Float(duration_seconds_f64(d)))
    }),
    f!("unixEpochMillis", [Time], None, |_c, a| Ok(string_val(
        time_arg(a, 0).unix_milli().to_string()
    ))),
    f!("unixEpochNanos", [Time], None, |_c, a| Ok(string_val(
        time_arg(a, 0).unix_nano().to_string()
    ))),
    f!("toDateInZone", [Str, Str, Str], None, |c, a| {
        let layout = bytes_of(&a[0]).to_vec();
        let zone = lossy(bytes_of(&a[1])).into_owned();
        let val = bytes_of(&a[2]).to_vec();
        let loc = timefns::load_location(&zone, c.env());
        let t = super::golayout::parse_in_location(&layout, &val, &loc, c.env())
            .unwrap_or_else(|_| GoTime::zero());
        Ok(Value::Time(t))
    }),
    f!("unixToTime", [Str], None, |_c, a| {
        let epoch = lossy(bytes_of(&a[0])).into_owned();
        unix_to_time(&epoch).map(Value::Time)
    }),
    f!("alignLeft", [Int, Str], None, |c, a| Ok(str_val(align(
        c,
        int_of(&a[0]),
        bytes_of(&a[1]),
        true,
    )?))),
    f!("alignRight", [Int, Str], None, |c, a| Ok(str_val(align(
        c,
        int_of(&a[0]),
        bytes_of(&a[1]),
        false,
    )?))),
    // -- injected per line (2) ------------------------------------------
    f!("__line__", [], None, |c, _a| {
        // One copy of the stored line — but REPEATABLE inside a
        // `range`/variable-only body (review round 2, the fifth
        // amplification class): charged like every retainable output.
        c.charge(c.line.len())?;
        Ok(str_val(c.line.to_vec()))
    }),
    f!("__timestamp__", [], None, |c, _a| Ok(Value::Time(
        GoTime::from_unix_ns(c.ts_ns)
    ))),
    // -- sprig subset (42) ----------------------------------------------
    f!("b64enc", [Str], None, |c, a| {
        use base64::Engine as _;
        c.charge(bytes_of(&a[0]).len() / 3 * 4 + 8)?;
        Ok(string_val(
            base64::engine::general_purpose::STANDARD.encode(bytes_of(&a[0])),
        ))
    }),
    f!("b64dec", [Str], None, |c, a| {
        // Output ≤ 3/4 input, charged before the decode buffer.
        c.charge(bytes_of(&a[0]).len() / 4 * 3 + 8)?;
        Ok(match b64_decode_go(bytes_of(&a[0])) {
            Ok(data) => str_val(data),
            // sprig returns err.Error() as the VALUE.
            Err(msg) => string_val(msg),
        })
    }),
    f!("lower", [Str], None, |c, a| {
        // Case mapping can EXPAND (≤4 bytes out per input byte).
        c.charge(4 * bytes_of(&a[0]).len() + 4)?;
        Ok(str_val(go_to_lower(bytes_of(&a[0]))))
    }),
    f!("upper", [Str], None, |c, a| {
        c.charge(4 * bytes_of(&a[0]).len() + 4)?;
        Ok(str_val(go_to_upper(bytes_of(&a[0]))))
    }),
    f!("title", [Str], None, |c, a| {
        c.charge(4 * bytes_of(&a[0]).len() + 4)?;
        Ok(str_val(go_title(bytes_of(&a[0]))))
    }),
    f!("trunc", [Int, Str], None, |c, a| {
        // sprig trunc: BYTE slicing (strings.go:189-197).
        let n = int_of(&a[0]);
        let s = bytes_of(&a[1]);
        let len = s.len() as i64;
        let out = if n < 0 && len + n > 0 {
            &s[(len + n) as usize..]
        } else if n >= 0 && len > n {
            &s[..n as usize]
        } else {
            s
        };
        c.charge(out.len())?;
        Ok(str_val(out.to_vec()))
    }),
    f!("substr", [Int, Int, Str], None, |c, a| {
        // sprig substring: BYTE slicing with Go's exact panic surfaces
        // (`sprig/strings.go:228-236` + the runtime boundsError texts).
        let start = int_of(&a[0]);
        let end = int_of(&a[1]);
        let s = bytes_of(&a[2]);
        let len = s.len() as i64;
        if start < 0 {
            // s[:end]
            if end < 0 {
                return Err(format!("runtime error: slice bounds out of range [:{end}]"));
            }
            if end > len {
                return Err(format!(
                    "runtime error: slice bounds out of range [:{end}] with length {len}"
                ));
            }
            c.charge(end as usize)?;
            return Ok(str_val(s[..end as usize].to_vec()));
        }
        if end < 0 || end > len {
            // s[start:] — a too-high low bound reports against len.
            if start > len {
                return Err(format!(
                    "runtime error: slice bounds out of range [{start}:{len}]"
                ));
            }
            c.charge(s.len() - start as usize)?;
            return Ok(str_val(s[start as usize..].to_vec()));
        }
        if start > end {
            return Err(format!(
                "runtime error: slice bounds out of range [{start}:{end}]"
            ));
        }
        c.charge((end - start) as usize)?;
        Ok(str_val(s[start as usize..end as usize].to_vec()))
    }),
    f!("contains", [Str, Str], None, |_c, a| {
        let (needle, hay) = (bytes_of(&a[0]), bytes_of(&a[1]));
        Ok(Value::Bool(find_sub(hay, needle).is_some()))
    }),
    f!("hasPrefix", [Str, Str], None, |_c, a| Ok(Value::Bool(
        bytes_of(&a[1]).starts_with(bytes_of(&a[0]))
    ))),
    f!("hasSuffix", [Str, Str], None, |_c, a| Ok(Value::Bool(
        bytes_of(&a[1]).ends_with(bytes_of(&a[0]))
    ))),
    f!("indent", [Int, Str], None, |c, a| Ok(str_val(indent_impl(
        c,
        int_of(&a[0]),
        bytes_of(&a[1]),
        false,
    )?))),
    f!("nindent", [Int, Str], None, |c, a| Ok(str_val(
        indent_impl(c, int_of(&a[0]), bytes_of(&a[1]), true,)?
    ))),
    f!("replace", [Str, Str, Str], None, |c, a| Ok(str_val(
        go_replace(c, bytes_of(&a[2]), bytes_of(&a[0]), bytes_of(&a[1]), -1,)?
    ))),
    f!("repeat", [Int, Str], None, |c, a| {
        let count = int_of(&a[0]);
        let s = bytes_of(&a[1]);
        if count < 0 {
            return Err("strings: negative Repeat count".to_string());
        }
        let total = (s.len() as u128) * (count as u128);
        if total > i64::MAX as u128 {
            return Err("strings: Repeat output length overflow".to_string());
        }
        // count × len is computable up front — charge it before the
        // allocation (issue #230 follow-up; the reference is unbounded
        // here and OOMs — ledgered `template-output-budget`).
        c.charge(total.min(u64::MAX as u128) as usize)?;
        Ok(str_val(s.repeat(count as usize)))
    }),
    f!("trim", [Str], None, |c, a| Ok(str_val(go_trim_space(
        c,
        bytes_of(&a[0])
    )?))),
    f!("trimAll", [Str, Str], None, |c, a| Ok(str_val(go_trim(
        c,
        bytes_of(&a[1]),
        bytes_of(&a[0]),
        true,
        true,
    )?))),
    f!("trimSuffix", [Str, Str], None, |c, a| {
        let (p, s) = (bytes_of(&a[0]), bytes_of(&a[1]));
        let out = s.strip_suffix(p).unwrap_or(s);
        c.charge(out.len())?;
        Ok(str_val(out.to_vec()))
    }),
    f!("trimPrefix", [Str, Str], None, |c, a| {
        let (p, s) = (bytes_of(&a[0]), bytes_of(&a[1]));
        let out = s.strip_prefix(p).unwrap_or(s);
        c.charge(out.len())?;
        Ok(str_val(out.to_vec()))
    }),
    f!("int", [Any], None, |_c, a| Ok(Value::Int(
        cast_to_int(&a[0]),
        IntKind::Int
    ))),
    f!("float64", [Any], None, |_c, a| Ok(Value::Float(
        cast_to_f64(&a[0])
    ))),
    f!("add", [], Some(Any), |_c, a| {
        let mut acc: i64 = 0;
        for v in a {
            acc = acc.wrapping_add(cast_to_i64(v));
        }
        Ok(Value::int64(acc))
    }),
    f!("sub", [Any, Any], None, |_c, a| Ok(Value::int64(
        cast_to_i64(&a[0]).wrapping_sub(cast_to_i64(&a[1]))
    ))),
    f!("mul", [Any], Some(Any), |_c, a| {
        let mut acc = cast_to_i64(&a[0]);
        for v in &a[1..] {
            acc = acc.wrapping_mul(cast_to_i64(v));
        }
        Ok(Value::int64(acc))
    }),
    f!("div", [Any, Any], None, |_c, a| {
        let (x, y) = (cast_to_i64(&a[0]), cast_to_i64(&a[1]));
        if y == 0 {
            return Err("runtime error: integer divide by zero".to_string());
        }
        Ok(Value::int64(x.wrapping_div(y)))
    }),
    f!("mod", [Any, Any], None, |_c, a| {
        let (x, y) = (cast_to_i64(&a[0]), cast_to_i64(&a[1]));
        if y == 0 {
            return Err("runtime error: integer divide by zero".to_string());
        }
        Ok(Value::int64(x.wrapping_rem(y)))
    }),
    f!("addf", [], Some(Any), |_c, a| {
        // execDecimalOp with a zero float64 accumulator.
        let mut acc = Dec::zero();
        for v in a {
            let d = Dec::from_float(cast_to_f64(v))?;
            acc = acc.add(&d);
        }
        Ok(Value::Float(acc.to_f64()))
    }),
    f!("subf", [Any], Some(Any), |_c, a| {
        let mut acc = Dec::from_float(cast_to_f64(&a[0]))?;
        for v in &a[1..] {
            let d = Dec::from_float(cast_to_f64(v))?;
            acc = acc.sub(&d);
        }
        Ok(Value::Float(acc.to_f64()))
    }),
    f!("mulf", [Any], Some(Any), |_c, a| {
        let mut acc = Dec::from_float(cast_to_f64(&a[0]))?;
        for v in &a[1..] {
            let d = Dec::from_float(cast_to_f64(v))?;
            acc = acc.mul(&d);
        }
        Ok(Value::Float(acc.to_f64()))
    }),
    f!("divf", [Any], Some(Any), |_c, a| {
        let mut acc = Dec::from_float(cast_to_f64(&a[0]))?;
        for v in &a[1..] {
            let d = Dec::from_float(cast_to_f64(v))?;
            acc = acc.div(&d)?;
        }
        Ok(Value::Float(acc.to_f64()))
    }),
    f!("max", [Any], Some(Any), |_c, a| {
        let mut acc = cast_to_i64(&a[0]);
        for v in &a[1..] {
            acc = acc.max(cast_to_i64(v));
        }
        Ok(Value::int64(acc))
    }),
    f!("min", [Any], Some(Any), |_c, a| {
        let mut acc = cast_to_i64(&a[0]);
        for v in &a[1..] {
            acc = acc.min(cast_to_i64(v));
        }
        Ok(Value::int64(acc))
    }),
    f!("maxf", [Any], Some(Any), |_c, a| {
        let mut acc = cast_to_f64(&a[0]);
        for v in &a[1..] {
            acc = go_math_max(acc, cast_to_f64(v));
        }
        Ok(Value::Float(acc))
    }),
    f!("minf", [Any], Some(Any), |_c, a| {
        let mut acc = cast_to_f64(&a[0]);
        for v in &a[1..] {
            acc = go_math_min(acc, cast_to_f64(v));
        }
        Ok(Value::Float(acc))
    }),
    f!("ceil", [Any], None, |_c, a| Ok(Value::Float(
        cast_to_f64(&a[0]).ceil()
    ))),
    f!("floor", [Any], None, |_c, a| Ok(Value::Float(
        cast_to_f64(&a[0]).floor()
    ))),
    f!("round", [Any, Int], Some(Float), |_c, a| {
        let round_on = match a.get(2) {
            Some(Value::Float(f)) => *f,
            _ => 0.5,
        };
        let val = cast_to_f64(&a[0]);
        let places = int_of(&a[1]) as f64;
        let pow = 10f64.powf(places);
        let digit = pow * val;
        let frac = digit - digit.trunc();
        let rounded = if frac >= round_on {
            digit.ceil()
        } else {
            digit.floor()
        };
        Ok(Value::Float(rounded / pow))
    }),
    f!("fromJson", [Str], None, |c, a| from_json(
        c,
        bytes_of(&a[0])
    )),
    f!("date", [Str, Any], None, |c, a| {
        // sprig dateInZone(fmt, date, "Local"). The layout is a
        // template value and expands ≤ ~10× (longest token: a month
        // name for a 3-byte "Jan") — charged before formatting.
        c.charge(10usize.saturating_mul(bytes_of(&a[0]).len()) + 64)?;
        let t = coerce_sprig_date(&a[1], c.env());
        let out = super::golayout::format_layout(&t.in_loc(GoLoc::Local), bytes_of(&a[0]), c.env());
        Ok(str_val(out))
    }),
    f!("toDate", [Str, Str], None, |c, a| {
        let t = super::golayout::parse_in_location(
            bytes_of(&a[0]),
            bytes_of(&a[1]),
            &GoLoc::Local,
            c.env(),
        )
        .unwrap_or_else(|_| GoTime::zero());
        Ok(Value::Time(t))
    }),
    f!("now", [], None, |c, _a| Ok(Value::Time(c.env().now()))),
    f!("unixEpoch", [Time], None, |_c, a| Ok(string_val(
        time_arg(a, 0).unix().to_string()
    ))),
    f!("default", [Any], Some(Any), |c, a| {
        // dfault(d, given...): d when given is empty or given[0] empty.
        // The winner is CLONED into the result (deep for owned
        // strings) — a retainable copy, charged like every production.
        let given = &a[1..];
        let winner = if given.is_empty() || sprig_empty(&given[0]) {
            &a[0]
        } else {
            &given[0]
        };
        c.charge(winner.charge_ceiling())?;
        Ok(winner.clone())
    }),
];

fn int_of(v: &Value<'_>) -> i64 {
    match v {
        Value::Int(n, _) => *n,
        Value::Uint(n, _) => *n as i64,
        Value::Duration(n) | Value::Month(n) | Value::Weekday(n) => *n,
        // Unreachable: ParamTy::Int coercion happened in eval.
        _ => 0,
    }
}

/// sprig `empty` (defaults.go:34-59).
fn sprig_empty(v: &Value<'_>) -> bool {
    match v {
        Value::Nil => true,
        Value::Str(s) => s.is_empty(),
        Value::Bytes(b) => b.is_empty(),
        Value::List(l) => l.is_empty(),
        Value::Map(m) => m.is_empty(),
        Value::LabelMap => false, // resolved by eval before the call
        Value::Bool(b) => !*b,
        Value::Int(n, _) => *n == 0,
        Value::Uint(n, _) => *n == 0,
        Value::Float(f) => *f == 0.0,
        Value::Complex(re, im) => *re == 0.0 && *im == 0.0,
        Value::Duration(n) | Value::Month(n) | Value::Weekday(n) => *n == 0,
        // Structs are never empty.
        Value::Time(_) => false,
        // Pointers: nil check — a Location value is always non-nil.
        Value::Location(_) => false,
    }
}

fn go_math_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else if a == 0.0 && b == 0.0 {
        // math.Max(±0, ±0) = +0 unless both -0.
        if a.is_sign_negative() && b.is_sign_negative() {
            -0.0
        } else {
            0.0
        }
    } else if a > b {
        a
    } else {
        b
    }
}

fn go_math_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else if a == 0.0 && b == 0.0 {
        if a.is_sign_negative() || b.is_sign_negative() {
            -0.0
        } else {
            0.0
        }
    } else if a < b {
        a
    } else {
        b
    }
}

// ---------------------------------------------------------------------
// String helpers (Go stdlib ports, byte-string aware)
// ---------------------------------------------------------------------

/// Decode the rune at `s[i]` like Go's `DecodeRune`: invalid bytes are
/// (U+FFFD, 1).
fn decode_rune(s: &[u8], i: usize) -> (char, usize) {
    let first = s[i];
    let len = match first {
        0x00..=0x7F => 1,
        0xC2..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF4 => 4,
        _ => return ('\u{FFFD}', 1),
    };
    if i + len > s.len() {
        return ('\u{FFFD}', 1);
    }
    match std::str::from_utf8(&s[i..i + len]) {
        Ok(t) => (t.chars().next().unwrap_or('\u{FFFD}'), len),
        Err(_) => ('\u{FFFD}', 1),
    }
}

fn cutset_contains(cutset: &[u8], r: char) -> bool {
    let mut i = 0;
    while i < cutset.len() {
        let (c, w) = decode_rune(cutset, i);
        if c == r {
            return true;
        }
        i += w;
    }
    false
}

fn go_trim(
    ctx: &FuncCtx<'_, '_>,
    s: &[u8],
    cutset: &[u8],
    left: bool,
    right: bool,
) -> Result<Vec<u8>, String> {
    let mut lo = 0;
    let mut hi = s.len();
    if left {
        while lo < hi {
            let (r, w) = decode_rune(s, lo);
            if !cutset_contains(cutset, r) {
                break;
            }
            lo += w;
        }
    }
    if right {
        while hi > lo {
            // Scan backward: find the last rune start.
            let mut start = hi - 1;
            while start > lo && (s[start] & 0xC0) == 0x80 {
                start -= 1;
            }
            let (r, w) = decode_rune(s, start);
            if start + w != hi {
                // Truncated/invalid tail byte: Go's DecodeLastRune
                // yields RuneError of size 1.
                if !cutset_contains(cutset, '\u{FFFD}') {
                    break;
                }
                hi -= 1;
                continue;
            }
            if !cutset_contains(cutset, r) {
                break;
            }
            hi = start;
        }
    }
    // The output copy is retainable — charged like every production.
    ctx.charge(hi - lo)?;
    Ok(s[lo..hi].to_vec())
}

fn is_go_space(r: char) -> bool {
    matches!(
        r,
        '\t' | '\n' | '\x0B' | '\x0C' | '\r' | ' ' | '\u{85}' | '\u{A0}'
    ) || r.is_whitespace()
}

fn go_trim_space(ctx: &FuncCtx<'_, '_>, s: &[u8]) -> Result<Vec<u8>, String> {
    let mut lo = 0;
    let mut hi = s.len();
    while lo < hi {
        let (r, w) = decode_rune(s, lo);
        if !is_go_space(r) {
            break;
        }
        lo += w;
    }
    while hi > lo {
        let mut start = hi - 1;
        while start > lo && (s[start] & 0xC0) == 0x80 {
            start -= 1;
        }
        let (r, w) = decode_rune(s, start);
        if start + w != hi || !is_go_space(r) {
            break;
        }
        hi = start;
    }
    ctx.charge(hi - lo)?;
    Ok(s[lo..hi].to_vec())
}

/// `strings.Replace` — the exact stdlib algorithm (n < 0 = all; an
/// empty `old` inserts at the start and after each rune).
fn go_replace(
    ctx: &FuncCtx<'_, '_>,
    s: &[u8],
    old: &[u8],
    new: &[u8],
    n: i64,
) -> Result<Vec<u8>, String> {
    if old == new || n == 0 {
        // IDENTITY copies are retainable productions too — a no-output
        // `range` repeats them past budget (review round 3, finding 1):
        // charge before every early-return copy.
        ctx.charge(s.len())?;
        return Ok(s.to_vec());
    }
    // Count occurrences (Go strings.Count).
    let m: i64 = if old.is_empty() {
        (count_runes(s) + 1) as i64
    } else {
        count_non_overlapping(s, old) as i64
    };
    if m == 0 {
        ctx.charge(s.len())?;
        return Ok(s.to_vec());
    }
    let n = if n < 0 || m < n { m } else { n };
    // Output ≤ len(s) + n×len(new) — an empty needle multiplies the
    // input by the replacement length, so charge up front.
    ctx.charge(
        s.len()
            .saturating_add((n as usize).saturating_mul(new.len())),
    )?;
    let mut out = Vec::with_capacity(s.len() + (n as usize) * new.len());
    let mut start = 0usize;
    for i in 0..n {
        let mut j = start;
        if old.is_empty() {
            if i > 0 {
                let (_, w) = decode_rune(s, start);
                j += w;
            }
        } else {
            j += find_sub(&s[start..], old).unwrap_or(0);
        }
        out.extend_from_slice(&s[start..j]);
        out.extend_from_slice(new);
        start = j + old.len();
    }
    out.extend_from_slice(&s[start..]);
    Ok(out)
}

fn count_runes(s: &[u8]) -> usize {
    let mut n = 0;
    let mut i = 0;
    while i < s.len() {
        let (_, w) = decode_rune(s, i);
        i += w;
        n += 1;
    }
    n
}

fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

fn count_non_overlapping(s: &[u8], sub: &[u8]) -> usize {
    let mut n = 0;
    let mut i = 0;
    while let Some(p) = find_sub(&s[i..], sub) {
        n += 1;
        i += p + sub.len();
        if i > s.len() {
            break;
        }
    }
    n
}

/// `unicode.ToUpper` — Go's SIMPLE per-rune mapping (`strings.ToUpper`
/// maps rune-wise): a rune whose Rust FULL mapping expands to multiple
/// chars has no simple mapping (`ß` stays `ß`).
fn go_rune_upper(r: char) -> char {
    let mut it = r.to_uppercase();
    match (it.next(), it.next()) {
        (Some(u), None) => u,
        _ => r,
    }
}

/// `unicode.ToLower` — simple mapping; U+0130 (`İ`) is the one rune
/// whose full lowercase is multi-char but whose SIMPLE mapping exists
/// (`i`, per UnicodeData).
fn go_rune_lower(r: char) -> char {
    if r == '\u{130}' {
        return 'i';
    }
    let mut it = r.to_lowercase();
    match (it.next(), it.next()) {
        (Some(l), None) => l,
        _ => r,
    }
}

/// `unicode.ToTitle` — simple titlecase: the digraph runes have
/// distinct titlecase forms; everything else follows ToUpper.
fn go_rune_title(r: char) -> char {
    match r {
        '\u{1C4}' | '\u{1C5}' | '\u{1C6}' => '\u{1C5}',
        '\u{1C7}' | '\u{1C8}' | '\u{1C9}' => '\u{1C8}',
        '\u{1CA}' | '\u{1CB}' | '\u{1CC}' => '\u{1CB}',
        '\u{1F1}' | '\u{1F2}' | '\u{1F3}' => '\u{1F2}',
        _ => go_rune_upper(r),
    }
}

fn map_runes(s: &[u8], f: impl Fn(char) -> char) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        let (r, w) = decode_rune(s, i);
        if w == 1 && s[i] >= 0x80 {
            // Invalid byte: Go's Map replaces it with U+FFFD.
            out.extend_from_slice("\u{FFFD}".as_bytes());
        } else {
            let mut buf = [0u8; 4];
            out.extend_from_slice(f(r).encode_utf8(&mut buf).as_bytes());
        }
        i += w;
    }
    out
}

fn go_to_upper(s: &[u8]) -> Vec<u8> {
    map_runes(s, go_rune_upper)
}

fn go_to_lower(s: &[u8]) -> Vec<u8> {
    map_runes(s, go_rune_lower)
}

/// `strings.Title` (the deprecated word-boundary version sprig uses).
fn go_title(s: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let mut prev_sep = true;
    let mut i = 0;
    while i < s.len() {
        let (r, w) = decode_rune(s, i);
        if prev_sep && r.is_alphabetic() {
            let mut buf = [0u8; 4];
            out.extend_from_slice(go_rune_title(r).encode_utf8(&mut buf).as_bytes());
        } else {
            out.extend_from_slice(&s[i..i + w]);
        }
        prev_sep = go_is_separator(r);
        i += w;
    }
    out
}

fn go_is_separator(r: char) -> bool {
    if (r as u32) <= 0x7F {
        return !(r.is_ascii_alphanumeric() || r == '_');
    }
    if r.is_alphabetic() || r.is_numeric() {
        return false;
    }
    r.is_whitespace()
}

fn indent_impl(
    ctx: &FuncCtx<'_, '_>,
    spaces: i64,
    v: &[u8],
    newline_first: bool,
) -> Result<Vec<u8>, String> {
    if spaces < 0 {
        return Err("strings: negative Repeat count".to_string());
    }
    // pad × (1 + newline count) + the input — computable up front.
    let lines = 1 + v.iter().filter(|&&b| b == b'\n').count();
    ctx.charge(
        (spaces as usize)
            .saturating_mul(lines)
            .saturating_add(v.len() + 1),
    )?;
    let pad = vec![b' '; spaces as usize];
    let mut out = Vec::with_capacity(v.len() + pad.len());
    if newline_first {
        out.push(b'\n');
    }
    out.extend_from_slice(&pad);
    let mut i = 0;
    while i < v.len() {
        out.push(v[i]);
        if v[i] == b'\n' {
            out.extend_from_slice(&pad);
        }
        i += 1;
    }
    Ok(out)
}

fn align(ctx: &FuncCtx<'_, '_>, count: i64, src: &[u8], left: bool) -> Result<Vec<u8>, String> {
    // alignLeft/alignRight operate on RUNES (fmt.go:460-484). Count and
    // cut WITHOUT materialising per-rune data (review round 1, miss 2:
    // the former `Vec<(usize, usize)>` intermediate allocated 16 bytes
    // per rune BEFORE the first charge — an uncharged 16× blow-up).
    let mut n_runes: i64 = 0;
    let mut i = 0;
    while i < src.len() {
        let (_, w) = decode_rune(src, i);
        i += w;
        n_runes += 1;
    }
    let l = n_runes;
    if count < 0 || count == l {
        // Identity copy: charged like every retainable production
        // (review round 3, finding 1).
        ctx.charge(src.len())?;
        return Ok(src.to_vec());
    }
    let pad = count - l;
    if pad > 0 {
        // The pad width is template-controlled: charge FIRST.
        ctx.charge((pad as usize).saturating_add(src.len()))?;
        let mut out = Vec::with_capacity(src.len() + pad as usize);
        if left {
            out.extend_from_slice(src);
            out.extend(std::iter::repeat_n(b' ', pad as usize));
        } else {
            out.extend(std::iter::repeat_n(b' ', pad as usize));
            out.extend_from_slice(src);
        }
        return Ok(out);
    }
    // Truncation: find the byte offset of the cut rune boundary by a
    // second scan; the output copy is retainable, so it is charged like
    // every production (review round 2).
    let keep_from_start = if left { count } else { l - count };
    let mut i = 0;
    let mut seen: i64 = 0;
    while seen < keep_from_start {
        let (_, w) = decode_rune(src, i);
        i += w;
        seen += 1;
    }
    Ok(if left {
        ctx.charge(i)?;
        src[..i].to_vec()
    } else {
        ctx.charge(src.len() - i)?;
        src[i..].to_vec()
    })
}

// ---------------------------------------------------------------------
// URL escaping (net/url ports)
// ---------------------------------------------------------------------

fn ishex(c: u8) -> bool {
    c.is_ascii_digit() || (b'a'..=b'f').contains(&c) || (b'A'..=b'F').contains(&c)
}

fn unhex(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0,
    }
}

/// `url.QueryUnescape`.
fn query_unescape(ctx: &FuncCtx<'_, '_>, s: &[u8]) -> Result<Vec<u8>, String> {
    // Output ≤ input, charged before the buffer (review round 2).
    ctx.charge(s.len())?;
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        match s[i] {
            b'%' => {
                if i + 2 >= s.len() || !ishex(s[i + 1]) || !ishex(s[i + 2]) {
                    let bad = &s[i..s.len().min(i + 3)];
                    return Err(format!(
                        "invalid URL escape {}",
                        gofmt::quote_bytes(bad, '"')
                    ));
                }
                out.push(unhex(s[i + 1]) << 4 | unhex(s[i + 2]));
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    Ok(out)
}

/// `url.QueryEscape`.
fn query_escape(s: &[u8]) -> Vec<u8> {
    const UPPERHEX: &[u8] = b"0123456789ABCDEF";
    let mut out = Vec::with_capacity(s.len());
    for &c in s {
        if c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b'~') {
            out.push(c);
        } else if c == b' ' {
            out.push(b'+');
        } else {
            out.push(b'%');
            out.push(UPPERHEX[(c >> 4) as usize]);
            out.push(UPPERHEX[(c & 0xF) as usize]);
        }
    }
    out
}

// ---------------------------------------------------------------------
// base64 (StdEncoding with Go's error offsets)
// ---------------------------------------------------------------------

/// `base64.StdEncoding.DecodeString` — Go skips `\r`/`\n` and reports
/// `illegal base64 data at input byte %d` with the offset in the
/// ORIGINAL input.
fn b64_decode_go(s: &[u8]) -> Result<Vec<u8>, String> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let mut quad: [u8; 4] = [0; 4];
    let mut quad_pos: [usize; 4] = [0; 4];
    let mut n = 0usize;
    let mut seen_pad = false;
    let mut pad_count = 0usize;
    for (i, &c) in s.iter().enumerate() {
        if c == b'\r' || c == b'\n' {
            continue;
        }
        if c == b'=' {
            // Padding legal only in positions 2/3 of a quad; '=' in
            // position 2 must be followed by another '=' (Go
            // decodeQuantum errors at THIS byte otherwise).
            if n < 2 || seen_pad && pad_count >= 2 {
                return Err(format!("illegal base64 data at input byte {i}"));
            }
            if n == 2 {
                let next = s[i + 1..].iter().find(|&&c2| c2 != b'\r' && c2 != b'\n');
                if next != Some(&b'=') {
                    return Err(format!("illegal base64 data at input byte {i}"));
                }
            }
            seen_pad = true;
            pad_count += 1;
            quad[n] = 0;
            quad_pos[n] = i;
            n += 1;
            if n == 4 {
                let trip = decode_quad(&quad);
                match pad_count {
                    1 => out.extend_from_slice(&trip[..2]),
                    _ => out.extend_from_slice(&trip[..1]),
                }
                // Trailing garbage after padding is illegal.
                for (j, &c2) in s.iter().enumerate().skip(i + 1) {
                    if c2 != b'\r' && c2 != b'\n' {
                        return Err(format!("illegal base64 data at input byte {j}"));
                    }
                }
                return Ok(out);
            }
            continue;
        }
        if seen_pad {
            return Err(format!("illegal base64 data at input byte {i}"));
        }
        match val(c) {
            Some(v) => {
                quad[n] = v;
                quad_pos[n] = i;
                n += 1;
                if n == 4 {
                    out.extend_from_slice(&decode_quad(&quad));
                    n = 0;
                }
            }
            None => return Err(format!("illegal base64 data at input byte {i}")),
        }
    }
    if n > 0 {
        // Unpadded remainder: Go reports the offset of the incomplete
        // quad's first byte.
        return Err(format!("illegal base64 data at input byte {}", quad_pos[0]));
    }
    Ok(out)
}

fn decode_quad(q: &[u8; 4]) -> [u8; 3] {
    let v = ((q[0] as u32) << 18) | ((q[1] as u32) << 12) | ((q[2] as u32) << 6) | q[3] as u32;
    [(v >> 16) as u8, (v >> 8) as u8, v as u8]
}

// ---------------------------------------------------------------------
// unixToTime / fromJson / regex / sprig date coercion
// ---------------------------------------------------------------------

/// `unixToTime` (`fmt.go:136-163`).
fn unix_to_time(epoch: &str) -> Result<GoTime, String> {
    let l = epoch.len();
    let parsed: Result<i64, String> = {
        let ok = {
            let (body, neg_ok) = match epoch.strip_prefix('-') {
                Some(rest) => (rest, true),
                None => (epoch.strip_prefix('+').unwrap_or(epoch), true),
            };
            let _ = neg_ok;
            !body.is_empty() && body.chars().all(|c| c.is_ascii_digit())
        };
        if !ok {
            Err(format!(
                "strconv.ParseInt: parsing {}: invalid syntax",
                gofmt::quote_bytes(epoch.as_bytes(), '"')
            ))
        } else {
            epoch.parse::<i64>().map_err(|_| {
                format!(
                    "strconv.ParseInt: parsing {}: value out of range",
                    gofmt::quote_bytes(epoch.as_bytes(), '"')
                )
            })
        }
    };
    let i = match parsed {
        Ok(i) => i,
        Err(inner) => {
            return Err(format!("unable to parse time '{epoch}': {inner}"));
        }
    };
    match l {
        5 => Ok(GoTime::from_unix(i * 86_400, 0)),
        10 => Ok(GoTime::from_unix(i, 0)),
        13 => Ok(GoTime::from_unix_ns(i.wrapping_mul(1_000_000))),
        16 => Ok(GoTime::from_unix_ns(i.wrapping_mul(1000))),
        19 => Ok(GoTime::from_unix_ns(i)),
        _ => Err(format!("unable to parse time '{epoch}': %!w(<nil>)")),
    }
}

/// sprig `dateInZone`'s date coercion: `time.Time` passes through,
/// int-kinds are Unix seconds, EVERYTHING else is `time.Now()`.
fn coerce_sprig_date(v: &Value<'_>, env: &TemplateEnv) -> GoTime {
    match v {
        Value::Time(t) => t.clone(),
        Value::Int(n, _) => GoTime::from_unix(*n, 0),
        _ => env.now(),
    }
}

/// `fromJson` — `encoding/json.Unmarshal` into `any` (errors swallowed:
/// a failed parse yields nil). Go's string decoding substitutes
/// U+FFFD where serde is strict, so the input is sanitised first
/// (see [`go_json_sanitize`]); the parse-tree ceiling is charged up
/// front: worst structural density is one element per two input bytes
/// ("[1,1,…"), each costing ≤64 bytes across the serde tree and the
/// converted Value tree — a ≤32× ceiling, plus the ≤3× sanitised copy.
fn from_json<'a>(ctx: &FuncCtx<'_, '_>, s: &[u8]) -> Result<Value<'a>, String> {
    ctx.charge(35usize.saturating_mul(s.len()) + 64)?;
    let sanitized = go_json_sanitize(s);
    Ok(
        match serde_json::from_slice::<serde_json::Value>(&sanitized) {
            Ok(v) => json_to_value(v),
            Err(_) => Value::Nil,
        },
    )
}

/// Reproduces the two places Go's `encoding/json` string decoding is
/// LENIENT where serde is strict, both container-captured on the
/// pinned reference (grafana/loki:3.7.4, go1.26.5):
///
/// - invalid UTF-8 **bytes** inside a JSON string become U+FFFD, one
///   replacement per invalid byte (`unquoteBytes` advances by
///   `utf8.DecodeRune`'s size-1 on error: `"a\xf0\x9fb"` → `a��b`,
///   a truncated `"\xf0\x9f\x92"` tail → three U+FFFD);
/// - a `\uXXXX` escape encoding a LONE surrogate becomes U+FFFD
///   (`\ud800` → `�`, `\ud800\ud800` → `��`, `\ud800x` → `�x`); a
///   valid surrogate PAIR is left for serde to combine (`😀`
///   → 😀).
///
/// Outside string literals nothing is rewritten: a stray invalid byte
/// is a syntax error for Go's scanner and for serde alike (→ nil).
/// The rewrite is length-preserving-or-expanding by ≤3×, under
/// `from_json`'s charge.
fn go_json_sanitize(s: &[u8]) -> Cow<'_, [u8]> {
    const REPLACEMENT: &[u8] = "\u{FFFD}".as_bytes();
    let mut out: Option<Vec<u8>> = None;
    let mut in_string = false;
    let mut i = 0;
    let mut copied = 0; // bytes of `s` already flushed into `out`
    let replace = |out: &mut Option<Vec<u8>>, copied: &mut usize, upto: usize, skip: usize| {
        let buf = out.get_or_insert_with(|| Vec::with_capacity(s.len() + 8));
        buf.extend_from_slice(&s[*copied..upto]);
        buf.extend_from_slice(REPLACEMENT);
        *copied = upto + skip;
    };
    while i < s.len() {
        let c = s[i];
        if !in_string {
            if c == b'"' {
                in_string = true;
            }
            i += 1;
            continue;
        }
        match c {
            b'"' => {
                in_string = false;
                i += 1;
            }
            b'\\' => {
                // An escape: only `\uXXXX` needs surrogate handling;
                // serde validates everything else identically to Go.
                if s[i + 1..].first() == Some(&b'u')
                    && let Some(r) = hex4(s, i + 2)
                    && (0xD800..=0xDFFF).contains(&r)
                {
                    // High surrogate followed by a valid low surrogate
                    // is a pair — leave both escapes for serde.
                    let paired = (0xD800..0xDC00).contains(&r)
                        && s.get(i + 6) == Some(&b'\\')
                        && s.get(i + 7) == Some(&b'u')
                        && hex4(s, i + 8).is_some_and(|r2| (0xDC00..=0xDFFF).contains(&r2));
                    if paired {
                        i += 12;
                    } else {
                        replace(&mut out, &mut copied, i, 6);
                        i += 6;
                    }
                } else {
                    // Skip the escape introducer + one byte so an
                    // escaped quote (`\"`) cannot flip the string state.
                    i += 2;
                }
            }
            0x20..=0x7F => i += 1,
            _ => {
                let (_, w) = decode_rune(s, i);
                if w == 1 && c >= 0x80 {
                    // Invalid byte: one U+FFFD per byte (Go DecodeRune).
                    replace(&mut out, &mut copied, i, 1);
                    i += 1;
                } else {
                    i += w;
                }
            }
        }
    }
    match out {
        Some(mut buf) => {
            buf.extend_from_slice(&s[copied..]);
            Cow::Owned(buf)
        }
        None => Cow::Borrowed(s),
    }
}

/// Four hex digits at `s[i..i+4]` (Go `getu4`, case-insensitive).
fn hex4(s: &[u8], i: usize) -> Option<u32> {
    let chunk = s.get(i..i + 4)?;
    let mut v: u32 = 0;
    for &c in chunk {
        v = v * 16 + (c as char).to_digit(16)?;
    }
    Some(v)
}

fn json_to_value<'a>(v: serde_json::Value) -> Value<'a> {
    match v {
        serde_json::Value::Null => Value::Nil,
        serde_json::Value::Bool(b) => Value::Bool(b),
        // encoding/json into `any` makes EVERY number a float64.
        serde_json::Value::Number(n) => Value::Float(n.as_f64().unwrap_or(f64::NAN)),
        serde_json::Value::String(s) => Value::Str(Cow::Owned(s.into_bytes())),
        serde_json::Value::Array(items) => {
            Value::List(Rc::new(items.into_iter().map(json_to_value).collect()))
        }
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(Vec<u8>, Value<'a>)> = map
                .into_iter()
                .map(|(k, v)| (k.into_bytes(), json_to_value(v)))
                .collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Map(Rc::new(entries))
        }
    }
}

/// The compiled-program ceiling for a DYNAMICALLY-built pattern (one
/// whose text a template computes per line, so it misses the
/// compile-time cache): `regex::RegexBuilder::size_limit` guarantees
/// the program stays under it, and the render budget is charged the
/// full ceiling BEFORE compiling (review round 2: regex programs are
/// caller-sized allocations like any other). 1 MiB keeps 64 dynamic
/// compiles inside one render budget; the reference is unbounded here
/// (Go regexp has no program cap) — the same ledgered class as the
/// output budget itself (`template-output-budget`).
const DYNAMIC_REGEX_PROGRAM_CEILING: usize = 1 << 20;

fn compile_regex(ctx: &FuncCtx<'_, '_>, pattern: &[u8]) -> Result<regex::Regex, String> {
    // The pattern copy is charged too (repeatable per call) — at the
    // EXACT repaired length when the pattern bytes are invalid UTF-8
    // (rounds 4+6: the conversion must neither allocate before its
    // charge nor grow past it).
    let text = match std::str::from_utf8(pattern) {
        Ok(s) => {
            ctx.charge(pattern.len())?;
            s.to_string()
        }
        Err(_) => {
            // Round 6: exact repaired length, one allocation, charged
            // first — same as `lossy_charged` (no growth to bound).
            let need = lossy_repaired_len(pattern);
            ctx.charge(need)?;
            lossy_repaired(pattern, need)
        }
    };
    if let Some(re) = ctx.regex_cache.get(&text) {
        // Query-compile-time program (literal pattern): `Regex` is
        // Arc-backed, the clone shares it — no per-line program.
        return Ok(re.clone());
    }
    ctx.charge(DYNAMIC_REGEX_PROGRAM_CEILING)?;
    regex::RegexBuilder::new(&text)
        .size_limit(DYNAMIC_REGEX_PROGRAM_CEILING)
        .build()
        .map_err(|e| {
            // The reference emits Go's `error parsing regexp: …` wording;
            // rust-regex words its diagnostics differently. The accept/
            // reject boundary matches (both are RE2-class engines); the
            // wording difference is ledgered (owner error-wording ruling).
            format!("error parsing regexp: {e}")
        })
}

/// `uint8` element of a `[]byte` (for `index $b 0`).
pub fn byte_elem<'a>(b: u8) -> Value<'a> {
    Value::Uint(b as u64, UintKind::Uint8)
}

// ---------------------------------------------------------------------
// time.ParseDuration (full stdlib port — the `duration`/
// `duration_seconds` template functions; the label-filter conversion
// keeps its own pinned path)
// ---------------------------------------------------------------------

fn duration_seconds_f64(d: i64) -> f64 {
    // `Duration.Seconds()`.
    let sec = d / 1_000_000_000;
    let nsec = d % 1_000_000_000;
    sec as f64 + nsec as f64 / 1e9
}

/// `time.ParseDuration` with Go's exact error texts (time-quote form).
pub(crate) fn go_parse_duration(orig: &str) -> Result<i64, String> {
    use pulsus_promql::eval::quote::go_time_quote;
    let invalid = || format!("time: invalid duration {}", go_time_quote(orig));
    let mut s = orig.as_bytes();
    let mut d: u64 = 0;
    let mut neg = false;
    if let Some(&c) = s.first()
        && (c == b'-' || c == b'+')
    {
        neg = c == b'-';
        s = &s[1..];
    }
    if s == b"0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(invalid());
    }
    while !s.is_empty() {
        if !(s[0] == b'.' || s[0].is_ascii_digit()) {
            return Err(invalid());
        }
        // leadingInt
        let mut v: u64 = 0;
        let pl = s.len();
        while let Some(&c) = s.first() {
            if !c.is_ascii_digit() {
                break;
            }
            if v > (1 << 63) / 10 {
                return Err(invalid());
            }
            v = v * 10 + (c - b'0') as u64;
            if v > 1 << 63 {
                return Err(invalid());
            }
            s = &s[1..];
        }
        let pre = pl != s.len();
        // (\.[0-9]*)?
        let mut f: u64 = 0;
        let mut scale: f64 = 1.0;
        let mut post = false;
        if s.first() == Some(&b'.') {
            s = &s[1..];
            let pl = s.len();
            let mut overflow = false;
            while let Some(&c) = s.first() {
                if !c.is_ascii_digit() {
                    break;
                }
                if !overflow {
                    if f > (1 << 63) / 10 {
                        overflow = true;
                    } else {
                        let y = f * 10 + (c - b'0') as u64;
                        if y > 1 << 63 {
                            overflow = true;
                        } else {
                            f = y;
                            scale *= 10.0;
                        }
                    }
                }
                s = &s[1..];
            }
            post = pl != s.len();
        }
        if !pre && !post {
            return Err(invalid());
        }
        // unit
        let mut i = 0;
        while i < s.len() {
            let c = s[i];
            if c == b'.' || c.is_ascii_digit() {
                break;
            }
            i += 1;
        }
        if i == 0 {
            return Err(format!(
                "time: missing unit in duration {}",
                go_time_quote(orig)
            ));
        }
        let u = &s[..i];
        s = &s[i..];
        let unit: u64 = match u {
            b"ns" => 1,
            b"us" => 1_000,
            [0xC2, 0xB5, b's'] => 1_000, // µs
            [0xCE, 0xBC, b's'] => 1_000, // μs
            b"ms" => 1_000_000,
            b"s" => 1_000_000_000,
            b"m" => 60_000_000_000,
            b"h" => 3_600_000_000_000,
            _ => {
                let u_str = String::from_utf8_lossy(u).into_owned();
                return Err(format!(
                    "time: unknown unit {} in duration {}",
                    go_time_quote(&u_str),
                    go_time_quote(orig)
                ));
            }
        };
        if v > (1 << 63) / unit {
            return Err(invalid());
        }
        v = v.wrapping_mul(unit);
        if f > 0 {
            v = v.wrapping_add((f as f64 * (unit as f64 / scale)) as u64);
            if v > 1 << 63 {
                return Err(invalid());
            }
        }
        d = d.wrapping_add(v);
        if d > 1 << 63 {
            return Err(invalid());
        }
    }
    if neg {
        return Ok((d as i64).wrapping_neg());
    }
    if d > (1 << 63) - 1 {
        return Err(invalid());
    }
    Ok(d as i64)
}

#[cfg(test)]
mod tests {
    use super::{lossy_repaired, lossy_repaired_len};

    /// Round 6: the single-allocation repair must be byte-identical to
    /// `String::from_utf8_lossy` (std's maximal-subpart substitution
    /// granularity — NOT the Go per-byte rule `go_json_sanitize`
    /// implements for fromJson), and `lossy_repaired_len` must be its
    /// exact length, across the awkward shapes: truncated tails,
    /// invalid continuations, overlong encodings, interleaved runs.
    #[test]
    fn lossy_repaired_matches_std_from_utf8_lossy_byte_for_byte() {
        let cases: &[&[u8]] = &[
            b"",
            b"plain ascii",
            "caf\u{e9} \u{1F600}".as_bytes(),
            &[0xFF],
            &[0xFF; 7],
            b"a\xFFb\xFFc",
            b"\xC2",                   // truncated 2-byte tail
            b"\xE0\xA0",               // truncated 3-byte tail
            b"\xF0\x9F\x92",           // truncated 4-byte tail
            b"a\xF0\x28b",             // invalid continuation mid-stream
            b"\xC0\xAF",               // overlong encoding
            b"\xED\xA0\x80",           // surrogate range
            b"x\xF0\x9F\x92\x96y\xFF", // valid 4-byte + trailing invalid
        ];
        for v in cases {
            let expected = String::from_utf8_lossy(v);
            let need = lossy_repaired_len(v);
            assert_eq!(need, expected.len(), "{v:X?}");
            assert_eq!(lossy_repaired(v, need), expected.as_ref(), "{v:X?}");
        }
    }
}
