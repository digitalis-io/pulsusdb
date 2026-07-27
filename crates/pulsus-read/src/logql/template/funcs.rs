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
    // reachable corpus shapes have none.
    let cleaned: String = body.chars().filter(|&c| c != '_').collect();
    if cleaned.is_empty() || (body.starts_with('_') || body.ends_with('_') || body.contains("__")) {
        return None;
    }
    let (base, digits) = if let Some(r) = cleaned
        .strip_prefix("0x")
        .or_else(|| cleaned.strip_prefix("0X"))
    {
        (16, r.to_string())
    } else if let Some(r) = cleaned
        .strip_prefix("0o")
        .or_else(|| cleaned.strip_prefix("0O"))
    {
        (8, r.to_string())
    } else if let Some(r) = cleaned
        .strip_prefix("0b")
        .or_else(|| cleaned.strip_prefix("0B"))
    {
        (2, r.to_string())
    } else if cleaned.len() > 1 && cleaned.starts_with('0') {
        (8, cleaned[1..].to_string())
    } else {
        (10, cleaned.clone())
    };
    if digits.is_empty() {
        return None;
    }
    let mag = u64::from_str_radix(&digits, base).ok()?;
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
    f!("Trim", [Str, Str], None, |_c, a| Ok(str_val(go_trim(
        bytes_of(&a[0]),
        bytes_of(&a[1]),
        true,
        true
    )))),
    f!("TrimLeft", [Str, Str], None, |_c, a| Ok(str_val(go_trim(
        bytes_of(&a[0]),
        bytes_of(&a[1]),
        true,
        false
    )))),
    f!("TrimRight", [Str, Str], None, |_c, a| Ok(str_val(go_trim(
        bytes_of(&a[0]),
        bytes_of(&a[1]),
        false,
        true
    )))),
    f!("TrimPrefix", [Str, Str], None, |_c, a| {
        let (s, p) = (bytes_of(&a[0]), bytes_of(&a[1]));
        Ok(str_val(s.strip_prefix(p).unwrap_or(s).to_vec()))
    }),
    f!("TrimSuffix", [Str, Str], None, |_c, a| {
        let (s, p) = (bytes_of(&a[0]), bytes_of(&a[1]));
        Ok(str_val(s.strip_suffix(p).unwrap_or(s).to_vec()))
    }),
    f!("TrimSpace", [Str], None, |_c, a| Ok(str_val(
        go_trim_space(bytes_of(&a[0]))
    ))),
    f!("regexReplaceAll", [Str, Str, Str], None, |c, a| {
        let re = compile_regex(c, bytes_of(&a[0]))?;
        let s = lossy(bytes_of(&a[1])).into_owned();
        let repl = lossy(bytes_of(&a[2])).into_owned();
        // ≤ (matches+1)×len(repl) + len(s); matches ≤ len(s)+1.
        c.charge(
            (s.len() + 2)
                .saturating_mul(repl.len().max(1))
                .saturating_add(s.len()),
        )?;
        Ok(string_val(re.replace_all(&s, repl.as_str()).into_owned()))
    }),
    f!("regexReplaceAllLiteral", [Str, Str, Str], None, |c, a| {
        let re = compile_regex(c, bytes_of(&a[0]))?;
        let s = lossy(bytes_of(&a[1])).into_owned();
        let repl = lossy(bytes_of(&a[2])).into_owned();
        c.charge(
            (s.len() + 2)
                .saturating_mul(repl.len().max(1))
                .saturating_add(s.len()),
        )?;
        Ok(string_val(
            re.replace_all(&s, regex::NoExpand(&repl)).into_owned(),
        ))
    }),
    f!("count", [Str, Str], None, |c, a| {
        let re = compile_regex(c, bytes_of(&a[0]))?;
        let s = lossy(bytes_of(&a[1])).into_owned();
        Ok(Value::int(re.find_iter(&s).count() as i64))
    }),
    f!("urldecode", [Str], None, |_c, a| {
        let s = bytes_of(&a[0]);
        query_unescape(s).map(str_val)
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
    f!("__line__", [], None, |c, _a| Ok(str_val(c.line.to_vec()))),
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
    f!("b64dec", [Str], None, |_c, a| Ok(
        match b64_decode_go(bytes_of(&a[0])) {
            Ok(data) => str_val(data),
            // sprig returns err.Error() as the VALUE.
            Err(msg) => string_val(msg),
        }
    )),
    f!("lower", [Str], None, |_c, a| Ok(str_val(go_to_lower(
        bytes_of(&a[0])
    )))),
    f!("upper", [Str], None, |_c, a| Ok(str_val(go_to_upper(
        bytes_of(&a[0])
    )))),
    f!("title", [Str], None, |_c, a| Ok(str_val(go_title(
        bytes_of(&a[0])
    )))),
    f!("trunc", [Int, Str], None, |_c, a| {
        // sprig trunc: BYTE slicing (strings.go:189-197).
        let c = int_of(&a[0]);
        let s = bytes_of(&a[1]);
        let len = s.len() as i64;
        Ok(str_val(if c < 0 && len + c > 0 {
            s[(len + c) as usize..].to_vec()
        } else if c >= 0 && len > c {
            s[..c as usize].to_vec()
        } else {
            s.to_vec()
        }))
    }),
    f!("substr", [Int, Int, Str], None, |_c, a| {
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
            return Ok(str_val(s[..end as usize].to_vec()));
        }
        if end < 0 || end > len {
            // s[start:] — a too-high low bound reports against len.
            if start > len {
                return Err(format!(
                    "runtime error: slice bounds out of range [{start}:{len}]"
                ));
            }
            return Ok(str_val(s[start as usize..].to_vec()));
        }
        if start > end {
            return Err(format!(
                "runtime error: slice bounds out of range [{start}:{end}]"
            ));
        }
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
    f!("trim", [Str], None, |_c, a| Ok(str_val(go_trim_space(
        bytes_of(&a[0])
    )))),
    f!("trimAll", [Str, Str], None, |_c, a| Ok(str_val(go_trim(
        bytes_of(&a[1]),
        bytes_of(&a[0]),
        true,
        true,
    )))),
    f!("trimSuffix", [Str, Str], None, |_c, a| {
        let (p, s) = (bytes_of(&a[0]), bytes_of(&a[1]));
        Ok(str_val(s.strip_suffix(p).unwrap_or(s).to_vec()))
    }),
    f!("trimPrefix", [Str, Str], None, |_c, a| {
        let (p, s) = (bytes_of(&a[0]), bytes_of(&a[1]));
        Ok(str_val(s.strip_prefix(p).unwrap_or(s).to_vec()))
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
    f!("fromJson", [Str], None, |_c, a| Ok(from_json(bytes_of(
        &a[0]
    )))),
    f!("date", [Str, Any], None, |c, a| {
        // sprig dateInZone(fmt, date, "Local").
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
    f!("default", [Any], Some(Any), |_c, a| {
        // dfault(d, given...): d when given is empty or given[0] empty.
        let given = &a[1..];
        if given.is_empty() || sprig_empty(&given[0]) {
            return Ok(a[0].clone());
        }
        Ok(given[0].clone())
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

fn go_trim(s: &[u8], cutset: &[u8], left: bool, right: bool) -> Vec<u8> {
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
    s[lo..hi].to_vec()
}

fn is_go_space(r: char) -> bool {
    matches!(
        r,
        '\t' | '\n' | '\x0B' | '\x0C' | '\r' | ' ' | '\u{85}' | '\u{A0}'
    ) || r.is_whitespace()
}

fn go_trim_space(s: &[u8]) -> Vec<u8> {
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
    s[lo..hi].to_vec()
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
        return Ok(s.to_vec());
    }
    // Count occurrences (Go strings.Count).
    let m: i64 = if old.is_empty() {
        (count_runes(s) + 1) as i64
    } else {
        count_non_overlapping(s, old) as i64
    };
    if m == 0 {
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
    // alignLeft/alignRight operate on RUNES (fmt.go:460-484).
    let runes: Vec<(usize, usize)> = {
        let mut v = Vec::new();
        let mut i = 0;
        while i < src.len() {
            let (_, w) = decode_rune(src, i);
            v.push((i, w));
            i += w;
        }
        v
    };
    let l = runes.len() as i64;
    if count < 0 || count == l {
        return Ok(src.to_vec());
    }
    let pad = count - l;
    if pad > 0 {
        // The pad width is template-controlled: charge it first.
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
    Ok(if left {
        let end = runes[count as usize].0;
        src[..end].to_vec()
    } else {
        let start = runes[(l - count) as usize].0;
        src[start..].to_vec()
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
fn query_unescape(s: &[u8]) -> Result<Vec<u8>, String> {
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
/// a failed parse yields nil).
fn from_json<'a>(s: &[u8]) -> Value<'a> {
    let Ok(text) = std::str::from_utf8(s) else {
        return Value::Nil;
    };
    match serde_json::from_str::<serde_json::Value>(text) {
        Ok(v) => json_to_value(v),
        Err(_) => Value::Nil,
    }
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

fn compile_regex(ctx: &FuncCtx<'_, '_>, pattern: &[u8]) -> Result<regex::Regex, String> {
    let text = lossy(pattern).into_owned();
    if let Some(re) = ctx.regex_cache.get(&text) {
        return Ok(re.clone());
    }
    regex::Regex::new(&text).map_err(|e| {
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
