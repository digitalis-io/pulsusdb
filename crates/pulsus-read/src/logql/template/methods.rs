//! `time` closure method dispatch (issue #230, plan v2 Δ1): Go's
//! `evalField` tries `MethodByName` BEFORE map keys, and only
//! value-receiver methods are reachable (a function result is not
//! addressable). The closure is exactly: `time.Time` (38 callable + 5
//! `goodFunc` rejects), `time.Duration` (10), `time.Month`/`time.
//! Weekday`/`*time.Location` (`String`), and `[]byte` (no methods).
//!
//! The five signature rejects carry Go's `goodFunc` text verbatim
//! (`funcs.go:112-123`; plan v3 Δ2 captured both receiver forms).

use std::borrow::Cow;

use super::funcs::ParamTy;
use super::gofmt;
use super::timefns::{self, GoTime, TemplateEnv};
use super::value::{GoLoc, IntKind, Value};

pub enum MethodImpl {
    /// A `goodFunc`-rejected signature: the pre-baked execution error.
    Reject(&'static str),
    Call(MethodFn),
}

pub type MethodFn = for<'a> fn(
    &Value<'a>,
    &[Value<'a>],
    &TemplateEnv,
    &super::RenderBudget,
) -> Result<Value<'a>, String>;

pub struct MethodSig {
    pub params: &'static [ParamTy],
    pub variadic: Option<ParamTy>,
    pub call: MethodImpl,
}

macro_rules! sig {
    ($params:expr, $impl:expr) => {{
        static SIG: MethodSig = MethodSig {
            params: $params,
            variadic: None,
            call: $impl,
        };
        Some(&SIG)
    }};
}

/// Resolves `receiver.name` — `None` falls through to the map-key
/// branch exactly as Go's `evalField` does.
pub fn method(receiver: &Value<'_>, name: &str) -> Option<&'static MethodSig> {
    match receiver {
        Value::Time(_) => time_method(name),
        Value::Duration(_) => duration_method(name),
        Value::Month(_) | Value::Weekday(_) => match name {
            "String" => sig!(&[], MethodImpl::Call(named_int_string)),
            _ => None,
        },
        Value::Location(_) => match name {
            "String" => sig!(&[], MethodImpl::Call(location_string)),
            _ => None,
        },
        _ => None,
    }
}

fn recv_time<'v>(receiver: &'v Value<'_>) -> &'v GoTime {
    match receiver {
        Value::Time(t) => t,
        _ => unreachable!("time method dispatched on a non-Time receiver"),
    }
}

fn recv_dur(receiver: &Value<'_>) -> i64 {
    match receiver {
        Value::Duration(d) => *d,
        _ => unreachable!("duration method dispatched on a non-Duration receiver"),
    }
}

fn arg_dur(args: &[Value<'_>], i: usize) -> i64 {
    match &args[i] {
        Value::Duration(d) => *d,
        _ => 0,
    }
}

fn arg_int(args: &[Value<'_>], i: usize) -> i64 {
    match &args[i] {
        Value::Int(n, _) => *n,
        _ => 0,
    }
}

fn arg_time<'v>(args: &'v [Value<'_>], i: usize) -> &'v GoTime {
    match &args[i] {
        Value::Time(t) => t,
        _ => unreachable!("ParamTy::Time coercion guarantees a Time value"),
    }
}

fn arg_bytes(args: &[Value<'_>], i: usize) -> Vec<u8> {
    match &args[i] {
        Value::Bytes(b) => b.clone().into_owned(),
        _ => Vec::new(),
    }
}

/// The size [`arg_bytes`] will copy — for charging BEFORE the copy.
fn arg_bytes_len(args: &[Value<'_>], i: usize) -> usize {
    match &args[i] {
        Value::Bytes(b) => b.len(),
        _ => 0,
    }
}

fn bytes_val<'a>(b: Vec<u8>) -> Value<'a> {
    Value::Bytes(Cow::Owned(b))
}

/// Named apart from `template/funcs.rs`'s `str_val` (issue #302 — the
/// census keys free functions by bare name). This one wraps an owned
/// `String`; that one wraps a `Vec<u8>`.
fn str_value_of<'a>(s: String) -> Value<'a> {
    Value::Str(Cow::Owned(s.into_bytes()))
}

fn time_method(name: &str) -> Option<&'static MethodSig> {
    use ParamTy::{BytesTy, Dur, Int, Loc, Str, Time};
    match name {
        // goodFunc rejects (plan v3 Δ2 — both texts container-captured).
        "Clock" => sig!(
            &[],
            MethodImpl::Reject("function Clock has 3 return values; should be 1 or 2")
        ),
        "Date" => sig!(
            &[],
            MethodImpl::Reject("function Date has 3 return values; should be 1 or 2")
        ),
        "ISOWeek" => sig!(
            &[],
            MethodImpl::Reject(
                "invalid function signature for ISOWeek: second return value should be error; is int"
            )
        ),
        "Zone" => sig!(
            &[],
            MethodImpl::Reject(
                "invalid function signature for Zone: second return value should be error; is int"
            )
        ),
        "ZoneBounds" => sig!(
            &[],
            MethodImpl::Reject(
                "invalid function signature for ZoneBounds: second return value should be error; is time.Time"
            )
        ),
        // The 38 callable methods.
        "Add" => sig!(
            &[Dur],
            MethodImpl::Call(|r, a, _e, _b| Ok(Value::Time(recv_time(r).add(arg_dur(a, 0)))))
        ),
        "AddDate" => sig!(
            &[Int, Int, Int],
            MethodImpl::Call(|r, a, e, _b| Ok(Value::Time(recv_time(r).add_date(
                e,
                arg_int(a, 0),
                arg_int(a, 1),
                arg_int(a, 2)
            ))))
        ),
        "After" => sig!(
            &[Time],
            MethodImpl::Call(|r, a, _e, _b| Ok(Value::Bool(recv_time(r).after(arg_time(a, 0)))))
        ),
        "AppendBinary" => sig!(
            &[BytesTy],
            MethodImpl::Call(|r, a, e, budget| {
                // The argument copy grows per call in an assignment
                // chain — charged (round 2).
                budget.charge(arg_bytes_len(a, 0) + 64)?;
                let mut b = arg_bytes(a, 0);
                b.extend_from_slice(&marshal_binary(recv_time(r), e)?);
                Ok(bytes_val(b))
            })
        ),
        "AppendFormat" => sig!(
            &[BytesTy, Str],
            MethodImpl::Call(|r, a, e, budget| {
                // Charged: the []byte argument copy (an `$b =
                // $t.AppendFormat $b <l>` chain GROWS per call) plus
                // the ≤10× layout expansion.
                let layout = match &a[1] {
                    Value::Str(s) => s.clone().into_owned(),
                    _ => Vec::new(),
                };
                budget.charge(
                    arg_bytes_len(a, 0).saturating_add(10usize.saturating_mul(layout.len())) + 64,
                )?;
                let mut b = arg_bytes(a, 0);
                b.extend_from_slice(&super::golayout::format_layout(recv_time(r), &layout, e));
                Ok(bytes_val(b))
            })
        ),
        "AppendText" => sig!(
            &[BytesTy],
            MethodImpl::Call(|r, a, e, budget| {
                budget.charge(arg_bytes_len(a, 0) + 64)?;
                let mut b = arg_bytes(a, 0);
                b.extend_from_slice(&marshal_text(recv_time(r), e)?);
                Ok(bytes_val(b))
            })
        ),
        "Before" => sig!(
            &[Time],
            MethodImpl::Call(|r, a, _e, _b| Ok(Value::Bool(recv_time(r).before(arg_time(a, 0)))))
        ),
        "Compare" => sig!(
            &[Time],
            MethodImpl::Call(|r, a, _e, _b| Ok(Value::Int(
                recv_time(r).compare(arg_time(a, 0)),
                IntKind::Int
            )))
        ),
        "Day" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(Value::Int(recv_time(r).date(e).2, IntKind::Int)))
        ),
        "Equal" => sig!(
            &[Time],
            MethodImpl::Call(|r, a, _e, _b| Ok(Value::Bool(recv_time(r).equal(arg_time(a, 0)))))
        ),
        "Format" => sig!(
            &[Str],
            MethodImpl::Call(|r, a, e, b| {
                // The layout is a template value expanding ≤ ~10×
                // (review round 2: `$a = $t.Format $a` compounds
                // without a charge).
                let layout = match &a[0] {
                    Value::Str(s) => s.clone().into_owned(),
                    _ => Vec::new(),
                };
                b.charge(10usize.saturating_mul(layout.len()) + 64)?;
                let out = super::golayout::format_layout(recv_time(r), &layout, e);
                Ok(Value::Str(Cow::Owned(out)))
            })
        ),
        "GoString" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(str_value_of(recv_time(r).go_string(e))))
        ),
        "GobEncode" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(bytes_val(marshal_binary(recv_time(r), e)?)))
        ),
        "Hour" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(Value::Int(recv_time(r).clock(e).0, IntKind::Int)))
        ),
        "In" => sig!(
            &[Loc],
            MethodImpl::Call(|r, a, _e, _b| match &a[0] {
                Value::Location(loc) => Ok(Value::Time(recv_time(r).in_loc(loc.clone()))),
                _ => Err("time: missing Location in call to Time.In".to_string()),
            })
        ),
        "IsDST" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(Value::Bool(timefns::is_dst(recv_time(r), e))))
        ),
        "IsZero" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::Bool(recv_time(r).is_zero())))
        ),
        "Local" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::Time(recv_time(r).local())))
        ),
        "Location" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::Location(recv_time(r).loc.clone())))
        ),
        "MarshalBinary" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(bytes_val(marshal_binary(recv_time(r), e)?)))
        ),
        "MarshalJSON" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| {
                let body = strict_rfc3339(recv_time(r), e)
                    .map_err(|err| format!("Time.MarshalJSON: {err}"))?;
                let mut out = vec![b'"'];
                out.extend_from_slice(&body);
                out.push(b'"');
                Ok(bytes_val(out))
            })
        ),
        "MarshalText" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(bytes_val(marshal_text(recv_time(r), e)?)))
        ),
        "Minute" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(Value::Int(recv_time(r).clock(e).1, IntKind::Int)))
        ),
        "Month" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(Value::Month(recv_time(r).date(e).1)))
        ),
        "Nanosecond" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::Int(
                recv_time(r).nsec as i64,
                IntKind::Int
            )))
        ),
        "Round" => sig!(
            &[Dur],
            MethodImpl::Call(|r, a, _e, _b| Ok(Value::Time(recv_time(r).round(arg_dur(a, 0)))))
        ),
        "Second" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(Value::Int(recv_time(r).clock(e).2, IntKind::Int)))
        ),
        "String" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(str_value_of(recv_time(r).string(e))))
        ),
        "Sub" => sig!(
            &[Time],
            MethodImpl::Call(|r, a, _e, _b| Ok(Value::Duration(recv_time(r).sub(arg_time(a, 0)))))
        ),
        "Truncate" => sig!(
            &[Dur],
            MethodImpl::Call(|r, a, _e, _b| Ok(Value::Time(recv_time(r).truncate(arg_dur(a, 0)))))
        ),
        "UTC" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::Time(recv_time(r).utc())))
        ),
        "Unix" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::int64(recv_time(r).unix())))
        ),
        "UnixMicro" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::int64(recv_time(r).unix_micro())))
        ),
        "UnixMilli" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::int64(recv_time(r).unix_milli())))
        ),
        "UnixNano" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::int64(recv_time(r).unix_nano())))
        ),
        "Weekday" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(Value::Weekday(recv_time(r).weekday(e))))
        ),
        "Year" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(Value::Int(recv_time(r).date(e).0, IntKind::Int)))
        ),
        "YearDay" => sig!(
            &[],
            MethodImpl::Call(|r, _a, e, _b| Ok(Value::Int(recv_time(r).year_day(e), IntKind::Int)))
        ),
        _ => None,
    }
}

fn duration_method(name: &str) -> Option<&'static MethodSig> {
    use ParamTy::Dur;
    match name {
        "String" => sig!(
            &[],
            MethodImpl::Call(
                |r, _a, _e, _b| Ok(str_value_of(timefns::duration_string(recv_dur(r))))
            )
        ),
        "Nanoseconds" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::int64(recv_dur(r))))
        ),
        "Microseconds" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::int64(recv_dur(r) / 1_000)))
        ),
        "Milliseconds" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| Ok(Value::int64(recv_dur(r) / 1_000_000)))
        ),
        "Seconds" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| {
                let d = recv_dur(r);
                let sec = d / 1_000_000_000;
                let nsec = d % 1_000_000_000;
                Ok(Value::Float(sec as f64 + nsec as f64 / 1e9))
            })
        ),
        "Minutes" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| {
                let d = recv_dur(r);
                let min = d / 60_000_000_000;
                let nsec = d % 60_000_000_000;
                Ok(Value::Float(min as f64 + nsec as f64 / (60.0 * 1e9)))
            })
        ),
        "Hours" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| {
                let d = recv_dur(r);
                let hour = d / 3_600_000_000_000;
                let nsec = d % 3_600_000_000_000;
                Ok(Value::Float(
                    hour as f64 + nsec as f64 / (60.0 * 60.0 * 1e9),
                ))
            })
        ),
        "Truncate" => sig!(
            &[Dur],
            MethodImpl::Call(|r, a, _e, _b| {
                let (d, m) = (recv_dur(r), arg_dur(a, 0));
                Ok(Value::Duration(if m <= 0 { d } else { d - d % m }))
            })
        ),
        "Round" => sig!(
            &[Dur],
            MethodImpl::Call(|r, a, _e, _b| {
                let (d, m) = (recv_dur(r), arg_dur(a, 0));
                Ok(Value::Duration(duration_round(d, m)))
            })
        ),
        "Abs" => sig!(
            &[],
            MethodImpl::Call(|r, _a, _e, _b| {
                let d = recv_dur(r);
                Ok(Value::Duration(if d >= 0 {
                    d
                } else if d == i64::MIN {
                    i64::MAX
                } else {
                    -d
                }))
            })
        ),
        _ => None,
    }
}

fn duration_round(d: i64, m: i64) -> i64 {
    fn less_than_half(x: i64, y: i64) -> bool {
        (x as u64).wrapping_add(x as u64) < y as u64
    }
    if m <= 0 {
        return d;
    }
    let mut r = d % m;
    if d < 0 {
        r = -r;
        if less_than_half(r, m) {
            return d + r;
        }
        let d1 = d - m + r;
        if d1 < d {
            return d1;
        }
        return i64::MIN;
    }
    if less_than_half(r, m) {
        return d - r;
    }
    let d1 = d + m - r;
    if d1 > d {
        return d1;
    }
    i64::MAX
}

fn named_int_string<'a>(
    receiver: &Value<'a>,
    _args: &[Value<'a>],
    _env: &TemplateEnv,
    _budget: &super::RenderBudget,
) -> Result<Value<'a>, String> {
    Ok(match receiver {
        Value::Month(m) => str_value_of(timefns::month_string(*m)),
        Value::Weekday(w) => str_value_of(timefns::weekday_string(*w)),
        _ => unreachable!("String method dispatched on a non-Month/Weekday receiver"),
    })
}

fn location_string<'a>(
    receiver: &Value<'a>,
    _args: &[Value<'a>],
    env: &TemplateEnv,
    _budget: &super::RenderBudget,
) -> Result<Value<'a>, String> {
    match receiver {
        Value::Location(loc) => Ok(str_value_of(timefns::location_name(loc, env))),
        _ => unreachable!("String method dispatched on a non-Location receiver"),
    }
}

/// `Time.MarshalBinary` (`time.go`): version 1/2 layout over the
/// INTERNAL seconds.
fn marshal_binary(t: &GoTime, env: &TemplateEnv) -> Result<Vec<u8>, String> {
    let (offset_min, offset_sec, version): (i16, i8, u8) = if matches!(t.loc, GoLoc::Utc) {
        (-1, 0, 1)
    } else {
        let (_, offset) = t.zone(env);
        let mut version = 1u8;
        let mut offset_sec = 0i8;
        if offset % 60 != 0 {
            version = 2;
            offset_sec = (offset % 60) as i8;
        }
        let offset_min64 = offset / 60;
        if !(-32768..=32767).contains(&offset_min64) || offset_min64 == -1 {
            return Err("Time.MarshalBinary: unexpected zone offset".to_string());
        }
        (offset_min64 as i16, offset_sec, version)
    };
    let (_, ext) = t.internal_repr();
    let sec = ext;
    let nsec = t.nsec;
    let mut enc = vec![
        version,
        (sec >> 56) as u8,
        (sec >> 48) as u8,
        (sec >> 40) as u8,
        (sec >> 32) as u8,
        (sec >> 24) as u8,
        (sec >> 16) as u8,
        (sec >> 8) as u8,
        sec as u8,
        (nsec >> 24) as u8,
        (nsec >> 16) as u8,
        (nsec >> 8) as u8,
        nsec as u8,
        ((offset_min as u16) >> 8) as u8,
        offset_min as u8,
    ];
    if version == 2 {
        enc.push(offset_sec as u8);
    }
    Ok(enc)
}

/// `Time.MarshalText` — `appendStrictRFC3339` with the Go error texts.
fn marshal_text(t: &GoTime, env: &TemplateEnv) -> Result<Vec<u8>, String> {
    strict_rfc3339(t, env).map_err(|e| format!("Time.MarshalText: {e}"))
}

fn strict_rfc3339(t: &GoTime, env: &TemplateEnv) -> Result<Vec<u8>, String> {
    let year = t.date(env).0;
    if !(0..=9999).contains(&year) {
        return Err("year outside of range [0,9999]".to_string());
    }
    let (_, offset) = t.zone(env);
    if offset % 60 != 0 {
        return Err("zone offset has fractional minute".to_string());
    }
    Ok(super::golayout::format_layout(
        t,
        b"2006-01-02T15:04:05.999999999Z07:00",
        env,
    ))
}

/// `%d`-struct-dump support: expose the gofmt module's needs (unused
/// helper suppression).
#[allow(unused)]
fn _quote(s: &str) -> String {
    gofmt::quote_bytes(s.as_bytes(), '"')
}
