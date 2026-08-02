//! Issue #230 review rounds 2–8: the RUNTIME allocation gate over the
//! whole template function registry — **the dominance and ordering
//! evidence** for charge-before-allocate (the AST census is a drift
//! tripwire only; dominance is a control-flow property no syntactic
//! walk can establish — round-3 adjudication).
//!
//! Every one of the 67 registry functions is invoked through its
//! BRANCH SHAPES — the happy path, an all-empty-inputs shape, and the
//! per-function identity / no-match / `n == 0` / error shapes (round 3:
//! `go_replace`'s identity early-returns passed a single-shape gate
//! while copying before their charge) — under a byte-counting
//! allocator and one fresh `RenderBudget`, and:
//!
//! - a **string/container** result (`Str`/`Bytes`/`List`/`Map` — bytes a
//!   template variable can RETAIN and feed back through
//!   `{{ $a = f $a }}` chains) must have its allocated bytes dominated
//!   by its budget charges: `alloc ≤ 4×charged + 64 KiB`. The factor 4
//!   absorbs `Vec` growth-doubling churn (cumulative allocation across
//!   reallocs ≤ ~2× the final size) — a genuinely uncharged copy of a
//!   1 MiB input fails by an order of magnitude;
//! - a **scalar** result (int/float/bool/time/…) may use transient
//!   parse scratch up to `4×input + 64 KiB` uncharged: nothing is
//!   retained past the call, so repetition cannot grow memory (the
//!   census's `TRANSIENT` disposition);
//! - an **error** result may allocate one bounded error rendering
//!   (`≤ 32×input + 128 KiB`): every function error aborts the render,
//!   so error paths run at most once per render (`ERROR_PATH`).
//!
//! Retainable shapes that allocate big additionally run an **ORDERING
//! leg** under a near-exhausted budget: correct charge-BEFORE-allocate
//! breaches at the charge and returns before copying, so allocation
//! stays under a small constant; a charge moved after its allocation
//! leaves the full copy on the counter (mutation-verified: relocating
//! `f!lower`'s charge after `go_to_lower` fails exactly this leg while
//! passing post-hoc dominance).
//!
//! **Round 7 — the declared partition.** Ordering coverage is no longer
//! a hand-maintained subset (nor the tautological `registered ==
//! executed`, whose two sides one loop populated): EVERY shape now
//! carries a declared [`ShapeClass`] — `OrderingRequired` /
//! `DominanceOnly(reason)` / `ScalarError` / `HarnessBlind(reason)` —
//! at its construction site.
//! Omitting a class is a missing-field COMPILE error; a registry
//! function absent from the [`Func`] partition fails the runtime
//! domain bridge; a `Func` variant without a `spec()`/`name()` arm is
//! a non-exhaustive-match COMPILE error; deleting an entry from
//! `Func::ALL` fails to compile whenever this test target is compiled
//! (the declared array length), while
//! ADDING a variant omitted from `ALL` is caught as `dead_code` only
//! under `-D warnings` — CI-enforced, not plain-build-enforced. The
//! gate then asserts
//! `required == reached`, where `required` derives ONLY from the
//! declarations and `reached` ONLY from runtime observation — two
//! independently-sourced sets that can disagree in both directions: a
//! shape declared `OrderingRequired` that errors or shrinks upstream
//! FAILS (it never reached its leg), and a shape that reaches the leg
//! while declared exempt FAILS (its declaration is stale).
//!
//! **Round 8 — the declared exception.** Exactly two shapes,
//! `regexReplaceAll/invalid-pattern` and
//! `regexReplaceAllLiteral/invalid-pattern`, are declared
//! [`ShapeClass::HarnessBlind`]: their branch CAN cross the ordering
//! trigger, but it allocates and then returns `Err`, and the ordering
//! leg re-runs only shapes returning a retainable `Ok` — so no probe
//! size reaches it. That is a limit of this instrument, not of the
//! branch, so it gets its own state instead of a `DominanceOnly`
//! reason that would assert something false. Charge ordering on
//! allocate-then-error branches is filed as #294.
//!
//! This is the gate that catches the fifth amplification class the
//! round-1 census could not judge: an existing site labelled
//! "input-bounded" whose per-call copy is repeatable (or compoundable)
//! inside a `range`/variable-only body that emits no text.
//!
//! Single `#[test]`, own binary: the counting allocator is
//! process-global (the alloc-gate flake rule: byte ceilings, never
//! exact counts).

use std::alloc::{GlobalAlloc, Layout, System};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

struct CountingAlloc;

static BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: delegates verbatim to the system allocator; the only side
// effect is a relaxed atomic add, which allocates nothing and cannot
// re-enter the allocator.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

use pulsus_read::logql::template::RenderBudget;
use pulsus_read::logql::template::funcs::{FuncCtx, ParamTy, REGISTRY};
use pulsus_read::logql::template::gofmt::PrintEnv;
use pulsus_read::logql::template::timefns::{GoTime, TemplateEnv};
use pulsus_read::logql::template::value::Value;

struct GateEnv {
    env: TemplateEnv,
    budget: RenderBudget,
}

impl PrintEnv for GateEnv {
    fn env(&self) -> &TemplateEnv {
        &self.env
    }
    fn label_pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        Vec::new()
    }
    fn budget(&self) -> &RenderBudget {
        &self.budget
    }
}

const BIG: usize = 1 << 20; // 1 MiB adversarial input
const SLACK: u64 = 64 * 1024;

fn big_str() -> Value<'static> {
    Value::Str(Cow::Owned(vec![b'x'; BIG]))
}

fn s(text: &str) -> Value<'static> {
    Value::Str(Cow::Owned(text.as_bytes().to_vec()))
}

/// A 1 MiB invalid-UTF-8 string (alternate `0xFF`).
fn invalid_big_str() -> Value<'static> {
    let mut v = vec![b'x'; BIG];
    for i in (0..v.len()).step_by(2) {
        v[i] = 0xFF;
    }
    Value::Str(Cow::Owned(v))
}

/// A 128-byte invalid-UTF-8 PATTERN — enough to exercise
/// `compile_regex`'s invalid-pattern conversion (the round-6 finding-2
/// gap: derived variants only invalidate BIG args, and patterns were
/// tiny), sized to stay inside the charged 1 MiB program ceiling.
///
/// It is deliberately NOT larger: measured on this toolchain, regex
/// COMPILATION allocates ~630x the pattern length and
/// `RegexBuilder::size_limit` does not bound it (a 16 KiB literal
/// pattern allocates 10.4 MB and still reports Ok under a 1 MiB
/// limit) — an amplification path INDEPENDENT of invalid UTF-8 (valid
/// patterns behave identically), filed separately as #291 rather than
/// papered over here. This shape is scoped to the conversion it was
/// added for.
fn invalid_pattern() -> Value<'static> {
    let mut v = vec![b'x'; 128];
    for i in (0..v.len()).step_by(2) {
        v[i] = 0xFF;
    }
    Value::Str(Cow::Owned(v))
}

fn default_arg(ty: ParamTy) -> Value<'static> {
    match ty {
        ParamTy::Any | ParamTy::Str => big_str(),
        ParamTy::Int => Value::int(1000),
        ParamTy::Float => Value::Float(1.5),
        ParamTy::Time => Value::Time(GoTime::from_unix(1_700_000_000, 0)),
        ParamTy::Dur => Value::Duration(1_500_000_000),
        ParamTy::Loc => Value::Nil,
        ParamTy::BytesTy => Value::Bytes(Cow::Owned(vec![b'x'; 4096])),
    }
}

/// Per-function argument overrides steering the call onto its happy
/// SUCCESS path with the largest reachable output (the dominance bound
/// bites on success; error paths get the looser once-per-render bound).
fn args_for(name: &str, params: &[ParamTy], variadic: Option<ParamTy>) -> Vec<Value<'static>> {
    let digits_big = || {
        let mut d = String::from("1");
        while d.len() < BIG {
            d.push('2');
        }
        Value::Str(Cow::Owned(d.into_bytes()))
    };
    match name {
        "repeat" => return vec![Value::int(4), big_str()],
        // Trim family: a cutset/affix that matches NOTHING so the
        // output copy stays input-sized (a full-input probe of the
        // copy, not a trivially-empty result).
        "Trim" | "TrimLeft" | "TrimRight" => return vec![big_str(), s("z")],
        "trimAll" => return vec![s("z"), big_str()],
        "TrimPrefix" | "TrimSuffix" => return vec![big_str(), s("zz")],
        "trimPrefix" | "trimSuffix" => return vec![s("zz"), big_str()],
        "contains" | "hasPrefix" | "hasSuffix" => return vec![s("zz"), big_str()],
        "alignLeft" | "alignRight" => return vec![Value::int(2_000_000), big_str()],
        "indent" | "nindent" => return vec![Value::int(200_000), s("a\nb\nc")],
        "regexReplaceAll" | "regexReplaceAllLiteral" => {
            return vec![s("x+"), big_str(), s("YY")];
        }
        "count" => return vec![s("x+"), big_str()],
        "Replace" => return vec![big_str(), s("x"), s("yy"), Value::int(-1)],
        "replace" => return vec![s("x"), s("yy"), big_str()],
        "b64dec" => {
            // Valid base64 of a large payload.
            use base64::Engine as _;
            let enc = base64::engine::general_purpose::STANDARD.encode(vec![b'q'; BIG]);
            return vec![Value::Str(Cow::Owned(enc.into_bytes()))];
        }
        "urldecode" => return vec![Value::Str(Cow::Owned(b"%41".repeat(BIG / 4)))],
        "bytes" => return vec![s("17MB")],
        "duration" | "duration_seconds" => return vec![s("300ms")],
        "unixToTime" => return vec![s("1700000000")],
        "toDate" => return vec![s("2006-01-02"), s("2024-05-06")],
        "toDateInZone" => return vec![s("2006-01-02"), s("UTC"), s("2024-05-06")],
        "substr" => return vec![Value::int(0), Value::int(500_000), big_str()],
        "trunc" => return vec![Value::int(500_000), big_str()],
        "int" | "float64" => return vec![digits_big()],
        "round" => return vec![Value::Float(1.234_567), Value::int(2)],
        "fromJson" => {
            // Worst structural density: one element per two input bytes.
            let mut json = String::with_capacity(BIG / 2);
            json.push('[');
            json.push('1');
            while json.len() < BIG / 4 {
                json.push_str(",1");
            }
            json.push(']');
            return vec![Value::Str(Cow::Owned(json.into_bytes()))];
        }
        _ => {}
    }
    let mut args: Vec<Value<'static>> = params.iter().map(|&p| default_arg(p)).collect();
    if let Some(v) = variadic {
        args.push(default_arg(v));
    }
    args
}

/// The all-empty-inputs shape (zero-length strings, zero ints).
fn empty_args(params: &[ParamTy], variadic: Option<ParamTy>) -> Vec<Value<'static>> {
    let empty = |ty: ParamTy| match ty {
        ParamTy::Any | ParamTy::Str => s(""),
        ParamTy::Int => Value::int(0),
        ParamTy::Float => Value::Float(0.0),
        ParamTy::Time => Value::Time(GoTime::from_unix(0, 0)),
        ParamTy::Dur => Value::Duration(0),
        ParamTy::Loc => Value::Nil,
        ParamTy::BytesTy => Value::Bytes(Cow::Owned(Vec::new())),
    };
    let mut args: Vec<Value<'static>> = params.iter().map(|&p| empty(p)).collect();
    if let Some(v) = variadic {
        args.push(empty(v));
    }
    args
}

/// The invalid-UTF-8 variant of a shape, DERIVED mechanically rather
/// than hand-enumerated (round 4: the regex functions' `lossy(...)`
/// conversion allocated a ≤3× owned buffer before its charge, and the
/// shape set had no invalid-byte member to see it): every big string
/// argument gets alternate `0xFF` bytes, forcing every
/// `String::from_utf8_lossy` on the call path onto its `Cow::Owned`
/// branch. Applied to EVERY shape of EVERY function, so any future
/// conversion of caller bytes inherits coverage without being named.
/// Whether a variant derives (and its class) is DECLARED per shape via
/// [`Utf8Variant`] and cross-checked against this function's actual
/// outcome — a mismatch in either direction fails the gate.
fn invalidate_utf8(args: &[Value<'static>]) -> Option<Vec<Value<'static>>> {
    let mut changed = false;
    let out = args
        .iter()
        .map(|v| match v {
            Value::Str(b) if b.len() >= 4096 => {
                changed = true;
                let mut nb = b.clone().into_owned();
                for i in (0..nb.len()).step_by(2) {
                    nb[i] = 0xFF;
                }
                Value::Str(Cow::Owned(nb))
            }
            other => other.clone(),
        })
        .collect();
    changed.then_some(out)
}

fn input_bytes(args: &[Value<'_>], line_len: usize) -> u64 {
    let mut total = line_len as u64;
    for a in args {
        if let Value::Str(b) | Value::Bytes(b) = a {
            total += b.len() as u64;
        }
    }
    total
}

fn is_retainable(v: &Value<'_>) -> bool {
    matches!(
        v,
        Value::Str(_) | Value::Bytes(_) | Value::List(_) | Value::Map(_)
    )
}

/// The near-exhausted budget the ORDERING leg runs under: whatever a
/// function allocates before its first (breaching) charge is visible as
/// allocated bytes with almost no budget left.
const TINY_REMAINING: u64 = 64 * 1024;
/// The ordering leg's allocation ceiling — far below the ≥256 KiB the
/// same shape allocated under a full budget, so a copy made BEFORE the
/// charge fails by an order of magnitude.
const ORDERING_CEILING: u64 = 192 * 1024;

// ===================== the declared partition (round 7) =====================

/// The declared evidence class of ONE shape — the partition side of
/// `required == reached`. Every shape carries one at its construction
/// site (a missing class is a missing-field compile error), so the set
/// of ordering-required shapes derives from declarations while the
/// reached set derives from runtime — independent sources that can
/// disagree, unlike the round-6 `registered == executed` whose two
/// sides one loop populated.
#[derive(Clone, Copy, Debug)]
enum ShapeClass {
    /// Retainable result whose success-path allocation meets the
    /// ordering trigger (≥ 4×SLACK): the shape MUST reach the
    /// charge-before-allocate leg; its absence from `reached` fails.
    OrderingRequired,
    /// Retainable result that cannot meet the ordering trigger, with
    /// the measured reason. Its PRESENCE in `reached` fails (the
    /// declaration is stale — promote it to `OrderingRequired`).
    DominanceOnly(&'static str),
    /// Scalar or error result: no retainable bytes exist, so the
    /// ordering leg does not apply by construction — verified, not
    /// trusted (a retainable `Ok` from such a shape fails the gate).
    ScalarError,
    /// **Declared exception (round 8).** The branch CAN cross the
    /// ordering trigger, and THIS HARNESS cannot observe it — so
    /// neither `OrderingRequired` (the shape can never reach the leg)
    /// nor `DominanceOnly` (whose reason asserts a branch limit that
    /// does not exist here) is a true statement about it.
    ///
    /// Deliberately a separate state rather than a carefully-worded
    /// `DominanceOnly`: the two are different KINDS of claim — one is
    /// about the branch, this one is about the instrument — and the
    /// reason-header rule below binds `DominanceOnly` reasons to branch
    /// relations. The reason string must name the tracking issue for
    /// the harness gap.
    ///
    /// Excluded from `required` (the leg is unreachable for it), but
    /// NOT trusted in the other direction: if such a shape ever DOES
    /// reach the leg, the harness was not blind and the declaration
    /// fails as stale.
    HarnessBlind(&'static str),
}
use ShapeClass::{DominanceOnly, HarnessBlind, OrderingRequired, ScalarError};

/// Whether [`invalidate_utf8`] derives a `+invalid-utf8` variant from
/// a shape (it does iff some `Str` argument is ≥ 4 KiB) and the
/// variant's OWN declared class — an invalid-byte variant can change
/// class (`urldecode/happy` is `OrderingRequired`, but its variant
/// errors at the first corrupted escape; `fromJson`'s parses to `Nil`).
/// Cross-checked against the mechanical derivation's actual outcome,
/// both directions.
#[derive(Clone, Copy, Debug)]
enum Utf8Variant {
    Derived(ShapeClass),
    NotDerived,
}
use Utf8Variant::{Derived, NotDerived};

#[derive(Clone, Copy, Debug)]
struct ShapeDecl {
    class: ShapeClass,
    utf8: Utf8Variant,
}

/// Shorthand: `d(class, utf8_variant)`.
const fn d(class: ShapeClass, utf8: Utf8Variant) -> ShapeDecl {
    ShapeDecl { class, utf8 }
}

struct ExtraShape {
    name: &'static str,
    decl: ShapeDecl,
    args: Vec<Value<'static>>,
}

struct FnSpec {
    happy: ShapeDecl,
    empty: ShapeDecl,
    extra: Vec<ExtraShape>,
}

/// `DominanceOnly` reasons. Round-7 review rule: a reason must state a
/// limit of the BRANCH, never of the arguments the probe happens to
/// pass — a branch whose output size is a free argument (a count, a
/// tail length, an affix remainder) can cross the 256 KiB ordering
/// trigger and MUST be probed at ≥ trigger size as `OrderingRequired`
/// (`align*/truncate` and `trunc/negative-tail` were fitted to
/// 200,000-byte probes and hid exactly that).
///
/// A branch that can cross the trigger but which this harness cannot
/// observe has NO such reason to state and does not belong here: it is
/// [`ShapeClass::HarnessBlind`], whose reasons live below.
const R_EMPTY: &str = "the shape is DEFINITIONALLY the all-zero-length-inputs probe: its \
     output is empty/constant-size at the only arguments that make it \
     this shape, far below the 256 KiB ordering trigger";
const R_SMALL: &str = "this shape's output is pinned EMPTY by its defining argument \
     RELATION (cutset covers every rune / the affix is the whole input / \
     n == 0) at ANY input size — a branch limit, not a fitted probe \
     magnitude; the same production's at-size charge ordering is covered \
     by the function's other shapes";
const R_CONST: &str = "constant-size decimal rendering of a timestamp, \
     orders of magnitude below the ordering trigger";
/// [`ShapeClass::HarnessBlind`] reasons — the ONLY one, round 8.
///
/// `compile_regex`'s invalid-UTF-8 repair arm is NOT branch-limited:
/// a large enough invalid pattern repairs to well past the 256 KiB
/// trigger, and only then does the compiled program exceed the 1 MiB
/// `size_limit`. Its round-7 `DominanceOnly` reason asserted the
/// opposite and was wrong: what stops the coverage is the instrument,
/// not the branch. The ordering leg re-runs only shapes that returned a
/// RETAINABLE `Ok`, so a branch that allocates and then errors cannot
/// enter it at ANY probe size — enlarging this shape's pattern would
/// move it from "too small to trigger" to "errors before the rerun",
/// never into the leg.
///
/// The `invalid-repl` / `+invalid-utf8` haystack shapes do NOT cover
/// this site: they charge through `lossy_charged`, a different call
/// site from the pattern conversion's `lossy_repaired*`.
///
/// Ordering coverage for allocate-then-error branches needs a
/// different instrument (bounded transient, not charge-before-allocate
/// on retained bytes) and is filed as **#294**, which carries the
/// acceptance criteria; **#291** (regex compilation allocating ~630x
/// the pattern) is the worst case already on file. This is a declared
/// exception, not a covered shape.
const HB_INVALID_PATTERN: &str = "the invalid-PATTERN repair CAN cross the 256 KiB ordering trigger \
     (a large invalid pattern repairs to ~2x its length before the compiled \
     program hits the 1 MiB size_limit), so no branch limit exempts it — but \
     the branch allocates and then returns Err, and the ordering leg re-runs \
     only shapes returning a retainable Ok, so this harness cannot observe it \
     at ANY probe size; allocate-then-error charge ordering is filed as #294 \
     (#291 is the worst case on file). The invalid-repl shapes cover a \
     DIFFERENT charge site (lossy_charged), not this one";

/// The partition's DOMAIN: one variant per registry function.
///
/// - a registry function with no variant here (or vice versa) fails
///   the runtime domain bridge with the set difference;
/// - a variant without a [`Func::name`]/[`Func::spec`] arm fails to
///   COMPILE (exhaustive matches, no wildcard);
/// - DELETING an entry from [`Func::ALL`] fails to COMPILE whenever
///   this test target is compiled: the declared `[Func; 67]` length no
///   longer matches (E0308). This file is an INTEGRATION TEST, so a
///   plain `cargo build`/`cargo check -p pulsus-read` does not build
///   it and exits 0 (measured); the error appears under
///   `cargo test --test logql_template_alloc_gate --no-run`, under
///   `cargo check/clippy --all-targets`, and hence in CI;
/// - ADDING a variant (with `name()`/`spec()` arms) while omitting it
///   from [`Func::ALL`] compiles even when this target IS built — the
///   declared array length is unchanged — and is caught only as
///   `dead_code` (never-constructed variant), a hard error solely
///   under `-D warnings`: CI-enforced (`.github/workflows/ci.yml` sets
///   `RUSTFLAGS: "-D warnings"`, as does the clippy gate), NOT
///   build-enforced.
///
/// Both legs verified by mutation (round 7; the deletion leg's
/// build-command scope re-measured in round 8).
///
/// Case-colliding sprig names carry a `Sprig` suffix (`Trim`/`trim`,
/// `Replace`/`replace`, `TrimPrefix`/`trimPrefix`,
/// `TrimSuffix`/`trimSuffix`); `name()` pins each mapping.
#[derive(Clone, Copy, Debug)]
enum Func {
    ToLower,
    ToUpper,
    Replace,
    Trim,
    TrimLeft,
    TrimRight,
    TrimPrefix,
    TrimSuffix,
    TrimSpace,
    RegexReplaceAll,
    RegexReplaceAllLiteral,
    Count,
    Urldecode,
    Urlencode,
    BytesFn,
    Duration,
    DurationSeconds,
    UnixEpochMillis,
    UnixEpochNanos,
    ToDateInZone,
    UnixToTime,
    AlignLeft,
    AlignRight,
    LineFn,
    TimestampFn,
    B64enc,
    B64dec,
    Lower,
    Upper,
    Title,
    Trunc,
    Substr,
    Contains,
    HasPrefix,
    HasSuffix,
    Indent,
    Nindent,
    ReplaceSprig,
    Repeat,
    TrimSprig,
    TrimAll,
    TrimSuffixSprig,
    TrimPrefixSprig,
    Int,
    Float64,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Addf,
    Subf,
    Mulf,
    Divf,
    Max,
    Min,
    Maxf,
    Minf,
    Ceil,
    Floor,
    Round,
    FromJson,
    Date,
    ToDate,
    Now,
    UnixEpoch,
    DefaultFn,
}

impl Func {
    const ALL: [Func; 67] = [
        Func::ToLower,
        Func::ToUpper,
        Func::Replace,
        Func::Trim,
        Func::TrimLeft,
        Func::TrimRight,
        Func::TrimPrefix,
        Func::TrimSuffix,
        Func::TrimSpace,
        Func::RegexReplaceAll,
        Func::RegexReplaceAllLiteral,
        Func::Count,
        Func::Urldecode,
        Func::Urlencode,
        Func::BytesFn,
        Func::Duration,
        Func::DurationSeconds,
        Func::UnixEpochMillis,
        Func::UnixEpochNanos,
        Func::ToDateInZone,
        Func::UnixToTime,
        Func::AlignLeft,
        Func::AlignRight,
        Func::LineFn,
        Func::TimestampFn,
        Func::B64enc,
        Func::B64dec,
        Func::Lower,
        Func::Upper,
        Func::Title,
        Func::Trunc,
        Func::Substr,
        Func::Contains,
        Func::HasPrefix,
        Func::HasSuffix,
        Func::Indent,
        Func::Nindent,
        Func::ReplaceSprig,
        Func::Repeat,
        Func::TrimSprig,
        Func::TrimAll,
        Func::TrimSuffixSprig,
        Func::TrimPrefixSprig,
        Func::Int,
        Func::Float64,
        Func::Add,
        Func::Sub,
        Func::Mul,
        Func::Div,
        Func::Mod,
        Func::Addf,
        Func::Subf,
        Func::Mulf,
        Func::Divf,
        Func::Max,
        Func::Min,
        Func::Maxf,
        Func::Minf,
        Func::Ceil,
        Func::Floor,
        Func::Round,
        Func::FromJson,
        Func::Date,
        Func::ToDate,
        Func::Now,
        Func::UnixEpoch,
        Func::DefaultFn,
    ];

    fn name(self) -> &'static str {
        match self {
            Func::ToLower => "ToLower",
            Func::ToUpper => "ToUpper",
            Func::Replace => "Replace",
            Func::Trim => "Trim",
            Func::TrimLeft => "TrimLeft",
            Func::TrimRight => "TrimRight",
            Func::TrimPrefix => "TrimPrefix",
            Func::TrimSuffix => "TrimSuffix",
            Func::TrimSpace => "TrimSpace",
            Func::RegexReplaceAll => "regexReplaceAll",
            Func::RegexReplaceAllLiteral => "regexReplaceAllLiteral",
            Func::Count => "count",
            Func::Urldecode => "urldecode",
            Func::Urlencode => "urlencode",
            Func::BytesFn => "bytes",
            Func::Duration => "duration",
            Func::DurationSeconds => "duration_seconds",
            Func::UnixEpochMillis => "unixEpochMillis",
            Func::UnixEpochNanos => "unixEpochNanos",
            Func::ToDateInZone => "toDateInZone",
            Func::UnixToTime => "unixToTime",
            Func::AlignLeft => "alignLeft",
            Func::AlignRight => "alignRight",
            Func::LineFn => "__line__",
            Func::TimestampFn => "__timestamp__",
            Func::B64enc => "b64enc",
            Func::B64dec => "b64dec",
            Func::Lower => "lower",
            Func::Upper => "upper",
            Func::Title => "title",
            Func::Trunc => "trunc",
            Func::Substr => "substr",
            Func::Contains => "contains",
            Func::HasPrefix => "hasPrefix",
            Func::HasSuffix => "hasSuffix",
            Func::Indent => "indent",
            Func::Nindent => "nindent",
            Func::ReplaceSprig => "replace",
            Func::Repeat => "repeat",
            Func::TrimSprig => "trim",
            Func::TrimAll => "trimAll",
            Func::TrimSuffixSprig => "trimSuffix",
            Func::TrimPrefixSprig => "trimPrefix",
            Func::Int => "int",
            Func::Float64 => "float64",
            Func::Add => "add",
            Func::Sub => "sub",
            Func::Mul => "mul",
            Func::Div => "div",
            Func::Mod => "mod",
            Func::Addf => "addf",
            Func::Subf => "subf",
            Func::Mulf => "mulf",
            Func::Divf => "divf",
            Func::Max => "max",
            Func::Min => "min",
            Func::Maxf => "maxf",
            Func::Minf => "minf",
            Func::Ceil => "ceil",
            Func::Floor => "floor",
            Func::Round => "round",
            Func::FromJson => "fromJson",
            Func::Date => "date",
            Func::ToDate => "toDate",
            Func::Now => "now",
            Func::UnixEpoch => "unixEpoch",
            Func::DefaultFn => "default",
        }
    }

    /// The exhaustive per-function shape declarations: the happy and
    /// empty shapes' classes, plus every extra branch shape with its
    /// arguments (review round 3: identity / no-match / `n == 0` /
    /// error branches each probed separately). Adding a `Func` variant
    /// without an arm here does not compile; adding a shape without a
    /// class does not compile.
    fn spec(self) -> FnSpec {
        // The two commonest declarations.
        let or_d = d(OrderingRequired, Derived(OrderingRequired));
        let empty_do = d(DominanceOnly(R_EMPTY), NotDerived);
        match self {
            // Big string in, big (charged) string out; the invalid-byte
            // variant still yields a big retainable string.
            Func::ToLower
            | Func::ToUpper
            | Func::TrimSpace
            | Func::TrimSprig
            | Func::Lower
            | Func::Upper
            | Func::Title
            | Func::B64enc
            | Func::Urlencode
            | Func::Date => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![],
            },
            // Cutset trims: 'x' strips every valid byte (empty output),
            // but the derived variant's 0xFF bytes survive the cutset,
            // so IT carries a big output to the ordering leg.
            Func::Trim | Func::TrimLeft | Func::TrimRight => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![ExtraShape {
                    name: "all-trimmed",
                    decl: d(DominanceOnly(R_SMALL), Derived(OrderingRequired)),
                    args: vec![big_str(), s("x")],
                }],
            },
            Func::TrimAll => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![ExtraShape {
                    name: "all-trimmed",
                    decl: d(DominanceOnly(R_SMALL), Derived(OrderingRequired)),
                    args: vec![s("x"), big_str()],
                }],
            },
            // Affix strips. Two matched-arm probes (round-7 audit): the
            // whole-input affix pins the output EMPTY by its defining
            // relation (and stays empty in the derived variant — both
            // arguments are invalidated identically, the affix still
            // matches the whole input); but the matched arm's output is
            // input − affix, a FREE size that probe pins to zero, so a
            // small matching affix probes the same strip at ~1 MiB. Its
            // derived variant reaches via the unmatched identity arm
            // instead (the 0xFF-corrupted input no longer starts/ends
            // with the valid-`x` affix) — still a ≥trigger retainable
            // copy.
            Func::TrimPrefix | Func::TrimSuffix => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![
                    ExtraShape {
                        name: "affix-matches-whole",
                        decl: d(DominanceOnly(R_SMALL), Derived(DominanceOnly(R_SMALL))),
                        args: vec![big_str(), big_str()],
                    },
                    ExtraShape {
                        name: "affix-strips-at-size",
                        decl: or_d,
                        args: vec![big_str(), s("xx")],
                    },
                ],
            },
            Func::TrimPrefixSprig | Func::TrimSuffixSprig => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![
                    ExtraShape {
                        name: "affix-matches-whole",
                        decl: d(DominanceOnly(R_SMALL), Derived(DominanceOnly(R_SMALL))),
                        args: vec![big_str(), big_str()],
                    },
                    ExtraShape {
                        name: "affix-strips-at-size",
                        decl: or_d,
                        args: vec![s("xx"), big_str()],
                    },
                ],
            },
            Func::Replace => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![
                    ExtraShape {
                        name: "identity-old==new",
                        decl: or_d,
                        args: vec![big_str(), s("x"), s("x"), Value::int(-1)],
                    },
                    ExtraShape {
                        name: "no-match",
                        decl: or_d,
                        args: vec![big_str(), s("ZZZ"), s("y"), Value::int(-1)],
                    },
                    ExtraShape {
                        name: "n==0",
                        decl: or_d,
                        args: vec![big_str(), s("x"), s("y"), Value::int(0)],
                    },
                    ExtraShape {
                        name: "empty-needle",
                        decl: or_d,
                        args: vec![big_str(), s(""), s("-"), Value::int(-1)],
                    },
                ],
            },
            Func::ReplaceSprig => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![
                    ExtraShape {
                        name: "identity-old==new",
                        decl: or_d,
                        args: vec![s("x"), s("x"), big_str()],
                    },
                    ExtraShape {
                        name: "no-match",
                        decl: or_d,
                        args: vec![s("ZZZ"), s("y"), big_str()],
                    },
                    ExtraShape {
                        name: "empty-needle",
                        decl: or_d,
                        args: vec![s(""), s("-"), big_str()],
                    },
                ],
            },
            Func::RegexReplaceAll | Func::RegexReplaceAllLiteral => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![
                    ExtraShape {
                        name: "no-match",
                        decl: or_d,
                        args: vec![s("ZZZ+"), big_str(), s("YY")],
                    },
                    ExtraShape {
                        name: "error-bad-pattern",
                        decl: d(ScalarError, Derived(ScalarError)),
                        args: vec![s("("), big_str(), s("YY")],
                    },
                    // A big replacement with a SMALL haystack (round 6:
                    // the old big-haystack form breached at the
                    // conservative expansion charge and never reached
                    // the ORDERING leg at all — exactly the silent
                    // narrowing `required == reached` now pins).
                    ExtraShape {
                        name: "big-repl",
                        decl: or_d,
                        args: vec![s("x+"), s("xxxx"), big_str()],
                    },
                    // 128-byte invalid pattern: exercises the pattern
                    // conversion under dominance (and is too small to
                    // derive a +invalid-utf8 variant). Round 8: the
                    // ONLY `HarnessBlind` shapes — this branch can
                    // cross the ordering trigger, but it errors on the
                    // way and the leg admits retainable-Ok shapes only,
                    // so no probe size reaches it. See #294.
                    ExtraShape {
                        name: "invalid-pattern",
                        decl: d(HarnessBlind(HB_INVALID_PATTERN), NotDerived),
                        args: vec![invalid_pattern(), s("xxxx"), s("YY")],
                    },
                    // Invalid REPLACEMENT bytes: the lossy conversion
                    // the round-4 finding was about, at ordering size —
                    // its derived variant is byte-identical (the repl
                    // is already alternate 0xFF) and must ALSO reach.
                    ExtraShape {
                        name: "invalid-repl",
                        decl: or_d,
                        args: vec![s("x+"), s("xxxx"), invalid_big_str()],
                    },
                    // An uncached pattern keeps the 1 MiB
                    // dynamic-program ceiling under dominance (the
                    // happy pattern sits in the pre-populated
                    // compile-time cache, mirroring production literal
                    // precompilation).
                    ExtraShape {
                        name: "uncached-pattern",
                        decl: or_d,
                        args: vec![s("y+"), big_str(), s("YY")],
                    },
                ],
            },
            Func::Count => FnSpec {
                happy: d(ScalarError, Derived(ScalarError)),
                empty: d(ScalarError, NotDerived),
                extra: vec![
                    ExtraShape {
                        name: "error-bad-pattern",
                        decl: d(ScalarError, Derived(ScalarError)),
                        args: vec![s("("), big_str()],
                    },
                    ExtraShape {
                        name: "invalid-pattern",
                        decl: d(ScalarError, NotDerived),
                        args: vec![invalid_pattern(), s("xxxx")],
                    },
                ],
            },
            // The derived variant's first corrupted `%` escape errors —
            // the variant changes class.
            Func::Urldecode => FnSpec {
                happy: d(OrderingRequired, Derived(ScalarError)),
                empty: empty_do,
                extra: vec![ExtraShape {
                    name: "error-bad-escape",
                    decl: d(ScalarError, NotDerived),
                    args: {
                        let mut v = vec![b'%'; 3];
                        v.extend_from_slice(b"zz");
                        vec![Value::Str(Cow::Owned(v))]
                    },
                }],
            },
            Func::BytesFn | Func::Duration | Func::DurationSeconds | Func::UnixToTime => FnSpec {
                happy: d(ScalarError, NotDerived),
                empty: d(ScalarError, NotDerived),
                extra: vec![ExtraShape {
                    name: "error-unparseable",
                    decl: d(ScalarError, Derived(ScalarError)),
                    args: vec![big_str()],
                }],
            },
            Func::UnixEpoch | Func::UnixEpochMillis | Func::UnixEpochNanos => FnSpec {
                happy: d(DominanceOnly(R_CONST), NotDerived),
                empty: d(DominanceOnly(R_CONST), NotDerived),
                extra: vec![],
            },
            // Scalar (time/float) results from small fixed arguments.
            Func::ToDateInZone | Func::ToDate | Func::Now | Func::TimestampFn | Func::Round => {
                FnSpec {
                    happy: d(ScalarError, NotDerived),
                    empty: d(ScalarError, NotDerived),
                    extra: vec![],
                }
            }
            // No arguments: both shapes return the 1 MiB line.
            Func::LineFn => FnSpec {
                happy: d(OrderingRequired, NotDerived),
                empty: d(OrderingRequired, NotDerived),
                extra: vec![],
            },
            Func::AlignLeft | Func::AlignRight => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![
                    // The truncation arm's own charge+copy emits `count`
                    // (left) / tail (right) bytes — a FREE argument, so
                    // the probe must clear the 256 KiB ordering trigger
                    // (round-7 finding: a 200,000-byte probe fitted this
                    // shape into `DominanceOnly` while the branch can
                    // emit up to the full input).
                    ExtraShape {
                        name: "truncate",
                        decl: or_d,
                        args: vec![Value::int(500_000), big_str()],
                    },
                    ExtraShape {
                        name: "identity-count==len",
                        decl: or_d,
                        args: vec![Value::int(BIG as i64), big_str()],
                    },
                    ExtraShape {
                        name: "identity-negative",
                        decl: or_d,
                        args: vec![Value::int(-1), big_str()],
                    },
                ],
            },
            Func::Trunc => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![
                    ExtraShape {
                        name: "identity-count>=len",
                        decl: or_d,
                        args: vec![Value::int(2 * BIG as i64), big_str()],
                    },
                    // The negative-count tail is a FREE magnitude (the
                    // shape pins only the sign): probe it above the
                    // 256 KiB ordering trigger (round-7 finding — the
                    // old 200,000-byte tail fitted it into
                    // `DominanceOnly`).
                    ExtraShape {
                        name: "negative-tail",
                        decl: or_d,
                        args: vec![Value::int(-500_000), big_str()],
                    },
                    ExtraShape {
                        name: "zero",
                        decl: d(DominanceOnly(R_SMALL), Derived(DominanceOnly(R_SMALL))),
                        args: vec![Value::int(0), big_str()],
                    },
                ],
            },
            Func::Substr => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![
                    ExtraShape {
                        name: "open-end",
                        decl: or_d,
                        args: vec![Value::int(0), Value::int(-1), big_str()],
                    },
                    ExtraShape {
                        name: "error-start>end",
                        decl: d(ScalarError, Derived(ScalarError)),
                        args: vec![Value::int(5), Value::int(2), big_str()],
                    },
                    ExtraShape {
                        name: "error-negative-end",
                        decl: d(ScalarError, Derived(ScalarError)),
                        args: vec![Value::int(-1), Value::int(-2), big_str()],
                    },
                ],
            },
            Func::Contains | Func::HasPrefix | Func::HasSuffix => FnSpec {
                happy: d(ScalarError, Derived(ScalarError)),
                empty: d(ScalarError, NotDerived),
                extra: vec![],
            },
            // Small fixed text, big width multiplier: no big Str to
            // invalidate on the happy/wide shapes.
            Func::Indent | Func::Nindent => FnSpec {
                happy: d(OrderingRequired, NotDerived),
                empty: empty_do,
                extra: vec![
                    ExtraShape {
                        name: "wide",
                        decl: d(OrderingRequired, NotDerived),
                        args: vec![Value::int(2_000_000), s("a\nb\nc")],
                    },
                    ExtraShape {
                        name: "error-negative",
                        decl: d(ScalarError, Derived(ScalarError)),
                        args: vec![Value::int(-1), big_str()],
                    },
                ],
            },
            Func::Repeat => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![
                    ExtraShape {
                        name: "zero",
                        decl: d(DominanceOnly(R_SMALL), Derived(DominanceOnly(R_SMALL))),
                        args: vec![Value::int(0), big_str()],
                    },
                    ExtraShape {
                        name: "error-negative",
                        decl: d(ScalarError, Derived(ScalarError)),
                        args: vec![Value::int(-1), big_str()],
                    },
                    ExtraShape {
                        name: "error-overflow",
                        decl: d(ScalarError, Derived(ScalarError)),
                        args: vec![Value::int(i64::MAX), big_str()],
                    },
                ],
            },
            // sprig returns the decode ERROR TEXT as the value,
            // embedding the offending input — a big retainable string,
            // so even the error-as-value branch is ordering-required.
            Func::B64dec => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![ExtraShape {
                    name: "error-as-value",
                    decl: or_d,
                    args: vec![big_str()],
                }],
            },
            Func::Int | Func::Float64 => FnSpec {
                happy: d(ScalarError, Derived(ScalarError)),
                empty: d(ScalarError, NotDerived),
                extra: vec![],
            },
            // Arithmetic over Any-coerced big strings: scalar (or
            // divide-by-zero error) results throughout.
            Func::Add
            | Func::Sub
            | Func::Mul
            | Func::Div
            | Func::Mod
            | Func::Addf
            | Func::Subf
            | Func::Mulf
            | Func::Divf
            | Func::Max
            | Func::Min
            | Func::Maxf
            | Func::Minf
            | Func::Ceil
            | Func::Floor => FnSpec {
                happy: d(ScalarError, Derived(ScalarError)),
                empty: d(ScalarError, NotDerived),
                extra: vec![],
            },
            // Go's fromJson swallows parse errors into `nil`, so every
            // corrupted-input variant parses to Nil — a class change.
            Func::FromJson => FnSpec {
                happy: d(OrderingRequired, Derived(ScalarError)),
                empty: d(ScalarError, NotDerived),
                extra: vec![
                    ExtraShape {
                        name: "error-not-json",
                        decl: d(ScalarError, Derived(ScalarError)),
                        args: vec![big_str()],
                    },
                    ExtraShape {
                        name: "invalid-utf8-string",
                        decl: d(OrderingRequired, Derived(ScalarError)),
                        args: {
                            let mut j = b"{\"x\":\"".to_vec();
                            j.extend(std::iter::repeat_n(0xFF, BIG / 2));
                            j.extend_from_slice(b"\"}");
                            vec![Value::Str(Cow::Owned(j))]
                        },
                    },
                ],
            },
            Func::DefaultFn => FnSpec {
                happy: or_d,
                empty: empty_do,
                extra: vec![
                    ExtraShape {
                        name: "winner-default",
                        decl: or_d,
                        args: vec![big_str(), s("")],
                    },
                    ExtraShape {
                        name: "winner-given",
                        decl: or_d,
                        args: vec![s("d"), big_str()],
                    },
                ],
            },
        }
    }
}

#[test]
fn every_registry_function_charge_dominates_its_allocations() {
    use pulsus_read::logql::template::MAX_TEMPLATE_RENDER_BYTES;
    // Issue #311: a PINNED environment, not the host's — allocation
    // charges must not depend on which machine runs the gate.
    let env = TemplateEnv::default();
    let line = vec![b'L'; BIG];
    // Literal patterns are compiled ONCE at query compile in production
    // and every per-line call hits this cache — mirrored here so the
    // ordering leg reaches the argument conversions instead of
    // breaching at the dynamic-program ceiling first (round 4).
    let mut regex_cache: HashMap<String, regex::Regex> = HashMap::new();
    for pat in ["x+", "ZZZ+"] {
        regex_cache.insert(pat.to_string(), regex::Regex::new(pat).expect("pattern"));
    }

    // The domain bridge: the partition enum and the production REGISTRY
    // must name exactly the same functions. The two sides come from
    // different sources (the test's declaration vs the production
    // table), so drift in EITHER direction fails here with the set
    // difference — the compile-time exhaustiveness of `spec()` then
    // guarantees every bridged function carries full declarations.
    let partition_names: std::collections::BTreeSet<&str> =
        Func::ALL.iter().map(|f| f.name()).collect();
    let registry_names: std::collections::BTreeSet<&str> =
        REGISTRY.iter().map(|d| d.name).collect();
    let partition_only: Vec<&str> = partition_names
        .difference(&registry_names)
        .copied()
        .collect();
    let registry_only: Vec<&str> = registry_names
        .difference(&partition_names)
        .copied()
        .collect();
    assert!(
        partition_only.is_empty() && registry_only.is_empty(),
        "the shape partition's function domain must match the registry exactly \
         (add the Func variant + spec arm, or remove the stale one) — \
         partition-only: {partition_only:?}, registry-only: {registry_only:?}"
    );

    let mut failures = Vec::new();
    // `declared`: shape -> class, from the per-shape DECLARATIONS.
    // `reached`: shapes OBSERVED to meet the ordering trigger at
    // runtime. Their OrderingRequired projection is compared at the
    // end — the round-7 replacement for `registered == executed`,
    // whose two sides one loop populated (tautological).
    let mut declared: std::collections::BTreeMap<String, ShapeClass> =
        std::collections::BTreeMap::new();
    let mut reached: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for func in Func::ALL {
        let def = REGISTRY
            .iter()
            .find(|d| d.name == func.name())
            .expect("domain bridge asserted above");
        let spec = func.spec();
        let mut base: Vec<(String, ShapeDecl, Vec<Value<'static>>)> = vec![
            (
                "happy".to_string(),
                spec.happy,
                args_for(def.name, def.params, def.variadic),
            ),
            (
                "empty".to_string(),
                spec.empty,
                empty_args(def.params, def.variadic),
            ),
        ];
        base.extend(
            spec.extra
                .into_iter()
                .map(|e| (e.name.to_string(), e.decl, e.args)),
        );
        // Derive the +invalid-utf8 variants, cross-checking each
        // shape's DECLARED derivation against the mechanical outcome:
        // a variant that derives undeclared, or is declared but does
        // not derive, fails — the declaration set cannot silently
        // diverge from the executed set.
        let mut shapes: Vec<(String, ShapeClass, Vec<Value<'static>>)> = Vec::new();
        for (name, decl, args) in base {
            match (invalidate_utf8(&args), decl.utf8) {
                (Some(inv), Derived(class)) => {
                    shapes.push((format!("{name}+invalid-utf8"), class, inv));
                }
                (Some(_), NotDerived) => failures.push(format!(
                    "{}/{name}: the mechanical invalid-utf8 derivation produced a \
                     variant but the shape declares NotDerived — declare the \
                     variant's class",
                    def.name
                )),
                (None, Derived(_)) => failures.push(format!(
                    "{}/{name}: declares a Derived invalid-utf8 variant but the \
                     mechanical derivation produced none (no Str argument ≥ 4 KiB)",
                    def.name
                )),
                (None, NotDerived) => {}
            }
            shapes.push((name, decl.class, args));
        }
        for (shape, class, args) in shapes {
            let full_name = format!("{}/{}", def.name, shape);
            if declared.insert(full_name.clone(), class).is_some() {
                failures.push(format!(
                    "{full_name}: duplicate shape name — declarations would shadow"
                ));
            }
            let gate = GateEnv {
                env: env.clone(),
                budget: RenderBudget::default(),
            };
            let ctx = FuncCtx {
                print_env: &gate,
                line: &line,
                ts_ns: 1_700_000_000_000_000_000,
                regex_cache: &regex_cache,
                budget: &gate.budget,
                _marker: std::marker::PhantomData,
            };
            let inputs = input_bytes(&args, line.len());
            // Clone for the ordering rerun BEFORE the first call.
            let args_rerun = args.clone();
            let before = BYTES.load(Ordering::Relaxed);
            let result = (def.call)(&ctx, &args);
            let alloc = BYTES.load(Ordering::Relaxed).saturating_sub(before);
            let charged = gate.budget.charged_bytes();
            let (bound, kind) = match &result {
                Ok(v) if is_retainable(v) => (4 * charged + SLACK, "retainable"),
                Ok(_) => (4 * inputs + SLACK, "scalar"),
                Err(_) => (32 * inputs + 2 * SLACK, "error"),
            };
            if alloc > bound {
                failures.push(format!(
                    "{} [{shape}]: allocated {alloc} B > {kind} bound {bound} B \
                     (charged {charged} B, inputs {inputs} B, result {})",
                    def.name,
                    match &result {
                        Ok(v) => v.type_name().to_string(),
                        Err(e) => format!("Err({e})"),
                    }
                ));
            }
            let retainable_ok = matches!(&result, Ok(v) if is_retainable(v));
            // `ScalarError` is verified, not trusted: a retainable Ok
            // from a shape declared scalar/error means the declaration
            // (and thus its ordering exemption) is wrong.
            if matches!(class, ScalarError) && retainable_ok {
                failures.push(format!(
                    "{full_name}: declared ScalarError but returned a retainable Ok({}) \
                     — reclassify as OrderingRequired or DominanceOnly",
                    match &result {
                        Ok(v) => v.type_name().to_string(),
                        Err(_) => unreachable!("retainable_ok implies Ok"),
                    }
                ));
            }
            drop(result);
            // --- the ORDERING leg (charge BEFORE allocate) -------------
            // Rerun retainable shapes that allocated big under a nearly
            // exhausted budget: correct ordering breaches at the charge
            // and returns before the copy, so allocation stays tiny; a
            // charge moved AFTER its allocation leaves the big copy on
            // the counter. (This is what a post-hoc charged-vs-allocated
            // comparison can never see.)
            if retainable_ok && alloc >= 4 * SLACK {
                reached.insert(full_name);
                let gate = GateEnv {
                    env: env.clone(),
                    budget: RenderBudget::default(),
                };
                gate.budget
                    .charge((MAX_TEMPLATE_RENDER_BYTES - TINY_REMAINING) as usize)
                    .expect("pre-exhaust");
                let ctx = FuncCtx {
                    print_env: &gate,
                    line: &line,
                    ts_ns: 1_700_000_000_000_000_000,
                    regex_cache: &regex_cache,
                    budget: &gate.budget,
                    _marker: std::marker::PhantomData,
                };
                let before = BYTES.load(Ordering::Relaxed);
                let result = (def.call)(&ctx, &args_rerun);
                let tiny_alloc = BYTES.load(Ordering::Relaxed).saturating_sub(before);
                if tiny_alloc > ORDERING_CEILING {
                    failures.push(format!(
                        "{} [{shape}] ORDERING: allocated {tiny_alloc} B under a \
                         {TINY_REMAINING} B budget remainder — a copy happens BEFORE \
                         its charge (result {})",
                        def.name,
                        match &result {
                            Ok(v) => v.type_name().to_string(),
                            Err(e) => format!("Err({e})"),
                        }
                    ));
                }
            }
        }
    }

    // --- round 7: `required == reached`, both directions ---------------
    // `required` derives ONLY from the declarations; `reached` ONLY
    // from runtime observation. A shape declared OrderingRequired that
    // errored or shrank upstream never reached its leg — fix the shape
    // or reclassify it WITH a reason. A shape that reached the leg
    // while declared exempt has a stale declaration — promote it.
    //
    // Round 8: `HarnessBlind` is excluded from `required` for the
    // reason it carries — the leg admits retainable-`Ok` shapes only,
    // so an allocate-then-error branch cannot enter it at any probe
    // size (#294). It is NOT trusted in the other direction: reaching
    // the leg disproves the blindness claim and fails, exactly like a
    // stale `DominanceOnly`.
    let required: std::collections::BTreeSet<String> = declared
        .iter()
        .filter(|(_, c)| matches!(c, OrderingRequired))
        .map(|(n, _)| n.clone())
        .collect();
    for missing in required.difference(&reached) {
        failures.push(format!(
            "{missing}: declared OrderingRequired but never reached the ordering leg \
             — the shape errored or shrank upstream; fix the shape or reclassify it \
             (DominanceOnly with the measured BRANCH limit, or HarnessBlind if the \
             branch can cross the trigger but this harness cannot observe it)"
        ));
    }
    for stale in reached.difference(&required) {
        let declared_as = match declared.get(stale.as_str()) {
            Some(DominanceOnly(reason)) => format!("DominanceOnly (stale reason: {reason})"),
            Some(ScalarError) => "ScalarError".to_string(),
            Some(HarnessBlind(reason)) => format!(
                "HarnessBlind (the declared exception is stale — this harness DID \
                 observe the branch: {reason})"
            ),
            // `required` contains every OrderingRequired declaration,
            // and `reached` only ever holds declared shapes.
            Some(OrderingRequired) | None => unreachable!("stale is reached \\ required"),
        };
        failures.push(format!(
            "{stale}: reached the ordering leg but is declared {declared_as} — promote \
             it to OrderingRequired"
        ));
    }

    // --- round 6, the single-allocation lossy repair (finding 1): an
    // all-invalid haystack must not allocate past its charge — the old
    // `from_utf8_lossy` path grow-doubled (len + 2len + 4len cumulative
    // against a 3len charge). Factor-1 bound: alloc ≤ charged + 512 KiB.
    {
        let gate = GateEnv {
            env: env.clone(),
            budget: RenderBudget::default(),
        };
        let ctx = FuncCtx {
            print_env: &gate,
            line: &line,
            ts_ns: 1_700_000_000_000_000_000,
            regex_cache: &regex_cache,
            budget: &gate.budget,
            _marker: std::marker::PhantomData,
        };
        let def = REGISTRY
            .iter()
            .find(|d| d.name == "count")
            .expect("count registered");
        let args = vec![s("x+"), Value::Str(Cow::Owned(vec![0xFF; BIG]))];
        let before = BYTES.load(Ordering::Relaxed);
        let result = (def.call)(&ctx, &args);
        let alloc = BYTES.load(Ordering::Relaxed).saturating_sub(before);
        let charged = gate.budget.charged_bytes();
        assert!(
            result.is_ok(),
            "count over an all-invalid haystack: {result:?}"
        );
        if alloc > charged + 512 * 1024 {
            failures.push(format!(
                "lossy repair churn: allocated {alloc} B > charged {charged} B + 512 KiB — \
                 the conversion must precompute its repaired length and allocate ONCE \
                 (round 6, finding 1)"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "registry functions whose allocations are NOT dominated by their budget \
         charges (the fifth-class detector — charge the output/intermediate before \
         constructing it):\n{}",
        failures.join("\n")
    );
}
