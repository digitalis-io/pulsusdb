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
//! `DominanceOnly(reason)` / `ScalarErrorOrdering` / `ScalarError` /
//! `DeclaredException(reason)` —
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
//! **Issue #294 — the ordering leg admits on ALLOCATION, not on a
//! retainable `Ok`.** Until #294 the leg re-ran a shape only when it
//! returned a retainable `Ok`, so **every branch that allocated and
//! then returned a scalar or an error was outside the gate at any
//! size** — charge-after-allocate there was undetectable however large
//! the copy. The allocation happened either way, and that is what the
//! leg measures, so admission is now `alloc >= 4 * SLACK` alone.
//! Widening it on `df4bdbd` turned **31 shapes across 22 registry
//! functions** red, all one mechanism: caller bytes converted or copied
//! inside a template function with no charge covering the copy. Those
//! 31 are sealed by name in [`WERE_RED_ON_DF4BDBD`] — a count with no
//! membership list drifts silently.
//!
//! The widening splits the old `ScalarError` class in two, because it
//! used to mean "no retainable output" and now has to distinguish
//! "below the trigger" from "above it":
//! [`ShapeClass::ScalarErrorOrdering`] is a scalar/error shape that
//! MUST reach the leg, [`ShapeClass::ScalarError`] one that must not.
//!
//! **Round 8's declared exception, corrected (#294).** The two
//! `invalid-pattern` shapes were declared a blindness-of-this-harness
//! exception, on the ground that their branch "allocates and then
//! returns `Err`". Both halves of that are false on `df4bdbd`: the
//! branch returns `Ok(string)` with 4,198,096 B charged, and the
//! widened leg admits error results anyway. The state is renamed
//! [`ShapeClass::DeclaredException`] — the NAME was half the claim —
//! and its reason rewritten to the measured one (see
//! [`DE_INVALID_PATTERN`]). The superseded reason is not deleted but
//! restated there, because the trail of a false claim being caught is
//! itself the record.
//!
//! **Issue #294 review round 1 — a probe can reach the SIZE and still
//! miss the BRANCH.** `bytes/error-unparseable` passes a 1 MiB argument
//! and allocates 136 B, because a leading `x` puts
//! `humanize.ParseBytes` on its cheapest arm. The criterion was
//! satisfiable through a weak representative while the guarded path
//! allocated 3,145,761 B under a 65,536-byte remainder. Two shapes were
//! added to drive the other two arms.
//!
//! The rest of the shape set was then swept the same way, and the sweep
//! is worth recording because it is not the same question as "is the
//! probe short":
//!
//! - **Parameter positions never probed at size.** Measured over every
//!   registry function: `Replace`/`replace`'s REPLACEMENT (added here —
//!   4,194,308 allocated against 4,194,308 charged, 96 B under a
//!   near-exhausted budget); `Trim*`/`trimAll`'s cutset (4 B — the
//!   cutset is scanned, never copied); `contains`/`hasPrefix`/
//!   `hasSuffix`'s needle (0 B, bool result); `round`'s `Any` and
//!   `toDate`'s value (0 B — both borrow); and the regex PATTERN of
//!   `regexReplaceAll`/`regexReplaceAllLiteral`/`count`, which is the
//!   declared exception below and must not be probed larger (#291).
//! - **Branches an at-size probe does not reach.** One remains, and it
//!   is NOT covered by anything here: `go_parse_int_base0` /
//!   `go_parse_float` strip underscores into a fresh `String`, and no
//!   shape passes an underscore-bearing argument. Measured directly at
//!   a 1,048,577-byte argument of `1_1_…1`: **2,097,144 B allocated,
//!   0 charged, under a 65,536-byte remainder**, for both `int` and
//!   `float64`, result `Ok(int)` / `Ok(float64)`. It is a TRANSIENT —
//!   scalar result, freed by return, ≤ ~2x the input — which is the
//!   class the ledger declares uncharged.
//!
//!   Closing it takes one of two changes, and neither belongs to #294.
//!   **Either** give the two casts a charging form at their call
//!   sites: every production caller lives in
//!   `src/logql/template/funcs.rs` — the arithmetic and coercion
//!   registry closures, which hold a `FuncCtx`, plus `cast_to_int`,
//!   itself called only from one of them — so the budget can be
//!   threaded to all of them. **Or** reimplement the
//!   underscore-skipping parse so it never copies, which means
//!   rewriting a `strconv.ParseInt(s, 0, 0)` port whose behaviour the
//!   corpus pins and whose oracle would have to be re-established.
//!   That second half is what makes this a separate piece of work.
//!
//!   **Adding the shape would ship a red gate, so it is not added:
//!   this paragraph is a stated limit of the instrument, not a check.**
//!   Per the #294 ruling it is RECORDED, not scheduled — no issue is
//!   filed, and the measurement lives here so the next reader finds it
//!   with its reproduction rather than re-deriving it.
//!
//! This is the gate that catches the fifth amplification class the
//! round-1 census could not judge: an existing site labelled
//! "input-bounded" whose per-call copy is repeatable (or compoundable)
//! inside a `range`/variable-only body that emits no text.
//!
//! Single `#[test]`, own binary: the counting allocator is
//! process-global (the alloc-gate flake rule: byte ceilings, never
//! exact counts).
//!
//! **One exception, and the condition it comes with** (issue #294). The
//! AC-9b block near the end of this file DOES assert an exact byte
//! count, because the property it holds is an EQUALITY —
//! `charged == allocated == err.len()` — and a ceiling cannot express
//! one. It is admissible only because it RE-SAMPLES: the global counter
//! was measured to add spurious bytes about once in 4,000 measurements,
//! so the equality is required to hold on at least one of three samples
//! instead of on a single one. Every sample still demands the exact
//! equality, so no persistent excess is forgiven at any magnitude —
//! this is a persistence test, not a tolerance. **An exact count
//! asserted from ONE sample is still the flake the rule above is
//! about**; see that block's own comment for the measurements.

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

/// A 1 MiB run of ASCII digits — a VALID-UTF-8 argument that parses as a
/// number and then overflows.
fn big_digits() -> Value<'static> {
    Value::Str(Cow::Owned(vec![b'9'; BIG]))
}

/// `1` followed by a 1 MiB unit suffix — drives `humanize.ParseBytes`'s
/// `unhandled size name:` arm, whose text embeds the whole
/// (Unicode-lowercased) suffix.
fn big_unit() -> Value<'static> {
    let mut v = vec![b'1'];
    v.extend(std::iter::repeat_n(b'X', BIG));
    Value::Str(Cow::Owned(v))
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
/// tiny).
///
/// It is deliberately NOT larger: regex COMPILATION allocates a large
/// multiple of the pattern length and `RegexBuilder::size_limit` does not
/// bound it — it is `nfa_size_limit`, governing only the last of three
/// compile phases. **The clause that used to stand here saying this shape
/// is "sized to stay inside the charged 1 MiB program ceiling" was false**
/// and issue #291 measured it: `\w`x64 is 128 bytes and peaks 2.20 MB at
/// a 1 MiB ceiling, 10.62 MB at the 10 MiB default. What makes the shape
/// safe now is not its size but the budget — `compile_regex` charges
/// `pulsus_re2::regex_compile_transient_bound_with` and the estimate is
/// an upper bound on the peak. The class-heavy block at the end of
/// [`every_registry_function_charge_dominates_its_allocations`] is what
/// checks that at this site; `\w`x16, 32 bytes, allocates 2.67 MB.
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
    /// **Issue #294.** Scalar-or-error result whose allocation meets
    /// the ordering trigger: the shape MUST reach the leg, and must NOT
    /// return a retainable `Ok` (that would make it `OrderingRequired`
    /// instead). This is the class the pre-#294 predicate could not
    /// express — it admitted on `Ok(retainable)`, so these shapes were
    /// outside the gate at every probe size.
    ScalarErrorOrdering,
    /// Scalar or error result BELOW the ordering trigger: the leg does
    /// not apply — verified, not trusted in either direction (a
    /// retainable `Ok` from such a shape fails the gate, and so does
    /// reaching the leg, which means the declaration is stale and the
    /// shape is `ScalarErrorOrdering`).
    ScalarError,
    /// **Declared exception (round 8, reason corrected by #294).** A
    /// shape probed below the ordering trigger for a reason that is
    /// neither a branch limit nor this harness's blindness.
    ///
    /// Deliberately a separate state rather than a carefully-worded
    /// `DominanceOnly`: the two are different KINDS of claim — one is
    /// about the branch, this one is about the probe — and the
    /// reason-header rule below binds `DominanceOnly` reasons to branch
    /// relations. The reason string must name its tracking issue.
    ///
    /// Excluded from `required`, but NOT trusted in the other
    /// direction: if such a shape DOES reach the leg, the declaration
    /// is stale and fails, exactly like a stale `DominanceOnly`.
    DeclaredException(&'static str),
}
use ShapeClass::{
    DeclaredException, DominanceOnly, OrderingRequired, ScalarError, ScalarErrorOrdering,
};

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
/// A shape held below the trigger by its PROBE rather than by a branch
/// limit has no such reason to state and does not belong here: it is
/// [`ShapeClass::DeclaredException`], whose reasons live below.
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
/// [`ShapeClass::DeclaredException`] reasons — the ONLY one.
///
/// **Round 8's reason was false, and #294 measured it.** It said the
/// branch "allocates and then returns `Err`, and the ordering leg
/// re-runs only shapes returning a retainable `Ok`". Both halves are
/// wrong on `df4bdbd`: #291's fix made `compile_charged_regex` charge
/// every conversion and the compile estimate BEFORE allocating, so the
/// shape returns `Ok(string)` with 4,198,096 B charged — and since #294
/// the leg admits on allocation, so an error result would no longer
/// exclude it either.
///
/// What actually keeps this shape out of the leg is its 128-byte probe:
/// 213,501 B allocated, just under the 262,144-byte trigger. Probing at
/// size is NOT the fix, because at a 2 KiB invalid pattern the leg
/// allocates 250,200 B under a 64 KiB remainder —
/// `pulsus_re2::regex_compile_transient_bound_with` PARSES the pattern
/// to compute the estimate, and that parse is the argument to the very
/// `ctx.charge(...)` meant to cover it. That is a defect in the bound
/// #291 shipped, reopened on **#291** with the measurement; choosing a
/// probe size that stays green would be fitting the probe to dodge it.
///
/// The `invalid-repl` / `+invalid-utf8` haystack shapes do NOT cover
/// this site: they charge through `lossy_charged`, a different call
/// site from the pattern conversion's `lossy_go*`.
const DE_INVALID_PATTERN: &str = "measured on df4bdbd: this shape returns Ok(string) with 4,198,096 B \
     charged — `compile_charged_regex` charges every conversion and the \
     compile estimate before allocating, so the round-8 claim that the \
     branch allocates and then returns Err is FALSE, and since #294 the \
     ordering leg admits on ALLOCATION so an Err result would not \
     exclude it either. What excludes it is the 128-byte probe: 213,501 B \
     allocated, just under the 262,144-byte trigger. Probing at size is \
     NOT a fix here — at a 2 KiB invalid pattern the leg allocates \
     250,200 B under a 64 KiB remainder, because \
     `pulsus_re2::regex_compile_transient_bound_with` PARSES the pattern \
     to compute the estimate and that parse is the argument to the very \
     `ctx.charge(...)` meant to cover it (~61x the repaired length; \
     59,769,176 B at a 512 KiB pattern). That is one compile whose own \
     estimator is unaccounted, reopened at #291. Choosing a probe size \
     that stays green would be fitting the probe to dodge it";

/// The 31 shapes that were RED under the widened predicate on
/// `df4bdbd` (issue #294) — the membership seal for the count.
///
/// A count with no membership list drifts silently: "31 shapes" can
/// stay true while the set behind it changes. Every name here must be a
/// shape this file CONSTRUCTS (a typo, a rename or a dropped shape
/// fails below), and the whole gate must be green, so none of them may
/// appear in `failures`.
///
/// All 31 are one mechanism — caller bytes converted or copied inside a
/// template function with no charge covering the copy — at nine call
/// sites that
/// `git grep -n 'lossy(\|bytes_of(&a\[[0-9]\])\.to_vec()' \
/// crates/pulsus-read/src/logql/template/funcs.rs`
/// enumerated on `df4bdbd`. Deleting `fn lossy` is what proves that
/// list complete: a missed site no longer compiles.
const WERE_RED_ON_DF4BDBD: [&str; 31] = [
    "add/happy+invalid-utf8",
    "addf/happy+invalid-utf8",
    "bytes/error-unparseable",
    "bytes/error-unparseable+invalid-utf8",
    "ceil/happy+invalid-utf8",
    "div/happy+invalid-utf8",
    "divf/happy+invalid-utf8",
    "duration/error-unparseable",
    "duration/error-unparseable+invalid-utf8",
    "duration_seconds/error-unparseable",
    "duration_seconds/error-unparseable+invalid-utf8",
    "float64/happy+invalid-utf8",
    "floor/happy+invalid-utf8",
    "int/happy+invalid-utf8",
    "max/happy+invalid-utf8",
    "maxf/happy+invalid-utf8",
    "min/happy+invalid-utf8",
    "minf/happy+invalid-utf8",
    "mod/happy+invalid-utf8",
    "mul/happy+invalid-utf8",
    "mulf/happy+invalid-utf8",
    "sub/happy+invalid-utf8",
    "subf/happy+invalid-utf8",
    "toDateInZone/big-layout",
    "toDateInZone/big-layout+invalid-utf8",
    "toDateInZone/big-value",
    "toDateInZone/big-value+invalid-utf8",
    "toDateInZone/big-zone",
    "toDateInZone/big-zone+invalid-utf8",
    "unixToTime/error-unparseable",
    "unixToTime/error-unparseable+invalid-utf8",
];

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
                    // Issue #294 review round 1: the REPLACEMENT is the
                    // multiplying position and no shape passed a big one
                    // (a small haystack keeps the product inside the
                    // budget). `go_replace` charges `len + n*len(new)`
                    // before `Vec::with_capacity`: 4,194,308 allocated
                    // against 4,194,308 charged, 96 B under a
                    // near-exhausted budget.
                    ExtraShape {
                        name: "big-new",
                        decl: or_d,
                        args: vec![s("xxxx"), s("x"), big_str(), Value::int(-1)],
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
                    // The sprig argument order of `Replace/big-new`.
                    ExtraShape {
                        name: "big-new",
                        decl: or_d,
                        args: vec![s("x"), big_str(), s("xxxx")],
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
                    // derive a +invalid-utf8 variant). The ONLY
                    // `DeclaredException` shapes — held below the
                    // trigger by the PROBE, for the measured reason in
                    // `DE_INVALID_PATTERN` (#291).
                    ExtraShape {
                        name: "invalid-pattern",
                        decl: d(DeclaredException(DE_INVALID_PATTERN), NotDerived),
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
            // The invalid-byte variant charges its repair through
            // `lossy_charged` and allocates 2.1 MB doing it — an
            // `Ok(int)`, so the pre-#294 leg never saw it.
            Func::Count => FnSpec {
                happy: d(ScalarError, Derived(ScalarErrorOrdering)),
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
                happy: d(OrderingRequired, Derived(ScalarErrorOrdering)),
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
            // Issue #294: `bytes` keeps its U+FFFD repair (the ledger's
            // `template-output-budget` entry says why) but pays for it
            // — `lossy_charged` borrows valid UTF-8 (136 B allocated,
            // below the trigger) and charges the exact repaired length
            // before allocating it on the invalid variant, which is
            // what puts that variant into the ordering leg.
            Func::BytesFn => FnSpec {
                happy: d(ScalarError, NotDerived),
                empty: d(ScalarError, NotDerived),
                extra: vec![
                    ExtraShape {
                        name: "error-unparseable",
                        decl: d(ScalarError, Derived(ScalarErrorOrdering)),
                        args: vec![big_str()],
                    },
                    // Issue #294 review round 1. `error-unparseable`
                    // reaches this function at 1 MiB and still allocates
                    // only 136 B, because a leading `x` puts it on
                    // `humanize.ParseBytes`'s CHEAPEST arm — an empty
                    // numeric prefix, quoted, and out. The criterion
                    // passed on a weak representative while the guarded
                    // path was broken. These two shapes drive the other
                    // two arms, each of which embeds the whole argument
                    // in a RETAINED `__error_details__`: measured
                    // 3,145,761 B and 3,145,807 B allocated against 0
                    // charged before the fix.
                    ExtraShape {
                        name: "error-too-large",
                        decl: d(ScalarErrorOrdering, Derived(ScalarErrorOrdering)),
                        args: vec![big_digits()],
                    },
                    ExtraShape {
                        name: "error-unknown-unit",
                        decl: d(ScalarErrorOrdering, Derived(ScalarErrorOrdering)),
                        args: vec![big_unit()],
                    },
                ],
            },
            // Issue #294: the three functions whose ERROR TEXT embeds
            // the caller's argument. Before the fix each copied its
            // argument before parsing (3.1 MB / 12.6 MB / 24.6 MB at a
            // 1 MiB argument, nothing charged) and the result was an
            // `Err`, so the pre-widening leg could not see any of it.
            // Now the exact text length is charged first, so under a
            // near-exhausted budget the call breaches at the charge and
            // returns before rendering. The exactness of that charge is
            // the `charged == allocated == err.len()` block at the end.
            Func::Duration | Func::DurationSeconds | Func::UnixToTime => FnSpec {
                happy: d(ScalarError, NotDerived),
                empty: d(ScalarError, NotDerived),
                extra: vec![ExtraShape {
                    name: "error-unparseable",
                    decl: d(ScalarErrorOrdering, Derived(ScalarErrorOrdering)),
                    args: vec![big_str()],
                }],
            },
            Func::UnixEpoch | Func::UnixEpochMillis | Func::UnixEpochNanos => FnSpec {
                happy: d(DominanceOnly(R_CONST), NotDerived),
                empty: d(DominanceOnly(R_CONST), NotDerived),
                extra: vec![],
            },
            // Scalar (time/float) results from small fixed arguments.
            Func::Now | Func::TimestampFn | Func::Round => FnSpec {
                happy: d(ScalarError, NotDerived),
                empty: d(ScalarError, NotDerived),
                extra: vec![],
            },
            // Issue #294: `toDateInZone` copied ALL THREE arguments
            // (`.to_vec()` on layout and value, a lossy repair of the
            // zone) while the gate probed it with three tiny literals —
            // a scalar `Time` result, so the pre-#294 leg could not have
            // seen it even at size. Each argument gets its own at-size
            // shape; `parse_in_location` takes byte slices, so nothing
            // is copied now and all six sit far below the trigger.
            Func::ToDateInZone => FnSpec {
                happy: d(ScalarError, NotDerived),
                empty: d(ScalarError, NotDerived),
                extra: vec![
                    ExtraShape {
                        name: "big-layout",
                        decl: d(ScalarError, Derived(ScalarError)),
                        args: vec![big_str(), s("UTC"), s("2024-05-06")],
                    },
                    ExtraShape {
                        name: "big-zone",
                        decl: d(ScalarError, Derived(ScalarError)),
                        args: vec![s("2006-01-02"), big_str(), s("2024-05-06")],
                    },
                    ExtraShape {
                        name: "big-value",
                        decl: d(ScalarError, Derived(ScalarError)),
                        args: vec![s("2006-01-02"), s("UTC"), big_str()],
                    },
                ],
            },
            // The CONTROL for `toDateInZone`: `toDate` reaches the same
            // layout parser and allocates 0 at a 1 MiB layout because it
            // borrows. It is here to record that difference, not because
            // it is a site.
            Func::ToDate => FnSpec {
                happy: d(ScalarError, NotDerived),
                empty: d(ScalarError, NotDerived),
                extra: vec![ExtraShape {
                    name: "big-layout",
                    decl: d(ScalarError, Derived(ScalarError)),
                    args: vec![big_str(), s("2024-05-06")],
                }],
            },
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
                        decl: d(OrderingRequired, Derived(ScalarErrorOrdering)),
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
            // The two scalar/error classes are verified, not trusted: a
            // retainable Ok from either means the declaration (and the
            // reasoning behind it) is wrong.
            if matches!(class, ScalarError | ScalarErrorOrdering) && retainable_ok {
                failures.push(format!(
                    "{full_name}: declared {} but returned a retainable Ok({}) \
                     — reclassify as OrderingRequired or DominanceOnly",
                    match class {
                        ScalarErrorOrdering => "ScalarErrorOrdering",
                        _ => "ScalarError",
                    },
                    match &result {
                        Ok(v) => v.type_name().to_string(),
                        Err(_) => unreachable!("retainable_ok implies Ok"),
                    }
                ));
            }
            drop(result);
            // --- the ORDERING leg (charge BEFORE allocate) -------------
            // Rerun EVERY shape that allocated big under a nearly
            // exhausted budget: correct ordering breaches at the charge
            // and returns before the copy, so allocation stays tiny; a
            // charge moved AFTER its allocation leaves the big copy on
            // the counter. (This is what a post-hoc charged-vs-allocated
            // comparison can never see.)
            //
            // **Issue #294 — the one-line widening.** This admission
            // used to read `retainable_ok && alloc >= 4 * SLACK`. The
            // `retainable_ok` conjunct put every allocate-then-scalar
            // and allocate-then-error branch outside the gate at ANY
            // probe size; the allocation happened either way, and the
            // allocation is what this leg measures. Removing it turned
            // 31 shapes red on `df4bdbd` ([`WERE_RED_ON_DF4BDBD`]).
            if alloc >= 4 * SLACK {
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
    // Issue #294: `required` is now BOTH ordering classes —
    // `OrderingRequired` (retainable) and `ScalarErrorOrdering`
    // (scalar/error above the trigger). `DeclaredException` is excluded
    // for the reason it carries, and NOT trusted in the other
    // direction: reaching the leg makes the declaration stale and
    // fails, exactly like a stale `DominanceOnly`.
    let required: std::collections::BTreeSet<String> = declared
        .iter()
        .filter(|(_, c)| matches!(c, OrderingRequired | ScalarErrorOrdering))
        .map(|(n, _)| n.clone())
        .collect();
    for missing in required.difference(&reached) {
        failures.push(format!(
            "{missing}: declared OrderingRequired but never reached the ordering leg \
             — the shape errored or shrank upstream; fix the shape or reclassify it \
             (DominanceOnly with the measured BRANCH limit, ScalarError if it now \
             falls below the trigger, or DeclaredException if the PROBE holds it \
             below one)"
        ));
    }
    for stale in reached.difference(&required) {
        let declared_as = match declared.get(stale.as_str()) {
            Some(DominanceOnly(reason)) => {
                format!("DominanceOnly (stale reason: {reason}) — promote it to OrderingRequired")
            }
            Some(ScalarError) => "ScalarError (it now allocates past the ordering \
                 trigger — promote it to ScalarErrorOrdering)"
                .to_string(),
            Some(DeclaredException(reason)) => format!(
                "DeclaredException (the declaration is stale — the shape DID reach \
                 the leg: {reason})"
            ),
            // `required` contains every ordering declaration, and
            // `reached` only ever holds declared shapes.
            Some(OrderingRequired | ScalarErrorOrdering) | None => {
                unreachable!("stale is reached \\ required")
            }
        };
        failures.push(format!(
            "{stale}: reached the ordering leg but is declared {declared_as}"
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

    // Issue #291: the DYNAMIC-REGEX row. `compile_regex` used to charge a
    // flat `DYNAMIC_REGEX_PROGRAM_CEILING` (1 MiB) before compiling, on
    // the belief that `size_limit` bounded what compiling costs. It does
    // not — it is `nfa_size_limit`, the LAST of three phases — so a
    // class-heavy pattern blew straight through the charge.
    //
    // Red before the fix, measured on this tree: `\w`x16 is **32 bytes**
    // and allocates 2.67 MB peak at the 1 MiB ceiling against a charge of
    // 1 MiB + 32 B — two and a half times over, from a pattern a quarter
    // the length of `invalid_pattern()`. Length is not the quantity that
    // predicts the cost, which is why the fix is a bound and not a length
    // cap.
    //
    // The bound is this file's own `4 * charged + SLACK`, and it has to
    // be: `BYTES` is CUMULATIVE (every `alloc`/`realloc`, never
    // decremented) while the charge bounds the compile's PEAK LIVE bytes.
    // Measured here, `\w`x16 peaks 2.67 MB and churns 7.81 MB through the
    // same call. A factor-1 assertion between those two units would be
    // asserting something false about a correct implementation. Red today
    // regardless: 4 x (1 MiB + 32 B) + 64 KiB = 4.26 MB against 7.81 MB
    // churned.
    {
        let line: Vec<u8> = b"app=frontend status=200".to_vec();
        let regex_cache = HashMap::new();
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
            .find(|d| d.name == "regexReplaceAll")
            .expect("regexReplaceAll registered");
        let pattern = r"\w".repeat(16);
        let args = vec![
            Value::Str(Cow::Owned(pattern.clone().into_bytes())),
            s("frontend"),
            s("Z"),
        ];
        let before = BYTES.load(Ordering::Relaxed);
        let result = (def.call)(&ctx, &args);
        let alloc = BYTES.load(Ordering::Relaxed).saturating_sub(before);
        let charged = gate.budget.charged_bytes();
        assert!(
            result.is_ok(),
            "premise: the class-heavy pattern must COMPILE, or this row measures a \
             rejection instead of a compile: {result:?}"
        );
        let bound = 4 * charged + SLACK;
        if alloc > bound {
            failures.push(format!(
                "dynamic class-heavy regex: allocated {alloc} B > bound {bound} B \
                 (charged {charged} B) for a {}-byte pattern — the render budget must be \
                 charged what compiling this pattern costs, not a flat program ceiling \
                 that models only the NFA phase (issue #291)",
                pattern.len()
            ));
        }
    }

    // Issue #291, owner ruling v2: the new per-compile charge is an
    // ACCEPT-SURFACE change and must not arrive silently. `compile_regex`
    // charged a flat 1 MiB before compiling a template-computed pattern;
    // it now charges what compiling that pattern costs, whose floor is
    // `NFA_PEAK_FACTOR * DYNAMIC_REGEX_PROGRAM_CEILING`. Both numbers are
    // asserted here so neither the floor nor its consequence can move
    // without this reddening, and both are in the ledger under
    // `regex-compile-budget`.
    {
        let line: Vec<u8> = b"app=frontend".to_vec();
        let regex_cache = HashMap::new();
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
            .find(|d| d.name == "regexReplaceAll")
            .expect("regexReplaceAll registered");
        // The CHEAPEST possible dynamic pattern: one literal character.
        // Whatever it is charged is the floor every dynamic compile pays.
        let args = vec![s("a"), s("frontend"), s("Z")];
        let result = (def.call)(&ctx, &args);
        assert!(result.is_ok(), "premise: `a` must compile: {result:?}");
        let floor = gate.budget.charged_bytes();

        // The 1 MiB program ceiling times `NFA_PEAK_FACTOR`, plus the
        // one-byte pattern copy: 4,194,329 B. Asserted as a narrow RANGE
        // so the test says what the floor IS, not merely that it is
        // large.
        assert!(
            (4 * 1024 * 1024..=4 * 1024 * 1024 + 4096).contains(&floor),
            "one dynamic regex compile now charges {floor} B; the ledger records \
             4,194,329. Before #291 it charged a flat 1 MiB, which under-reported: \
             `\\w`x16 (32 bytes) allocates 2.67 MB at this same program ceiling. If this \
             moved, update `regex-compile-budget`'s template bullet with it"
        );

        // The consequence, as the number a user would feel: how many
        // distinct dynamic compiles fit in one render budget.
        let fits = MAX_TEMPLATE_RENDER_BYTES / floor;
        assert_eq!(
            fits, 15,
            "one render now fits {fits} distinct dynamic regex compiles; the ledger records \
             15, down from 64 under the flat 1 MiB charge. That narrowing is deliberate \
             (the old charge was wrong) but it is an accept-surface change and must not \
             move without the ledger moving with it"
        );
    }

    // Issue #291 review finding 1, `[high]`: the literal-regex PREWARM in
    // `template::compile` was the one user-pattern compile in the
    // workspace outside the budget, and it is reachable inside the
    // 131,072-byte query-text cap. The reviewer's own shape, replayed:
    // a template carrying a literal `\w`x43000 as a regex argument,
    // 129,033 bytes of template text. Before the fix it peaked
    // **298.92 MB** and returned `Ok` — a cache prewarm allocating three
    // hundred megabytes of a user's pattern at query-compile time.
    //
    // Measured over `template::compile` itself, not over a registry
    // function, because the prewarm runs there and nowhere else. The
    // ceiling is the compile budget's own cap: the prewarm now estimates
    // the pattern before compiling it and declines to cache what it
    // cannot afford, exactly as it already declined to cache what would
    // not compile.
    {
        let pattern = r"\w".repeat(43_000);
        // A Go RAW string (backquotes) for the pattern: `\w` is not a Go
        // escape, so the double-quoted form is a template parse error
        // before the prewarm is reached — and a user writing this regex
        // writes it exactly this way.
        let text = format!("{{{{ regexReplaceAll `{pattern}` .app \"z\" }}}}");
        // Two lengths, and the larger is the one that has to fit. The
        // template TEXT is 86,033 bytes; written into a LogQL
        // double-quoted `line_format` argument every backslash doubles,
        // so the QUERY is 129,033 — the figure the reviewer measured, and
        // still inside the 131,072-byte cap. Both are asserted so this
        // stays a reachable input and not a curiosity.
        let query_bytes = text.len() + text.matches('\\').count();
        assert_eq!(text.len(), 86_033);
        assert_eq!(query_bytes, 129_033);
        assert!(query_bytes < 131_072, "premise: inside the query-text cap");
        let before = BYTES.load(Ordering::Relaxed);
        let compiled = pulsus_read::logql::template::compile(
            &text,
            pulsus_read::logql::template::TemplateKind::Line,
        );
        let alloc = BYTES.load(Ordering::Relaxed).saturating_sub(before);
        assert!(
            compiled.is_ok(),
            "premise: the TEMPLATE still parses — the budget declines to prewarm the \
             pattern, it does not reject the template: {:?}",
            compiled.err()
        );
        assert!(
            alloc <= pulsus_re2::MAX_REGEX_COMPILE_TRANSIENT_BYTES,
            "the literal-regex prewarm allocated {alloc} B compiling a template of {} \
             bytes — inside the query-text cap. This site is `template/mod.rs`'s \
             `regex_cache` fill; it must go through `pulsus_re2::compile_user_regex` \
             like every other user-pattern compile (issue #291 review finding 1)",
            text.len()
        );
    }

    // --- issue #294: the membership seal for the 31 -------------------
    // Every name in `WERE_RED_ON_DF4BDBD` must be a shape this file
    // constructs. A typo, a rename or a dropped shape fails here rather
    // than silently shrinking the set the count stands for.
    {
        let sealed: std::collections::BTreeSet<&str> =
            WERE_RED_ON_DF4BDBD.iter().copied().collect();
        assert_eq!(
            sealed.len(),
            WERE_RED_ON_DF4BDBD.len(),
            "WERE_RED_ON_DF4BDBD has duplicate entries"
        );
        let missing: Vec<&str> = sealed
            .iter()
            .copied()
            .filter(|n| !declared.contains_key(*n))
            .collect();
        assert!(
            missing.is_empty(),
            "WERE_RED_ON_DF4BDBD names shapes this file does not construct: {missing:?} \
             — the count and the set it stands for have drifted apart (#294)"
        );
        // None of them may be among the failures: the whole point of
        // the change is that all 31 are green.
        let joined = failures.join("\n");
        let still_red: Vec<&str> = sealed
            .iter()
            .copied()
            .filter(|n| {
                let (func, shape) = n.split_once('/').expect("shape names are fn/shape");
                joined.contains(&format!("{func} [{shape}]")) || joined.contains(*n)
            })
            .collect();
        assert!(
            still_red.is_empty(),
            "shapes #294 fixed are red again: {still_red:?}\n{joined}"
        );
    }

    // --- issue #294 AC-9b: the error text is charged EXACTLY ----------
    // For every shape whose error message embeds the caller's argument,
    // `charged == allocated == err.len()` — an EQUALITY, not a bound.
    // It is a relation, not a magnitude: change the message prefix and
    // both sides move together, so it cannot be satisfied by fitting a
    // constant.
    //
    // The break is observed, not hypothetical: building `render()`'s
    // halves as temporaries before appending them gives
    // `alloc=4,194,375 charged=2,097,221` on
    // `unixToTime/error-unparseable+invalid-utf8`'s smaller sibling.
    //
    // **Why the measurement is re-sampled, and why that is not a
    // weakening** (issue #294 review round 2). This file's own header
    // states the rule: "the counting allocator is process-global (the
    // alloc-gate flake rule: byte ceilings, never exact counts)". The
    // equality below asserts an exact count, and it therefore broke that
    // rule — nobody noticed until it reddened CI once.
    //
    // The two sides are not alike. Note first what the loop actually
    // does, because it is easy to describe wrongly: ALL THREE values are
    // recomputed on every sample. Each sample builds a fresh `GateEnv`
    // and `RenderBudget`, calls the function again, and reads
    // `err.len()` off the `String` that sample returned; nothing is
    // carried across samples. What differs is which value is TREATED as
    // noisy:
    //
    //   * `charged` comes from THAT sample's own `RenderBudget` and
    //     `err.len()` from the `String` it returned. Both are
    //     deterministic functions of the argument, so they are expected
    //     to come out IDENTICAL on every sample; the re-sample is not
    //     there for them. (Nothing here ENFORCES that identity — it is
    //     a property of the code, and the printed sample list below is
    //     where a violation of it would show.)
    //   * `alloc` is a difference of two reads of `BYTES`, a
    //     PROCESS-GLOBAL cumulative counter. It occasionally counts
    //     bytes this call did not allocate, and it is the only value
    //     the re-sample exists for.
    //
    // Measured: replaying `unixToTime`'s invalid-UTF-8 measurement 4,000
    // times in one process gave ONE deviation, `charged` exact and
    // `alloc` high by a few hundred bytes — about 1 in 4,000
    // measurements, so about 1 in 670 runs of this gate, which is the
    // rate at which it was seen to fail. **The excess is a random draw,
    // not a constant**: separate runs of that replay observed 594 B and
    // 758 B, and the CI failure that started this was 758 B. What is
    // stable is the RATE and the fact that only `alloc` moves. The
    // magnitude is hundreds of bytes against a 4,718,661-byte error
    // text; the render is a single `String::with_capacity` whose length
    // a `debug_assert_eq!` already pins, so a capacity shortfall would
    // grow by megabytes, not by hundreds of bytes.
    //
    // So the pass condition is still the EXACT equality — what is
    // tolerated is INSTRUMENT NOISE, not a range of real behaviour. Up
    // to three samples are taken and the equality must hold on at least
    // one. A real defect deviates on EVERY call and by megabytes: the
    // temporaries break above is `+2,097,154 B` every time, three orders
    // of magnitude clear of the few-hundred-byte artefact, so it fails
    // all three samples deterministically. Three artefacts in a row is
    // about 1 in 6e10.
    //
    // `bytes` is deliberately NOT here: since review round 1 it charges
    // a BOUND (`4*len + 64`) covering both the U+FFFD repair it keeps
    // and `humanize.ParseBytes`'s three failure texts, because the arm
    // is chosen by the parse and the exact length is not knowable before
    // it. Its relation is a bound, not this equality; the two
    // `bytes/error-*` shapes above are what hold it.
    {
        let line = vec![b'L'; 16];
        let regex_cache: HashMap<String, regex::Regex> = HashMap::new();
        for (name, arg) in [
            ("duration", big_str()),
            ("duration", invalid_big_str()),
            ("duration_seconds", big_str()),
            ("duration_seconds", invalid_big_str()),
            ("unixToTime", big_str()),
            ("unixToTime", invalid_big_str()),
        ] {
            let def = REGISTRY
                .iter()
                .find(|d| d.name == name)
                .expect("registered");
            let args = vec![arg];
            let invalid = std::str::from_utf8(match &args[0] {
                Value::Str(b) => b.as_ref(),
                _ => unreachable!("the probe passes a Str"),
            })
            .is_err();
            // Up to SAMPLES measurements; the equality must hold on at
            // least one. Everything is recomputed per sample; see the
            // comment above the block for why `alloc` is the only value
            // treated as noisy.
            const SAMPLES: usize = 3;
            let mut seen: Vec<(u64, u64, usize)> = Vec::with_capacity(SAMPLES);
            for _ in 0..SAMPLES {
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
                let before = BYTES.load(Ordering::Relaxed);
                let result = (def.call)(&ctx, &args);
                let alloc = BYTES.load(Ordering::Relaxed).saturating_sub(before);
                let charged = gate.budget.charged_bytes();
                let err = result.expect_err("premise: the probe argument must not parse");
                seen.push((alloc, charged, err.len()));
                if alloc == err.len() as u64 && charged == err.len() as u64 {
                    break;
                }
            }
            let held = seen
                .iter()
                .any(|&(alloc, charged, len)| alloc == len as u64 && charged == len as u64);
            assert!(
                held,
                "{name}{}: the charge must be the EXACT rendered length and the render \
                 must be ONE allocation of it (#294 AC-9b), and it did not hold on any \
                 of {} samples — (allocated, charged, err.len()) = {seen:?}. Every \
                 sample deviating is a REAL defect, not the instrument: the measured \
                 artefact rate is ~1 in 4,000 measurements, so three in a row is ~1 in \
                 6e10, while a charge-after-allocate or a materialised temporary \
                 deviates on every call by megabytes",
                if invalid { " (invalid utf-8)" } else { "" },
                seen.len()
            );
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
