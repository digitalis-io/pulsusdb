//! Issue #230 review rounds 2–3: the RUNTIME allocation gate over the
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
        "substr" => return vec![Value::int(0), Value::int(200_000), big_str()],
        "trunc" => return vec![Value::int(200_000), big_str()],
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

/// BRANCH shapes beyond the happy path (review round 3: `go_replace`'s
/// identity/no-match/`n == 0` early returns and `align`'s identity
/// copies passed a single-shape gate while staying uncharged). Each
/// entry is `(shape_name, args)`; every function additionally gets an
/// all-empty-inputs shape.
fn extra_shapes(name: &str) -> Vec<(&'static str, Vec<Value<'static>>)> {
    match name {
        "Replace" => vec![
            (
                "identity-old==new",
                vec![big_str(), s("x"), s("x"), Value::int(-1)],
            ),
            (
                "no-match",
                vec![big_str(), s("ZZZ"), s("y"), Value::int(-1)],
            ),
            ("n==0", vec![big_str(), s("x"), s("y"), Value::int(0)]),
            (
                "empty-needle",
                vec![big_str(), s(""), s("-"), Value::int(-1)],
            ),
        ],
        "replace" => vec![
            ("identity-old==new", vec![s("x"), s("x"), big_str()]),
            ("no-match", vec![s("ZZZ"), s("y"), big_str()]),
            ("empty-needle", vec![s(""), s("-"), big_str()]),
        ],
        "alignLeft" | "alignRight" => vec![
            ("truncate", vec![Value::int(200_000), big_str()]),
            (
                "identity-count==len",
                vec![Value::int(BIG as i64), big_str()],
            ),
            ("identity-negative", vec![Value::int(-1), big_str()]),
        ],
        "trunc" => vec![
            (
                "identity-count>=len",
                vec![Value::int(2 * BIG as i64), big_str()],
            ),
            ("negative-tail", vec![Value::int(-200_000), big_str()]),
            ("zero", vec![Value::int(0), big_str()]),
        ],
        "substr" => vec![
            ("open-end", vec![Value::int(0), Value::int(-1), big_str()]),
            (
                "error-start>end",
                vec![Value::int(5), Value::int(2), big_str()],
            ),
            (
                "error-negative-end",
                vec![Value::int(-1), Value::int(-2), big_str()],
            ),
        ],
        "repeat" => vec![
            ("zero", vec![Value::int(0), big_str()]),
            ("error-negative", vec![Value::int(-1), big_str()]),
            ("error-overflow", vec![Value::int(i64::MAX), big_str()]),
        ],
        "Trim" | "TrimLeft" | "TrimRight" => vec![("all-trimmed", vec![big_str(), s("x")])],
        "trimAll" => vec![("all-trimmed", vec![s("x"), big_str()])],
        "TrimPrefix" | "TrimSuffix" => vec![("affix-matches-whole", vec![big_str(), big_str()])],
        "trimPrefix" | "trimSuffix" => vec![("affix-matches-whole", vec![big_str(), big_str()])],
        "b64dec" => vec![("error-as-value", vec![big_str()])],
        "urldecode" => vec![("error-bad-escape", {
            let mut v = vec![b'%'; 3];
            v.extend_from_slice(b"zz");
            vec![Value::Str(Cow::Owned(v))]
        })],
        "regexReplaceAll" | "regexReplaceAllLiteral" => vec![
            ("no-match", vec![s("ZZZ+"), big_str(), s("YY")]),
            ("error-bad-pattern", vec![s("("), big_str(), s("YY")]),
            // A big replacement so the auto-derived invalid-UTF-8
            // variant exercises the REPL conversion too (round 4).
            ("big-repl", vec![s("x+"), big_str(), big_str()]),
            // An uncached pattern keeps the 1 MiB dynamic-program
            // ceiling under dominance (the happy pattern now sits in
            // the pre-populated compile-time cache, mirroring
            // production literal precompilation).
            ("uncached-pattern", vec![s("y+"), big_str(), s("YY")]),
        ],
        "count" => vec![("error-bad-pattern", vec![s("("), big_str()])],
        "fromJson" => vec![
            ("error-not-json", vec![big_str()]),
            ("invalid-utf8-string", {
                let mut j = b"{\"x\":\"".to_vec();
                j.extend(std::iter::repeat_n(0xFF, BIG / 2));
                j.extend_from_slice(b"\"}");
                vec![Value::Str(Cow::Owned(j))]
            }),
        ],
        "indent" | "nindent" => vec![
            ("wide", vec![Value::int(2_000_000), s("a\nb\nc")]),
            ("error-negative", vec![Value::int(-1), big_str()]),
        ],
        "default" => vec![
            ("winner-default", vec![big_str(), s("")]),
            ("winner-given", vec![s("d"), big_str()]),
        ],
        "bytes" | "duration" | "duration_seconds" | "unixToTime" => {
            vec![("error-unparseable", vec![big_str()])]
        }
        _ => Vec::new(),
    }
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

#[test]
fn every_registry_function_charge_dominates_its_allocations() {
    use pulsus_read::logql::template::MAX_TEMPLATE_RENDER_BYTES;
    let env = TemplateEnv::process();
    let line = vec![b'L'; BIG];
    // Literal patterns are compiled ONCE at query compile in production
    // and every per-line call hits this cache — mirrored here so the
    // ordering leg reaches the argument conversions instead of
    // breaching at the dynamic-program ceiling first (round 4).
    let mut regex_cache: HashMap<String, regex::Regex> = HashMap::new();
    for pat in ["x+", "ZZZ+"] {
        regex_cache.insert(pat.to_string(), regex::Regex::new(pat).expect("pattern"));
    }
    let mut failures = Vec::new();
    for def in REGISTRY.iter() {
        // Branch shapes, not just the happy path (review round 3):
        // identity / no-match / n==0 / empty / error arguments each get
        // their own dominance run — plus a DERIVED invalid-UTF-8
        // variant of every shape (round 4).
        let mut shapes: Vec<(String, Vec<Value<'static>>)> = vec![
            (
                "happy".to_string(),
                args_for(def.name, def.params, def.variadic),
            ),
            ("empty".to_string(), empty_args(def.params, def.variadic)),
        ];
        shapes.extend(
            extra_shapes(def.name)
                .into_iter()
                .map(|(n, a)| (n.to_string(), a)),
        );
        let invalid: Vec<(String, Vec<Value<'static>>)> = shapes
            .iter()
            .filter_map(|(n, a)| invalidate_utf8(a).map(|ia| (format!("{n}+invalid-utf8"), ia)))
            .collect();
        shapes.extend(invalid);
        for (shape, args) in shapes {
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
            drop(result);
            // --- the ORDERING leg (charge BEFORE allocate) -------------
            // Rerun retainable shapes that allocated big under a nearly
            // exhausted budget: correct ordering breaches at the charge
            // and returns before the copy, so allocation stays tiny; a
            // charge moved AFTER its allocation leaves the big copy on
            // the counter. (This is what a post-hoc charged-vs-allocated
            // comparison can never see.)
            if retainable_ok && alloc >= 4 * SLACK {
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
    assert!(
        failures.is_empty(),
        "registry functions whose allocations are NOT dominated by their budget \
         charges (the fifth-class detector — charge the output/intermediate before \
         constructing it):\n{}",
        failures.join("\n")
    );
}
