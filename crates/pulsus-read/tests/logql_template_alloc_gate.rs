//! Issue #230 review round 2: the RUNTIME allocation-dominance gate
//! over the whole template function registry — the executable evidence
//! behind the census's `CHARGED` dispositions (the static census proves
//! a charge call EXISTS; this gate proves it DOMINATES the allocation).
//!
//! Every one of the 67 registry functions is invoked with adversarial
//! large inputs under a byte-counting allocator and one fresh
//! `RenderBudget`, and:
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

/// Per-function argument overrides steering the call onto its SUCCESS
/// path with the largest reachable output (the dominance bound bites on
/// success; error paths get the looser once-per-render bound).
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

#[test]
fn every_registry_function_charge_dominates_its_allocations() {
    let env = TemplateEnv::process();
    let line = vec![b'L'; BIG];
    let regex_cache: HashMap<String, regex::Regex> = HashMap::new();
    let mut failures = Vec::new();
    for def in REGISTRY.iter() {
        let args = args_for(def.name, def.params, def.variadic);
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
                "{}: allocated {alloc} B > {kind} bound {bound} B (charged {charged} B, \
                 inputs {inputs} B, result {})",
                def.name,
                match &result {
                    Ok(v) => v.type_name().to_string(),
                    Err(e) => format!("Err({e})"),
                }
            ));
        }
        drop(result);
    }
    assert!(
        failures.is_empty(),
        "registry functions whose allocations are NOT dominated by their budget \
         charges (the fifth-class detector — charge the output/intermediate before \
         constructing it):\n{}",
        failures.join("\n")
    );
}
