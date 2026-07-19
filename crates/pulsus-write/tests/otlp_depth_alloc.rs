//! Allocation-bound guard for `ensure_anyvalue_depth` (issue #115, track 1
//! codex re-review), modeled on `otlp_metrics_parse_alloc.rs`: a counting
//! global allocator — scoped to this one test binary — proves the depth
//! guard's auxiliary memory is O(nesting depth), NOT O(container width).
//!
//! The finding: a work-stack that pushes ALL of a container's children in one
//! step makes a WIDE (not deep) `ArrayValue`/`KvlistValue` drive peak stack
//! size to O(width), an allocation-DoS vector the frame-stack-of-iterators
//! rewrite closes. This gate pins the fix scale-invariantly: the guard's
//! allocation count for a wide-but-shallow container of width N equals that
//! for width 4N (AC-14 style, no wall-time assert) — it cannot depend on
//! width. It FAILS against the old push-all-children guard, whose `Vec` grows
//! (and reallocates) proportionally to the sibling count.
//!
//! Everything runs in the SINGLE `#[test]` below so no parallel test thread
//! can pollute the process-global counter.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};

struct CountingAlloc;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

// SAFETY: delegates verbatim to the system allocator; the only side effect
// is a relaxed atomic increment, which allocates nothing and cannot
// re-enter the allocator.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList};

use pulsus_write::protocols::otlp_depth::{MAX_ANYVALUE_DEPTH, ensure_anyvalue_depth};

/// A scalar `AnyValue` leaf. `IntValue` holds no heap data, so building the
/// fixture allocates only its owning `Vec`, never per-child — keeping the
/// fixture out of the guard's measured window entirely.
fn scalar() -> AnyValue {
    AnyValue {
        value: Some(Value::IntValue(0)),
    }
}

/// A wide, shallow `ArrayValue`: a single container holding `width` scalar
/// siblings (depth 2). The guard must walk all `width` siblings one at a time
/// from a single open frame — never buffering them.
fn wide_array(width: usize) -> AnyValue {
    AnyValue {
        value: Some(Value::ArrayValue(ArrayValue {
            values: (0..width).map(|_| scalar()).collect(),
        })),
    }
}

/// The kvlist analog of [`wide_array`]: one `KvlistValue` with `width` scalar
/// entries, exercising the `Kvlist` iterator arm.
fn wide_kvlist(width: usize) -> AnyValue {
    AnyValue {
        value: Some(Value::KvlistValue(KeyValueList {
            values: (0..width)
                .map(|_| KeyValue {
                    key: String::new(),
                    value: Some(scalar()),
                    key_strindex: 0,
                })
                .collect(),
        })),
    }
}

/// Counts allocations charged *only* during the `ensure_anyvalue_depth` call
/// on `value` (the fixture is already built), asserting acceptance.
fn guard_allocs(value: &AnyValue) -> u64 {
    let start = ALLOCS.load(Ordering::Relaxed);
    let result = ensure_anyvalue_depth(value);
    let allocs = ALLOCS.load(Ordering::Relaxed) - start;
    black_box(&result);
    result.expect("wide-but-shallow container is within the depth cap");
    allocs
}

/// Narrow and 4x-wider fixtures for the width scale-invariance arms. Small
/// absolute widths keep the fixture cheap; the property is width-independence,
/// not magnitude.
const WIDTH: usize = 4096;
const WIDTH_4X: usize = WIDTH * 4;

#[test]
fn depth_guard_aux_memory_is_width_invariant() {
    // -- warm-up: prime allocator internals so neither measured window pays a
    //    one-time cost the other does not. ----------------------------------
    black_box(guard_allocs(&wide_array(WIDTH)));
    black_box(guard_allocs(&wide_kvlist(WIDTH)));

    // -- (a) ArrayValue: allocations must not scale with sibling count -------
    let array_n = wide_array(WIDTH);
    let array_4n = wide_array(WIDTH_4X);
    let array_allocs_n = guard_allocs(&array_n);
    let array_allocs_4n = guard_allocs(&array_4n);

    assert_eq!(
        array_allocs_n, array_allocs_4n,
        "ensure_anyvalue_depth: wide ArrayValue guard allocated {array_allocs_n} for width \
         {WIDTH} but {array_allocs_4n} for width {WIDTH_4X} — auxiliary memory scales with \
         container WIDTH (the O(width) push-all-children regression), not nesting depth"
    );

    // -- (b) KvlistValue: same, through the Kvlist iterator arm --------------
    let kvlist_n = wide_kvlist(WIDTH);
    let kvlist_4n = wide_kvlist(WIDTH_4X);
    let kvlist_allocs_n = guard_allocs(&kvlist_n);
    let kvlist_allocs_4n = guard_allocs(&kvlist_4n);

    assert_eq!(
        kvlist_allocs_n, kvlist_allocs_4n,
        "ensure_anyvalue_depth: wide KvlistValue guard allocated {kvlist_allocs_n} for width \
         {WIDTH} but {kvlist_allocs_4n} for width {WIDTH_4X} — auxiliary memory scales with \
         container WIDTH, not nesting depth"
    );

    // -- (c) absolute ceiling: the whole shallow walk is a single frame-stack
    //    `Vec::with_capacity(MAX_ANYVALUE_DEPTH)` — one allocation, no growth,
    //    independent of width. Bounded well below any width-proportional
    //    count (a push-all-children `Vec` reallocates O(log width) times as it
    //    grows to hold every sibling). ---------------------------------------
    const AUX_CEILING: u64 = MAX_ANYVALUE_DEPTH as u64;
    assert!(
        array_allocs_n <= AUX_CEILING && kvlist_allocs_n <= AUX_CEILING,
        "ensure_anyvalue_depth aux allocations (array {array_allocs_n}, kvlist \
         {kvlist_allocs_n}) exceed the O(MAX_ANYVALUE_DEPTH) ceiling {AUX_CEILING} — the \
         frame-stack must not grow with container width"
    );
}
