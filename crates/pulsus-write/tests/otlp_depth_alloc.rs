//! Allocation-bound guard for `ensure_anyvalue_depth` (issue #115, track 1
//! codex re-review), modeled on `otlp_metrics_parse_alloc.rs`: a counting
//! global allocator — scoped to this one test binary — proves the depth
//! guard's auxiliary memory is O(nesting depth), NOT O(container width).
//!
//! The finding: a work-stack that pushes ALL of a container's children in one
//! step makes a WIDE (not deep) `ArrayValue`/`KvlistValue` drive peak stack
//! size to O(width), an allocation-DoS vector the frame-stack-of-iterators
//! rewrite closes. This gate pins the fix by BYTES, not allocation *count*:
//! the correct guard's only heap cost is a single
//! `Vec::with_capacity(MAX_ANYVALUE_DEPTH)` frame stack (~1 KiB, fixed) plus
//! the fast-path zero for scalar roots, so its bytes are a small
//! width-INDEPENDENT constant; the old push-all-children guard's `Vec` grows to
//! hold every sibling, so its bytes are O(width) — hundreds of KiB for the wide
//! fixtures below. Asserting a width-independent byte CEILING (rather than an
//! exact allocation-*count* equality) is what makes this robust: a
//! process-global allocation COUNTER measured over a tiny window catches stray
//! runtime allocations that vary run-to-run and grow with the longer walk of a
//! wider input (the flake that turned an exact-count assertion red in CI),
//! whereas the guard's own BYTE footprint is deterministic and the O(width)
//! regression overshoots the ceiling by two orders of magnitude.
//!
//! Everything runs in the SINGLE `#[test]` below so no parallel test thread
//! can pollute the process-global counter.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};

struct CountingAlloc;

/// Total bytes requested from the allocator, process-global. Bytes (not call
/// count) are the load-bearing metric: the correct guard's footprint is a
/// fixed ~1 KiB frame stack regardless of width, while an O(width) regression
/// allocates a sibling buffer proportional to the level's width.
static BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: delegates verbatim to the system allocator; the only side effect is
// a relaxed atomic add of the requested size, which allocates nothing and
// cannot re-enter the allocator.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A grow reallocation charges the new size; this is what makes an
        // O(width) sibling `Vec`'s repeated doublings visible in bytes.
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList};

use pulsus_write::protocols::otlp_depth::ensure_anyvalue_depth;

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

/// Bytes charged *only* during the `ensure_anyvalue_depth` call on `value` (the
/// fixture is already built), asserting acceptance.
fn guard_bytes(value: &AnyValue) -> u64 {
    let start = BYTES.load(Ordering::Relaxed);
    let result = ensure_anyvalue_depth(value);
    let bytes = BYTES.load(Ordering::Relaxed) - start;
    black_box(&result);
    result.expect("wide-but-shallow container is within the depth cap");
    bytes
}

/// Narrow and 4x-wider fixtures for the width scale-invariance arms. Small
/// absolute widths keep the fixture cheap; the property is width-independence,
/// not magnitude.
const WIDTH: usize = 4096;
const WIDTH_4X: usize = WIDTH * 4;

/// Width-independent byte ceiling on the guard's auxiliary heap. The correct
/// guard allocates a single `Vec::with_capacity(MAX_ANYVALUE_DEPTH)` frame
/// stack (~`MAX_ANYVALUE_DEPTH * size_of::<Frame>()` bytes, well under 2 KiB)
/// and nothing more, for ANY width. This ceiling sits ~two orders of magnitude
/// below the O(width) footprint of the old push-all-children guard (a sibling
/// `Vec` of `WIDTH_4X = 16384` `(&AnyValue, usize)` entries reallocates through
/// hundreds of KiB), so it is non-vacuous against that regression while leaving
/// ample slack for stray process-global allocations in the measured window.
const AUX_CEILING_BYTES: u64 = 64 * 1024;

#[test]
fn depth_guard_aux_memory_is_width_bounded() {
    // -- warm-up: prime allocator internals so the measured windows do not pay
    //    a one-time cost. ----------------------------------------------------
    black_box(guard_bytes(&wide_array(WIDTH)));
    black_box(guard_bytes(&wide_kvlist(WIDTH)));

    // -- (a) ArrayValue: auxiliary bytes must not scale with sibling count ----
    let array_n = wide_array(WIDTH);
    let array_4n = wide_array(WIDTH_4X);
    let array_bytes_n = guard_bytes(&array_n);
    let array_bytes_4n = guard_bytes(&array_4n);

    // -- (b) KvlistValue: same, through the Kvlist iterator arm --------------
    let kvlist_n = wide_kvlist(WIDTH);
    let kvlist_4n = wide_kvlist(WIDTH_4X);
    let kvlist_bytes_n = guard_bytes(&kvlist_n);
    let kvlist_bytes_4n = guard_bytes(&kvlist_4n);

    // Every arm — narrow and 4x-wider, array and kvlist — must stay under the
    // width-independent ceiling. A push-all-children guard buffers every
    // sibling, so its bytes climb with WIDTH and blow past this bound at both
    // 4096 and 16384 (making the assertion non-vacuous); the frame-stack guard
    // holds one iterator per open level, so all four measurements are the same
    // fixed frame-stack footprint plus incidental noise.
    for (label, bytes) in [
        ("array/N", array_bytes_n),
        ("array/4N", array_bytes_4n),
        ("kvlist/N", kvlist_bytes_n),
        ("kvlist/4N", kvlist_bytes_4n),
    ] {
        assert!(
            bytes <= AUX_CEILING_BYTES,
            "ensure_anyvalue_depth aux heap for {label} was {bytes} bytes, over the \
             width-independent ceiling {AUX_CEILING_BYTES} — auxiliary memory scales with \
             container WIDTH (the O(width) push-all-children regression), not nesting depth"
        );
    }

    // The 4x-wider fixtures must not cost materially more than the narrow ones:
    // a genuinely width-independent guard adds at most stray-allocation noise
    // between the two, never a width-proportional buffer. Allow generous slack
    // for process-global noise while still failing a footprint that grows with
    // the 4x sibling count.
    let array_growth = array_bytes_4n.saturating_sub(array_bytes_n);
    let kvlist_growth = kvlist_bytes_4n.saturating_sub(kvlist_bytes_n);
    assert!(
        array_growth <= AUX_CEILING_BYTES && kvlist_growth <= AUX_CEILING_BYTES,
        "ensure_anyvalue_depth aux heap grew by array {array_growth} / kvlist {kvlist_growth} \
         bytes from width {WIDTH} to {WIDTH_4X} — a width-independent guard must not grow with \
         sibling count"
    );
}
