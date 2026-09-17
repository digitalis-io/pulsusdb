//! Allocation-bound guard for the bounded OTLP/JSON traces decode's unknown-key
//! handling (issue #115 track 6a, codex round-2 finding 2), modeled on
//! `otlp_depth_alloc.rs`: a counting global allocator — scoped to this one test
//! binary — proves a GENUINELY UNKNOWN key's (attacker-controlled) value is NOT
//! materialized into a `serde_json::Value` before the derive discards it.
//!
//! The finding: every buffer-and-delegate seed routed unknown keys through
//! `map.next_value::<serde_json::Value>()`, fully building an arbitrarily wide
//! unknown value tree (O(width) heap) only to have the vendored derive — which
//! carries no `deny_unknown_fields` — ignore it. The fix skips unknown values
//! with `serde::de::IgnoredAny`, so the decode's heap is width-INDEPENDENT.
//!
//! This gate pins the fix by BYTES, not allocation count: the correct decode's
//! heap cost is bounded by the small in-bounds envelope (a handful of `Vec`s),
//! independent of the unknown value's width; the old materialize-then-discard
//! path allocates a `Vec<serde_json::Value>` proportional to the unknown array's
//! width, so its bytes scale with WIDTH. Asserting a width-independent ceiling
//! (rather than an exact count) is what makes this robust to stray run-to-run
//! allocations, while the O(width) regression overshoots by orders of magnitude.
//!
//! Everything runs in the SINGLE `#[test]` below so no parallel test thread can
//! pollute the per-thread counter.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;

struct CountingAlloc;

// Total bytes requested from the allocator, per thread.

// SAFETY: delegates verbatim to the system allocator; the only side effect is a
// relaxed atomic add of the requested size, which allocates nothing and cannot
// re-enter the allocator.
// The counters are **per thread**, not per process.
//
// A `#[global_allocator]` serves every thread in the test binary, so a
// single process-wide figure charges whatever any other thread allocates
// to whichever measured window happens to be open — and an allocation
// made elsewhere is then indistinguishable from one the code under test
// made itself. Three CI failures came from exactly that, none of them
// caused by the change being tested.
//
// `const`-initialised, so the slot is a plain thread-local word with no
// lazy heap box behind it and the allocator cannot re-enter itself; and
// written through `try_with` rather than `with`, because during
// thread-local destruction the slot is gone and `with` panics inside the
// allocator. An allocation at that point belongs to teardown and to no
// measured window, so dropping it is the right answer as well as the
// safe one.
thread_local! {
    static BYTES: Cell<u64> = const { Cell::new(0) };
}

/// What this thread has counted into `BYTES` so far.
fn bytes_here() -> u64 {
    BYTES.with(Cell::get)
}

/// Adds to this thread's `BYTES` tally.
fn charge_bytes(n: u64) {
    let _ = BYTES.try_with(|c| c.set(c.get() + n));
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge_bytes(layout.size() as u64);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        charge_bytes(new_size as u64);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

use pulsus_write::protocols::otlp_traces::decode_json;

/// A minimal in-bounds traces request whose ONLY non-repeated span field is a
/// GENUINELY UNKNOWN key (`__unknown__`) carrying an array of `width` integers.
/// The known envelope is fixed-size; the unknown array is what the pre-fix path
/// materialized into a `Vec<serde_json::Value>` and the fix skips.
fn body_with_unknown_wide_value(width: usize) -> String {
    let mut arr = String::with_capacity(width * 2 + 2);
    arr.push('[');
    for i in 0..width {
        if i > 0 {
            arr.push(',');
        }
        arr.push('1');
    }
    arr.push(']');
    format!(r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[{{"__unknown__":{arr}}}]}}]}}]}}"#)
}

/// Like [`body_with_unknown_wide_value`] but the unknown key (`__x__`) is nested
/// INSIDE the span's `status` MESSAGE object (alongside a valid `code`). `status`
/// is a message, not a scalar leaf — a naive scalar-classification would buffer
/// the whole status object (including this wide unknown array) via
/// `serde_json::Value` before the vendored `Status` derive discards it (issue
/// #115 track-6a round-3). The bounded `StatusSeed` must IgnoredAny-skip `__x__`.
fn body_with_unknown_wide_value_in_status(width: usize) -> String {
    let mut arr = String::with_capacity(width * 2 + 2);
    arr.push('[');
    for i in 0..width {
        if i > 0 {
            arr.push(',');
        }
        arr.push('1');
    }
    arr.push(']');
    format!(
        r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[{{"status":{{"code":0,"__x__":{arr}}}}}]}}]}}]}}"#
    )
}

/// Bytes charged *only* during `decode_json` (the body is already built),
/// asserting the unknown key is ignored and the request decodes `Ok`.
fn decode_bytes(body: &[u8]) -> u64 {
    let start = bytes_here();
    let result = decode_json(body);
    let bytes = bytes_here() - start;
    black_box(&result);
    result.expect("unknown key is ignored → request decodes Ok");
    bytes
}

/// Narrow and 4x-wider unknown-value fixtures. Small absolute widths keep the
/// fixtures cheap; the property under test is width-INDEPENDENCE, not magnitude.
const WIDTH: usize = 8192;
const WIDTH_4X: usize = WIDTH * 4;

/// Width-independent byte ceiling on the decode's heap. The correct decode skips
/// the unknown value (IgnoredAny) and allocates only the small in-bounds envelope
/// (one `ResourceSpans`/`ScopeSpans`/`Span`), a fixed cost for ANY unknown width.
/// The pre-fix materialize path builds a `Vec<serde_json::Value>` of the unknown
/// array — at WIDTH_4X = 32768 that is ~1 MiB of `serde_json::Value` nodes plus
/// the backing `Vec` — two orders of magnitude over this bound, so the assertion
/// is non-vacuous against that regression while leaving slack for stray noise.
const DECODE_CEILING_BYTES: u64 = 64 * 1024;

#[test]
fn unknown_key_value_is_not_materialized_width_independent() {
    // -- warm-up: prime allocator internals so the measured windows do not pay a
    //    one-time cost. --------------------------------------------------------
    black_box(decode_bytes(body_with_unknown_wide_value(WIDTH).as_bytes()));

    let narrow = body_with_unknown_wide_value(WIDTH);
    let wide = body_with_unknown_wide_value(WIDTH_4X);
    let narrow_bytes = decode_bytes(narrow.as_bytes());
    let wide_bytes = decode_bytes(wide.as_bytes());

    // Both widths must stay under the width-independent ceiling. The
    // materialize-then-discard regression allocates a `Vec<serde_json::Value>`
    // proportional to the unknown array width, blowing past this bound at both
    // 8192 and 32768 (non-vacuity); the IgnoredAny skip allocates nothing for the
    // unknown value, so both measurements are the fixed envelope footprint.
    for (label, bytes) in [("narrow", narrow_bytes), ("wide", wide_bytes)] {
        assert!(
            bytes <= DECODE_CEILING_BYTES,
            "decode_json heap for the {label} unknown value was {bytes} bytes, over the \
             width-independent ceiling {DECODE_CEILING_BYTES} — the unknown value is being \
             materialized (O(width)) rather than skipped via IgnoredAny"
        );
    }

    // The 4x-wider unknown value must not cost materially more than the narrow
    // one: a genuinely non-materializing decode adds at most stray-allocation
    // noise between the two, never a width-proportional buffer.
    let growth = wide_bytes.saturating_sub(narrow_bytes);
    assert!(
        growth <= DECODE_CEILING_BYTES,
        "decode_json heap grew by {growth} bytes as the unknown value widened from {WIDTH} to \
         {WIDTH_4X} — a non-materializing decode must not grow with the unknown value's width"
    );

    // -- Same property, one level deeper: the unknown key nested INSIDE the span's
    //    `status` MESSAGE (issue #115 track-6a round-3). A scalar-misclassification
    //    of `status` would materialize the wide unknown array via serde_json::Value
    //    before the vendored Status derive discards it; StatusSeed must IgnoredAny-
    //    skip it. Kept in THIS single #[test] (the file's invariant) so no parallel
    //    test thread pollutes the process-global byte counter. -------------------
    black_box(decode_bytes(
        body_with_unknown_wide_value_in_status(WIDTH).as_bytes(),
    ));
    let narrow = body_with_unknown_wide_value_in_status(WIDTH);
    let wide = body_with_unknown_wide_value_in_status(WIDTH_4X);
    let narrow_bytes = decode_bytes(narrow.as_bytes());
    let wide_bytes = decode_bytes(wide.as_bytes());

    for (label, bytes) in [("narrow", narrow_bytes), ("wide", wide_bytes)] {
        assert!(
            bytes <= DECODE_CEILING_BYTES,
            "decode_json heap for the {label} unknown-in-status value was {bytes} bytes, over the \
             width-independent ceiling {DECODE_CEILING_BYTES} — the unknown value nested in the \
             status message is being materialized rather than skipped via IgnoredAny"
        );
    }

    let status_growth = wide_bytes.saturating_sub(narrow_bytes);
    assert!(
        status_growth <= DECODE_CEILING_BYTES,
        "decode_json heap grew by {status_growth} bytes as the unknown-in-status value widened \
         from {WIDTH} to {WIDTH_4X} — StatusSeed must not materialize the unknown nested value"
    );
}
