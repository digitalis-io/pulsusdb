//! Scale-invariance allocation guard for the Loki-push protobuf decode-time
//! byte budget (issue #168, AC 5 drain proof), modeled on
//! `remote_write_decode_alloc.rs`: a counting global allocator — scoped to this
//! one test binary — proves that an over-budget decode's allocation stays under
//! a width-INDEPENDENT ceiling that is a small multiple of `MAX_DECODED_BYTES`,
//! no matter how many more structured-metadata-bearing entries the body carries.
//! Once the running decoded-byte estimate passes the budget, the per-entry
//! interposer DRAINS further entries without materializing them — a regression
//! that kept materializing would scale the measured bytes with the extra wire
//! width and blow both asserts.
//!
//! Everything runs in the SINGLE `#[test]` below so no parallel test thread can
//! pollute the per-thread counter. Per the project's alloc-bound testing
//! rule the asserts are byte CEILINGS, never exact allocation-count equalities
//! (per-thread counters flake on stray allocations).

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

use pulsus_write::LogsIngestError;
use pulsus_write::protocols::loki_push::{
    EntryAdapter, LabelPairAdapter, MAX_ENTRIES_PER_STREAM, MAX_STRUCTURED_METADATA_PER_ENTRY,
    decode_protobuf,
};
use pulsus_write::protocols::otlp_prescan::MAX_DECODED_BYTES;

/// Appends a base-128 varint.
fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

/// One `StreamAdapter.entries` (tag 2) wire record carrying
/// `MAX_STRUCTURED_METADATA_PER_ENTRY` empty `LabelPairAdapter` pairs
/// (`0x1a 0x00` each — 2 wire bytes, `size_of::<LabelPairAdapter>()` heap bytes).
fn full_entry_wire() -> Vec<u8> {
    let mut payload = Vec::with_capacity(MAX_STRUCTURED_METADATA_PER_ENTRY * 2);
    for _ in 0..MAX_STRUCTURED_METADATA_PER_ENTRY {
        payload.extend_from_slice(&[0x1a, 0x00]);
    }
    let mut out = vec![0x12];
    put_uvarint(&mut out, payload.len() as u64);
    out.extend_from_slice(&payload);
    out
}

/// An over-budget body: ONE stream whose 256-pair entries cross the byte budget
/// partway through, followed by `extra` further full entries the byte gate must
/// DRAIN without materializing. `extra` is the width variable the ceiling must
/// be independent of.
fn over_budget_body(extra: usize) -> Vec<u8> {
    let full_w = std::mem::size_of::<EntryAdapter>()
        + MAX_STRUCTURED_METADATA_PER_ENTRY * std::mem::size_of::<LabelPairAdapter>();
    let cross_n = MAX_DECODED_BYTES / full_w + 1;
    assert!(
        cross_n * full_w > MAX_DECODED_BYTES,
        "prefix must cross the budget"
    );
    assert!(
        cross_n + extra <= MAX_ENTRIES_PER_STREAM,
        "entries fit one stream under the count cap"
    );

    let entry = full_entry_wire();
    let mut stream_payload = Vec::with_capacity(entry.len() * (cross_n + extra));
    for _ in 0..cross_n + extra {
        stream_payload.extend_from_slice(&entry);
    }
    let mut body = vec![0x0a];
    put_uvarint(&mut body, stream_payload.len() as u64);
    body.extend_from_slice(&stream_payload);
    body
}

/// Bytes requested during the `decode_protobuf` call alone (the fixture is
/// prebuilt), asserting the byte-budget reject fires.
fn decode_bytes(body: &[u8]) -> u64 {
    let start = bytes_here();
    let result = decode_protobuf(body);
    let bytes = bytes_here() - start;
    match black_box(result) {
        Err(LogsIngestError::OversizeMessage { field, .. }) => {
            assert_eq!(field, "decoded bytes (estimated)");
        }
        other => panic!("over-budget body must reject at the byte budget, got {other:?}"),
    }
    bytes
}

/// Width-independent allocation ceiling: a small multiple of the budget. The
/// drain bounds materialization to ~`MAX_DECODED_BYTES` of structs plus Vec
/// doubling-growth (each realloc's full new size is counted), so the honest
/// footprint sits near 2-3× the budget for ANY wire width; a regression that
/// materialized the drained entries scales with `extra` and overshoots.
const CEILING_BYTES: u64 = 4 * MAX_DECODED_BYTES as u64;

/// Extra drained-entry width at 1× — and 4× to prove width independence.
const EXTRA: usize = 10_000;

#[test]
fn over_budget_decode_allocations_are_width_bounded() {
    let body_n = over_budget_body(EXTRA);
    let body_4n = over_budget_body(EXTRA * 4);
    let bytes_n = decode_bytes(&body_n);
    let bytes_4n = decode_bytes(&body_4n);

    assert!(
        bytes_n <= CEILING_BYTES,
        "over-budget decode allocated {bytes_n} bytes at extra={EXTRA}, over the ceiling \
         {CEILING_BYTES} — the byte-budget drain is materializing past the budget"
    );
    assert!(
        bytes_4n <= CEILING_BYTES,
        "over-budget decode allocated {bytes_4n} bytes at extra={}, over the ceiling \
         {CEILING_BYTES}",
        EXTRA * 4
    );
    // The 4x-wider drained tail must not cost materially more: a genuine drain
    // adds at most stray-allocation noise, never a width-proportional buffer.
    let growth = bytes_4n.saturating_sub(bytes_n);
    assert!(
        growth <= CEILING_BYTES / 8,
        "decode allocation grew by {growth} bytes from extra={EXTRA} to extra={} — the \
         over-budget drain must be O(1) in the drained wire width",
        EXTRA * 4
    );
}
