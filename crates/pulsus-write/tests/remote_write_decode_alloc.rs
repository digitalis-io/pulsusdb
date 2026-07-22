//! Scale-invariance allocation guard for the remote-write decode-time byte
//! budget (issue #127, AC 8 drain proof), modeled on `otlp_prescan_alloc.rs`:
//! a counting global allocator — scoped to this one test binary — proves that
//! an over-budget decode's allocation stays under a width-INDEPENDENT ceiling
//! that is a small multiple of `MAX_DECODED_BYTES`, no matter how many more
//! wire elements the body carries. Once the running decoded-byte estimate
//! passes the budget, `BoundedWriteRequest` DRAINS further series without
//! materializing them — a regression that kept materializing would scale the
//! measured bytes with the extra wire width and blow both asserts.
//!
//! Everything runs in the SINGLE `#[test]` below so no parallel test thread
//! can pollute the process-global counter. Per the project's alloc-bound
//! testing rule the asserts are byte CEILINGS, never exact allocation-count
//! equalities (process-global counters flake on stray allocations).

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};

struct CountingAlloc;

/// Total bytes requested from the allocator, process-global.
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
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

use pulsus_write::LogsIngestError;
use pulsus_write::protocols::otlp_prescan::MAX_DECODED_BYTES;
use pulsus_write::protocols::remote_write::{
    Label, MAX_LABELS_PER_SERIES, MAX_SAMPLES_PER_SERIES, MAX_TOTAL_LABELS_PER_REQUEST,
    MAX_TOTAL_SAMPLES_PER_REQUEST, Sample, TimeSeries, decode,
};

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

/// One `WriteRequest.timeseries` wire occurrence of `labels` empty labels and
/// `samples` empty samples (2 wire bytes each; the structs are NOT built here).
fn wire_series(out: &mut Vec<u8>, labels: usize, samples: usize) {
    let mut payload = Vec::with_capacity(2 * (labels + samples));
    for _ in 0..labels {
        payload.extend_from_slice(&[0x0A, 0x00]);
    }
    for _ in 0..samples {
        payload.extend_from_slice(&[0x12, 0x00]);
    }
    out.push(0x0A);
    put_uvarint(out, payload.len() as u64);
    out.extend_from_slice(&payload);
}

/// An over-budget body (the `remote_write_fixtures.rs` AC-8 shape: label and
/// sample fan-out at — never over — every count cap, crossing the byte budget
/// partway through the sample series) followed by `extra` further full-label
/// series the byte gate must DRAIN without materializing. `extra` is the
/// width variable the ceiling must be independent of.
fn over_budget_body(extra: usize) -> Vec<u8> {
    let label_series = MAX_TOTAL_LABELS_PER_REQUEST / MAX_LABELS_PER_SERIES;
    let sample_series = MAX_TOTAL_SAMPLES_PER_REQUEST / MAX_SAMPLES_PER_SERIES;
    let estimate = label_series
        * (std::mem::size_of::<TimeSeries>()
            + MAX_LABELS_PER_SERIES * std::mem::size_of::<Label>())
        + sample_series
            * (std::mem::size_of::<TimeSeries>()
                + MAX_SAMPLES_PER_SERIES * std::mem::size_of::<Sample>());
    assert!(estimate > MAX_DECODED_BYTES, "prefix must cross the budget");

    let mut body = Vec::new();
    for _ in 0..label_series {
        wire_series(&mut body, MAX_LABELS_PER_SERIES, 0);
    }
    for _ in 0..sample_series {
        wire_series(&mut body, 0, MAX_SAMPLES_PER_SERIES);
    }
    for _ in 0..extra {
        wire_series(&mut body, MAX_LABELS_PER_SERIES, 0);
    }
    body
}

/// Bytes requested during the `decode` call alone (the fixture is prebuilt),
/// asserting the byte-budget reject fires.
fn decode_bytes(body: &[u8]) -> u64 {
    let start = BYTES.load(Ordering::Relaxed);
    let result = decode(body);
    let bytes = BYTES.load(Ordering::Relaxed) - start;
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
/// footprint sits near 2–3× the budget for ANY wire width; a regression that
/// materialized the drained series scales with `extra` (hundreds of MiB more
/// at 4×) and overshoots.
const CEILING_BYTES: u64 = 4 * MAX_DECODED_BYTES as u64;

/// Extra drained-series width at 1× — and 4× to prove width independence.
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
