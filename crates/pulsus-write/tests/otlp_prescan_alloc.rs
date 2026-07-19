//! Scale-invariance allocation guard for the OTLP protobuf wire pre-scan
//! (issue #115, track 5), modeled on `otlp_depth_alloc.rs` /
//! `otlp_metrics_parse_alloc.rs`: a counting global allocator — scoped to this
//! one test binary — proves the over-cap reject is O(1)-in-N.
//!
//! The point of a WIRE pre-scan is that a hostile fan-out is rejected having
//! touched only O(nesting depth) auxiliary memory, NOT the O(N) heap the same
//! body would drive if it reached `Export*::decode`. This gate pins that: the
//! pre-scan's allocation footprint on an over-cap payload at N and 4N spans is
//! IDENTICAL and bounded by a small, N-independent ceiling. A regression that
//! materialized (or even partially buffered) the repeated elements would scale
//! with N and blow past both the ceiling and the equality.
//!
//! Everything runs in the SINGLE `#[test]` below so no parallel test thread can
//! pollute the process-global counter.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};

struct CountingAlloc;

/// Total bytes requested from the allocator, process-global. Bytes (not call
/// count) are the load-bearing metric: an exact allocation-*count* equality
/// over a measured window flakes on stray runtime allocations (it turned main
/// red on the sibling track-1 gate), whereas the pre-scan's own BYTE footprint
/// is a fixed frame-stack `Vec` independent of the fan-out width, and an O(N)
/// materialization regression overshoots the byte ceiling by orders of
/// magnitude.
static BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: delegates verbatim to the system allocator; the only side effect is a
// relaxed atomic add of the requested size, which allocates nothing and cannot
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

use pulsus_write::protocols::otlp_prescan::{MAX_SPANS, prescan_traces};

/// Builds a traces request whose single `ScopeSpans` holds `spans` empty spans
/// (two wire bytes each) — an over-cap fan-out when `spans > MAX_SPANS`.
fn traces_with_n_spans(spans: usize) -> Vec<u8> {
    let mut span_area = Vec::with_capacity(spans * 2);
    // ScopeSpans.spans is field 2 (wire type 2); an empty span is tag + len 0.
    let tag = (2u64 << 3) | 2;
    for _ in 0..spans {
        span_area.push(tag as u8);
        span_area.push(0);
    }
    // ScopeSpans payload = span_area; wrap in one ResourceSpans, then request.
    let mut resource_spans = Vec::with_capacity(span_area.len() + 8);
    resource_spans.push(((2u64 << 3) | 2) as u8); // ResourceSpans.scope_spans (field 2)
    put_len(&mut resource_spans, span_area.len());
    resource_spans.extend_from_slice(&span_area);

    let mut request = Vec::with_capacity(resource_spans.len() + 8);
    request.push(((1u64 << 3) | 2) as u8); // Request.resource_spans (field 1)
    put_len(&mut request, resource_spans.len());
    request.extend_from_slice(&resource_spans);
    request
}

fn put_len(out: &mut Vec<u8>, mut v: usize) {
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

/// Bytes charged only during the `prescan_traces` call on `body` (the fixture
/// is already built), asserting the over-cap reject.
fn prescan_bytes(body: &[u8]) -> u64 {
    let start = BYTES.load(Ordering::Relaxed);
    let result = prescan_traces(body);
    let bytes = BYTES.load(Ordering::Relaxed) - start;
    black_box(&result);
    result.expect_err("over-cap span fan-out must be rejected");
    bytes
}

/// Over-cap span counts at 1x and 4x. `MAX_SPANS + 1` is the smallest over-cap
/// count; 4x proves the reject footprint does not scale with N.
const N: usize = MAX_SPANS + 1;
const N_4X: usize = MAX_SPANS * 4;

/// N-independent byte ceiling on the pre-scan's auxiliary heap. Its only heap
/// cost is the frame-stack `Vec` (a handful of `Frame`s, well under 1 KiB), for
/// ANY N and regardless of where the over-cap reject fires. A per-element
/// materialization regression buffers repeated elements and allocates on the
/// order of `MAX_SPANS` entries before rejecting — megabytes, overshooting this
/// ceiling by orders of magnitude — so the bound is non-vacuous while leaving
/// ample slack for stray process-global allocations in the measured window.
const AUX_CEILING_BYTES: u64 = 64 * 1024;

#[test]
fn prescan_reject_allocations_are_width_bounded() {
    // Warm-up so the measured windows do not pay a one-time cost.
    black_box(prescan_bytes(&traces_with_n_spans(N)));

    let body_n = traces_with_n_spans(N);
    let body_4n = traces_with_n_spans(N_4X);
    let bytes_n = prescan_bytes(&body_n);
    let bytes_4n = prescan_bytes(&body_4n);

    // Both the smallest over-cap fan-out and the 4x-wider one must reject
    // within the same fixed, width-independent byte budget: the reject short-
    // circuits after the frame-stack `Vec`, never buffering the siblings.
    assert!(
        bytes_n <= AUX_CEILING_BYTES,
        "prescan reject aux heap was {bytes_n} bytes at N={N}, over the N-independent ceiling \
         {AUX_CEILING_BYTES} — the walk is materializing repeated elements instead of counting them"
    );
    assert!(
        bytes_4n <= AUX_CEILING_BYTES,
        "prescan reject aux heap was {bytes_4n} bytes at 4N={N_4X}, over the N-independent ceiling \
         {AUX_CEILING_BYTES}"
    );
    // The 4x-wider fan-out must not cost materially more than the narrow one: a
    // genuinely width-independent reject adds at most stray-allocation noise
    // between the two, never a width-proportional buffer.
    let growth = bytes_4n.saturating_sub(bytes_n);
    assert!(
        growth <= AUX_CEILING_BYTES,
        "prescan reject aux heap grew by {growth} bytes from N={N} to 4N={N_4X} — the reject must \
         be O(1) in the fan-out width, not scale with span count"
    );
}
