//! Scale-invariance allocation guard for the Zipkin decode-time byte budget
//! (issue #168, AC 9 drain proof), modeled on `remote_write_decode_alloc.rs`: a
//! counting global allocator — scoped to this one test binary — proves that an
//! over-budget decode's allocation stays under a width-INDEPENDENT ceiling that
//! is a small multiple of `MAX_DECODED_BYTES`, no matter how many more trailing
//! spans the body carries. `BoundedSpans` rejects the moment the running
//! decoded-byte estimate passes the budget, so serde_json STOPS before
//! deserializing the trailing spans — a regression that kept parsing them would
//! scale the measured bytes with the extra wire width and blow both asserts.
//!
//! Everything runs in the SINGLE `#[test]` below so no parallel test thread can
//! pollute the process-global counter. Per the project's alloc-bound testing
//! rule the asserts are byte CEILINGS, never exact allocation-count equalities.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};

struct CountingAlloc;

/// Total bytes requested from the allocator, process-global.
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

use pulsus_write::LogsIngestError;
use pulsus_write::protocols::otlp_prescan::MAX_DECODED_BYTES;
use pulsus_write::protocols::zipkin::{
    MAX_SPANS_PER_REQUEST, MAX_TAGS_PER_SPAN, ZipkinSpan, decode,
};

/// One tags-heavy span carrying `MAX_TAGS_PER_SPAN` distinct tags — the
/// worst-case single-span materialization (`size_of::<ZipkinSpan>() +
/// MAX_TAGS_PER_SPAN * size_of::<(String, String)>()`).
fn tags_heavy_span() -> String {
    let mut tags = String::with_capacity(MAX_TAGS_PER_SPAN * 10);
    tags.push('{');
    for i in 0..MAX_TAGS_PER_SPAN {
        if i > 0 {
            tags.push(',');
        }
        tags.push_str(&format!(r#""k{i}":"""#));
    }
    tags.push('}');
    format!(r#"{{"traceId":"0000000000000001","id":"0000000000000002","tags":{tags}}}"#)
}

/// An over-budget body: a prefix of tags-heavy spans crossing the byte budget,
/// followed by `extra` MINIMAL trailing spans serde must NOT deserialize once
/// the budget is crossed. `extra` is the width variable the ceiling must be
/// independent of (minimal spans keep the body small so `extra` can grow 4×).
fn over_budget_body(extra: usize) -> String {
    let full_w = std::mem::size_of::<ZipkinSpan>()
        + MAX_TAGS_PER_SPAN * std::mem::size_of::<(String, String)>();
    let cross_n = MAX_DECODED_BYTES / full_w + 3;
    assert!(
        cross_n * full_w > MAX_DECODED_BYTES,
        "prefix must cross the budget"
    );
    assert!(
        cross_n + extra < MAX_SPANS_PER_REQUEST,
        "under the span cap"
    );

    let heavy = tags_heavy_span();
    let mut body = String::from("[");
    for i in 0..cross_n {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&heavy);
    }
    for _ in 0..extra {
        body.push_str(r#",{"traceId":"","id":""}"#);
    }
    body.push(']');
    body
}

/// Bytes requested during the `decode` call alone (the fixture is prebuilt),
/// asserting the byte-budget reject fires.
fn decode_bytes(body: &str) -> u64 {
    let start = BYTES.load(Ordering::Relaxed);
    let result = decode(body.as_bytes());
    let bytes = BYTES.load(Ordering::Relaxed) - start;
    match black_box(result) {
        Err(LogsIngestError::ZipkinDecode(msg)) => {
            assert!(
                msg.contains("decoded bytes (estimated)"),
                "over-budget body must reject at the byte budget: {msg}"
            );
        }
        other => panic!("over-budget body must reject at the byte budget, got {other:?}"),
    }
    bytes
}

/// Width-independent allocation ceiling: a small multiple of the budget. The
/// reject bounds materialization to ~`MAX_DECODED_BYTES` of span structs plus
/// map/string growth; a regression that kept parsing the trailing spans scales
/// with `extra` and overshoots.
const CEILING_BYTES: u64 = 4 * MAX_DECODED_BYTES as u64;

/// Extra trailing-span width at 1× — and 4× to prove width independence.
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
         {CEILING_BYTES} — the byte-budget reject is materializing past the budget"
    );
    assert!(
        bytes_4n <= CEILING_BYTES,
        "over-budget decode allocated {bytes_4n} bytes at extra={}, over the ceiling \
         {CEILING_BYTES}",
        EXTRA * 4
    );
    // The 4x-wider trailing tail must not cost materially more: a genuine
    // stop-on-reject adds at most stray-allocation noise, never a
    // width-proportional buffer.
    let growth = bytes_4n.saturating_sub(bytes_n);
    assert!(
        growth <= CEILING_BYTES / 8,
        "decode allocation grew by {growth} bytes from extra={EXTRA} to extra={} — the \
         over-budget reject must be O(1) in the trailing wire width",
        EXTRA * 4
    );
}
