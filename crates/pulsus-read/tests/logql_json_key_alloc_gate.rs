//! **The `| json` key ledger covers what the flatten asks the allocator
//! for** (issue #287 review round 1 finding 1, as re-judged in round 2).
//!
//! Round 1 asserted `String::capacity() == String::len()`. That is not a
//! property the language gives you — `String::with_capacity(n)`
//! guarantees `capacity >= n` and nothing more — so the assertion held
//! only by the standard library's current behaviour, and an allocator
//! that over-provisioned would have made a true charge look false (or,
//! worse, a false one look true).
//!
//! The charge is now `charge::alloc_block_bytes(len)`, this crate's
//! pinned over-approximation of the block a real allocator RETAINS for
//! ONE exactly-reserved allocation. That model has a precondition — one
//! request, of exactly the final length — and the precondition is what
//! this gate measures, from the allocator rather than from the `String`:
//! the bytes the flatten REQUESTS must stay inside the bytes the ledger
//! charged.
//!
//! A `format!` join breaks the precondition rather than the arithmetic.
//! It reserves nothing up front, then requests `prefix.len()` and
//! reallocates to `2 · prefix.len()` — `3p` requested where the ledger
//! charged `2 · (p + 7)`. Note that its final CAPACITY, `2p`, still sits
//! under `alloc_block_bytes(p + 7)`, so no capacity-based assertion at
//! any threshold can separate the two shapes; only the request traffic
//! can. That is the whole reason this gate exists as its own binary.
//!
//! Own binary, one `#[test]`, a byte CEILING and never an exact count —
//! the counting allocator is process-global (the project's alloc-gate
//! flake rule).

use std::alloc::{GlobalAlloc, Layout, System};
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

use pulsus_read::logql::pipeline::CompiledPipeline;

/// A 64 KiB parent key over 500 leaves. The emitted keys are 32.8 MB of
/// the fixture's ~33 MB of allocator traffic, so the ceiling below is
/// about the KEYS: everything else the run touches — the parsed value's
/// own key strings, the pair vector, the one-byte values — is 0.4 % of
/// it and cannot move the verdict either way.
const PREFIX: usize = 64 * 1024;
const LEAVES: usize = 500;

fn line() -> String {
    let mut s = String::with_capacity(PREFIX + 12 * LEAVES + 8);
    s.push_str("{\"");
    for _ in 0..PREFIX {
        s.push('a');
    }
    s.push_str("\":{");
    for i in 0..LEAVES {
        if i > 0 {
            s.push(',');
        }
        s.push_str("\"k");
        s.push_str(&format!("{i:05}"));
        s.push_str("\":0");
    }
    s.push_str("}}");
    s
}

fn compiled(query: &str) -> CompiledPipeline {
    let expr = pulsus_logql::parse(query).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query: {query}");
    };
    CompiledPipeline::compile(&log.pipeline).expect("compile")
}

#[test]
fn the_flatten_requests_no_more_than_the_key_ledger_charged() {
    // `alloc_block_bytes` is `2 · content` above its 32-byte floor, and
    // every key here is far above it. Spelled out rather than imported
    // so the gate states its own ceiling.
    let block = |content: usize| 2 * content;

    let compiled = compiled(r#"{app="a"} | json"#);
    let line = line();

    // The keys the flatten builds: one intermediate prefix, then one
    // leaf per field.
    let prefix_key = PREFIX;
    let leaf_key = PREFIX + 1 + 6;
    let charged = block(prefix_key) + LEAVES * block(leaf_key);

    // Warm every lazily-initialised path, so the measured window holds
    // the run and not one-time setup.
    drop(compiled.run(&line, &[], 0).expect("no breach"));

    let before = BYTES.load(Ordering::Relaxed);
    let out = compiled
        .run(&line, &[], 0)
        .expect("well inside the key budget")
        .expect("kept");
    let requested = BYTES.load(Ordering::Relaxed) - before;

    let emitted: usize = out.labels.iter().map(|(k, _)| k.len()).sum();
    assert_eq!(
        emitted,
        LEAVES * leaf_key,
        "the fixture's keys are as sized"
    );

    assert!(
        requested <= charged as u64,
        "the flatten requested {requested} bytes against a {charged}-byte charge — a key is \
         being grown into rather than reserved (a `format!` join requests ~3x the prefix per \
         key where the ledger charged ~2x)"
    );

    // Non-vacuity, stated as a ceiling and not an equality: the keys
    // have to DOMINATE the traffic, or the assertion above would pass
    // for a fixture that never exercised them.
    assert!(
        requested > (LEAVES * leaf_key) as u64,
        "the measured window ({requested} B) must at least contain the keys it is bounding"
    );
}
