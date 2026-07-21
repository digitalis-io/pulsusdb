//! Allocation-bound guard for issue #89 (retroactive re-review): the
//! multi-metric resolver's cache-enumeration walk must never allocate
//! proportionally to the resident cache's *name universe* size, even on a
//! reject-all selector that examines every name it is permitted to before
//! bailing to `ScanBudgetExceeded`. Modeled on `otlp_depth_alloc.rs` /
//! `logql_pipeline_alloc.rs`: a counting global allocator, scoped to this
//! one test binary, sums BYTES (not allocation *count* — the alloc-bound
//! test-flake lesson: a process-global counter measured over a tiny window
//! is noisy, but the correct walk's own byte footprint is a small,
//! deterministic, width-independent constant).
//!
//! **Why this is the NON-VACUOUS gate (plan round-2 review finding):** an
//! `examined`-counter unit test alone cannot distinguish the fix from a
//! regression that keeps the old pre-loop `by_metric.keys().collect() +
//! sort_unstable()` full-name-universe materialization *and* adds the same
//! loop counter — both report the identical `examined == budget + 1`,
//! because the counter only observes loop iterations, never the deleted
//! pre-loop step. Only a BYTES measurement observes the O(name-universe)
//! `Vec<&String>` a regressed pre-loop would allocate before the loop ever
//! runs:
//! - **Fixed walk (this repo's current code):** a reject-all `__name__`
//!   regex means every examined name `continue`s; the walk bails at
//!   `examined == scan_budget + 1`, touching at most that many of the
//!   `by_metric` entries by reference only — `groups`/`matched` are never
//!   allocated (nothing matches). Measured aux heap is a small, FLAT
//!   constant across a 2B-name and a 4B-name universe.
//! - **Regressed walk (the round-1 finding):** a pre-loop
//!   `let mut names: Vec<&String> = by_metric.keys().collect();
//!   names.sort_unstable();` materializes ALL resident names before the
//!   loop's counter can fire — `8 * N` bytes of pointer storage alone
//!   (`~800 KiB` at `N = 100_000`, `~1.6 MiB` at `N = 200_000`), which
//!   blows both the width-independent ceiling and the no-growth-between-
//!   universes bound asserted below by more than an order of magnitude.
//!
//! Everything runs in the SINGLE `#[test]` below so no parallel test
//! thread can pollute the process-global counter.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

struct CountingAlloc;

/// Total bytes requested from the allocator, process-global. Bytes (not
/// call count) are the load-bearing metric here — see the module doc for
/// why a regressed pre-loop name-universe materialization is only visible
/// in bytes, not in the `examined` loop counter.
static BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: delegates verbatim to the system allocator; the only side effect
// is a relaxed atomic add of the requested size, which allocates nothing
// and cannot re-enter the allocator.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        BYTES.fetch_add(layout.size() as u64, Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A grow reallocation charges the new size — this is what makes an
        // O(name-universe) `Vec<&String>`'s repeated doublings visible in
        // bytes.
        BYTES.fetch_add(new_size as u64, Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

use pulsus_read::metrics::{CacheSnapshot, MultiMetricResolution, MultiMetricScanProbe};

/// The scan budget under test (`B`).
const SCAN_BUDGET: u64 = 50_000;
/// `2B` distinct resident metric names.
const UNIVERSE_2B: usize = 100_000;
/// `4B` distinct resident metric names.
const UNIVERSE_4B: usize = 200_000;
/// Width-independent byte ceiling on the walk's auxiliary heap over the
/// reject-all path. The fixed walk's own footprint (borrows only, no
/// `groups`/`matched` allocation on a reject-all matcher) sits near zero
/// for any name-universe size; this ceiling is ~two orders of magnitude
/// under the regressed pre-loop's O(name-universe) footprint (`~800 KiB`
/// at `UNIVERSE_2B`), so it is non-vacuous against that regression while
/// leaving ample slack for stray process-global allocations in the
/// measured window.
const AUX_CEILING_BYTES: u64 = 64 * 1024;

/// Bytes charged *only* during one `resolve_for_test` call, asserting the
/// walk bails to `ScanBudgetExceeded` (never a false `Groups`/`Unresolvable`
/// on this reject-all fixture).
fn reject_bytes(probe: &MultiMetricScanProbe, snapshot: &CacheSnapshot) -> (u64, usize, u64) {
    let start = BYTES.load(Relaxed);
    let resolution = probe.resolve_for_test(snapshot, SCAN_BUDGET);
    let bytes = BYTES.load(Relaxed) - start;
    match resolution {
        MultiMetricResolution::ScanBudgetExceeded { examined, cap } => (bytes, examined, cap),
        other => panic!("expected ScanBudgetExceeded on the reject-all fixture, got {other:?}"),
    }
}

#[test]
fn multi_metric_scan_reject_aux_heap_is_name_universe_independent() {
    let probe = MultiMetricScanProbe::new_reject_all_for_test();
    let snap_2b = CacheSnapshot::with_distinct_metric_names_for_test(UNIVERSE_2B);
    let snap_4b = CacheSnapshot::with_distinct_metric_names_for_test(UNIVERSE_4B);

    // Warm-up OUTSIDE every measured window: compiles+caches the
    // reject-all regex (`RegexCache`'s one-time compile cost) and primes
    // allocator internals, so the measured windows below pay only the
    // walk's own steady-state cost.
    let (_, examined_warm, _) = reject_bytes(&probe, &snap_2b);
    assert_eq!(examined_warm, (SCAN_BUDGET + 1) as usize);
    let _ = reject_bytes(&probe, &snap_4b);

    let (bytes_2b, examined_2b, cap_2b) = reject_bytes(&probe, &snap_2b);
    let (bytes_4b, examined_4b, cap_4b) = reject_bytes(&probe, &snap_4b);

    // Secondary bound: the reject bail is reached deterministically at
    // `budget + 1` regardless of the name universe's size — the demoted
    // check this file supersedes as the PRIMARY, non-vacuous proof (see
    // module doc: this counter alone cannot detect a regressed pre-loop
    // full-map materialization).
    assert_eq!(examined_2b, (SCAN_BUDGET + 1) as usize);
    assert_eq!(cap_2b, SCAN_BUDGET);
    assert_eq!(examined_4b, (SCAN_BUDGET + 1) as usize);
    assert_eq!(cap_4b, SCAN_BUDGET);

    // PRIMARY: both universes' aux heap stays under a width-independent
    // ceiling ...
    assert!(
        bytes_2b <= AUX_CEILING_BYTES,
        "2B ({UNIVERSE_2B}-name) reject aux heap was {bytes_2b} bytes, over the \
         width-independent ceiling {AUX_CEILING_BYTES} — the walk is allocating \
         proportionally to the resident name universe (a regressed pre-loop \
         `keys().collect() + sort` materialization)"
    );
    assert!(
        bytes_4b <= AUX_CEILING_BYTES,
        "4B ({UNIVERSE_4B}-name) reject aux heap was {bytes_4b} bytes, over the \
         width-independent ceiling {AUX_CEILING_BYTES} — the walk is allocating \
         proportionally to the resident name universe (a regressed pre-loop \
         `keys().collect() + sort` materialization)"
    );
    // ... and does not grow with the name universe (a genuinely bounded
    // walk adds at most stray-allocation noise between the two; an O(N)
    // regression's footprint roughly doubles from 2B to 4B).
    let growth = bytes_4b.saturating_sub(bytes_2b);
    assert!(
        growth <= AUX_CEILING_BYTES,
        "aux heap grew by {growth} bytes from {UNIVERSE_2B} to {UNIVERSE_4B} resident names — \
         a name-universe-independent walk must not grow with resident cache size"
    );
}
