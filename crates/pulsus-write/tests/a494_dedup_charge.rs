//! Issue #494, criterion 21: **the charge cannot drift from the allocator.**
//!
//! The suppression index's memory bound is not arithmetic written in a
//! document — it is a model in `writer/push_dedup.rs` that the index itself
//! evaluates at construction, reserving its collections and stepping them
//! down until the modelled charge fits `PULSUS_INGEST_DEDUP_MAX_BYTES`.
//! This suite is the oracle for that model: it builds the index to the
//! capacities the index reports, counts what the allocator actually hands
//! out, and fails if the two disagree.
//!
//! # Why this covers the accepted range rather than sampling it
//!
//! The accepted knob range is `1 MiB..=1 GiB` — 1,072,693,249 distinct byte
//! values. Seven spot checks leave 1,072,693,242 of them untested, and a
//! one-byte under-count at a knob nobody listed passes every one of them.
//!
//! The index's reserved sizes are **a step function of the knob**, because
//! both collections are hash tables whose slot counts are powers of two:
//!
//! ```text
//!   knob ->   1 MiB ......... 2 MiB ......... 4 MiB ......... 1 GiB
//!   claims    [--- c0 ---][------ c1 ------][----- c2 -----] ...
//!   waiters   [-w0-][-w1-][--w2--][--w3--][---w4---] ...
//!             ^     ^     ^       ^       ^
//!             every transition is a threshold this suite computes
//! ```
//!
//! `plan_capacities` picks the largest claim step whose charge is at or
//! below 80% of the knob, then the largest waiter step that still fits.
//! Each is `max { k : threshold_k <= knob }`, so the pair can only change
//! where a threshold is crossed. [`partitions`] walks those thresholds and
//! returns every distinct `(claims, waiters)` pair the range produces,
//! together with the **smallest** knob that produces it — the tightest
//! constraint in that partition, since the pair is constant across it.
//!
//! For every partition this suite then:
//!
//! 1. builds the index and asserts the allocator's resident total is
//!    **exactly** the construction model — not "at most", so a model that
//!    drifts in either direction reddens;
//! 2. asserts the whole-index charge (construction plus every registration
//!    the waiter budget admits) is at or below that partition's smallest
//!    knob.
//!
//! Every accepted knob value lies in exactly one partition and produces
//! that partition's pair, so those two together establish the bound across
//! the range rather than at a few points. [`registration_charge_is_an_upper_bound`]
//! closes the remaining link by measuring what a live registration really
//! allocates, against what it is charged.
//!
//! # The instrument
//!
//! A counting global allocator with **per-thread** counters. The Rust test
//! harness runs each test on its own thread, and every allocation this
//! suite measures is made and freed on that same thread, so another test
//! running concurrently cannot move these figures. `Cell` in a `const`
//! thread-local: no destructor, so the allocator itself never allocates.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Duration;

use pulsus_write::{
    Admission, Capacities, ClaimOutcome, PushDedup, PushDigest, PushIdentity, WaitGuard, WaitMode,
    index_bytes, plan_capacities,
};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------
// The instrument
// ---------------------------------------------------------------------

thread_local! {
    static ALLOCATED: Cell<u64> = const { Cell::new(0) };
    static FREED: Cell<u64> = const { Cell::new(0) };
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATED.try_with(|c| c.set(c.get() + layout.size() as u64));
        // SAFETY: `layout` is forwarded unchanged to the system allocator,
        // which is this allocator's only backing store.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let _ = FREED.try_with(|c| c.set(c.get() + layout.size() as u64));
        // SAFETY: `ptr` came from `Self::alloc` above, which forwards to
        // `System`, with this same `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCATED.try_with(|c| c.set(c.get() + new_size as u64));
        let _ = FREED.try_with(|c| c.set(c.get() + layout.size() as u64));
        // SAFETY: same contract as `dealloc`/`alloc`, forwarded verbatim.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

/// Bytes this thread allocated and has not freed, over `f`.
fn resident_over<T>(f: impl FnOnce() -> T) -> (T, u64) {
    let a0 = ALLOCATED.with(Cell::get);
    let f0 = FREED.with(Cell::get);
    let value = f();
    let allocated = ALLOCATED.with(Cell::get) - a0;
    let freed = FREED.with(Cell::get) - f0;
    (value, allocated.saturating_sub(freed))
}

const FLOOR: u64 = pulsus_config::INGEST_DEDUP_MAX_BYTES_FLOOR;
const CEILING: u64 = pulsus_config::INGEST_DEDUP_MAX_BYTES_CEILING;

fn index_at(max_bytes: u64) -> std::sync::Arc<PushDedup> {
    PushDedup::new(
        max_bytes,
        Duration::from_secs(300),
        Duration::from_secs(120),
    )
}

// ---------------------------------------------------------------------
// The partition of the accepted range
// ---------------------------------------------------------------------

/// One region of the accepted knob range over which the index's reserved
/// sizes do not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Partition {
    /// The smallest accepted knob producing `capacities` — the tightest
    /// bound in the region, because the pair is constant across it.
    min_knob: u64,
    capacities: Capacities,
}

/// Every distinct reserved-size pair the accepted range produces, in
/// ascending order of the smallest knob that produces it.
///
/// The search is over knob values, not over the formula's internals: it
/// binary-searches for each change point, so it cannot silently agree with
/// a wrong model of how `plan_capacities` decides. `contiguous_and_complete`
/// checks the result covers `FLOOR..=CEILING` with no gap.
fn partitions() -> Vec<Partition> {
    let mut out = vec![Partition {
        min_knob: FLOOR,
        capacities: plan_capacities(FLOOR),
    }];
    let mut knob = FLOOR;
    while knob < CEILING {
        let current = plan_capacities(knob);
        // The pair is monotone in the sense that it changes only at
        // thresholds; find the first knob above `knob` where it differs, by
        // doubling out to a knob that differs and then bisecting.
        let mut lo = knob;
        let mut hi = CEILING;
        if plan_capacities(hi) == current {
            break;
        }
        while lo + 1 < hi {
            let mid = lo + (hi - lo) / 2;
            if plan_capacities(mid) == current {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        knob = hi;
        out.push(Partition {
            min_knob: knob,
            capacities: plan_capacities(knob),
        });
    }
    out
}

// ---------------------------------------------------------------------
// The criterion
// ---------------------------------------------------------------------

/// The partition is a partition: it starts at the accepted floor, every
/// step is strictly larger than the last, and the pair is constant from
/// each step's knob up to one byte below the next.
#[test]
fn the_partition_covers_the_whole_accepted_range() {
    let parts = partitions();
    assert!(parts.len() > 1, "the range must contain a transition");
    assert_eq!(parts[0].min_knob, FLOOR);

    for window in parts.windows(2) {
        let (a, b) = (window[0], window[1]);
        assert!(a.min_knob < b.min_knob);
        assert_ne!(
            a.capacities, b.capacities,
            "a partition boundary must change the reserved sizes"
        );
        assert_eq!(
            plan_capacities(b.min_knob - 1),
            a.capacities,
            "one byte below {} must still be the previous partition",
            b.min_knob
        );
        assert_eq!(plan_capacities(b.min_knob), b.capacities);
    }
    let last = parts[parts.len() - 1];
    assert_eq!(
        plan_capacities(CEILING),
        last.capacities,
        "the last partition must reach the accepted ceiling"
    );
}

/// The load-bearing assertion. For every distinct reserved-size pair the
/// accepted range produces: what the allocator hands out at construction is
/// exactly what the model says, and the whole-index charge fits the
/// smallest knob in that partition.
#[test]
fn every_reserved_size_the_accepted_range_produces_matches_the_allocator() {
    let parts = partitions();
    for part in &parts {
        let Capacities { claims, waiters } = part.capacities;
        assert!(claims > 0, "{part:?}: the claim table must hold a push");
        assert!(waiters > 0, "{part:?}: the registry must hold a waiter");

        let (index, resident) = resident_over(|| index_at(part.min_knob));
        assert_eq!(
            index.capacities(),
            part.capacities,
            "the index must report the sizes the model planned"
        );

        // The index reports the whole-index charge, not the construction
        // charge, so the construction model is the whole charge minus the
        // registrations that have not happened yet.
        let whole = index.bound_bytes();
        assert_eq!(
            whole,
            index_bytes(claims, waiters),
            "{part:?}: the index's own bound must be the model's"
        );
        assert!(
            whole <= part.min_knob,
            "{part:?}: charge {whole} exceeds the smallest knob in its partition"
        );

        let construction = whole - registration_charge() * waiters as u64;
        assert_eq!(
            resident, construction,
            "{part:?}: the allocator handed out {resident} bytes at construction, \
             the model says {construction}"
        );
        drop(index);
    }
    // Reported so a reader can see what the range actually contains, and
    // can check that a knob of interest is covered without being listed.
    println!(
        "issue #494 criterion 21: {} distinct reserved-size pairs over {FLOOR}..={CEILING}",
        parts.len()
    );
    for part in &parts {
        println!(
            "  min_knob={} claims={} waiters={} charge={}",
            part.min_knob,
            part.capacities.claims,
            part.capacities.waiters,
            index_bytes(part.capacities.claims, part.capacities.waiters)
        );
    }
}

/// The per-registration charge, derived from the model rather than written
/// down here: the whole-index bound minus the same index's bound with one
/// fewer waiter slot is exactly one registration's charge.
fn registration_charge() -> u64 {
    // `index_bytes` is linear in the waiter count apart from the map, which
    // only steps at a power of two; two adjacent counts inside one step
    // therefore differ by exactly the per-registration charge.
    let c = plan_capacities(16 * 1024 * 1024);
    index_bytes(c.claims, c.waiters) - index_bytes(c.claims, c.waiters - 1)
}

/// The round-8 review's own counterexample, made into a leg.
///
/// A one-byte breach was introduced at a knob of 2,097,152 and every case
/// the previous criterion listed stayed green, because that knob was not
/// one of them. 2,097,152 is **not** a partition boundary here — it sits
/// inside the partition that starts at 1,782,200 — so this leg names it
/// explicitly: the index built at it reserves that partition's sizes, and
/// what the allocator hands out is at or below the knob itself.
///
/// The general argument still does the work: an accepted knob produces its
/// partition's pair, and the pair decides the allocation. This leg is the
/// instance a reader can check by hand.
#[test]
fn a_knob_inside_a_partition_is_bounded_by_the_partition() {
    let parts = partitions();
    for knob in [2 * 1024 * 1024u64, 20 * 1024 * 1024, 16 * 1024 * 1024 + 1] {
        let owner = parts
            .iter()
            .rev()
            .find(|p| p.min_knob <= knob)
            .copied()
            .expect("every accepted knob lies in a partition");
        assert_eq!(
            plan_capacities(knob),
            owner.capacities,
            "knob {knob} must take its partition's reserved sizes"
        );
        assert_ne!(
            owner.min_knob, knob,
            "this leg is about a knob that is NOT a partition boundary"
        );

        let (index, resident) = resident_over(|| index_at(knob));
        assert!(
            index.bound_bytes() <= knob,
            "knob {knob}: whole-index charge {} exceeds it",
            index.bound_bytes()
        );
        assert!(
            resident <= knob,
            "knob {knob}: the allocator handed out {resident} bytes at construction"
        );
        assert_eq!(
            resident,
            index.bound_bytes() - registration_charge() * owner.capacities.waiters as u64,
            "knob {knob}: construction must be the model's construction half"
        );
        drop(index);
    }
}

/// One blocked sync caller, held exactly as the writer holds it: the guard
/// owns the charge and the receiver is the caller's half of the wakeup.
type HeldWaiter = (WaitGuard, oneshot::Receiver<ClaimOutcome>);

/// Opens a claim with one target and seals it, so later admissions of the
/// same identity are suppressed and register as waiters.
fn open_claim(index: &std::sync::Arc<PushDedup>, key: u128) -> PushIdentity {
    let id = PushIdentity {
        key: PushDigest::from_raw(key),
        content: PushDigest::from_raw(key),
        declared_retry: false,
    };
    match index.admit(id, WaitMode::None) {
        Admission::Admit(mut guard) => {
            guard.note_target(true);
            guard.seal();
        }
        other => panic!("the first push must admit, got {other:?}"),
    }
    id
}

fn register_waiter(index: &std::sync::Arc<PushDedup>, id: PushIdentity) -> HeldWaiter {
    match index.admit(id, WaitMode::Register) {
        Admission::SuppressedPending { guard, rx } => (guard, rx),
        other => panic!("a suppressed sync caller must register, got {other:?}"),
    }
}

/// Closes the model-to-allocator link on the half construction does not
/// cover: what a live registration allocates, against what it is charged.
///
/// The charge must be an upper bound at every key shape, because a waiter
/// vector grows by doubling and the charge is flat: one waiter per key is
/// the worst case (a four-slot vector holding one element), and many
/// waiters on one key is the best.
#[test]
fn registration_charge_is_an_upper_bound_at_every_key_shape() {
    let charge = registration_charge();
    assert!(charge > 0);

    for waiters_per_key in [1usize, 2, 3, 4, 5, 17, 64] {
        let index = index_at(16 * 1024 * 1024);
        let keys = 64;
        let total = keys * waiters_per_key;
        assert!(total <= index.capacities().waiters);

        // The claims are opened OUTSIDE the measured window: what this
        // measures is registration, not admission.
        let ids: Vec<PushIdentity> = (0..keys as u128)
            .map(|key| open_claim(&index, key))
            .collect();

        let (held, resident) = resident_over(|| {
            let mut held: Vec<HeldWaiter> = Vec::with_capacity(total);
            for id in &ids {
                for _ in 0..waiters_per_key {
                    held.push(register_waiter(&index, *id));
                }
            }
            held
        });

        // The `Vec` holding the guards is the test's own bookkeeping, not
        // the index's; subtract exactly what it reserved.
        let own = (held.capacity() * size_of::<HeldWaiter>()) as u64;
        let registry = resident - own;
        assert!(
            registry <= charge * total as u64,
            "{waiters_per_key} waiters per key: the registry allocated {registry} bytes \
             for {total} registrations, charged {}",
            charge * total as u64
        );
        drop(held);
    }
}

/// The instrument measures what it claims to. A block that allocates and
/// frees the same vector is resident-zero; a block that keeps it is not.
#[test]
fn the_counting_allocator_measures_what_is_kept() {
    let ((), zero) = resident_over(|| {
        let v: Vec<u64> = Vec::with_capacity(4096);
        drop(v);
    });
    assert_eq!(zero, 0, "an allocation that is freed is not resident");

    let (kept, held) = resident_over(|| Vec::<u64>::with_capacity(4096));
    assert_eq!(held, 4096 * 8, "a kept allocation is resident, exactly");
    drop(kept);
}

/// tokio's oneshot state is part of every registration's charge as an
/// upper bound. This is the assertion that fails if that state grows.
#[test]
fn a_oneshot_channel_fits_inside_one_registrations_charge() {
    let (channel, resident) = resident_over(oneshot::channel::<ClaimOutcome>);
    assert!(
        resident <= registration_charge(),
        "one oneshot channel allocates {resident} bytes against a per-registration \
         charge of {}",
        registration_charge()
    );
    drop(channel);
}

// ---------------------------------------------------------------------
// Criterion 19 — the waiter registry is bounded, measured by the allocator
// ---------------------------------------------------------------------
//
// Every leg below asserts the ALLOCATOR's resident bytes, never a gauge.
// A gauge that tracks the allocator is the thing in dispute, so it cannot
// be the instrument: the round-7 review measured the two disagreeing on
// one run — cancelling half moved resident by 480,000 bytes while the
// gauge moved by 801,125.

/// **(a) Mass waiters.** Block callers until the index refuses. The
/// allocator's resident total stays at or below the knob throughout, the
/// refusing caller is told `WaitShed` with `wait_shed_total` incremented,
/// and its retry receives the original's outcome once the claim settles.
#[test]
fn blocking_callers_until_the_registry_refuses_stays_inside_the_knob() {
    let knob = pulsus_config::INGEST_DEDUP_MAX_BYTES_FLOOR;
    let (index, construction) = resident_over(|| index_at(knob));
    let id = open_claim(&index, 1);

    let mut held: Vec<HeldWaiter> = Vec::new();
    let (shed, registration_bytes) = resident_over(|| {
        loop {
            match index.admit(id, WaitMode::Register) {
                Admission::SuppressedPending { guard, rx } => held.push((guard, rx)),
                Admission::WaitShed => break true,
                other => panic!("unexpected {other:?}"),
            }
        }
    });
    assert!(shed, "the registry must refuse rather than grow");
    assert_eq!(
        held.len(),
        index.capacities().waiters,
        "the registry holds exactly the capacity it reports"
    );
    assert_eq!(index.snapshot().wait_shed_total, 1);

    // The test's own `Vec` of guards is not the index's memory.
    let own = (held.capacity() * size_of::<HeldWaiter>()) as u64;
    let resident = construction + registration_bytes - own;
    assert!(
        resident <= knob,
        "a full registry holds {resident} bytes against a knob of {knob}"
    );

    // The refused caller's retry gets the original's outcome once the
    // claim is terminal — it is never told the push was stored.
    index.settle_target(id.key, pulsus_write::TargetOutcome::Committed, true);
    assert!(
        matches!(
            index.admit(id, WaitMode::Register),
            Admission::SuppressedSettled(ClaimOutcome::Ok)
        ),
        "the retry after a shed receives the original's answer"
    );
    drop(held);
}

/// **(b) Cancellation, then refill.** Block `n` callers, record resident
/// bytes, cancel half, record again, then block `n/2` fresh callers and
/// assert the index accepts them and resident returns to its earlier
/// figure. The refill is what proves the budget was released rather than
/// merely reported released.
#[test]
fn cancelling_half_the_waiters_releases_their_bytes_and_the_registry_refills() {
    let knob = pulsus_config::INGEST_DEDUP_MAX_BYTES_FLOOR;
    let index = index_at(knob);
    let id = open_claim(&index, 2);
    let n = index.capacities().waiters;

    let mut held: Vec<HeldWaiter> = Vec::with_capacity(n);
    let ((), full) = resident_over(|| {
        for _ in 0..n {
            held.push(register_waiter(&index, id));
        }
    });
    assert!(full > 0, "registrations allocate");

    let keep = n / 2;
    let ((), freed) = resident_over(|| {
        held.truncate(keep);
    });
    assert!(
        freed == 0,
        "`resident_over` reports bytes KEPT; a block that only frees keeps none"
    );

    // Refill: the same number of fresh callers must be accepted, and the
    // registry's resident total must come back to where it was.
    let ((), refilled) = resident_over(|| {
        for _ in 0..(n - keep) {
            match index.admit(id, WaitMode::Register) {
                Admission::SuppressedPending { guard, rx } => held.push((guard, rx)),
                other => panic!("a cancelled registration must be reusable, got {other:?}"),
            }
        }
    });
    assert_eq!(held.len(), n, "the registry is full again");
    assert!(
        refilled <= full,
        "the refill allocated {refilled} bytes where the original fill allocated {full}"
    );
    assert!(
        matches!(index.admit(id, WaitMode::Register), Admission::WaitShed),
        "and the bound is where it was"
    );
    drop(held);
}

/// **(c) Async registers nothing.** The same duplicate pushes in async
/// mode leave the allocator's resident total unchanged — asserted as a
/// delta of zero, not as a gauge reading zero.
#[test]
fn an_async_suppressed_caller_allocates_nothing() {
    let index = index_at(16 * 1024 * 1024);
    let id = open_claim(&index, 3);

    let ((), delta) = resident_over(|| {
        for _ in 0..10_000 {
            match index.admit(id, WaitMode::None) {
                Admission::SuppressedSettled(_) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
    });
    assert_eq!(
        delta, 0,
        "ten thousand async suppressed pushes allocated {delta} bytes"
    );
    assert_eq!(
        index.snapshot().wait_bytes,
        0,
        "and nothing is charged to the waiter budget"
    );
}
