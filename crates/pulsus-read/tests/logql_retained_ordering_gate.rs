//! **Charge BEFORE allocate, inside the mechanism that enforces it**
//! (issue #260, review round 4).
//!
//! `Retained::concat` sizes its pieces itself and charges the total
//! before allocating, which makes the well-behaved path correct by
//! construction. The residual is a source that yields MORE on the write
//! walk than on the sizing walk: round 3 reconciled the two totals
//! AFTERWARDS, so `push_str` had already reallocated past the charged
//! capacity by the time the excess charge could refuse. The mechanism
//! that exists to enforce charge-before-allocate was itself allocating
//! before charging.
//!
//! A behavioural test cannot see that ordering — the ledger ends on the
//! same number either way. This gate can: with the budget left holding
//! exactly the sizing walk's total, the excess charge MUST refuse, and
//! then the two orderings differ in the only observable that matters —
//! whether the buffer grew first.
//!
//! Own binary, one `#[test]`, byte CEILING never an exact count: the
//! counting allocator is process-global (the project's alloc-gate flake
//! rule).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAlloc;

// SAFETY: delegates verbatim to the system allocator; the only side
// effect is a relaxed atomic add, which allocates nothing and cannot
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

use pulsus_read::logql::template::{MAX_TEMPLATE_RENDER_BYTES, RenderBudget, Retained};

/// 1 MiB per piece — far above any stray allocation the harness makes,
/// so the ceiling below separates the two orderings by a wide margin
/// rather than by a countable number of allocations.
const PIECE: usize = 1 << 20;

/// An iterator whose SECOND walk yields more than its first, sharing the
/// walk counter across clones. This is the only shape left that can make
/// `concat` write more than it sized, now that no caller supplies a
/// length or a writer.
#[derive(Clone)]
struct GrowsOnTheSecondWalk<'a> {
    pieces: &'a [&'a str],
    walk: &'a Cell<usize>,
    next: usize,
}

impl<'a> Iterator for GrowsOnTheSecondWalk<'a> {
    type Item = &'a str;
    fn next(&mut self) -> Option<&'a str> {
        // First walk (the sizing one): the first piece only. Later
        // walks: everything.
        let visible = if self.walk.get() == 0 {
            1
        } else {
            self.pieces.len()
        };
        if self.next >= visible {
            self.walk.set(self.walk.get() + 1);
            self.next = 0;
            return None;
        }
        let item = self.pieces[self.next];
        self.next += 1;
        Some(item)
    }
}

#[test]
fn concat_refuses_an_overrun_before_the_buffer_grows() {
    let a = "a".repeat(PIECE);
    let b = "b".repeat(PIECE);
    let pieces: &[&str] = &[a.as_str(), b.as_str()];
    let walk = Cell::new(0usize);
    let liar = GrowsOnTheSecondWalk {
        pieces,
        walk: &walk,
        next: 0,
    };

    // Leave the ledger holding EXACTLY the sizing walk's total, so the
    // sizing charge succeeds and the excess charge cannot. That is the
    // near-exhausted-budget trigger the template gate uses for the same
    // purpose: correct ordering refuses at the charge and returns before
    // the copy; a charge moved after its allocation leaves the copy on
    // the counter.
    let budget = RenderBudget::default();
    budget
        .charge_retained(MAX_TEMPLATE_RENDER_BYTES as usize - PIECE)
        .expect("the pre-charge itself must fit");
    assert_eq!(
        budget.charged_bytes(),
        MAX_TEMPLATE_RENDER_BYTES - PIECE as u64
    );

    let before = bytes_here();
    let refused = Retained::concat(&budget, liar).is_err();
    let allocated = bytes_here() - before;

    assert!(
        refused,
        "the second piece has nothing left to pay with — this fixture must refuse, or it \
         measures the wrong thing"
    );
    assert!(budget.breached());

    // The one observable that separates the two orderings.
    //
    // Charge-before-allocate: `String::with_capacity(PIECE)` is the only
    // allocation, and the refusal happens before the second `push_str`.
    // Charge-after: `push_str` reallocates to hold 2 x PIECE first, so
    // the counter carries the grown buffer (~3 x PIECE with the old one
    // still mapped) before the excess charge can reject.
    //
    // A CEILING, not an equality: the per-thread counter picks up
    // stray allocations, and the two orderings are a megabyte apart.
    let ceiling = PIECE as u64 + (256 * 1024);
    assert!(
        allocated <= ceiling,
        "concat allocated {allocated} bytes for a {PIECE}-byte charge before refusing — the \
         buffer grew before the excess was charged (ceiling {ceiling})"
    );
}
