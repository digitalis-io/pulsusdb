//! **A binary operation the byte budget admits pays NOTHING for the
//! class-(P) preflight** (issue #290, review round 1's `[medium]`).
//!
//! `decide_binary` runs the (P1) preflight only when the stage charge it
//! is about to levy would refuse. That is the only condition under which
//! the preflight can change an answer — issue #290 exists because a
//! budget rejection preempted a decidable semantic error, and where the
//! budget does not reject there is nothing to preempt. Ordinary traffic
//! is admitted, so ordinary traffic must not pay for six scratch buffers
//! and an `O(S log S · L)` signature sort.
//!
//! "Must not pay" is MEASURED here rather than argued: a counting global
//! allocator brackets the shipped decision path and the admitted rows
//! assert **exactly zero bytes requested**, not a ceiling. A ceiling
//! would have passed on the code this gate was written against.
//!
//! Own binary and one `#[test]`, like `logql_preflight_alloc_gate.rs`.
//!
//! **What is counted is what the MEASURING THREAD requested** (issue
//! #548). A `#[global_allocator]` serves every thread in the process, so
//! a counter kept in one process-wide figure charges whatever any other
//! thread allocates to whichever measured window happens to be open —
//! and an allocation made elsewhere is then indistinguishable from one
//! the code under test made itself. That is not a theory: this gate
//! failed in CI with `requested 900 B`, and a thread spawned beside the
//! test, allocating 900 bytes every 50 µs and touching nothing the code
//! under test can see, reproduces the failure with the same wording and
//! the same figure — only the size pair moves, because the case that
//! reddens is whichever window the foreign allocation lands in.
//!
//! So the counter is a thread-local cell. The assertion is unchanged and
//! is still **exactly zero**, not a ceiling: the quantity is "did the
//! decision path allocate at all", which is a property of the code and
//! not of the operand width, and a ceiling would pass on the code this
//! gate was written against.
//!
//! **The non-vacuity half.** Every fixture is also driven at a REFUSING
//! charge, where it must request more than zero. Without that, a deleted
//! preflight, an empty fixture set or a probe that returned early would
//! all read as "the guard works". It covers the counter too: a counter
//! that counted nothing would make the admitted rows read zero for the
//! wrong reason, and the refusing rows fail when that happens.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAlloc;

thread_local! {
    /// Bytes requested BY THIS THREAD. `const`-initialised, so the slot
    /// is a plain TLS word with no lazy heap box behind it — the
    /// allocator can read it without re-entering itself.
    static REQUESTED: Cell<u64> = const { Cell::new(0) };
}

/// What the calling thread has requested so far. The two marks either
/// side of the call under test read this, so another thread's
/// allocations are not in the difference.
fn requested_here() -> u64 {
    REQUESTED.with(Cell::get)
}

/// Adds to the calling thread's tally.
///
/// `try_with`, not `with`: during thread-local destruction the slot is
/// gone and `with` would panic inside the allocator. An allocation at
/// that point belongs to teardown and to no measured window, so dropping
/// it is the right answer as well as the safe one.
fn charge_here(bytes: usize) {
    let _ = REQUESTED.try_with(|c| c.set(c.get() + bytes as u64));
}

// SAFETY: delegates verbatim to the system allocator; the only side
// effect is a `Cell` update on a `const`-initialised thread-local, which
// allocates nothing and cannot re-enter the allocator.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge_here(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        charge_here(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

use pulsus_logql::{BinOp, MatchGroup, VectorMatching};
use pulsus_read::logql::{
    MAX_POST_AGG_BYTES, MatrixSeries, QueryResult, VectorSample, preflight_alloc_probe,
};

/// The charge admits: a clean counter against the production cap. Every
/// fixture below models far fewer than 8 GiB, so this is the ordinary
/// dashboard query.
const ADMITS: (u64, u64) = (0, MAX_POST_AGG_BYTES);
/// The charge refuses for every operand pair (`1 + bytes > 0`).
const REFUSING: (u64, u64) = (1, 0);

fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

/// One side's label sets, by KIND — the same four shapes
/// `logql_preflight_alloc_gate.rs` uses. `0` gives every series a
/// distinct `x`; `1` repeats an `x` while the full label sets stay
/// distinct; `2` repeats a whole label set; `3` differs only in `y`; `4`
/// carries `x` alone.
fn side_labels(n: usize, kind: usize) -> Vec<Vec<(String, String)>> {
    (0..n)
        .map(|i| match kind {
            0 => labels(&[("x", &format!("{i:04}")), ("z", "a")]),
            1 => labels(&[("x", &format!("{:04}", i / 2)), ("z", &format!("{i:04}"))]),
            2 => labels(&[("x", &format!("{:04}", i / 2)), ("z", "a")]),
            3 => labels(&[("x", &format!("{:04}", i / 2)), ("y", &format!("{i:04}"))]),
            _ => labels(&[("x", &format!("{i:04}"))]),
        })
        .collect()
}

/// `(lhs kind, rhs kind)`: no collision, then each of the three refusal
/// paths. An admitted query must pay nothing on ALL of them — a guard
/// that ran the preflight whenever a collision existed would be no guard
/// at all, since the collision is unknown until the preflight runs.
const MODES: [(usize, usize); 4] = [(0, 0), (0, 1), (2, 0), (3, 4)];

fn build(n: usize, kind: usize, matrix: bool) -> QueryResult {
    let sets = side_labels(n, kind);
    if matrix {
        QueryResult::Matrix(
            sets.into_iter()
                .enumerate()
                .map(|(i, ls)| MatrixSeries {
                    labels: ls,
                    points: (0..4).map(|t| (t as i64, (i + 1) as f64)).collect(),
                })
                .collect(),
        )
    } else {
        QueryResult::Vector(
            sets.into_iter()
                .enumerate()
                .map(|(i, ls)| VectorSample {
                    labels: ls,
                    value: (i + 1) as f64,
                })
                .collect(),
        )
    }
}

/// The four matchings, INCLUDING the two with no include list — the
/// `[medium]` this gate answers was specifically that a query with
/// `matching = None` or no `group_left`/`group_right` paid the full
/// preflight cost.
fn matchings() -> Vec<(&'static str, Option<VectorMatching>)> {
    vec![
        ("one-to-one", None),
        (
            "on(x)",
            Some(VectorMatching {
                on: true,
                labels: vec!["x".to_string()],
                group: None,
            }),
        ),
        (
            "on(x) group_left(y)",
            Some(VectorMatching {
                on: true,
                labels: vec!["x".to_string()],
                group: Some(MatchGroup::Left(vec!["y".to_string()])),
            }),
        ),
        (
            "ignoring(y) group_left(y)",
            Some(VectorMatching {
                on: false,
                labels: vec!["y".to_string()],
                group: Some(MatchGroup::Left(vec!["y".to_string()])),
            }),
        ),
    ]
}

#[test]
fn an_admitted_binary_operation_requests_no_preflight_bytes() {
    // Pre-committed: the same size sweep as the scratch gate, so the
    // admitted rows cover the regime where the preflight's cost would be
    // dominated by the 32-byte allocator floor (S = 2) and the regime
    // where the `O(S)` product dominates (S = 512).
    const SIZES: [(usize, usize); 4] = [(1, 1), (2, 3), (16, 16), (256, 256)];

    // Warm every lazily-initialised path so the measured window holds the
    // run and not one-time setup.
    let _ = preflight_alloc_probe(
        BinOp::Div,
        None,
        build(4, 2, true),
        build(4, 0, true),
        REFUSING.0,
        REFUSING.1,
    );

    let mut admitted_cases = 0usize;
    let mut refusing_cases = 0usize;
    for &(nl, nr) in &SIZES {
        for (mname, m) in matchings() {
            for matrix in [false, true] {
                for (mode, &(lk, rk)) in MODES.iter().enumerate() {
                    // The admitted row: zero, by equality.
                    let (lhs, rhs) = (build(nl, lk, matrix), build(nr, rk, matrix));
                    let before = requested_here();
                    let out =
                        preflight_alloc_probe(BinOp::Div, m.as_ref(), lhs, rhs, ADMITS.0, ADMITS.1);
                    let requested = requested_here() - before;
                    assert!(
                        out.is_ok(),
                        "({nl}, {nr}) {mname} matrix = {matrix} mode = {mode}: an admitted charge \
                         must decide no (P1) refusal — the join raises it instead: {out:?}"
                    );
                    assert_eq!(
                        requested, 0,
                        "({nl}, {nr}) {mname} matrix = {matrix} mode = {mode}: an admitted binary \
                         operation requested {requested} B for the preflight — the guard is not \
                         short-circuiting, and every ordinary query is paying for it"
                    );
                    admitted_cases += 1;

                    // The refusing row: the same fixture, more than zero.
                    // This is what stops the assertion above from being
                    // satisfied by a preflight that does nothing at all.
                    let (lhs, rhs) = (build(nl, lk, matrix), build(nr, rk, matrix));
                    let before = requested_here();
                    let _ = preflight_alloc_probe(
                        BinOp::Div,
                        m.as_ref(),
                        lhs,
                        rhs,
                        REFUSING.0,
                        REFUSING.1,
                    );
                    let requested = requested_here() - before;
                    assert!(
                        requested > 0,
                        "({nl}, {nr}) {mname} matrix = {matrix} mode = {mode}: a REFUSING charge \
                         requested nothing — the fixture never reaches the preflight, so the \
                         admitted row above compares nothing"
                    );
                    refusing_cases += 1;
                }
            }
        }
    }

    assert_eq!(
        admitted_cases, 128,
        "the pre-committed fixture set lost cases"
    );
    assert_eq!(refusing_cases, 128);
}
