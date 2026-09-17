//! **The class-(P) preflight allocates no more than its charge covers**
//! (issue #290 §3).
//!
//! The source rule that would otherwise carry this claim —
//! `the_preflight_module_allocates_only_its_six_reserved_buffers` in
//! `logql_post_agg_witness.rs` — reads `region_census::FnInfo.callees`,
//! which records DIRECT calls only. A preflight helper that calls a
//! one-line helper defined outside `mod preflight` passes it while the
//! callee allocates freely, so that rule proves "no function in
//! `mod preflight` names a forbidden allocating token" and NOT "the
//! preflight allocates at most its six reserved buffers". Those are
//! different statements, and this gate is the second one: it is closed
//! over the callee closure by construction, because it measures the
//! allocator rather than the source.
//!
//! Own binary, one `#[test]`, a byte CEILING and never an exact count —
//! the counting allocator is process-global (the project's alloc-gate
//! flake rule). Precedent and shape: `logql_json_key_alloc_gate.rs`.
//!
//! **What the ceiling is, exactly.** `preflight_scratch_bytes(&lm, &rm)`
//! plus, on a refusing case, an allowance for the REFUSAL PAYLOAD — the
//! `String` the client is going to receive. That string is not scratch:
//! every semantic refusal in this crate builds its message under no
//! charge at all (`set_op_scalar_error` does so today), and issue #290
//! neither changes that nor claims to. The allowance is the crate's own
//! growth model for a buffer built by `format!` without a reservation,
//! `3 · max(2 · len, 32)`, spelled out here rather than imported so the
//! gate states its own ceiling. On the non-refusing cases the allowance
//! is zero and the bound is tight.

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

use pulsus_logql::{BinOp, MatchGroup, VectorMatching};
use pulsus_read::logql::{
    MatrixSeries, QueryResult, ReadError, VectorSample, measure_matrix, measure_vector,
    preflight_alloc_probe, preflight_scratch_bytes,
};

/// `3 · alloc_block_bytes(len)` — the crate's `grown_alloc_bytes`, for a
/// buffer grown into rather than reserved.
fn grown(len: usize) -> u64 {
    3 * (2 * len as u64).max(32)
}

/// A `(stage_charged, stage_cap)` pair the stage charge refuses for every
/// operand pair (`1 + bytes > 0`), so `decide_binary`'s guard never skips
/// and this gate measures the preflight rather than the guard. The
/// guard's own measurement — that an ADMITTED charge requests zero bytes
/// — is `logql_preflight_guard_gate.rs`, its own binary because the
/// counting allocator is process-global.
const REFUSING: (u64, u64) = (1, 0);

fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

/// One side's label sets, by KIND. `0` gives every series a distinct
/// `x`; `1` repeats an `x` while the full label sets stay distinct (a
/// match-signature collision under `on(x)`); `2` repeats a whole label
/// set (an exact duplicate); `3` differs only in `y` (the include-copy
/// collapse); `4` carries `x` alone, which as the ONE side is what makes
/// the copy drop `y` from both many-side outputs.
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

/// The four pre-committed collision modes as `(lhs kind, rhs kind)`. The
/// collision has to sit on ONE side at a time: with both sides colliding,
/// `duplicate_one_side_error` wins every step and the many-side refusals
/// are never reached.
const MODES: [(usize, usize); 4] = [
    (0, 0), // no collision — the tight, zero-allowance rows
    (0, 1), // the ONE side's signatures collide
    (2, 0), // the MANY side repeats a whole label set (one-to-one)
    (3, 4), // the MANY side collapses onto one grouped identity
];

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

fn measure(r: &QueryResult) -> pulsus_read::logql::StageInput {
    match r {
        QueryResult::Vector(items) => measure_vector(items),
        QueryResult::Matrix(items) => measure_matrix(items),
        _ => unreachable!("the fixtures are vectors and matrices only"),
    }
}

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
fn the_preflight_requests_no_more_than_its_scratch_charge_covers() {
    // Pre-committed: the sizes span the regime where the 32-byte
    // allocator floor dominates (S = 2) to the regime where the product
    // does (S = 512), and the collision modes reach the no-refusal path
    // and all three refusals.
    const SIZES: [(usize, usize); 4] = [(1, 1), (2, 3), (16, 16), (256, 256)];
    let mut outcomes: Vec<String> = Vec::new();
    let mut cases = 0usize;

    // Warm every lazily-initialised path so the measured window holds
    // the run and not one-time setup.
    let (w_l, w_r) = (build(4, 2, true), build(4, 0, true));
    let _ = preflight_alloc_probe(BinOp::Div, None, w_l, w_r, REFUSING.0, REFUSING.1);

    for &(nl, nr) in &SIZES {
        for (mname, m) in matchings() {
            for matrix in [false, true] {
                for (mode, &(lk, rk)) in MODES.iter().enumerate() {
                    let lhs = build(nl, lk, matrix);
                    let rhs = build(nr, rk, matrix);
                    let (lm, rm) = (measure(&lhs), measure(&rhs));
                    let charge = preflight_scratch_bytes(&lm, &rm);

                    let before = bytes_here();
                    let out = preflight_alloc_probe(
                        BinOp::Div,
                        m.as_ref(),
                        lhs,
                        rhs,
                        REFUSING.0,
                        REFUSING.1,
                    );
                    let requested = bytes_here() - before;

                    let payload = match &out {
                        Ok(()) => 0,
                        Err(ReadError::PipelineInvalid { reason }) => {
                            outcomes.push(reason.clone());
                            grown(reason.len())
                        }
                        Err(other) => panic!("unexpected preflight error {other:?}"),
                    };
                    if out.is_ok() {
                        outcomes.push("ok".to_string());
                    }
                    assert!(
                        requested <= charge + payload,
                        "({nl}, {nr}) {mname} matrix = {matrix} mode = {mode}: the preflight \
                         requested {requested} B against a {charge} B scratch charge (+{payload} \
                         B of refusal payload) — an allocation is escaping the six reserved \
                         buffers"
                    );
                    cases += 1;
                }
            }
        }
    }

    assert_eq!(cases, 128, "the pre-committed fixture set lost cases");

    // Non-vacuity: the fixture set has to REACH the paths it claims to
    // bound, or every assertion above passed on an early return.
    for want in [
        "ok",
        "found duplicate series on the right hand-side;many-to-many matching not allowed: \
         matching labels must be unique on one side",
        "multiple matches for labels: many-to-one matching must be explicit \
         (group_left/group_right)",
        "multiple matches for labels: grouping labels must ensure unique matches",
    ] {
        assert!(
            outcomes.iter().any(|o| o == want),
            "no fixture reached `{want}` — the gate bounds a path it never exercised"
        );
    }
}
