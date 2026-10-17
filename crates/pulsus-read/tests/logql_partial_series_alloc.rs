//! Issue #605: a drilldown request's partial result is selected in place.
//! Cutting a result over the series cap to its first 500 allocates the same
//! small, fixed amount — the warning — whatever the result's size, so it
//! needs no budget charge (`docs/architecture.md` §5.6).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use pulsus_read::Warnings;
use pulsus_read::logql::{
    MatrixSeries, PartialOrder, QueryResult, SortDir, VectorSample, truncate_or_refuse,
};

struct CountingAlloc;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
    static BYTES: Cell<u64> = const { Cell::new(0) };
}

fn charge(n: u64) {
    let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
    let _ = BYTES.try_with(|c| c.set(c.get() + n));
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size() as u64);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        charge(new_size as u64);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

/// `(allocations, bytes)` made by one call, the input built beforehand.
fn measure(input: QueryResult, order: PartialOrder) -> (u64, u64) {
    let mut warnings = Warnings::new();
    let (a0, b0) = (ALLOCS.with(Cell::get), BYTES.with(Cell::get));
    let out = truncate_or_refuse(input, true, order, &mut warnings).expect("served partially");
    let (a1, b1) = (ALLOCS.with(Cell::get), BYTES.with(Cell::get));
    drop(out);
    (a1 - a0, b1 - b0)
}

fn matrix(n: usize) -> QueryResult {
    QueryResult::Matrix(
        (0..n)
            .rev()
            .map(|i| MatrixSeries {
                labels: vec![("id".to_string(), format!("{i:06}"))],
                points: vec![((i % 7) as i64, i as f64), (9, 1.0)],
            })
            .collect(),
    )
}

fn vector(n: usize) -> QueryResult {
    QueryResult::Vector(
        (0..n)
            .rev()
            .map(|i| VectorSample {
                labels: vec![("id".to_string(), format!("{i:06}"))],
                value: i as f64,
            })
            .collect(),
    )
}

/// One row: a name, the result builder, and the order to keep.
type Case = (&'static str, fn(usize) -> QueryResult, PartialOrder);

#[test]
fn the_partial_selection_allocates_a_fixed_amount_whatever_the_result_size() {
    let cases: [Case; 5] = [
        ("matrix labels", matrix, PartialOrder::Labels),
        (
            "matrix step-major desc",
            matrix,
            PartialOrder::StepMajor(SortDir::Desc),
        ),
        (
            "matrix step-major asc",
            matrix,
            PartialOrder::StepMajor(SortDir::Asc),
        ),
        ("vector labels", vector, PartialOrder::Labels),
        ("vector as given", vector, PartialOrder::AsGiven),
    ];
    for (name, build, order) in cases {
        let small = measure(build(1_000), order);
        let large = measure(build(100_000), order);
        assert_eq!(
            small, large,
            "{name}: allocation must not grow with the result"
        );
        assert!(large.1 < 1024, "{name}: {} bytes allocated", large.1);
    }
}
