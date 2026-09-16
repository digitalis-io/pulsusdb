//! Issue #507: JSON whitespace before and after the value, and a leading
//! byte-order mark, cost no allocation. Each case runs the same object with
//! and without 4 KiB of JSON whitespace through `run_metric_into` and asserts
//! the allocation count and the bytes requested are the same.
//!
//! Own binary and one `#[test]`: the counting allocator is process-global,
//! so nothing else may run beside it. The comparison is between two runs of
//! the same pipeline in the same thread, which is why an exact equality is
//! safe here where a count against a fixed figure would not be.

use std::alloc::{GlobalAlloc, Layout, System};
use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};

use pulsus_read::logql::pipeline::CompiledPipeline;

struct CountingAlloc;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: delegates verbatim to the system allocator; the only side effect
// is two relaxed atomic adds, which allocate nothing and cannot re-enter the
// allocator.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

fn compiled(query: &str) -> CompiledPipeline {
    match pulsus_logql::parse(query).expect("parse") {
        pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Range { ref range, .. }) => {
            CompiledPipeline::compile(&range.selector.pipeline).expect("compile")
        }
        _ => panic!("expected a range query: {query}"),
    }
}

/// `(allocations, bytes requested)` for one metric run of `line`.
fn cost(c: &CompiledPipeline, line: &str, base: &[(String, String)]) -> (u64, u64) {
    let mut labels: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::with_capacity(8);
    let (a0, b0) = (
        ALLOCS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    );
    let r = c.run_metric_into(line, base, 0, None, &mut labels);
    let out = (
        ALLOCS.load(Ordering::Relaxed) - a0,
        BYTES.load(Ordering::Relaxed) - b0,
    );
    std::hint::black_box((r.is_ok(), labels.len()));
    out
}

#[test]
fn json_whitespace_around_the_value_allocates_nothing_more() {
    let base = vec![("service_name".to_string(), "checkout".to_string())];
    let ws = " \t\r\n".repeat(1 << 10);
    let queries = [
        r#"count_over_time({x="y"} | json [1m])"#,
        r#"count_over_time({x="y"} | json latency="latency" [1m])"#,
        r#"count_over_time({x="y"} | unpack [1m])"#,
    ];
    let objects = [r#"{"latency":5}"#, r#"{"_entry":"x","latency":"5"}"#];
    for q in queries {
        let c = compiled(q);
        for obj in objects {
            let plain = cost(&c, obj, &base);
            for (shape, line) in [
                ("leading", format!("{ws}{obj}")),
                ("mark then leading", format!("\u{feff}{ws}{obj}")),
                ("trailing", format!("{obj}{ws}")),
            ] {
                let got = cost(&c, &line, &base);
                assert_eq!(
                    got, plain,
                    "{q} over {obj}: {shape} whitespace changed the allocations"
                );
            }
        }
    }
}
