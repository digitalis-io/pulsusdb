//! The pprof model's allocations for the common push: one CPU profile with
//! two sample types, 2,000 distinct stacks of 30 frames and 4,000 sample
//! rows (plan-a §6). The rows keep what plan-a §6 lists: 3 `Vec`s per
//! distinct stack, 5 per distinct location and one `Arc<str>` per distinct
//! string. Anything per frame or per sample on top of that is a defect:
//! 60,000 frames each allocating a throwaway buffer is the case this pins.
//!
//! A counting global allocator, scoped to this one test binary and counting
//! allocation calls per thread, as `otlp_metrics_parse_alloc.rs` does.
//! Everything runs in the single `#[test]` below so no parallel test thread
//! can share the counter.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;

use pulsus_profiles_wire::google_v1::{Function, Line, Location, Profile, Sample, ValueType};
use pulsus_write::{ProfileInput, parse_profiles};

struct CountingAlloc;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<u64> = const { Cell::new(0) };
}

fn count_call() {
    if COUNTING.try_with(Cell::get).unwrap_or(false) {
        let _ = CALLS.try_with(|c| c.set(c.get() + 1));
    }
}

// SAFETY: delegates verbatim to the system allocator; the only side effect
// is a thread-local integer update, which allocates nothing and cannot
// re-enter the allocator.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_call();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_call();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_call();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

const STACKS: u64 = 2_000;
const DEPTH: u64 = 30;
const NOW_MS: i64 = 1_800_000_000_000;

/// Every stack shares its 29 root-side frames (locations `1..=29`) and has
/// its own leaf (location `29 + k`), each location one line on its own
/// function `f<id>` in `main.go`.
fn common_push() -> ProfileInput {
    let mut st = vec![
        String::new(),
        "samples".to_owned(),
        "count".to_owned(),
        "cpu".to_owned(),
        "nanoseconds".to_owned(),
        "main.go".to_owned(),
    ];
    let shared = DEPTH - 1;
    let locations = shared + STACKS;
    let mut function = Vec::new();
    let mut location = Vec::new();
    for id in 1..=locations {
        st.push(format!("f{id}"));
        function.push(Function {
            id,
            name: (st.len() - 1) as i64,
            system_name: 0,
            filename: 5,
            start_line: 1,
        });
        location.push(Location {
            id,
            mapping_id: 0,
            address: 0,
            line: vec![Line {
                function_id: id,
                line: 10,
            }],
            is_folded: false,
        });
    }
    let sample = (1..=STACKS)
        .map(|k| {
            let mut ids = vec![shared + k];
            ids.extend((1..=shared).rev());
            Sample {
                location_id: ids,
                value: vec![1, 10_000_000],
                label: Vec::new(),
            }
        })
        .collect();
    ProfileInput {
        labels: vec![
            ("__name__".to_owned(), "process_cpu".to_owned()),
            ("service_name".to_owned(), "checkout".to_owned()),
        ],
        profile: Profile {
            sample_type: vec![
                ValueType { r#type: 1, unit: 2 },
                ValueType { r#type: 3, unit: 4 },
            ],
            sample,
            location,
            function,
            string_table: st,
            time_nanos: NOW_MS * 1_000_000,
            duration_nanos: 10_000_000_000,
            period_type: Some(ValueType { r#type: 3, unit: 4 }),
            ..Profile::default()
        },
        decompressed_bytes: 1_000_000,
        id: None,
        annotations: Vec::new(),
    }
}

#[test]
fn the_common_push_allocates_per_row_not_per_frame() {
    let input = common_push();
    CALLS.with(|c| c.set(0));
    COUNTING.with(|c| c.set(true));
    let out = black_box(parse_profiles(vec![input], NOW_MS));
    COUNTING.with(|c| c.set(false));
    let calls = CALLS.with(Cell::get);
    let landing = out.expect("the push is accepted");

    let stacks = landing.stacks.len() as u64;
    let locations = landing.locations.len() as u64;
    assert_eq!(stacks, STACKS);
    assert_eq!(locations, STACKS + DEPTH - 1);
    assert_eq!(landing.samples.len() as u64, 2 * STACKS);
    assert_eq!(landing.stacks[0].names.len() as u64, DEPTH);

    // What the rows keep (one interned name per distinct location), plus a
    // fixed allowance for the per-push and per-series work and the
    // collections' logarithmic growth.
    let budget = 3 * stacks + 5 * locations + locations + 1_000;
    println!("allocation calls: {calls} (budget {budget})");
    assert!(
        calls <= budget,
        "{calls} allocation calls for the common push, budget {budget}"
    );
}
