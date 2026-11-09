//! Issue #778, U39: with the read cache's allocator settings applied, a
//! freed block of 100 KiB (under glibc's default threshold of 128 KiB, over
//! the fixed 64 KiB) goes back to the system. Its control without the
//! settings is `read_cache_allocator_control.rs`, a test binary of its own
//! because the settings last for the process.
#![cfg(all(target_os = "linux", target_env = "gnu"))]

#[path = "support/maps.rs"]
mod maps;

#[test]
fn u39_a_freed_block_over_the_fixed_threshold_leaves_the_process() {
    assert!(
        pulsus_server::allocator::prepare_read_cache(1 << 30),
        "the settings applied"
    );
    // glibc serves a request from the free space at the top of the heap
    // when it fits, and maps it only when it does not; the first blocks use
    // that space up, so the last is placed by the threshold alone.
    let mut held: Vec<Vec<u8>> = (0..4).map(|_| vec![7u8; 100 * 1024]).collect();
    let v = held.pop().expect("four blocks");
    let range = maps::range_of(&v);
    assert!(maps::covered(&range), "mapped while held");
    drop(v);
    assert!(
        !maps::covered(&range),
        "no mapping covers any byte of the freed block"
    );
    drop(held);
}
