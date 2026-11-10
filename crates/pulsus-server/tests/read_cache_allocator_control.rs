//! Issue #778, U39's control: without the read cache's allocator settings,
//! a freed block of 100 KiB stays mapped, back in an arena rather than
//! returned to the system.
#![cfg(all(target_os = "linux", target_env = "gnu"))]

#[path = "support/maps.rs"]
mod maps;

#[test]
fn u39_control_a_freed_block_under_the_default_threshold_stays_mapped() {
    // glibc serves a request from the free space at the top of the heap
    // when it fits, and maps it only when it does not; the first blocks use
    // that space up, so the last is placed by the threshold alone.
    let mut held: Vec<Vec<u8>> = (0..4).map(|_| vec![7u8; 100 * 1024]).collect();
    let v = held.pop().expect("four blocks");
    let range = maps::range_of(&v);
    assert!(maps::covered(&range), "mapped while held");
    drop(v);
    assert!(
        maps::covered(&range),
        "the freed block is still inside a mapping"
    );
    drop(held);
}
