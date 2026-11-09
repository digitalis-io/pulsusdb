//! Issue #778: the allocator settings the read cache runs with.
//!
//! glibc serves a block above its mapping threshold from a mapping of its
//! own and returns it to the system when it is freed; a block below the
//! threshold comes from the heap, which keeps what is freed for reuse.
//! Unless the threshold is fixed, glibc raises it to the size of each mapped
//! block freed, so after the first kept answer is evicted the next ones are
//! heap blocks again, and memory the cache freed stays with the process.
//! Fixing the threshold at 64 KiB makes each kept answer of 64 KiB or more
//! its own mapping.

/// The fixed mapping threshold, `M_MMAP_THRESHOLD`.
pub const MMAP_THRESHOLD_BYTES: i32 = 65_536;
/// The most mappings the allocator makes, `M_MMAP_MAX`: half the kernel's
/// default limit of 65,530, so the allocator's own mappings leave room for
/// the rest of the process. Past it the allocator uses the heap.
pub const MMAP_MAX: i32 = 32_765;

/// Applies the settings when the read cache's budget is above 0, as server
/// startup does. `false` when the budget is 0 or a setting did not apply
/// (logged at warn): the read cache is then not built, and requests are
/// answered as with the cache off. With a C library other than glibc
/// nothing is set.
pub fn prepare_read_cache(budget_bytes: u64) -> bool {
    prepare_with(budget_bytes, set)
}

/// [`prepare_read_cache`] with the setter passed in, for its tests.
#[doc(hidden)]
pub fn prepare_with(budget_bytes: u64, mut set: impl FnMut(i32, i32) -> i32) -> bool {
    if budget_bytes == 0 {
        return false;
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    for (name, param, value) in [
        (
            "M_MMAP_THRESHOLD",
            libc::M_MMAP_THRESHOLD,
            MMAP_THRESHOLD_BYTES,
        ),
        ("M_MMAP_MAX", libc::M_MMAP_MAX, MMAP_MAX),
    ] {
        if set(param, value) == 0 {
            tracing::warn!(
                setting = name,
                value,
                "the allocator did not accept a setting the read cache needs; \
                 the read cache is off"
            );
            return false;
        }
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    let _ = &mut set;
    true
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn set(param: i32, value: i32) -> i32 {
    // SAFETY: `mallopt` takes two integers and changes only the allocator's
    // own parameters; it is safe to call at any time from any thread.
    unsafe { libc::mallopt(param, value) }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn set(_param: i32, _value: i32) -> i32 {
    1
}

#[cfg(all(test, target_os = "linux", target_env = "gnu"))]
mod tests {
    use super::*;

    /// Each setting that does not apply leaves the cache off; both applied
    /// leave it on, and a budget of 0 sets nothing.
    #[test]
    fn a_setting_that_does_not_apply_leaves_the_read_cache_off() {
        let mut calls = Vec::new();
        assert!(prepare_with(1 << 30, |p, v| {
            calls.push((p, v));
            1
        }));
        assert_eq!(
            calls,
            [
                (libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD_BYTES),
                (libc::M_MMAP_MAX, MMAP_MAX)
            ],
            "control: both settings applied"
        );
        for failing in [libc::M_MMAP_THRESHOLD, libc::M_MMAP_MAX] {
            let setter = |param: i32, _value: i32| i32::from(param != failing);
            assert!(!prepare_with(1 << 30, setter), "setting {failing} failed");
        }
        let mut called = false;
        assert!(!prepare_with(0, |_, _| {
            called = true;
            1
        }));
        assert!(!called, "a budget of 0 sets nothing");
    }
}
