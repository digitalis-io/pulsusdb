//! Issue #778: the allocator settings the read cache runs with.
//!
//! glibc serves a request from free heap space when a free chunk or the top
//! of the heap holds it. Otherwise a request whose block (glibc's chunk size:
//! the request plus an 8-byte size field, rounded up to a multiple of 16
//! bytes, at least 32) is at or above its mapping threshold gets a mapping
//! of its own, returned to the system when it is freed, and a smaller one
//! grows the heap, which keeps what is freed for reuse except what glibc
//! trims from the top of the heap.
//! Unless the threshold is fixed, glibc raises it to the size of each mapped
//! block freed, so after the first kept answer is evicted the next ones are
//! heap blocks again, and memory the cache freed stays with the process.
//! With the threshold fixed at 64 KiB, a kept answer whose block is 64 KiB
//! or more is its own mapping unless free heap space already holds it; one
//! whose block is under 64 KiB is always on the heap.

/// The fixed mapping threshold, `M_MMAP_THRESHOLD`.
pub const MMAP_THRESHOLD_BYTES: i32 = 65_536;
/// The most mappings the allocator makes, `M_MMAP_MAX`: half the kernel's
/// default limit of 65,530, so the allocator's own mappings leave room for
/// the rest of the process. Past it the allocator uses the heap.
pub const MMAP_MAX: i32 = 32_765;
/// glibc's default `M_MMAP_MAX`, put back when the threshold is refused.
/// glibc gives no way to read the value it had, so one set through the
/// environment before start is not what is put back.
pub const GLIBC_DEFAULT_MMAP_MAX: i32 = 65_536;

/// Applies the settings when the read cache's budget is above 0, as server
/// startup does. `false` when the budget is 0 or a setting did not apply
/// (logged at warn): the read cache is then not built, and requests are
/// answered as with the cache off. The bound on mappings goes first: if it
/// is refused nothing has changed. If the threshold is then refused, the
/// bound is put back at glibc's default, but glibc no longer moves the
/// threshold, since setting either value stops that and nothing restores
/// it. With a C library other than glibc nothing is set, and `true` is
/// returned whenever the budget is above 0.
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
    {
        let refused = |name: &str, value: i32| {
            tracing::warn!(
                setting = name,
                value,
                "the allocator did not accept a setting the read cache needs; \
                 the read cache is off"
            );
        };
        if set(libc::M_MMAP_MAX, MMAP_MAX) == 0 {
            refused("M_MMAP_MAX", MMAP_MAX);
            return false;
        }
        if set(libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD_BYTES) == 0 {
            refused("M_MMAP_THRESHOLD", MMAP_THRESHOLD_BYTES);
            if set(libc::M_MMAP_MAX, GLIBC_DEFAULT_MMAP_MAX) == 0 {
                tracing::warn!(
                    value = GLIBC_DEFAULT_MMAP_MAX,
                    "the allocator did not take M_MMAP_MAX back to its default"
                );
            }
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

    /// Every call a setter receives, and what it answers.
    fn run(fail_on: Option<i32>) -> (bool, Vec<(i32, i32)>) {
        let mut calls = Vec::new();
        let on = prepare_with(1 << 30, |p, v| {
            calls.push((p, v));
            i32::from(Some(p) != fail_on || calls.iter().filter(|c| c.0 == p).count() > 1)
        });
        (on, calls)
    }

    /// Both settings applied leave the cache on. A setting that does not
    /// apply leaves it off. The bound on mappings goes first, so a refused
    /// bound sets nothing more, and a refused threshold has the bound put
    /// back at glibc's default (glibc still stops moving the threshold once
    /// the bound is set). A budget of 0 sets nothing.
    #[test]
    fn a_setting_that_does_not_apply_leaves_the_read_cache_off() {
        let (on, calls) = run(None);
        assert!(on, "control");
        assert_eq!(
            calls,
            [
                (libc::M_MMAP_MAX, MMAP_MAX),
                (libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD_BYTES)
            ],
            "control: both settings applied"
        );
        let (on, calls) = run(Some(libc::M_MMAP_MAX));
        assert!(!on, "the bound on mappings refused");
        assert_eq!(calls, [(libc::M_MMAP_MAX, MMAP_MAX)], "nothing else set");
        let (on, calls) = run(Some(libc::M_MMAP_THRESHOLD));
        assert!(!on, "the threshold refused");
        assert_eq!(
            calls,
            [
                (libc::M_MMAP_MAX, MMAP_MAX),
                (libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD_BYTES),
                (libc::M_MMAP_MAX, GLIBC_DEFAULT_MMAP_MAX)
            ],
            "the bound on mappings put back"
        );
        let mut called = false;
        assert!(!prepare_with(0, |_, _| {
            called = true;
            1
        }));
        assert!(!called, "a budget of 0 sets nothing");
    }
}
