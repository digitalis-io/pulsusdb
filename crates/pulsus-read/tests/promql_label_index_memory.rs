//! **T6 (issue #880): the label cache's heap per series and its peaks**,
//! measured through the pass's own batch path (`LabelPassProbe`, which
//! applies 1,024-row batches of `RegistrationRow` exactly as a pass does)
//! on 200,000 series shaped like the dev cluster's: 5,000 targets, 13 to 14
//! labels each, about 600 bytes of label JSON.
//!
//! A counting global allocator charges `malloc_usable_size + 8` per live
//! allocation, so the figures are heap, not RSS. Every row is generated as
//! it is sent, so the fixture never holds the series list; its base labels
//! are allocated before the first reading and are not in any figure.
//!
//! Everything runs in the single `#[test]` below: the counters are
//! process-wide, and a second test in this binary would be counted too.
#![cfg(target_os = "linux")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

use pulsus_model::{ACTIVITY_BUCKET_MS, Fingerprint, LabelSet};
use pulsus_read::metrics::{LabelPassProbe, RegistrationRow};

unsafe extern "C" {
    fn malloc_usable_size(ptr: *mut std::ffi::c_void) -> usize;
}

struct CountingAlloc;

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

/// The bytes an allocation holds: what the allocator gave it, plus its
/// header word.
fn held(p: *mut u8) -> isize {
    // SAFETY: `p` is a live pointer returned by the system allocator.
    (unsafe { malloc_usable_size(p.cast()) }) as isize + 8
}

fn charge(n: isize) {
    let v = LIVE.fetch_add(n, Ordering::Relaxed) + n;
    PEAK.fetch_max(v, Ordering::Relaxed);
}

// SAFETY: delegates to the system allocator; the only side effects are
// relaxed atomic updates, which allocate nothing.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            charge(held(p));
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            charge(held(p));
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(held(ptr), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let before = held(ptr);
        let q = unsafe { System.realloc(ptr, layout, new_size) };
        if !q.is_null() {
            LIVE.fetch_sub(before, Ordering::Relaxed);
            charge(held(q));
        }
        q
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

fn live() -> isize {
    LIVE.load(Ordering::Relaxed)
}

/// Starts a new peak reading from the current live bytes.
fn reset_peak() {
    PEAK.store(live(), Ordering::Relaxed);
}

fn peak() -> isize {
    PEAK.load(Ordering::Relaxed)
}

/// A small xorshift generator: the same series on every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn hex(&mut self, len: usize) -> String {
        (0..len)
            .map(|_| char::from(b"0123456789abcdef"[self.below(16) as usize]))
            .collect()
    }
}

const TARGETS: u64 = 5_000;
const NAMES: u64 = 7_346;

/// The dev-shaped series, one at a time: per target twelve base labels
/// (one 64-hex value, one 140-byte cgroup path among them), plus `le` on
/// a seventh of the metrics, `cpu` on a third and `device` on a fifth of
/// the series, and a per-series `scope`. A quarter of the series go to 1 %
/// of the metric names.
struct Fixture {
    rng: Rng,
    names: Vec<String>,
    targets: Vec<Vec<(String, String)>>,
    per_target: u64,
    t: u64,
    j: u64,
}

impl Fixture {
    fn new(series: u64, seed: u64) -> Self {
        let mut rng = Rng(seed);
        let names = (0..NAMES)
            .map(|i| {
                let family = [
                    "container",
                    "apiserver",
                    "etcd",
                    "node",
                    "kube",
                    "longhorn",
                    "prober",
                    "go",
                    "process",
                    "http",
                ][(i % 10) as usize];
                let suffix = if i % 7 == 0 {
                    "seconds_bucket"
                } else if i % 3 == 0 {
                    "total"
                } else {
                    "bytes"
                };
                format!("{family}_metric_{i}_{suffix}")
            })
            .collect();
        let targets = (0..TARGETS)
            .map(|t| {
                let s = |v: &str| v.to_string();
                vec![
                    (s("container"), format!("container-{}", t % 300)),
                    (
                        s("endpoint"),
                        s(["https-metrics", "http", "metrics", "web", "cadvisor"][(t % 5) as usize]),
                    ),
                    (
                        s("id"),
                        format!(
                            "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod{}.slice/cri-containerd-{}.scope",
                            rng.hex(32),
                            rng.hex(64)
                        ),
                    ),
                    (
                        s("image"),
                        format!("registry.example.com/org/image-{}:v1.{}.{}", t % 500, t % 13, t % 7),
                    ),
                    (s("instance"), format!("10.16.{}.{}:10250", t % 8, t % 60)),
                    (s("job"), format!("job-{}", t % 20)),
                    (
                        s("metrics_path"),
                        s(["/metrics", "/metrics/cadvisor", "/metrics/probes"][(t % 3) as usize]),
                    ),
                    (s("name"), rng.hex(64)),
                    (s("namespace"), format!("namespace-{}", t % 40)),
                    (s("node"), format!("worker-node-{:03}.cluster.internal", t % 60)),
                    (
                        s("pod"),
                        format!("workload-{}-{}-{}", t % 900, rng.hex(10), rng.hex(5)),
                    ),
                    (s("service"), format!("service-{}", t % 100)),
                ]
            })
            .collect();
        Fixture {
            rng,
            names,
            targets,
            per_target: series / TARGETS,
            t: 0,
            j: 0,
        }
    }

    /// The next series' registration at `bucket_ms`.
    fn next_row(&mut self, bucket_ms: i64) -> Option<RegistrationRow> {
        if self.j == self.per_target {
            self.j = 0;
            self.t += 1;
        }
        if self.t == TARGETS {
            return None;
        }
        let (t, j) = (self.t, self.j);
        self.j += 1;
        let r = &mut self.rng;
        let d = r.below(10);
        let m = match d {
            0 => 0,
            1 => 1 + r.below(10),
            2 | 3 => r.below(NAMES / 100),
            _ => r.below(NAMES),
        };
        let mut pairs = self.targets[t as usize].clone();
        if m % 7 == 0 {
            let le = [
                "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10",
                "+Inf",
            ][(j % 12) as usize];
            pairs.push(("le".to_string(), le.to_string()));
        }
        if j % 3 == 0 {
            pairs.push(("cpu".to_string(), format!("{}", j % 16)));
        }
        if j % 5 == 0 {
            pairs.push(("device".to_string(), format!("/dev/nvme{}n1", j % 4)));
        }
        pairs.push(("scope".to_string(), format!("s{j}")));
        let fp = (u128::from(r.next()) << 64) | u128::from(r.next());
        Some(RegistrationRow {
            fingerprint: Fingerprint::from_raw(fp),
            metric_name: self.names[m as usize].clone(),
            bucket_ms,
            labels: LabelSet::from_verbatim(pairs).to_canonical_json(),
        })
    }
}

const BATCH: usize = 1_024;
const SERIES: u64 = 200_000;
const NEW_SERIES: u64 = 10_000;
const H: i64 = ACTIVITY_BUCKET_MS;
/// An hour edge; every pass below is ten minutes past one.
const HOUR: i64 = 1_788_818_400_000;

/// Streams `fixture`'s rows at `bucket_ms` through the probe in batches,
/// skipping each row `skip` selects.
fn stream(
    probe: &LabelPassProbe,
    fixture: &mut Fixture,
    bucket_ms: i64,
    build: bool,
    now_ms: i64,
    lower_bound_ms: i64,
    skip: impl Fn(u64) -> bool,
) {
    let mut batch = Vec::with_capacity(BATCH);
    let mut i = 0u64;
    while let Some(row) = fixture.next_row(bucket_ms) {
        if !skip(i) {
            batch.push(row);
        }
        i += 1;
        if batch.len() == BATCH {
            probe.apply_batch_for_test(&mut batch, build, now_ms, lower_bound_ms);
        }
    }
    probe.apply_batch_for_test(&mut batch, build, now_ms, lower_bound_ms);
}

#[test]
fn the_label_cache_holds_a_series_in_under_220_bytes_and_peaks_under_1_2x() {
    // Every pass's generator is made before the first reading: each holds
    // its targets' base labels, which are the fixture's, not the cache's.
    let mut first = Fixture::new(SERIES, 0x880);
    let mut again = Fixture::new(SERIES, 0x880);
    let mut new = Fixture::new(NEW_SERIES, 0x881);
    let mut third = Fixture::new(SERIES, 0x880);
    let mut third_new = Fixture::new(NEW_SERIES, 0x881);
    let base = live();
    let probe = LabelPassProbe::new_for_test();

    // (a) The build: every series at `HOUR`, the window one hour.
    let now = HOUR + 600_000;
    stream(&probe, &mut first, HOUR, true, now, HOUR - H, |_| false);
    probe.commit_for_test(now, HOUR - H);
    assert_eq!(probe.series_count_for_test(), SERIES as usize);
    let built = live() - base;
    let per_series = built as f64 / SERIES as f64;
    eprintln!("(a) {built} B after the build, {per_series:.1} B per series");
    assert!(
        per_series <= 220.0,
        "(a) {per_series:.1} B per series, over 220"
    );

    // (b) The next hour's feed: every series again, and 10,000 new.
    let now = HOUR + H + 600_000;
    reset_peak();
    stream(&probe, &mut again, HOUR + H, false, now, HOUR, |_| false);
    stream(&probe, &mut new, HOUR + H, false, now, HOUR, |_| false);
    probe.commit_for_test(now, HOUR);
    assert_eq!(
        probe.series_count_for_test(),
        (SERIES + NEW_SERIES) as usize
    );
    let updated = live() - base;
    let pass_peak = peak() - base;
    let ratio = pass_peak as f64 / updated as f64;
    eprintln!(
        "(b) {updated} B after the feed, peak {pass_peak} B: {ratio:.3}x after, {:.3}x before",
        pass_peak as f64 / built as f64
    );
    assert!(ratio <= 1.2, "(b) the feed peaked at {ratio:.3}x");

    // (c) Two hours later every series but one in fifty registers again, and
    // the age-out removes the rest.
    let now = HOUR + 3 * H + 600_000;
    let one_in_fifty = |i: u64| i.is_multiple_of(50);
    stream(
        &probe,
        &mut third,
        HOUR + 3 * H,
        false,
        now,
        HOUR + 2 * H,
        one_in_fifty,
    );
    stream(
        &probe,
        &mut third_new,
        HOUR + 3 * H,
        false,
        now,
        HOUR + 2 * H,
        one_in_fifty,
    );
    let before = live() - base;
    reset_peak();
    probe.commit_for_test(now, HOUR + 2 * H);
    let removed = (SERIES + NEW_SERIES) as usize - probe.series_count_for_test();
    let age_out_peak = peak() - base;
    let ratio = age_out_peak as f64 / before as f64;
    eprintln!(
        "(c) removed {removed}, {before} B before, peak {age_out_peak} B: {ratio:.3}x, {} B after",
        live() - base
    );
    assert_eq!(removed, ((SERIES + NEW_SERIES) / 50) as usize);
    assert!(ratio <= 1.2, "(c) the age-out peaked at {ratio:.3}x");
}
