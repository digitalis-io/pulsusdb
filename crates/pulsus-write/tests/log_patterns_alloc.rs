//! Live-peak allocation gate for log-pattern aggregation (M7-C3, issue #171,
//! v4 AC 4). Unlike the cumulative-bytes model of `otlp_prescan_alloc.rs`, the
//! reservation must cover the *peak* transient (a mid-rehash old+new table both
//! live), which a cumulative counter cannot see — so this allocator is
//! **dealloc-aware**: a `live` counter (add on alloc, subtract on dealloc) and a
//! `fetch_max` peak, measured across one `aggregate_patterns` call.
//!
//! Two properties (v4 AC 4), each with the ceiling = the PRODUCTION reservation
//! formula (not a separate constant): for a batch,
//! `charge = Σ est_template_bound(row) + AGG_BASE_OVERHEAD
//!        + min(distinct_or_rows, CAP) × PATTERN_ROW_OVERHEAD`.
//!  1. peak-vs-charge: measured aggregation live-peak ≤ `charge` — the safety
//!     property "charge ≥ true peak" asserted directly (non-tautological: the
//!     formula is the claim under test). Cases: n=1 (minimum-table floor), the
//!     first hashbrown resize thresholds, and at-CAP.
//!  2. scale-invariance: per-row peak at N vs 2N distinct lines is linear and
//!     independent of body bytes beyond the 1 KiB prefix.
//!
//! Everything runs in the SINGLE `#[test]` below so no parallel test thread can
//! pollute the process-global counters.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pulsus_model::UnixNano;
use pulsus_write::LogRow;
use pulsus_write::patterns::{
    AGG_BASE_OVERHEAD, MAX_DISTINCT_PATTERNS_PER_BATCH, PATTERN_ROW_OVERHEAD, aggregate_patterns,
    est_template_bound,
};

/// Bytes currently live (add on alloc, subtract on dealloc), tracked only while
/// [`MEASURING`] is set.
static LIVE: AtomicU64 = AtomicU64::new(0);
/// The high-water mark of [`LIVE`] within a measured window.
static PEAK: AtomicU64 = AtomicU64::new(0);
/// Gate: counting is active only during the measured `aggregate_patterns` call,
/// so the test-harness/fixture allocations outside the window never pollute it.
static MEASURING: AtomicBool = AtomicBool::new(false);

struct LivePeakAlloc;

fn on_alloc(size: u64) {
    if MEASURING.load(Ordering::Relaxed) {
        let now = LIVE.fetch_add(size, Ordering::Relaxed) + size;
        PEAK.fetch_max(now, Ordering::Relaxed);
    }
}

fn on_dealloc(size: u64) {
    if MEASURING.load(Ordering::Relaxed) {
        // Saturating: a pre-window allocation freed inside the window would push
        // `live` negative; clamping at 0 only ever over-states the peak (a
        // conservative, never-under-counting bias for a `≤ charge` assertion).
        let mut cur = LIVE.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(size);
            match LIVE.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }
}

// SAFETY: delegates verbatim to the system allocator; the only side effects are
// relaxed atomic updates (gated by `MEASURING`) which allocate nothing and
// cannot re-enter the allocator.
unsafe impl GlobalAlloc for LivePeakAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        on_alloc(layout.size() as u64);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        on_dealloc(layout.size() as u64);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Charge the FULL old+new transient (issue #171 review test-gap fix): a
        // non-in-place realloc allocates the new buffer while the old one is
        // still live and memcpy's across, so the live peak momentarily includes
        // BOTH. Modeling only the net delta (new - old) would hide exactly the
        // `Vec<String>` grow transient the finding-1 under-reservation produced.
        let old = layout.size() as u64;
        let new = new_size as u64;
        on_alloc(new); // both buffers live — updates the peak at old+new
        on_dealloc(old); // the old buffer is then freed
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: LivePeakAlloc = LivePeakAlloc;

fn row(fingerprint: u64, body: String) -> LogRow {
    LogRow {
        service: "svc".to_string(),
        fingerprint,
        timestamp_ns: UnixNano(0),
        severity: 0,
        body,
        structured_metadata: String::new(),
    }
}

/// The PRODUCTION reservation charge for `rows` (mirrors
/// `LogWriter::admit_batch`).
fn charge(rows: &[LogRow]) -> u64 {
    let template_bounds: u64 = rows.iter().map(est_template_bound).sum();
    let distinct_cap = rows.len().min(MAX_DISTINCT_PATTERNS_PER_BATCH) as u64;
    template_bounds + AGG_BASE_OVERHEAD + distinct_cap * PATTERN_ROW_OVERHEAD
}

/// Measured live-peak (dealloc-aware) during `aggregate_patterns(rows)`, plus
/// the number of rows produced (kept alive until after the window closes).
fn measured_peak(rows: &[LogRow]) -> (u64, usize) {
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let agg = aggregate_patterns(rows);
    MEASURING.store(false, Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed);
    let n = agg.rows.len();
    black_box(&agg);
    (peak, n)
}

/// N distinct digit-free literal bodies (one row each) at `fingerprint`.
fn distinct_rows(n: usize, fingerprint: u64) -> Vec<LogRow> {
    (0..n)
        .map(|i| row(fingerprint, format!("alpha bravo charlie {}", ident(i))))
        .collect()
}

/// A deterministic all-alpha id (no digits ⇒ literal, distinct templates).
fn ident(mut n: usize) -> String {
    let mut s = String::new();
    loop {
        s.push((b'a' + (n % 26) as u8) as char);
        n /= 26;
        if n == 0 {
            break;
        }
    }
    s
}

#[test]
fn aggregation_live_peak_never_exceeds_the_charged_reservation() {
    // Warm-up so first-touch allocator costs are not attributed to a window.
    black_box(measured_peak(&distinct_rows(4, 1)));

    // (i) n = 1 — the minimum hashbrown table floor (the small-n boundary the
    // AGG_BASE_OVERHEAD term exists to cover).
    let one = distinct_rows(1, 1);
    let (peak, produced) = measured_peak(&one);
    assert_eq!(produced, 1);
    assert!(
        peak > 0,
        "the measured window must observe real allocations"
    );
    assert!(
        peak <= charge(&one),
        "n=1 peak {peak} exceeded charge {}",
        charge(&one)
    );

    // (ii) Resize thresholds: at and one past the first two hashbrown growth
    // boundaries (7/8-load crossings from a 4- then 8-bucket table), catching
    // the old+new mid-rehash transient.
    for n in [3usize, 4, 7, 8, 14, 15] {
        let rows = distinct_rows(n, 1);
        let (peak, _) = measured_peak(&rows);
        assert!(
            peak <= charge(&rows),
            "n={n} peak {peak} exceeded charge {} (mid-rehash transient not covered)",
            charge(&rows)
        );
    }

    // (iii) At-CAP: CAP distinct templates plus excess rows. The map holds
    // exactly CAP entries, the excess is dropped, and the peak stays under the
    // charge (whose per-entry term is capped at CAP).
    let mut at_cap = distinct_rows(MAX_DISTINCT_PATTERNS_PER_BATCH, 1);
    // 500 extra UNSEEN templates — all dropped from accounting.
    for i in 0..500 {
        at_cap.push(row(1, format!("zzz distinct excess {}", ident(i))));
    }
    let (peak, produced) = measured_peak(&at_cap);
    assert_eq!(
        produced, MAX_DISTINCT_PATTERNS_PER_BATCH,
        "the map holds exactly CAP entries; excess unseen rows add none"
    );
    assert!(
        peak <= charge(&at_cap),
        "at-CAP peak {peak} exceeded charge {}",
        charge(&at_cap)
    );

    // (iv) Scale-invariance: per-row peak at N vs 2N distinct lines is linear
    // (both under the charge), and body-size beyond the 1 KiB prefix does not
    // change the peak (the template is identical).
    let n1 = distinct_rows(200, 1);
    let n2 = distinct_rows(400, 1);
    let (peak1, _) = measured_peak(&n1);
    let (peak2, _) = measured_peak(&n2);
    assert!(peak1 <= charge(&n1) && peak2 <= charge(&n2));
    // Body-size independence: a huge body (well past the 1 KiB prefix) whose
    // examined prefix yields the SAME template as a short one produces an equal
    // peak (± allocator noise), because only templates enter the aggregation.
    let short: Vec<LogRow> = (0..64)
        .map(|i| row(1, format!("prefix token {} 9", ident(i))))
        .collect();
    let long: Vec<LogRow> = (0..64)
        .map(|i| {
            row(
                1,
                format!("prefix token {} 9 {}", ident(i), "x".repeat(4096)),
            )
        })
        .collect();
    let (peak_short, _) = measured_peak(&short);
    let (peak_long, _) = measured_peak(&long);
    let spread = peak_short.abs_diff(peak_long);
    assert!(
        spread <= 64 * 1024,
        "aggregation peak must be independent of body bytes beyond the 1 KiB prefix: \
         short={peak_short} long={peak_long} spread={spread}"
    );

    // (v) Dense short tokens (issue #171 review finding 1 + test-gap): a body
    // of ~512 one-byte tokens in 1 KiB. A SMALL batch (n=2) so the per-row
    // extraction scratch — not the map — dominates the charge. Before the
    // streaming fix, `extract_template` collected all ~512 tokens into a
    // `Vec<String>` (rendering each into a heap String), a transient far above
    // the charge; with the full old+new realloc accounting above, this case
    // goes RED against the old code and GREEN against the token-capped stream.
    let dense = dense_rows(2);
    let (peak, produced) = measured_peak(&dense);
    assert_eq!(produced, 2, "two distinct dense templates");
    assert!(
        peak <= charge(&dense),
        "dense-short-token peak {peak} exceeded charge {} — the extraction scratch is not \
         bounded to the {}-token cap (finding 1)",
        charge(&dense),
        pulsus_write::patterns::PATTERN_MAX_TOKENS
    );

    // (vi) Per-token render transient bound (issue #171 review finding 3, now
    // the round-4 BUDGET-AWARE render — no raw-length gate): a single token
    // whose render path uses a component VERBATIM (the `key` half of
    // `key=value`) must not build a transient larger than the 512-byte template
    // cap, whatever the token's structure. Each is an n=1 batch (the
    // single-token transient dominates the ~1792 B charge); all must hold
    // `peak ≤ charge`. Each long token sits inside the 1 KiB prefix, followed
    // by " end" so it is a COMPLETE token (not the dropped mid-token partial).
    let long_key = vec![row(1, format!("{}=x end", "a".repeat(1000)))];
    let short_key_long_value = vec![row(1, format!("k={} end", "a".repeat(1000)))];
    let long_unsplit = vec![row(1, format!("{} end", "a".repeat(1000)))];
    // A 257–512 byte digit-free `key=value` token renders with its LITERAL key
    // (D1 rule 3) — the near-template-cap literal render path; its single output
    // buffer (≤ 512 B) is the largest literal-render transient.
    let near_cap_literal_kv = vec![row(1, format!("{}=info end", "a".repeat(400)))];
    // A huge `key=value` with a multi-KB literal key AND value: its natural
    // render exceeds the 512-byte template cap, so the whole-token render (D1
    // rule 5) builds NOTHING for it (returns `overflowed`) — the token is
    // dropped at the boundary and the template gets a trailing `<_>`. The
    // transient stays ≤ charge with no per-token buffer allocated for the
    // oversized token at all.
    let huge_kv = vec![row(
        1,
        format!("{}={} end", "a".repeat(600), "b".repeat(400)),
    )];
    for (name, batch) in [
        ("long_key", &long_key),
        ("short_key_long_value", &short_key_long_value),
        ("long_unsplit", &long_unsplit),
        ("near_cap_literal_kv", &near_cap_literal_kv),
        ("huge_kv", &huge_kv),
    ] {
        let (peak, _) = measured_peak(batch);
        assert!(
            peak <= charge(batch),
            "{name}: per-token render transient peak {peak} exceeded charge {} — a render path \
             is not budget-bounded to the 512-byte template cap (finding 3)",
            charge(batch),
        );
    }
}

/// `n` DISTINCT bodies, each ~512 one-byte tokens in ~1 KiB: a leading literal
/// id token (keeps the templates distinct) followed by 511 single-digit tokens
/// (each classifying to `<_>`). The stress is the raw token COUNT, not bytes.
fn dense_rows(n: usize) -> Vec<LogRow> {
    (0..n)
        .map(|i| {
            let mut body = format!("row{} ", ident(i));
            body.push_str(&"1 ".repeat(511));
            row(1, body)
        })
        .collect()
}
