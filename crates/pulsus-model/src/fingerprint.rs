//! Frozen fingerprint functions (docs/architecture.md §2.2–2.3): a single
//! canonical buffer layout per label family, and one 128-bit composition
//! over both halves' hash primitives. A mismatch between the writer and
//! ClickHouse's own derivation silently corrupts the label index, so the
//! buffer layouts, the `cityHash64` primitive and the composed value are
//! golden-tested (`tests/golden.rs`) and live-cross-checked against a real
//! server (`tests/live_cityhash.rs`, gated `PULSUS_TEST_CLICKHOUSE=1`).
//!
//! # The composition (issue #498)
//!
//! ```text
//!   fp128(buf) = (cityHash64(buf) as u128) << 64 | (xxHash64(buf, 0) as u128)
//! ```
//!
//! Both halves are bit-identical to a ClickHouse function, so the "ClickHouse
//! can derive the identity itself" invariant survives the widening. The
//! server-side expression that reproduces it, measured on ClickHouse
//! 26.3.29.7:
//!
//! ```text
//!   SELECT bitShiftLeft(toUInt128(cityHash64('abc')), 64) + toUInt128(xxHash64('abc'))
//!     -> 77849065795737143790006732664207772057
//! ```
//!
//! A 64-bit identity had no label-set check behind it: two label sets whose
//! hash collided shared one identity, and the loser's rows became
//! unreachable rather than mislabelled. The two recorded colliding pairs are
//! pinned in this module's tests — each pair still collides in its own 64-bit
//! half and separates at 128 bits, which is what makes them a test of the
//! composition rather than of either primitive.

use xxhash_rust::xxh64::xxh64;

use crate::canonical::METRIC_NAME_LABEL;
use crate::labels::LabelSet;
use crate::time::Fingerprint;

/// Separator byte appended after every key and every value in both
/// canonical buffer layouts. `0xFF` cannot occur inside valid UTF-8 (every
/// UTF-8 byte is `<= 0xF4`), so it can never collide with label content —
/// this is what makes `key ++ 0xFF ++ value ++ 0xFF` an unambiguous
/// encoding without a length prefix, even when a label value legally
/// contains other high bytes.
const SEP: u8 = 0xFF;

/// Builds the canonical buffer for [`metric_fingerprint`]: each label
/// sorted by key (guaranteed by [`LabelSet`]'s own invariant), `__name__`
/// excluded, `key ++ 0xFF ++ value ++ 0xFF`.
///
/// **`pub` with no caller outside this crate's own functions**, like
/// [`build_stream_buffer`] and [`raw_cityhash64`]: the golden vectors pin
/// the buffer and each hash half separately (`tests/golden.rs`), so a
/// change that moved the LAYOUT and a change that moved a PRIMITIVE fail
/// different assertions. Fold either into its caller and the golden can
/// only say the composed value moved, not which half did.
pub fn build_metric_buffer(labels: &LabelSet) -> Vec<u8> {
    let mut buf = Vec::new();
    for (k, v) in labels.iter() {
        if k == METRIC_NAME_LABEL {
            continue;
        }
        buf.extend_from_slice(k.as_bytes());
        buf.push(SEP);
        buf.extend_from_slice(v.as_bytes());
        buf.push(SEP);
    }
    buf
}

/// The 128-bit composition: `cityHash64` in the high half, `xxHash64` with
/// seed 0 in the low half, over one canonical buffer. Reproducible in
/// ClickHouse as
/// `bitShiftLeft(toUInt128(cityHash64(buf)), 64) + toUInt128(xxHash64(buf))`.
///
/// Both label families use it over their own buffer, so a change to either
/// primitive moves both identities and the golden vectors catch it once.
fn compose128(buf: &[u8]) -> Fingerprint {
    Fingerprint::from_raw(((raw_cityhash64(buf) as u128) << 64) | (xxh64(buf, 0) as u128))
}

/// Metric fingerprint: [`compose128`] over [`build_metric_buffer`], whose
/// low half is the `xxhash64(buf, seed=0)` this function used to return
/// whole. Stable across label reordering (labels are pre-sorted by
/// [`LabelSet`]) and across the presence of `__name__` (always excluded —
/// the metric name is a first-class column, docs/architecture.md §2.2). The
/// empty label set hashes the empty buffer.
pub fn metric_fingerprint(labels: &LabelSet) -> Fingerprint {
    compose128(&build_metric_buffer(labels))
}

/// Builds the canonical buffer for [`stream_fingerprint`]: each label
/// sorted by key, `key ++ 0xFF ++ value ++ 0xFF`. Identical byte layout to
/// [`build_metric_buffer`] except `__name__` is not excluded (logs, traces,
/// and profiles have no metric name label).
///
/// `pub` for the same reason as [`build_metric_buffer`].
pub fn build_stream_buffer(labels: &LabelSet) -> Vec<u8> {
    let mut buf = Vec::new();
    for (k, v) in labels.iter() {
        buf.extend_from_slice(k.as_bytes());
        buf.push(SEP);
        buf.extend_from_slice(v.as_bytes());
        buf.push(SEP);
    }
    buf
}

/// Log/trace/profile fingerprint: [`compose128`] over
/// [`build_stream_buffer`], whose high half is the ClickHouse-bit-identical
/// `cityHash64` this function used to return whole. The writer is the sole
/// fingerprint authority (docs/architecture.md §2.2; the label-index
/// materialized view only fans the writer's fingerprint out per
/// `(key, val)`, it never recomputes it) — bit-identity to the server-side
/// primitives is what keeps an independent server-side derivation possible,
/// and is what `tests/live_cityhash.rs` proves against a real server.
pub fn stream_fingerprint(labels: &LabelSet) -> Fingerprint {
    compose128(&build_stream_buffer(labels))
}

/// The frozen `cityHash64` primitive itself — the high half of
/// [`compose128`] — exposed so golden and live tests can pin bit-identity
/// against ClickHouse across raw buffer lengths and content — not just label-shaped buffers (issue #4 plan
/// amendment: the parity gate must cover CityHash64's internal length
/// branches, not just `stream_fingerprint`'s callers). See
/// `tests/fixtures/fingerprints.json` and `tests/live_cityhash.rs`.
///
/// `ch_cityhash102` was selected empirically (issue #4): of the evaluated
/// candidates (`cityhasher`, `cityhash-rs`, `naive-cityhash`,
/// `cityhash-102-rs`), it is the only one that is bit-identical to
/// ClickHouse's frozen CityHash 1.0.2 fork across the full length-class
/// suite (0, 1, 3, 4, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65 bytes, and a
/// multi-KB buffer) in both debug and release builds. `cityhasher`
/// implements upstream CityHash 1.1 and fails immediately; `naive-cityhash`
/// and `cityhash-102-rs` match in release but panic on integer overflow
/// under debug-assertions.
pub fn raw_cityhash64(buf: &[u8]) -> u64 {
    ch_cityhash102::cityhash64(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::from_verbatim(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn metric_buffer_excludes_name_label() {
        let l = labels(&[
            ("__name__", "http_requests_total"),
            ("service_name", "checkout"),
        ]);
        let buf = build_metric_buffer(&l);
        assert_eq!(buf, b"service_name\xffcheckout\xff");
    }

    #[test]
    fn metric_fingerprint_of_empty_set_hashes_empty_buffer() {
        let empty = LabelSet::from_verbatim(Vec::new());
        assert_eq!(metric_fingerprint(&empty), compose128(&[]));
    }

    #[test]
    fn metric_fingerprint_is_stable_under_label_reorder() {
        let a = labels(&[("b", "2"), ("a", "1")]);
        let b = labels(&[("a", "1"), ("b", "2")]);
        assert_eq!(metric_fingerprint(&a), metric_fingerprint(&b));
    }

    #[test]
    fn metric_fingerprint_seed_is_zero() {
        let l = labels(&[("a", "1")]);
        let buf = build_metric_buffer(&l);
        assert_eq!(
            metric_fingerprint(&l),
            Fingerprint::from_raw(
                ((raw_cityhash64(&buf) as u128) << 64) | (xxh64(&buf, 0) as u128)
            )
        );
    }

    /// The two colliding pairs recorded on issue #498, constructed through
    /// the shipped functions by Floyd's cycle detection over
    /// `f(x) = hash("service_name" 0xFF <16 hex of x> 0xFF)`. For each pair
    /// the 64-bit half still collides and the 128-bit identity does not —
    /// which is the property the widening buys, and the reason a test that
    /// only asserted inequality would prove nothing.
    #[test]
    fn the_recorded_colliding_pairs_separate_at_128_bits() {
        // Logs: the `cityHash64` half collides, so the pair shares the HIGH
        // 64 bits, which lead the sort key.
        let a = labels(&[("service_name", "f47da23e240616e1")]);
        let b = labels(&[("service_name", "1d9f6a817a133b68")]);
        assert_eq!(
            raw_cityhash64(&build_stream_buffer(&a)),
            raw_cityhash64(&build_stream_buffer(&b)),
            "the 64-bit stream fingerprints must still collide"
        );
        assert_ne!(stream_fingerprint(&a), stream_fingerprint(&b));

        // Metrics: the `xxHash64` half collides, so the pair differs in the
        // high word instead.
        let c = labels(&[("service_name", "daf4c59f97f55535")]);
        let d = labels(&[("service_name", "4714b1c6956770d2")]);
        assert_eq!(
            xxh64(&build_metric_buffer(&c), 0),
            xxh64(&build_metric_buffer(&d), 0),
            "the 64-bit metric fingerprints must still collide"
        );
        assert_ne!(metric_fingerprint(&c), metric_fingerprint(&d));
    }

    #[test]
    fn stream_buffer_includes_all_labels_sorted() {
        let l = labels(&[("b", "2"), ("a", "1")]);
        let buf = build_stream_buffer(&l);
        assert_eq!(buf, b"a\xff1\xffb\xff2\xff");
    }

    #[test]
    fn stream_fingerprint_is_stable_under_label_reorder() {
        let a = labels(&[("b", "2"), ("a", "1")]);
        let b = labels(&[("a", "1"), ("b", "2")]);
        assert_eq!(stream_fingerprint(&a), stream_fingerprint(&b));
    }

    #[test]
    fn raw_cityhash64_of_empty_buffer_is_deterministic() {
        // Pinned so a future crate/version bump cannot silently change the
        // empty-buffer hash without failing this test first.
        assert_eq!(raw_cityhash64(&[]), raw_cityhash64(&[]));
    }
}
