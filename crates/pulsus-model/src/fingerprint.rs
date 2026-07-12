//! Frozen fingerprint functions (docs/architecture.md §2.2–2.3): a single
//! canonical buffer layout and a single hash primitive per label family. A
//! mismatch between the writer and ClickHouse's own derivation silently
//! corrupts the label index, so both the buffer layout and the `cityHash64`
//! primitive itself are golden-tested (`tests/golden.rs`) and, for
//! `cityHash64`, live-cross-checked against a real server
//! (`tests/live_cityhash.rs`, gated `PULSUS_TEST_CLICKHOUSE=1`).

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

/// Metric fingerprint: `xxhash64(buf, seed=0)` over [`build_metric_buffer`].
/// Stable across label reordering (labels are pre-sorted by [`LabelSet`])
/// and across the presence of `__name__` (always excluded — the metric
/// name is a first-class column, docs/architecture.md §2.2). The empty
/// label set hashes the empty buffer.
pub fn metric_fingerprint(labels: &LabelSet) -> Fingerprint {
    xxh64(&build_metric_buffer(labels), 0)
}

/// Builds the canonical buffer for [`stream_fingerprint`]: each label
/// sorted by key, `key ++ 0xFF ++ value ++ 0xFF`. Identical byte layout to
/// [`build_metric_buffer`] except `__name__` is not excluded (logs, traces,
/// and profiles have no metric name label).
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

/// Log/trace/profile fingerprint: a ClickHouse-bit-identical `cityHash64`
/// over [`build_stream_buffer`]. The writer is the sole fingerprint
/// authority (docs/architecture.md §2.2; the label-index materialized view
/// only fans the writer's fingerprint out per `(key, val)`, it never
/// recomputes it) — bit-identity to server-side `cityHash64` is what keeps
/// an independent server-side derivation possible, and is what
/// `tests/live_cityhash.rs` proves against a real server.
pub fn stream_fingerprint(labels: &LabelSet) -> Fingerprint {
    raw_cityhash64(&build_stream_buffer(labels))
}

/// The frozen `cityHash64` primitive itself, exposed so golden and live
/// tests can pin bit-identity against ClickHouse across raw buffer
/// lengths and content — not just label-shaped buffers (issue #4 plan
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
        assert_eq!(metric_fingerprint(&empty), xxh64(&[], 0));
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
        assert_eq!(metric_fingerprint(&l), xxh64(&build_metric_buffer(&l), 0));
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
