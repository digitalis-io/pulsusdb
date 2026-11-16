//! Stub.

use axum::http::HeaderMap;

pub(crate) fn allows_utf8_label_names(_headers: &HeaderMap) -> bool {
    false
}

pub(crate) fn attime(_s: &str, _now_ms: i64) -> i64 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn accept(values: &[&str]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for v in values {
            h.append("accept", HeaderValue::from_str(v).expect("a header value"));
        }
        h
    }

    /// C4: the capability is an `Accept` parameter equal to `true`, on any
    /// of the `Accept` headers.
    #[test]
    fn c4_the_capability_is_an_accept_parameter() {
        assert!(allows_utf8_label_names(&accept(&[
            "application/json; allow-utf8-labelnames=true"
        ])));
        assert!(allows_utf8_label_names(&accept(&[
            "application/json",
            "*/*; allow-utf8-labelnames=true"
        ])));
        for h in [
            accept(&["application/json"]),
            accept(&["application/json; allow-utf8-labelnames=false"]),
            HeaderMap::new(),
        ] {
            assert!(!allows_utf8_label_names(&h), "{h:?}");
        }
    }

    /// C5: the reference's time parser. A compound offset reads its first
    /// number with the rest of the text as its unit: the reference's unit
    /// loop (`pkg/og/util/attime/attime.go:79-85`) takes every byte up to
    /// the end, so `1h30m` is one hour.
    #[test]
    fn c5_attime_reads_the_reference_forms() {
        const N: i64 = 1_794_750_000_000;
        for (input, want) in [
            ("1794744600", 1_794_744_600_000),
            ("1794744600000", 1_794_744_600_000),
            ("1794744600000000000", 1_794_744_600_000),
            ("20261115", 1_794_700_800_000),
            ("now-1h", N - 3_600_000),
            ("now-1h30m", N - 3_600_000),
            ("now-30m", N - 1_800_000),
            ("now", N),
        ] {
            assert_eq!(attime(input, N), want, "{input}");
        }
    }
}
