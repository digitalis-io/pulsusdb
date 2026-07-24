//! IP CIDR/range/single-address matching for the M8-LQ2 `ip()` line and
//! label filters (`|= ip("…")` / `!= ip("…")` / `| addr = ip("…")`).
//!
//! Clean-room from the published LogQL `ip()` semantics: a spec is a single
//! address (`10.1.2.3` / `2001:db8::1`), a CIDR block (`10.0.0.0/8`), or an
//! inclusive `start-end` range (`10.0.0.1-10.0.0.100`), for IPv4 **and**
//! IPv6. All matching is client-side ([`super::pipeline`]) — an IP-range
//! test over IP-shaped substrings has no token/skip-index prefilter, so it
//! cannot push down (see [`super::plan::is_pushable_line_filter`]).
//!
//! Uses only `std::net` (parsing + integer comparison) and the already-present
//! `regex` crate (the line-scan candidate extractor, compiled once per query).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::OnceLock;

use regex::Regex;

/// A compiled `ip()` matcher. Both families reduce to an inclusive integer
/// interval (`[start, end]`): a single address is `start == end`, a CIDR is
/// `[network, broadcast]`, an `a-b` range is the two endpoints. A candidate
/// address matches iff it is the same family and its integer value falls in
/// the interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IpMatcher {
    V4 { start: u32, end: u32 },
    V6 { start: u128, end: u128 },
}

/// Malformed `ip()` specifications — all client-caused parse errors (400-class
/// once surfaced through `PipelineError`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum IpMatchErr {
    #[error("empty ip() specification")]
    Empty,
    #[error("invalid ip() address {0:?}")]
    InvalidAddr(String),
    #[error("invalid ip() CIDR prefix length in {0:?}")]
    InvalidPrefix(String),
    #[error("mismatched address families in ip() range {0:?}")]
    MixedFamily(String),
    #[error("reversed ip() range {0:?} (start is after end)")]
    ReversedRange(String),
}

impl IpMatcher {
    /// Parses a single-address, CIDR (`addr/prefix`), or inclusive range
    /// (`start-end`) spec for IPv4 or IPv6. The forms are disjoint: only a
    /// CIDR contains `/`, only a range contains `-` (IPv6 addresses use `:`,
    /// never `-`), and a bare address contains neither.
    pub(crate) fn parse(spec: &str) -> Result<Self, IpMatchErr> {
        if spec.is_empty() {
            return Err(IpMatchErr::Empty);
        }
        if let Some((addr, prefix)) = spec.split_once('/') {
            Self::parse_cidr(spec, addr, prefix)
        } else if let Some((start, end)) = spec.split_once('-') {
            Self::parse_range(spec, start, end)
        } else {
            match spec
                .parse::<IpAddr>()
                .map_err(|_| IpMatchErr::InvalidAddr(spec.to_string()))?
            {
                IpAddr::V4(a) => {
                    let n = u32::from(a);
                    Ok(IpMatcher::V4 { start: n, end: n })
                }
                IpAddr::V6(a) => {
                    let n = u128::from(a);
                    Ok(IpMatcher::V6 { start: n, end: n })
                }
            }
        }
    }

    fn parse_cidr(spec: &str, addr: &str, prefix: &str) -> Result<Self, IpMatchErr> {
        let ip = addr
            .parse::<IpAddr>()
            .map_err(|_| IpMatchErr::InvalidAddr(spec.to_string()))?;
        let prefix: u32 = prefix
            .parse()
            .map_err(|_| IpMatchErr::InvalidPrefix(spec.to_string()))?;
        match ip {
            IpAddr::V4(a) => {
                if prefix > 32 {
                    return Err(IpMatchErr::InvalidPrefix(spec.to_string()));
                }
                let base = u32::from(a);
                // `<< 32` is UB for a u32 shift, so mask the /0 case explicitly.
                let mask = if prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - prefix)
                };
                let start = base & mask;
                Ok(IpMatcher::V4 {
                    start,
                    end: start | !mask,
                })
            }
            IpAddr::V6(a) => {
                if prefix > 128 {
                    return Err(IpMatchErr::InvalidPrefix(spec.to_string()));
                }
                let base = u128::from(a);
                let mask = if prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - prefix)
                };
                let start = base & mask;
                Ok(IpMatcher::V6 {
                    start,
                    end: start | !mask,
                })
            }
        }
    }

    fn parse_range(spec: &str, start: &str, end: &str) -> Result<Self, IpMatchErr> {
        let s = start
            .parse::<IpAddr>()
            .map_err(|_| IpMatchErr::InvalidAddr(spec.to_string()))?;
        let e = end
            .parse::<IpAddr>()
            .map_err(|_| IpMatchErr::InvalidAddr(spec.to_string()))?;
        match (s, e) {
            (IpAddr::V4(s), IpAddr::V4(e)) => {
                let (s, e) = (u32::from(s), u32::from(e));
                if s > e {
                    return Err(IpMatchErr::ReversedRange(spec.to_string()));
                }
                Ok(IpMatcher::V4 { start: s, end: e })
            }
            (IpAddr::V6(s), IpAddr::V6(e)) => {
                let (s, e) = (u128::from(s), u128::from(e));
                if s > e {
                    return Err(IpMatchErr::ReversedRange(spec.to_string()));
                }
                Ok(IpMatcher::V6 { start: s, end: e })
            }
            _ => Err(IpMatchErr::MixedFamily(spec.to_string())),
        }
    }

    /// `true` iff `addr` is in the matcher's family and inclusive interval.
    /// A family mismatch is never a match (an IPv4 label can never satisfy an
    /// IPv6 `ip()` spec, and vice versa).
    pub(crate) fn contains(&self, addr: &IpAddr) -> bool {
        match (self, addr) {
            (IpMatcher::V4 { start, end }, IpAddr::V4(a)) => {
                let n = u32::from(*a);
                *start <= n && n <= *end
            }
            (IpMatcher::V6 { start, end }, IpAddr::V6(a)) => {
                let n = u128::from(*a);
                *start <= n && n <= *end
            }
            _ => false,
        }
    }
}

/// The IPv4 candidate extractor (`a.b.c.d` dotted quads). Each match is then
/// validated by `std::net` — an out-of-range octet like `999.1.1.1` simply
/// fails to parse and is skipped.
fn ipv4_candidate_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"[0-9]{1,3}(?:\.[0-9]{1,3}){3}").expect("static ipv4 regex"))
}

/// A permissive IPv6 candidate extractor: a maximal (greedy) run of hex
/// groups separated by colons, with at least two colon-groups (the minimum a
/// valid IPv6 address — full or `::`-compressed — ever has). Greedy so it
/// captures the *whole* address in one match (avoiding the boundary bug where
/// a `::`-trailing branch would stop early and drop the tail); every match is
/// then validated by `std::net`, so a non-IPv6 run like `12:34:56` simply
/// fails to parse and is skipped.
///
/// The greedy run is *not* a pure superset, though: an unbracketed
/// `addr:port` such as `2001:db8::1:8080` is itself a valid but *different*
/// 8-group address, so the run would parse and could produce a false in-range
/// match. That ambiguous form is filtered out by [`is_ambiguous_v6_addr_port`]
/// in [`line_has_ip_in`] (the bracketed `[addr]:port` form is unaffected —
/// the regex stops at `]`, so the port never enters the candidate).
fn ipv6_candidate_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"[A-Fa-f0-9]{0,4}(?::[A-Fa-f0-9]{0,4}){2,}").expect("static ipv6 regex")
    })
}

/// Detects the ambiguous *unbracketed* IPv6 `addr:port` candidate form.
///
/// A greedy candidate like `2001:db8::1:8080` is indistinguishable, from the
/// candidate string alone, between the host `2001:db8::1` on port `8080` and
/// the literal 8-group address `2001:db8:0:0:0:0:1:8080`. Because the `::`
/// compression lets a trailing decimal group masquerade as a port (and vice
/// versa), we cannot know which the log author meant. The reference disambiguates
/// this in raw log text with the bracketed `[addr]:port` form, which the
/// extractor already handles correctly (the regex terminates at `]`).
///
/// For the genuinely ambiguous unbracketed form we take the conservative
/// choice mandated for this line filter: skip the candidate entirely rather
/// than emit a false in-range match from the mis-extended address. The trade
/// is that a real 8-group address whose last group happens to be a small
/// decimal (e.g. `2001:db8::1:2`) is also skipped — but that string is itself
/// ambiguous with a host+port, so declining to match it is the safe outcome.
///
/// The form is flagged only when the candidate's trailing `:group` is a decimal
/// port (`1..=65535`, ≤4 chars per the group regex) *and* the address prefix
/// with that group removed is *independently* a complete, valid IPv6 address —
/// which is only possible when the prefix contains `::`. A full 8-group address
/// (`2001:db8:0:0:0:0:1:2`) is never flagged: dropping its last group leaves 7
/// uncompressed groups, which does not parse.
fn is_ambiguous_v6_addr_port(candidate: &str) -> bool {
    let Some((prefix, last)) = candidate.rsplit_once(':') else {
        return false;
    };
    // Trailing group must look like a decimal port literal.
    if last.is_empty() || !last.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    if last.parse::<u16>().is_err() {
        return false;
    }
    // Ambiguous only if the port-stripped prefix is itself a valid IPv6 address
    // (implying a `::` absorbed the groups the trailing port would otherwise be).
    prefix.parse::<Ipv6Addr>().is_ok()
}

/// Scans `line` for any address (of the matcher's family) that satisfies
/// `matcher`. Used by the `ip()` line filter: the reference matches when the
/// log line contains an IP inside the spec. Only the matcher's family is
/// scanned (a V4 matcher never needs the IPv6 pass).
pub(crate) fn line_has_ip_in(matcher: &IpMatcher, line: &str) -> bool {
    match matcher {
        IpMatcher::V4 { .. } => ipv4_candidate_re().find_iter(line).any(|m| {
            m.as_str()
                .parse::<Ipv4Addr>()
                .is_ok_and(|a| matcher.contains(&IpAddr::V4(a)))
        }),
        IpMatcher::V6 { .. } => ipv6_candidate_re().find_iter(line).any(|m| {
            let candidate = m.as_str();
            // Skip the ambiguous unbracketed `addr:port` form so a mis-extended
            // address cannot produce a false in-range match (see
            // `is_ambiguous_v6_addr_port`).
            !is_ambiguous_v6_addr_port(candidate)
                && candidate
                    .parse::<Ipv6Addr>()
                    .is_ok_and(|a| matcher.contains(&IpAddr::V6(a)))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(spec: &str) -> IpMatcher {
        IpMatcher::parse(spec).unwrap_or_else(|e| panic!("parse {spec:?}: {e}"))
    }

    fn addr(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn contains(spec: &str, ip: &str) -> bool {
        m(spec).contains(&addr(ip))
    }

    // --- v4 single ---
    #[test]
    fn v4_single_address_matches_only_itself() {
        assert!(contains("10.1.2.3", "10.1.2.3"));
        assert!(!contains("10.1.2.3", "10.1.2.4"));
        assert!(!contains("10.1.2.3", "10.1.2.2"));
    }

    // --- v4 CIDR: network + broadcast, and just-outside both edges ---
    #[test]
    fn v4_cidr_boundaries() {
        // network and broadcast/last host are IN.
        assert!(contains("10.0.0.0/24", "10.0.0.0"));
        assert!(contains("10.0.0.0/24", "10.0.0.255"));
        // first address of the next block, and last address before the block.
        assert!(!contains("10.0.0.0/24", "10.0.1.0"));
        assert!(!contains("10.0.0.0/24", "9.255.255.255"));
    }

    #[test]
    fn v4_cidr_slash_eight_covers_its_range() {
        assert!(contains("10.0.0.0/8", "10.1.2.3"));
        assert!(!contains("10.0.0.0/8", "8.8.8.8"));
    }

    // --- v4 range: first, last, and just-outside both edges ---
    #[test]
    fn v4_range_boundaries() {
        assert!(contains("10.0.0.1-10.0.0.5", "10.0.0.1")); // first
        assert!(contains("10.0.0.1-10.0.0.5", "10.0.0.5")); // last
        assert!(!contains("10.0.0.1-10.0.0.5", "10.0.0.6")); // just past
        assert!(!contains("10.0.0.1-10.0.0.5", "10.0.0.0")); // just before
    }

    // --- v6 single ---
    #[test]
    fn v6_single_address_matches_only_itself() {
        assert!(contains("2001:db8::1", "2001:db8::1"));
        assert!(!contains("2001:db8::1", "2001:db8::2"));
    }

    // --- v6 CIDR: network, last, next block, AND predecessor block ---
    #[test]
    fn v6_cidr_boundaries() {
        assert!(contains("2001:db8::/126", "2001:db8::")); // network
        assert!(contains("2001:db8::/126", "2001:db8::3")); // last of the block
        assert!(!contains("2001:db8::/126", "2001:db8::4")); // first of the next block
        // last address of the immediately-preceding block (just before network).
        assert!(!contains(
            "2001:db8::/126",
            "2001:db7:ffff:ffff:ffff:ffff:ffff:ffff"
        ));
    }

    #[test]
    fn v6_cidr_slash_thirtytwo_covers_its_range() {
        assert!(contains("2001:db8::/32", "2001:db8::1"));
        assert!(!contains("2001:db8::/32", "2001:db9::1"));
    }

    // --- v6 range: first, last, and just-outside both edges ---
    #[test]
    fn v6_range_boundaries() {
        assert!(contains("2001:db8::1-2001:db8::5", "2001:db8::1")); // first
        assert!(contains("2001:db8::1-2001:db8::5", "2001:db8::5")); // last
        assert!(!contains("2001:db8::1-2001:db8::5", "2001:db8::6")); // just past
        assert!(!contains("2001:db8::1-2001:db8::5", "2001:db8::")); // just before
    }

    // --- family isolation ---
    #[test]
    fn a_family_mismatch_never_matches() {
        assert!(!contains("10.0.0.0/8", "2001:db8::1"));
        assert!(!contains("2001:db8::/32", "10.1.2.3"));
    }

    // --- malformed specs ---
    #[test]
    fn malformed_specs_are_parse_errors() {
        assert_eq!(IpMatcher::parse(""), Err(IpMatchErr::Empty));
        assert!(matches!(
            IpMatcher::parse("10.0.0.0/33"),
            Err(IpMatchErr::InvalidPrefix(_))
        ));
        assert!(matches!(
            IpMatcher::parse("2001:db8::/129"),
            Err(IpMatchErr::InvalidPrefix(_))
        ));
        assert!(matches!(
            IpMatcher::parse("nonsense"),
            Err(IpMatchErr::InvalidAddr(_))
        ));
        assert!(matches!(
            IpMatcher::parse("10.0.0.5-10.0.0.1"),
            Err(IpMatchErr::ReversedRange(_))
        ));
        assert!(matches!(
            IpMatcher::parse("2001:db8::5-2001:db8::1"),
            Err(IpMatchErr::ReversedRange(_))
        ));
        assert!(matches!(
            IpMatcher::parse("10.0.0.1-2001:db8::1"),
            Err(IpMatchErr::MixedFamily(_))
        ));
    }

    // --- line scan (candidate extraction inside a larger log line) ---
    #[test]
    fn line_scan_finds_a_v4_ip_embedded_in_text() {
        let m = m("10.0.0.0/8");
        assert!(line_has_ip_in(&m, "request from 10.1.2.3 completed in 4ms"));
        assert!(!line_has_ip_in(&m, "request from 8.8.8.8 completed in 4ms"));
        assert!(!line_has_ip_in(&m, "no ip here at all"));
    }

    #[test]
    fn line_scan_finds_a_v6_ip_embedded_in_text() {
        let m = m("2001:db8::/32");
        assert!(line_has_ip_in(&m, "client [2001:db8::1]:8080 connected"));
        assert!(!line_has_ip_in(&m, "client [2001:db9::1]:8080 connected"));
    }

    #[test]
    fn line_scan_respects_range_boundaries() {
        let m = m("10.0.0.1-10.0.0.5");
        assert!(line_has_ip_in(&m, "addr=10.0.0.5 ok"));
        assert!(!line_has_ip_in(&m, "addr=10.0.0.6 ok"));
    }

    // --- addr:port boundary (M8-LQ2 review finding) ---

    #[test]
    fn line_scan_bracketed_v6_host_port_matches_the_host() {
        // `[addr]:port` is unambiguous: the regex terminates at `]`, so the
        // port never pollutes the candidate and the host address matches.
        let range = m("2001:db8::/32");
        assert!(line_has_ip_in(
            &range,
            "client [2001:db8::1]:8080 connected"
        ));
        // An exact single-address filter also matches through the brackets.
        let exact = m("2001:db8::1");
        assert!(line_has_ip_in(
            &exact,
            "client [2001:db8::1]:8080 connected"
        ));
    }

    #[test]
    fn line_scan_unbracketed_v6_addr_port_is_not_mis_extended() {
        // `2001:db8::1:8080` (host `2001:db8::1` + port 8080, no brackets) is a
        // *valid but different* 8-group address. A filter covering the
        // mis-extended address but NOT the intended host must not produce a
        // false in-range match — the ambiguous candidate is skipped instead.
        //
        // Range endpoints: `2001:db8::1:8000`..=`2001:db8::1:9000` contain the
        // mis-parsed `2001:db8::1:8080` (last group 0x8080) but exclude the
        // host `2001:db8::1` (== `2001:db8::0:1`).
        let range = m("2001:db8::1:8000-2001:db8::1:9000");
        assert!(!line_has_ip_in(&range, "peer 2001:db8::1:8080 up"));
    }

    #[test]
    fn line_scan_ipv4_addr_port_matches_the_host() {
        // IPv4 `a.b.c.d:port`: the dotted-quad regex stops at `:`, so the port
        // is excluded from the candidate and the host matches normally (there
        // is no addr:port ambiguity for IPv4).
        let m = m("10.0.0.0/8");
        assert!(line_has_ip_in(&m, "peer 10.0.0.1:8080 up"));
    }
}
