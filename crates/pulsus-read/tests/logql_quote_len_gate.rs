//! Issue #294: the non-allocating LENGTH twins of the two Go quoting
//! rules the LogQL template's error texts embed, checked against the
//! text producers over an exhaustive input domain.
//!
//! **Why a length twin exists at all.** Charging the exact byte length
//! of an error message BEFORE rendering it (the `charged == allocated
//! == err.len()` relation the allocation gate asserts) needs the length
//! without the string. `gofmt::quote_bytes_len` and
//! `quote::go_time_quote_bytes_len` are that length.
//!
//! **Two oracles that catch different things — the round-4 finding.**
//! v5 of the plan derived the length and the text from ONE emitted-atom
//! stream. That is not an oracle: a walk emitting the wrong ATOMS is
//! invisible to both sides at every input length (measured — swapping
//! `Ch('a')` for `Ch('b')` left the whole sweep green). So:
//!
//! - the sweep below is the LENGTH's oracle, and its oracle is the text
//!   producer, which it never reads the source of;
//! - the text producer's oracle is the container-captured LogQL corpus
//!   (`tests/logqltest/corpus/`), whose expected values came from
//!   `grafana/loki:3.7.4`, not from us.
//!
//! Demonstrated, both directions — **for the `quote_bytes` pair only**
//! (issue #294 implementation notes):
//!
//! | perturbation of `quote_bytes` | this sweep | captured corpus |
//! |---|---|---|
//! | wrong text, same length (invalid byte escaped as `s[i] ^ 1`) | green | RED, `t2_printf.test:694` |
//! | wrong length, same text (`\t` counted as 1) | RED, `left: 3 right: 4` | green, `24 passed` |
//!
//! **The `go_time_quote_bytes` pair is NOT fully orthogonal, and saying
//! it was would be the false claim this file exists to avoid.** Its
//! wrong-text break behaves the same way (sweep green, corpus red at
//! `t6_errors_edges.test:77`), but its wrong-LENGTH break reddens the
//! corpus too: `DurationParseError::render` sizes one allocation from
//! `go_time_quote_bytes_len` and then `debug_assert_eq!`s the written
//! length against it, so a wrong length panics in debug builds. That is
//! a second observer of wrong-length, which strengthens detection and
//! creates no escape — but it means only the `quote_bytes` row above is
//! a demonstration that the two oracles are independent.
//!
//! **Where this stops.** The length side is exhaustive over the input
//! classes below. The text side is only as good as the captured corpus:
//! a text bug in a byte class no corpus row exercises is caught by
//! neither. That gap is bounded by adding rows, and #294 added
//! invalid-byte rows to `t6_errors_edges.test` to narrow it.

use pulsus_promql::eval::quote::{go_time_quote_bytes, go_time_quote_bytes_len};
use pulsus_read::logql::template::gofmt::{quote_bytes, quote_bytes_len};

/// Every input the sweep runs, in one place so both rules see the same
/// domain: every byte at run lengths 1..=8, every 2-byte pair, and
/// every Unicode scalar value as its UTF-8 bytes.
fn each_input(mut visit: impl FnMut(&[u8], &str)) {
    let mut buf = Vec::with_capacity(8);
    for b in 0u16..=255 {
        for n in 1..=8usize {
            buf.clear();
            buf.resize(n, b as u8);
            visit(&buf, "run");
        }
    }
    for a in 0u16..=255 {
        for b in 0u16..=255 {
            let pair = [a as u8, b as u8];
            visit(&pair, "pair");
        }
    }
    let mut utf8 = [0u8; 4];
    for c in '\0'..=char::MAX {
        visit(c.encode_utf8(&mut utf8).as_bytes(), "scalar");
    }
}

#[test]
fn quote_bytes_len_agrees_with_quote_bytes_over_the_whole_input_domain() {
    let start = std::time::Instant::now();
    let mut cases = 0u64;
    each_input(|input, class| {
        for quote in ['"', '\''] {
            let text = quote_bytes(input, quote);
            let len = quote_bytes_len(input, quote);
            assert_eq!(
                len,
                text.len(),
                "quote_bytes_len disagrees with quote_bytes: {class} input={input:02x?} \
                 quote={quote:?} text={text:?}"
            );
            cases += 1;
        }
    });
    println!(
        "LENSWEEP quote_bytes cases={cases} elapsed={:?}",
        start.elapsed()
    );
}

#[test]
fn go_time_quote_bytes_len_agrees_with_go_time_quote_bytes_over_the_whole_input_domain() {
    let start = std::time::Instant::now();
    let mut cases = 0u64;
    each_input(|input, class| {
        let text = go_time_quote_bytes(input);
        let len = go_time_quote_bytes_len(input);
        assert_eq!(
            len,
            text.len(),
            "go_time_quote_bytes_len disagrees with go_time_quote_bytes: {class} \
             input={input:02x?} text={text:?}"
        );
        cases += 1;
    });
    println!(
        "LENSWEEP go_time_quote_bytes cases={cases} elapsed={:?}",
        start.elapsed()
    );
}
