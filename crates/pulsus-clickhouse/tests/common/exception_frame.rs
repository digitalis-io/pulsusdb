//! The tagged-exception-frame layout, in **one** place (issue #412).
//!
//! [`frame_bytes`] is shared by the hermetic mock
//! (`tests/mock_clickhouse.rs`) and the live capture gate
//! (`tests/live_clickhouse.rs`'s
//! `the_mock_frame_layout_matches_a_real_streamed_exception`). That is
//! deliberate: a mock whose byte layout is asserted only against our own
//! reading of our own code is one edit away from being a false gate, so this
//! builder is checked against a real streamed frame from the connected server
//! on every CI run rather than reasoned about.
//!
//! Not duplicated anywhere. Both test binaries pull it in with
//! `#[path = "common/exception_frame.rs"] mod exception_frame;` — files under
//! `tests/` subdirectories are not compiled as their own test targets.
//!
//! # The layout, from a live 26.3.17.110 capture
//!
//! ```text
//! opening   \r\n__exception__\r\n<tag>\r\nCode: 395. DB::Exception: …
//! closing   …(official build))\n<len> <tag>\r\n__exception__\r\n
//! ```
//!
//! **The two ends are in opposite orders**: the opening is marker-then-tag,
//! the closing is tag-then-marker. `extract_exception_new`'s `strip_suffix`
//! chain (`vendor/clickhouse/src/response.rs`) parses the closing only, so it
//! establishes that order and says nothing about the opening.
//!
//! `<len>` counts the message **including** its terminating `\n`.

#![allow(dead_code)] // each test binary uses a subset

/// The tenant-controlled literal at the centre of #412, **without** its
/// terminating newline: a stored ClickHouse error message, which is one of the
/// things people point a log database at.
///
/// Three properties are load-bearing and must not be "improved" away — they
/// are exactly what `extract_exception_old` keys on
/// (`vendor/clickhouse/src/response.rs`): with a newline appended it ends
/// `))\n`, and it carries `DB::` and `Exception:` after its last `Code:`.
/// Dropping the trailing newline or the `(official build))` tail makes every
/// fixture using it vacuous, so its byte length is pinned by
/// `the_forged_literal_still_has_the_shape_the_old_extractor_keys_on`
/// (`tests/mock_clickhouse.rs`) and re-asserted on the live round trip.
pub const FORGED_BODY: &str =
    "Code: 210. DB::Exception: forged (FAKE) (version 26.3.17.110 (official build))";

/// [`FORGED_BODY`] as a whole log line: 79 bytes, ending `))\n`.
pub fn forged_line() -> String {
    format!("{FORGED_BODY}\n")
}

/// The literal that opens a tagged exception frame, before the tag.
pub const EXC_OPEN: &[u8] = b"\r\n__exception__\r\n";

/// The literal that closes it.
pub const EXC_CLOSE: &[u8] = b"__exception__\r\n";

/// Builds the exact bytes ClickHouse streams for a tagged exception.
///
/// `message` is the exception text **without** its terminating newline (i.e.
/// `Code: N. DB::Exception: … (official build))`); the newline this appends is
/// what the declared length counts.
pub fn frame_bytes(message: &str, tag: &str) -> Vec<u8> {
    let declared = message.len() + 1; // the trailing `\n` is inside the length
    let mut out = Vec::new();
    out.extend_from_slice(EXC_OPEN);
    out.extend_from_slice(tag.as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(message.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(format!("{declared} ").as_bytes());
    out.extend_from_slice(tag.as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(EXC_CLOSE);
    out
}

/// Every start offset of [`EXC_OPEN`] in `haystack`.
///
/// A well-formed frame contains exactly two: its opening, and the `\r\n`
/// immediately before the closing marker. So the frame begins at the
/// second-to-last hit.
pub fn exc_open_offsets(haystack: &[u8]) -> Vec<usize> {
    let mut out = Vec::new();
    let mut from = 0usize;
    while from + EXC_OPEN.len() <= haystack.len() {
        match haystack[from..]
            .windows(EXC_OPEN.len())
            .position(|w| w == EXC_OPEN)
        {
            Some(rel) => {
                out.push(from + rel);
                from += rel + 1;
            }
            None => break,
        }
    }
    out
}

/// Splits a captured frame into `(message_without_newline, declared_len)`
/// using the **closing** trailer only — the same rule
/// `extract_exception_new` applies — so a rebuild-and-compare against
/// [`frame_bytes`] is not circular on the opening's field order.
///
/// `tag` comes from the `X-ClickHouse-Exception-Tag` response header, never
/// from the frame.
pub fn message_from_closing_trailer(frame: &[u8], tag: &str) -> (String, usize) {
    let rem = frame
        .strip_suffix(b"\r\n__exception__\r\n".as_slice())
        .expect("frame ends with the closing marker")
        .strip_suffix(tag.as_bytes())
        .expect("the tag precedes the closing marker")
        .strip_suffix(b" ".as_slice())
        .expect("a space separates the declared length from the tag");

    let len_start = rem
        .iter()
        .rposition(|&b| b == b'\n')
        .expect("the message is newline-terminated")
        + 1;
    let declared: usize = std::str::from_utf8(&rem[len_start..])
        .expect("the declared length is ASCII")
        .parse()
        .expect("the declared length is a number");

    let msg = &rem[len_start - declared..len_start];
    let text = String::from_utf8_lossy(&msg[..msg.len() - 1]).into_owned();
    (text, declared)
}
