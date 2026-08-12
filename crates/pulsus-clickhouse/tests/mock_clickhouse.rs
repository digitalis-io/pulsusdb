//! Hermetic **client-parser gates** for issue #412, driven by a raw-TCP mock
//! ClickHouse.
//!
//! # What these establish, and what they do not
//!
//! Every test here chooses the decompressed chunk boundaries the client's
//! parser sees, by emitting one LZ4 block per chunk (`Lz4Decoder` yields
//! exactly one `Chunk` per block, `vendor/clickhouse/src/compression/lz4.rs`).
//! That makes them **client-parser gates**: they establish what our parser
//! does with a given byte sequence and chunk split. They do **not** establish
//! that ClickHouse emits that framing — protocol behaviour is gated only by
//! the live suite (`tests/live_clickhouse.rs`), and the frame layout used here
//! is pinned against a real streamed frame on every CI run by
//! `the_mock_frame_layout_matches_a_real_streamed_exception` there, through
//! the shared [`exception_frame::frame_bytes`] builder.
//!
//! # The defect (issue #412)
//!
//! `extract_exception` ran `extract_exception_old`'s `rfind(b"Code:")` on any
//! chunk ending `))\n`, including on a **tagged** response where the server
//! frames its exceptions properly. Result bytes are tenant data — a stored
//! ClickHouse error message is the realistic case — so a successful query
//! whose last row ends `))\n` came back as zero rows and a fabricated
//! `Code: 210`. 210 is in `RETRYABLE_SERVER_CODES`, so the fabrication was
//! also retried and demoted a healthy endpoint through
//! `PooledConn::report_transport_failure`.
//!
//! No new dependency: `std::net` + `std::thread` for the server, and
//! `clickhouse::_priv::lz4_compress` (`#[doc(hidden)]`, semver-exempt —
//! acceptable because the crate is vendored and pinned; recorded in
//! `vendor/clickhouse/PATCHES.md` §2) for the block framing.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChError, ChProto, QuerySettings, Row};

#[path = "common/exception_frame.rs"]
mod exception_frame;
use exception_frame::{EXC_CLOSE, EXC_OPEN, FORGED_BODY, forged_line, frame_bytes};

// === fixtures ===

/// The tag the mock declares on a tagged response. Shape taken from the
/// measured census (200 consecutive responses on 26.3.17.110: 200 distinct
/// 16-byte lowercase `a–z` tags).
const TAG: &str = "zgnglmkjouifsqby";

/// A *different* 16-byte tag, for the fixture a tenant plants in its own data.
const WRONG_TAG: &str = "aaaaaaaaaaaaaaaa";

/// A real streamed exception's text, without its terminating newline.
/// `FUNCTION_THROW_IF_VALUE_IS_NON_ZERO` is code 395 and is **not** in
/// `RETRYABLE_SERVER_CODES`, so a test that accepts the forged 210 instead
/// also flips the retry decision.
const REAL_MSG: &str = "Code: 395. DB::Exception: boom: while executing 'FUNCTION throwIf(equals(__table1.number, 2500000_UInt32) :: 1, 'boom'_String) :: 0'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 26.3.17.110 (official build))";

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct OneCol {
    v: String,
}

// === RowBinaryWithNamesAndTypes helpers ===

/// LEB128, which is what RowBinary uses for string lengths.
fn varint(mut n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

/// The `RowBinaryWithNamesAndTypes` prelude for a single `v String` column —
/// the format `Query::fetch` requests when validation is on (the default).
fn header_one_string_col() -> Vec<u8> {
    let mut out = vec![1u8]; // one column
    out.extend_from_slice(&varint(1));
    out.extend_from_slice(b"v");
    out.extend_from_slice(&varint(6));
    out.extend_from_slice(b"String");
    out
}

fn row(value: &[u8]) -> Vec<u8> {
    let mut out = varint(value.len());
    out.extend_from_slice(value);
    out
}

/// One block's worth of result bytes: the column header plus `values`.
fn data_block(values: &[&[u8]]) -> Vec<u8> {
    let mut out = header_one_string_col();
    for v in values {
        out.extend_from_slice(&row(v));
    }
    out
}

// === the mock server ===

/// A single-purpose HTTP/1.1 server that answers the pool's `SELECT 1` probe
/// with an empty 200 and every other request with a canned, LZ4-framed body.
///
/// One LZ4 block per element of `blocks`, so the caller chooses exactly the
/// chunk boundaries `DetectDbException` will see.
///
/// Binds an **ephemeral** loopback port (`127.0.0.1:0`) rather than a fixed
/// one, so it declares nothing in the reserved 31000-31999 range that
/// `pulsus-server`'s `live_port_uniqueness` guard has to arbitrate, and two
/// of these can run concurrently. The bound address is kept whole (never a
/// `port`-named binding) for the same reason.
struct MockCh {
    addr: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockCh {
    fn start(tag: Option<&str>, blocks: &[Vec<u8>]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock");
        let addr = listener.local_addr().expect("local_addr");
        listener.set_nonblocking(true).expect("nonblocking");

        let mut body = Vec::new();
        for block in blocks {
            body.extend_from_slice(
                &clickhouse::_priv::lz4_compress(block).expect("lz4 compress a mock block"),
            );
        }

        let tag = tag.map(str::to_string);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            while !stop_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((sock, _)) => serve_one(sock, tag.as_deref(), &body),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            addr,
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for MockCh {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn serve_one(mut sock: TcpStream, tag: Option<&str>, body: &[u8]) {
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
    sock.set_nonblocking(false).ok();

    // Read the request head, then exactly `Content-Length` more bytes.
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let head_end = loop {
        match sock.read(&mut tmp) {
            Ok(0) => return,
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4) {
                    break i;
                }
            }
            Err(_) => return,
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
    let content_length: usize = head
        .split("\r\n")
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    while buf.len() < head_end + content_length {
        match sock.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => break,
        }
    }
    let req_body = String::from_utf8_lossy(&buf[head_end..]).to_string();

    // The pool's startup probe. Answered with an empty 200 so `ChClient::new`
    // succeeds; the canned body is reserved for the query under test.
    if req_body.trim() == "SELECT 1" {
        let _ =
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        let _ = sock.flush();
        return;
    }

    let mut resp = String::from("HTTP/1.1 200 OK\r\n");
    if let Some(tag) = tag {
        resp.push_str(&format!("X-ClickHouse-Exception-Tag: {tag}\r\n"));
    }
    resp.push_str(&format!("Content-Length: {}\r\n", body.len()));
    resp.push_str("Connection: close\r\n\r\n");
    let _ = sock.write_all(resp.as_bytes());
    let _ = sock.write_all(body);
    let _ = sock.flush();
}

// === driver ===

/// Runs one scenario end to end through the production wrapper — `ChClient`,
/// `ChRowStream`, `ChError::from` — not through `clickhouse::Client` directly,
/// so the classification a caller actually sees is what is asserted.
async fn drive(tag: Option<&str>, blocks: &[Vec<u8>]) -> (Vec<String>, Option<ChError>) {
    let mock = MockCh::start(tag, blocks);
    let cfg = ChConnConfig {
        server: "127.0.0.1".to_string(),
        http_port: mock.addr.port(),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 1,
        query_timeout: Duration::from_secs(10),
        ..ChConnConfig::default()
    };
    let client = ChClient::new(cfg).await.expect("connect to the mock");
    let mut stream = client
        .query_stream::<OneCol>("SELECT v FROM mock", &QuerySettings::new())
        .await
        .expect("query_stream");

    let mut rows = Vec::new();
    let mut failure = None;
    while let Some(item) = stream.next().await {
        match item {
            Ok(r) => rows.push(r.v),
            Err(e) => {
                failure = Some(e);
                break;
            }
        }
    }
    (rows, failure)
}

fn expect_395(err: Option<ChError>) -> String {
    match err.expect("the real exception must reach the caller") {
        ChError::Server { code, message } => {
            assert_eq!(
                code, 395,
                "the real code, not a fabricated one: {message:?}"
            );
            assert!(
                message.contains("FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"),
                "the body must be the real exception, not something else \
                 carrying a code: {message:?}"
            );
            message
        }
        other => panic!("expected ChError::Server {{ code: 395 }}, got {other:?}"),
    }
}

// === the gates ===

/// The literal every `))\n` fixture below rests on. Pinned so an "improving"
/// edit that drops the trailing newline or the `(official build))` tail — the
/// three conditions `extract_exception_old` keys on — reddens here instead of
/// silently making those tests vacuous (plan v1 edge case 6).
#[test]
fn the_forged_literal_still_has_the_shape_the_old_extractor_keys_on() {
    let forged = forged_line();
    assert_eq!(
        FORGED_BODY.len(),
        78,
        "verbatim fixture, pinned against an edit"
    );
    assert_eq!(forged.len(), 79);
    assert!(forged.ends_with("))\n"));
    let last_code = forged.rfind("Code:").expect("a Code: marker");
    assert!(forged[last_code..].contains("DB::"));
    assert!(forged[last_code..].contains("Exception:"));
}

/// **AC1 (hermetic twin), the headline defect.** A tagged, entirely
/// successful response whose last row ends `))\n` must deliver its row.
///
/// Measured before the fix: `rows=0` and `ChError::Server { code: 210 }` —
/// a query ClickHouse completed, reported to the caller as a retryable server
/// failure. Client-parser gate; the live twin is
/// `a_successful_read_whose_last_row_ends_in_close_parens_is_not_an_error`.
#[tokio::test]
async fn a_tagged_success_whose_last_row_ends_in_close_parens_yields_its_row() {
    let forged = forged_line();
    let (rows, err) = drive(Some(TAG), &[data_block(&[forged.as_bytes()])]).await;
    assert!(err.is_none(), "a successful query must not fail: {err:?}");
    assert_eq!(rows, vec![forged]);
}

/// **AC5.** The retained `tag == None` arm, exercised for real.
///
/// The mock serves a **pre-25.11-shaped** body: no `X-ClickHouse-Exception-Tag`
/// header, and the raw exception text appended to the result bytes, ending
/// `))\n`. Only that shape reaches `extract_exception_old` — a header-stripped
/// *modern* response would be a false gate, because the body still carries the
/// tagged trailer, so the chunk ends `__exception__\r\n` rather than `))\n`.
///
/// A tag-absent server is reachable on `main` despite the 26.3 floor:
/// `PULSUS_SKIP_DDL` skips `run_init` entirely
/// (`crates/pulsus-server/src/serve.rs:645-685`), and the version gate lives
/// inside it (`crates/pulsus-schema/src/controller.rs:84-100`). Deleting the
/// old extractor turns this into
/// `Decode("not enough data, probably a row type mismatches a database
/// schema")` — a wrong diagnosis of a real server exception, and
/// non-retryable where the truth may be transient. That is the regression
/// this exists to catch.
#[tokio::test]
async fn an_untagged_response_still_reports_its_real_exception_code() {
    let mut block = data_block(&[b"ok"]);
    block.extend_from_slice(REAL_MSG.as_bytes());
    block.push(b'\n');
    let (rows, err) = drive(None, &[block]).await;
    expect_395(err);
    assert!(
        rows.is_empty(),
        "the crate rejects the whole chunk it found the exception in"
    );
}

/// **AC10.** A tagged frame cut across two chunks must still be sliced by its
/// declared length.
///
/// Before the fix: `Code: 210` (the forgery, from the data chunk). Under the
/// withdrawn gate-only design (`Some(_) => None`): four garbage rows decoded
/// out of the frame bytes and then `not enough data…` — i.e. the frame is not
/// merely dropped, it is handed to the row decoder. Client-parser gate.
#[tokio::test]
async fn a_tagged_exception_frame_split_across_chunks_reports_its_real_code() {
    let frame = frame_bytes(REAL_MSG, TAG);
    let cut = frame.len() - 24;
    assert!(!frame[..cut].ends_with(EXC_CLOSE));
    let (rows, err) = drive(
        Some(TAG),
        &[
            data_block(&[b"row-1"]),
            frame[..cut].to_vec(),
            frame[cut..].to_vec(),
        ],
    )
    .await;
    expect_395(err);
    assert_eq!(
        rows,
        vec!["row-1".to_string()],
        "no row may be decoded out of the frame bytes"
    );
}

/// **AC11.** A frame that opens and never closes must fail the stream with the
/// server's own code, never end as `Ok` and never be a short read.
///
/// The withheld bytes are surfaced rather than dropped: the anchor and the
/// `\r\n` after it are stripped, so byte 0 of the message is the server's
/// `Code: N` and `parse_exception_code` classifies it correctly. The tail of
/// the partial trailer stays attached to the description — lossy there, exact
/// in the code. Client-parser gate.
#[tokio::test]
async fn a_tagged_frame_that_never_terminates_is_an_error_not_a_short_read() {
    let frame = frame_bytes(REAL_MSG, TAG);
    let cut = frame.len() - 24;
    let (rows, err) = drive(Some(TAG), &[data_block(&[b"row-1"]), frame[..cut].to_vec()]).await;
    expect_395(err);
    assert_eq!(rows, vec!["row-1".to_string()]);
}

/// **AC12.** The anchor must not become a second forgery surface: tenant bytes
/// that mimic a whole exception frame but carry a tag that is not this
/// response's are result data.
///
/// Written so that hard-coding the response's real tag into the fixture makes
/// it fail — with the real tag the anchor matches, the bytes are withheld, no
/// row is yielded and the stream fails. The assumption this rests on is
/// recorded, not argued (`vendor/clickhouse/PATCHES.md` §2): the tag is
/// chosen by the server per response, and nothing measured establishes
/// non-reuse.
#[tokio::test]
async fn tenant_bytes_that_mimic_an_exception_frame_with_the_wrong_tag_are_data() {
    assert_ne!(
        WRONG_TAG, TAG,
        "the whole point is that the planted tag is NOT the response's"
    );
    let mut planted = Vec::new();
    planted.extend_from_slice(&frame_bytes(REAL_MSG, WRONG_TAG));
    assert!(
        planted.ends_with(EXC_CLOSE),
        "the strongest form: the chunk itself ends with the closing marker"
    );
    let planted = String::from_utf8(std::mem::take(&mut planted)).expect("utf-8 fixture");

    let (rows, err) = drive(Some(TAG), &[data_block(&[planted.as_bytes()])]).await;
    assert!(
        err.is_none(),
        "a forged frame carrying the wrong tag must not become an error: {err:?}"
    );
    assert_eq!(rows, vec![planted]);
}

/// **AC14** — the gate on arbitrary anchor position. A frame whose opening is
/// appended after result data **in one chunk** and which closes in the next.
///
/// Measured: fails before the fix and on the `starts_with(anchor)` design
/// (`rows=4` garbage rows, then a `Decode`), passes on the scan (`rows=1`,
/// `395`). `starts_with` carried the same shape as the defect being fixed — a
/// check that inspects one position.
///
/// **`a_frame_opening_straddling_a_chunk_boundary_is_reassembled` below is NOT
/// a substitute for this test.** The straddle shape passes even on
/// `starts_with`, because its second block ends with the closing marker and
/// `extract_exception_new` recovers the whole frame from that block alone.
/// It is coverage; this is the gate.
///
/// Client-parser gate: nothing measured shows ClickHouse emitting
/// `<data><frame opening>` in one chunk — in every capture the trailer arrived
/// as its own block (Lz4) or its own chunked piece (`Compression::None`). The
/// case is gated because the parser must not depend on that holding.
#[tokio::test]
async fn a_frame_opening_after_result_data_in_one_chunk_is_reassembled() {
    let frame = frame_bytes(REAL_MSG, TAG);
    let cut = 40;
    assert!(
        cut > EXC_OPEN.len() + TAG.len(),
        "the anchor is whole in the first block"
    );
    let mut first = data_block(&[b"row-1"]);
    assert!(
        !first.starts_with(EXC_OPEN),
        "the anchor must not be at offset 0"
    );
    first.extend_from_slice(&frame[..cut]);
    assert!(!first.ends_with(EXC_CLOSE));

    let (rows, err) = drive(Some(TAG), &[first, frame[cut..].to_vec()]).await;
    expect_395(err);
    assert_eq!(
        rows,
        vec!["row-1".to_string()],
        "rows that arrived ahead of the frame are delivered, not discarded"
    );
}

/// Coverage, not a gate — see
/// `a_frame_opening_after_result_data_in_one_chunk_is_reassembled`. The anchor
/// itself is cut across the boundary, so the straddle buffer is what has to
/// notice it.
#[tokio::test]
async fn a_frame_opening_straddling_a_chunk_boundary_is_reassembled() {
    let frame = frame_bytes(REAL_MSG, TAG);
    let cut = 9; // inside `\r\n__exception__\r\n`
    let mut first = data_block(&[b"row-1"]);
    first.extend_from_slice(&frame[..cut]);

    let (rows, err) = drive(Some(TAG), &[first, frame[cut..].to_vec()]).await;
    expect_395(err);
    assert_eq!(rows, vec!["row-1".to_string()]);
}

/// **AC15.** The response's exception tag must never reach a client-visible
/// message — assumption 2 of the anchor's soundness, turned into a check.
///
/// Scope, stated so it is not read as wider than it is: this covers a
/// well-formed frame, which is every frame a healthy server emits.
/// A **truncated** frame (`a_tagged_frame_that_never_terminates_…`) surfaces
/// the bytes it withheld, and those can include the head of the closing
/// trailer — i.e. part of that response's own tag. That is deliberate (the
/// alternative is dropping a real error) and is not a channel to a *future*
/// response's tag.
#[tokio::test]
async fn an_exception_tag_never_reaches_a_client_visible_message() {
    let (_rows, err) = drive(
        Some(TAG),
        &[data_block(&[b"row-1"]), frame_bytes(REAL_MSG, TAG)],
    )
    .await;
    let err = err.expect("the frame must produce an error");
    let rendered = err.to_string();
    let ChError::Server { code, message } = &err else {
        panic!("expected ChError::Server, got {err:?}")
    };
    assert_eq!(*code, 395, "{message:?}");
    assert!(
        !message.contains(TAG),
        "the tag must not reach ChError::Server.message: {message:?}"
    );
    assert!(
        !rendered.contains(TAG),
        "the tag must not reach the Display rendering either: {rendered:?}"
    );
}

/// The third withheld-bytes case: a frame that keeps growing without closing
/// is a **memory bound**, so it fails the stream and its buffer is dropped.
///
/// `EXC_FRAME_CAP` is 16 MiB — roughly 167x the largest exception body
/// measured on 26.3.17.110 (100,334 bytes from
/// `SELECT throwIf(1, repeat('x', 100000))`). It is not a claim about any size
/// ClickHouse guarantees, and this path was not reproducible against a real
/// server. `Error::Other` maps to `ChError::Config` (poison, never retried),
/// which is the right classification for a malformed stream.
#[tokio::test]
async fn a_tagged_frame_larger_than_the_memory_cap_fails_the_stream() {
    let frame = frame_bytes(REAL_MSG, TAG);
    let mut blocks = vec![data_block(&[b"row-1"]), frame[..40].to_vec()];
    // 17 MiB of filler after the anchor, in 1 MiB blocks, none of which closes
    // the frame.
    for _ in 0..17 {
        blocks.push(vec![b'x'; 1024 * 1024]);
    }
    let (rows, err) = drive(Some(TAG), &blocks).await;
    let err = err.expect("an unterminated frame past the cap must fail the stream");
    assert!(
        matches!(err, ChError::Config(_)),
        "Error::Other maps to ChError::Config (poison): {err:?}"
    );
    assert!(
        err.to_string().contains("without terminating"),
        "the message must say what happened: {err:?}"
    );
    assert_eq!(rows, vec!["row-1".to_string()]);
}
