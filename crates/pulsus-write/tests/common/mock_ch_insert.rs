//! A hermetic raw-TCP mock ClickHouse that answers the three requests one
//! `ChClient::insert_block_with` makes, and records the request line of each,
//! so a case can read what the **production** insert path actually put on the
//! wire (issue #603).
//!
//! The same shape as `pulsus-clickhouse`'s `tests/mock_clickhouse.rs`: no new
//! dependency, `std::net` + `std::thread` for the server and
//! `clickhouse::_priv::lz4_compress` (`#[doc(hidden)]`, semver-exempt because
//! the crate is vendored and pinned — `vendor/clickhouse/PATCHES.md` §2) for
//! the response framing the client's default compression expects.
//!
//! It answers, in the order the client asks:
//!
//! 1. the pool's `SELECT 1` probe, with an empty `200`, so `ChClient::new`
//!    succeeds;
//! 2. `DESCRIBE TABLE <t>`, with a one-column `RowBinaryWithNamesAndTypes`
//!    answer, because the vendored client fetches insert metadata before it
//!    opens the insert request (validation is on by default);
//! 3. the `INSERT`, with whatever the case scripted — an empty `200`, or a
//!    `500` carrying an exception code in the header the vendored patch puts
//!    at byte 0 of the body.
//!
//! **What it establishes and what it does not.** It establishes what our own
//! insert path sends and how our own classifier reads the answer. It does not
//! establish that ClickHouse answers this way — protocol behaviour is gated
//! only by the live suites.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use pulsus_clickhouse::{ChConnConfig, ChProto, Row};

/// The one-column row type the mock describes and the case inserts. Its
/// column name and type are what `DESCRIBE`'s canned answer declares, so the
/// client's own schema check passes.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct OneCol {
    pub v: u64,
}

/// What the mock answers the `INSERT` request with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertAnswer {
    /// An empty `200`: the insert committed.
    Ok,
    /// A `500` whose body starts with the exception code the vendored client
    /// reads out of `X-ClickHouse-Exception-Code`, then the exception text.
    /// This is an answer that arrives **after** the block was transmitted.
    Exception { code: i32, text: &'static str },
}

/// What the mock answers `DESCRIBE TABLE` with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DescribeAnswer {
    /// The canned one-column description.
    Ok,
    /// A `500` exception, which reaches the caller **before** any of the
    /// block is transmitted.
    Exception { code: i32, text: &'static str },
}

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

fn rb_str(s: &str) -> Vec<u8> {
    let mut out = varint(s.len());
    out.extend_from_slice(s.as_bytes());
    out
}

/// `DESCRIBE TABLE`'s answer for a single `v UInt64` column, in the
/// `RowBinaryWithNamesAndTypes` shape the client requests: the column count,
/// then each column's name and type, then one row per described column with
/// the seven `String` fields `DESCRIBE` returns.
fn describe_block() -> Vec<u8> {
    const FIELDS: [&str; 7] = [
        "name",
        "type",
        "default_type",
        "default_expression",
        "comment",
        "codec_expression",
        "ttl_expression",
    ];
    let mut out = vec![FIELDS.len() as u8];
    for f in FIELDS {
        out.extend_from_slice(&rb_str(f));
    }
    for _ in FIELDS {
        out.extend_from_slice(&rb_str("String"));
    }
    for cell in ["v", "UInt64", "", "", "", "", ""] {
        out.extend_from_slice(&rb_str(cell));
    }
    out
}

/// One request the mock served: its request target (path and query string)
/// and its body.
#[derive(Clone, Debug)]
pub struct Seen {
    pub target: String,
    pub body: String,
}

impl Seen {
    /// The value of one HTTP query parameter, percent-decoding only the
    /// escapes the settings values here actually use.
    pub fn param(&self, key: &str) -> Option<String> {
        let query = self.target.split_once('?').map(|(_, q)| q)?;
        query.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == key).then(|| v.replace('+', " ").replace("%20", " "))
        })
    }
}

/// The mock server. Drop stops it.
pub struct MockChInsert {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    seen: Arc<Mutex<Vec<Seen>>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockChInsert {
    pub fn start(describe: DescribeAnswer, insert: InsertAnswer) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the mock");
        let addr = listener.local_addr().expect("local_addr");
        listener.set_nonblocking(true).expect("nonblocking");
        let stop = Arc::new(AtomicBool::new(false));
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::new(Mutex::new(Vec::new()));
        let stop_thread = Arc::clone(&stop);
        let seen_thread = Arc::clone(&seen);
        let handle = thread::spawn(move || {
            while !stop_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((sock, _)) => serve_one(sock, describe, insert, &seen_thread),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        MockChInsert {
            addr,
            stop,
            seen,
            handle: Some(handle),
        }
    }

    /// A connection config pointing at this mock, with one pooled connection
    /// so the request order is the client's own.
    pub fn conn_config(&self) -> ChConnConfig {
        ChConnConfig {
            server: "127.0.0.1".to_string(),
            http_port: self.addr.port(),
            database: "default".to_string(),
            proto: ChProto::Http,
            pool_size: 1,
            query_timeout: Duration::from_secs(10),
            ..ChConnConfig::default()
        }
    }

    /// Every request served so far, in order.
    pub fn requests(&self) -> Vec<Seen> {
        self.seen.lock().expect("mock mutex poisoned").clone()
    }

    /// The request whose body is the `INSERT` statement, which is the one
    /// carrying the per-insert settings.
    pub fn insert_request(&self) -> Seen {
        self.requests()
            .into_iter()
            .find(|r| r.body.starts_with("INSERT INTO"))
            .expect("the mock served no INSERT request")
    }
}

impl Drop for MockChInsert {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn exception_body(code: i32, text: &str) -> Vec<u8> {
    // The vendored client reads the code out of the response header and puts
    // it at byte 0 of the body it hands the classifier
    // (`vendor/clickhouse/PATCHES.md`), so the mock writes the header and the
    // text and lets the client assemble the two.
    format!("Code: {code}. DB::Exception: {text}. (SOME_CODE)\n").into_bytes()
}

fn serve_one(
    mut sock: TcpStream,
    describe: DescribeAnswer,
    insert: InsertAnswer,
    seen: &Arc<Mutex<Vec<Seen>>>,
) {
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
    sock.set_nonblocking(false).ok();

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
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let target = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("")
        .to_string();
    let lower = head.to_ascii_lowercase();
    let content_length: usize = lower
        .split("\r\n")
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    let chunked = lower.contains("transfer-encoding: chunked");
    if chunked {
        // Read until the terminating zero-length chunk: an insert body is
        // streamed, so there is no `Content-Length` to count against.
        while !buf[head_end..].ends_with(b"0\r\n\r\n") {
            match sock.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                Err(_) => break,
            }
        }
    } else {
        while buf.len() < head_end + content_length {
            match sock.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                Err(_) => break,
            }
        }
    }
    let body = String::from_utf8_lossy(&buf[head_end..]).to_string();
    // The insert's statement arrives in the query string, not the body, when
    // the client streams the block; read whichever of the two carries it.
    let statement = if body.trim_start().starts_with("INSERT INTO") {
        body.trim().to_string()
    } else if target.contains("INSERT+INTO") || target.contains("INSERT%20INTO") {
        "INSERT INTO <in the query string>".to_string()
    } else {
        body.trim().to_string()
    };
    seen.lock().expect("mock mutex poisoned").push(Seen {
        target: target.clone(),
        body: statement.clone(),
    });

    if statement == "SELECT 1" {
        let _ =
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        let _ = sock.flush();
        return;
    }

    if statement.starts_with("DESCRIBE TABLE") {
        match describe {
            DescribeAnswer::Ok => {
                let payload = clickhouse::_priv::lz4_compress(&describe_block())
                    .expect("lz4 compress the describe block");
                let mut resp = String::from("HTTP/1.1 200 OK\r\n");
                resp.push_str(&format!("Content-Length: {}\r\n", payload.len()));
                resp.push_str("Connection: close\r\n\r\n");
                let _ = sock.write_all(resp.as_bytes());
                let _ = sock.write_all(&payload);
            }
            DescribeAnswer::Exception { code, text } => write_exception(&mut sock, code, text),
        }
        let _ = sock.flush();
        return;
    }

    match insert {
        InsertAnswer::Ok => {
            let _ = sock
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        }
        InsertAnswer::Exception { code, text } => write_exception(&mut sock, code, text),
    }
    let _ = sock.flush();
}

fn write_exception(sock: &mut TcpStream, code: i32, text: &str) {
    let payload = exception_body(code, text);
    let mut resp = String::from("HTTP/1.1 500 Internal Server Error\r\n");
    resp.push_str(&format!("X-ClickHouse-Exception-Code: {code}\r\n"));
    resp.push_str(&format!("Content-Length: {}\r\n", payload.len()));
    resp.push_str("Connection: close\r\n\r\n");
    let _ = sock.write_all(resp.as_bytes());
    let _ = sock.write_all(&payload);
}
