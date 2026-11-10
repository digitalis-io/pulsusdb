//! A stub ClickHouse HTTP server for read-failure tests (issue #803).
//!
//! A std-thread server on an ephemeral loopback port (`127.0.0.1:0`, so it
//! declares nothing in the reserved fixed-port range and two can run at
//! once). It reads request after request on each connection. A request
//! whose query text is exactly `SELECT 1` gets `200` and `1\n`, so the
//! pool's connect ping passes; every other request is answered by the
//! stub's [`Mode`], and its query text is recorded in arrival order.
//!
//! Success bodies are framed as `mock_clickhouse.rs` frames its blocks: one
//! LZ4 block (`clickhouse::_priv::lz4_compress`) holding a
//! `RowBinaryWithNamesAndTypes` result.
//!
//! Included by `#[path]` from `pulsus-clickhouse`, `pulsus-read` and
//! `pulsus-server` tests.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// One scripted answer.
#[derive(Clone, Debug)]
pub enum Reply {
    /// `500` with `X-ClickHouse-Exception-Code: n` and the body
    /// `Code: n. DB::Exception: stub`.
    Code(i32),
    /// A `200` result with this header, as `(name, ClickHouse type)` pairs
    /// copied from the fields of the row struct the statement decodes, and
    /// no rows.
    Empty(Vec<(&'static str, &'static str)>),
    /// A `200` result with one `String` column of this name and these rows.
    Strings(&'static str, Vec<String>),
}

/// How a stub answers every request other than `SELECT 1`.
#[derive(Clone, Debug)]
pub enum Mode {
    /// Holds the socket and never answers.
    Silent,
    /// [`Reply::Code`] to every request.
    Code(i32),
    /// One reply per request, in arrival order; a request past the end
    /// gets `Code(60)`.
    Script(Vec<Reply>),
    /// `reply` to a request whose query text starts with `prefix`,
    /// `otherwise` to every other.
    Route {
        prefix: &'static str,
        reply: Reply,
        otherwise: Reply,
    },
}

/// What the serving threads share.
struct Shared {
    mode: Mode,
    script: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<String>>,
    stop: AtomicBool,
}

/// A running stub. Dropping it stops the accept loop and every connection.
pub struct StubServer {
    pub addr: SocketAddr,
    shared: Arc<Shared>,
    handle: Option<thread::JoinHandle<()>>,
}

impl StubServer {
    pub fn start(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the stub");
        let addr = listener.local_addr().expect("local_addr");
        listener.set_nonblocking(true).expect("nonblocking");
        let script = match &mode {
            Mode::Script(replies) => replies.iter().cloned().collect(),
            _ => VecDeque::new(),
        };
        let shared = Arc::new(Shared {
            mode,
            script: Mutex::new(script),
            requests: Mutex::new(Vec::new()),
            stop: AtomicBool::new(false),
        });
        let accept_shared = Arc::clone(&shared);
        let handle = thread::spawn(move || {
            while !accept_shared.stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((sock, _)) => {
                        let conn_shared = Arc::clone(&accept_shared);
                        thread::spawn(move || serve_connection(sock, &conn_shared));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr,
            shared,
            handle: Some(handle),
        }
    }

    /// The query text of every request other than `SELECT 1`, in arrival
    /// order.
    pub fn requests(&self) -> Vec<String> {
        lock(&self.shared.requests).clone()
    }

    /// How many requests other than `SELECT 1` arrived.
    pub fn answered(&self) -> usize {
        lock(&self.shared.requests).len()
    }
}

impl Drop for StubServer {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// One parsed request: its query text, from the body or else the `query`
/// URL parameter.
fn read_request(sock: &mut TcpStream, shared: &Shared, pending: &mut Vec<u8>) -> Option<String> {
    let mut tmp = [0u8; 4096];
    let head_end = loop {
        if let Some(i) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        match sock.read(&mut tmp) {
            Ok(0) => return None,
            Ok(n) => pending.extend_from_slice(&tmp[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                if shared.stop.load(Ordering::Relaxed) {
                    return None;
                }
            }
            Err(_) => return None,
        }
    };
    let head = String::from_utf8_lossy(&pending[..head_end]).to_string();
    let content_length: usize = head
        .split("\r\n")
        .find_map(|line| {
            let lower = line.to_ascii_lowercase();
            lower
                .strip_prefix("content-length:")
                .map(|v| v.trim().to_string())
        })
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    while pending.len() < head_end + content_length {
        match sock.read(&mut tmp) {
            Ok(0) => return None,
            Ok(n) => pending.extend_from_slice(&tmp[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                if shared.stop.load(Ordering::Relaxed) {
                    return None;
                }
            }
            Err(_) => return None,
        }
    }
    let body = String::from_utf8_lossy(&pending[head_end..head_end + content_length]).to_string();
    pending.drain(..head_end + content_length);
    if !body.trim().is_empty() {
        return Some(body.trim().to_string());
    }
    let target = head.split_whitespace().nth(1).unwrap_or("");
    let query = target
        .split_once('?')
        .map(|(_, q)| q)
        .unwrap_or("")
        .split('&')
        .find_map(|kv| kv.strip_prefix("query="))
        .map(percent_decode)
        .unwrap_or_default();
    Some(query.trim().to_string())
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn serve_connection(mut sock: TcpStream, shared: &Shared) {
    sock.set_nonblocking(false).ok();
    sock.set_read_timeout(Some(Duration::from_millis(100))).ok();
    let mut pending = Vec::new();
    while let Some(query) = read_request(&mut sock, shared, &mut pending) {
        if query == "SELECT 1" {
            if write_result(&mut sock, b"1\n").is_err() {
                return;
            }
            continue;
        }
        lock(&shared.requests).push(query.clone());
        let reply = match &shared.mode {
            Mode::Silent => {
                while !shared.stop.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(20));
                }
                return;
            }
            Mode::Code(n) => Reply::Code(*n),
            Mode::Script(_) => lock(&shared.script).pop_front().unwrap_or(Reply::Code(60)),
            Mode::Route {
                prefix,
                reply,
                otherwise,
            } => {
                if query.starts_with(prefix) {
                    reply.clone()
                } else {
                    otherwise.clone()
                }
            }
        };
        let sent = match reply {
            Reply::Code(n) => write_code(&mut sock, n),
            Reply::Empty(columns) => write_result(&mut sock, &header(&columns)),
            Reply::Strings(column, values) => {
                let mut body = header(&[(column, "String")]);
                for v in &values {
                    body.extend_from_slice(&varint(v.len()));
                    body.extend_from_slice(v.as_bytes());
                }
                write_result(&mut sock, &body)
            }
        };
        if sent.is_err() {
            return;
        }
    }
}

/// LEB128, which is what RowBinary uses for lengths.
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

/// The `RowBinaryWithNamesAndTypes` prelude: the column count, every
/// name, then every type.
fn header(columns: &[(&str, &str)]) -> Vec<u8> {
    let mut out = varint(columns.len());
    for (name, _) in columns {
        out.extend_from_slice(&varint(name.len()));
        out.extend_from_slice(name.as_bytes());
    }
    for (_, ty) in columns {
        out.extend_from_slice(&varint(ty.len()));
        out.extend_from_slice(ty.as_bytes());
    }
    out
}

fn write_result(sock: &mut TcpStream, block: &[u8]) -> std::io::Result<()> {
    let body = clickhouse::_priv::lz4_compress(block).expect("lz4 compress a stub block");
    let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
    sock.write_all(head.as_bytes())?;
    sock.write_all(&body)?;
    sock.flush()
}

fn write_code(sock: &mut TcpStream, code: i32) -> std::io::Result<()> {
    let body = format!("Code: {code}. DB::Exception: stub");
    let head = format!(
        "HTTP/1.1 500 Internal Server Error\r\nX-ClickHouse-Exception-Code: {code}\r\n\
         Content-Length: {}\r\n\r\n",
        body.len()
    );
    sock.write_all(head.as_bytes())?;
    sock.write_all(body.as_bytes())?;
    sock.flush()
}
