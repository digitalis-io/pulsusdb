//! Every request that asks ClickHouse for a compressed reply also says which
//! codec to compress it with: `network_compression_method=lz4`.
//!
//! The client decodes only LZ4 replies. From ClickHouse 26.10 a `compress=1`
//! reply is compressed with the server's `network_compression_method`, which
//! defaults to ZSTD there, so the first reply — the pool's connect probe —
//! failed to decode with `incorrect magic number` and PulsusDB never became
//! ready. The live half of this check is
//! `a_user_whose_replies_default_to_zstd_still_connects_queries_and_inserts`
//! in `tests/live_clickhouse.rs`; this file pins the request shape without a
//! server.
//!
//! A raw-TCP mock records the request line of every request the production
//! wrapper (`ChClient`) sends — the connect probe, `ping`, `execute`, a
//! streamed read, and the column read an insert begins with — and the test
//! asserts on those request lines.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct OneCol {
    v: String,
}

/// One request the mock saw: its request target and its body.
#[derive(Debug, Clone)]
struct Seen {
    target: String,
    body: String,
}

impl Seen {
    /// The decoded-enough query-string pairs of the request target, in order.
    fn pairs(&self) -> Vec<(String, String)> {
        let query = self.target.split_once('?').map_or("", |(_, q)| q);
        query
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let (k, v) = p.split_once('=').unwrap_or((p, ""));
                (k.to_string(), v.to_string())
            })
            .collect()
    }

    fn param(&self, name: &str) -> Vec<String> {
        self.pairs()
            .into_iter()
            .filter(|(k, _)| k == name)
            .map(|(_, v)| v)
            .collect()
    }

    /// The statement, whether it came in the body (queries) or in the
    /// `query` parameter (inserts).
    fn statement(&self) -> String {
        let from_param = self.param("query").join(" ");
        format!("{} {}", self.body.trim(), from_param)
    }
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

/// A `RowBinaryWithNamesAndTypes` body for one `v String` column holding
/// one row, `x`, as one LZ4 block.
fn one_row_reply() -> Vec<u8> {
    let mut block = vec![1u8];
    block.extend_from_slice(&varint(1));
    block.extend_from_slice(b"v");
    block.extend_from_slice(&varint(6));
    block.extend_from_slice(b"String");
    block.extend_from_slice(&varint(1));
    block.extend_from_slice(b"x");
    clickhouse::_priv::lz4_compress(&block)
        .expect("lz4 compress the mock reply")
        .to_vec()
}

/// Answers the streamed read with one LZ4-framed row and every other request
/// with an empty 200, recording each request it sees. Binds an ephemeral
/// loopback port, so it declares no port any other suite must avoid.
struct MockCh {
    addr: std::net::SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockCh {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock");
        let addr = listener.local_addr().expect("local_addr");
        listener.set_nonblocking(true).expect("nonblocking");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (seen_thread, stop_thread) = (Arc::clone(&seen), Arc::clone(&stop));
        let handle = thread::spawn(move || {
            while !stop_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((sock, _)) => serve_one(sock, &seen_thread),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr,
            seen,
            stop,
            handle: Some(handle),
        }
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("mock record lock").clone()
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

fn serve_one(mut sock: TcpStream, seen: &Mutex<Vec<Seen>>) {
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
        .and_then(|line| line.split(' ').nth(1))
        .unwrap_or("")
        .to_string();
    let content_length: usize = head
        .to_ascii_lowercase()
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
    let body = String::from_utf8_lossy(&buf[head_end..]).to_string();
    let reply = if body.contains("FROM mock_rows") {
        one_row_reply()
    } else {
        Vec::new()
    };
    seen.lock()
        .expect("mock record lock")
        .push(Seen { target, body });

    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        reply.len()
    );
    let _ = sock.write_all(head.as_bytes());
    let _ = sock.write_all(&reply);
    let _ = sock.flush();
}

#[tokio::test]
async fn every_request_asking_for_a_compressed_reply_names_lz4() {
    let mock = MockCh::start();
    let client = ChClient::new(ChConnConfig {
        server: "127.0.0.1".to_string(),
        http_port: mock.addr.port(),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 1,
        query_timeout: Duration::from_secs(10),
        ..ChConnConfig::default()
    })
    .await
    .expect("connect to the mock");

    client.ping().await.expect("ping");
    client
        .execute(
            "CREATE TABLE mock_ddl (v String) ENGINE = Memory",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("execute");
    let mut stream = client
        .query_stream::<OneCol>("SELECT v FROM mock_rows", &QuerySettings::new())
        .await
        .expect("query_stream");
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.expect("row decode").v);
    }
    drop(stream);
    assert_eq!(
        rows,
        ["x"],
        "the streamed read decodes the mock's LZ4 reply"
    );
    // The mock gives the insert's column read an empty reply, so the insert
    // stops there; that read is the request under test.
    let _ = client
        .insert_block("mock_insert", &[OneCol { v: "y".to_string() }])
        .await;

    let seen = mock.seen();
    for statement in [
        "SELECT 1",
        "CREATE TABLE mock_ddl",
        "FROM mock_rows",
        "DESCRIBE TABLE",
    ] {
        assert!(
            seen.iter().any(|s| s.statement().contains(statement)),
            "the mock must have seen {statement:?}; it saw {seen:#?}"
        );
    }

    let compressed: Vec<&Seen> = seen
        .iter()
        .filter(|s| s.param("compress") == ["1"])
        .collect();
    assert!(
        compressed.len() >= 5,
        "the probe, ping, execute, streamed read and column read all ask for a compressed \
         reply; saw {seen:#?}"
    );
    for s in compressed {
        assert_eq!(
            s.param("network_compression_method"),
            ["lz4"],
            "a request asking for a compressed reply must name the one codec the client \
             decodes, exactly once: {s:#?}"
        );
    }
}
