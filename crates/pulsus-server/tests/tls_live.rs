//! Live inbound-TLS end-to-end suite (issue #174): spawns the real
//! `pulsusdb` binary with `PULSUS_TLS_CERT`/`PULSUS_TLS_KEY` set and
//! proves the TLS listener over loopback — `/ready` reaches 200 over TLS
//! (the full stack, schema reconcile included, served through the
//! `TlsListener`), a *plaintext* GET on the same port gets no valid HTTP
//! response (rustls rejects the non-ClientHello and the connection is
//! closed), and `/config` over TLS shows both path fields (redacted dump
//! intact — the paths are not secrets).
//!
//! Certificate material is rcgen-generated at run time and written to the
//! OS temp dir (pid+thread-unique names) — no key is ever committed to
//! this public repo, and nothing can expire under CI's clock.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` so plain `cargo test` stays
//! hermetic (and TLS-free — KISS rule); shares the podman/docker setup
//! documented in `tests/live_server.rs`. The TLS client is sync
//! `rustls::StreamOwned` over `std::net::TcpStream` with a root store
//! holding the generated cert — no HTTP-client or async dependency, the
//! same bare-GET idiom `live_server.rs` uses, just wrapped in TLS.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn should_run() -> bool {
    std::env::var("PULSUS_TEST_CLICKHOUSE").as_deref() == Ok("1")
}

/// Kills the spawned `pulsusdb` process on drop (including on test
/// panic), so a failing assertion never leaks a background server.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Writes `contents` to a uniquely-named file under the OS temp directory
/// (the pulsus-config `write_temp_yaml` idiom — pid+thread unique, no
/// tempfile dependency) and returns its path.
fn write_temp_pem(name: &str, contents: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "pulsus-tls-live-{name}-{}-{:?}.pem",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::write(&path, contents).expect("write temp pem");
    path
}

/// A rustls client config trusting exactly the one generated server cert,
/// built with the explicit `ring` provider (`builder_with_provider` — the
/// process-default resolver panics with two provider features compiled
/// into the graph, same as the server side).
fn client_config(
    server_cert: &rustls_pki_types::CertificateDer<'static>,
) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(server_cert.clone())
        .expect("add generated cert to root store");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("safe default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(config)
}

/// Issues a bare HTTP/1.1 GET *over TLS* and returns `(status, body)`.
/// `None` on any connection/handshake failure (e.g. the server has not
/// bound yet). Reads leniently: a peer closing without close_notify after
/// `Connection: close` must not discard an already-received response.
fn tls_get(port: u16, config: &Arc<rustls::ClientConfig>, path: &str) -> Option<(u16, String)> {
    let sock = TcpStream::connect(("127.0.0.1", port)).ok()?;
    sock.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let server_name = rustls_pki_types::ServerName::try_from("localhost").ok()?;
    let conn = rustls::ClientConnection::new(Arc::clone(config), server_name).ok()?;
    let mut tls = rustls::StreamOwned::new(conn, sock);
    write!(
        tls,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match tls.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    let text = String::from_utf8_lossy(&buf).into_owned();
    let mut parts = text.splitn(2, "\r\n\r\n");
    let head = parts.next()?;
    let body = parts.next().unwrap_or("").to_string();
    let status = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((status, body))
}

#[test]
fn tls_listener_serves_ready_and_config_and_rejects_plaintext() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see tests/live_server.rs for setup)"
        );
        return;
    }

    // Fixed loopback port, distinct from every other live suite's pin
    // (they occupy 31100-31154).
    let port: u16 = 31_160;

    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("rcgen self-signed generation");
    let cert_path = write_temp_pem("cert", &certified.cert.pem());
    let key_path = write_temp_pem("key", &certified.key_pair.serialize_pem());
    let client = client_config(certified.cert.der());

    let child = Command::new(env!("CARGO_BIN_EXE_pulsusdb"))
        .env("PULSUS_HOST", "127.0.0.1")
        .env("PULSUS_PORT", port.to_string())
        .env("PULSUS_TLS_CERT", &cert_path)
        .env("PULSUS_TLS_KEY", &key_path)
        .env(
            "CLICKHOUSE_SERVER",
            std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        )
        .env(
            "CLICKHOUSE_HTTP_PORT",
            std::env::var("PULSUS_TEST_CH_HTTP_PORT").unwrap_or_else(|_| "19123".to_string()),
        )
        .env("CLICKHOUSE_DB", "pulsus_tls_live_test")
        .spawn()
        .expect("spawn pulsusdb");
    let _guard = ChildGuard(child);

    // 1. /ready reaches 200 over TLS within the same 60s cold-start
    //    deadline live_server.rs uses (schema reconcile included) — the
    //    whole readiness transition is observed through the TLS listener.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut became_ready = false;
    while Instant::now() < deadline {
        if let Some((status, _)) = tls_get(port, &client, "/ready") {
            match status {
                503 => {}
                200 => {
                    became_ready = true;
                    break;
                }
                other => panic!("unexpected /ready status {other}"),
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(became_ready, "/ready never reached 200 over TLS within 60s");

    // 2. A plaintext GET on the TLS port gets no valid HTTP response:
    //    rustls rejects the bytes as a malformed ClientHello and the
    //    server closes the connection (at most a TLS alert record comes
    //    back — never an HTTP status line).
    let mut plain = TcpStream::connect(("127.0.0.1", port)).expect("plaintext connect");
    plain
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    write!(
        plain,
        "GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("plaintext write");
    let mut response = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        match plain.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => response.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    assert!(
        !response.starts_with(b"HTTP/"),
        "a plaintext request on the TLS port must never get an HTTP response, got {:?}",
        String::from_utf8_lossy(&response)
    );

    // 3. /config over TLS shows both path fields (they are plain paths,
    //    not secrets — the redacted dump carries them verbatim).
    let (status, body) = tls_get(port, &client, "/config").expect("/config over TLS reachable");
    assert_eq!(status, 200);
    let cert_str = cert_path.to_str().expect("utf-8 temp path");
    let key_str = key_path.to_str().expect("utf-8 temp path");
    assert!(
        body.contains(cert_str),
        "/config must show tls_cert ({cert_str}) in: {body}"
    );
    assert!(
        body.contains(key_str),
        "/config must show tls_key ({key_str}) in: {body}"
    );

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}
