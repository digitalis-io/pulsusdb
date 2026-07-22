//! Inbound TLS termination for the one HTTP listener (issue #174).
//!
//! Two halves, both consumed by `serve::run`:
//!
//! - [`load_server_config`]: PEM cert/key files → `rustls::ServerConfig`,
//!   called *before* the bind so a bad `PULSUS_TLS_CERT`/`PULSUS_TLS_KEY`
//!   is a clean pre-listen startup failure (the TOCTOU-safe net —
//!   `pulsus-config::validate` only enforces the I/O-free pairing rule).
//! - [`TlsListener`]: wraps the already-bound `TcpListener` behind axum
//!   0.8's `serve::Listener` trait, so the `axum::serve(...)
//!   .with_graceful_shutdown(...)` ordering contract in `serve::run` is
//!   untouched and the plaintext branch keeps passing the raw
//!   `TcpListener` unchanged.
//!
//! Handshakes run in per-connection spawned tasks feeding a bounded
//! channel, so a stalled or plaintext peer can never head-of-line-block
//! the accept loop. Boundedness invariant (proven by the saturation test
//! below): sockets held by this layer never exceed
//! [`MAX_INFLIGHT_HANDSHAKES`] (permit-gated handshake tasks) +
//! [`ACCEPT_QUEUE_DEPTH`] (completed streams awaiting axum) + 1 (the one
//! connection mid-accept); everything past the permit gate is closed
//! immediately, never queued or spawned.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

/// Failures loading the operator-supplied PEM certificate/key pair. All
/// variants surface as a pre-bind startup failure in `serve::run` (via
/// `ServeError::Tls`), never a half-started server.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TlsError {
    #[error("failed to read {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("no certificate found in {path} (expected at least one PEM CERTIFICATE block)")]
    NoCert { path: String },
    #[error("no private key found in {path} (expected a PKCS#8, PKCS#1, or SEC1 PEM block)")]
    NoKey { path: String },
    #[error("invalid TLS certificate/key pair: {0}")]
    Rustls(#[from] rustls::Error),
}

/// PEM files → rustls `ServerConfig`. The `ring` provider is bound
/// explicitly via `builder_with_provider` (the
/// `pulsus-clickhouse/src/tls.rs` precedent): both `ring` and `aws-lc-rs`
/// are compiled into this workspace's dependency graph, so the
/// process-default-resolving `ServerConfig::builder()` would panic on
/// provider ambiguity. Protocol versions are rustls' safe defaults
/// (TLS 1.2 + 1.3, no knobs — issue #174 open-question resolution 2).
/// ALPN pins `http/1.1` only: axum's `http2` feature is not compiled into
/// this binary, so advertising `h2` would negotiate a protocol the server
/// cannot speak (resolution 3).
pub(crate) fn load_server_config(
    cert_path: &str,
    key_path: &str,
) -> Result<Arc<rustls::ServerConfig>, TlsError> {
    let cert_bytes = std::fs::read(cert_path).map_err(|source| TlsError::Io {
        path: cert_path.to_string(),
        source,
    })?;
    let certs = rustls_pemfile::certs(&mut cert_bytes.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsError::Io {
            path: cert_path.to_string(),
            source,
        })?;
    if certs.is_empty() {
        return Err(TlsError::NoCert {
            path: cert_path.to_string(),
        });
    }

    let key_bytes = std::fs::read(key_path).map_err(|source| TlsError::Io {
        path: key_path.to_string(),
        source,
    })?;
    let key = rustls_pemfile::private_key(&mut key_bytes.as_slice())
        .map_err(|source| TlsError::Io {
            path: key_path.to_string(),
            source,
        })?
        .ok_or_else(|| TlsError::NoKey {
            path: key_path.to_string(),
        })?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        // Also verifies the key actually matches the certificate, so a
        // mixed-up pair fails here at startup, not at first handshake.
        .with_single_cert(certs, key)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// Bound on one connection's TLS handshake; a stalled peer (e.g. a
/// plaintext client that never sends a ClientHello) is dropped when it
/// elapses, releasing that connection's admission permit.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Completed-handshake queue depth between the accept task and axum's
/// serve loop (a delivery backpressure bound, not a connection limit —
/// admission is bounded by [`MAX_INFLIGHT_HANDSHAKES`]).
const ACCEPT_QUEUE_DEPTH: usize = 64;

/// Cap on concurrent in-flight handshake tasks — each holds one fd plus a
/// few KB of rustls buffers, so the worst case is ~256 fds + a few MB.
/// Excess accepted sockets are closed immediately, never queued or
/// spawned. A documented constant, not a `PULSUS_*` var (the
/// `WRITER_DRAIN_DEADLINE` precedent in `serve.rs`: promote to config
/// only when a deployment needs to tune it); 256 = parity with
/// `query_eval_concurrency`'s default.
const MAX_INFLIGHT_HANDSHAKES: usize = 256;

/// Injectable bounds so the saturation/error-path tests run with tiny,
/// fast values; [`TlsListener::new`] wires the production constants.
#[derive(Debug, Clone, Copy)]
struct HandshakeLimits {
    /// Production: [`HANDSHAKE_TIMEOUT`].
    handshake_timeout: Duration,
    /// Production: [`MAX_INFLIGHT_HANDSHAKES`] — must equal the permit
    /// count of the semaphore passed alongside (carried here so rejection
    /// logs can name the limit without re-deriving it).
    max_inflight: usize,
}

/// Mirrors axum 0.8's own `Listener for TcpListener` error taxonomy:
/// connection-class errors are retried immediately and silently.
fn is_connection_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    )
}

/// The accept loop, factored as a free fn generic over the accept effect
/// and IO type (the `rotation_tick` testability idiom in `serve.rs`) so
/// the error-injection test proves recovery without a real socket error.
/// Production instantiates it with `TcpListener::accept`.
///
/// Per accepted connection: `try_acquire_owned` on `semaphore` — no
/// permit ⇒ the socket is dropped (closed) on the spot (counter + debug
/// log), never spawned or queued; with a permit, the handshake runs in
/// its own task (bounded by `limits.handshake_timeout`) and the permit is
/// held across the handshake AND the channel send, so its drop — success,
/// failure, or timeout — is the release point.
///
/// Accept errors replicate axum 0.8's `Listener` semantics exactly:
/// connection-class errors ([`is_connection_error`]) retry immediately
/// and silently; anything else (e.g. EMFILE) logs at `error` and sleeps
/// 1s before retrying. The loop never exits on error, so the delivery
/// channel only closes when the spawning [`TlsListener`] is dropped
/// (i.e. after `axum::serve` has already returned).
///
/// Handshake tasks are spawned into a `JoinSet` owned by this future —
/// NOT detached — so when the accept task is aborted ([`TlsListener`]'s
/// `Drop`), the set drops with it and immediately aborts every
/// still-running handshake: a stalled peer's socket closes promptly at
/// listener shutdown instead of lingering until the handshake timeout
/// (code-review finding on issue #174). Aborting a handshake task drops
/// its future, which owns both the socket and its admission permit, so
/// the semaphore accounting survives cancellation too.
async fn accept_loop<S, A, F>(
    mut accept: A,
    tls: Arc<rustls::ServerConfig>,
    semaphore: Arc<Semaphore>,
    tx: mpsc::Sender<(TlsStream<S>, SocketAddr)>,
    limits: HandshakeLimits,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    A: FnMut() -> F,
    F: Future<Output = std::io::Result<(S, SocketAddr)>>,
{
    let acceptor = TlsAcceptor::from(tls);
    let mut handshakes = tokio::task::JoinSet::new();
    loop {
        // Reap already-finished handshake tasks (non-blocking) so the set
        // only ever holds the permit-bounded in-flight entries plus an
        // equally bounded backlog of finished results awaiting this sweep
        // — never unbounded growth.
        while handshakes.try_join_next().is_some() {}

        let (stream, peer) = match accept().await {
            Ok(conn) => conn,
            Err(e) if is_connection_error(&e) => continue,
            Err(e) => {
                // EMFILE-class: waiting may free fds (axum's own comment
                // trail, via hyper 0.14) — big enough a deal for `error!`,
                // but never fatal to the listener.
                tracing::error!(error = %e, "accept error");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let permit = match Arc::clone(&semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                // Admission bound hit: close the socket immediately —
                // spawning or queueing here is exactly the unbounded
                // fd/task accumulation the bound exists to prevent.
                metrics::counter!("pulsus_tls_handshakes_rejected_total").increment(1);
                tracing::debug!(
                    %peer,
                    limit = limits.max_inflight,
                    "tls handshake admission limit reached; closing connection"
                );
                drop(stream);
                continue;
            }
        };

        let acceptor = acceptor.clone();
        let tx = tx.clone();
        handshakes.spawn(async move {
            // Held across handshake AND send: the permit only releases
            // once this connection is either delivered to axum or gone.
            let _permit = permit;
            match tokio::time::timeout(limits.handshake_timeout, acceptor.accept(stream)).await {
                Ok(Ok(tls_stream)) => {
                    // A send error means the listener (and axum's serve
                    // loop) is gone — nothing left to hand the stream to.
                    let _ = tx.send((tls_stream, peer)).await;
                }
                Ok(Err(err)) => {
                    // `debug`, not `warn`: a port scanner or plaintext
                    // probe must not be a log-spam vector.
                    metrics::counter!("pulsus_tls_handshake_failures_total").increment(1);
                    tracing::debug!(%peer, error = %err, "tls handshake failed");
                }
                Err(_elapsed) => {
                    metrics::counter!("pulsus_tls_handshake_failures_total").increment(1);
                    tracing::debug!(
                        %peer,
                        timeout_ms = limits.handshake_timeout.as_millis() as u64,
                        "tls handshake timed out"
                    );
                }
            }
        });
    }
}

/// Wraps the already-bound `TcpListener`: one spawned accept-loop task
/// ([`accept_loop`] with the production constants) accepts TCP and runs
/// permit-bounded per-connection handshake tasks; completed streams flow
/// through a bounded mpsc into axum's serve loop. Dropping this aborts
/// the accept task — which fires exactly when `axum::serve` returns,
/// i.e. after the graceful drain; aborting it drops the loop's `JoinSet`,
/// which in turn promptly aborts every in-flight handshake task, so
/// connections still mid-handshake at that point are closed immediately
/// (no request exists on them yet), never held open until the handshake
/// timeout.
#[derive(Debug)]
pub(crate) struct TlsListener {
    /// Captured from the pre-wrap `TcpListener` (the bound never fails
    /// after a successful bind; kept as a `Result` only because
    /// `local_addr()` is fallible on both sides of the trait).
    local_addr: std::io::Result<SocketAddr>,
    rx: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    accept_task: JoinHandle<()>,
}

impl TlsListener {
    pub(crate) fn new(inner: TcpListener, config: Arc<rustls::ServerConfig>) -> Self {
        let local_addr = inner.local_addr();
        let (tx, rx) = mpsc::channel(ACCEPT_QUEUE_DEPTH);
        let semaphore = Arc::new(Semaphore::new(MAX_INFLIGHT_HANDSHAKES));
        let limits = HandshakeLimits {
            handshake_timeout: HANDSHAKE_TIMEOUT,
            max_inflight: MAX_INFLIGHT_HANDSHAKES,
        };
        // `Arc` so the accept closure can hand out an owned future per
        // call (a plain `FnMut` cannot lend a borrow of its capture).
        let inner = Arc::new(inner);
        let accept = move || {
            let inner = Arc::clone(&inner);
            async move { inner.accept().await }
        };
        let accept_task = tokio::spawn(accept_loop(accept, config, semaphore, tx, limits));
        TlsListener {
            local_addr,
            rx,
            accept_task,
        }
    }
}

impl Drop for TlsListener {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(conn) => conn,
            // A closed channel means the accept task is gone, which only
            // `Drop` causes — unreachable while `axum::serve` is still
            // polling this listener. Park rather than panic.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        match &self.local_addr {
            Ok(addr) => Ok(*addr),
            Err(e) => Err(std::io::Error::new(e.kind(), e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::ErrorKind;
    use std::sync::Mutex;

    use tokio::io::AsyncReadExt;

    /// rcgen-generated material, unique per test invocation — no key is
    /// ever committed to this (public) repo, and nothing can expire.
    fn self_signed() -> rcgen::CertifiedKey {
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("rcgen self-signed generation")
    }

    /// Writes `contents` to a uniquely-named file under the OS temp dir
    /// (pid + thread unique, the pulsus-config `write_temp_yaml` idiom).
    fn write_temp_pem(name: &str, contents: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "pulsus-tls-test-{name}-{}-{:?}.pem",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, contents).expect("write temp pem fixture");
        path
    }

    /// In-memory `ServerConfig` for the listener tests — no files, no
    /// loader involvement.
    fn in_memory_server_config() -> Arc<rustls::ServerConfig> {
        let ck = self_signed();
        let cert = ck.cert.der().clone();
        let key = rustls_pki_types::PrivateKeyDer::Pkcs8(ck.key_pair.serialize_der().into());
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("safe default protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .expect("rcgen pair is consistent");
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Arc::new(config)
    }

    #[test]
    fn valid_pair_loads_with_http11_only_alpn() {
        let ck = self_signed();
        let cert_path = write_temp_pem("valid-cert", &ck.cert.pem());
        let key_path = write_temp_pem("valid-key", &ck.key_pair.serialize_pem());

        let config = load_server_config(
            cert_path.to_str().expect("utf-8 temp path"),
            key_path.to_str().expect("utf-8 temp path"),
        )
        .expect("a freshly generated pair must load");
        assert_eq!(
            config.alpn_protocols,
            vec![b"http/1.1".to_vec()],
            "ALPN must pin http/1.1 only — axum's http2 feature is not compiled"
        );

        let _ = std::fs::remove_file(cert_path);
        let _ = std::fs::remove_file(key_path);
    }

    #[test]
    fn nonexistent_cert_path_is_an_io_error_naming_the_path() {
        let ck = self_signed();
        let key_path = write_temp_pem("io-key", &ck.key_pair.serialize_pem());

        let err = load_server_config("/nonexistent/pulsus/server.crt", key_path.to_str().unwrap())
            .expect_err("a missing cert file must fail");
        assert!(
            matches!(&err, TlsError::Io { path, .. } if path == "/nonexistent/pulsus/server.crt"),
            "got {err:?}"
        );

        let _ = std::fs::remove_file(key_path);
    }

    #[test]
    fn nonexistent_key_path_is_an_io_error_naming_the_path() {
        let ck = self_signed();
        let cert_path = write_temp_pem("io-cert", &ck.cert.pem());

        let err = load_server_config(
            cert_path.to_str().unwrap(),
            "/nonexistent/pulsus/server.key",
        )
        .expect_err("a missing key file must fail");
        assert!(
            matches!(&err, TlsError::Io { path, .. } if path == "/nonexistent/pulsus/server.key"),
            "got {err:?}"
        );

        let _ = std::fs::remove_file(cert_path);
    }

    #[test]
    fn directory_as_cert_path_is_an_io_error() {
        let ck = self_signed();
        let key_path = write_temp_pem("dir-key", &ck.key_pair.serialize_pem());
        let dir = std::env::temp_dir();

        let err = load_server_config(dir.to_str().unwrap(), key_path.to_str().unwrap())
            .expect_err("a directory as the cert path must fail");
        assert!(matches!(err, TlsError::Io { .. }), "got {err:?}");

        let _ = std::fs::remove_file(key_path);
    }

    #[test]
    fn garbage_cert_pem_is_no_cert() {
        let ck = self_signed();
        let cert_path = write_temp_pem("garbage-cert", "this is not a pem certificate");
        let key_path = write_temp_pem("garbage-cert-key", &ck.key_pair.serialize_pem());

        let err = load_server_config(cert_path.to_str().unwrap(), key_path.to_str().unwrap())
            .expect_err("a PEM-free cert file must fail");
        assert!(matches!(err, TlsError::NoCert { .. }), "got {err:?}");

        let _ = std::fs::remove_file(cert_path);
        let _ = std::fs::remove_file(key_path);
    }

    #[test]
    fn garbage_key_pem_is_no_key() {
        let ck = self_signed();
        let cert_path = write_temp_pem("garbage-key-cert", &ck.cert.pem());
        let key_path = write_temp_pem("garbage-key", "this is not a pem private key");

        let err = load_server_config(cert_path.to_str().unwrap(), key_path.to_str().unwrap())
            .expect_err("a PEM-free key file must fail");
        assert!(matches!(err, TlsError::NoKey { .. }), "got {err:?}");

        let _ = std::fs::remove_file(cert_path);
        let _ = std::fs::remove_file(key_path);
    }

    #[test]
    fn key_from_a_different_cert_is_a_rustls_error() {
        let a = self_signed();
        let b = self_signed();
        let cert_path = write_temp_pem("mismatch-cert", &a.cert.pem());
        let key_path = write_temp_pem("mismatch-key", &b.key_pair.serialize_pem());

        let err = load_server_config(cert_path.to_str().unwrap(), key_path.to_str().unwrap())
            .expect_err("a mismatched cert/key pair must fail at load, not at first handshake");
        assert!(matches!(err, TlsError::Rustls(_)), "got {err:?}");

        let _ = std::fs::remove_file(cert_path);
        let _ = std::fs::remove_file(key_path);
    }

    /// Spawns [`accept_loop`] over a freshly bound loopback listener with
    /// tiny injectable limits, returning everything the listener-behavior
    /// tests observe. The delivery receiver is returned (not dropped) so
    /// the channel stays open — a closed channel is the `Drop` path, not
    /// these scenarios; nothing is ever read from it because no stalled
    /// plain-TCP client ever completes a handshake (no ClientHello, no
    /// TLS environment — the real handshake leg is the CI-gated
    /// `tls_live` suite).
    async fn spawn_loop_over_loopback(
        max_inflight: usize,
        queue: usize,
        handshake_timeout: Duration,
    ) -> (
        SocketAddr,
        Arc<Semaphore>,
        JoinHandle<()>,
        mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let semaphore = Arc::new(Semaphore::new(max_inflight));
        let (tx, rx) = mpsc::channel(queue);
        let limits = HandshakeLimits {
            handshake_timeout,
            max_inflight,
        };
        let inner = Arc::new(listener);
        let accept = {
            let inner = Arc::clone(&inner);
            move || {
                let inner = Arc::clone(&inner);
                async move { inner.accept().await }
            }
        };
        let task = tokio::spawn(accept_loop(
            accept,
            in_memory_server_config(),
            Arc::clone(&semaphore),
            tx,
            limits,
        ));
        (addr, semaphore, task, rx)
    }

    /// Polls until `semaphore` reports exactly `want` available permits,
    /// with a generous deadline (never a wall-time assert — only an
    /// upper-bound guard against hanging forever on a regression).
    async fn wait_for_permits(semaphore: &Semaphore, want: usize, scenario: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while semaphore.available_permits() != want {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{scenario}: permits stuck at {} (want {want})",
                semaphore.available_permits()
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Issue #174 review finding 1 (saturation), tightened per the code
    /// review: the handshake timeout here is 60s — far beyond every read
    /// window below — so a closed socket is *unambiguously* the admission
    /// gate's immediate rejection, never a handshake timeout firing.
    /// With 10 stalled plain-TCP clients against `max_inflight = 4`,
    /// exactly the 6 excess connections observe their socket closed
    /// (rejected, never queued or spawned) and exactly the 4
    /// permit-holding ones stay open mid-"handshake" (which also
    /// satisfies the plan's `>= 10 - (4 + 2 + 1)` floor).
    #[tokio::test]
    async fn saturation_rejects_excess_connections_at_the_admission_gate() {
        const MAX_INFLIGHT: usize = 4;
        const QUEUE: usize = 2;
        const CLIENTS: usize = 10;

        let (addr, semaphore, task, _rx) =
            spawn_loop_over_loopback(MAX_INFLIGHT, QUEUE, Duration::from_secs(60)).await;

        let mut clients = Vec::with_capacity(CLIENTS);
        for _ in 0..CLIENTS {
            clients.push(TcpStream::connect(addr).await.expect("client connect"));
        }

        // Every permit must be consumed by in-flight handshake tasks.
        wait_for_permits(&semaphore, 0, "saturation").await;

        // Classify every client: rejected ⇒ closed (EOF/reset) fast —
        // well under the 60s timeout; admitted ⇒ still open, its
        // handshake task waiting on a ClientHello that never comes.
        let mut closed = 0;
        let mut open = 0;
        for mut client in clients {
            let mut buf = [0u8; 8];
            match tokio::time::timeout(Duration::from_secs(1), client.read(&mut buf)).await {
                Ok(Ok(0)) | Ok(Err(_)) => closed += 1,
                Ok(Ok(_)) => panic!("the server must never write on an unhandshaken socket"),
                Err(_) => open += 1, // still open at the 1s read window
            }
        }
        assert_eq!(
            closed,
            CLIENTS - MAX_INFLIGHT,
            "exactly the excess connections must be closed by the admission gate"
        );
        assert_eq!(
            open, MAX_INFLIGHT,
            "exactly the permit-holding connections must remain open mid-handshake"
        );

        task.abort();
    }

    /// The timeout half of the original saturation scenario, separated so
    /// its EOFs can't confound the admission-gate proof above: stalled
    /// handshakes that hold every permit must release them all once the
    /// (tiny) handshake timeout fires — no permit leak on the timeout
    /// path.
    #[tokio::test]
    async fn stalled_handshakes_time_out_and_release_every_permit() {
        const MAX_INFLIGHT: usize = 2;

        let (addr, semaphore, task, _rx) =
            spawn_loop_over_loopback(MAX_INFLIGHT, 2, Duration::from_millis(200)).await;

        let mut clients = Vec::with_capacity(MAX_INFLIGHT);
        for _ in 0..MAX_INFLIGHT {
            clients.push(TcpStream::connect(addr).await.expect("client connect"));
        }

        wait_for_permits(&semaphore, 0, "all permits held by stalled handshakes").await;
        wait_for_permits(
            &semaphore,
            MAX_INFLIGHT,
            "recovery after handshake timeouts",
        )
        .await;

        task.abort();
    }

    /// Codex re-review finding (issue #174): aborting the accept task
    /// must *promptly* cancel the in-flight handshake tasks it spawned
    /// (they live in the loop's `JoinSet`, not detached), closing their
    /// sockets — deterministic at this level because `available_permits
    /// == 0` proves the handshake task owns the socket before the abort,
    /// and the 60s handshake timeout means the prompt EOF below can only
    /// be the cancellation.
    #[tokio::test]
    async fn aborting_the_accept_loop_promptly_cancels_inflight_handshakes() {
        let (addr, semaphore, task, _rx) =
            spawn_loop_over_loopback(1, 2, Duration::from_secs(60)).await;

        let mut client = TcpStream::connect(addr).await.expect("client connect");
        wait_for_permits(&semaphore, 0, "handshake admitted").await;

        task.abort();

        let mut buf = [0u8; 8];
        match tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => {} // closed promptly by the cancellation
            Ok(Ok(_)) => panic!("the server must never write on an unhandshaken socket"),
            Err(_) => panic!(
                "socket still open 5s after the accept task was aborted — \
                 the handshake task leaked past shutdown"
            ),
        }
    }

    /// The same contract through the production surface: dropping
    /// [`TlsListener`] (what happens when `axum::serve` returns) must
    /// promptly close a mid-handshake socket — well under the 10s
    /// production [`HANDSHAKE_TIMEOUT`], so the EOF below can only be the
    /// cancellation path, never the timeout.
    #[tokio::test]
    async fn dropping_the_listener_promptly_closes_a_mid_handshake_socket() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let tls_listener = TlsListener::new(listener, in_memory_server_config());

        let mut client = TcpStream::connect(addr).await.expect("client connect");
        // Give the accept loop a moment to admit the connection into a
        // handshake task (the production listener exposes no permit
        // counter to poll; if the drop below raced ahead of admission the
        // socket would close via the dropped `TcpListener` instead — the
        // same prompt-closure outcome either way, and the deterministic
        // handshake-task variant is proven by the accept_loop-level test
        // above).
        tokio::time::sleep(Duration::from_millis(100)).await;

        drop(tls_listener);

        let mut buf = [0u8; 8];
        match tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => {} // closed promptly, not after 10s
            Ok(Ok(_)) => panic!("the server must never write on an unhandshaken socket"),
            Err(_) => panic!(
                "socket still open 5s after the listener was dropped — \
                 in-flight handshakes must be cancelled at shutdown, not left \
                 to run out the 10s handshake timeout"
            ),
        }
    }

    /// Issue #174 review finding 2 (accept-error recovery), deterministic
    /// under a paused clock: a connection-class error is retried
    /// immediately (no backoff), a non-connection error takes exactly the
    /// 1s axum-parity backoff, and the loop reaches the next accept both
    /// times — it never exits on error.
    #[tokio::test(start_paused = true)]
    async fn accept_loop_survives_both_error_classes_with_axum_parity_backoff() {
        let calls: Arc<Mutex<Vec<tokio::time::Instant>>> = Arc::new(Mutex::new(Vec::new()));
        let accept = {
            let calls = Arc::clone(&calls);
            move || {
                let calls = Arc::clone(&calls);
                async move {
                    let n = {
                        let mut calls = calls.lock().expect("call log lock");
                        calls.push(tokio::time::Instant::now());
                        calls.len()
                    };
                    match n {
                        1 => Err(std::io::Error::from(ErrorKind::ConnectionReset)),
                        2 => Err(std::io::Error::other("injected transient accept failure")),
                        _ => {
                            std::future::pending::<
                                std::io::Result<(tokio::io::DuplexStream, SocketAddr)>,
                            >()
                            .await
                        }
                    }
                }
            }
        };
        let (tx, _rx) = mpsc::channel(1);
        let limits = HandshakeLimits {
            handshake_timeout: Duration::from_millis(100),
            max_inflight: 1,
        };
        let task = tokio::spawn(accept_loop(
            accept,
            in_memory_server_config(),
            Arc::new(Semaphore::new(1)),
            tx,
            limits,
        ));

        // Paused clock: this sleep auto-advances time through the loop's
        // own 1s backoff, deterministically.
        tokio::time::sleep(Duration::from_secs(5)).await;

        let calls = calls.lock().expect("call log lock").clone();
        assert_eq!(
            calls.len(),
            3,
            "the loop must survive both injected errors and accept again"
        );
        assert_eq!(
            calls[1] - calls[0],
            Duration::ZERO,
            "a connection-class error must retry immediately, with no backoff"
        );
        assert!(
            calls[2] - calls[1] >= Duration::from_secs(1),
            "a non-connection error must take the 1s axum-parity backoff (took {:?})",
            calls[2] - calls[1]
        );
        assert!(
            !task.is_finished(),
            "the accept loop must never exit on an accept error"
        );
        task.abort();
    }
}
