//! Issue #790: `https` without `CLICKHOUSE_TLS_CA_FILE` dials TLS and checks
//! the server against the public roots built into the binary; the CA-file
//! and skip-verify arms keep their behaviour; every TLS client carries the
//! plain-HTTP default client's keepalive and idle timeout.
//!
//! Every test runs on the default current-thread runtime: `TEST_ROOTS` is
//! thread-local, and `ChPool::connect` builds its clients on the test's
//! thread. Never `flavor = "multi_thread"`.

use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use rustls::AlertDescription;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::error::ChError;
use crate::tls::{self, TransportKnobs};
use crate::{ChConnConfig, ChEndpoint, ChPool, ChProto};

type Leaf = (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>);

static LOOP: LazyLock<String> = LazyLock::new(|| Ipv4Addr::LOCALHOST.to_string());

const WRONG_NAME: &str = "wrong-name.invalid";

fn ring() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A self-signed CA and the leaves it signs.
struct Pki {
    key: rcgen::KeyPair,
    ca: rcgen::Certificate,
}

impl Pki {
    fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        // A distinct name per CA: a verifier picks the anchor by the
        // issuer's name, so two CAs of one name would be confused.
        static SERIAL: AtomicUsize = AtomicUsize::new(0);
        let n = SERIAL.fetch_add(1, Ordering::SeqCst);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, format!("pulsus 790 test CA {n}"));
        let key = rcgen::KeyPair::generate().unwrap();
        let ca = params.self_signed(&key).unwrap();
        Self { key, ca }
    }

    fn leaf(&self, san: &str) -> Leaf {
        let mut params = rcgen::CertificateParams::new(vec![san.to_string()]).unwrap();
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.ca, &self.key).unwrap();
        (
            vec![cert.der().clone()],
            PrivateKeyDer::Pkcs8(key.serialize_der().into()),
        )
    }

    /// Trusts this CA through `tls::public_roots`, on this thread.
    fn trust(&self) {
        tls::TEST_ROOTS.with(|roots| roots.borrow_mut().push(self.ca.der().clone()));
    }

    /// This CA alone, as a root store.
    fn roots(&self) -> rustls::RootCertStore {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.ca.der().clone()).unwrap();
        roots
    }

    /// Writes the CA PEM to a temporary file; the test removes it.
    fn pem_file(&self, tag: &str) -> String {
        let path =
            std::env::temp_dir().join(format!("pulsus-790-{}-{tag}.pem", std::process::id()));
        std::fs::write(&path, self.ca.pem()).unwrap();
        path.to_string_lossy().into_owned()
    }
}

/// What a test server saw of its one connection.
#[derive(Clone, Debug, PartialEq)]
enum Seen {
    /// The handshake failed with this error.
    Refused(rustls::Error),
    /// The handshake failed with an error that is not a `rustls::Error`.
    Other(String),
    /// The handshake succeeded and this request head was served.
    Served(String),
}

fn acceptor(leaf: Leaf) -> tokio_rustls::TlsAcceptor {
    let (chain, key) = leaf;
    let config = rustls::ServerConfig::builder_with_provider(ring())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap();
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}

/// Reads one whole request from `stream` into `buf` — the head, then its
/// body by length or to the last chunk — and returns its head, leaving any
/// bytes past it in `buf`. `None` when the stream ends first.
async fn read_request(stream: &mut (impl AsyncRead + Unpin), buf: &mut Vec<u8>) -> Option<String> {
    let mut chunk = [0u8; 4096];
    loop {
        let text = String::from_utf8_lossy(buf).to_ascii_lowercase();
        if let Some(head_end) = text.find("\r\n\r\n") {
            let head = &text[..head_end];
            let body_start = head_end + 4;
            let length = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse::<usize>().ok());
            let end = match length {
                Some(len) => (buf.len() >= body_start + len).then_some(body_start + len),
                None if head.contains("transfer-encoding: chunked") => text[body_start..]
                    .find("0\r\n\r\n")
                    .map(|i| body_start + i + 5),
                None => Some(body_start),
            };
            if let Some(end) = end {
                let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
                buf.drain(..end);
                return Some(head);
            }
        }
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn refused(e: std::io::Error) -> Seen {
    match e
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
    {
        Some(err) => Seen::Refused(err.clone()),
        None => Seen::Other(e.to_string()),
    }
}

/// A TLS server on loopback presenting `leaf` to **one** connection.
async fn tls_server(leaf: Leaf) -> (u16, oneshot::Receiver<Seen>) {
    let listener = TcpListener::bind((LOOP.as_str(), 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = acceptor(leaf);
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        match acceptor.accept(tcp).await {
            Err(e) => {
                let _ = tx.send(refused(e));
            }
            Ok(mut stream) => {
                let head = read_request(&mut stream, &mut Vec::new())
                    .await
                    .unwrap_or_default();
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await
                    .ok();
                stream.shutdown().await.ok();
                let _ = tx.send(Seen::Served(head));
            }
        }
    });
    (port, rx)
}

async fn seen(rx: oneshot::Receiver<Seen>) -> Option<Seen> {
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .ok()
        .and_then(Result::ok)
}

/// A TLS server on loopback that accepts any number of connections,
/// counts them, and serves any number of requests on each.
async fn keepalive_server(leaf: Leaf) -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind((LOOP.as_str(), 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = acceptor(leaf);
    let count = Arc::new(AtomicUsize::new(0));
    let counter = count.clone();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut stream) = acceptor.accept(tcp).await else {
                    return;
                };
                let mut buf = Vec::new();
                while read_request(&mut stream, &mut buf).await.is_some() {
                    if stream
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    (port, count)
}

fn cfg(port: u16) -> ChConnConfig {
    ChConnConfig {
        server: LOOP.clone(),
        http_port: port,
        database: "default".to_string(),
        proto: ChProto::Https,
        pool_size: 1,
        query_timeout: Duration::from_secs(5),
        ..Default::default()
    }
}

async fn connect(c: ChConnConfig) -> Result<ChPool, ChError> {
    tokio::time::timeout(Duration::from_secs(10), ChPool::connect(c))
        .await
        .expect("ChPool::connect finished within 10 s")
}

fn refused_with(alert: AlertDescription) -> Option<Seen> {
    Some(Seen::Refused(rustls::Error::AlertReceived(alert)))
}

fn served(s: Option<Seen>) -> bool {
    matches!(s, Some(Seen::Served(head)) if head.starts_with("POST /"))
}

#[tokio::test]
async fn public_roots_are_the_built_in_set() {
    assert!(!webpki_roots::TLS_SERVER_ROOTS.is_empty());
    assert_eq!(
        tls::public_roots().roots,
        webpki_roots::TLS_SERVER_ROOTS.to_vec()
    );
}

#[tokio::test]
async fn https_without_a_ca_file_dials_tls_and_checks_the_public_roots() {
    let pki = Pki::new();
    let (port, rx) = tls_server(pki.leaf(&LOOP)).await;
    assert!(connect(cfg(port)).await.is_err());
    assert_eq!(seen(rx).await, refused_with(AlertDescription::UnknownCA));
}

#[tokio::test]
async fn https_without_a_ca_file_accepts_a_certificate_the_roots_trust() {
    let pki = Pki::new();
    pki.trust();
    let (port, rx) = tls_server(pki.leaf(&LOOP)).await;
    let r = connect(cfg(port)).await;
    assert!(r.is_ok(), "{:?}", r.err());
    let s = seen(rx).await;
    assert!(served(s.clone()), "{s:?}");
}

#[tokio::test]
async fn https_without_a_ca_file_refuses_a_certificate_for_another_name() {
    let pki = Pki::new();
    pki.trust();
    let (port, rx) = tls_server(pki.leaf(WRONG_NAME)).await;
    assert!(connect(cfg(port)).await.is_err());
    assert_eq!(
        seen(rx).await,
        refused_with(AlertDescription::BadCertificate)
    );
}

#[tokio::test]
async fn every_servers_endpoint_dials_tls_and_verifies() {
    let pki = Pki::new();
    let (p1, rx1) = tls_server(pki.leaf(&LOOP)).await;
    let (p2, rx2) = tls_server(pki.leaf(&LOOP)).await;
    let mut c = cfg(p1);
    c.endpoints = [p1, p2]
        .into_iter()
        .map(|http_port| ChEndpoint {
            host: LOOP.clone(),
            http_port,
            zone: None,
        })
        .collect();
    assert!(connect(c).await.is_err());
    assert_eq!(seen(rx1).await, refused_with(AlertDescription::UnknownCA));
    assert_eq!(seen(rx2).await, refused_with(AlertDescription::UnknownCA));
}

#[tokio::test]
async fn https_with_a_ca_file_checks_that_file() {
    let a = Pki::new();
    let ca = a.pem_file("t5");

    let (port, rx) = tls_server(a.leaf(&LOOP)).await;
    let mut c = cfg(port);
    c.tls_ca_file = Some(ca.clone());
    let r = connect(c).await;
    let s = seen(rx).await;

    let (port_b, rx_b) = tls_server(a.leaf(WRONG_NAME)).await;
    let mut c = cfg(port_b);
    c.tls_ca_file = Some(ca.clone());
    let r_b = connect(c).await;
    let s_b = seen(rx_b).await;
    std::fs::remove_file(&ca).ok();

    assert!(r.is_ok(), "{:?}", r.err());
    assert!(served(s.clone()), "{s:?}");
    assert!(r_b.is_err());
    assert_eq!(s_b, refused_with(AlertDescription::BadCertificate));
}

#[tokio::test]
async fn https_with_a_ca_file_trusts_nothing_else() {
    let a = Pki::new();
    let ca = a.pem_file("t5c");
    let b = Pki::new();
    b.trust();
    let (port, rx) = tls_server(b.leaf(&LOOP)).await;
    let mut c = cfg(port);
    c.tls_ca_file = Some(ca.clone());
    let r = connect(c).await;
    let s = seen(rx).await;
    std::fs::remove_file(&ca).ok();

    assert!(r.is_err());
    assert_eq!(s, refused_with(AlertDescription::UnknownCA));
}

#[tokio::test]
async fn skip_verify_accepts_any_certificate_and_overrides_the_ca_file() {
    let a = Pki::new();
    let ca = a.pem_file("t6");
    let b = Pki::new();
    let (port, rx) = tls_server(b.leaf(WRONG_NAME)).await;
    let mut c = cfg(port);
    c.tls_skip_verify = true;
    c.tls_ca_file = Some(ca.clone());
    let r = connect(c).await;
    let s = seen(rx).await;
    std::fs::remove_file(&ca).ok();

    assert!(r.is_ok(), "{:?}", r.err());
    assert!(served(s.clone()), "{s:?}");
}

/// Two queries 100 ms apart share one connection; a third after 2.5 s idle
/// opens a second.
async fn drops_a_connection_idle_past_two_seconds(
    client: clickhouse::Client,
    count: Arc<AtomicUsize>,
) {
    let q1 = client.query("SELECT 1").execute().await;
    assert!(q1.is_ok(), "{q1:?}");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let q2 = client.query("SELECT 1").execute().await;
    assert!(q2.is_ok(), "{q2:?}");
    assert_eq!(count.load(Ordering::SeqCst), 1, "the connection is reused");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let q3 = client.query("SELECT 1").execute().await;
    assert!(q3.is_ok(), "{q3:?}");
    assert_eq!(
        count.load(Ordering::SeqCst),
        2,
        "a connection idle past 2 s is not reused"
    );
}

#[tokio::test]
async fn verified_client_drops_a_connection_idle_past_two_seconds() {
    let a = Pki::new();
    let (port, count) = keepalive_server(a.leaf(&LOOP)).await;
    let client = tls::verified_ch_client(a.roots())
        .unwrap()
        .with_url(format!("https://{}:{port}", *LOOP));
    drops_a_connection_idle_past_two_seconds(client, count).await;
}

#[tokio::test]
async fn skip_verify_client_drops_a_connection_idle_past_two_seconds() {
    let a = Pki::new();
    let (port, count) = keepalive_server(a.leaf(&LOOP)).await;
    let client = tls::skip_verify_ch_client()
        .unwrap()
        .with_url(format!("https://{}:{port}", *LOOP));
    drops_a_connection_idle_past_two_seconds(client, count).await;
}

#[derive(Default)]
struct Recorder {
    keepalive: Option<Duration>,
    idle: Option<Duration>,
}

impl TransportKnobs for Recorder {
    fn set_tcp_keepalive(&mut self, d: Duration) {
        self.keepalive = Some(d);
    }
    fn set_pool_idle_timeout(&mut self, d: Duration) {
        self.idle = Some(d);
    }
}

#[tokio::test]
async fn apply_transport_sets_keepalive_and_idle_timeout() {
    let mut r = Recorder::default();
    tls::apply_transport(&mut r);
    // Literals copied from vendor/clickhouse/src/http_client.rs:40 and :44.
    assert_eq!(r.keepalive, Some(Duration::from_secs(60)));
    assert_eq!(r.idle, Some(Duration::from_secs(2)));
}
