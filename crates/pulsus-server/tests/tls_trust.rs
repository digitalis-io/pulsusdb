//! Trusting a private CA (issue #746 part D): the connection pool and
//! `schema/schema.sh` verify a ClickHouse server's certificate against
//! `CLICKHOUSE_TLS_CA_FILE`. Tests D-2 (the pool's half), D-3, D-4 and
//! D-5, against an HTTPS stand-in on loopback whose certificates are made
//! here with `rcgen`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsus_clickhouse::{ChConnConfig, ChPool, ChProto};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A CA, and a server certificate for `127.0.0.1` and `localhost` it
/// signed, as PEM.
struct Material {
    ca_pem: String,
    server: Arc<rustls::ServerConfig>,
}

fn material(name: &str) -> Material {
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, format!("pulsus test CA {name}"));
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let params =
        rcgen::CertificateParams::new(vec!["127.0.0.1".to_string(), "localhost".to_string()])
            .unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &ca, &ca_key).unwrap();

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let server = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            rustls_pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
        )
        .unwrap();
    Material {
        ca_pem: ca.pem(),
        server: Arc::new(server),
    }
}

fn write_temp(name: &str, contents: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "pulsus-tls-trust-{name}-{}-{:?}.pem",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::write(&path, contents).unwrap();
    path
}

/// An HTTPS stand-in: connection 0 presents `first`, every later one
/// `later`; every request is answered `200` with the body `26.8.21.10`.
/// `bodies` holds each request body received, by connection.
struct Stub {
    port: u16,
    bodies: Arc<Mutex<Vec<(usize, String)>>>,
}

async fn stub(first: Arc<rustls::ServerConfig>, later: Arc<rustls::ServerConfig>) -> Stub {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let seen = bodies.clone();
    let count = Arc::new(AtomicUsize::new(0));
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let n = count.fetch_add(1, Ordering::SeqCst);
            let config = if n == 0 { first.clone() } else { later.clone() };
            let seen = seen.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = tokio_rustls::TlsAcceptor::from(config).accept(tcp).await else {
                    return;
                };
                // One request per connection, then close.
                let mut buf = Vec::new();
                let mut chunk = [0u8; 8192];
                let mut continued = false;
                loop {
                    let Ok(read) = tls.read(&mut chunk).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..read]);
                    let text = String::from_utf8_lossy(&buf).to_string();
                    let Some(head_end) = text.find("\r\n\r\n") else {
                        continue;
                    };
                    let head = text[..head_end].to_ascii_lowercase();
                    if head.contains("expect: 100-continue") && !continued {
                        continued = true;
                        if tls
                            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    let length = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .and_then(|v| v.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if buf.len() - head_end - 4 >= length {
                        let body = String::from_utf8_lossy(&buf[head_end + 4..]).to_string();
                        seen.lock().unwrap().push((n, body));
                        break;
                    }
                }
                let _ = tls
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\n26.8.21.10",
                    )
                    .await;
                let _ = tls.shutdown().await;
            });
        }
    });
    Stub { port, bodies }
}

fn conn_config(port: u16, ca: Option<&Path>) -> ChConnConfig {
    ChConnConfig {
        server: "127.0.0.1".to_string(),
        http_port: port,
        proto: ChProto::Https,
        tls_ca_file: ca.map(|p| p.to_str().unwrap().to_string()),
        pool_size: 1,
        query_timeout: Duration::from_secs(5),
        ..ChConnConfig::default()
    }
}

/// **D-3: the pool trusts the named CA, and only it.**
#[tokio::test(flavor = "multi_thread")]
async fn d3_the_pool_verifies_against_the_ca_file() {
    let signing = material("d3-signing");
    let other = material("d3-other");
    let ca = write_temp("d3-ca", &signing.ca_pem);

    let trusted = stub(signing.server.clone(), signing.server.clone()).await;
    ChPool::connect(conn_config(trusted.port, Some(&ca)))
        .await
        .expect("a server signed by the CA is trusted");
    assert!(
        !trusted.bodies.lock().unwrap().is_empty(),
        "the ping arrived"
    );

    let untrusted = stub(other.server.clone(), other.server.clone()).await;
    let refused = ChPool::connect(conn_config(untrusted.port, Some(&ca))).await;
    assert!(refused.is_err(), "a server signed by another CA is refused");
    assert!(
        untrusted.bodies.lock().unwrap().is_empty(),
        "no request was sent"
    );

    let _ = std::fs::remove_file(ca);
}

/// **D-2 (the pool's half): a CA file that cannot be read is an error
/// naming its variable.**
#[tokio::test]
async fn d2_the_pool_names_an_unreadable_ca_file() {
    let missing = Path::new("/nonexistent/pulsus-d2/ca.crt");
    let err = match ChPool::connect(conn_config(1, Some(missing))).await {
        Ok(_) => panic!("an unreadable CA file is refused"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("CLICKHOUSE_TLS_CA_FILE"), "{err}");

    let not_pem = write_temp("d2-not-pem", "not a certificate\n");
    let err = match ChPool::connect(conn_config(1, Some(&not_pem))).await {
        Ok(_) => panic!("a file with no certificate is refused"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("CLICKHOUSE_TLS_CA_FILE"), "{err}");
    let _ = std::fs::remove_file(not_pem);
}

fn script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../schema/schema.sh")
}

/// Runs `schema.sh --mode single` against the stub on `port`, trusting
/// `ca`.
async fn run_script(port: u16, ca: &Path) -> (Option<i32>, String, String) {
    let port = port.to_string();
    let ca = ca.to_str().unwrap().to_string();
    tokio::task::spawn_blocking(move || {
        let out = std::process::Command::new("sh")
            .arg(script())
            .args(["--mode", "single"])
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("PULSUS_RETENTION_DAYS", "7")
            .env("CLICKHOUSE_SERVER", "127.0.0.1")
            .env("CLICKHOUSE_HTTP_PORT", port)
            .env("CLICKHOUSE_PROTO", "https")
            .env("CLICKHOUSE_TLS_CA_FILE", ca)
            .env("PULSUS_SCHEMA_WAIT_SECONDS", "2")
            .output()
            .expect("run sh");
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stdout).to_string(),
            String::from_utf8_lossy(&out.stderr).to_string(),
        )
    })
    .await
    .unwrap()
}

/// **D-4: the script reaches a server signed by the named CA, and waits
/// out one signed by another.**
#[tokio::test(flavor = "multi_thread")]
async fn d4_the_script_verifies_against_the_ca_file() {
    let signing = material("d4-signing");
    let other = material("d4-other");
    let ca = write_temp("d4-ca", &signing.ca_pem);

    let trusted = stub(signing.server.clone(), signing.server.clone()).await;
    let (code, out, err) = run_script(trusted.port, &ca).await;
    assert_eq!(code, Some(0), "{out}\n{err}");
    assert!(out.contains("statements applied"), "{out}");

    let untrusted = stub(other.server.clone(), other.server.clone()).await;
    let (code, _, err) = run_script(untrusted.port, &ca).await;
    assert_eq!(code, Some(1), "{err}");
    assert!(err.contains("did not answer within 2s"), "{err}");
    assert!(untrusted.bodies.lock().unwrap().is_empty());

    let _ = std::fs::remove_file(ca);
}

/// **D-5: after the wait, a server whose certificate changes to another
/// CA's gets no statement.**
#[tokio::test(flavor = "multi_thread")]
async fn d5_no_statement_reaches_a_server_signed_by_another_ca() {
    let signing = material("d5-signing");
    let other = material("d5-other");
    let ca = write_temp("d5-ca", &signing.ca_pem);

    let swapped = stub(signing.server.clone(), other.server.clone()).await;
    let (code, out, err) = run_script(swapped.port, &ca).await;
    assert_ne!(code, Some(0), "{out}\n{err}");
    let bodies = swapped.bodies.lock().unwrap().clone();
    // The wait reached the server over TLS on the first connection.
    assert_eq!(bodies, vec![(0, "SELECT 1".to_string())], "{err}");
    for (_, body) in &bodies {
        assert!(!body.contains("DROP") && !body.contains("CREATE"), "{body}");
    }

    let _ = std::fs::remove_file(ca);
}
