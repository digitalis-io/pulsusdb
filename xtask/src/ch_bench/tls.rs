//! TLS scenario: connect to a TLS-enabled ClickHouse over each candidate's
//! own transport (HTTPS for `clickhouse`, native+TLS for `klickhouse`) and
//! run one insert + one fetch, with and without certificate verification,
//! to prove the transport works end-to-end (issue #3 plan).
//!
//! Requires a ClickHouse instance with the secure ports enabled (native TLS
//! 9440, HTTPS 8443) and the self-signed CA from `xtask/docker/gen-certs.sh`.
//! Not run by default — see `xtask/docker/docker-compose.ch.yml` and the ADR
//! methodology section for how to bring one up.

use std::sync::Arc;

use super::CrateUnderTest;
use super::rows::gen_metric_rows;

#[derive(Clone, Debug, serde::Serialize)]
pub struct TlsReport {
    pub crate_name: &'static str,
    pub verify_mode: &'static str, // "verified" | "skip-verify"
    pub insert_ok: bool,
    pub fetch_ok: bool,
    pub rows_round_tripped: u64,
    pub error: Option<String>,
}

/// Runs one insert + one fetch against an already-connected TLS candidate.
/// The candidate connection itself (verified vs skip-verify, CA loading) is
/// constructed by the caller (main.rs) since it is transport-specific; this
/// function only exercises the crate-agnostic operations once connected.
pub async fn bench_tls_roundtrip<C: CrateUnderTest>(
    c: &C,
    table: &str,
    verify_mode: &'static str,
) -> TlsReport {
    let mut report = TlsReport {
        crate_name: c.name(),
        verify_mode,
        insert_ok: false,
        fetch_ok: false,
        rows_round_tripped: 0,
        error: None,
    };

    if let Err(e) = c.execute_ddl(&super::insert::metric_table_ddl(table)).await {
        report.error = Some(format!("create table over TLS: {e}"));
        return report;
    }
    if let Err(e) = c.execute_ddl(&format!("TRUNCATE TABLE {table}")).await {
        report.error = Some(format!("truncate over TLS: {e}"));
        return report;
    }

    let rows = gen_metric_rows(1_000, 0, 9);
    if let Err(e) = c.insert_metric_block(table, &rows).await {
        report.error = Some(format!("insert over TLS: {e}"));
        return report;
    }
    report.insert_ok = true;

    match c.fetch_metric_projection(table, &rows[0].metric_name).await {
        Ok((n, _cksum)) => {
            report.fetch_ok = true;
            report.rows_round_tripped = n;
        }
        Err(e) => {
            report.error = Some(format!("fetch over TLS: {e}"));
        }
    }
    report
}

/// Builds a `rustls` `ClientConfig` that skips certificate verification
/// entirely. Used only for the `skip-verify` TLS scenario, gated behind
/// `CLICKHOUSE_TLS_SKIP_VERIFY` in the shipped wrapper — this must never be
/// the default and must emit a startup warning there (edge case #5).
pub fn danger_skip_verify_tls_config() -> Arc<rustls::ClientConfig> {
    #[derive(Debug)]
    struct NoVerify;
    impl rustls::client::danger::ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls_pki_types::CertificateDer<'_>,
            _intermediates: &[rustls_pki_types::CertificateDer<'_>],
            _server_name: &rustls_pki_types::ServerName<'_>,
            _ocsp_response: &[u8],
            _now: rustls_pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls_pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &rustls_pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }

    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    Arc::new(config)
}

/// Builds a `rustls` `ClientConfig` that verifies the server certificate
/// against the benchmark's self-signed CA (`gen-certs.sh`'s `ca.crt`).
pub fn verified_tls_config(ca_pem_path: &str) -> anyhow::Result<rustls::ClientConfig> {
    let ca_pem = std::fs::read(ca_pem_path)?;
    let mut reader = std::io::Cursor::new(ca_pem);
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut reader) {
        roots.add(cert?)?;
    }
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

/// Wraps a `rustls::ClientConfig` into an HTTPS `hyper` connector suitable
/// for [`clickhouse::Client::with_http_client`].
fn https_connector(
    config: rustls::ClientConfig,
) -> hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector> {
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(config)
        .https_only()
        .enable_http1()
        .build()
}

/// Builds a `clickhouse` (HTTP) candidate that dials `https_url` through the
/// given TLS config, i.e. exercising `clickhouse`'s HTTPS transport rather
/// than its default plaintext/webpki-roots client.
pub fn ch_candidate_over_tls(
    https_url: &str,
    database: &str,
    user: &str,
    password: &str,
    config: rustls::ClientConfig,
) -> super::ChCandidate {
    let connector = https_connector(config);
    let http = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build(connector);
    let mut client = clickhouse::Client::with_http_client(http)
        .with_url(https_url)
        .with_database(database)
        .with_user(user);
    if !password.is_empty() {
        client = client.with_password(password);
    }
    super::ChCandidate { client }
}

/// Connects a `klickhouse` (native) candidate over TLS to `addr`.
pub async fn kl_candidate_over_tls(
    addr: &str,
    database: &str,
    user: &str,
    password: &str,
    server_name: &str,
    config: Arc<rustls::ClientConfig>,
) -> anyhow::Result<super::KlCandidate> {
    let options = klickhouse::ClientOptions {
        username: user.to_string(),
        password: password.to_string(),
        default_database: database.to_string(),
        tcp_nodelay: true,
    };
    let name = rustls_pki_types::ServerName::try_from(server_name.to_string())?;
    let connector = tokio_rustls::TlsConnector::from(config);
    let client = klickhouse::Client::connect_tls(addr, options, name, &connector).await?;
    Ok(super::KlCandidate { client })
}
