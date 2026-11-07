//! TLS client construction: skip-verify for `CLICKHOUSE_TLS_SKIP_VERIFY`, a
//! private CA for `CLICKHOUSE_TLS_CA_FILE` (issue #746 part D), and the
//! public roots built into the binary otherwise (issue #790). All three are
//! built by [`https_ch_client`].
//!
//! Encryption is preserved end-to-end; only the peer certificate is left
//! unchecked (edge case #5, see [`crate::ChClient::new`]'s startup warning).
//! This duplicates the mechanism proven in `xtask/src/ch_bench/tls.rs`
//! rather than depending on it, since a library crate cannot depend on a
//! workspace `bin` target.

use std::sync::Arc;
use std::time::Duration;

use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;

use crate::error::ChError;

/// A `rustls::client::danger::ServerCertVerifier` that accepts any server
/// certificate. Used only when the caller has explicitly opted into
/// `tls_skip_verify` — never the default.
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
        // Matches the explicit `ring` provider this config is built with
        // below (never the process-global default, which this module no
        // longer installs).
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Builds a `rustls` `ClientConfig` that skips certificate verification
/// entirely. Only reachable when `ChConnConfig::tls_skip_verify` is set.
fn danger_skip_verify_tls_config() -> Result<Arc<rustls::ClientConfig>, ChError> {
    // Bind an explicit `ring` `CryptoProvider` to this config via
    // `builder_with_provider` instead of `ClientConfig::builder()`, which
    // resolves the ambiguous *process-global* default provider and panics
    // if more than one provider feature is compiled in. This workspace pins
    // `rustls`/`hyper-rustls` to the `ring` feature, but Cargo's feature
    // unification also pulls in `hyper-rustls`'s default `aws-lc-rs` feature
    // elsewhere in the dependency graph, so both providers end up compiled
    // in. Carrying the provider on the config itself avoids mutating any
    // process-global state (no `install_default()` call) and guarantees
    // `NoVerify::supported_verify_schemes` above — which reads `ring`'s
    // schemes — always matches the provider this config actually uses.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| ChError::Config(format!("rustls: default protocol versions: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// Builds a `clickhouse::Client` whose HTTPS transport skips certificate
/// verification (`hyper-rustls` connector over the no-verify `rustls`
/// config above). The caller (`pool::build_base_client`) still must chain
/// `.with_url`/`.with_database`/`.with_user`/`.with_password`.
pub(crate) fn skip_verify_ch_client() -> Result<clickhouse::Client, ChError> {
    let tls_config = (*danger_skip_verify_tls_config()?).clone();
    Ok(https_ch_client(tls_config))
}

/// The CA certificates of the PEM file at `path`, as a root store. Every
/// failure names `CLICKHOUSE_TLS_CA_FILE`: the file cannot be read, a
/// certificate does not parse, or it holds none.
pub fn ca_root_store(path: &str) -> Result<rustls::RootCertStore, ChError> {
    let fail = |msg: String| ChError::Config(format!("CLICKHOUSE_TLS_CA_FILE {path}: {msg}"));
    let pem = std::fs::read(path).map_err(|e| fail(e.to_string()))?;
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
        let cert = cert.map_err(|e| fail(e.to_string()))?;
        roots.add(cert).map_err(|e| fail(e.to_string()))?;
    }
    if roots.is_empty() {
        return Err(fail("holds no certificate".to_string()));
    }
    Ok(roots)
}

/// The roots an `https` server is checked against when
/// `CLICKHOUSE_TLS_CA_FILE` is unset: the public set built into the binary.
pub(crate) fn public_roots() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    #[cfg(test)]
    TEST_ROOTS.with(|extra| {
        for cert in extra.borrow().iter() {
            roots.add(cert.clone()).expect("a test root parses");
        }
    });
    roots
}

#[cfg(test)]
thread_local! {
    /// Unit tests only: CA certificates `public_roots` appends, so a test server
    /// can be trusted through the no-CA-file path. Read on the building thread.
    pub(crate) static TEST_ROOTS: std::cell::RefCell<Vec<rustls_pki_types::CertificateDer<'static>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// A client whose HTTPS transport verifies the server's certificate and name
/// against `roots`, with the explicit `ring` provider.
pub(crate) fn verified_ch_client(
    roots: rustls::RootCertStore,
) -> Result<clickhouse::Client, ChError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| ChError::Config(format!("rustls: default protocol versions: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(https_ch_client(tls_config))
}

/// The plain-HTTP default client's transport settings
/// (vendor/clickhouse/src/http_client.rs:40-44).
pub(crate) const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
pub(crate) const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// What `apply_transport` sets. A test can implement it to see both calls.
pub(crate) trait TransportKnobs {
    fn set_tcp_keepalive(&mut self, d: Duration);
    fn set_pool_idle_timeout(&mut self, d: Duration);
}

pub(crate) fn apply_transport(k: &mut impl TransportKnobs) {
    k.set_tcp_keepalive(TCP_KEEPALIVE);
    k.set_pool_idle_timeout(POOL_IDLE_TIMEOUT);
}

struct Transport {
    connector: HttpConnector,
    client: hyper_util::client::legacy::Builder,
}

impl TransportKnobs for Transport {
    fn set_tcp_keepalive(&mut self, d: Duration) {
        self.connector.set_keepalive(Some(d));
    }
    fn set_pool_idle_timeout(&mut self, d: Duration) {
        self.client.pool_idle_timeout(d);
    }
}

/// The one place a TLS `clickhouse::Client` is built.
pub(crate) fn https_ch_client(tls_config: rustls::ClientConfig) -> clickhouse::Client {
    let mut t = Transport {
        connector: HttpConnector::new(),
        client: HyperClient::builder(TokioExecutor::new()),
    };
    apply_transport(&mut t);
    // The URL is `https`; the TLS layer below does the dialling.
    t.connector.enforce_http(false);
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_only()
        .enable_http1()
        .wrap_connector(t.connector);
    clickhouse::Client::with_http_client(t.client.build(https))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_verify_ch_client_builds_without_panicking() {
        // The main risk here is a `rustls` crypto-provider-ambiguity panic
        // if more than one provider feature is compiled in (edge case #4 of
        // the fix plan); this test exercises that construction path.
        let client = skip_verify_ch_client();
        assert!(client.is_ok());
    }
}
