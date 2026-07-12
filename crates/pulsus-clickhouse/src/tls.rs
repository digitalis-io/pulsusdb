//! Minimal skip-verify TLS client construction for `CLICKHOUSE_TLS_SKIP_VERIFY`.
//!
//! Encryption is preserved end-to-end; only the peer certificate is left
//! unchecked (edge case #5, see [`crate::ChClient::new`]'s startup warning).
//! This duplicates the mechanism proven in `xtask/src/ch_bench/tls.rs`
//! rather than depending on it, since a library crate cannot depend on a
//! workspace `bin` target.

use std::sync::Arc;

use hyper_util::client::legacy::Client as HyperClient;
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
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_only()
        .enable_http1()
        .build();
    let http = HyperClient::builder(TokioExecutor::new()).build(connector);
    Ok(clickhouse::Client::with_http_client(http))
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
