//! Plain connection-settings struct. Deliberately has no dependency on
//! `pulsus-config` (issue #2 supplies these values later via a `From`
//! conversion — that is the single integration point, not a build
//! dependency). Field names mirror docs/configuration.md §2.

use std::time::Duration;

use crate::error::ChError;

/// Which transport `ChClient` dials. Chosen over a `tls: bool` flag (boolean
/// blindness) since plaintext-native, plaintext-HTTP, and TLS-HTTP are three
/// distinct connection strategies, not one axis with a toggle.
///
/// Only [`ChProto::Http`] and [`ChProto::Https`] are implemented: the M0
/// spike (docs/decisions/0001-clickhouse-client.md) selected the HTTP-only
/// `clickhouse` crate, whose own transport reliably serves DDL, bulk
/// insert, and streaming fetch alike, so no native-protocol path is shipped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChProto {
    /// Native TCP protocol. Not implemented by this wrapper (docs/decisions/0001)
    /// — kept as a variant so `CLICKHOUSE_PROTO=native` fails validation with a
    /// clear error rather than silently behaving like `Http`.
    Native,
    /// Plaintext HTTP interface (the default, and the only fully wired path).
    Http,
    /// HTTPS: TLS to ClickHouse's HTTP interface.
    Https,
}

/// Connection settings for [`crate::ChClient`]. Every field maps 1:1 to a
/// docs/configuration.md §2 variable; defaults match that table.
#[derive(Clone, Debug)]
pub struct ChConnConfig {
    /// `CLICKHOUSE_SERVER` (default `localhost`).
    pub server: String,
    /// `CLICKHOUSE_PORT`, native protocol port (default `9000`). Retained in
    /// the config surface for forward compatibility even though this
    /// wrapper does not dial it (see [`ChProto`]).
    pub native_port: u16,
    /// `CLICKHOUSE_HTTP_PORT` (default `8123`).
    pub http_port: u16,
    /// `CLICKHOUSE_DB` (default `pulsus`).
    pub database: String,
    /// `CLICKHOUSE_AUTH` user half (default `default`).
    pub user: String,
    /// `CLICKHOUSE_AUTH` password half (default empty).
    pub password: String,
    /// `CLICKHOUSE_PROTO` (default [`ChProto::Http`]).
    pub proto: ChProto,
    /// `CLICKHOUSE_TLS_SKIP_VERIFY` (default `false`). Relaxes certificate
    /// verification only — never downgrades to plaintext. See
    /// [`crate::client::ChClient::new`] for the startup warning this must emit.
    pub tls_skip_verify: bool,
    /// `PULSUS_CH_POOL_SIZE` (default `8`).
    pub pool_size: usize,
    /// `PULSUS_QUERY_TIMEOUT` (default `2m`). Drives both the client-side
    /// tokio deadline and the server-side `max_execution_time` (edge case
    /// #4 — a query-timeout split-brain otherwise leaves the server running
    /// an abandoned query or the client cancelling a valid one).
    pub query_timeout: Duration,
}

impl Default for ChConnConfig {
    fn default() -> Self {
        Self {
            server: "localhost".to_string(),
            native_port: 9000,
            http_port: 8123,
            database: "pulsus".to_string(),
            user: "default".to_string(),
            password: String::new(),
            proto: ChProto::Http,
            tls_skip_verify: false,
            pool_size: 8,
            query_timeout: Duration::from_secs(120),
        }
    }
}

impl ChConnConfig {
    /// Validates cross-field invariants not expressible in the type system.
    /// Called by [`crate::ChClient::new`] before any connection is made.
    pub fn validate(&self) -> Result<(), ChError> {
        if self.server.trim().is_empty() {
            return Err(ChError::Config("server must not be empty".to_string()));
        }
        if self.http_port == 0 {
            return Err(ChError::Config("http_port must not be 0".to_string()));
        }
        if self.native_port == 0 {
            return Err(ChError::Config("native_port must not be 0".to_string()));
        }
        if self.database.trim().is_empty() {
            return Err(ChError::Config("database must not be empty".to_string()));
        }
        if self.pool_size == 0 {
            return Err(ChError::Config("pool_size must be >= 1".to_string()));
        }
        if self.query_timeout.is_zero() {
            return Err(ChError::Config("query_timeout must be > 0".to_string()));
        }
        if self.proto == ChProto::Native {
            return Err(ChError::Config(
                "native transport is not supported: the M0 client is HTTP-only \
                 (docs/decisions/0001-clickhouse-client.md, ADR 0001) — use `http` or `https`"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// The base URL `ChClient` dials, e.g. `http://localhost:8123`.
    pub fn base_url(&self) -> String {
        let scheme = match self.proto {
            ChProto::Https => "https",
            _ => "http",
        };
        format!("{scheme}://{}:{}", self.server, self.http_port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        assert!(ChConnConfig::default().validate().is_ok());
    }

    #[test]
    fn empty_server_is_rejected() {
        let cfg = ChConnConfig {
            server: String::new(),
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(ChError::Config(_))));
    }

    #[test]
    fn zero_http_port_is_rejected() {
        let cfg = ChConnConfig {
            http_port: 0,
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(ChError::Config(_))));
    }

    #[test]
    fn zero_pool_size_is_rejected() {
        let cfg = ChConnConfig {
            pool_size: 0,
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(ChError::Config(_))));
    }

    #[test]
    fn zero_query_timeout_is_rejected() {
        let cfg = ChConnConfig {
            query_timeout: Duration::ZERO,
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(ChError::Config(_))));
    }

    #[test]
    fn native_proto_is_rejected_with_explanation() {
        let cfg = ChConnConfig {
            proto: ChProto::Native,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("native"));
    }

    #[test]
    fn base_url_uses_https_scheme_for_https_proto() {
        let cfg = ChConnConfig {
            proto: ChProto::Https,
            server: "ch.internal".to_string(),
            http_port: 8443,
            ..Default::default()
        };
        assert_eq!(cfg.base_url(), "https://ch.internal:8443");
    }

    #[test]
    fn base_url_uses_http_scheme_by_default() {
        let cfg = ChConnConfig::default();
        assert_eq!(cfg.base_url(), "http://localhost:8123");
    }
}
