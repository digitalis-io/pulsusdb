//! Plain connection-settings struct. Deliberately has no dependency on
//! `pulsus-config` (issue #2 supplies these values later via a `From`
//! conversion — that is the single integration point, not a build
//! dependency). Field names mirror docs/configuration.md §2.

use std::borrow::Cow;
use std::time::Duration;

use crate::error::ChError;

/// Wraps a bare IPv6 literal in `[...]` so it forms a valid URL authority
/// (`http://[::1]:8123`); hostnames and IPv4 literals pass through unchanged.
/// A host is treated as an IPv6 literal when it contains a `:` and is not
/// already bracketed (idempotent). Applied on every URL the pool dials — both
/// the single `server` fallback and each configured endpoint — so IPv6 works
/// regardless of how the endpoint was supplied.
fn bracket_host(host: &str) -> Cow<'_, str> {
    if host.contains(':') && !host.starts_with('[') {
        Cow::Owned(format!("[{host}]"))
    } else {
        Cow::Borrowed(host)
    }
}

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

/// One ClickHouse endpoint in a multi-endpoint deployment (issue #43).
/// `host`/`http_port` are the dial target; `zone` is the endpoint's
/// availability zone (when known), used by the pool's zone-preferring
/// selection policy. An empty [`ChConnConfig::endpoints`] list falls back
/// to the single `server`/`http_port` endpoint (backward-compatible
/// default), so this type is only populated when connection spreading is
/// configured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChEndpoint {
    pub host: String,
    pub http_port: u16,
    pub zone: Option<String>,
}

/// A single resolved dial target derived from a [`ChConnConfig`] by
/// [`ChConnConfig::resolved_endpoints`]: a fully-formed `url` (scheme +
/// host + port), the endpoint's `zone`, and a `label` used for
/// per-endpoint telemetry/test observability. The pool builds exactly one
/// `clickhouse::Client` per resolved endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedEndpoint {
    pub url: String,
    pub zone: Option<String>,
    pub label: String,
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
    /// Multi-endpoint connection list (issue #43). **Empty** (the default)
    /// means a single endpoint synthesized from `server`/`http_port` —
    /// byte-for-byte the pre-#43 behavior. When populated, the pool holds
    /// one `clickhouse::Client` per endpoint and spreads requests across
    /// them (zone-preferring round-robin).
    pub endpoints: Vec<ChEndpoint>,
    /// This PulsusDB node's own availability zone (issue #43). When `Some`,
    /// endpoints whose `zone` matches are preferred; `None` (the default)
    /// spreads evenly across all endpoints. A zone that matches no endpoint
    /// is not an error — the policy degrades to even spreading.
    pub local_zone: Option<String>,
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
            endpoints: Vec::new(),
            local_zone: None,
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
        for (i, ep) in self.endpoints.iter().enumerate() {
            if ep.host.trim().is_empty() {
                return Err(ChError::Config(format!(
                    "endpoints[{i}].host must not be empty"
                )));
            }
            if ep.http_port == 0 {
                return Err(ChError::Config(format!(
                    "endpoints[{i}].http_port must not be 0"
                )));
            }
        }
        Ok(())
    }

    /// The URL scheme for this config's transport (`https` for
    /// [`ChProto::Https`], `http` otherwise).
    fn scheme(&self) -> &'static str {
        match self.proto {
            ChProto::Https => "https",
            _ => "http",
        }
    }

    /// The base URL `ChClient` dials in single-endpoint mode, e.g.
    /// `http://localhost:8123`. Retained for the single-endpoint fallback.
    pub fn base_url(&self) -> String {
        format!(
            "{}://{}:{}",
            self.scheme(),
            bracket_host(&self.server),
            self.http_port
        )
    }

    /// The dial URL for one `host:port` under this config's transport. IPv6
    /// host literals are bracketed (see [`bracket_host`]).
    pub fn endpoint_url(&self, host: &str, http_port: u16) -> String {
        format!("{}://{}:{http_port}", self.scheme(), bracket_host(host))
    }

    /// The set of endpoints the pool actually dials. When [`Self::endpoints`]
    /// is empty (the backward-compatible default) this is exactly one
    /// endpoint built from `server`/`http_port` (equal to [`Self::base_url`]);
    /// otherwise it is one [`ResolvedEndpoint`] per configured endpoint. The
    /// `label` is the dial URL, giving each entry a stable name for
    /// per-endpoint telemetry.
    pub fn resolved_endpoints(&self) -> Vec<ResolvedEndpoint> {
        if self.endpoints.is_empty() {
            let url = self.base_url();
            return vec![ResolvedEndpoint {
                label: url.clone(),
                url,
                // The legacy single server carries no declared zone; the
                // policy degrades to a single-endpoint order regardless.
                zone: None,
            }];
        }
        self.endpoints
            .iter()
            .map(|ep| {
                let url = self.endpoint_url(&ep.host, ep.http_port);
                ResolvedEndpoint {
                    label: url.clone(),
                    url,
                    zone: ep.zone.clone(),
                }
            })
            .collect()
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

    #[test]
    fn resolved_endpoints_defaults_to_the_single_server_endpoint() {
        // Backward-compat (AC1): an empty `endpoints` list resolves to
        // exactly one endpoint equal to `server`/`http_port`.
        let cfg = ChConnConfig::default();
        assert!(cfg.endpoints.is_empty());
        let resolved = cfg.resolved_endpoints();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].url, "http://localhost:8123");
        assert_eq!(resolved[0].label, "http://localhost:8123");
        assert_eq!(resolved[0].zone, None);
    }

    #[test]
    fn resolved_endpoints_maps_each_configured_endpoint() {
        let cfg = ChConnConfig {
            endpoints: vec![
                ChEndpoint {
                    host: "ch1".to_string(),
                    http_port: 8123,
                    zone: Some("az-a".to_string()),
                },
                ChEndpoint {
                    host: "ch2".to_string(),
                    http_port: 9123,
                    zone: Some("az-b".to_string()),
                },
            ],
            ..Default::default()
        };
        let resolved = cfg.resolved_endpoints();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].url, "http://ch1:8123");
        assert_eq!(resolved[0].zone.as_deref(), Some("az-a"));
        assert_eq!(resolved[1].url, "http://ch2:9123");
        assert_eq!(resolved[1].zone.as_deref(), Some("az-b"));
    }

    #[test]
    fn resolved_endpoints_use_https_scheme_for_https_proto() {
        let cfg = ChConnConfig {
            proto: ChProto::Https,
            endpoints: vec![ChEndpoint {
                host: "ch.internal".to_string(),
                http_port: 8443,
                zone: None,
            }],
            ..Default::default()
        };
        assert_eq!(cfg.resolved_endpoints()[0].url, "https://ch.internal:8443");
    }

    #[test]
    fn endpoint_url_brackets_ipv6_literal_hosts() {
        // A bare IPv6 literal must be bracketed to form a valid authority;
        // hostnames and IPv4 literals are untouched, and an already-bracketed
        // literal is left as-is (idempotent).
        let cfg = ChConnConfig::default();
        assert_eq!(cfg.endpoint_url("::1", 8123), "http://[::1]:8123");
        assert_eq!(
            cfg.endpoint_url("2001:db8::1", 9123),
            "http://[2001:db8::1]:9123"
        );
        assert_eq!(cfg.endpoint_url("[::1]", 8123), "http://[::1]:8123");
        assert_eq!(cfg.endpoint_url("ch1", 8123), "http://ch1:8123");
        assert_eq!(cfg.endpoint_url("10.0.0.5", 8123), "http://10.0.0.5:8123");
    }

    #[test]
    fn resolved_endpoints_bracket_ipv6_yaml_servers() {
        // The YAML `servers:` / `CLICKHOUSE_SERVERS` path (populates
        // `endpoints`) brackets IPv6 literals too.
        let cfg = ChConnConfig {
            endpoints: vec![ChEndpoint {
                host: "fe80::1".to_string(),
                http_port: 8123,
                zone: None,
            }],
            ..Default::default()
        };
        assert_eq!(cfg.resolved_endpoints()[0].url, "http://[fe80::1]:8123");
    }

    #[test]
    fn base_url_brackets_ipv6_server_literal() {
        let cfg = ChConnConfig {
            server: "::1".to_string(),
            ..Default::default()
        };
        assert_eq!(cfg.base_url(), "http://[::1]:8123");
    }

    #[test]
    fn empty_endpoint_host_is_rejected() {
        let cfg = ChConnConfig {
            endpoints: vec![ChEndpoint {
                host: "  ".to_string(),
                http_port: 8123,
                zone: None,
            }],
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(ChError::Config(_))));
    }

    #[test]
    fn zero_endpoint_port_is_rejected() {
        let cfg = ChConnConfig {
            endpoints: vec![ChEndpoint {
                host: "ch1".to_string(),
                http_port: 0,
                zone: None,
            }],
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(ChError::Config(_))));
    }
}
