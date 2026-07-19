//! Plain connection-settings struct. Deliberately has no dependency on
//! `pulsus-config` (issue #2 supplies these values later via a `From`
//! conversion — that is the single integration point, not a build
//! dependency). Field names mirror docs/configuration.md §2.

use std::borrow::Cow;
use std::time::Duration;

use crate::error::ChError;
use crate::settings::QuerySettings;

/// ClickHouse consistency policy (issue #114), carried on
/// [`ChConnConfig`] and applied per-statement by [`crate::ChClient`]:
/// the quorum trio rides the insert path, `select_sequential_consistency`
/// the read path. Defaults are **all-off** (quorum disabled, sequential
/// consistency disabled) — byte-for-byte the pre-#114 insert/select — so
/// strong consistency is strictly opt-in (both add latency, and quorum is
/// only meaningful on `Replicated*` engines).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsistencyConfig {
    /// `clickhouse.insert_quorum` (default `0` = off). The number of
    /// replicas that must confirm a block before the insert is
    /// acknowledged. Integer-only (`auto`/majority is unsupported —
    /// docs/configuration.md §2).
    pub insert_quorum: u64,
    /// `clickhouse.insert_quorum_parallel` (default `true`). Only emitted
    /// when `insert_quorum > 0`.
    pub insert_quorum_parallel: bool,
    /// `clickhouse.insert_quorum_timeout` (default `120s`, reconciled to the
    /// default `query_timeout`). The quorum wait bound; must not exceed the
    /// insert deadline (`query_timeout`), which would preempt it. Only
    /// emitted when `insert_quorum > 0`, rendered in milliseconds.
    pub insert_quorum_timeout: Duration,
    /// `clickhouse.select_sequential_consistency` (default `false`). When
    /// set, reads see all prior quorum-committed writes (read-your-writes).
    pub select_sequential_consistency: bool,
}

impl Default for ConsistencyConfig {
    fn default() -> Self {
        Self {
            insert_quorum: 0,
            insert_quorum_parallel: true,
            insert_quorum_timeout: Duration::from_secs(120),
            select_sequential_consistency: false,
        }
    }
}

impl ConsistencyConfig {
    /// The per-statement settings the insert path emits: empty when
    /// `insert_quorum == 0` (off), else the quorum trio (see
    /// [`QuerySettings::with_insert_quorum`]).
    pub fn insert_settings(&self) -> QuerySettings {
        QuerySettings::new().with_insert_quorum(
            self.insert_quorum,
            self.insert_quorum_parallel,
            self.insert_quorum_timeout,
        )
    }

    /// Folds the read-side consistency setting onto a caller's `base`
    /// settings: adds `select_sequential_consistency = 1` iff enabled,
    /// leaving `base` otherwise untouched (engine budgets survive).
    pub fn apply_read(&self, base: QuerySettings) -> QuerySettings {
        base.with_select_sequential_consistency(self.select_sequential_consistency)
    }

    /// The single, pool-free source of the quorum/deadline invariant
    /// (issue #114). When `insert_quorum > 0`, enforces
    /// `0 < insert_quorum_timeout <= deadline`: a zero timeout means a
    /// no/infinite wait, and a timeout above the insert deadline can never
    /// be observed because the deadline (`query_timeout`) fires first
    /// (`insert_block` bounds the whole insert by it). Inert when quorum is
    /// off. Both authoritative construction gates
    /// ([`ChConnConfig::validate`] against `query_timeout`, and the fallible
    /// [`crate::ChClient::with_consistency`] against the client's
    /// `default_timeout`) delegate here, so no construction path can install
    /// a self-defeating quorum timeout.
    ///
    /// Ordering: zero is rejected before the `> deadline` check, so a zero
    /// timeout always yields the zero-specific message.
    pub fn validate_for_deadline(&self, deadline: Duration) -> Result<(), ChError> {
        if self.insert_quorum > 0 {
            if self.insert_quorum_timeout.is_zero() {
                return Err(ChError::Config(
                    "insert_quorum_timeout must be greater than zero when insert_quorum \
                     is enabled (a zero quorum timeout means no/infinite wait)"
                        .to_string(),
                ));
            }
            if self.insert_quorum_timeout > deadline {
                return Err(ChError::Config(
                    "insert_quorum_timeout must not exceed query_timeout when insert_quorum \
                     is enabled: the insert deadline (query_timeout) preempts the quorum wait"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }
}

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
    /// ClickHouse consistency policy (issue #114): the quorum trio on the
    /// insert path and `select_sequential_consistency` on the read path.
    /// Default is all-off (strong consistency is opt-in), so this is
    /// byte-for-byte the pre-#114 insert/select behaviour.
    pub consistency: ConsistencyConfig,
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
            consistency: ConsistencyConfig::default(),
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
        // Issue #114: authoritative quorum/deadline gate — the value
        // `ChClient::new` installs as `default_timeout` is this
        // `query_timeout`, which bounds the whole insert (client tokio
        // deadline + server `max_execution_time`). Delegates to the single
        // pool-free invariant the fallible `with_consistency` also enforces.
        self.consistency.validate_for_deadline(self.query_timeout)?;
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

    /// AC13 (issue #114): the single-source invariant enforces the FULL
    /// `0 < insert_quorum_timeout <= deadline` when quorum is enabled, and
    /// is inert when it is off.
    #[test]
    fn validate_for_deadline_enforces_the_full_quorum_invariant() {
        let deadline = Duration::from_secs(120);

        // Zero timeout with quorum on -> rejected, zero-specific message.
        let zero = ConsistencyConfig {
            insert_quorum: 2,
            insert_quorum_timeout: Duration::ZERO,
            ..ConsistencyConfig::default()
        };
        let err = zero.validate_for_deadline(deadline).unwrap_err();
        assert!(matches!(err, ChError::Config(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("insert_quorum_timeout") && msg.contains("greater than zero"),
            "zero must yield the zero-specific message, got: {msg}"
        );

        // Timeout above the deadline with quorum on -> rejected (preempt).
        let over = ConsistencyConfig {
            insert_quorum: 2,
            insert_quorum_timeout: Duration::from_secs(300),
            ..ConsistencyConfig::default()
        };
        assert!(matches!(
            over.validate_for_deadline(deadline),
            Err(ChError::Config(_))
        ));

        // Equal timeout is allowed.
        let equal = ConsistencyConfig {
            insert_quorum: 2,
            insert_quorum_timeout: Duration::from_secs(120),
            ..ConsistencyConfig::default()
        };
        assert!(equal.validate_for_deadline(deadline).is_ok());

        // Inert when quorum is off — a zero timeout is irrelevant.
        let off = ConsistencyConfig {
            insert_quorum: 0,
            insert_quorum_timeout: Duration::ZERO,
            ..ConsistencyConfig::default()
        };
        assert!(off.validate_for_deadline(deadline).is_ok());
    }

    /// AC12 (issue #114): `ChConnConfig::validate` (the `ChClient::new`
    /// gate) delegates to the shared invariant, rejecting BOTH a zero and an
    /// over-deadline quorum timeout; inert when quorum is off; the default
    /// (120s == default query_timeout 120s) passes.
    #[test]
    fn validate_rejects_both_zero_and_over_deadline_quorum_timeout() {
        let base = ChConnConfig {
            query_timeout: Duration::from_secs(120),
            ..Default::default()
        };

        let zero = ChConnConfig {
            consistency: ConsistencyConfig {
                insert_quorum: 2,
                insert_quorum_timeout: Duration::ZERO,
                ..ConsistencyConfig::default()
            },
            ..base.clone()
        };
        let err = zero.validate().unwrap_err();
        assert!(matches!(err, ChError::Config(_)));
        assert!(err.to_string().contains("insert_quorum_timeout"));

        let over = ChConnConfig {
            consistency: ConsistencyConfig {
                insert_quorum: 2,
                insert_quorum_timeout: Duration::from_secs(300),
                ..ConsistencyConfig::default()
            },
            ..base.clone()
        };
        let err = over.validate().unwrap_err();
        assert!(matches!(err, ChError::Config(_)));
        assert!(err.to_string().contains("insert_quorum_timeout"));

        // Inert when quorum is off (same over-deadline timeout).
        let off = ChConnConfig {
            consistency: ConsistencyConfig {
                insert_quorum: 0,
                insert_quorum_timeout: Duration::from_secs(300),
                ..ConsistencyConfig::default()
            },
            ..base
        };
        assert!(off.validate().is_ok());

        // Default is self-consistent (120s == 120s).
        assert!(ChConnConfig::default().validate().is_ok());
    }
}
