//! The `Config` → `ChConnConfig` / `SchemaParams` mappings, in exactly one
//! place (task-manager resolution on issue #6, carried over from issue #5's
//! resolution #4): `pulsus-clickhouse` and `pulsus-schema` stay config-free,
//! and every caller in this binary — the `--mode init` schema controller
//! (`schema_init.rs`) and the serving startup path (`serve.rs`, which runs
//! the same schema reconcile before flipping readiness — issue #6 review
//! fix) — derives its connection settings and schema parameters from these
//! functions.

use std::sync::Arc;

use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChEndpoint, ChError, ChPool, ChProto, ConsistencyConfig,
};
use pulsus_config::Config;
use pulsus_read::{
    EngineConfig, LabelCache, LabelCacheConfig, LogQlEngine, MetricsConfig, MetricsEngine,
    TraceEngine, TraceReadConfig,
};
use pulsus_schema::{RenderCtx, SchemaParams};
use pulsus_write::{MetricWriterTables, TraceWriterTables, WriterTables};

/// Maps `Config` to the connection settings any part of this binary dials
/// ClickHouse with, targeting the configured `clickhouse.database`. Callers
/// that need a bootstrap connection instead (the target database does not
/// exist yet) use [`bootstrap_conn_config_from`]; the field-by-field mapping
/// itself still lives here exactly once.
pub(crate) fn conn_config_from(config: &Config) -> ChConnConfig {
    ChConnConfig {
        server: config.clickhouse.server.clone(),
        native_port: config.clickhouse.port,
        http_port: config.clickhouse.http_port,
        database: config.clickhouse.database.clone(),
        user: config.clickhouse.auth.user.clone(),
        password: config.clickhouse.auth.password.expose().to_string(),
        proto: match config.clickhouse.proto {
            pulsus_config::ChProto::Native => ChProto::Native,
            pulsus_config::ChProto::Http => ChProto::Http,
            pulsus_config::ChProto::Https => ChProto::Https,
        },
        tls_skip_verify: config.clickhouse.tls_skip_verify,
        pool_size: config.clickhouse.pool_size as usize,
        query_timeout: config.query_timeout.0,
        // Issue #43: multi-endpoint connection spreading. An empty
        // `clickhouse.servers` maps to an empty `endpoints`, which the pool
        // resolves to the single `server`/`http_port` endpoint — byte-for-byte
        // the pre-#43 behavior. A per-entry port defaults to
        // `clickhouse.http_port`. `local_zone` is the already-resolved
        // availability zone (operator-set or auto-detected — see
        // `azdetect::resolve_local_zone`).
        endpoints: config
            .clickhouse
            .servers
            .iter()
            .map(|s| ChEndpoint {
                host: s.host.clone(),
                http_port: s.http_port.unwrap_or(config.clickhouse.http_port),
                zone: s.zone.clone(),
            })
            .collect(),
        local_zone: config.availability_zone.clone(),
        // Issue #114: the consistency policy carried by the write/DDL path's
        // `ChClient::new`. `bootstrap_conn_config_from` inherits this via
        // `..conn_config_from`. The shared-pool read/write clients install
        // it via the fallible `ChClient::with_consistency`.
        consistency: consistency_from(config),
    }
}

/// Maps `Config` to [`pulsus_clickhouse::ConsistencyConfig`] (issue #114):
/// the four `clickhouse.*` consistency keys the write insert path and the
/// read select path apply per-statement. Defaults are all-off (strong
/// consistency is opt-in), so an unconfigured deployment's insert/select is
/// byte-for-byte the pre-#114 behaviour.
pub(crate) fn consistency_from(config: &Config) -> ConsistencyConfig {
    ConsistencyConfig {
        insert_quorum: config.clickhouse.insert_quorum,
        insert_quorum_parallel: config.clickhouse.insert_quorum_parallel,
        insert_quorum_timeout: config.clickhouse.insert_quorum_timeout.0,
        select_sequential_consistency: config.clickhouse.select_sequential_consistency,
    }
}

/// [`conn_config_from`]'s mapping, with `.database` overridden to
/// ClickHouse's built-in `default` database, never `config.clickhouse.database`
/// directly: on a fresh server the target database does not exist yet, and
/// every ClickHouse HTTP request — even a bare `SELECT 1` health probe —
/// fails with `UNKNOWN_DATABASE` if `?database=` names a database that isn't
/// there yet. `pulsus-schema`'s DDL is fully `{{db}}.`-qualified, so this
/// binding never affects which database the DDL actually targets
/// (docs/schemas.md's "CREATE DATABASE chicken-and-egg" edge case). Used by
/// both `--mode init` (`schema_init.rs`) and the serving reconnect loop's
/// schema-reconcile step (`serve.rs`).
pub(crate) fn bootstrap_conn_config_from(config: &Config) -> ChConnConfig {
    ChConnConfig {
        database: "default".to_string(),
        ..conn_config_from(config)
    }
}

/// Maps `Config` to the schema controller's rendering/reconcile parameters
/// (`pulsus_schema::run_init`/`reconcile`'s `SchemaParams`). Used by both
/// `--mode init` and the serving reconnect loop's schema-reconcile step.
pub(crate) fn schema_params_from(config: &Config) -> SchemaParams {
    RenderCtx {
        db: config.clickhouse.database.clone(),
        cluster: config.cluster.clone(),
        dist_suffix: config.dist_suffix.clone(),
        storage_policy: config.storage_policy.clone(),
        retention_days: config.retention_days,
        log_rollup: config.log_rollup_resolution.0,
    }
}

/// Maps `Config` to [`pulsus_read::LogQlEngine`]'s table-name/budget
/// context (issue #13 architect plan: `EngineConfig` construction, deferred
/// from #12, lands here alongside the other `Config → *` mappings).
/// Cluster-aware table-name resolution mirrors `pulsus-schema`'s own
/// `_dist`-suffix rule (`controller.rs`): a configured `cluster` reads
/// through the `_dist` wrapper tables, an unclustered deployment reads the
/// base tables directly.
pub(crate) fn engine_config_from(config: &Config) -> EngineConfig {
    let dist = if config.cluster.is_some() {
        config.dist_suffix.as_str()
    } else {
        ""
    };
    EngineConfig {
        db: config.clickhouse.database.clone(),
        streams_idx: format!("log_streams_idx{dist}"),
        streams: format!("log_streams{dist}"),
        samples: format!("log_samples{dist}"),
        rollup_table: format!(
            "log_metrics_{}{dist}",
            pulsus_schema::rollup_suffix(config.log_rollup_resolution.0)
        ),
        rollup_res_ns: config.log_rollup_resolution.0.as_nanos() as u64,
        scan_budget_bytes: config.reader.logql_scan_budget_bytes.0,
        max_streams: pulsus_read::DEFAULT_MAX_STREAMS,
        pipeline_scan_factor: config.reader.logql_pipeline_scan_factor,
    }
}

/// Maps `Config` to [`pulsus_write::WriterTables`] (issue #15 architect
/// plan, Design A): the writer becomes `_dist`-aware, deriving table names
/// the *same way* [`engine_config_from`] does — a configured `cluster`
/// writes through the `_dist` wrapper tables, an unclustered deployment
/// writes the base tables directly. schemas.md §7: "all inserts go
/// through the `_dist` wrappers … the writer never freelances shard
/// placement" — this function is that mandate's one enforcement point on
/// the write path, mirroring `engine_config_from`'s enforcement on the
/// read path.
pub(crate) fn writer_tables_from(config: &Config) -> WriterTables {
    let dist = if config.cluster.is_some() {
        config.dist_suffix.as_str()
    } else {
        ""
    };
    WriterTables {
        samples: Arc::from(format!("log_samples{dist}")),
        streams: Arc::from(format!("log_streams{dist}")),
    }
}

/// Maps `Config` to [`pulsus_write::MetricWriterTables`] (issue #26
/// architect plan), deriving `_dist` names the *same way*
/// [`writer_tables_from`] does for `metric_samples`/`metric_series` — a
/// configured `cluster` writes through the `_dist` wrapper tables, an
/// unclustered deployment writes the base tables directly. `metadata`
/// NEVER carries a `_dist` suffix: `metric_metadata` is a global catalog
/// table (docs/schemas.md §2.1/§7, catalog id 3, `family: None`), not
/// sharded, so there is no `_dist` wrapper for it to reconcile in the
/// first place.
pub(crate) fn metric_writer_tables_from(config: &Config) -> MetricWriterTables {
    let dist = if config.cluster.is_some() {
        config.dist_suffix.as_str()
    } else {
        ""
    };
    MetricWriterTables {
        samples: Arc::from(format!("metric_samples{dist}")),
        series: Arc::from(format!("metric_series{dist}")),
        metadata: Arc::from("metric_metadata"),
        // `metric_hist_samples` (M7-A4, issue #120) is a co-sharded
        // Metrics-family table, `_dist`-aware exactly like `metric_samples`.
        hist_samples: Arc::from(format!("metric_hist_samples{dist}")),
    }
}

/// Maps `Config` to [`pulsus_write::TraceWriterTables`] (issue #54),
/// deriving `_dist` names the *same way* [`writer_tables_from`]/
/// [`metric_writer_tables_from`] do — a configured `cluster` writes both
/// per-shard trace tables through their `_dist` wrappers (`Family::Traces`,
/// `cityHash64(trace_id)` sharding, docs/schemas.md §7), an unclustered
/// deployment writes the base tables directly. `trace_tag_catalog` is
/// deliberately absent: it is MV-populated (issue #53), never
/// writer-written, so there is no table name for the writer to resolve.
pub(crate) fn trace_writer_tables_from(config: &Config) -> TraceWriterTables {
    let dist = if config.cluster.is_some() {
        config.dist_suffix.as_str()
    } else {
        ""
    };
    TraceWriterTables {
        spans: Arc::from(format!("trace_spans{dist}")),
        attrs: Arc::from(format!("trace_attrs_idx{dist}")),
    }
}

/// Builds a [`LogQlEngine`] over `pool` — the same `Arc<ChPool>` `AppState`
/// already holds (`ChClient::from_shared_pool`, issue #13 resolved open
/// question #1), so a `/api/logs/v1` request never opens a second
/// connection pool.
pub(crate) fn logql_engine(pool: Arc<ChPool>, config: &Config) -> Result<LogQlEngine, ChError> {
    let client = ChClient::from_shared_pool(pool, config.query_timeout.0)
        .with_consistency(consistency_from(config))?;
    Ok(LogQlEngine::new(client, engine_config_from(config)))
}

/// Maps `Config` to [`pulsus_read::LabelCacheConfig`] (issue #30 architect
/// plan): `metric_series` is `_dist`-aware exactly as
/// [`metric_writer_tables_from`] derives it — cache reads and writer
/// registrations must agree on which physical table they mean.
/// `staleness_multiplier` is the documented constant
/// ([`pulsus_read::DEFAULT_STALENESS_MULTIPLIER`], task-manager resolution
/// #2 on issue #30), not yet promoted to a config field.
pub(crate) fn label_cache_config_from(config: &Config) -> LabelCacheConfig {
    let dist = if config.cluster.is_some() {
        config.dist_suffix.as_str()
    } else {
        ""
    };
    LabelCacheConfig {
        db: config.clickhouse.database.clone(),
        series_table: format!("metric_series{dist}"),
        bucket_ms: config.reader.series_activity_bucket.0.as_millis() as i64,
        window_ms: config.reader.cache_window.0.as_millis() as i64,
        cache_max_series: config.reader.cache_max_series,
        ttl: config.reader.cache_ttl.0,
        staleness_multiplier: pulsus_read::DEFAULT_STALENESS_MULTIPLIER,
    }
}

/// Builds a [`LabelCache`] over `pool`, mirroring [`logql_engine`]'s
/// "shared pool, no second connection" contract.
pub(crate) fn build_label_cache(pool: Arc<ChPool>, config: &Config) -> Result<LabelCache, ChError> {
    let client = ChClient::from_shared_pool(pool, config.query_timeout.0)
        .with_consistency(consistency_from(config))?;
    Ok(LabelCache::new(client, label_cache_config_from(config)))
}

/// Maps `Config` to [`pulsus_read::MetricsConfig`] (issue #32 architect
/// plan): `metric_samples`/`metric_series` are `_dist`-aware exactly as
/// [`metric_writer_tables_from`]/[`label_cache_config_from`] derive them;
/// `metric_metadata` is **never** `_dist`-suffixed (docs/schemas.md §2.1: a
/// global, unsharded catalog table), mirroring
/// [`metric_writer_tables_from`]'s own carve-out for it.
pub(crate) fn metrics_config_from(config: &Config) -> MetricsConfig {
    let dist = if config.cluster.is_some() {
        config.dist_suffix.as_str()
    } else {
        ""
    };
    MetricsConfig {
        db: config.clickhouse.database.clone(),
        samples_table: format!("metric_samples{dist}"),
        series_table: format!("metric_series{dist}"),
        metadata_table: "metric_metadata".to_string(),
        // M7-A5a: the dual-read's complementary histogram table, `_dist`-
        // aware exactly like `samples_table` (co-sharded Metrics family).
        hist_samples_table: format!("metric_hist_samples{dist}"),
        // Issue #65 (M6-02): the experimental-function gate's production
        // carrier — `ReaderConfig -> MetricsConfig -> PlanParams`.
        experimental_functions: config.reader.promql_experimental_functions,
        // Issue #85 (M6-08c): the name-less-selector fan-out cap's
        // production carrier — `ReaderConfig -> MetricsConfig ->
        // MetricsEngine::plan_multi_metric_fetch`.
        max_metric_fanout: config.reader.promql_max_metric_fanout,
        // Issue #89 (retroactive re-review): the independent
        // cache-enumeration scan-budget's production carrier —
        // `ReaderConfig -> MetricsConfig -> resolve_multi_metric`.
        max_cache_scan: config.reader.promql_max_cache_scan,
        // Issue #82 (retroactive re-review): the info() metadata-family
        // cardinality cap's production carrier — `ReaderConfig ->
        // MetricsConfig -> MetricsEngine::query_inner`'s info_family cap.
        max_info_series: config.reader.promql_max_info_series,
    }
}

/// Builds a [`MetricsEngine`] over `pool` and the already-constructed
/// `label_cache` — the same `Arc<ChPool>`/`Arc<LabelCache>` `AppState`
/// already holds, mirroring [`logql_engine`]'s "shared pool, no second
/// connection" contract (issue #32).
pub(crate) fn metrics_engine(
    pool: Arc<ChPool>,
    label_cache: Arc<LabelCache>,
    config: &Config,
    eval_gate: Arc<pulsus_read::EvalGate>,
) -> Result<MetricsEngine, ChError> {
    let client = ChClient::from_shared_pool(pool, config.query_timeout.0)
        .with_consistency(consistency_from(config))?;
    Ok(
        MetricsEngine::new(client, label_cache, metrics_config_from(config))
            .with_eval_gate(eval_gate),
    )
}

/// Maps `Config` to [`pulsus_read::TraceReadConfig`] (issues #55/#57):
/// both trace tables are `_dist`-aware exactly as every other read/write
/// table mapping in this module — a configured `cluster` reads through
/// the `_dist` wrappers ([`trace_writer_tables_from`]'s write-side twin)
/// and flips `distributed` so the search engine injects the
/// docs/schemas.md §7 clustered-reader settings; an unclustered
/// deployment reads the base tables directly. Search budgets/caps map
/// from `reader.traceql_max_candidates` / `reader.traceql_scan_budget_rows`.
pub(crate) fn trace_read_config_from(config: &Config) -> TraceReadConfig {
    let dist = if config.cluster.is_some() {
        config.dist_suffix.as_str()
    } else {
        ""
    };
    TraceReadConfig {
        spans_table: format!("trace_spans{dist}"),
        attrs_table: format!("trace_attrs_idx{dist}"),
        // `trace_tag_catalog` NEVER carries a `_dist` suffix (issue #58):
        // it is a global catalog table (migration 18, `Replication::Global`,
        // `family: None` — no `_dist` wrapper exists to name), so tag
        // discovery reads the local replica without fan-out — the
        // `metric_metadata` carve-out pattern.
        catalog_table: "trace_tag_catalog".to_string(),
        max_candidates: config.reader.traceql_max_candidates,
        scan_budget_rows: config.reader.traceql_scan_budget_rows,
        distributed: config.cluster.is_some(),
        skip_unavailable_shards: config.skip_unavailable_shards,
    }
}

/// Builds a [`TraceEngine`] over `pool`, mirroring [`logql_engine`]'s
/// "shared pool, no second connection" contract.
pub(crate) fn trace_engine(pool: Arc<ChPool>, config: &Config) -> Result<TraceEngine, ChError> {
    let client = ChClient::from_shared_pool(pool, config.query_timeout.0)
        .with_consistency(consistency_from(config))?;
    Ok(TraceEngine::new(client, trace_read_config_from(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conn_config_from_targets_the_configured_database() {
        let mut config = Config::default();
        config.clickhouse.database = "pulsus".to_string();
        let ch_cfg = conn_config_from(&config);
        assert_eq!(ch_cfg.database, "pulsus");
    }

    #[test]
    fn conn_config_from_defaults_to_no_endpoints_and_no_zone() {
        // Backward-compat (issue #43 AC1): no configured servers -> empty
        // `endpoints` (the pool falls back to the single server/http_port
        // endpoint), no `local_zone`.
        let ch_cfg = conn_config_from(&Config::default());
        assert!(ch_cfg.endpoints.is_empty());
        assert_eq!(ch_cfg.local_zone, None);
    }

    #[test]
    fn conn_config_from_maps_servers_with_port_fallback_and_zone() {
        use pulsus_config::ChServerEntry;
        let mut config = Config::default();
        config.clickhouse.http_port = 8123;
        config.clickhouse.servers = vec![
            ChServerEntry {
                host: "ch1".to_string(),
                http_port: Some(9123),
                zone: Some("az-a".to_string()),
            },
            ChServerEntry {
                host: "ch2".to_string(),
                http_port: None, // inherits clickhouse.http_port
                zone: Some("az-b".to_string()),
            },
        ];
        config.availability_zone = Some("az-a".to_string());

        let ch_cfg = conn_config_from(&config);
        assert_eq!(ch_cfg.local_zone.as_deref(), Some("az-a"));
        assert_eq!(ch_cfg.endpoints.len(), 2);
        assert_eq!(ch_cfg.endpoints[0].host, "ch1");
        assert_eq!(ch_cfg.endpoints[0].http_port, 9123);
        assert_eq!(ch_cfg.endpoints[0].zone.as_deref(), Some("az-a"));
        assert_eq!(ch_cfg.endpoints[1].host, "ch2");
        assert_eq!(
            ch_cfg.endpoints[1].http_port, 8123,
            "an entry omitting its port inherits clickhouse.http_port"
        );
        assert_eq!(ch_cfg.endpoints[1].zone.as_deref(), Some("az-b"));
    }

    #[test]
    fn bootstrap_conn_config_from_inherits_endpoints_and_zone() {
        use pulsus_config::ChServerEntry;
        let mut config = Config::default();
        config.clickhouse.servers = vec![ChServerEntry {
            host: "ch1".to_string(),
            http_port: Some(8123),
            zone: Some("az-a".to_string()),
        }];
        config.availability_zone = Some("az-a".to_string());
        let ch_cfg = bootstrap_conn_config_from(&config);
        assert_eq!(ch_cfg.database, "default");
        assert_eq!(ch_cfg.endpoints.len(), 1);
        assert_eq!(ch_cfg.local_zone.as_deref(), Some("az-a"));
    }

    /// Issue #114: `consistency_from` maps each of the four keys both ways
    /// (default off; overrides carried), and `conn_config_from` populates
    /// `ChConnConfig.consistency` — the production carrier proven with no
    /// live server.
    #[test]
    fn consistency_from_maps_each_field_and_conn_config_carries_it() {
        // Default: all-off (byte-for-byte pre-#114).
        let default = consistency_from(&Config::default());
        assert_eq!(default, ConsistencyConfig::default());
        assert_eq!(default.insert_quorum, 0);
        assert!(default.insert_quorum_parallel);
        assert_eq!(
            default.insert_quorum_timeout,
            std::time::Duration::from_secs(120)
        );
        assert!(!default.select_sequential_consistency);

        // Overrides carried through, including the HumanDuration -> Duration.
        let mut config = Config::default();
        config.clickhouse.insert_quorum = 3;
        config.clickhouse.insert_quorum_parallel = false;
        config.clickhouse.insert_quorum_timeout =
            pulsus_config::HumanDuration(std::time::Duration::from_secs(90));
        config.clickhouse.select_sequential_consistency = true;

        let c = consistency_from(&config);
        assert_eq!(c.insert_quorum, 3);
        assert!(!c.insert_quorum_parallel);
        assert_eq!(c.insert_quorum_timeout, std::time::Duration::from_secs(90));
        assert!(c.select_sequential_consistency);

        // The production carrier: conn_config_from populates the field.
        assert_eq!(conn_config_from(&config).consistency, c);
        // bootstrap inherits it via `..conn_config_from`.
        assert_eq!(bootstrap_conn_config_from(&config).consistency, c);
    }

    #[test]
    fn conn_config_from_maps_proto_variants() {
        let mut config = Config::default();
        config.clickhouse.proto = pulsus_config::ChProto::Https;
        assert_eq!(conn_config_from(&config).proto, ChProto::Https);
    }

    #[test]
    fn conn_config_from_maps_pool_size_and_timeout() {
        let mut config = Config::default();
        config.clickhouse.pool_size = 16;
        let ch_cfg = conn_config_from(&config);
        assert_eq!(ch_cfg.pool_size, 16);
        assert_eq!(ch_cfg.query_timeout, config.query_timeout.0);
    }

    #[test]
    fn bootstrap_conn_config_from_binds_the_default_database_not_the_target_one() {
        let mut config = Config::default();
        config.clickhouse.database = "pulsus".to_string();
        let ch_cfg = bootstrap_conn_config_from(&config);
        assert_eq!(ch_cfg.database, "default");
    }

    #[test]
    fn bootstrap_conn_config_from_still_maps_the_shared_fields() {
        let mut config = Config::default();
        config.clickhouse.proto = pulsus_config::ChProto::Https;
        let ch_cfg = bootstrap_conn_config_from(&config);
        assert_eq!(ch_cfg.proto, ChProto::Https);
        assert_eq!(ch_cfg.server, config.clickhouse.server);
    }

    #[test]
    fn schema_params_from_maps_the_target_database_and_cluster() {
        let mut config = Config::default();
        config.clickhouse.database = "pulsus".to_string();
        config.cluster = Some("prod".to_string());
        let params = schema_params_from(&config);
        assert_eq!(params.db, "pulsus");
        assert_eq!(params.cluster.as_deref(), Some("prod"));
    }

    #[test]
    fn engine_config_from_uses_base_table_names_when_unclustered() {
        let config = Config::default();
        let engine_cfg = engine_config_from(&config);
        assert_eq!(engine_cfg.streams_idx, "log_streams_idx");
        assert_eq!(engine_cfg.streams, "log_streams");
        assert_eq!(engine_cfg.samples, "log_samples");
        assert_eq!(engine_cfg.rollup_table, "log_metrics_5s");
    }

    #[test]
    fn engine_config_from_uses_dist_table_names_when_clustered() {
        let config = Config {
            cluster: Some("prod".to_string()),
            ..Config::default()
        };
        let engine_cfg = engine_config_from(&config);
        assert_eq!(engine_cfg.streams_idx, "log_streams_idx_dist");
        assert_eq!(engine_cfg.streams, "log_streams_dist");
        assert_eq!(engine_cfg.samples, "log_samples_dist");
        assert_eq!(engine_cfg.rollup_table, "log_metrics_5s_dist");
    }

    #[test]
    fn writer_tables_from_uses_base_table_names_when_unclustered() {
        let config = Config::default();
        let tables = writer_tables_from(&config);
        assert_eq!(&*tables.samples, "log_samples");
        assert_eq!(&*tables.streams, "log_streams");
    }

    #[test]
    fn writer_tables_from_uses_dist_table_names_when_clustered() {
        let config = Config {
            cluster: Some("prod".to_string()),
            ..Config::default()
        };
        let tables = writer_tables_from(&config);
        assert_eq!(&*tables.samples, "log_samples_dist");
        assert_eq!(&*tables.streams, "log_streams_dist");
    }

    #[test]
    fn metric_writer_tables_from_uses_base_table_names_when_unclustered() {
        let config = Config::default();
        let tables = metric_writer_tables_from(&config);
        assert_eq!(&*tables.samples, "metric_samples");
        assert_eq!(&*tables.series, "metric_series");
        assert_eq!(&*tables.metadata, "metric_metadata");
        assert_eq!(&*tables.hist_samples, "metric_hist_samples");
    }

    #[test]
    fn metric_writer_tables_from_uses_dist_table_names_when_clustered_except_metadata() {
        let config = Config {
            cluster: Some("prod".to_string()),
            ..Config::default()
        };
        let tables = metric_writer_tables_from(&config);
        assert_eq!(&*tables.samples, "metric_samples_dist");
        assert_eq!(&*tables.series, "metric_series_dist");
        assert_eq!(
            &*tables.metadata, "metric_metadata",
            "metric_metadata is a global catalog table and must never carry a _dist suffix"
        );
        assert_eq!(&*tables.hist_samples, "metric_hist_samples_dist");
    }

    #[test]
    fn trace_writer_tables_from_uses_base_table_names_when_unclustered() {
        let config = Config::default();
        let tables = trace_writer_tables_from(&config);
        assert_eq!(&*tables.spans, "trace_spans");
        assert_eq!(&*tables.attrs, "trace_attrs_idx");
    }

    #[test]
    fn trace_writer_tables_from_uses_dist_table_names_when_clustered() {
        let config = Config {
            cluster: Some("prod".to_string()),
            ..Config::default()
        };
        let tables = trace_writer_tables_from(&config);
        assert_eq!(&*tables.spans, "trace_spans_dist");
        assert_eq!(&*tables.attrs, "trace_attrs_idx_dist");
    }

    #[test]
    fn engine_config_from_maps_the_rollup_resolution_and_scan_budget() {
        let config = Config::default();
        let engine_cfg = engine_config_from(&config);
        assert_eq!(
            engine_cfg.rollup_res_ns,
            config.log_rollup_resolution.0.as_nanos() as u64
        );
        assert_eq!(
            engine_cfg.scan_budget_bytes,
            config.reader.logql_scan_budget_bytes.0
        );
        assert_eq!(engine_cfg.max_streams, pulsus_read::DEFAULT_MAX_STREAMS);
    }

    /// Issue M6-09: `reader.logql_pipeline_scan_factor` maps into
    /// `EngineConfig.pipeline_scan_factor` — the default 10 and an
    /// override both survive the production carrier.
    #[test]
    fn engine_config_from_maps_the_pipeline_scan_factor() {
        let mut config = Config::default();
        assert_eq!(engine_config_from(&config).pipeline_scan_factor, 10);
        config.reader.logql_pipeline_scan_factor = 3;
        assert_eq!(engine_config_from(&config).pipeline_scan_factor, 3);
    }

    #[test]
    fn label_cache_config_from_uses_the_base_series_table_when_unclustered() {
        let config = Config::default();
        let cfg = label_cache_config_from(&config);
        assert_eq!(cfg.series_table, "metric_series");
    }

    #[test]
    fn label_cache_config_from_uses_the_dist_series_table_when_clustered() {
        let config = Config {
            cluster: Some("prod".to_string()),
            ..Config::default()
        };
        let cfg = label_cache_config_from(&config);
        assert_eq!(cfg.series_table, "metric_series_dist");
    }

    #[test]
    fn metrics_config_from_uses_base_table_names_when_unclustered() {
        let config = Config::default();
        let cfg = metrics_config_from(&config);
        assert_eq!(cfg.samples_table, "metric_samples");
        assert_eq!(cfg.series_table, "metric_series");
        assert_eq!(cfg.metadata_table, "metric_metadata");
    }

    #[test]
    fn metrics_config_from_uses_dist_table_names_when_clustered_except_metadata() {
        let config = Config {
            cluster: Some("prod".to_string()),
            ..Config::default()
        };
        let cfg = metrics_config_from(&config);
        assert_eq!(cfg.samples_table, "metric_samples_dist");
        assert_eq!(cfg.series_table, "metric_series_dist");
        assert_eq!(
            cfg.metadata_table, "metric_metadata",
            "metric_metadata is a global catalog table and must never carry a _dist suffix"
        );
    }

    /// Issue #65 (M6-02): `reader.promql_experimental_functions` maps
    /// into `MetricsConfig.experimental_functions`, in both flag states.
    #[test]
    fn metrics_config_from_maps_the_experimental_functions_flag_both_states() {
        let mut config = Config::default();
        assert!(!metrics_config_from(&config).experimental_functions);
        config.reader.promql_experimental_functions = true;
        assert!(metrics_config_from(&config).experimental_functions);
    }

    /// Issue #85 (M6-08c): `reader.promql_max_metric_fanout` maps into
    /// `MetricsConfig.max_metric_fanout` — the adjudicated 1000 default
    /// and an override both survive the production carrier.
    #[test]
    fn metrics_config_from_maps_the_metric_fanout_cap() {
        let mut config = Config::default();
        assert_eq!(metrics_config_from(&config).max_metric_fanout, 1_000);
        config.reader.promql_max_metric_fanout = 250;
        assert_eq!(metrics_config_from(&config).max_metric_fanout, 250);
    }

    /// Issue #89 (retroactive re-review): `reader.promql_max_cache_scan`
    /// maps into `MetricsConfig.max_cache_scan` — the 200_000 default and
    /// an override both survive the production carrier.
    #[test]
    fn metrics_config_from_maps_the_cache_scan_budget() {
        let mut config = Config::default();
        assert_eq!(metrics_config_from(&config).max_cache_scan, 200_000);
        config.reader.promql_max_cache_scan = 500;
        assert_eq!(metrics_config_from(&config).max_cache_scan, 500);
    }

    /// Issue #65 (M6-02) plan v2 Δ4: the hermetic production-path
    /// composition — the *real* production functions, chained exactly as
    /// `MetricsEngine::query_inner` chains them
    /// (`ReaderConfig -> metrics_config_from -> MetricQueryParams::
    /// plan_params -> pulsus_promql::plan`), with no engine/ChClient.
    /// Flag off: a named experimental rejection before any I/O could
    /// happen. Flag on: the query plans to `ScalarFn::MaxOf`.
    #[test]
    fn promql_experimental_flag_reaches_plan_through_the_production_composition() {
        use pulsus_read::MetricQueryParams;

        let expr = pulsus_promql::parse("max_of(1, 1)").expect("parse");
        let qp = MetricQueryParams {
            start_ms: 0,
            end_ms: 0,
            step_ms: 0,
        };

        // Flag off (the default): rejected by name at plan time.
        let config = Config::default();
        let mc = metrics_config_from(&config);
        let pp = qp.plan_params(mc.experimental_functions);
        match pulsus_promql::plan(&expr, pp) {
            Err(pulsus_promql::PromqlError::Unsupported { construct }) => assert!(
                construct.contains("max_of") && construct.contains("experimental"),
                "rejection must name the function and the gate, got {construct:?}"
            ),
            other => panic!("expected Unsupported with the flag off, got {other:?}"),
        }

        // Flag on: plans to the experimental scalar function.
        let mut config = Config::default();
        config.reader.promql_experimental_functions = true;
        let mc = metrics_config_from(&config);
        let pp = qp.plan_params(mc.experimental_functions);
        let plan = pulsus_promql::plan(&expr, pp).expect("plan with the flag on");
        assert!(
            matches!(
                plan.root,
                pulsus_promql::PlanExpr::ScalarFn {
                    func: pulsus_promql::ScalarFn::MaxOf,
                    ..
                }
            ),
            "expected a ScalarFn::MaxOf root, got {:?}",
            plan.root
        );
    }

    #[test]
    fn trace_read_config_from_uses_the_base_tables_when_unclustered() {
        let config = Config::default();
        let cfg = trace_read_config_from(&config);
        assert_eq!(cfg.spans_table, "trace_spans");
        assert_eq!(cfg.attrs_table, "trace_attrs_idx");
        assert_eq!(cfg.catalog_table, "trace_tag_catalog");
        assert!(!cfg.distributed);
    }

    #[test]
    fn trace_read_config_from_uses_the_dist_tables_and_flag_when_clustered() {
        let config = Config {
            cluster: Some("prod".to_string()),
            ..Config::default()
        };
        let cfg = trace_read_config_from(&config);
        assert_eq!(cfg.spans_table, "trace_spans_dist");
        assert_eq!(cfg.attrs_table, "trace_attrs_idx_dist");
        assert!(cfg.distributed);
    }

    #[test]
    fn trace_read_config_from_never_dist_suffixes_the_tag_catalog_when_clustered() {
        let config = Config {
            cluster: Some("prod".to_string()),
            ..Config::default()
        };
        let cfg = trace_read_config_from(&config);
        assert_eq!(
            cfg.catalog_table, "trace_tag_catalog",
            "trace_tag_catalog is a global catalog table (Replication::Global, no _dist \
             wrapper) and must never carry a _dist suffix"
        );
    }

    #[test]
    fn trace_read_config_from_maps_the_search_budgets() {
        let config = Config::default();
        let cfg = trace_read_config_from(&config);
        assert_eq!(cfg.max_candidates, config.reader.traceql_max_candidates);
        assert_eq!(cfg.scan_budget_rows, config.reader.traceql_scan_budget_rows);
        assert_eq!(cfg.skip_unavailable_shards, config.skip_unavailable_shards);
    }

    #[test]
    fn label_cache_config_from_maps_the_reader_settings() {
        let config = Config::default();
        let cfg = label_cache_config_from(&config);
        assert_eq!(cfg.db, config.clickhouse.database);
        assert_eq!(
            cfg.bucket_ms,
            config.reader.series_activity_bucket.0.as_millis() as i64
        );
        assert_eq!(
            cfg.window_ms,
            config.reader.cache_window.0.as_millis() as i64
        );
        assert_eq!(cfg.cache_max_series, config.reader.cache_max_series);
        assert_eq!(cfg.ttl, config.reader.cache_ttl.0);
        assert_eq!(
            cfg.staleness_multiplier,
            pulsus_read::DEFAULT_STALENESS_MULTIPLIER
        );
    }
}
