//! The M0 migration and materialized-view catalog: every DDL block is
//! transcribed byte-for-byte from docs/schemas.md (the authoritative
//! source), with `<db>` promoted to the `{{db}}` token, retention literals
//! promoted to `{{retention_days}}`, an `{{on_cluster}}` token placed right
//! after every object name, and `IF NOT EXISTS` added to every `CREATE
//! TABLE` (docs/schemas.md is itself the single-node, unclustered form; the
//! clustering/idempotency machinery is layered on by `render::render`, not
//! baked into these strings).
//!
//! Ordering is db → base tables (bookkeeping first, so `schema_migrations`
//! exists before any later migration tries to record itself) → `_dist`
//! wrappers, per the architect plan. M0 covers only the logs + metrics
//! families (docs/schemas.md §2.1, §3.1, §6); traces/profiles/rules and the
//! metric tiers are out of scope (issue #5).

use crate::render::Family;

/// One migration: `IF NOT EXISTS`-idempotent DDL, applied at most once per
/// `id` and recorded in `schema_migrations`. `family` drives whether the
/// table also gets a `_dist` wrapper in clustered mode; `Ddl::Dist` entries
/// generate that wrapper from [`crate::render::dist_ddl_template`] rather
/// than carrying their own literal template, so every `_dist` in a family
/// is byte-identical by construction (docs/schemas.md §7 invariant).
pub struct Migration {
    pub id: u32,
    /// Unqualified table/view name template (may contain
    /// `{{log_rollup_suffix}}`, never `{{db}}` or `{{on_cluster}}`).
    pub name: &'static str,
    pub family: Option<Family>,
    pub ddl: Ddl,
    /// Identity/drift-detection strategy (issue #5 fix plan F1). See
    /// [`MigrationScope`].
    pub scope: MigrationScope,
    /// Zoo-path replication scope in clustered mode (issue #5 fix plan F2).
    /// See [`Replication`].
    pub replication: Replication,
}

pub enum Ddl {
    /// A literal DDL template, rendered as-is by `render::render`.
    Static(&'static str),
    /// The `_dist` `Distributed` wrapper for this migration's `name` +
    /// `family`, generated (never hand-duplicated) from
    /// `render::dist_ddl_template`. Only applied when clustering is
    /// enabled; skipped (not recorded) otherwise — see `controller::reconcile`.
    Dist,
}

/// How `controller::apply_migration` decides whether an already-recorded
/// migration id is current (issue #5 fix plan F1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationScope {
    /// Append-only, checksummed over [`crate::render::identity_ddl`] (which
    /// excludes mutable operational config — retention/storage policy — from
    /// the checksum surface). A mismatch on an existing id is a hard
    /// [`crate::error::SchemaError::MigrationDrift`]: the shipped template
    /// changed structurally after it was already applied.
    Checksum,
    /// The config-resolved `name` itself IS the identity (e.g.
    /// `log_metrics_<res>`, whose name changes with
    /// `PULSUS_LOG_ROLLUP_RESOLUTION`). Gated purely on `system.tables`
    /// existence, like a materialized view: absent ⇒ create + record;
    /// present ⇒ no-op. Never drifts — a resolution change creates a new,
    /// differently-named object and leaves the old one (and its data) in
    /// place, with an orphan warning naming it.
    ConfigName,
}

/// Which zoo-path replica set a clustered `Replicated*` table joins (issue
/// #5 fix plan F2, docs/schemas.md §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replication {
    /// One replica set per shard: `/clickhouse/tables/{shard}/<db>.<table>`.
    /// The default for family-sharded data tables.
    PerShard,
    /// One replica set spanning every shard:
    /// `/clickhouse/tables/all/<db>.<table>` — catalog/bookkeeping tables
    /// (`schema_migrations`, `mv_checksums`, `metric_metadata`) that must
    /// read/write identically regardless of which shard's connection is
    /// used. Requires cluster-unique `{replica}` macros (docs/schemas.md
    /// §7).
    Global,
}

/// A materialized view reconciled by checksum + `system.tables` presence
/// (docs/schemas.md §2.2/§6, issue #5 plan amendment 1) rather than
/// append-only migration bookkeeping: an MV's definition depends on
/// config-derived rendering (`{{log_rollup_suffix}}`/`{{log_rollup_ns}}`),
/// and a materialized view can be safely `DROP`+`CREATE`d (it holds no data
/// of its own) where a base table cannot.
pub struct MvDef {
    pub name: &'static str,
    pub tmpl: &'static str,
}

pub const MIGRATIONS: &[Migration] = &[
    // --- bookkeeping first: schema_migrations must exist before any later
    // migration tries to record itself into it (chicken-and-egg, docs/schemas.md §6).
    Migration {
        id: 1,
        name: "schema_migrations",
        family: None,
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.schema_migrations{{on_cluster}} (\n\
                 id           UInt32,\n\
                 checksum     String,\n\
                 applied_at   DateTime\n\
             ) ENGINE = ReplacingMergeTree(applied_at)\n\
             ORDER BY id;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::Global,
    },
    Migration {
        id: 2,
        name: "mv_checksums",
        family: None,
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.mv_checksums{{on_cluster}} (\n\
                 mv_name     String,\n\
                 checksum    String,\n\
                 updated_at  DateTime\n\
             ) ENGINE = ReplacingMergeTree(updated_at)\n\
             ORDER BY mv_name;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::Global,
    },
    // --- metrics family (docs/schemas.md §2.1) ---
    Migration {
        id: 3,
        name: "metric_metadata",
        family: None, // catalog table: replicated to all shards, no Distributed writes (§7)
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.metric_metadata{{on_cluster}} (\n\
                 metric_name  LowCardinality(String),\n\
                 metric_type  LowCardinality(String),\n\
                 help         String,\n\
                 unit         String,\n\
                 updated_ns   Int64\n\
             ) ENGINE = ReplacingMergeTree(updated_ns)\n\
             ORDER BY metric_name;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::Global,
    },
    Migration {
        id: 4,
        name: "metric_series",
        family: Some(Family::Metrics),
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.metric_series{{on_cluster}} (\n\
                 metric_name  LowCardinality(String),\n\
                 fingerprint  UInt64  CODEC(Delta(8), ZSTD(1)),\n\
                 unix_milli   Int64   CODEC(Delta(8), ZSTD(1)),\n\
                 labels       String  CODEC(ZSTD(5))\n\
             ) ENGINE = MergeTree\n\
             PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(unix_milli))\n\
             ORDER BY (metric_name, fingerprint, unix_milli);",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    Migration {
        id: 5,
        name: "metric_samples",
        family: Some(Family::Metrics),
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.metric_samples{{on_cluster}} (\n\
                 metric_name  LowCardinality(String),\n\
                 fingerprint  UInt64   CODEC(Delta(8), ZSTD(1)),\n\
                 unix_milli   Int64    CODEC(DoubleDelta, ZSTD(1)),\n\
                 value        Float64  CODEC(Gorilla, ZSTD(1))\n\
             ) ENGINE = MergeTree\n\
             PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli))\n\
             ORDER BY (metric_name, fingerprint, unix_milli)\n\
             TTL toDateTime(fromUnixTimestamp64Milli(unix_milli)) + INTERVAL {{retention_days}} DAY DELETE\n\
             SETTINGS ttl_only_drop_parts = 1;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // --- logs family (docs/schemas.md §3.1) ---
    Migration {
        id: 6,
        name: "log_streams",
        family: Some(Family::Logs),
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.log_streams{{on_cluster}} (\n\
                 month        Date,\n\
                 fingerprint  UInt64,\n\
                 service      LowCardinality(String),\n\
                 labels       String  CODEC(ZSTD(5)),\n\
                 updated_ns   Int64\n\
             ) ENGINE = ReplacingMergeTree(updated_ns)\n\
             PARTITION BY month\n\
             ORDER BY fingerprint;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    Migration {
        id: 7,
        name: "log_streams_idx",
        family: Some(Family::Logs),
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.log_streams_idx{{on_cluster}} (\n\
                 month        Date,\n\
                 key          LowCardinality(String),\n\
                 val          String,\n\
                 fingerprint  UInt64\n\
             ) ENGINE = ReplacingMergeTree\n\
             PARTITION BY month\n\
             ORDER BY (key, val, fingerprint);",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    Migration {
        id: 8,
        name: "log_samples",
        family: Some(Family::Logs),
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.log_samples{{on_cluster}} (\n\
                 service       LowCardinality(String),\n\
                 fingerprint   UInt64,\n\
                 timestamp_ns  Int64   CODEC(DoubleDelta, ZSTD(1)),\n\
                 severity      Int8    DEFAULT 0,\n\
                 body          String  CODEC(ZSTD(1)),\n\
                 INDEX idx_body_tokens body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,\n\
                 INDEX idx_body_ngrams body TYPE ngrambf_v1(4, 32768, 3, 0) GRANULARITY 1,\n\
                 INDEX idx_severity severity TYPE minmax GRANULARITY 4\n\
             ) ENGINE = MergeTree\n\
             PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))\n\
             ORDER BY (service, fingerprint, timestamp_ns)\n\
             TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL {{retention_days}} DAY DELETE\n\
             SETTINGS ttl_only_drop_parts = 1;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // The resolved name (`log_metrics_<res>`) IS the identity — existence-
    // gated, never id-checksum-drifted (issue #5 fix plan F1): a
    // `PULSUS_LOG_ROLLUP_RESOLUTION` change creates a new, differently-named
    // table and leaves the old one (and its data) in place.
    Migration {
        id: 9,
        name: "log_metrics_{{log_rollup_suffix}}",
        family: Some(Family::Logs),
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.log_metrics_{{log_rollup_suffix}}{{on_cluster}} (\n\
                 fingerprint  UInt64,\n\
                 bucket_ns    Int64,\n\
                 count        SimpleAggregateFunction(sum, UInt64),\n\
                 bytes        SimpleAggregateFunction(sum, UInt64)\n\
             ) ENGINE = AggregatingMergeTree\n\
             PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))\n\
             ORDER BY (fingerprint, bucket_ns);",
        ),
        scope: MigrationScope::ConfigName,
        replication: Replication::PerShard,
    },
    // --- _dist wrappers (clustered mode only, docs/schemas.md §7) ---
    Migration {
        id: 10,
        name: "metric_series",
        family: Some(Family::Metrics),
        ddl: Ddl::Dist,
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    Migration {
        id: 11,
        name: "metric_samples",
        family: Some(Family::Metrics),
        ddl: Ddl::Dist,
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    Migration {
        id: 12,
        name: "log_streams",
        family: Some(Family::Logs),
        ddl: Ddl::Dist,
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    Migration {
        id: 13,
        name: "log_streams_idx",
        family: Some(Family::Logs),
        ddl: Ddl::Dist,
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    Migration {
        id: 14,
        name: "log_samples",
        family: Some(Family::Logs),
        ddl: Ddl::Dist,
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // Same existence-gated identity as id 9 (its base table): the _dist
    // wrapper's name is also config-resolved.
    Migration {
        id: 15,
        name: "log_metrics_{{log_rollup_suffix}}",
        family: Some(Family::Logs),
        ddl: Ddl::Dist,
        scope: MigrationScope::ConfigName,
        replication: Replication::PerShard,
    },
];

/// Materialized views (docs/schemas.md §3.1), reconciled separately from
/// [`MIGRATIONS`] by `controller::reconcile_mvs`.
pub const MVS: &[MvDef] = &[
    MvDef {
        name: "log_streams_idx_mv",
        tmpl: "CREATE MATERIALIZED VIEW {{db}}.log_streams_idx_mv{{on_cluster}} TO {{db}}.log_streams_idx AS\n\
               SELECT\n\
                   month,\n\
                   kv.1 AS key,\n\
                   kv.2 AS val,\n\
                   fingerprint\n\
               FROM {{db}}.log_streams\n\
               ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv;",
    },
    MvDef {
        name: "log_metrics_{{log_rollup_suffix}}_mv",
        tmpl: "CREATE MATERIALIZED VIEW {{db}}.log_metrics_{{log_rollup_suffix}}_mv{{on_cluster}} TO {{db}}.log_metrics_{{log_rollup_suffix}} AS\n\
               SELECT\n\
                   fingerprint,\n\
                   intDiv(timestamp_ns, {{log_rollup_ns}}) * {{log_rollup_ns}} AS bucket_ns,\n\
                   count() AS count,\n\
                   sum(length(body)) AS bytes\n\
               FROM {{db}}.log_samples\n\
               GROUP BY fingerprint, bucket_ns;",
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_ids_are_unique_and_ascending() {
        let ids: Vec<u32> = MIGRATIONS.iter().map(|m| m.id).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(
            ids, sorted,
            "MIGRATIONS must be listed in ascending id order"
        );
        let mut deduped = sorted.clone();
        deduped.dedup();
        assert_eq!(sorted, deduped, "migration ids must be unique");
    }

    #[test]
    fn schema_migrations_is_the_first_migration() {
        assert_eq!(MIGRATIONS[0].name, "schema_migrations");
        assert_eq!(MIGRATIONS[0].id, 1);
    }

    #[test]
    fn mv_checksums_is_created_before_any_other_bookkeeping_read() {
        assert_eq!(MIGRATIONS[1].name, "mv_checksums");
    }

    #[test]
    fn every_dist_migration_carries_a_family() {
        for m in MIGRATIONS {
            if matches!(m.ddl, Ddl::Dist) {
                assert!(
                    m.family.is_some(),
                    "dist migration {} ({}) must carry a family",
                    m.id,
                    m.name
                );
            }
        }
    }

    #[test]
    fn mvs_are_non_empty_and_named() {
        assert_eq!(MVS.len(), 2);
        for mv in MVS {
            assert!(!mv.name.is_empty());
            assert!(mv.tmpl.contains("CREATE MATERIALIZED VIEW"));
        }
    }

    /// Issue #5 fix plan F1: only the rollup table (id 9) and its `_dist`
    /// wrapper (id 15) — whose resolved name is config-derived — are
    /// existence-gated; every other migration is checksum-gated.
    #[test]
    fn only_the_rollup_migrations_are_config_name_scoped() {
        for m in MIGRATIONS {
            let expected = matches!(m.id, 9 | 15);
            assert_eq!(
                m.scope == MigrationScope::ConfigName,
                expected,
                "migration {} ({}) has unexpected scope {:?}",
                m.id,
                m.name,
                m.scope
            );
        }
    }

    /// Issue #5 fix plan F2: only the three catalog/bookkeeping tables join
    /// the shard-less, cluster-wide replica set.
    #[test]
    fn only_catalog_and_bookkeeping_migrations_are_globally_replicated() {
        for m in MIGRATIONS {
            let expected = matches!(m.id, 1..=3);
            assert_eq!(
                m.replication == Replication::Global,
                expected,
                "migration {} ({}) has unexpected replication {:?}",
                m.id,
                m.name,
                m.replication
            );
        }
    }
}
