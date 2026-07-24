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
//!
//! **Amendment policy:** migrations are append-only from the first tagged
//! release onward. In-place amendment of an already-listed migration was
//! permitted only pre-release (no tagged release, no persistent
//! deployments, CI databases created fresh per run), and issue #54's scope
//! amendment of migrations 17/18 + `trace_tag_catalog_mv` was the last such
//! amendment window (task-manager ruling on #54). Developers with a local
//! schema created before that amendment must drop and re-reconcile it —
//! the checksum drift guard ([`MigrationScope::Checksum`]) correctly
//! refuses to touch the stale tables.

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
    /// A literal DDL template with `Ddl::Static` rendering semantics, but
    /// cluster-gated exactly like `Ddl::Dist`: only applied when clustering
    /// is enabled, skipped (not recorded) otherwise. Used to `ALTER` a
    /// `_dist` `Distributed` wrapper (which only exists in clustered mode)
    /// additively — the wrapper copies columns at creation and does not
    /// inherit a base-table `ALTER`, so a column added to a family table's
    /// base must also be added to its `_dist` object. Chosen over a
    /// `cluster_only: bool` field on `Migration` so no existing `Migration`
    /// literal changes shape (issue #97).
    StaticClusterOnly(&'static str),
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
    // --- traces family (docs/schemas.md §4.1, issue #53) ---
    Migration {
        id: 16,
        name: "trace_spans",
        family: Some(Family::Traces),
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.trace_spans{{on_cluster}} (\n\
                 trace_id      FixedString(16),\n\
                 span_id       FixedString(8),\n\
                 parent_id     FixedString(8),\n\
                 name          LowCardinality(String),\n\
                 service       LowCardinality(String),\n\
                 timestamp_ns  Int64  CODEC(DoubleDelta, ZSTD(1)),\n\
                 duration_ns   Int64  CODEC(T64, ZSTD(1)),\n\
                 status_code   Int8,\n\
                 kind          Int8,\n\
                 payload_type  Int8,\n\
                 payload       String CODEC(ZSTD(3)),\n\
                 INDEX idx_duration duration_ns TYPE minmax GRANULARITY 4,\n\
                 PROJECTION service_time (\n\
                     SELECT * ORDER BY (service, timestamp_ns)\n\
                 )\n\
             ) ENGINE = MergeTree\n\
             PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))\n\
             ORDER BY (trace_id, timestamp_ns)\n\
             TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL {{retention_days}} DAY DELETE\n\
             SETTINGS ttl_only_drop_parts = 1;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    Migration {
        id: 17,
        name: "trace_attrs_idx",
        family: Some(Family::Traces),
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.trace_attrs_idx{{on_cluster}} (\n\
                 date          Date,\n\
                 key           LowCardinality(String),\n\
                 val           String,\n\
                 scope         LowCardinality(String),\n\
                 val_num       Nullable(Float64),\n\
                 timestamp_ns  Int64,\n\
                 trace_id      FixedString(16),\n\
                 span_id       FixedString(8),\n\
                 duration_ns   Int64\n\
             ) ENGINE = ReplacingMergeTree\n\
             PARTITION BY date\n\
             ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)\n\
             TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL {{retention_days}} DAY DELETE\n\
             SETTINGS ttl_only_drop_parts = 1;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // Catalog table: one cluster-wide replica set, no `_dist` wrapper —
    // tag-API reads serve from the local replica without fan-out
    // (docs/schemas.md §7, task-manager adjudication on issue #53; mirrors
    // `metric_metadata`).
    Migration {
        id: 18,
        name: "trace_tag_catalog",
        family: None,
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.trace_tag_catalog{{on_cluster}} (\n\
                 scope  LowCardinality(String),\n\
                 key    LowCardinality(String),\n\
                 val    String\n\
             ) ENGINE = ReplacingMergeTree\n\
             ORDER BY (scope, key, val);",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::Global,
    },
    Migration {
        id: 19,
        name: "trace_spans",
        family: Some(Family::Traces),
        ddl: Ddl::Dist,
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    Migration {
        id: 20,
        name: "trace_attrs_idx",
        family: Some(Family::Traces),
        ddl: Ddl::Dist,
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // --- log_samples per-entry structured metadata (issue #97) ---
    // Additive ALTERs, never a mutation of id 8's frozen CREATE (which would
    // trip `MigrationDrift` on every already-initialized deployment). A fresh
    // DB runs CREATE(id 8) then ADD COLUMN(id 21) and converges byte-
    // identically to an upgraded DB; `ADD COLUMN IF NOT EXISTS` is SQL-
    // idempotent and the recorded id prevents re-run. Pre-existing rows read
    // back the empty string (`DEFAULT ''`), which the reader treats as "no
    // structured metadata" — no data migration. The column is a canonical
    // sorted-key JSON String (the `log_streams.labels` convention,
    // docs/schemas.md §1: Map(String,String) is rejected for label-shaped
    // data), NOT a Map.
    Migration {
        id: 21,
        name: "log_samples",
        family: Some(Family::Logs),
        ddl: Ddl::Static(
            "ALTER TABLE {{db}}.log_samples{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS structured_metadata String DEFAULT '';",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // The `_dist` wrapper is a `CREATE ... AS log_samples` that copies columns
    // at creation and does NOT inherit id 21's base ALTER; in cluster mode all
    // reads/writes go through `log_samples_dist`, so it must gain the column
    // too. Cluster-gated (`StaticClusterOnly`) — skipped and unrecorded on a
    // single node, applied the first time clustering is enabled. Not runnable
    // locally (rootless podman, no cluster); CI's clustered leg is
    // authoritative.
    Migration {
        id: 22,
        name: "log_samples",
        family: Some(Family::Logs),
        ddl: Ddl::StaticClusterOnly(
            "ALTER TABLE {{db}}.log_samples{{dist_suffix}}{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS structured_metadata String DEFAULT '';",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // --- native-histogram samples (M7-A2, issue #113; A1 design #112) ---
    // A new, dedicated Metrics-family table storing native-histogram samples
    // in the Prometheus integer sparse wire form (spans + delta buckets,
    // `UInt64` counts) — lossless for BOTH the standard exponential schema
    // (−4..8) and NHCB (schema −53; NHCB populates `custom_values` and leaves
    // the negative/zero fields empty). It carries the same identity/ordering/
    // partition/TTL contract as `metric_samples` (id 5) and reuses
    // `Family::Metrics`, so its `_dist` wrapper (id 24) co-shards byte-
    // identically (`cityHash64(metric_name, fingerprint)`) with float samples
    // and `metric_series`. `metric_samples` (id 5) is NOT altered — the float
    // read path (SQL, EXPLAIN gate, id-5 checksum) cannot regress. Array
    // columns carry `CODEC(ZSTD(1))` (accepted + round-tripped on live CH
    // 24.8.14 per the A1 review; no fallback). The engine value model, OTLP
    // ingest, and histogram functions/routing are downstream (A3/A4/A5).
    Migration {
        id: 23,
        name: "metric_hist_samples",
        family: Some(Family::Metrics),
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.metric_hist_samples{{on_cluster}} (\n\
                 metric_name        LowCardinality(String),\n\
                 fingerprint        UInt64   CODEC(Delta(8), ZSTD(1)),\n\
                 unix_milli         Int64    CODEC(DoubleDelta, ZSTD(1)),\n\
                 schema             Int8     CODEC(ZSTD(1)),\n\
                 zero_threshold     Float64  CODEC(Gorilla, ZSTD(1)),\n\
                 zero_count         UInt64   CODEC(T64, ZSTD(1)),\n\
                 count              UInt64   CODEC(T64, ZSTD(1)),\n\
                 sum                Float64  CODEC(Gorilla, ZSTD(1)),\n\
                 pos_span_offsets   Array(Int32)   CODEC(ZSTD(1)),\n\
                 pos_span_lengths   Array(UInt32)  CODEC(ZSTD(1)),\n\
                 pos_bucket_deltas  Array(Int64)   CODEC(ZSTD(1)),\n\
                 neg_span_offsets   Array(Int32)   CODEC(ZSTD(1)),\n\
                 neg_span_lengths   Array(UInt32)  CODEC(ZSTD(1)),\n\
                 neg_bucket_deltas  Array(Int64)   CODEC(ZSTD(1)),\n\
                 custom_values      Array(Float64) CODEC(ZSTD(1))\n\
             ) ENGINE = MergeTree\n\
             PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli))\n\
             ORDER BY (metric_name, fingerprint, unix_milli)\n\
             TTL toDateTime(fromUnixTimestamp64Milli(unix_milli)) + INTERVAL {{retention_days}} DAY DELETE\n\
             SETTINGS ttl_only_drop_parts = 1;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    Migration {
        id: 24,
        name: "metric_hist_samples",
        family: Some(Family::Metrics),
        ddl: Ddl::Dist,
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // --- metric_series per-series value-type routing signal (M7-A2, #113) ---
    // Additive ALTERs (the id 21/22 `structured_metadata` precedent), never a
    // mutation of id 4's frozen `metric_series` CREATE. `value_type`: 0 =
    // float, 1 = histogram; pre-M7 rows read back 0 (float) — no data
    // migration. This is the per-series float/histogram/mixed routing signal:
    // A4 writes it (LRU key gains `value_type`), A5 reads it (the type-mask
    // co-load) — A2 only adds the column. An ALTER carries no `ENGINE =`
    // clause, so it passes through `render` unchanged in cluster mode.
    Migration {
        id: 25,
        name: "metric_series",
        family: Some(Family::Metrics),
        ddl: Ddl::Static(
            "ALTER TABLE {{db}}.metric_series{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS value_type UInt8 DEFAULT 0;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // The `_dist` wrapper is a `CREATE ... AS metric_series` that copies
    // columns at creation and does NOT inherit id 25's base ALTER; in cluster
    // mode all reads/writes go through `metric_series_dist`, so it must gain
    // the column too. Cluster-gated (`StaticClusterOnly`) — skipped and
    // unrecorded on a single node, applied the first time clustering is
    // enabled. Not runnable locally (rootless podman, no cluster); CI's
    // clustered leg is authoritative. Mirrors id 22.
    Migration {
        id: 26,
        name: "metric_series",
        family: Some(Family::Metrics),
        ddl: Ddl::StaticClusterOnly(
            "ALTER TABLE {{db}}.metric_series{{dist_suffix}}{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS value_type UInt8 DEFAULT 0;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // --- metric_hist_samples counter-reset hint (issue #125) ---
    // Additive ALTER (the id 25/26 `value_type` precedent), never a mutation
    // of id 23's frozen `metric_hist_samples` CREATE. `counter_reset_hint`
    // stores the Prometheus hint byte (0 = Unknown, 1 = CounterReset,
    // 2 = NotCounterReset, 3 = Gauge); pre-#125 rows read back 0 = Unknown —
    // semantically exact, no data migration. Ingest writes 0 today (OTLP
    // exponential histograms carry no monotonicity signal and delta
    // temporality is rejected at the seam); Gauge-capable ingest is #140.
    Migration {
        id: 27,
        name: "metric_hist_samples",
        family: Some(Family::Metrics),
        ddl: Ddl::Static(
            "ALTER TABLE {{db}}.metric_hist_samples{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS counter_reset_hint UInt8 DEFAULT 0;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // The `_dist` wrapper copy of id 27 — cluster-gated (`StaticClusterOnly`),
    // the id 26 precedent: `CREATE ... AS metric_hist_samples` copies columns
    // at creation and does not inherit the base ALTER.
    Migration {
        id: 28,
        name: "metric_hist_samples",
        family: Some(Family::Metrics),
        ddl: Ddl::StaticClusterOnly(
            "ALTER TABLE {{db}}.metric_hist_samples{{dist_suffix}}{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS counter_reset_hint UInt8 DEFAULT 0;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // --- log patterns (M7-C3, issue #171) ---
    // A fourth Logs-family table storing ingest-extracted log templates,
    // batch-pre-aggregated per `(fingerprint, bucket_ns, pattern)` in the
    // writer (NOT a materialized view — extraction is Rust, not SQL — and NOT
    // per-line rows, which would double `log_samples` write volume). The
    // `SimpleAggregateFunction(sum, UInt64)` `count` merges across batches,
    // shards, replicas, and retries because the template identity is a pure
    // function of the line (`patterns::extract_template`). `ORDER BY
    // (fingerprint, bucket_ns, pattern)` (bucket_ns BEFORE pattern) so a
    // bounded time range prunes at the PK level inside each fingerprint's key
    // range, not only via daily partitions — the `/api/logs/v1/patterns` read
    // (a `fingerprint IN (...)` + `bucket_ns` window) engages the PK prefix.
    // Same tokenized delete-TTL / part-level drops as `log_samples` (id 8):
    // patterns follow raw retention, being a drilldown over raw lines. The
    // fixed 10s ingest bucket is a code constant
    // (`patterns::PATTERN_BUCKET_NS`), not a config-resolved name, so this is
    // checksum-gated like every other structural table (unlike the
    // config-named `log_metrics_<res>`, id 9).
    Migration {
        id: 29,
        name: "log_patterns",
        family: Some(Family::Logs),
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.log_patterns{{on_cluster}} (\n\
                 fingerprint  UInt64,\n\
                 bucket_ns    Int64,\n\
                 pattern      String  CODEC(ZSTD(1)),\n\
                 count        SimpleAggregateFunction(sum, UInt64)\n\
             ) ENGINE = AggregatingMergeTree\n\
             PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))\n\
             ORDER BY (fingerprint, bucket_ns, pattern)\n\
             TTL toDateTime(fromUnixTimestamp64Nano(bucket_ns)) + INTERVAL {{retention_days}} DAY DELETE\n\
             SETTINGS ttl_only_drop_parts = 1;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // The `_dist` wrapper co-shards `log_patterns` with
    // `log_samples`/`log_streams`/`log_metrics_<res>` on the same
    // `cityHash64(fingerprint)` Logs-family expression (render.rs), so the
    // fingerprint-pruned read is the already-graduated shard-local shape.
    Migration {
        id: 30,
        name: "log_patterns",
        family: Some(Family::Logs),
        ddl: Ddl::Dist,
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // --- service-graph edge ledger (M7-E1, issue #173) ---
    // The Zipkin shared-span signal promoted onto `trace_spans` (issue #173
    // Fix 1): an additive `ADD COLUMN IF NOT EXISTS` (the id 21/22 & 25/26
    // precedent), never a mutation of id 16's frozen CREATE. `shared` = 1
    // iff the span carried the `zipkin.shared = "true"` attribute at OTLP
    // parse time (documented wire contract); pre-#173 rows read back 0 — no
    // data migration. The edge MV (`trace_edges_mv`) keys a shared server
    // half by its OWN `span_id` (Zipkin's shared model: both RPC sides carry
    // the same id) so it pairs with — and only with — its same-id client
    // twin, never fabricating an edge from a coincidental `parent_id`
    // collision. `ADD COLUMN` on `trace_spans` rebuilds its `service_time`
    // `SELECT *` projection in place (live-gated in `live_traces.rs`, the
    // first ALTER precedent over a projection-carrying table).
    Migration {
        id: 31,
        name: "trace_spans",
        family: Some(Family::Traces),
        ddl: Ddl::Static(
            "ALTER TABLE {{db}}.trace_spans{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS shared UInt8 DEFAULT 0;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // The `_dist` wrapper is a `CREATE ... AS trace_spans` that copies columns
    // at creation and does NOT inherit id 31's base ALTER; in cluster mode all
    // reads/writes go through `trace_spans_dist`, so it must gain the column
    // too. Cluster-gated (`StaticClusterOnly`) — skipped and unrecorded on a
    // single node, applied the first time clustering is enabled. Mirrors
    // id 22/26.
    Migration {
        id: 32,
        name: "trace_spans",
        family: Some(Family::Traces),
        ddl: Ddl::StaticClusterOnly(
            "ALTER TABLE {{db}}.trace_spans{{dist_suffix}}{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS shared UInt8 DEFAULT 0;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // `trace_edges` is a ReplacingMergeTree half-row LEDGER (issue #173, plan
    // v2 core redesign): one narrow row per edge-relevant span (its own plain
    // `timestamp_ns`, no `SimpleAggregateFunction`), and the directed
    // `client -> server` edge is assembled at QUERY time by a within-type
    // equi-join of the deduped server halves to the deduped client halves —
    // so pair completion is a pure function of the stored half-row multiset,
    // never of background-merge progress. `side` leads the ORDER BY so each
    // read subquery PK-prunes to its own half (a second prune besides the
    // daily-partition MinMax prune); Zipkin shared spans (SERVER stored under
    // the client's `span_id`) never collapse because `side` is in the dedup
    // key. `pair_id` is the edge's CLIENT-side span id (`span_id` for a
    // client/shared-server half, else `parent_id`), the single-valued join
    // key. `conn_type` (`'rpc'`|`'messaging'`) is derived from the emitting
    // span's OWN kind, so only CLIENT(3)->SERVER(2) and PRODUCER(4)->
    // CONSUMER(5) can pair. Same tokenized delete-TTL / part-level drops as
    // `trace_spans` (superseded at runtime by `apply_ttl`'s saturating form).
    Migration {
        id: 33,
        name: "trace_edges",
        family: Some(Family::Traces),
        ddl: Ddl::Static(
            "CREATE TABLE IF NOT EXISTS {{db}}.trace_edges{{on_cluster}} (\n\
                 date          Date,\n\
                 side          UInt8,\n\
                 trace_id      FixedString(16),\n\
                 span_id       FixedString(8),\n\
                 pair_id       FixedString(8),\n\
                 conn_type     LowCardinality(String),\n\
                 timestamp_ns  Int64  CODEC(DoubleDelta, ZSTD(1)),\n\
                 service       LowCardinality(String),\n\
                 duration_ns   Int64  CODEC(T64, ZSTD(1)),\n\
                 failed        UInt8\n\
             ) ENGINE = ReplacingMergeTree\n\
             PARTITION BY date\n\
             ORDER BY (side, trace_id, span_id)\n\
             TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL {{retention_days}} DAY DELETE\n\
             SETTINGS ttl_only_drop_parts = 1;",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // The `trace_edges` `_dist` wrapper co-shards with
    // `trace_spans`/`trace_attrs_idx` on the same `cityHash64(trace_id)`
    // Traces-family expression (render.rs), so every joinable client/server
    // pair is shard-local (both halves share `trace_id`) and the read join
    // executes per shard.
    Migration {
        id: 34,
        name: "trace_edges",
        family: Some(Family::Traces),
        ddl: Ddl::Dist,
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // --- span status message (M7-TQ5, issue #184) ---
    // The OTLP `Status.message` promoted onto `trace_spans` so the
    // `statusMessage` / `span:statusMessage` intrinsic is queryable — an
    // additive `ADD COLUMN IF NOT EXISTS` (the id 21/22 & 25/26 & 31/32
    // precedent), never a mutation of id 16's frozen CREATE. Pre-#184 rows
    // read back `''` — no data migration. Like id 31, the `ADD COLUMN` on
    // `trace_spans` rebuilds its `service_time` `SELECT *` projection in
    // place (live-gated in `live_traces.rs`).
    Migration {
        id: 35,
        name: "trace_spans",
        family: Some(Family::Traces),
        ddl: Ddl::Static(
            "ALTER TABLE {{db}}.trace_spans{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS status_message String DEFAULT '';",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // The `_dist` wrapper copy of id 35 — cluster-gated (`StaticClusterOnly`),
    // skipped and unrecorded on a single node, applied the first time
    // clustering is enabled. Mirrors id 22/26/32.
    Migration {
        id: 36,
        name: "trace_spans",
        family: Some(Family::Traces),
        ddl: Ddl::StaticClusterOnly(
            "ALTER TABLE {{db}}.trace_spans{{dist_suffix}}{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS status_message String DEFAULT '';",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // --- instrumentation scope name/version (M7-TQ, issue #192) ---
    // The OTLP `InstrumentationScope` `name`/`version` promoted onto
    // `trace_spans` so the `instrumentation.name`/`instrumentation.version`
    // intrinsics are queryable and `compare()` has a per-span source — the
    // additive `ADD COLUMN IF NOT EXISTS` pattern (id 21/22 & 25/26 & 31/32 &
    // 35/36 precedent), never a mutation of id 16's frozen CREATE. Pre-#192
    // rows read back `''` — no data migration. Unlike id 35's free-text
    // `status_message`, instrumentation library name/version are genuinely
    // low-cardinality (a handful per deployment) and match the sibling
    // `trace_spans.name`/`service` `LowCardinality(String)` columns. Like id
    // 31/35, the `ADD COLUMN` rebuilds the `service_time` `SELECT *`
    // projection in place (live-gated in `live_traces.rs`).
    Migration {
        id: 37,
        name: "trace_spans",
        family: Some(Family::Traces),
        ddl: Ddl::Static(
            "ALTER TABLE {{db}}.trace_spans{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS scope_name LowCardinality(String) DEFAULT '',\n\
             ADD COLUMN IF NOT EXISTS scope_version LowCardinality(String) DEFAULT '';",
        ),
        scope: MigrationScope::Checksum,
        replication: Replication::PerShard,
    },
    // The `_dist` wrapper copy of id 37 — cluster-gated (`StaticClusterOnly`),
    // skipped and unrecorded on a single node, applied the first time
    // clustering is enabled. Mirrors id 22/26/32/36.
    Migration {
        id: 38,
        name: "trace_spans",
        family: Some(Family::Traces),
        ddl: Ddl::StaticClusterOnly(
            "ALTER TABLE {{db}}.trace_spans{{dist_suffix}}{{on_cluster}}\n\
             ADD COLUMN IF NOT EXISTS scope_name LowCardinality(String) DEFAULT '',\n\
             ADD COLUMN IF NOT EXISTS scope_version LowCardinality(String) DEFAULT '';",
        ),
        scope: MigrationScope::Checksum,
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
    // Fires per shard in cluster mode; every shard writes the same Global
    // replica set and `ReplacingMergeTree(scope, key, val)` +
    // duplicate-tolerant reads absorb the redundancy (docs/schemas.md §8) —
    // the established catalog pattern (`metric_metadata`).
    MvDef {
        name: "trace_tag_catalog_mv",
        tmpl: "CREATE MATERIALIZED VIEW {{db}}.trace_tag_catalog_mv{{on_cluster}} TO {{db}}.trace_tag_catalog AS\n\
               SELECT scope, key, val\n\
               FROM {{db}}.trace_attrs_idx;",
    },
    // The service-graph edge ledger MV (M7-E1, issue #173): a pure per-row
    // projection over `trace_spans` — no MV-side GROUP BY/join/state, so
    // ingest-time precompute is just the kind-filter plus a payload-free
    // narrowing. Emits one half-row per edge-relevant span: `side` 0 for a
    // client half (kind 3|4), 1 for a server half (kind 2|5). `pair_id` is
    // the CLIENT-side span id — a client/producer half and a *shared* server
    // half key by their own `span_id` (Zipkin's shared model), a non-shared
    // server half by `parent_id`. `conn_type` is `'rpc'` (kind 2|3) or
    // `'messaging'` (kind 4|5), from the emitting span's own kind, so the
    // query-time join admits only CLIENT->SERVER and PRODUCER->CONSUMER.
    // Root server halves (zero parent) are excluded — but a shared server
    // half is admitted even with a zero parent (its pair key is its own id).
    // Fires per shard on distributed-forwarded inserts; halves co-shard via
    // `cityHash64(trace_id)`, so pairing is shard-local complete (§7).
    MvDef {
        name: "trace_edges_mv",
        tmpl: "CREATE MATERIALIZED VIEW {{db}}.trace_edges_mv{{on_cluster}} TO {{db}}.trace_edges AS\n\
               SELECT\n\
                   toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,\n\
                   toUInt8(kind IN (2, 5)) AS side,\n\
                   trace_id,\n\
                   span_id,\n\
                   if(kind IN (3, 4) OR shared = 1, span_id, parent_id) AS pair_id,\n\
                   if(kind IN (2, 3), 'rpc', 'messaging') AS conn_type,\n\
                   timestamp_ns,\n\
                   service,\n\
                   duration_ns,\n\
                   toUInt8(status_code = 2) AS failed\n\
               FROM {{db}}.trace_spans\n\
               WHERE kind IN (3, 4)\n\
                  OR (kind IN (2, 5) AND (shared = 1 OR parent_id != toFixedString(unhex('0000000000000000'), 8)));",
    },
];

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::render::{self, RenderCtx};

    fn ctx() -> RenderCtx {
        RenderCtx {
            db: "pulsus".to_string(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: Duration::from_secs(5),
        }
    }

    /// Migration `id`'s `Ddl::Static` template, unrendered.
    fn static_tmpl(id: u32) -> &'static str {
        let m = MIGRATIONS
            .iter()
            .find(|m| m.id == id)
            .unwrap_or_else(|| panic!("no migration with id {id}"));
        let Ddl::Static(tmpl) = m.ddl else {
            panic!("migration {id} is not Ddl::Static");
        };
        tmpl
    }

    /// Renders migration `id`'s `Ddl::Static` template in single-node mode.
    fn rendered_static(id: u32) -> String {
        render::render(static_tmpl(id), "", &ctx(), false)
    }

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
    fn mvs_are_exactly_the_expected_set() {
        let names: Vec<&str> = MVS.iter().map(|mv| mv.name).collect();
        assert_eq!(
            names,
            [
                "log_streams_idx_mv",
                "log_metrics_{{log_rollup_suffix}}_mv",
                "trace_tag_catalog_mv",
                "trace_edges_mv",
            ],
            "MVS must contain exactly the catalog's materialized views"
        );
        for mv in MVS {
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

    /// AC1b (issue #53): trace_spans transcribes docs/schemas.md §4.1 —
    /// dual physical order (projection), duration skip index, tokenized
    /// delete-TTL, part-level TTL drops.
    #[test]
    fn trace_spans_ddl_carries_projection_index_and_tokenized_ttl() {
        let ddl = rendered_static(16);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS pulsus.trace_spans"));
        assert!(ddl.contains("PROJECTION service_time"));
        assert!(ddl.contains("SELECT * ORDER BY (service, timestamp_ns)"));
        assert!(ddl.contains("INDEX idx_duration duration_ns TYPE minmax GRANULARITY 4"));
        assert!(ddl.contains("ORDER BY (trace_id, timestamp_ns)"));
        assert!(ddl.contains(
            "TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE"
        ));
        assert!(ddl.contains("SETTINGS ttl_only_drop_parts = 1;"));
        // Tokenized, not a hard-coded literal: retention stays mutable
        // operational config, excluded from migration identity.
        assert!(static_tmpl(16).contains("INTERVAL {{retention_days}} DAY DELETE"));
    }

    /// AC1b (issue #53, plan v2 delta 1) as amended in place by issue #54
    /// (scope discriminator, last pre-release amendment window — see the
    /// module doc's amendment policy): trace_attrs_idx carries the
    /// adjudicated retention lifecycle — the same tokenized delete-TTL as
    /// trace_spans — plus the §4.1 typed-numeric column, the `scope`
    /// discriminator, and the scoped index key with `scope` AFTER `val`
    /// (preserves the proven `(key, val)` prefix pruning; scoped TraceQL
    /// fixes `scope` for near-free post-prefix time pruning).
    #[test]
    fn trace_attrs_idx_ddl_carries_scope_val_num_order_and_tokenized_ttl() {
        let ddl = rendered_static(17);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS pulsus.trace_attrs_idx"));
        assert!(ddl.contains("scope         LowCardinality(String)"));
        assert!(ddl.contains("val_num       Nullable(Float64)"));
        assert!(ddl.contains("ENGINE = ReplacingMergeTree"));
        assert!(ddl.contains("PARTITION BY date"));
        assert!(ddl.contains("ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)"));
        assert!(ddl.contains(
            "TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE"
        ));
        assert!(ddl.contains("SETTINGS ttl_only_drop_parts = 1;"));
        assert!(static_tmpl(17).contains("INTERVAL {{retention_days}} DAY DELETE"));
    }

    /// AC1b (issue #53) as amended in place by issue #54: trace_tag_catalog
    /// is a bounded catalog — deduped `(scope, key, val)` (scope-aware for
    /// T6's Tempo `/api/v2/search/tags`), no TTL, no `_dist` wrapper
    /// (Replication::Global).
    #[test]
    fn trace_tag_catalog_ddl_is_a_bounded_replacing_catalog() {
        let ddl = rendered_static(18);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS pulsus.trace_tag_catalog"));
        assert!(ddl.contains("scope  LowCardinality(String)"));
        assert!(ddl.contains("ENGINE = ReplacingMergeTree"));
        assert!(ddl.contains("ORDER BY (scope, key, val);"));
        assert!(!ddl.contains("TTL"));
        let m = MIGRATIONS.iter().find(|m| m.id == 18).expect("id 18");
        assert_eq!(m.replication, Replication::Global);
        assert!(m.family.is_none(), "catalog tables carry no family");
        assert!(
            !MIGRATIONS
                .iter()
                .any(|m| m.name == "trace_tag_catalog" && matches!(m.ddl, Ddl::Dist)),
            "trace_tag_catalog must not have a _dist wrapper"
        );
    }

    /// Issue #54 (scope amendment): the tag-catalog MV forwards the `scope`
    /// discriminator — a scope-less MV would land every row with the
    /// column's empty-string default, silently collapsing the catalog's
    /// scope dimension.
    #[test]
    fn trace_tag_catalog_mv_selects_scope_key_val() {
        let mv = MVS
            .iter()
            .find(|mv| mv.name == "trace_tag_catalog_mv")
            .expect("trace_tag_catalog_mv present");
        assert!(mv.tmpl.contains("SELECT scope, key, val"));
    }

    /// Issue #53: the `_dist` wrappers for both per-shard trace tables carry
    /// the Traces family (and therefore render `cityHash64(trace_id)` via
    /// `dist_ddl_template` — the render-side test proves the expression).
    #[test]
    fn trace_dist_migrations_carry_the_traces_family() {
        for (id, name) in [(19, "trace_spans"), (20, "trace_attrs_idx")] {
            let m = MIGRATIONS.iter().find(|m| m.id == id).expect("present");
            assert_eq!(m.name, name);
            assert!(matches!(m.ddl, Ddl::Dist));
            assert_eq!(m.family, Some(Family::Traces));
        }
    }

    /// Issue #97: the base `log_samples` structured-metadata ALTER (id 21) is
    /// an additive `ADD COLUMN IF NOT EXISTS` — never a mutation of id 8's
    /// frozen CREATE — and stores a canonical JSON String (docs/schemas.md §1
    /// convention), not a `Map`. An ALTER carries no `ENGINE = ` clause, so it
    /// passes through `render` unchanged even in cluster mode (no engine swap,
    /// no storage-policy injection).
    #[test]
    fn log_samples_structured_metadata_base_alter_is_additive_json_string() {
        let ddl = rendered_static(21);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.log_samples\n\
             ADD COLUMN IF NOT EXISTS structured_metadata String DEFAULT '';",
        );
        assert!(!ddl.contains("Map("), "must be a JSON String, not a Map");
        // Cluster mode: still a plain ALTER (no engine swap, no Replicated).
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        clustered.storage_policy = Some("hot_cold".to_string());
        let ddl_clustered = render::render(static_tmpl(21), "log_samples", &clustered, false);
        assert!(ddl_clustered.contains("ON CLUSTER 'prod'"));
        assert!(!ddl_clustered.contains("Replicated"));
        assert!(!ddl_clustered.contains("storage_policy"));
    }

    /// Issue #97: the `_dist` structured-metadata ALTER (id 22) is
    /// `StaticClusterOnly` — it targets `log_samples_dist` (via
    /// `{{dist_suffix}}`) and is cluster-gated. Rendered here directly since
    /// `rendered_static` only handles `Ddl::Static`.
    #[test]
    fn log_samples_structured_metadata_dist_alter_targets_the_dist_object() {
        let m = MIGRATIONS.iter().find(|m| m.id == 22).expect("id 22");
        assert_eq!(m.name, "log_samples");
        let Ddl::StaticClusterOnly(tmpl) = m.ddl else {
            panic!("migration 22 must be Ddl::StaticClusterOnly");
        };
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        let ddl = render::render(tmpl, "log_samples", &clustered, false);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.log_samples_dist ON CLUSTER 'prod'\n\
             ADD COLUMN IF NOT EXISTS structured_metadata String DEFAULT '';",
        );
    }

    /// Issue #97: the structured-metadata ALTERs are checksum-gated and
    /// per-shard replicated (not config-named, not global) — they inherit the
    /// same identity/replication scope as every other logs-family DDL.
    #[test]
    fn structured_metadata_alters_are_checksum_scoped_per_shard() {
        for id in [21, 22] {
            let m = MIGRATIONS.iter().find(|m| m.id == id).expect("present");
            assert_eq!(m.scope, MigrationScope::Checksum);
            assert_eq!(m.replication, Replication::PerShard);
            assert_eq!(m.family, Some(Family::Logs));
        }
    }

    /// Issue #113 (M7-A2): `metric_hist_samples` (id 23) carries the same
    /// identity/ordering/partition/TTL contract as `metric_samples` (id 5) and
    /// stores the Prometheus integer sparse wire form (spans + delta buckets,
    /// `UInt64` counts, `custom_values` for NHCB). The array columns carry
    /// `CODEC(ZSTD(1))`. Tokenized retention (mutable, excluded from identity).
    #[test]
    fn metric_hist_samples_ddl_stores_the_integer_wire_form_with_metric_samples_contract() {
        let ddl = rendered_static(23);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS pulsus.metric_hist_samples"));
        // Scalars: identity + the Prometheus integer histogram head.
        assert!(ddl.contains("metric_name        LowCardinality(String),"));
        assert!(ddl.contains("fingerprint        UInt64   CODEC(Delta(8), ZSTD(1)),"));
        assert!(ddl.contains("unix_milli         Int64    CODEC(DoubleDelta, ZSTD(1)),"));
        assert!(ddl.contains("schema             Int8     CODEC(ZSTD(1)),"));
        assert!(ddl.contains("zero_threshold     Float64  CODEC(Gorilla, ZSTD(1)),"));
        assert!(ddl.contains("zero_count         UInt64   CODEC(T64, ZSTD(1)),"));
        assert!(ddl.contains("count              UInt64   CODEC(T64, ZSTD(1)),"));
        assert!(ddl.contains("sum                Float64  CODEC(Gorilla, ZSTD(1)),"));
        // Sparse spans + delta buckets (positive, negative) + NHCB bounds,
        // every array column ZSTD-coded (A1 review confirmed on live 24.8.14).
        assert!(ddl.contains("pos_span_offsets   Array(Int32)   CODEC(ZSTD(1)),"));
        assert!(ddl.contains("pos_span_lengths   Array(UInt32)  CODEC(ZSTD(1)),"));
        assert!(ddl.contains("pos_bucket_deltas  Array(Int64)   CODEC(ZSTD(1)),"));
        assert!(ddl.contains("neg_span_offsets   Array(Int32)   CODEC(ZSTD(1)),"));
        assert!(ddl.contains("neg_span_lengths   Array(UInt32)  CODEC(ZSTD(1)),"));
        assert!(ddl.contains("neg_bucket_deltas  Array(Int64)   CODEC(ZSTD(1)),"));
        assert!(ddl.contains("custom_values      Array(Float64) CODEC(ZSTD(1))"));
        // Same identity/ordering/partition/TTL contract as metric_samples.
        assert!(ddl.contains("ENGINE = MergeTree"));
        assert!(ddl.contains("PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli))"));
        assert!(ddl.contains("ORDER BY (metric_name, fingerprint, unix_milli)"));
        assert!(ddl.contains(
            "TTL toDateTime(fromUnixTimestamp64Milli(unix_milli)) + INTERVAL 7 DAY DELETE"
        ));
        assert!(ddl.contains("SETTINGS ttl_only_drop_parts = 1;"));
        // Retention stays mutable operational config, excluded from identity.
        assert!(static_tmpl(23).contains("INTERVAL {{retention_days}} DAY DELETE"));
    }

    /// Issue #113: the `metric_hist_samples` `_dist` wrapper (id 24) carries
    /// the Metrics family, so `dist_ddl_template` renders the byte-identical
    /// `cityHash64(metric_name, fingerprint)` co-shard expression it shares
    /// with `metric_samples`/`metric_series`.
    #[test]
    fn metric_hist_samples_dist_wrapper_reuses_the_metrics_family_co_shard() {
        let m = MIGRATIONS.iter().find(|m| m.id == 24).expect("id 24");
        assert_eq!(m.name, "metric_hist_samples");
        assert!(matches!(m.ddl, Ddl::Dist));
        assert_eq!(m.family, Some(Family::Metrics));
        let tmpl = render::dist_ddl_template("metric_hist_samples", Family::Metrics);
        let out = render::render(&tmpl, "metric_hist_samples", &ctx(), false);
        assert!(out.contains("pulsus.metric_hist_samples_dist"));
        assert!(out.contains(
            "Distributed('', pulsus, metric_hist_samples, cityHash64(metric_name, fingerprint))"
        ));
    }

    /// Issue #113: the `value_type` routing signal is added to `metric_series`
    /// via an additive `ADD COLUMN IF NOT EXISTS` (id 25) — never a mutation
    /// of id 4's frozen CREATE — as `UInt8 DEFAULT 0` (0 = float; pre-M7 rows
    /// read back 0, no data migration). An ALTER carries no `ENGINE =` clause,
    /// so it passes through `render` unchanged even in cluster mode.
    #[test]
    fn metric_series_value_type_base_alter_is_additive_uint8_default_zero() {
        let ddl = rendered_static(25);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.metric_series\n\
             ADD COLUMN IF NOT EXISTS value_type UInt8 DEFAULT 0;",
        );
        // Cluster mode: still a plain ALTER (no engine swap, no Replicated).
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        clustered.storage_policy = Some("hot_cold".to_string());
        let ddl_clustered = render::render(static_tmpl(25), "metric_series", &clustered, false);
        assert!(ddl_clustered.contains("ON CLUSTER 'prod'"));
        assert!(!ddl_clustered.contains("Replicated"));
        assert!(!ddl_clustered.contains("storage_policy"));
    }

    /// Issue #113: the `value_type` `_dist` ALTER (id 26) is
    /// `StaticClusterOnly` — it targets `metric_series_dist` (via
    /// `{{dist_suffix}}`) and is cluster-gated. Mirrors id 22.
    #[test]
    fn metric_series_value_type_dist_alter_targets_the_dist_object() {
        let m = MIGRATIONS.iter().find(|m| m.id == 26).expect("id 26");
        assert_eq!(m.name, "metric_series");
        let Ddl::StaticClusterOnly(tmpl) = m.ddl else {
            panic!("migration 26 must be Ddl::StaticClusterOnly");
        };
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        let ddl = render::render(tmpl, "metric_series", &clustered, false);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.metric_series_dist ON CLUSTER 'prod'\n\
             ADD COLUMN IF NOT EXISTS value_type UInt8 DEFAULT 0;",
        );
    }

    /// Issue #113: the frozen `metric_series` (id 4) and `metric_samples`
    /// (id 5) base CREATEs are NOT mutated by A2 — `value_type` arrives only
    /// via the additive ALTER (id 25), and no histogram column touches the
    /// float samples table. Guards the read-perf-neutrality basis (id-4/id-5
    /// checksums stay byte-frozen; the live `MigrationDrift` guard enforces it
    /// end-to-end).
    #[test]
    fn frozen_metric_base_creates_are_not_mutated_by_a2() {
        assert!(
            !static_tmpl(4).contains("value_type"),
            "value_type must arrive via the additive ALTER (id 25), not id 4's CREATE"
        );
        assert!(
            !static_tmpl(5).contains("schema") && !static_tmpl(5).contains("bucket"),
            "metric_samples (id 5) must not gain any histogram column"
        );
    }

    /// Issue #113: the M7-A2 migrations inherit the same identity/replication
    /// scope as every other Metrics-family DDL — checksum-gated, per-shard
    /// replicated (never config-named, never global). Issue #125 extends the
    /// set with the `counter_reset_hint` pair (ids 27/28).
    #[test]
    fn native_histogram_migrations_are_checksum_scoped_per_shard_metrics() {
        for id in [23, 24, 25, 26, 27, 28] {
            let m = MIGRATIONS.iter().find(|m| m.id == id).expect("present");
            assert_eq!(m.scope, MigrationScope::Checksum);
            assert_eq!(m.replication, Replication::PerShard);
            assert_eq!(m.family, Some(Family::Metrics));
        }
    }

    /// Issue #125: `counter_reset_hint` is added to `metric_hist_samples` via
    /// an additive `ADD COLUMN IF NOT EXISTS` (id 27) — never a mutation of
    /// id 23's frozen CREATE — as `UInt8 DEFAULT 0` (0 = Unknown; pre-#125
    /// rows read back 0, no data migration). The id-25 `value_type`
    /// precedent shape exactly.
    #[test]
    fn hist_counter_reset_hint_base_alter_is_additive_uint8_default_zero() {
        let ddl = rendered_static(27);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.metric_hist_samples\n\
             ADD COLUMN IF NOT EXISTS counter_reset_hint UInt8 DEFAULT 0;",
        );
        // Cluster mode: still a plain ALTER (no engine swap, no Replicated).
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        clustered.storage_policy = Some("hot_cold".to_string());
        let ddl_clustered =
            render::render(static_tmpl(27), "metric_hist_samples", &clustered, false);
        assert!(ddl_clustered.contains("ON CLUSTER 'prod'"));
        assert!(!ddl_clustered.contains("Replicated"));
        assert!(!ddl_clustered.contains("storage_policy"));
    }

    /// Issue #125: the `counter_reset_hint` `_dist` ALTER (id 28) is
    /// `StaticClusterOnly` — it targets `metric_hist_samples_dist` (via
    /// `{{dist_suffix}}`) and is cluster-gated. Mirrors id 26.
    #[test]
    fn hist_counter_reset_hint_dist_alter_targets_the_dist_object() {
        let m = MIGRATIONS.iter().find(|m| m.id == 28).expect("id 28");
        assert_eq!(m.name, "metric_hist_samples");
        let Ddl::StaticClusterOnly(tmpl) = m.ddl else {
            panic!("migration 28 must be Ddl::StaticClusterOnly");
        };
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        let ddl = render::render(tmpl, "metric_hist_samples", &clustered, false);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.metric_hist_samples_dist ON CLUSTER 'prod'\n\
             ADD COLUMN IF NOT EXISTS counter_reset_hint UInt8 DEFAULT 0;",
        );
    }

    /// Issue #125: id 23's frozen `metric_hist_samples` CREATE is NOT
    /// mutated — `counter_reset_hint` arrives only via the additive ALTER
    /// (id 27), keeping the id-23 checksum byte-frozen.
    #[test]
    fn frozen_hist_base_create_is_not_mutated_by_the_hint_column() {
        assert!(
            !static_tmpl(23).contains("counter_reset_hint"),
            "counter_reset_hint must arrive via the additive ALTER (id 27), not id 23's CREATE"
        );
    }

    /// Issue #171 (M7-C3): `log_patterns` (id 29) is an AggregatingMergeTree
    /// keyed `(fingerprint, bucket_ns, pattern)` — bucket_ns BEFORE pattern
    /// (v2 finding-2 PK order) so a bounded time range prunes at the PK level
    /// inside each fingerprint's key range. Same tokenized delete-TTL /
    /// part-level drops as `log_samples`; `count` is a mergeable
    /// `SimpleAggregateFunction(sum, UInt64)`.
    #[test]
    fn log_patterns_ddl_is_a_time_pruned_aggregating_mergetree() {
        let ddl = rendered_static(29);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS pulsus.log_patterns"));
        assert!(ddl.contains("fingerprint  UInt64,"));
        assert!(ddl.contains("bucket_ns    Int64,"));
        assert!(ddl.contains("pattern      String  CODEC(ZSTD(1)),"));
        assert!(ddl.contains("count        SimpleAggregateFunction(sum, UInt64)"));
        assert!(ddl.contains("ENGINE = AggregatingMergeTree"));
        assert!(ddl.contains("PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))"));
        // v2 finding 2: bucket_ns BEFORE pattern (PK-level time pruning).
        assert!(ddl.contains("ORDER BY (fingerprint, bucket_ns, pattern)"));
        assert!(ddl.contains(
            "TTL toDateTime(fromUnixTimestamp64Nano(bucket_ns)) + INTERVAL 7 DAY DELETE"
        ));
        assert!(ddl.contains("SETTINGS ttl_only_drop_parts = 1;"));
        // Retention stays mutable operational config, excluded from identity.
        assert!(static_tmpl(29).contains("INTERVAL {{retention_days}} DAY DELETE"));
    }

    /// Issue #171: the `log_patterns` `_dist` wrapper (id 30) carries the Logs
    /// family, so `dist_ddl_template` renders the byte-identical `fingerprint`
    /// co-shard expression it shares with
    /// `log_samples`/`log_streams`/`log_metrics_<res>`.
    #[test]
    fn log_patterns_dist_wrapper_reuses_the_logs_family_co_shard() {
        let m = MIGRATIONS.iter().find(|m| m.id == 30).expect("id 30");
        assert_eq!(m.name, "log_patterns");
        assert!(matches!(m.ddl, Ddl::Dist));
        assert_eq!(m.family, Some(Family::Logs));
        assert_eq!(m.scope, MigrationScope::Checksum);
        assert_eq!(m.replication, Replication::PerShard);
        let tmpl = render::dist_ddl_template("log_patterns", Family::Logs);
        let out = render::render(&tmpl, "log_patterns", &ctx(), false);
        assert!(out.contains("pulsus.log_patterns_dist"));
        assert!(out.contains("Distributed('', pulsus, log_patterns, fingerprint)"));
    }

    /// Issue #173 (M7-E1) Fix 1: the Zipkin `shared` signal is added to
    /// `trace_spans` via an additive `ADD COLUMN IF NOT EXISTS` (id 31) —
    /// never a mutation of id 16's frozen CREATE — as `UInt8 DEFAULT 0`
    /// (pre-#173 rows read back 0, no data migration). Mirrors the id 25/26
    /// `value_type` shape. An ALTER carries no `ENGINE =` clause, so it passes
    /// through `render` unchanged even in cluster mode.
    #[test]
    fn trace_spans_shared_base_alter_is_additive_uint8_default_zero() {
        let ddl = rendered_static(31);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.trace_spans\n\
             ADD COLUMN IF NOT EXISTS shared UInt8 DEFAULT 0;",
        );
        // Cluster mode: still a plain ALTER (no engine swap, no Replicated).
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        clustered.storage_policy = Some("hot_cold".to_string());
        let ddl_clustered = render::render(static_tmpl(31), "trace_spans", &clustered, false);
        assert!(ddl_clustered.contains("ON CLUSTER 'prod'"));
        assert!(!ddl_clustered.contains("Replicated"));
        assert!(!ddl_clustered.contains("storage_policy"));
        // The frozen id-16 CREATE never gains the column (its checksum stays
        // byte-frozen).
        assert!(
            !static_tmpl(16).contains("shared"),
            "shared must arrive via the additive ALTER (id 31), not id 16's CREATE"
        );
    }

    /// Issue #173 Fix 1: the `shared` `_dist` ALTER (id 32) is
    /// `StaticClusterOnly` — it targets `trace_spans_dist` (via
    /// `{{dist_suffix}}`) and is cluster-gated. Mirrors id 22/26.
    #[test]
    fn trace_spans_shared_dist_alter_targets_the_dist_object() {
        let m = MIGRATIONS.iter().find(|m| m.id == 32).expect("id 32");
        assert_eq!(m.name, "trace_spans");
        let Ddl::StaticClusterOnly(tmpl) = m.ddl else {
            panic!("migration 32 must be Ddl::StaticClusterOnly");
        };
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        let ddl = render::render(tmpl, "trace_spans", &clustered, false);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.trace_spans_dist ON CLUSTER 'prod'\n\
             ADD COLUMN IF NOT EXISTS shared UInt8 DEFAULT 0;",
        );
    }

    /// Issue #173 (M7-E1): `trace_edges` (id 33) is a ReplacingMergeTree
    /// half-row ledger keyed `(side, trace_id, span_id)` — `side` leads so
    /// each per-half read subquery PK-prunes. Partitioned by the plain `date`
    /// column, tokenized delete-TTL / part-level drops like `trace_spans`.
    #[test]
    fn trace_edges_ddl_is_a_side_keyed_replacing_ledger_with_tokenized_ttl() {
        let ddl = rendered_static(33);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS pulsus.trace_edges"));
        assert!(ddl.contains("side          UInt8,"));
        assert!(ddl.contains("pair_id       FixedString(8),"));
        assert!(ddl.contains("conn_type     LowCardinality(String),"));
        assert!(ddl.contains("failed        UInt8"));
        assert!(ddl.contains("ENGINE = ReplacingMergeTree"));
        assert!(ddl.contains("PARTITION BY date"));
        assert!(ddl.contains("ORDER BY (side, trace_id, span_id)"));
        assert!(ddl.contains(
            "TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE"
        ));
        assert!(ddl.contains("SETTINGS ttl_only_drop_parts = 1;"));
        // Retention stays mutable operational config, excluded from identity.
        assert!(static_tmpl(33).contains("INTERVAL {{retention_days}} DAY DELETE"));
    }

    /// Issue #173: the `trace_edges` `_dist` wrapper (id 34) carries the
    /// Traces family, so `dist_ddl_template` renders the byte-identical
    /// `cityHash64(trace_id)` co-shard expression it shares with
    /// `trace_spans`/`trace_attrs_idx` — pairing is shard-local.
    #[test]
    fn trace_edges_dist_wrapper_reuses_the_traces_family_co_shard() {
        let m = MIGRATIONS.iter().find(|m| m.id == 34).expect("id 34");
        assert_eq!(m.name, "trace_edges");
        assert!(matches!(m.ddl, Ddl::Dist));
        assert_eq!(m.family, Some(Family::Traces));
        assert_eq!(m.scope, MigrationScope::Checksum);
        assert_eq!(m.replication, Replication::PerShard);
        let tmpl = render::dist_ddl_template("trace_edges", Family::Traces);
        let out = render::render(&tmpl, "trace_edges", &ctx(), false);
        assert!(out.contains("pulsus.trace_edges_dist"));
        assert!(out.contains("Distributed('', pulsus, trace_edges, cityHash64(trace_id))"));
    }

    /// Issue #173: the edge MV is a pure per-row projection over
    /// `trace_spans` — the `side` discriminator, the `pair_id` expression
    /// (shared server halves key by their own `span_id`), the per-kind
    /// `conn_type` map, and the kind filter with the shared-OR-zero-parent
    /// admission clause (root non-shared server halves excluded).
    #[test]
    fn trace_edges_mv_projects_side_pair_id_conn_type_and_the_shared_admission() {
        let mv = MVS
            .iter()
            .find(|mv| mv.name == "trace_edges_mv")
            .expect("trace_edges_mv present");
        assert!(mv.tmpl.contains("TO {{db}}.trace_edges AS"));
        assert!(mv.tmpl.contains("toUInt8(kind IN (2, 5)) AS side"));
        assert!(
            mv.tmpl
                .contains("if(kind IN (3, 4) OR shared = 1, span_id, parent_id) AS pair_id")
        );
        assert!(
            mv.tmpl
                .contains("if(kind IN (2, 3), 'rpc', 'messaging') AS conn_type")
        );
        assert!(mv.tmpl.contains("toUInt8(status_code = 2) AS failed"));
        assert!(mv.tmpl.contains("FROM {{db}}.trace_spans"));
        assert!(mv.tmpl.contains(
            "WHERE kind IN (3, 4)\n\
                  OR (kind IN (2, 5) AND (shared = 1 OR parent_id != toFixedString(unhex('0000000000000000'), 8)));"
        ));
    }

    /// Issue #173: the two new trace-family migrations (the `shared` ALTERs)
    /// and the `trace_edges` table + `_dist` are checksum-gated, per-shard
    /// replicated, and carry the Traces family — never config-named, never
    /// global.
    #[test]
    fn service_graph_migrations_are_checksum_scoped_per_shard_traces() {
        // Issue #184 extends the trace-family set with the `status_message`
        // ALTER pair (ids 35/36); issue #192 with the instrumentation
        // `scope_name`/`scope_version` ALTER pair (ids 37/38).
        for id in [31, 32, 33, 34, 35, 36, 37, 38] {
            let m = MIGRATIONS.iter().find(|m| m.id == id).expect("present");
            assert_eq!(m.scope, MigrationScope::Checksum);
            assert_eq!(m.replication, Replication::PerShard);
            assert_eq!(m.family, Some(Family::Traces));
        }
    }

    /// Issue #184 (M7-TQ5): OTLP `Status.message` is added to `trace_spans`
    /// via an additive `ADD COLUMN IF NOT EXISTS` (id 35) — never a mutation
    /// of id 16's frozen CREATE — as `String DEFAULT ''` (pre-#184 rows read
    /// back `''`, no data migration). Mirrors the id 31 `shared` shape.
    #[test]
    fn trace_spans_status_message_base_alter_is_additive_string_default_empty() {
        let ddl = rendered_static(35);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.trace_spans\n\
             ADD COLUMN IF NOT EXISTS status_message String DEFAULT '';",
        );
        // Cluster mode: still a plain ALTER (no engine swap, no Replicated).
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        clustered.storage_policy = Some("hot_cold".to_string());
        let ddl_clustered = render::render(static_tmpl(35), "trace_spans", &clustered, false);
        assert!(ddl_clustered.contains("ON CLUSTER 'prod'"));
        assert!(!ddl_clustered.contains("Replicated"));
        assert!(!ddl_clustered.contains("storage_policy"));
        // The frozen id-16 CREATE never gains the column (its checksum stays
        // byte-frozen).
        assert!(
            !static_tmpl(16).contains("status_message"),
            "status_message must arrive via the additive ALTER (id 35), not id 16's CREATE"
        );
    }

    /// Issue #184: the `status_message` `_dist` ALTER (id 36) is
    /// `StaticClusterOnly` — it targets `trace_spans_dist` (via
    /// `{{dist_suffix}}`) and is cluster-gated. Mirrors id 32.
    #[test]
    fn trace_spans_status_message_dist_alter_targets_the_dist_object() {
        let m = MIGRATIONS.iter().find(|m| m.id == 36).expect("id 36");
        assert_eq!(m.name, "trace_spans");
        let Ddl::StaticClusterOnly(tmpl) = m.ddl else {
            panic!("migration 36 must be Ddl::StaticClusterOnly");
        };
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        let ddl = render::render(tmpl, "trace_spans", &clustered, false);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.trace_spans_dist ON CLUSTER 'prod'\n\
             ADD COLUMN IF NOT EXISTS status_message String DEFAULT '';",
        );
    }

    /// Issue #192: OTLP `InstrumentationScope` `name`/`version` are added to
    /// `trace_spans` via an additive `ADD COLUMN IF NOT EXISTS` (id 37) —
    /// never a mutation of id 16's frozen CREATE — as
    /// `LowCardinality(String) DEFAULT ''` (pre-#192 rows read back `''`, no
    /// data migration). Matches the sibling `trace_spans.name`/`service`
    /// low-cardinality columns; deliberately not id 35's free-text `String`.
    #[test]
    fn trace_spans_scope_name_version_base_alter_is_additive_lowcard_default_empty() {
        let ddl = rendered_static(37);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.trace_spans\n\
             ADD COLUMN IF NOT EXISTS scope_name LowCardinality(String) DEFAULT '',\n\
             ADD COLUMN IF NOT EXISTS scope_version LowCardinality(String) DEFAULT '';",
        );
        // Cluster mode: still a plain ALTER (no engine swap, no Replicated).
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        clustered.storage_policy = Some("hot_cold".to_string());
        let ddl_clustered = render::render(static_tmpl(37), "trace_spans", &clustered, false);
        assert!(ddl_clustered.contains("ON CLUSTER 'prod'"));
        assert!(!ddl_clustered.contains("Replicated"));
        assert!(!ddl_clustered.contains("storage_policy"));
        // The frozen id-16 CREATE never gains the columns (its checksum stays
        // byte-frozen).
        assert!(
            !static_tmpl(16).contains("scope_name"),
            "scope_name must arrive via the additive ALTER (id 37), not id 16's CREATE"
        );
    }

    /// Issue #192: the `scope_name`/`scope_version` `_dist` ALTER (id 38) is
    /// `StaticClusterOnly` — it targets `trace_spans_dist` (via
    /// `{{dist_suffix}}`) and is cluster-gated. Mirrors id 36.
    #[test]
    fn trace_spans_scope_name_version_dist_alter_targets_the_dist_object() {
        let m = MIGRATIONS.iter().find(|m| m.id == 38).expect("id 38");
        assert_eq!(m.name, "trace_spans");
        let Ddl::StaticClusterOnly(tmpl) = m.ddl else {
            panic!("migration 38 must be Ddl::StaticClusterOnly");
        };
        let mut clustered = ctx();
        clustered.cluster = Some("prod".to_string());
        let ddl = render::render(tmpl, "trace_spans", &clustered, false);
        assert_eq!(
            ddl,
            "ALTER TABLE pulsus.trace_spans_dist ON CLUSTER 'prod'\n\
             ADD COLUMN IF NOT EXISTS scope_name LowCardinality(String) DEFAULT '',\n\
             ADD COLUMN IF NOT EXISTS scope_version LowCardinality(String) DEFAULT '';",
        );
    }

    /// Issue #5 fix plan F2 (+ issue #53): only the catalog/bookkeeping
    /// tables — `schema_migrations`, `mv_checksums`, `metric_metadata`, and
    /// `trace_tag_catalog` — join the shard-less, cluster-wide replica set.
    #[test]
    fn only_catalog_and_bookkeeping_migrations_are_globally_replicated() {
        for m in MIGRATIONS {
            let expected = matches!(m.id, 1..=3 | 18);
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
