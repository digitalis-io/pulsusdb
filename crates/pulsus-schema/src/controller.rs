//! `run_init` / `reconcile`: the schema controller's entry points.
//!
//! Public API takes an already-connected [`ChClient`] plus [`SchemaParams`]
//! (task-manager resolution #4 on issue #5: the `Config` → `ChConnConfig`
//! mapping lives exactly once, in `pulsus-server`, which builds the pool
//! anyway for #6; this crate stays connection-agnostic and takes only
//! plain, already-derived data).
//!
//! Data flow (single-node): `CREATE DATABASE` → migrations (id order, `IF
//! NOT EXISTS`, + bookkeeping) → MV reconcile (checksum + existence) →
//! `apply_ttl`. Clustered: the same list, engines swapped + `ON CLUSTER` +
//! `_dist` wrappers appended (docs/schemas.md §7).

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChError, Idempotency, QuerySettings, Row};

use crate::bookkeeping::{
    checksum_hex, find_migration, find_mv_checksum, record_migration, upsert_mv_checksum,
};
use crate::catalog::{Ddl, MIGRATIONS, MVS, MigrationScope, Replication};
use crate::error::SchemaError;
use crate::render::{self, RenderCtx};

/// Config-derived rendering context and public `run_init`/`reconcile`
/// parameter (see [`crate::render::RenderCtx`] for field docs — the two are
/// the same type so there is exactly one config-shaped struct in this
/// crate).
pub type SchemaParams = RenderCtx;

/// Refuses `--mode init` together with `PULSUS_SKIP_DDL=1` (task-manager
/// resolution #1 on issue #5): contradictory intent, since init exists to
/// run DDL. Pure and side-effect-free so `pulsus-server` can call this
/// *before* building a `ChClient` — no need to attempt a ClickHouse
/// connection just to refuse.
pub fn guard_skip_ddl_in_init(skip_ddl: bool) -> Result<(), SchemaError> {
    if skip_ddl {
        return Err(SchemaError::SkipDdlInInit);
    }
    Ok(())
}

/// Parses a ClickHouse `SELECT version()` string (e.g. `26.3.17.110`) and
/// refuses anything older than 26.3 (docs/schemas.md §8). Pure and
/// injectable (task-manager resolution #3 on issue #5) so refusal messages
/// are unit-tested without a live server; `run_init` supplies the real
/// server-reported string.
pub fn check_version(version: &str) -> Result<(), SchemaError> {
    let mut parts = version.trim().split('.');
    let major: u32 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| SchemaError::Version(version.to_string()))?;
    let minor: u32 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| SchemaError::Version(version.to_string()))?;
    if (major, minor) < (26, 3) {
        return Err(SchemaError::UnsupportedVersion {
            found: version.to_string(),
        });
    }
    Ok(())
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct VersionRow {
    v: String,
}

/// Fetches the connected server's `version()` string.
///
/// Public because the startup name check gates on the version before it asks
/// which names are present: a name this build sends that arrived in a later
/// server version is absent *because* the server is too old, and the version
/// is the one an operator can act on (issue #603 code review round 7).
pub async fn server_version(client: &ChClient) -> Result<String, SchemaError> {
    let mut stream = client
        .query_stream::<VersionRow>("SELECT version() AS v", &QuerySettings::new())
        .await?;
    match stream.next().await {
        Some(Ok(row)) => Ok(row.v),
        Some(Err(e)) => Err(e.into()),
        None => Err(SchemaError::Version(
            "empty result from SELECT version()".to_string(),
        )),
    }
}

/// The full `--mode init` pipeline: version gate → reconcile (migrations +
/// MVs) → apply TTL once. Idempotent — a second run against an
/// already-initialized database is a no-op past the version check.
pub async fn run_init(client: &ChClient, params: &SchemaParams) -> Result<(), SchemaError> {
    let version = server_version(client).await?;
    check_version(&version)?;
    reconcile(client, params).await?;
    apply_ttl(client, params).await?;
    Ok(())
}

/// Creates the database, then applies every migration in [`MIGRATIONS`]
/// (id order), then reconciles every materialized view in [`MVS`].
pub async fn reconcile(client: &ChClient, ctx: &RenderCtx) -> Result<(), SchemaError> {
    let db_ddl = render::render(
        "CREATE DATABASE IF NOT EXISTS {{db}}{{on_cluster}};",
        "",
        ctx,
        false, // no ENGINE clause; global is a no-op here
    );
    client
        .execute(&db_ddl, &QuerySettings::new(), Idempotency::Idempotent)
        .await?;

    for m in MIGRATIONS {
        apply_migration(client, ctx, m).await?;
    }

    reconcile_mvs(client, ctx).await
}

/// True for DDL that only exists in clustered mode — the `_dist`
/// `Distributed` wrapper (`Ddl::Dist`) and cluster-only `_dist` ALTERs
/// (`Ddl::StaticClusterOnly`, issue #97). Such a migration is skipped
/// entirely (never attempted, never recorded) on a single node.
fn is_cluster_only(ddl: &Ddl) -> bool {
    matches!(ddl, Ddl::Dist | Ddl::StaticClusterOnly(_))
}

async fn apply_migration(
    client: &ChClient,
    ctx: &RenderCtx,
    m: &crate::catalog::Migration,
) -> Result<(), SchemaError> {
    // `_dist` wrappers (and cluster-only `_dist` ALTERs) only exist in
    // clustered mode; skip entirely (never attempted, never recorded) when no
    // cluster is configured — the id stays reserved and gets applied the first
    // time clustering is enabled.
    if is_cluster_only(&m.ddl) && ctx.cluster.is_none() {
        return Ok(());
    }

    let name = render::render_name(m.name, ctx);
    let tmpl = match &m.ddl {
        Ddl::Static(tmpl) | Ddl::StaticClusterOnly(tmpl) => (*tmpl).to_string(),
        Ddl::Dist => {
            let family = m
                .family
                .expect("catalog invariant: every Ddl::Dist migration carries a family");
            render::dist_ddl_template(&name, family)
        }
    };
    let global = matches!(m.replication, Replication::Global);
    let rendered = render::render(&tmpl, &name, ctx, global);

    // The actual created *object*'s name: `name` itself for `Ddl::Static`,
    // but `{name}{dist_suffix}` for `Ddl::Dist` (the `_dist` wrapper is a
    // distinct object from the table it wraps) — existence checks and
    // orphan scans must key on this, not on `name` (which, for a `Ddl::Dist`
    // rollup entry, is the *base* table's name and would already exist from
    // its own migration, silently no-op-ing the wrapper's creation).
    let object_name = match &m.ddl {
        // `StaticClusterOnly` ALTERs a `_dist` object, but under
        // `MigrationScope::Checksum` `object_name` is only consumed by the
        // `Ddl::Dist` orphan/existence scans — never for a Checksum ALTER —
        // so its value is inert here (issue #97 plan v2 delta 3).
        Ddl::Static(_) | Ddl::StaticClusterOnly(_) => name.clone(),
        Ddl::Dist => format!("{name}{}", ctx.dist_suffix),
    };

    match m.scope {
        MigrationScope::Checksum => {
            // Checksummed over `identity_ddl`, not `rendered`: mutable
            // operational config (retention/storage policy, issue #5 fix
            // plan F1) must not change a structurally-unchanged migration's
            // identity. The executed statement is still the full `rendered`
            // DDL, so a fresh `CREATE` still gets the real current values.
            let identity = render::identity_ddl(&tmpl, &name, ctx, global);
            let checksum = checksum_hex(&identity);
            match find_migration(client, ctx, m.id).await? {
                Some(row) if row.checksum == checksum => Ok(()), // already applied and current: true no-op
                Some(_) => Err(SchemaError::MigrationDrift { id: m.id }),
                None => {
                    client
                        .execute(&rendered, &QuerySettings::new(), Idempotency::Idempotent)
                        .await?;
                    record_migration(client, ctx, m.id, &checksum).await
                }
            }
        }
        MigrationScope::ConfigName => {
            apply_config_name_migration(client, ctx, m, &object_name, &rendered).await
        }
    }
}

/// Applies a [`MigrationScope::ConfigName`] migration (issue #5 fix plan
/// F1): the resolved `object_name` itself is the identity, so this is gated
/// purely on `system.tables` existence — like an MV, never by comparing
/// checksums against the recorded id, and never `MigrationDrift`. Absent ⇒
/// create + record; present ⇒ no-op. A resolution change therefore creates
/// a new, differently-named object and leaves any prior one (and its data)
/// in place; `warn_orphaned_rollup_siblings` names the orphan.
async fn apply_config_name_migration(
    client: &ChClient,
    ctx: &RenderCtx,
    m: &crate::catalog::Migration,
    object_name: &str,
    rendered: &str,
) -> Result<(), SchemaError> {
    if table_exists(client, ctx, object_name).await? {
        return Ok(());
    }
    client
        .execute(rendered, &QuerySettings::new(), Idempotency::Idempotent)
        .await?;
    // The checksum is recorded for audit visibility only (`schema_migrations`
    // stays append-only) — it is never read back for drift comparison on
    // this scope.
    let checksum = checksum_hex(rendered);
    record_migration(client, ctx, m.id, &checksum).await?;
    let kind = if matches!(m.ddl, Ddl::Dist) {
        RollupObjectKind::Dist
    } else {
        RollupObjectKind::Table
    };
    warn_orphaned_rollup_siblings(client, ctx, object_name, kind).await
}

/// The kind of `log_metrics_*` object a sibling-orphan scan is looking for
/// (issue #5 fix plan F1) — each config-named migration/MV only warns about
/// siblings of its own kind, since each kind is reconciled independently.
#[derive(Clone, Copy)]
enum RollupObjectKind {
    /// The base rollup table, e.g. `log_metrics_5s`.
    Table,
    /// Its `_dist` `Distributed` wrapper, e.g. `log_metrics_5s_dist`.
    Dist,
    /// Its materialized view, e.g. `log_metrics_5s_mv`.
    Mv,
}

impl RollupObjectKind {
    fn matches(self, name: &str) -> bool {
        match self {
            RollupObjectKind::Dist => name.ends_with("_dist"),
            RollupObjectKind::Mv => name.ends_with("_mv"),
            RollupObjectKind::Table => !name.ends_with("_dist") && !name.ends_with("_mv"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            RollupObjectKind::Table => "rollup table",
            RollupObjectKind::Dist => "rollup _dist table",
            RollupObjectKind::Mv => "rollup materialized view",
        }
    }
}

/// One name read off a catalogue: a `system.tables` row here, and a
/// `system.settings`/`system.merge_tree_settings`/`system.functions` row for
/// [`absent_server_names`] (issue #603).
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct NameRow {
    name: String,
}

/// Lists every `system.tables` name in `ctx.db` starting with `prefix`.
async fn list_tables_with_prefix(
    client: &ChClient,
    ctx: &RenderCtx,
    prefix: &str,
) -> Result<Vec<String>, SchemaError> {
    let escaped_db = ctx.db.replace('\'', "''");
    let escaped_prefix = prefix
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('\'', "''");
    let sql = format!(
        "SELECT name FROM system.tables WHERE database = '{escaped_db}' AND name LIKE '{escaped_prefix}%' ORDER BY name"
    );
    let mut stream = client
        .query_stream::<NameRow>(&sql, &QuerySettings::new())
        .await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?.name);
    }
    Ok(out)
}

/// Pure "who to warn about" decision: given every `log_metrics_*` name in
/// `system.tables` and the currently-resolved `keep` name, returns the ones
/// of the same `kind` that are orphaned (present, not `keep`). Split out
/// from the live `tracing::warn!` wrapper below so the selection logic is
/// unit-tested without a live server.
fn orphaned_rollup_siblings<'a>(
    siblings: &'a [String],
    keep: &str,
    kind: RollupObjectKind,
) -> Vec<&'a str> {
    siblings
        .iter()
        .filter(|s| s.as_str() != keep && kind.matches(s))
        .map(String::as_str)
        .collect()
}

/// Warns (`tracing::warn!`, migrated from `eprintln!` now that issue #6 has
/// wired the subscriber, per task-manager resolution on issue #5) about any
/// `log_metrics_*` object of `kind` left behind by a prior
/// `PULSUS_LOG_ROLLUP_RESOLUTION` value. Data objects are never auto-dropped
/// (issue #5 fix plan F1) — this is purely an operator-visible heads-up so
/// orphaned storage doesn't go unnoticed.
async fn warn_orphaned_rollup_siblings(
    client: &ChClient,
    ctx: &RenderCtx,
    keep: &str,
    kind: RollupObjectKind,
) -> Result<(), SchemaError> {
    let siblings = list_tables_with_prefix(client, ctx, "log_metrics_").await?;
    for sibling in orphaned_rollup_siblings(&siblings, keep, kind) {
        tracing::warn!(
            "pulsus-schema: orphaned {} {}.{sibling} left in place after a \
             PULSUS_LOG_ROLLUP_RESOLUTION change (current is {}.{keep}); data is retained, not \
             auto-dropped",
            kind.label(),
            ctx.db,
            ctx.db
        );
    }
    Ok(())
}

/// Reconciles every materialized view: recreated when EITHER the rendered
/// checksum differs from `mv_checksums` OR the object is absent from
/// `system.tables` (issue #5 plan amendment 1 — crash-safety). Strict order
/// per view: `DROP VIEW IF EXISTS` → `CREATE MATERIALIZED VIEW` → checksum
/// upsert LAST, so a crash at any point leaves the existence/checksum check
/// failing on the next run and self-heals rather than masking a missing
/// view behind a stale-current checksum.
async fn reconcile_mvs(client: &ChClient, ctx: &RenderCtx) -> Result<(), SchemaError> {
    for mv in MVS {
        let name = render::render_name(mv.name, ctx);
        // MVs carry no `ENGINE = ` clause, so `global` never affects their
        // rendering — passed `false` for consistency with `render`'s API.
        let rendered = render::render(mv.tmpl, &name, ctx, false);
        let checksum = checksum_hex(&rendered);

        let recorded = find_mv_checksum(client, ctx, &name).await?;
        let exists = table_exists(client, ctx, &name).await?;
        let current = recorded.as_deref() == Some(checksum.as_str()) && exists;
        if current {
            continue;
        }

        let full_name = format!("{}.{name}", ctx.db);
        let on_cluster = match &ctx.cluster {
            Some(c) => format!(" ON CLUSTER '{}'", c.replace('\'', "''")),
            None => String::new(),
        };
        client
            .execute(
                &format!("DROP VIEW IF EXISTS {full_name}{on_cluster}"),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await?;
        client
            .execute(&rendered, &QuerySettings::new(), Idempotency::Idempotent)
            .await?;
        upsert_mv_checksum(client, ctx, &name, &checksum).await?;

        // The rollup MV's resolved name is config-derived
        // (`log_metrics_<res>_mv`); the fixed-name `log_streams_idx_mv` is
        // not (issue #5 fix plan F1) and never has orphan siblings by
        // construction.
        if mv.name.contains("{{log_rollup_suffix}}") {
            warn_orphaned_rollup_siblings(client, ctx, &name, RollupObjectKind::Mv).await?;
        }
    }
    Ok(())
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExistsRow {
    hit: u8,
}

/// True if `name` appears in `system.tables` for `ctx.db`.
async fn table_exists(client: &ChClient, ctx: &RenderCtx, name: &str) -> Result<bool, SchemaError> {
    let escaped_db = ctx.db.replace('\'', "''");
    let escaped_name = name.replace('\'', "''");
    let sql = format!(
        "SELECT 1 AS hit FROM system.tables WHERE database = '{escaped_db}' AND name = '{escaped_name}' LIMIT 1"
    );
    let mut stream = client
        .query_stream::<ExistsRow>(&sql, &QuerySettings::new())
        .await?;
    match stream.next().await {
        Some(Ok(_)) => Ok(true),
        Some(Err(e)) => Err(e.into()),
        None => Ok(false),
    }
}

/// The `apply_ttl` statement templates. A module-level constant (not a
/// local) so the unit tests below pin the rendered ALTER text (issues
/// #131 AC9 / #137 AC1).
///
/// Every TTL expression is the saturating form (issue #131 Resolution C
/// for the trace tables; issue #137 extends it to the metric/log tables):
/// `toDateTime(least(<seconds> + {{retention_days}} * 86400, 4294967295))`
/// where `<seconds>` is `intDiv(timestamp_ns, 1000000000)` for the
/// nanosecond tables (`log_samples`, `trace_spans`, `trace_attrs_idx`),
/// `intDiv(bucket_ns, 1000000000)` for `log_patterns` (its nanosecond
/// column is `bucket_ns`), and `intDiv(unix_milli, 1000)` for the
/// millisecond tables
/// (`metric_samples`, `metric_hist_samples`). The arithmetic is Int64
/// (max operand sum ≈ 3.71e14 at `retention_days = u32::MAX`, far below
/// `i64::MAX`), clamped to `u32::MAX` **before** `toDateTime`, so the
/// expression cannot wrap in the 32-bit DateTime domain for any stored row
/// under any `retention_days` value — pre-fix, a row whose
/// `floor(seconds) + retention_days*86400` exceeded `u32::MAX` wrapped to
/// a ~1970-epoch expiry and its part became drop-eligible immediately
/// (`ttl_only_drop_parts = 1`). For rows below the clamp the expiry
/// instant is bit-identical to the previous
/// `toDateTime(fromUnixTimestamp64Nano/Milli(...)) + INTERVAL N DAY`
/// form — `intDiv` truncation equals that form's floor only for
/// timestamps `>= 0`, which every ingest gate guarantees (pre-1970 is
/// rejected on every path, issues #8/#126). A row's effective expiry is
/// `min(seconds + retention_days*86400, 4294967295)` (docs/schemas.md
/// §2.1/§3.1/§4.1). The tables' CREATE DDL is untouched (byte-frozen —
/// the checksum identity surface excludes TTL drift, and this ALTER
/// lawfully supersedes the CREATE-time TTL from `run_init` before ingest
/// serves).
///
/// `metric_hist_samples` was absent from this list until issue #137
/// (its TTL was render-time-static from migration 23's CREATE, so a
/// `PULSUS_RETENTION_DAYS` change did not propagate to it); its pair is
/// deliberately appended LAST so an operator-managed schema lacking the
/// table cannot block the eight pre-existing statements (rotation
/// warns-and-continues).
const TTL_STMTS: [&str; 18] = [
    "ALTER TABLE {{db}}.metric_samples{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(unix_milli, 1000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.metric_samples{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    "ALTER TABLE {{db}}.log_samples{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(timestamp_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.log_samples{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    "ALTER TABLE {{db}}.trace_spans{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(timestamp_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.trace_spans{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    "ALTER TABLE {{db}}.trace_attrs_idx{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(timestamp_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.trace_attrs_idx{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    "ALTER TABLE {{db}}.metric_hist_samples{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(unix_milli, 1000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.metric_hist_samples{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    // `trace_edges` (M7-E1, issue #173) — the service-graph half-row ledger's
    // `timestamp_ns` is a plain nanosecond column, so it carries the same
    // saturating row-granular delete-TTL as `trace_spans`/`trace_attrs_idx`.
    // Appended LAST so an operator-managed schema lacking the table cannot
    // block the pre-existing statements (rotation warns-and-continues), the
    // #137 `metric_hist_samples` precedent.
    "ALTER TABLE {{db}}.trace_edges{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(timestamp_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.trace_edges{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    // `log_patterns` (M7-C3, issue #171) — the drain-pattern rollup's `bucket_ns`
    // is a plain nanosecond column, so it carries the same saturating row-granular
    // delete-TTL as `log_samples` (nanosecond scale, `intDiv(bucket_ns, ...)`).
    // Absent from this list until issue #187 (its TTL was render-time-static from
    // migration 29's CREATE, so a `PULSUS_RETENTION_DAYS` change did not propagate
    // to it). Appended LAST so an operator-managed schema lacking the table cannot
    // block the pre-existing statements (rotation warns-and-continues), the
    // #137 `metric_hist_samples` / #173 `trace_edges` precedent.
    "ALTER TABLE {{db}}.log_patterns{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(bucket_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.log_patterns{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    // The two derived trace tables (issue #560), appended LAST — the #137 /
    // #173 / #187 precedent. `trace_recent`'s TTL reads `ts_max`, the
    // newest span in the (bucket, trace) row, not `date`: a `date` TTL
    // would expire a whole partition at midnight of `date + N` and
    // under-retain a span written at 23:59 by almost a day.
    "ALTER TABLE {{db}}.trace_recent{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(ts_max, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.trace_recent{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    "ALTER TABLE {{db}}.trace_error_spans{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(timestamp_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.trace_error_spans{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
];

/// Applies the current `{{retention_days}}`-derived TTL ([`TTL_STMTS`]) to
/// every retained table (docs/schemas.md §2.1/§2.4/§3.1/§4.1): the raw
/// metric/log sample tables, `metric_hist_samples` (added by issue #137,
/// which also closes its retention-propagation gap), plus both trace
/// tables (`trace_attrs_idx` is time-scoped derived data — task-manager
/// adjudication on issue #53; `trace_tag_catalog` is a bounded catalog
/// and carries no TTL). `ALTER
/// TABLE ... MODIFY TTL` is naturally idempotent (re-applying the same
/// expression is a no-op), so this is safe both from `run_init` (applied
/// once) and [`crate::rotation::spawn_rotation`] (applied on every tick, so
/// a changed `PULSUS_RETENTION_DAYS` propagates without a restart).
///
/// `ttl_only_drop_parts` is a per-table `MergeTree` *engine* setting, not a
/// query-level one — `MODIFY TTL <expr> SETTINGS ttl_only_drop_parts = 1`
/// in one statement is rejected by the server (`UNKNOWN_SETTING`: it tries
/// to apply the name as a query setting). It is instead reasserted with its
/// own `MODIFY SETTING` statement, immediately after the TTL change, so an
/// operator who manually altered it away is corrected on the next rotation
/// tick too.
pub async fn apply_ttl(client: &ChClient, ctx: &RenderCtx) -> Result<(), SchemaError> {
    for stmt in TTL_STMTS
        .iter()
        .chain(METRIC_LANDING_STMTS)
        .chain(LOG_LANDING_STMTS)
        .chain(TRACE_LANDING_STMTS)
        .chain(if ctx.cluster.is_some() {
            CLUSTER_DEDUP_SECONDS_STMTS
        } else {
            &[]
        })
    {
        let rendered = render::substitute_tokens(stmt, ctx);
        client
            .execute(&rendered, &QuerySettings::new(), Idempotency::Idempotent)
            .await?;
    }
    Ok(())
}

/// The metrics landing table's own delete-TTL, plus the block-deduplication
/// window every table on the metrics write path carries (issue #603).
///
/// Appended to the statements [`apply_ttl`] itself runs rather than applied
/// by a function of its own, so `run_init`, [`crate::spawn_rotation`] and
/// the server's rotation tick all reapply them with no new wiring and no
/// call site left to miss. `apply_ttl` stops at its first failing
/// statement, so the order is a dependency order: these come after
/// [`TTL_STMTS`] — the `metric_hist_samples` precedent — and the three
/// naming the landing table come last within them, so an operator-managed
/// schema without that table stops nothing that does not name it.
///
/// The four derived tables need a window of their own because a view's
/// insert carries a block id derived from the source block, and only a
/// table with a window recognises the repeat.
const METRIC_LANDING_STMTS: &[&str] = &[
    "ALTER TABLE {{db}}.metric_samples{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{metrics_dedup_window}};",
    "ALTER TABLE {{db}}.metric_series{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{metrics_dedup_window}};",
    "ALTER TABLE {{db}}.metric_metadata{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{metrics_dedup_window}};",
    "ALTER TABLE {{db}}.metric_hist_samples{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{metrics_dedup_window}};",
    "ALTER TABLE {{db}}.metric_landing{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(received_ms, 1000) + {{metrics_landing_retention_hours}} * 3600, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.metric_landing{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{metrics_dedup_window}};",
];

/// The logs landing table's own delete-TTL, plus the block-deduplication
/// window every table on the logs write path carries (issue #603).
///
/// [`METRIC_LANDING_STMTS`]'s twin, chained after it in [`apply_ttl`] for the
/// same reasons, and with the same order rule: the two naming `log_landing`
/// come last, so a schema managed by hand without that table stops nothing
/// that does not name it.
///
/// Five of the six windows are the tables the views maintain. A view's insert
/// into its target carries a block id derived from the source block, and only
/// a table with a window recognises the repeat.
const LOG_LANDING_STMTS: &[&str] = &[
    "ALTER TABLE {{db}}.log_samples{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{log_dedup_window}};",
    "ALTER TABLE {{db}}.log_streams{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{log_dedup_window}};",
    "ALTER TABLE {{db}}.log_streams_idx{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{log_dedup_window}};",
    "ALTER TABLE {{db}}.log_metrics_{{log_rollup_suffix}}{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{log_dedup_window}};",
    "ALTER TABLE {{db}}.log_patterns{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{log_dedup_window}};",
    "ALTER TABLE {{db}}.log_landing{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(received_ms, 1000) + {{log_landing_retention_hours}} * 3600, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.log_landing{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{log_dedup_window}};",
];

/// The traces landing table's own delete-TTL, the three day-column TTLs of
/// the retained trace tables, and the block-deduplication window every table
/// on the traces write path carries (issues #584 to #586).
///
/// [`METRIC_LANDING_STMTS`]'s and [`LOG_LANDING_STMTS`]'s twin, chained after
/// both in [`apply_ttl`] for the same reasons, and with the same order rule:
/// the two naming `trace_landing` come last, so a schema managed by hand
/// without that table stops nothing that does not name it.
///
/// **Six windows, one per write-path table.** A view's insert into its target
/// carries a block id derived from the source block, and only a table with a
/// window recognises the repeat. `tag_names` and `tag_values` carry a window
/// and **no TTL**: `docs/api.md` §4.3 requires catalog entries to outlive
/// span retention.
///
/// The three day-column TTLs clamp at the top of the 32-bit `DateTime`
/// domain, as every statement in [`TTL_STMTS`] does: at
/// `{{retention_days}} = 7` and a span at 2106-02-06T23:59:59Z both the
/// nanosecond and the `Date` form answer `2106-02-07 06:28:15` rather than a
/// wrapped 1970 instant.
const TRACE_LANDING_STMTS: &[&str] = &[
    "ALTER TABLE {{db}}.spans{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(start_ns, 1000000000) + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.spans{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    "ALTER TABLE {{db}}.traces{{on_cluster}} MODIFY TTL \
     toDateTime(least(toUInt32(day) * 86400 + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.traces{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    "ALTER TABLE {{db}}.resources{{on_cluster}} MODIFY TTL \
     toDateTime(least(toUInt32(day) * 86400 + {{retention_days}} * 86400, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.resources{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    "ALTER TABLE {{db}}.spans{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{trace_dedup_window}};",
    "ALTER TABLE {{db}}.traces{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{trace_dedup_window}};",
    "ALTER TABLE {{db}}.resources{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{trace_dedup_window}};",
    "ALTER TABLE {{db}}.tag_names{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{trace_dedup_window}};",
    "ALTER TABLE {{db}}.tag_values{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{trace_dedup_window}};",
    "ALTER TABLE {{db}}.trace_landing{{on_cluster}} MODIFY TTL \
     toDateTime(least(intDiv(received_ms, 1000) + {{trace_landing_retention_hours}} * 3600, 4294967295)) DELETE;",
    "ALTER TABLE {{db}}.trace_landing{{on_cluster}} MODIFY SETTING {{dedup_window_setting}} = {{trace_dedup_window}};",
];

/// The seconds half of a replicated table's block-deduplication window, one
/// statement per write-path table (issue #603).
///
/// **Chained only when a cluster is configured**, because the setting exists
/// on a `Replicated*` engine alone. A clustered deployment is the production
/// shape, so this is the case that matters: a replicated table forgets a block
/// hash after `replicated_deduplication_window_seconds` **even if fewer than
/// `replicated_deduplication_window` newer blocks have arrived**, so the block
/// window alone does not bound a resend. A deployment that lowered this below
/// the landing budget would forget a token while the writer is still entitled
/// to resend under it.
///
/// `{{dedup_window_seconds}}` renders [`DEDUP_WINDOW_SECONDS`] — a constant
/// and not a knob, because the precondition of the defect is a deployment
/// setting the value too small.
const CLUSTER_DEDUP_SECONDS_STMTS: &[&str] = &[
    "ALTER TABLE {{db}}.metric_samples{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.metric_series{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.metric_metadata{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.metric_hist_samples{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.metric_landing{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.log_samples{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.log_streams{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.log_streams_idx{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.log_metrics_{{log_rollup_suffix}}{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.log_patterns{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.log_landing{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.spans{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.traces{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.resources{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.tag_names{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.tag_values{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
    "ALTER TABLE {{db}}.trace_landing{{on_cluster}} MODIFY SETTING replicated_deduplication_window_seconds = {{dedup_window_seconds}};",
];

/// The seconds deduplication window every clustered write-path table is
/// pinned to (issue #603), in seconds.
///
/// **Why this number is enough.** The window guards one thing: the writer
/// resending its own block. That is bounded by the landing budget —
/// `WriterRuntime::landing_budget`, 120 s, measured from the push's admission
/// — so a block settles strictly before it. A client's re-push is a different
/// question, answered by the suppression index. The relation, not the number,
/// is what a case holds, in the one crate that can see both sides
/// (`pulsus-server`'s `chconfig`): `pulsus-schema` and `pulsus-write` do not
/// depend on each other.
///
/// It is the server's own default, so pinning changes nothing where the server
/// keeps it and raises it back where a server configuration lowered it.
pub const DEDUP_WINDOW_SECONDS: u64 = 3600;

/// The projection one materialized view applies, read out of that view's own
/// rendered statement so the two cannot drift (issue #603).
///
/// `None` when no view of that name is in the catalogue. Used by the metrics
/// rebuild path, which replays the landing table through the same projection
/// the view applies to a new insert.
pub fn mv_projection(mv_name: &str, ctx: &RenderCtx) -> Option<String> {
    let mv = MVS.iter().find(|mv| mv.name == mv_name)?;
    let rendered = render::render(mv.tmpl, &render::render_name(mv.name, ctx), ctx, false);
    let (_, body) = rendered.split_once(" AS\n")?;
    Some(body.trim_end().trim_end_matches(';').to_string())
}

/// Which `system` table answers whether one name exists (issue #603).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameCatalogue {
    Setting,
    MergeTreeSetting,
    Function,
}

impl NameCatalogue {
    /// The `system` table this catalogue is read from.
    pub(crate) const fn table(self) -> &'static str {
        match self {
            NameCatalogue::Setting => "system.settings",
            NameCatalogue::MergeTreeSetting => "system.merge_tree_settings",
            NameCatalogue::Function => "system.functions",
        }
    }
}

/// Every setting and function name this build sends that was not already
/// somewhere in `crates/`, read back from the server before anything sends
/// it (issue #603). A name that might not exist is never sent: absent,
/// startup refuses and says which.
pub const REQUIRED_SERVER_NAMES: &[(&str, NameCatalogue)] = &[
    ("insert_deduplication_token", NameCatalogue::Setting),
    // The seven settings one landing insert pins, so that one push is one
    // deduplicated block: `QuerySettings::landing_insert` names them
    // together, quotes what each one's own catalogue entry says, and states
    // how the set was derived. A pin added there without a row here is
    // caught by
    // `the_settings_read_back_at_startup_are_the_ones_the_landing_insert_sends`.
    ("max_insert_block_size", NameCatalogue::Setting),
    ("max_insert_block_size_bytes", NameCatalogue::Setting),
    ("input_format_max_block_size_bytes", NameCatalogue::Setting),
    ("min_insert_block_size_rows", NameCatalogue::Setting),
    ("min_insert_block_size_bytes", NameCatalogue::Setting),
    ("input_format_connection_handling", NameCatalogue::Setting),
    ("input_format_max_block_wait_ms", NameCatalogue::Setting),
    ("merge_with_ttl_timeout", NameCatalogue::MergeTreeSetting),
    // The three block/seconds deduplication-window names `apply_ttl` sends
    // (issue #603). Which of the first two a statement carries is rendered
    // from the same thing that renders the engine
    // (`render::dedup_window_setting`), and the third is sent on a clustered
    // deployment only — but startup does not know which tables a later
    // reconfiguration will render, so all three are read back. The
    // non-replicated name was absent from this list although this build
    // already sent it.
    (
        "non_replicated_deduplication_window",
        NameCatalogue::MergeTreeSetting,
    ),
    (
        "replicated_deduplication_window",
        NameCatalogue::MergeTreeSetting,
    ),
    (
        "replicated_deduplication_window_seconds",
        NameCatalogue::MergeTreeSetting,
    ),
    // The nineteen further settings one TRACE landing insert pins, of seven
    // classes the metrics set did not need: how the block's bytes are read,
    // a limit that refuses a block rather than dividing it, what an error
    // does, what an acknowledgement means, where a row is placed, what path
    // a value is stored under, and whether exceeding a limit is an error or
    // a success that need not be complete.
    // `QuerySettings::trace_landing_insert` names them together and quotes
    // what each one's own catalogue entry says. A pin added there without a
    // row here is caught by
    // `the_settings_read_back_at_startup_are_the_ones_the_trace_insert_sends`,
    // which derives both directions rather than carrying a list.
    (
        "input_format_binary_read_json_as_string",
        NameCatalogue::Setting,
    ),
    ("format_binary_max_object_size", NameCatalogue::Setting),
    ("max_partitions_per_insert_block", NameCatalogue::Setting),
    (
        "throw_on_max_partitions_per_insert_block",
        NameCatalogue::Setting,
    ),
    ("materialized_views_ignore_errors", NameCatalogue::Setting),
    (
        "ignore_materialized_views_with_dropped_target_table",
        NameCatalogue::Setting,
    ),
    (
        "min_insert_block_size_rows_for_materialized_views",
        NameCatalogue::Setting,
    ),
    (
        "min_insert_block_size_bytes_for_materialized_views",
        NameCatalogue::Setting,
    ),
    ("distributed_foreground_insert", NameCatalogue::Setting),
    ("insert_shard_id", NameCatalogue::Setting),
    ("json_type_escape_dots_in_keys", NameCatalogue::Setting),
    ("type_json_skip_duplicated_paths", NameCatalogue::Setting),
    // The seven overflow modes the repair's statements reach. Each is one
    // `DECLARE` line in the engine's own `src/Core/Settings.cpp` at
    // `v26.3.29.7-lts`, defaulting to `throw`, and each is present in
    // `system.settings` on that build with that default — so pinning
    // changes nothing where a deployment keeps them, and makes a
    // deployment that set `break` fail loudly where it had a success that
    // need not have been complete.
    ("read_overflow_mode", NameCatalogue::Setting),
    ("read_overflow_mode_leaf", NameCatalogue::Setting),
    ("timeout_overflow_mode", NameCatalogue::Setting),
    ("group_by_overflow_mode", NameCatalogue::Setting),
    ("distinct_overflow_mode", NameCatalogue::Setting),
    ("sort_overflow_mode", NameCatalogue::Setting),
    ("result_overflow_mode", NameCatalogue::Setting),
    ("generateUUIDv7", NameCatalogue::Function),
    ("toStartOfHour", NameCatalogue::Function),
    ("tupleElement", NameCatalogue::Function),
];

/// One `SELECT name FROM <catalogue> WHERE name IN (…)` statement per
/// catalogue that has at least one required name, in catalogue order. An
/// empty `required` renders no statement.
///
/// **One statement per catalogue, not one joined statement.** A deployment's
/// ClickHouse user may hold `SELECT` on some of these `system` tables and not
/// others — `system.settings` and `system.functions` are readable by any
/// user, while `system.merge_tree_settings` needs an explicit grant — and a
/// joined statement fails whole on the one it cannot read, so a readable
/// catalogue would go unchecked because of an unreadable one.
pub fn required_names_sql(
    required: &[(&'static str, NameCatalogue)],
) -> Vec<(NameCatalogue, String)> {
    let mut out: Vec<(NameCatalogue, String)> = Vec::new();
    for catalogue in [
        NameCatalogue::Setting,
        NameCatalogue::MergeTreeSetting,
        NameCatalogue::Function,
    ] {
        let names: Vec<String> = required
            .iter()
            .filter(|(_, c)| *c == catalogue)
            .map(|(name, _)| format!("'{name}'"))
            .collect();
        if names.is_empty() {
            continue;
        }
        out.push((
            catalogue,
            format!(
                "SELECT name FROM {} WHERE name IN ({})",
                catalogue.table(),
                names.join(", ")
            ),
        ));
    }
    out
}

/// The required names absent from `present`, in `required` order. It matches
/// on the name alone; the statement is what binds a name to its catalogue.
pub fn missing_server_names(
    required: &[(&'static str, NameCatalogue)],
    present: &[String],
) -> Vec<&'static str> {
    required
        .iter()
        .filter(|(name, _)| !present.iter().any(|p| p == name))
        .map(|(name, _)| *name)
        .collect()
}

/// Reads each of [`required_names_sql`]'s statements off the server and
/// reports what [`missing_server_names`] makes of the answers. An empty
/// `required` runs no statement and returns an empty list.
///
/// **A catalogue this user has no grant to read is a catalogue this build
/// cannot check, not a refusal** — and nothing wider than that: see
/// [`catalogue_read_is_unchecked`]. A name a readable catalogue reports ABSENT
/// still refuses.
pub async fn absent_server_names(
    client: &ChClient,
    required: &[(&'static str, NameCatalogue)],
) -> Result<Vec<&'static str>, SchemaError> {
    let mut reads = Vec::new();
    for (catalogue, sql) in required_names_sql(required) {
        reads.push((catalogue, read_names(client, &sql).await));
    }
    fold_catalogue_reads(required, reads)
}

/// ClickHouse's `ACCESS_DENIED` server error code, which a `SELECT` on a
/// `system` table the deployment's user holds no grant for is answered with.
/// The only failure a catalogue read is allowed to continue past.
const ACCESS_DENIED: i32 = 497;

/// Whether one catalogue read's failure leaves that catalogue **unchecked**
/// rather than refusing startup.
///
/// Access denial alone. A denied `SELECT` says nothing about whether the name
/// is there, and refusing on it would stop every least-privilege deployment
/// from starting — `system.merge_tree_settings` needs a grant a user granted
/// only its own database does not hold. Every other failure — a timeout, a
/// transport fault, a decode failure, any other server exception — means the
/// catalogue was not read for a reason that says nothing about grants, so
/// continuing would let startup send a name nothing checked (issue #603 code
/// review, finding 6).
fn catalogue_read_is_unchecked(err: &SchemaError) -> bool {
    matches!(
        err,
        SchemaError::Clickhouse(ChError::Server {
            code: ACCESS_DENIED,
            ..
        })
    )
}

/// Turns one catalogue read per catalogue into the list of required names the
/// server reported absent. Pure, so which failures are tolerated and which
/// names go unchecked is testable without a server.
///
/// A read that succeeded contributes its names and puts its catalogue's
/// required names into the checked set. A read denied by access control
/// contributes neither, with a warning naming the catalogue. Any other
/// failure is returned.
fn fold_catalogue_reads(
    required: &[(&'static str, NameCatalogue)],
    reads: Vec<(NameCatalogue, Result<Vec<String>, SchemaError>)>,
) -> Result<Vec<&'static str>, SchemaError> {
    let mut present: Vec<String> = Vec::new();
    let mut checked: Vec<(&'static str, NameCatalogue)> = Vec::new();
    for (catalogue, read) in reads {
        match read {
            Ok(names) => {
                present.extend(names);
                checked.extend(required.iter().filter(|(_, c)| *c == catalogue).copied());
            }
            Err(err) if catalogue_read_is_unchecked(&err) => tracing::warn!(
                catalogue = catalogue.table(),
                error = %err,
                "no grant to read a name catalogue; the names it holds are not checked"
            ),
            Err(err) => return Err(err),
        }
    }
    Ok(missing_server_names(&checked, &present))
}

/// Every `name` one catalogue statement returns.
async fn read_names(client: &ChClient, sql: &str) -> Result<Vec<String>, SchemaError> {
    let mut stream = client
        .query_stream::<NameRow>(sql, &QuerySettings::new())
        .await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?.name);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- the metrics landing statements (issue #603) -------------------

    fn metrics_ctx(retention_hours: u32, window: u64) -> RenderCtx {
        RenderCtx {
            db: "pulsus".to_string(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: std::time::Duration::from_secs(5),
            metrics_landing_retention_hours: retention_hours,
            metrics_dedup_window: window,
            log_landing_retention_hours: retention_hours,
            log_dedup_window: window,
            trace_landing_retention_hours: retention_hours,
            trace_dedup_window: window,
        }
    }

    /// The landing table's own delete-TTL renders the configured hours, in
    /// the same clamped Int64-seconds form every other table's TTL uses. A
    /// statement hard-coding 6 hours passes at the default and fails here.
    #[test]
    fn the_landing_ttl_renders_the_configured_hours() {
        for hours in [1u32, 168] {
            let rendered: Vec<String> = METRIC_LANDING_STMTS
                .iter()
                .map(|s| render::substitute_tokens(s, &metrics_ctx(hours, 10_000)))
                .filter(|s| s.contains("MODIFY TTL"))
                .collect();
            assert_eq!(rendered.len(), 1, "one TTL statement, the landing table's");
            assert_eq!(
                rendered[0],
                format!(
                    "ALTER TABLE pulsus.metric_landing MODIFY TTL \
                     toDateTime(least(intDiv(received_ms, 1000) + {hours} * 3600, 4294967295)) \
                     DELETE;"
                )
            );
        }
    }

    /// The landing table and all four tables the views maintain carry the
    /// configured deduplication window: a view's insert carries a block id
    /// derived from the source block, and only a table with a window
    /// recognises the repeat. A statement hard-coding 10000 passes at the
    /// default and fails here.
    ///
    /// **One setting per table, the count**: the engine has no seconds-based
    /// counterpart to `non_replicated_deduplication_window`, so a block is
    /// remembered until that many newer blocks have arrived rather than for a
    /// stated time.
    #[test]
    fn the_dedup_window_statements_name_all_five_tables() {
        let rendered: Vec<String> = METRIC_LANDING_STMTS
            .iter()
            .map(|s| render::substitute_tokens(s, &metrics_ctx(6, 5_000)))
            .collect();
        assert_eq!(
            rendered.len(),
            6,
            "one per target, plus the source's TTL and its own window"
        );
        for table in [
            "metric_samples",
            "metric_series",
            "metric_metadata",
            "metric_hist_samples",
            "metric_landing",
        ] {
            let want = format!(
                "ALTER TABLE pulsus.{table} MODIFY SETTING \
                 non_replicated_deduplication_window = 5000;"
            );
            assert!(
                rendered.contains(&want),
                "missing: {want}\nrendered: {rendered:#?}"
            );
        }
        assert!(
            !rendered
                .iter()
                .any(|s| s.contains("non_replicated_deduplication_window_seconds")),
            "the engine has no such setting: sending it refuses every startup"
        );
    }

    /// `apply_ttl` stops at its first failing statement, so the order is a
    /// dependency order: the landing table's own two statements come last, so
    /// an operator-managed schema without that table stops nothing that does
    /// not name it.
    #[test]
    fn the_landing_statements_come_last_within_the_metrics_block() {
        let first_landing = METRIC_LANDING_STMTS
            .iter()
            .position(|s| s.contains("metric_landing"))
            .expect("the landing table has statements");
        assert!(
            METRIC_LANDING_STMTS[first_landing..]
                .iter()
                .all(|s| s.contains("metric_landing")),
            "nothing but landing statements may follow the first one"
        );
        assert_eq!(METRIC_LANDING_STMTS.len() - first_landing, 2);
    }

    /// **T28.** The three landing statement lists, rendered, written out.
    ///
    /// **Both modes**, because the setting name a window statement carries is
    /// rendered from the same thing that renders the engine — and the seconds
    /// list is chained **only** when a cluster is configured, which the last
    /// assertion holds. It fails if a target is dropped from any list, if a
    /// setting name is hard-coded again, or if the seconds list reaches a
    /// single-node deployment, where the setting does not exist.
    #[test]
    fn the_landing_statements_are_exactly_this_text() {
        fn rendered(list: &[&str], ctx: &RenderCtx) -> Vec<String> {
            list.iter()
                .map(|s| render::substitute_tokens(s, ctx))
                .collect()
        }

        let single = metrics_ctx(6, 5_000);
        let clustered = RenderCtx {
            cluster: Some("prod".to_string()),
            ..metrics_ctx(6, 5_000)
        };

        for (mode, ctx, window_setting, on_cluster) in [
            (
                "single-node",
                &single,
                "non_replicated_deduplication_window",
                "",
            ),
            (
                "clustered",
                &clustered,
                "replicated_deduplication_window",
                " ON CLUSTER 'prod'",
            ),
        ] {
            let metrics = rendered(METRIC_LANDING_STMTS, ctx);
            assert_eq!(
                metrics,
                vec![
                    format!(
                        "ALTER TABLE pulsus.metric_samples{on_cluster} MODIFY SETTING {window_setting} = 5000;"
                    ),
                    format!(
                        "ALTER TABLE pulsus.metric_series{on_cluster} MODIFY SETTING {window_setting} = 5000;"
                    ),
                    format!(
                        "ALTER TABLE pulsus.metric_metadata{on_cluster} MODIFY SETTING {window_setting} = 5000;"
                    ),
                    format!(
                        "ALTER TABLE pulsus.metric_hist_samples{on_cluster} MODIFY SETTING {window_setting} = 5000;"
                    ),
                    format!(
                        "ALTER TABLE pulsus.metric_landing{on_cluster} MODIFY TTL toDateTime(least(intDiv(received_ms, 1000) + 6 * 3600, 4294967295)) DELETE;"
                    ),
                    format!(
                        "ALTER TABLE pulsus.metric_landing{on_cluster} MODIFY SETTING {window_setting} = 5000;"
                    ),
                ],
                "{mode}: the metrics landing statements"
            );

            let logs = rendered(LOG_LANDING_STMTS, ctx);
            assert_eq!(
                logs,
                vec![
                    format!(
                        "ALTER TABLE pulsus.log_samples{on_cluster} MODIFY SETTING {window_setting} = 5000;"
                    ),
                    format!(
                        "ALTER TABLE pulsus.log_streams{on_cluster} MODIFY SETTING {window_setting} = 5000;"
                    ),
                    format!(
                        "ALTER TABLE pulsus.log_streams_idx{on_cluster} MODIFY SETTING {window_setting} = 5000;"
                    ),
                    format!(
                        "ALTER TABLE pulsus.log_metrics_5s{on_cluster} MODIFY SETTING {window_setting} = 5000;"
                    ),
                    format!(
                        "ALTER TABLE pulsus.log_patterns{on_cluster} MODIFY SETTING {window_setting} = 5000;"
                    ),
                    format!(
                        "ALTER TABLE pulsus.log_landing{on_cluster} MODIFY TTL toDateTime(least(intDiv(received_ms, 1000) + 6 * 3600, 4294967295)) DELETE;"
                    ),
                    format!(
                        "ALTER TABLE pulsus.log_landing{on_cluster} MODIFY SETTING {window_setting} = 5000;"
                    ),
                ],
                "{mode}: the logs landing statements"
            );
        }

        // The seconds list, rendered once — it is only ever sent clustered.
        let seconds = rendered(CLUSTER_DEDUP_SECONDS_STMTS, &clustered);
        let want: Vec<String> = [
            "metric_samples",
            "metric_series",
            "metric_metadata",
            "metric_hist_samples",
            "metric_landing",
            "log_samples",
            "log_streams",
            "log_streams_idx",
            "log_metrics_5s",
            "log_patterns",
            "log_landing",
            // The six traces write-path tables (issues #584 to #586).
            "spans",
            "traces",
            "resources",
            "tag_names",
            "tag_values",
            "trace_landing",
        ]
        .iter()
        .map(|t| {
            format!(
                "ALTER TABLE pulsus.{t} ON CLUSTER 'prod' MODIFY SETTING \
                 replicated_deduplication_window_seconds = {DEDUP_WINDOW_SECONDS};"
            )
        })
        .collect();
        assert_eq!(
            seconds, want,
            "one statement per write-path table, in the same order the two \
             lists above use"
        );

        // **Chained only when a cluster is configured.** The setting exists on
        // a `Replicated*` engine alone, so sending it to a single-node
        // deployment names a setting that table does not carry.
        for (mode, ctx, expect_seconds) in [
            ("single-node", &single, false),
            ("clustered", &clustered, true),
        ] {
            let chained: Vec<&&str> = TTL_STMTS
                .iter()
                .chain(METRIC_LANDING_STMTS)
                .chain(LOG_LANDING_STMTS)
                .chain(if ctx.cluster.is_some() {
                    CLUSTER_DEDUP_SECONDS_STMTS
                } else {
                    &[]
                })
                .collect();
            let has_seconds = chained
                .iter()
                .any(|s| s.contains("replicated_deduplication_window_seconds"));
            assert_eq!(
                has_seconds, expect_seconds,
                "{mode}: the seconds window is a clustered deployment's alone"
            );
        }
    }

    /// **T29.** `apply_ttl` stops at its first failing statement, so the order
    /// is a dependency order: within `LOG_LANDING_STMTS` the two naming
    /// `log_landing` come last, so a schema managed by hand without that table
    /// stops nothing that does not name it. The twin of
    /// `the_landing_statements_come_last_within_the_metrics_block`.
    #[test]
    fn the_landing_statements_come_last_within_the_logs_block() {
        let first_landing = LOG_LANDING_STMTS
            .iter()
            .position(|s| s.contains("log_landing"))
            .expect("the landing table has statements");
        assert!(
            LOG_LANDING_STMTS[first_landing..]
                .iter()
                .all(|s| s.contains("log_landing")),
            "nothing but landing statements may follow the first one"
        );
        assert_eq!(LOG_LANDING_STMTS.len() - first_landing, 2);
    }

    // -- the startup name check (issue #603) --------------------------

    /// The decision is pure: over an empty server every required name is
    /// reported, over a complete one none is, and over each subset with one
    /// name left out exactly that name. A list that hard-codes one absent
    /// name cannot pass the subsets.
    ///
    /// **The production list is asserted non-empty first**, because every
    /// assertion below is over it: an empty list makes each one hold
    /// vacuously — the two `missing_server_names` calls both answer `[]`, the
    /// subset loop runs no iteration — and the case would pass over a build
    /// that requires nothing (issue #603 code review round 2). Two names is
    /// the least that exercises a subset with one left out.
    #[test]
    fn the_startup_name_decision_is_pure() {
        assert!(
            REQUIRED_SERVER_NAMES.len() >= 2,
            "the assertions below are over this list; it must name at least \
             two required names: {REQUIRED_SERVER_NAMES:?}"
        );
        let all: Vec<String> = REQUIRED_SERVER_NAMES
            .iter()
            .map(|(n, _)| (*n).to_string())
            .collect();
        assert_eq!(
            missing_server_names(REQUIRED_SERVER_NAMES, &[]),
            REQUIRED_SERVER_NAMES
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>(),
            "over an empty server every required name is missing, in order"
        );
        assert!(missing_server_names(REQUIRED_SERVER_NAMES, &all).is_empty());
        for (i, (left_out, _)) in REQUIRED_SERVER_NAMES.iter().enumerate() {
            let mut present = all.clone();
            present.remove(i);
            assert_eq!(
                missing_server_names(REQUIRED_SERVER_NAMES, &present),
                vec![*left_out],
                "the subset without {left_out} reports exactly it"
            );
        }
        assert!(missing_server_names(&[], &[]).is_empty());
    }

    /// The statements ask for exactly the required names, and each names its
    /// own catalogue alone — a merge-tree setting looked for in
    /// `system.settings` is absent there and would refuse every startup.
    ///
    /// **One statement per catalogue**, so a catalogue this user cannot read
    /// does not blind the ones it can.
    #[test]
    fn the_sql_asks_for_exactly_the_required_names() {
        let statements = required_names_sql(REQUIRED_SERVER_NAMES);
        assert_eq!(
            statements.len(),
            3,
            "one per catalogue with a required name"
        );
        let quoted: std::collections::BTreeSet<&str> = statements
            .iter()
            .flat_map(|(_, sql)| sql.split('\'').skip(1).step_by(2))
            .collect();
        let want: std::collections::BTreeSet<&str> =
            REQUIRED_SERVER_NAMES.iter().map(|(n, _)| *n).collect();
        assert_eq!(quoted, want, "the literals ARE the list: {statements:?}");
        for (catalogue, sql) in &statements {
            for table in [
                "system.settings",
                "system.merge_tree_settings",
                "system.functions",
            ] {
                let expected = usize::from(table == catalogue.table());
                assert_eq!(
                    sql.matches(table).count(),
                    expected,
                    "{table} in the {} statement: {sql}",
                    catalogue.table()
                );
            }
        }

        // One row of each catalogue names that catalogue alone.
        for (catalogue, table) in [
            (NameCatalogue::Setting, "system.settings"),
            (
                NameCatalogue::MergeTreeSetting,
                "system.merge_tree_settings",
            ),
            (NameCatalogue::Function, "system.functions"),
        ] {
            assert_eq!(
                required_names_sql(&[("only_one", catalogue)]),
                vec![(
                    catalogue,
                    format!("SELECT name FROM {table} WHERE name IN ('only_one')")
                )]
            );
        }
        assert!(
            required_names_sql(&[]).is_empty(),
            "an empty list runs no statement"
        );
    }

    /// **The setting names read back at startup are exactly the setting names
    /// the landing insert sends**, checked both ways against one written-out
    /// set, so neither side can move without the other (issue #603 code
    /// review round 8, finding 1: two rounds each found further settings that
    /// end a block, and each time the list was maintained beside the pins by
    /// hand).
    ///
    /// The two exemptions are the pair the **shipped** span insert already
    /// sends (`QuerySettings::deduplicate_through_views`, issue #560). They
    /// were in `crates/` before this work, which is the class the startup
    /// check draws: every name this work sends that was not already there.
    /// A server too old to carry them is refused by its version, which is a
    /// separate check.
    ///
    /// **The logs landing path widens neither side** (issue #603): its insert
    /// pins the same ten settings the metrics one does, and its read path
    /// sends no query setting at all — the discovery reads dispatch with the
    /// budget settings and nothing more. `the_discovery_reads_send_no_distributed_product_mode`
    /// (`crates/pulsus-read/src/logql/exec.rs`) asserts the other side of
    /// that.
    #[test]
    fn the_settings_read_back_at_startup_are_the_ones_the_landing_insert_sends() {
        use std::collections::BTreeSet;

        /// In `crates/` before this work, so outside the probed class.
        const ALREADY_SHIPPED: &[&str] = &[
            "deduplicate_insert",
            "deduplicate_blocks_in_dependent_materialized_views",
        ];

        let want: BTreeSet<&str> = BTreeSet::from([
            "insert_deduplication_token",
            "max_insert_block_size",
            "max_insert_block_size_bytes",
            "input_format_max_block_size_bytes",
            "min_insert_block_size_rows",
            "min_insert_block_size_bytes",
            "input_format_connection_handling",
            "input_format_max_block_wait_ms",
        ]);

        // The trace landing insert's own nineteen pins are rows too (issues
        // #584 to #586), and they are **derived from that constructor** here
        // rather than written out a second time: this case owns the eight
        // above as a literal set, and
        // `the_settings_read_back_at_startup_are_the_ones_the_trace_insert_sends`
        // owns the rest, both ways.
        let trace_only_set = QuerySettings::trace_landing_insert("tok-1", 1_048_576);
        let trace_only: BTreeSet<&str> = trace_only_set
            .entries()
            .map(|(k, _)| k)
            .filter(|k| !ALREADY_SHIPPED.contains(k) && !want.contains(k))
            .collect();
        assert!(
            !trace_only.is_empty(),
            "the trace landing insert adds no pin of its own, so the union \
             below is the metrics set and this case closes nothing new"
        );
        let want_all: BTreeSet<&str> = want.union(&trace_only).copied().collect();

        let read_back: BTreeSet<&str> = REQUIRED_SERVER_NAMES
            .iter()
            .filter(|(_, c)| *c == NameCatalogue::Setting)
            .map(|(n, _)| *n)
            .collect();
        assert_eq!(
            read_back, want_all,
            "the settings catalogue's required names are the two landing \
             inserts' pins and nothing else"
        );

        let settings = QuerySettings::landing_insert("tok-1", 1_048_576);
        let sent: BTreeSet<&str> = settings.entries().map(|(k, _)| k).collect();
        let sent_new: BTreeSet<&str> = sent
            .iter()
            .copied()
            .filter(|k| !ALREADY_SHIPPED.contains(k))
            .collect();
        assert_eq!(
            sent_new, want,
            "every setting one landing insert sends is either read back at \
             startup or one of the two the shipped span insert already sends"
        );
        for name in ALREADY_SHIPPED {
            assert!(
                sent.contains(name),
                "{name} is exempted from the check but is not sent at all"
            );
        }
    }

    /// **T30.** The three statements, written out, so the list and the
    /// statements cannot drift apart unnoticed.
    ///
    /// The `MergeTree` catalogue carries §4's **three** deduplication-window
    /// names and nothing else: which of the two block names a statement
    /// renders follows the engine, and the seconds name is sent on a clustered
    /// deployment only, but startup cannot know which shape a later
    /// reconfiguration renders, so all three are read back. **No query setting
    /// is added**: the discovery reads send none.
    #[test]
    fn the_required_names_statements_are_exactly_this_text() {
        assert_eq!(
            required_names_sql(REQUIRED_SERVER_NAMES),
            vec![
                (
                    NameCatalogue::Setting,
                    "SELECT name FROM system.settings WHERE name IN \
                     ('insert_deduplication_token', 'max_insert_block_size', \
                     'max_insert_block_size_bytes', 'input_format_max_block_size_bytes', \
                     'min_insert_block_size_rows', 'min_insert_block_size_bytes', \
                     'input_format_connection_handling', 'input_format_max_block_wait_ms', \
                     'input_format_binary_read_json_as_string', \
                     'format_binary_max_object_size', 'max_partitions_per_insert_block', \
                     'throw_on_max_partitions_per_insert_block', \
                     'materialized_views_ignore_errors', \
                     'ignore_materialized_views_with_dropped_target_table', \
                     'min_insert_block_size_rows_for_materialized_views', \
                     'min_insert_block_size_bytes_for_materialized_views', \
                     'distributed_foreground_insert', 'insert_shard_id', \
                     'json_type_escape_dots_in_keys', 'type_json_skip_duplicated_paths', \
                     'read_overflow_mode', 'read_overflow_mode_leaf', \
                     'timeout_overflow_mode', 'group_by_overflow_mode', \
                     'distinct_overflow_mode', 'sort_overflow_mode', \
                     'result_overflow_mode')"
                        .to_string()
                ),
                (
                    NameCatalogue::MergeTreeSetting,
                    "SELECT name FROM system.merge_tree_settings WHERE name IN \
                     ('merge_with_ttl_timeout', 'non_replicated_deduplication_window', \
                     'replicated_deduplication_window', \
                     'replicated_deduplication_window_seconds')"
                        .to_string()
                ),
                (
                    NameCatalogue::Function,
                    "SELECT name FROM system.functions WHERE name IN \
                     ('generateUUIDv7', 'toStartOfHour', 'tupleElement')"
                        .to_string()
                ),
            ]
        );
    }

    // -- the traces landing statements (issues #584 to #586) -----------

    /// **T-R2, the rendering half.** The three day-column TTLs, the landing
    /// TTL and the six windows, rendered. The `spans` statement is asserted
    /// byte for byte — a TTL that read the wrong column, or dropped the
    /// `least(…, 4294967295)` clamp, renders differently — and the `traces`
    /// one is asserted to take its seconds from the `Date` column rather than
    /// from a nanosecond one.
    #[test]
    fn the_rendered_ttl_statements_are_byte_exact() {
        let ctx = metrics_ctx(6, 10_000);
        let rendered: Vec<String> = TRACE_LANDING_STMTS
            .iter()
            .map(|s| render::substitute_tokens(s, &ctx))
            .collect();

        assert!(
            rendered.iter().any(|s| s
                == "ALTER TABLE pulsus.spans MODIFY TTL toDateTime(least(intDiv(start_ns, \
                    1000000000) + 7 * 86400, 4294967295)) DELETE;"),
            "the spans TTL statement is not byte-exact: {rendered:#?}"
        );
        assert!(
            rendered.iter().any(|s| s
                == "ALTER TABLE pulsus.traces MODIFY TTL toDateTime(least(toUInt32(day) * 86400 \
                    + 7 * 86400, 4294967295)) DELETE;"),
            "the traces TTL takes its seconds from the Date column: {rendered:#?}"
        );
        assert!(
            rendered.iter().any(|s| s
                == "ALTER TABLE pulsus.resources MODIFY TTL toDateTime(least(toUInt32(day) * \
                    86400 + 7 * 86400, 4294967295)) DELETE;"),
            "the resources TTL takes its seconds from the Date column: {rendered:#?}"
        );
        assert!(
            rendered.iter().any(|s| s
                == "ALTER TABLE pulsus.trace_landing MODIFY TTL \
                    toDateTime(least(intDiv(received_ms, 1000) + 6 * 3600, 4294967295)) DELETE;"),
            "the landing TTL is the configured hours: {rendered:#?}"
        );

        // Six windows, one per write-path table; the two catalogs carry a
        // window and no TTL, because docs/api.md §4.3 requires catalog
        // entries to outlive span retention.
        for table in [
            "spans",
            "traces",
            "resources",
            "tag_names",
            "tag_values",
            "trace_landing",
        ] {
            let want = format!(
                "ALTER TABLE pulsus.{table} MODIFY SETTING \
                 non_replicated_deduplication_window = 10000;"
            );
            assert!(
                rendered.contains(&want),
                "no window statement for {table}: {rendered:#?}"
            );
        }
        for catalog in ["tag_names", "tag_values"] {
            assert!(
                !rendered
                    .iter()
                    .any(|s| s.contains(&format!("pulsus.{catalog} MODIFY TTL"))),
                "{catalog} must carry no TTL: {rendered:#?}"
            );
        }

        // The two naming the landing table come last, so a schema managed by
        // hand without that table stops nothing that does not name it.
        let first_landing = TRACE_LANDING_STMTS
            .iter()
            .position(|s| s.contains("trace_landing"));
        assert!(
            first_landing.is_some(),
            "no statement names the landing table: {rendered:#?}"
        );
        let first_landing = first_landing.expect("checked above");
        assert!(
            TRACE_LANDING_STMTS[first_landing..]
                .iter()
                .all(|s| s.contains("trace_landing")),
            "the landing statements must be contiguous and last"
        );
        assert_eq!(TRACE_LANDING_STMTS.len() - first_landing, 2);
    }

    /// **The setting names read back at startup are exactly the setting names
    /// the trace landing insert sends**, derived both ways with no list on
    /// either side.
    ///
    /// **Forward**: every key `QuerySettings::trace_landing_insert` sends has
    /// a `REQUIRED_SERVER_NAMES` row of catalogue `Setting`, bar the pair the
    /// shipped span insert already sent — the same exemption
    /// `the_settings_read_back_at_startup_are_the_ones_the_landing_insert_sends`
    /// carries, and for the same reason: they were in `crates/` before this
    /// work, which is the class the startup check draws.
    ///
    /// **Reverse**: the rows this change added, taken as the set difference
    /// between the `Setting` rows and the keys the **metrics** landing insert
    /// sends — which is what those rows were at the base revision, pinned by
    /// the case named above — must every one be a key the trace constructor
    /// sends.
    ///
    /// Neither direction names a count or a name, so a pin added without a
    /// row, a row added without a pin, and a row added for one of the
    /// nineteen while the reverse check only walked some of them all fail.
    #[test]
    fn the_settings_read_back_at_startup_are_the_ones_the_trace_insert_sends() {
        use std::collections::BTreeSet;

        /// In `crates/` before this work, so outside the probed class.
        const ALREADY_SHIPPED: &[&str] = &[
            "deduplicate_insert",
            "deduplicate_blocks_in_dependent_materialized_views",
        ];

        let rows: BTreeSet<&str> = REQUIRED_SERVER_NAMES
            .iter()
            .filter(|(_, c)| *c == NameCatalogue::Setting)
            .map(|(n, _)| *n)
            .collect();
        let trace_sent_set = QuerySettings::trace_landing_insert("tok-1", 1_048_576);
        let trace_sent: BTreeSet<&str> = trace_sent_set.entries().map(|(k, _)| k).collect();
        let metrics_sent_set = QuerySettings::landing_insert("tok-1", 1_048_576);
        let metrics_sent: BTreeSet<&str> = metrics_sent_set.entries().map(|(k, _)| k).collect();

        // Forward.
        let unread: Vec<&str> = trace_sent
            .iter()
            .copied()
            .filter(|k| !ALREADY_SHIPPED.contains(k) && !rows.contains(k))
            .collect();
        assert!(
            unread.is_empty(),
            "the trace landing insert sends settings nothing reads back at \
             startup: {unread:?}"
        );

        // Reverse: the rows this change added.
        let added: Vec<&str> = rows
            .iter()
            .copied()
            .filter(|k| !metrics_sent.contains(k))
            .collect();
        assert!(
            !added.is_empty(),
            "this change adds no setting row at all, so the reverse direction \
             checks nothing"
        );
        let unsent: Vec<&str> = added
            .iter()
            .copied()
            .filter(|k| !trace_sent.contains(k))
            .collect();
        assert!(
            unsent.is_empty(),
            "a required-name row was added for a setting the trace landing \
             insert does not send: {unsent:?}"
        );
        for name in ALREADY_SHIPPED {
            assert!(
                trace_sent.contains(name),
                "{name} is exempted from the check but is not sent at all"
            );
        }
    }

    /// Issue #603 code review, finding 6: a catalogue read that failed for a
    /// reason other than a missing grant must refuse startup, not leave the
    /// names it holds silently unchecked. A timeout, a transport fault, a
    /// decode failure or any other server exception says nothing about
    /// grants — continuing past one lets the build send a name nothing ever
    /// looked for, which is the whole point of the check.
    #[test]
    fn only_a_denied_grant_leaves_a_catalogue_unchecked() {
        const NAMES: &[(&str, NameCatalogue)] = &[
            ("a_setting", NameCatalogue::Setting),
            ("a_function", NameCatalogue::Function),
        ];
        let denied = || {
            SchemaError::Clickhouse(ChError::Server {
                code: 497,
                message: "Not enough privileges on system.merge_tree_settings".to_string(),
            })
        };

        // Access denial on the settings catalogue: its name goes unchecked,
        // and the readable catalogue's absent name still refuses.
        let absent = fold_catalogue_reads(
            NAMES,
            vec![
                (NameCatalogue::Setting, Err(denied())),
                (NameCatalogue::Function, Ok(Vec::new())),
            ],
        )
        .expect("a denied grant is not a refusal");
        assert_eq!(
            absent,
            vec!["a_function"],
            "the unreadable catalogue's name is unchecked; the readable \
             catalogue's absent name still refuses"
        );

        // Every other failure is returned.
        for (label, err) in [
            (
                "a timeout",
                SchemaError::Clickhouse(ChError::Timeout("deadline".to_string())),
            ),
            (
                "a transport fault",
                SchemaError::Clickhouse(ChError::Io("reset by peer".to_string())),
            ),
            (
                "a decode failure",
                SchemaError::Clickhouse(ChError::Decode("not a NameRow".to_string())),
            ),
            (
                "another server exception",
                SchemaError::Clickhouse(ChError::Server {
                    code: 60,
                    message: "Table system.settings does not exist".to_string(),
                }),
            ),
            (
                "a non-ClickHouse schema error",
                SchemaError::Version("nonsense".to_string()),
            ),
        ] {
            let got = fold_catalogue_reads(
                NAMES,
                vec![
                    (NameCatalogue::Setting, Err(err)),
                    (NameCatalogue::Function, Ok(vec!["a_function".to_string()])),
                ],
            );
            assert!(
                got.is_err(),
                "{label} must refuse rather than leave a catalogue unchecked, got {got:?}"
            );
        }

        // Both readable: nothing absent.
        let absent = fold_catalogue_reads(
            NAMES,
            vec![
                (NameCatalogue::Setting, Ok(vec!["a_setting".to_string()])),
                (NameCatalogue::Function, Ok(vec!["a_function".to_string()])),
            ],
        )
        .expect("two good reads");
        assert!(absent.is_empty(), "{absent:?}");
    }

    /// Issue #131 AC9: both trace `MODIFY TTL` statements render the
    /// saturating expression — Int64 arithmetic clamped to `u32::MAX`
    /// before `toDateTime` — and no longer the wrap-prone
    /// `fromUnixTimestamp64Nano(...) + INTERVAL ... DAY` form. Fails on the
    /// pre-#131 statement text.
    #[test]
    fn apply_ttl_trace_statements_render_the_saturating_datetime_expression() {
        let ctx = RenderCtx {
            db: "pulsus".to_string(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: std::time::Duration::from_secs(5),
            metrics_landing_retention_hours: 6,
            metrics_dedup_window: 10_000,
            log_landing_retention_hours: 6,
            log_dedup_window: 10_000,
            trace_landing_retention_hours: 6,
            trace_dedup_window: 10_000,
        };
        let trace_ttl_stmts: Vec<String> = TTL_STMTS
            .iter()
            .filter(|s| s.contains("MODIFY TTL") && s.contains("trace_"))
            .map(|s| render::substitute_tokens(s, &ctx))
            .collect();
        assert_eq!(
            trace_ttl_stmts.len(),
            5,
            "exactly trace_spans + trace_attrs_idx + trace_edges + trace_recent + \
             trace_error_spans carry a trace MODIFY TTL"
        );
        for stmt in &trace_ttl_stmts {
            // Issue #560: `trace_recent`'s TTL reads its newest span in the
            // bucket, `ts_max`; every other trace table's reads its own
            // `timestamp_ns`.
            let column = if stmt.contains(".trace_recent ") {
                "ts_max"
            } else {
                "timestamp_ns"
            };
            assert!(
                stmt.contains(&format!("least(intDiv({column}, 1000000000) + ")),
                "trace TTL must use the clamped Int64-seconds form on {column}: {stmt}"
            );
            assert!(
                stmt.contains(", 4294967295))"),
                "trace TTL must clamp to u32::MAX before toDateTime: {stmt}"
            );
            assert!(
                stmt.contains("7 * 86400"),
                "retention_days must render into the seconds arithmetic: {stmt}"
            );
            assert!(
                !stmt.contains("fromUnixTimestamp64Nano"),
                "the wrap-prone DateTime64 form must be gone: {stmt}"
            );
        }
    }

    /// Issue #137 AC1: ALL five `MODIFY TTL` statements render the clamped
    /// `least(intDiv(...) + N * 86400, 4294967295)` form — the metric
    /// tables at millisecond scale (`intDiv(unix_milli, 1000)`), the
    /// log/trace tables at nanosecond scale — with no wrap-prone
    /// `fromUnixTimestamp64*`/`INTERVAL` remnants, and exactly one
    /// MODIFY TTL + MODIFY SETTING pair targets `metric_hist_samples`
    /// (absent from `apply_ttl` entirely before #137). Fails on the
    /// pre-#137 statement list.
    #[test]
    fn apply_ttl_all_statements_render_the_saturating_datetime_expression() {
        let ctx = RenderCtx {
            db: "pulsus".to_string(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: std::time::Duration::from_secs(5),
            metrics_landing_retention_hours: 6,
            metrics_dedup_window: 10_000,
            log_landing_retention_hours: 6,
            log_dedup_window: 10_000,
            trace_landing_retention_hours: 6,
            trace_dedup_window: 10_000,
        };
        let rendered: Vec<String> = TTL_STMTS
            .iter()
            .map(|s| render::substitute_tokens(s, &ctx))
            .collect();

        let ttl_stmts: Vec<&String> = rendered
            .iter()
            .filter(|s| s.contains("MODIFY TTL"))
            .collect();
        assert_eq!(ttl_stmts.len(), 9, "nine retained tables carry a TTL");
        for stmt in &ttl_stmts {
            assert!(
                stmt.contains("least(intDiv("),
                "every TTL must use the clamped Int64-seconds form: {stmt}"
            );
            assert!(
                stmt.contains(", 4294967295))"),
                "every TTL must clamp to u32::MAX before toDateTime: {stmt}"
            );
            assert!(
                stmt.contains("7 * 86400"),
                "retention_days must render into the seconds arithmetic: {stmt}"
            );
        }
        for stmt in &rendered {
            assert!(
                !stmt.contains("fromUnixTimestamp64Nano")
                    && !stmt.contains("fromUnixTimestamp64Milli")
                    && !stmt.contains("INTERVAL"),
                "the wrap-prone DateTime64/INTERVAL forms must be gone: {stmt}"
            );
        }
        for table in ["metric_samples", "metric_hist_samples"] {
            let stmt = ttl_stmts
                .iter()
                .find(|s| s.contains(&format!(".{table} ")))
                .unwrap_or_else(|| panic!("no MODIFY TTL for {table}"));
            assert!(
                stmt.contains("intDiv(unix_milli, 1000)"),
                "{table} is millisecond-scale: {stmt}"
            );
        }
        for table in [
            "log_samples",
            "trace_spans",
            "trace_attrs_idx",
            "trace_edges",
        ] {
            let stmt = ttl_stmts
                .iter()
                .find(|s| s.contains(&format!(".{table} ")))
                .unwrap_or_else(|| panic!("no MODIFY TTL for {table}"));
            assert!(
                stmt.contains("intDiv(timestamp_ns, 1000000000)"),
                "{table} is nanosecond-scale: {stmt}"
            );
        }

        let setting_stmts: Vec<&String> = rendered
            .iter()
            .filter(|s| s.contains("MODIFY SETTING ttl_only_drop_parts = 1"))
            .collect();
        assert_eq!(setting_stmts.len(), 9, "one MODIFY SETTING per table");
        assert_eq!(
            ttl_stmts
                .iter()
                .filter(|s| s.contains(".metric_hist_samples "))
                .count(),
            1,
            "exactly one MODIFY TTL targets metric_hist_samples"
        );
        assert_eq!(
            setting_stmts
                .iter()
                .filter(|s| s.contains(".metric_hist_samples "))
                .count(),
            1,
            "exactly one MODIFY SETTING targets metric_hist_samples"
        );

        // Issue #187: `log_patterns` renders exactly one saturating MODIFY TTL
        // on its `bucket_ns` nanosecond column + one MODIFY SETTING pair.
        let log_patterns_ttl = ttl_stmts
            .iter()
            .find(|s| s.contains(".log_patterns "))
            .unwrap_or_else(|| panic!("no MODIFY TTL for log_patterns"));
        assert!(
            log_patterns_ttl.contains("least(intDiv(bucket_ns, 1000000000) + "),
            "log_patterns TTL divides its bucket_ns column: {log_patterns_ttl}"
        );
        assert!(
            log_patterns_ttl.contains(", 4294967295))"),
            "log_patterns TTL clamps to u32::MAX before toDateTime: {log_patterns_ttl}"
        );
        assert_eq!(
            ttl_stmts
                .iter()
                .filter(|s| s.contains(".log_patterns "))
                .count(),
            1,
            "exactly one MODIFY TTL targets log_patterns"
        );
        assert_eq!(
            setting_stmts
                .iter()
                .filter(|s| s.contains(".log_patterns "))
                .count(),
            1,
            "exactly one MODIFY SETTING targets log_patterns"
        );
    }

    #[test]
    fn guard_skip_ddl_in_init_refuses_when_set() {
        assert!(matches!(
            guard_skip_ddl_in_init(true),
            Err(SchemaError::SkipDdlInInit)
        ));
    }

    #[test]
    fn guard_skip_ddl_in_init_allows_when_unset() {
        assert!(guard_skip_ddl_in_init(false).is_ok());
    }

    #[test]
    fn check_version_accepts_the_minimum_supported_version() {
        assert!(check_version("26.3.0.1").is_ok());
    }

    #[test]
    fn check_version_accepts_newer_versions() {
        assert!(check_version("26.4.0.0").is_ok());
        assert!(check_version("27.1.0.0").is_ok());
    }

    #[test]
    fn check_version_refuses_older_minor_versions() {
        let err = check_version("26.2.9.1").unwrap_err();
        assert!(matches!(err, SchemaError::UnsupportedVersion { .. }));
        assert!(err.to_string().contains("26.2.9.1"));
    }

    #[test]
    fn check_version_refuses_older_major_versions() {
        // The version this floor replaced (issue #376). 24.8 is refused
        // for the reason `SchemaError::UnsupportedVersion` gives: an
        // HTTP-200 mid-stream exception carries no server-declared
        // length there, so it cannot be told from result text.
        let err = check_version("24.8.14.39").unwrap_err();
        assert!(matches!(err, SchemaError::UnsupportedVersion { .. }));
    }

    #[test]
    fn check_version_reports_unparseable_strings_distinctly() {
        let err = check_version("not-a-version").unwrap_err();
        assert!(matches!(err, SchemaError::Version(_)));
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn orphaned_rollup_siblings_flags_only_same_kind_and_excludes_keep() {
        let siblings = names(&[
            "log_metrics_5s",
            "log_metrics_5s_dist",
            "log_metrics_5s_mv",
            "log_metrics_10s",
            "log_metrics_10s_dist",
            "log_metrics_10s_mv",
        ]);
        assert_eq!(
            orphaned_rollup_siblings(&siblings, "log_metrics_10s", RollupObjectKind::Table),
            vec!["log_metrics_5s"]
        );
        assert_eq!(
            orphaned_rollup_siblings(&siblings, "log_metrics_10s_dist", RollupObjectKind::Dist),
            vec!["log_metrics_5s_dist"]
        );
        assert_eq!(
            orphaned_rollup_siblings(&siblings, "log_metrics_10s_mv", RollupObjectKind::Mv),
            vec!["log_metrics_5s_mv"]
        );
    }

    #[test]
    fn orphaned_rollup_siblings_is_empty_on_a_fresh_database() {
        let siblings = names(&["log_metrics_5s", "log_metrics_5s_dist", "log_metrics_5s_mv"]);
        assert!(
            orphaned_rollup_siblings(&siblings, "log_metrics_5s", RollupObjectKind::Table)
                .is_empty()
        );
    }

    /// Issue #97: `StaticClusterOnly` (the `_dist` structured-metadata ALTER,
    /// id 22) and `Dist` are the only cluster-only DDL — both skipped on a
    /// single node — while a plain `Static` ALTER (id 21) always applies.
    #[test]
    fn is_cluster_only_matches_dist_and_static_cluster_only() {
        assert!(is_cluster_only(&Ddl::Dist));
        assert!(is_cluster_only(&Ddl::StaticClusterOnly("ALTER ...")));
        assert!(!is_cluster_only(&Ddl::Static("ALTER ...")));
    }

    #[test]
    fn rollup_object_kind_classifies_base_dist_and_mv_names() {
        assert!(RollupObjectKind::Table.matches("log_metrics_5s"));
        assert!(!RollupObjectKind::Table.matches("log_metrics_5s_dist"));
        assert!(!RollupObjectKind::Table.matches("log_metrics_5s_mv"));
        assert!(RollupObjectKind::Dist.matches("log_metrics_5s_dist"));
        assert!(RollupObjectKind::Mv.matches("log_metrics_5s_mv"));
    }
}
