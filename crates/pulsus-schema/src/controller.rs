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
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings, Row};

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

/// Parses a ClickHouse `SELECT version()` string (e.g. `24.8.14.10`) and
/// refuses anything older than 24.8 (docs/schemas.md §8). Pure and
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
    if (major, minor) < (24, 8) {
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
async fn fetch_version(client: &ChClient) -> Result<String, SchemaError> {
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
    let version = fetch_version(client).await?;
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
const TTL_STMTS: [&str; 14] = [
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
    for stmt in TTL_STMTS {
        let rendered = render::substitute_tokens(stmt, ctx);
        client
            .execute(&rendered, &QuerySettings::new(), Idempotency::Idempotent)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        };
        let trace_ttl_stmts: Vec<String> = TTL_STMTS
            .iter()
            .filter(|s| s.contains("MODIFY TTL") && s.contains("trace_"))
            .map(|s| render::substitute_tokens(s, &ctx))
            .collect();
        assert_eq!(
            trace_ttl_stmts.len(),
            3,
            "exactly trace_spans + trace_attrs_idx + trace_edges carry a trace MODIFY TTL"
        );
        for stmt in &trace_ttl_stmts {
            assert!(
                stmt.contains("least(intDiv(timestamp_ns, 1000000000) + "),
                "trace TTL must use the clamped Int64-seconds form: {stmt}"
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
        };
        let rendered: Vec<String> = TTL_STMTS
            .iter()
            .map(|s| render::substitute_tokens(s, &ctx))
            .collect();

        let ttl_stmts: Vec<&String> = rendered
            .iter()
            .filter(|s| s.contains("MODIFY TTL"))
            .collect();
        assert_eq!(ttl_stmts.len(), 7, "seven retained tables carry a TTL");
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
        assert_eq!(setting_stmts.len(), 7, "one MODIFY SETTING per table");
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
        assert!(check_version("24.8.0.1").is_ok());
    }

    #[test]
    fn check_version_accepts_newer_versions() {
        assert!(check_version("25.1.3.9").is_ok());
        assert!(check_version("24.9.0.0").is_ok());
    }

    #[test]
    fn check_version_refuses_older_minor_versions() {
        let err = check_version("24.7.9.1").unwrap_err();
        assert!(matches!(err, SchemaError::UnsupportedVersion { .. }));
        assert!(err.to_string().contains("24.7.9.1"));
    }

    #[test]
    fn check_version_refuses_older_major_versions() {
        let err = check_version("24.3.2.1").unwrap_err();
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
