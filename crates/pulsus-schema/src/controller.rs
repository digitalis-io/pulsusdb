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

async fn apply_migration(
    client: &ChClient,
    ctx: &RenderCtx,
    m: &crate::catalog::Migration,
) -> Result<(), SchemaError> {
    // `_dist` wrappers only exist in clustered mode; skip entirely (never
    // attempted, never recorded) when no cluster is configured — the id
    // stays reserved and gets applied the first time clustering is enabled.
    if matches!(m.ddl, Ddl::Dist) && ctx.cluster.is_none() {
        return Ok(());
    }

    let name = render::render_name(m.name, ctx);
    let tmpl = match &m.ddl {
        Ddl::Static(tmpl) => (*tmpl).to_string(),
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
        Ddl::Static(_) => name.clone(),
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
/// from the live `eprintln!` wrapper below so the selection logic is
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

/// Warns (`eprintln!`; matches the crate's existing style — migrates to
/// `tracing` when #6 lands the subscriber, per task-manager resolution on
/// issue #5) about any `log_metrics_*` object of `kind` left behind by a
/// prior `PULSUS_LOG_ROLLUP_RESOLUTION` value. Data objects are never
/// auto-dropped (issue #5 fix plan F1) — this is purely an operator-visible
/// heads-up so orphaned storage doesn't go unnoticed.
async fn warn_orphaned_rollup_siblings(
    client: &ChClient,
    ctx: &RenderCtx,
    keep: &str,
    kind: RollupObjectKind,
) -> Result<(), SchemaError> {
    let siblings = list_tables_with_prefix(client, ctx, "log_metrics_").await?;
    for sibling in orphaned_rollup_siblings(&siblings, keep, kind) {
        eprintln!(
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

/// Applies the current `{{retention_days}}`-derived TTL to both raw sample
/// tables (docs/schemas.md §2.1/§3.1). `ALTER TABLE ... MODIFY TTL` is
/// naturally idempotent (re-applying the same expression is a no-op), so
/// this is safe both from `run_init` (applied once) and
/// [`crate::rotation::spawn_rotation`] (applied on every tick, so a changed
/// `PULSUS_RETENTION_DAYS` propagates without a restart).
///
/// `ttl_only_drop_parts` is a per-table `MergeTree` *engine* setting, not a
/// query-level one — `MODIFY TTL <expr> SETTINGS ttl_only_drop_parts = 1`
/// in one statement is rejected by the server (`UNKNOWN_SETTING`: it tries
/// to apply the name as a query setting). It is instead reasserted with its
/// own `MODIFY SETTING` statement, immediately after the TTL change, so an
/// operator who manually altered it away is corrected on the next rotation
/// tick too.
pub async fn apply_ttl(client: &ChClient, ctx: &RenderCtx) -> Result<(), SchemaError> {
    let stmts = [
        "ALTER TABLE {{db}}.metric_samples{{on_cluster}} MODIFY TTL \
         toDateTime(fromUnixTimestamp64Milli(unix_milli)) + INTERVAL {{retention_days}} DAY DELETE;",
        "ALTER TABLE {{db}}.metric_samples{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
        "ALTER TABLE {{db}}.log_samples{{on_cluster}} MODIFY TTL \
         toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL {{retention_days}} DAY DELETE;",
        "ALTER TABLE {{db}}.log_samples{{on_cluster}} MODIFY SETTING ttl_only_drop_parts = 1;",
    ];
    for stmt in stmts {
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

    #[test]
    fn rollup_object_kind_classifies_base_dist_and_mv_names() {
        assert!(RollupObjectKind::Table.matches("log_metrics_5s"));
        assert!(!RollupObjectKind::Table.matches("log_metrics_5s_dist"));
        assert!(!RollupObjectKind::Table.matches("log_metrics_5s_mv"));
        assert!(RollupObjectKind::Dist.matches("log_metrics_5s_dist"));
        assert!(RollupObjectKind::Mv.matches("log_metrics_5s_mv"));
    }
}
