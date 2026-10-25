//! `cargo xtask bench <scenario>` — the benchmark/evidence-harness
//! subcommand. Two scenarios:
//!
//! - `logs-read` (issue #16) — the M1 logs read-path benchmark. Generates a
//!   deterministic logs corpus (two profiles: `ci`, minutes-scale, wired
//!   into the `schema-it` CI job; and `full`, the parameterized 1 TB/7d/
//!   50-service/5k-stream Tier-2 reference shape, manual-only), runs the
//!   product planner's own generated SQL for the three issue query shapes
//!   plus the §9-mandated label/series discovery shape, captures per-query
//!   `system.query_log` evidence and `EXPLAIN indexes = 1`, and emits JSON
//!   (+ optionally a markdown table body) evidence.
//! - `match-flag-head` (issue #331) — the ClickHouse `match()`
//!   flag-group-head remedy benchmark, committed so the
//!   rewrite-vs-defeat comparison is reproducible on any machine
//!   rather than argued from unshared runs. Printing only, asserts
//!   nothing. See [`match_flag_head`].
//!
//! Example:
//! ```text
//! cargo run -p xtask -- bench logs-read \
//!     --http-url http://127.0.0.1:19123 --database pulsus_bench \
//!     --user default --password "" \
//!     --profile ci --seed 42 --services 50 --streams 500 \
//!     --lines-per-sec 500 --duration-secs 3600 --reps 5 \
//!     --out docs/benchmarks/data/logs-read-ci.json
//! ```
//!
//! **CI regression gates versus this tool** (docs/schemas.md §9's two-tier
//! evidence model): the *asserted* scale-invariant `system.query_log`
//! ratios live in `crates/pulsus-read/tests/query_log_gates.rs`, run
//! per-PR. This tool is the *recorded* numbers + reproducible evidence
//! generator — wall-clock percentiles here are never gated, only recorded
//! (docs/schemas.md §9's two-tier model, edge case #1 of the architect
//! plan: recorded numbers are warm, after an explicit warmup pass).

pub mod dataset;
pub mod logs_hydration;
pub mod match_flag_head;
pub mod metadata_filter;
pub mod queries;
mod query_log;
pub mod report;

use std::time::Duration;

use clap::{Parser, ValueEnum};
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

#[derive(Parser, Debug, Clone)]
pub struct BenchArgs {
    /// `"logs-read"` (issue #16), or another scenario [`run`] names.
    #[arg(default_value = "logs-read")]
    pub scenario: String,
    #[arg(long, value_enum, default_value_t = Profile::Ci)]
    pub profile: Profile,
    /// PRNG seed for the deterministic dataset generator.
    #[arg(long, default_value_t = 0xA5A5_1234_5678_9ABC)]
    pub seed: u64,
    #[arg(long, default_value_t = 50)]
    pub services: u32,
    #[arg(long, default_value_t = 500)]
    pub streams: u32,
    /// Aggregate lines/sec across every stream — total corpus rows are
    /// `lines_per_sec * duration_secs`.
    #[arg(long, default_value_t = 200)]
    pub lines_per_sec: u64,
    #[arg(long, default_value_t = 3600)]
    pub duration_secs: u64,
    #[arg(long, default_value = "http://127.0.0.1:8123")]
    pub http_url: String,
    #[arg(long, default_value = "pulsus_bench")]
    pub database: String,
    #[arg(long, default_value = "default")]
    pub user: String,
    #[arg(long, default_value = "")]
    pub password: String,
    /// Capture per-shard evidence against the `_dist` tables (the manual
    /// `ci/bench-cluster` 4-shard fixture).
    #[arg(long, default_value_t = false)]
    pub dist: bool,
    /// Cluster name used for `_dist` DDL/reader settings when `--dist`.
    #[arg(long, default_value = "pulsus_bench_cluster")]
    pub cluster: String,
    /// Timed repetitions per query, after one discarded warmup pass.
    #[arg(long, default_value_t = 5)]
    pub reps: usize,
    /// Write machine-readable evidence here (JSON).
    #[arg(long)]
    pub out: Option<String>,
    /// Also render a markdown evidence table to this path.
    #[arg(long)]
    pub report_out: Option<String>,

    // --- `logs-hydration` scenario only (issue #35); all defaulted so
    // `logs-read` is unaffected. ---
    /// Selector breadths (streams-per-service), comma-separated
    /// (`logs-hydration` only). **`--profile ci` hard-codes
    /// [`logs_hydration::CI_BREADTHS`] and rejects any override**; only
    /// `--profile full` accepts this override, defaulting to
    /// [`logs_hydration::FULL_BREADTHS`] when unset.
    #[arg(long)]
    pub breadths: Option<String>,
    /// Hidden internal mode (architect plan v3/v5): runs exactly one
    /// `(--rss-variant, --rss-breadth)` query against an already-loaded
    /// database, self-reports `/proc/self/status` RSS over a parent-signalled
    /// window, then exits — never set by a human directly, only spawned by
    /// the parent `logs-hydration` process as a fresh child per RSS
    /// repetition.
    #[arg(long, default_value_t = false, hide = true)]
    pub rss_probe: bool,
    /// `"eager"` | `"late_idx"` | `"late_proj"` — required when
    /// `--rss-probe`.
    #[arg(long, hide = true)]
    pub rss_variant: Option<String>,
    /// The breadth to probe — required when `--rss-probe`.
    #[arg(long, hide = true)]
    pub rss_breadth: Option<u32>,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Profile {
    Ci,
    Full,
}

/// Dispatches on `args.scenario` — `"logs-read"` (issue #16),
/// `"logs-hydration"` (issue #35), `"match-flag-head"` (issue #331) or
/// `"logql-metadata-filter"`; any other value is a hard error.
pub async fn run(args: BenchArgs) -> anyhow::Result<()> {
    match args.scenario.as_str() {
        "logs-read" => run_logs_read(args).await,
        "logs-hydration" => logs_hydration::run(args).await,
        "match-flag-head" => match_flag_head::run(args).await,
        "logql-metadata-filter" => metadata_filter::run(args).await,
        other => anyhow::bail!(
            "unknown bench scenario {other:?} (expected \"logs-read\", \
             \"logs-hydration\", \"match-flag-head\" or \
             \"logql-metadata-filter\")"
        ),
    }
}

async fn run_logs_read(args: BenchArgs) -> anyhow::Result<()> {
    let (server, http_port) = parse_http_url(&args.http_url)?;
    let admin_cfg = ChConnConfig {
        server,
        http_port,
        database: "default".to_string(),
        user: args.user.clone(),
        password: args.password.clone(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(600),
        ..ChConnConfig::default()
    };
    let admin = ChClient::new(admin_cfg.clone()).await?;
    // `--dist` drops the database on every shard: a rerun must not find an
    // earlier run's rows on the other shards.
    let drop_sql = if args.dist {
        format!(
            "DROP DATABASE IF EXISTS {} ON CLUSTER {} SYNC",
            args.database, args.cluster
        )
    } else {
        format!("DROP DATABASE IF EXISTS {}", args.database)
    };
    admin
        .execute(&drop_sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await?;

    let cluster = args.dist.then(|| args.cluster.clone());
    let schema_ctx = RenderCtx {
        db: args.database.clone(),
        cluster,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
        metrics_landing_retention_hours: 6,
        metrics_dedup_window: 10_000,
        log_landing_retention_hours: 6,
        log_dedup_window: 10_000,
        trace_landing_retention_hours: 6,
        trace_dedup_window: 10_000,
        trace_indexed_attributes: Vec::new(),
        graph_job_max_rows: pulsus_schema::GRAPH_JOB_MAX_ROWS,
        rollup_settle_ms: pulsus_schema::ROLLUP_SETTLE_MS,
        rollup_levels: pulsus_schema::DEFAULT_ROLLUP_LEVELS.to_vec(),
    };
    eprintln!(
        "=== initializing schema (db={}, cluster={:?}) ===",
        args.database, schema_ctx.cluster
    );
    run_init(&admin, &schema_ctx).await?;
    let landing_shards = if args.dist {
        create_landing_route(&admin, &args.database, &args.cluster).await?
    } else {
        0
    };

    let mut data_cfg = admin_cfg.clone();
    data_cfg.database = args.database.clone();
    let client = ChClient::new(data_cfg).await?;

    let spec = dataset::DatasetSpec {
        profile: args.profile,
        seed: args.seed,
        services: args.services,
        streams: args.streams,
        lines_per_sec: args.lines_per_sec,
        duration_secs: args.duration_secs,
        dist: args.dist,
        landing_shards,
    };
    eprintln!(
        "=== generating dataset (profile={:?}, seed={}, services={}, streams={}, \
         lines_per_sec={}, duration_secs={}) ===",
        args.profile,
        args.seed,
        args.services,
        args.streams,
        args.lines_per_sec,
        args.duration_secs
    );
    let summary = dataset::load(&client, &spec).await?;
    eprintln!(
        "dataset: {} rows ({} needle rows) across {} streams / {} services, loaded in {} ms",
        summary.total_rows,
        summary.needle_rows,
        summary.streams,
        summary.services,
        summary.load_elapsed_ms
    );

    eprintln!(
        "=== running query set (reps={}, dist={}) ===",
        args.reps, args.dist
    );
    let evidence = queries::run_all(
        &client,
        &args.database,
        &summary,
        args.reps,
        args.dist,
        &args.cluster,
    )
    .await?;
    for e in &evidence {
        eprintln!(
            "{:>32}: wall p50={:.1}ms p95={:.1}ms p99={:.1}ms returned={} read_rows={} \
             selected_marks={}/{} read_bytes={} mem={}",
            e.name,
            e.wall_ms_p50,
            e.wall_ms_p95,
            e.wall_ms_p99,
            e.returned_rows,
            e.read_rows,
            e.selected_marks,
            e.total_marks,
            e.read_bytes,
            e.memory_usage,
        );
    }

    let report = report::BenchReport {
        profile: args.profile,
        seed: args.seed,
        dist: args.dist,
        cluster: args.dist.then_some(args.cluster.clone()),
        dataset: summary,
        queries: evidence,
    };
    if let Some(out) = &args.out {
        std::fs::write(out, serde_json::to_string_pretty(&report)?)?;
        eprintln!("wrote {out}");
    }
    if let Some(report_out) = &args.report_out {
        std::fs::write(report_out, report::render_markdown(&report))?;
        eprintln!("wrote {report_out}");
    }

    Ok(())
}

/// `--dist` (issue #603): reads the cluster's shard count `n`, requires its
/// shards to be numbered `1..=n` (what `insert_shard_id` addresses), and
/// creates the bench-only `Distributed` table over `log_landing` with no
/// sharding key that the loader lands each block through.
async fn create_landing_route(admin: &ChClient, db: &str, cluster: &str) -> anyhow::Result<u32> {
    use futures::StreamExt;
    #[derive(pulsus_clickhouse::Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct ShardSpan {
        n: u32,
        lo: u32,
        hi: u32,
    }
    let sql = format!(
        "SELECT toUInt32(count(DISTINCT shard_num)) AS n, toUInt32(min(shard_num)) AS lo, \
         toUInt32(max(shard_num)) AS hi FROM system.clusters WHERE cluster = '{cluster}'"
    );
    let mut stream = admin
        .query_stream::<ShardSpan>(&sql, &QuerySettings::new())
        .await?;
    let span = stream
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("no row from system.clusters for {cluster:?}"))??;
    drop(stream);
    anyhow::ensure!(
        span.n >= 1 && span.lo == 1 && span.hi == span.n,
        "cluster {cluster:?} has {} shards numbered {}..={}; the loader addresses shards 1..=n \
         with insert_shard_id",
        span.n,
        span.lo,
        span.hi
    );
    admin
        .execute(
            &format!(
                "CREATE TABLE {db}.{} ON CLUSTER {cluster} AS {db}.log_landing \
                 ENGINE = Distributed('{cluster}', '{db}', 'log_landing')",
                dataset::LANDING_ROUTE_TABLE
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await?;
    Ok(span.n)
}

/// Splits `--http-url` (e.g. `http://127.0.0.1:19123`) into
/// `pulsus_clickhouse::ChConnConfig`'s separate `server`/`http_port`
/// fields.
fn parse_http_url(url: &str) -> anyhow::Result<(String, u16)> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .ok_or_else(|| anyhow::anyhow!("--http-url must start with http:// or https://: {url}"))?;
    let host_port = rest.split('/').next().unwrap_or(rest);
    let (host, port) = host_port
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("--http-url must include a port, e.g. host:8123: {url}"))?;
    Ok((host.to_string(), port.parse()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_http_url_splits_host_and_port() {
        assert_eq!(
            parse_http_url("http://127.0.0.1:19123").unwrap(),
            ("127.0.0.1".to_string(), 19123)
        );
    }

    #[test]
    fn parse_http_url_accepts_https() {
        assert_eq!(
            parse_http_url("https://ch.internal:8443").unwrap(),
            ("ch.internal".to_string(), 8443)
        );
    }

    #[test]
    fn parse_http_url_rejects_a_missing_scheme() {
        assert!(parse_http_url("127.0.0.1:19123").is_err());
    }

    #[test]
    fn parse_http_url_rejects_a_missing_port() {
        assert!(parse_http_url("http://127.0.0.1").is_err());
    }
}
