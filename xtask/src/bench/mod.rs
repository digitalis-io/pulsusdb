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
//! - `metrics-labels` (issue #34) — the M2 label-resolution benchmark:
//!   benchmarks the docs/schemas.md §2.1 strategy ladder's three paths
//!   (cache matcher, SQL fallback, prototype `metric_series_idx`) on a
//!   deterministic `metric_series` corpus. See [`metrics_labels`].
//! - `traces-read` (issue #57 AC4) — the M4 traces read-path
//!   shard-locality evidence harness on the 2-shard
//!   `ci/clickhouse-cluster` fixture: per-stage per-shard
//!   `system.query_log` evidence for the two-phase TraceQL search +
//!   trace-by-ID, verdicted (hard errors) against a client-computed
//!   `cityHash64(trace_id) % total_weight` roster. See [`traces_read`].
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
pub mod metrics_labels;
pub mod queries;
mod query_log;
pub mod report;
pub mod traces_read;

use std::time::Duration;

use clap::{Parser, ValueEnum};
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_schema::{RenderCtx, run_init};

#[derive(Parser, Debug, Clone)]
pub struct BenchArgs {
    /// `"logs-read"` (issue #16) or `"metrics-labels"` (issue #34).
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

    // --- `metrics-labels` scenario only (issue #34); all defaulted so
    // `logs-read` is unaffected. ---
    /// Per-metric series cardinalities, comma-separated (`metrics-labels`
    /// only). **`--profile ci` hard-codes a fixed small set and rejects any
    /// override** (never silently runs the 5M-series shape in CI); only
    /// `--profile full` accepts this override, defaulting to
    /// `10000,500000,5000000` when unset. See
    /// [`metrics_labels::CI_CARDINALITIES`]/[`metrics_labels::FULL_CARDINALITIES`].
    #[arg(long)]
    pub metric_cardinalities: Option<String>,
    /// Activity-bucket sizes to benchmark, comma-separated (`1h`/`1d`
    /// tokens only, `metrics-labels` only).
    #[arg(long, default_value = "1h,1d")]
    pub activity_buckets: String,
    /// The `metric_series` corpus window in hours (`metrics-labels` only) —
    /// default matches `PULSUS_CACHE_WINDOW`'s own default (24h).
    #[arg(long, default_value_t = 24)]
    pub corpus_window_hours: u64,
    /// `PULSUS_CACHE_MAX_SERIES` override for the benchmarked
    /// [`pulsus_read::metrics::LabelCache`] (`metrics-labels` only) —
    /// raised well above the product default so the in-process matcher
    /// actually evaluates every benchmarked selector instead of degrading
    /// to the SQL fallback before doing any work (architect plan edge case
    /// 1).
    #[arg(long, default_value_t = 10_000_000)]
    pub cache_max_series: u64,
    /// Timed repetitions of the pure in-process `SeriesResolver::resolve`
    /// call, per selector/cardinality (`metrics-labels` only, path 1).
    #[arg(long, default_value_t = 1_000)]
    pub matcher_reps: usize,

    // --- `logs-hydration` scenario only (issue #35); all defaulted so
    // `logs-read`/`metrics-labels` are unaffected. ---
    /// Selector breadths (streams-per-service), comma-separated
    /// (`logs-hydration` only). **`--profile ci` hard-codes
    /// [`logs_hydration::CI_BREADTHS`] and rejects any override**; only
    /// `--profile full` accepts this override, defaulting to
    /// [`logs_hydration::FULL_BREADTHS`] when unset — same posture as
    /// `--metric-cardinalities`.
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
/// `"metrics-labels"` (issue #34), `"logs-hydration"` (issue #35), or
/// `"traces-read"` (issue #57); any other value is a hard error.
pub async fn run(args: BenchArgs) -> anyhow::Result<()> {
    match args.scenario.as_str() {
        "logs-read" => run_logs_read(args).await,
        "metrics-labels" => metrics_labels::run(args).await,
        "logs-hydration" => logs_hydration::run(args).await,
        "traces-read" => traces_read::run(args).await,
        other => anyhow::bail!(
            "unknown bench scenario {other:?} (expected \"logs-read\", \"metrics-labels\", \
             \"logs-hydration\", or \"traces-read\")"
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
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {}", args.database),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await?;

    let cluster = args.dist.then(|| args.cluster.clone());
    let schema_ctx = RenderCtx {
        db: args.database.clone(),
        cluster,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };
    eprintln!(
        "=== initializing schema (db={}, cluster={:?}) ===",
        args.database, schema_ctx.cluster
    );
    run_init(&admin, &schema_ctx).await?;

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
