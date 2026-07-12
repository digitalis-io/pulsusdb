//! `xtask` — benchmark/regression harness for the ClickHouse client spike
//! (issue #3). Kept out of the shipping build graph (docs/decisions/0001).
//!
//! Example:
//! ```text
//! cargo run -p xtask -- ch-bench \
//!     --native-addr 127.0.0.1:9000 --http-url http://127.0.0.1:8123 \
//!     --database default --user default --password "" \
//!     --scenario all --rows 1000000 --reps 5 --out /tmp/ch-bench.json
//! ```

mod ch_bench;

use clap::{Parser, Subcommand};
use serde::Serialize;

use ch_bench::{
    ChCandidate, KlCandidate, aggstate::AggstateReport, ddl::DdlReport, fetch::FetchReport,
    insert::InsertReport, pool::PoolReport, tls::TlsReport,
};

#[derive(Parser)]
#[command(name = "xtask")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the ClickHouse client comparative benchmark.
    ChBench(ChBenchArgs),
}

#[derive(Parser)]
struct ChBenchArgs {
    /// Native protocol address, e.g. `127.0.0.1:9000` (klickhouse).
    #[arg(long, default_value = "127.0.0.1:9000")]
    native_addr: String,
    /// HTTP interface URL, e.g. `http://127.0.0.1:8123` (clickhouse).
    #[arg(long, default_value = "http://127.0.0.1:8123")]
    http_url: String,
    #[arg(long, default_value = "default")]
    database: String,
    #[arg(long, default_value = "default")]
    user: String,
    #[arg(long, default_value = "")]
    password: String,
    /// `all|insert|fetch|aggstate|ddl|pool`. `tls` is exercised separately
    /// (see xtask/docker/gen-certs.sh) and not part of `all`.
    #[arg(long, default_value = "all")]
    scenario: String,
    /// Rows per shape for the insert/fetch scenarios.
    #[arg(long, default_value_t = 1_000_000)]
    rows: u64,
    /// Block size (rows per insert call).
    #[arg(long, default_value_t = 200_000)]
    block_rows: u64,
    /// Repetitions per scenario.
    #[arg(long, default_value_t = 5)]
    reps: usize,
    /// Concurrent connections for the pool scenario.
    #[arg(long, default_value_t = 8)]
    pool_size: usize,
    /// Write machine-readable results here (JSON).
    #[arg(long)]
    out: Option<String>,

    /// HTTPS interface URL for the `tls` scenario, e.g. `https://127.0.0.1:8443`.
    #[arg(long)]
    https_url: Option<String>,
    /// Native-over-TLS address for the `tls` scenario, e.g. `127.0.0.1:9440`.
    #[arg(long)]
    native_tls_addr: Option<String>,
    /// TLS server name (must match the cert's CN/SAN from gen-certs.sh).
    #[arg(long, default_value = "localhost")]
    tls_server_name: String,
    /// Path to `gen-certs.sh`'s `ca.crt`, for the verified-mode TLS scenario.
    #[arg(long, default_value = "xtask/docker/certs/ca.crt")]
    tls_ca_cert: String,
}

#[derive(Default, Serialize)]
struct Results {
    insert: Vec<InsertReport>,
    fetch: Vec<FetchReport>,
    aggstate: Vec<AggstateReport>,
    ddl: Vec<DdlReport>,
    pool: Vec<PoolReport>,
    tls: Vec<TlsReport>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Both candidate crates pull in rustls; with more than one crypto
    // provider feature reachable in the dependency graph, rustls requires
    // an explicit process-wide default (the `tls` scenario is the only
    // consumer of this).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();
    match cli.command {
        Command::ChBench(args) => run_ch_bench(args).await,
    }
}

async fn run_ch_bench(args: ChBenchArgs) -> anyhow::Result<()> {
    let mut results = Results::default();
    let run_insert = matches!(args.scenario.as_str(), "all" | "insert");
    let run_fetch = matches!(args.scenario.as_str(), "all" | "fetch");
    let run_aggstate = matches!(args.scenario.as_str(), "all" | "aggstate");
    let run_ddl = matches!(args.scenario.as_str(), "all" | "ddl");
    let run_pool = matches!(args.scenario.as_str(), "all" | "pool");
    let run_tls = args.scenario.as_str() == "tls";

    if run_tls {
        run_tls_scenario(&args, &mut results).await?;
        if let Some(out) = &args.out {
            std::fs::write(out, serde_json::to_string_pretty(&results)?)?;
            eprintln!("wrote {out}");
        }
        return Ok(());
    }

    let ch = ChCandidate::connect(&args.http_url, &args.database, &args.user, &args.password);
    let kl = KlCandidate::connect(
        &args.native_addr,
        &args.database,
        &args.user,
        &args.password,
    )
    .await?;

    if run_insert {
        eprintln!("=== insert scenario ===");
        for (name, table) in [
            ("clickhouse", "bench_metric_samples_ch"),
            ("klickhouse", "bench_metric_samples_kl"),
        ] {
            let r = if name == "clickhouse" {
                ch_bench::insert::bench_metric_insert(
                    &ch,
                    table,
                    args.rows,
                    args.block_rows,
                    args.reps,
                )
                .await?
            } else {
                ch_bench::insert::bench_metric_insert(
                    &kl,
                    table,
                    args.rows,
                    args.block_rows,
                    args.reps,
                )
                .await?
            };
            eprintln!(
                "{:>10} metric insert: p50={:.1}ms p95={:.1}ms rows/s(p50)={:.0} MiB/s(p50)={:.1} parts={}",
                r.crate_name,
                r.stats.p50_ms,
                r.stats.p95_ms,
                r.rows_per_sec_p50,
                r.mib_per_sec_p50,
                r.parts_after
            );
            results.insert.push(r);
        }
        for (name, table) in [
            ("clickhouse", "bench_log_samples_ch"),
            ("klickhouse", "bench_log_samples_kl"),
        ] {
            let r = if name == "clickhouse" {
                ch_bench::insert::bench_log_insert(
                    &ch,
                    table,
                    args.rows,
                    args.block_rows,
                    args.reps,
                )
                .await?
            } else {
                ch_bench::insert::bench_log_insert(
                    &kl,
                    table,
                    args.rows,
                    args.block_rows,
                    args.reps,
                )
                .await?
            };
            eprintln!(
                "{:>10} log insert: p50={:.1}ms p95={:.1}ms rows/s(p50)={:.0} MiB/s(p50)={:.1} parts={}",
                r.crate_name,
                r.stats.p50_ms,
                r.stats.p95_ms,
                r.rows_per_sec_p50,
                r.mib_per_sec_p50,
                r.parts_after
            );
            results.insert.push(r);
        }
    }

    if run_fetch {
        eprintln!("=== fetch scenario ===");
        // Reuses the metric bench tables populated by the insert scenario.
        let metric_name = "bench_metric_0000";
        let r = ch_bench::fetch::bench_metric_fetch(
            &ch,
            "bench_metric_samples_ch",
            metric_name,
            args.reps,
        )
        .await?;
        eprintln!(
            "{:>10} fetch: p50={:.1}ms rows/s(p50)={:.0} rows={} peak_rss_kib={:?}",
            r.crate_name, r.stats.p50_ms, r.rows_per_sec_p50, r.rows, r.peak_rss_kib
        );
        results.fetch.push(r);
        let r = ch_bench::fetch::bench_metric_fetch(
            &kl,
            "bench_metric_samples_kl",
            metric_name,
            args.reps,
        )
        .await?;
        eprintln!(
            "{:>10} fetch: p50={:.1}ms rows/s(p50)={:.0} rows={} peak_rss_kib={:?}",
            r.crate_name, r.stats.p50_ms, r.rows_per_sec_p50, r.rows, r.peak_rss_kib
        );
        results.fetch.push(r);
    }

    if run_ddl {
        eprintln!("=== ddl scenario ===");
        let r = ch_bench::ddl::bench_ddl(
            &ch,
            "bench_metric_samples_ch",
            "bench_metric_samples_5m_ch",
            "bench_metric_samples_5m_mv_ch",
            4,
        )
        .await;
        eprintln!(
            "{:>10} ddl: reliable={} {:?}",
            r.crate_name,
            r.reliable(),
            r.error
        );
        results.ddl.push(r);
        let r = ch_bench::ddl::bench_ddl(
            &kl,
            "bench_metric_samples_kl",
            "bench_metric_samples_5m_kl",
            "bench_metric_samples_5m_mv_kl",
            4,
        )
        .await;
        eprintln!(
            "{:>10} ddl: reliable={} {:?}",
            r.crate_name,
            r.reliable(),
            r.error
        );
        results.ddl.push(r);
    }

    if run_aggstate {
        eprintln!("=== aggstate scenario ===");
        let r = ch_bench::aggstate::bench_aggstate(
            &ch,
            "aggstate_raw_ch",
            "aggstate_5m_ch",
            "aggstate_5m_mv_ch",
        )
        .await;
        eprintln!(
            "{:>10} aggstate: ok={} actual={:?} err={:?}",
            r.crate_name, r.ok, r.actual, r.error
        );
        results.aggstate.push(r);
        let r = ch_bench::aggstate::bench_aggstate(
            &kl,
            "aggstate_raw_kl",
            "aggstate_5m_kl",
            "aggstate_5m_mv_kl",
        )
        .await;
        eprintln!(
            "{:>10} aggstate: ok={} actual={:?} err={:?}",
            r.crate_name, r.ok, r.actual, r.error
        );
        results.aggstate.push(r);
    }

    if run_pool {
        eprintln!("=== pool scenario ===");
        let pool_rows = args.rows.min(200_000);
        let single_ch = ch_bench::pool::single_conn_rows_per_sec(
            &ch,
            "bench_pool_ch",
            pool_rows,
            args.block_rows,
        )
        .await?;
        let mut conns_ch = Vec::with_capacity(args.pool_size);
        for _ in 0..args.pool_size {
            conns_ch.push(ChCandidate::connect(
                &args.http_url,
                &args.database,
                &args.user,
                &args.password,
            ));
        }
        let concurrent_ch = ch_bench::pool::concurrent_rows_per_sec(
            "bench_pool_ch",
            conns_ch,
            pool_rows,
            args.block_rows,
        )
        .await?;
        let r = PoolReport {
            crate_name: "clickhouse",
            connections: args.pool_size,
            total_rows: pool_rows * args.pool_size as u64,
            single_conn_rows_per_sec: single_ch,
            concurrent_rows_per_sec: concurrent_ch,
            speedup: concurrent_ch / single_ch,
        };
        eprintln!(
            "{:>10} pool: single={:.0} rows/s concurrent={:.0} rows/s speedup={:.2}x",
            r.crate_name, r.single_conn_rows_per_sec, r.concurrent_rows_per_sec, r.speedup
        );
        results.pool.push(r);

        let single_kl = ch_bench::pool::single_conn_rows_per_sec(
            &kl,
            "bench_pool_kl",
            pool_rows,
            args.block_rows,
        )
        .await?;
        let mut conns_kl = Vec::with_capacity(args.pool_size);
        for _ in 0..args.pool_size {
            conns_kl.push(
                KlCandidate::connect(
                    &args.native_addr,
                    &args.database,
                    &args.user,
                    &args.password,
                )
                .await?,
            );
        }
        let concurrent_kl = ch_bench::pool::concurrent_rows_per_sec(
            "bench_pool_kl",
            conns_kl,
            pool_rows,
            args.block_rows,
        )
        .await?;
        let r = PoolReport {
            crate_name: "klickhouse",
            connections: args.pool_size,
            total_rows: pool_rows * args.pool_size as u64,
            single_conn_rows_per_sec: single_kl,
            concurrent_rows_per_sec: concurrent_kl,
            speedup: concurrent_kl / single_kl,
        };
        eprintln!(
            "{:>10} pool: single={:.0} rows/s concurrent={:.0} rows/s speedup={:.2}x",
            r.crate_name, r.single_conn_rows_per_sec, r.concurrent_rows_per_sec, r.speedup
        );
        results.pool.push(r);
    }

    if let Some(out) = args.out {
        std::fs::write(&out, serde_json::to_string_pretty(&results)?)?;
        eprintln!("wrote {out}");
    }

    Ok(())
}

/// Runs the `tls` scenario: one insert + one fetch per candidate, per
/// verify mode, against a TLS-enabled ClickHouse (see
/// `xtask/docker/gen-certs.sh` + `docker-compose.ch.yml`'s `clickhouse-tls`
/// service). Requires `--https-url` and `--native-tls-addr`.
async fn run_tls_scenario(args: &ChBenchArgs, results: &mut Results) -> anyhow::Result<()> {
    let https_url = args
        .https_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--https-url is required for --scenario tls"))?;
    let native_tls_addr = args
        .native_tls_addr
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--native-tls-addr is required for --scenario tls"))?;

    eprintln!("=== tls scenario ===");

    // skip-verify mode
    let ch_skip = ch_bench::tls::ch_candidate_over_tls(
        https_url,
        &args.database,
        &args.user,
        &args.password,
        ch_bench::tls::danger_skip_verify_tls_config()
            .as_ref()
            .clone(),
    );
    let r = ch_bench::tls::bench_tls_roundtrip(&ch_skip, "bench_tls_ch", "skip-verify").await;
    eprintln!(
        "{:>10} tls[skip-verify]: insert_ok={} fetch_ok={} err={:?}",
        r.crate_name, r.insert_ok, r.fetch_ok, r.error
    );
    results.tls.push(r);

    let kl_skip = ch_bench::tls::kl_candidate_over_tls(
        native_tls_addr,
        &args.database,
        &args.user,
        &args.password,
        &args.tls_server_name,
        ch_bench::tls::danger_skip_verify_tls_config(),
    )
    .await?;
    let r = ch_bench::tls::bench_tls_roundtrip(&kl_skip, "bench_tls_kl", "skip-verify").await;
    eprintln!(
        "{:>10} tls[skip-verify]: insert_ok={} fetch_ok={} err={:?}",
        r.crate_name, r.insert_ok, r.fetch_ok, r.error
    );
    results.tls.push(r);

    // verified mode, against the benchmark's self-signed CA
    if std::path::Path::new(&args.tls_ca_cert).exists() {
        let verified_cfg = ch_bench::tls::verified_tls_config(&args.tls_ca_cert)?;
        let ch_verified = ch_bench::tls::ch_candidate_over_tls(
            https_url,
            &args.database,
            &args.user,
            &args.password,
            verified_cfg.clone(),
        );
        let r = ch_bench::tls::bench_tls_roundtrip(&ch_verified, "bench_tls_ch", "verified").await;
        eprintln!(
            "{:>10} tls[verified]: insert_ok={} fetch_ok={} err={:?}",
            r.crate_name, r.insert_ok, r.fetch_ok, r.error
        );
        results.tls.push(r);

        let kl_verified = ch_bench::tls::kl_candidate_over_tls(
            native_tls_addr,
            &args.database,
            &args.user,
            &args.password,
            &args.tls_server_name,
            std::sync::Arc::new(verified_cfg),
        )
        .await?;
        let r = ch_bench::tls::bench_tls_roundtrip(&kl_verified, "bench_tls_kl", "verified").await;
        eprintln!(
            "{:>10} tls[verified]: insert_ok={} fetch_ok={} err={:?}",
            r.crate_name, r.insert_ok, r.fetch_ok, r.error
        );
        results.tls.push(r);
    } else {
        eprintln!(
            "skipping verified-mode TLS scenario: {} not found (run xtask/docker/gen-certs.sh first)",
            args.tls_ca_cert
        );
    }

    Ok(())
}
