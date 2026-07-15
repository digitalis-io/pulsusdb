//! `cargo xtask bench logs-hydration` — the late-label-hydration
//! investigation (issue #35). A **benchmark-first investigation, not a
//! product change**: the three-stage log read path (docs/schemas.md §3.2)
//! hydrates the `labels` column for every selector-matched stream in stage
//! 2 before stage 3's `LIMIT` applies — a selector matching N streams with
//! `limit=100` therefore parses N label blobs to return 100 rows. This
//! scenario measures the current pipeline (`eager`) against two bench-local
//! late-hydration prototypes (`late_idx`/`late_proj`) that derive the
//! stage-3 `service` set cheaply and hydrate labels only for the ≤limit
//! result fingerprints — pure SQL over the existing
//! `pulsus_read::logql::sql` builders, **no product read-path change**.
//!
//! Ships as a **sibling scenario** to `logs-read` (mirroring #34's
//! `metrics-labels`) — not a new row in `logs-read`'s fixed four-shape
//! matrix — so #16's committed Tier-1 evidence and its
//! `query_log_gates.rs` ratio gates stay byte-stable while this A/B
//! investigation gets its own corpus-breadth sweep, correctness gate, and
//! decision report.
//!
//! **Two profiles:**
//! - `--profile ci` — [`CI_BREADTHS`] (`[1_000, 10_000]`), always; an
//!   explicit `--breadths` override is a hard error.
//! - `--profile full` — the explicit `--breadths` override if given, else
//!   [`FULL_BREADTHS`] (`[1_000, 10_000, 50_000]`) — manual-only, the
//!   50,000-breadth cell the materiality verdict is evaluated at.
//!
//! Module layout: [`paths`] (the three path runners, the 6-round
//! Latin-square rotation, the RSS-probe protocol, the correctness gate),
//! [`report`] (evidence schema, markdown rendering, the verdict predicate).
//!
//! **Hidden RSS-probe child mode** (architect plan v5 [R1]): `--rss-probe
//! --rss-variant <V> --rss-breadth <N>` runs exactly one query against an
//! already-loaded database and self-reports RSS over a parent-signalled
//! window, then exits — spawned only by this module's own parent flow
//! ([`paths::run_breadth`]), never invoked by a human directly.

pub mod paths;
pub mod report;
pub mod rss_probe;

use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_schema::{RenderCtx, run_init};

use super::dataset::{self, BroadDatasetSpec};
use super::{BenchArgs, Profile, parse_http_url};
use report::{CloseoutRef, LogsHydrationReport, Variant, evaluate_verdict};

/// `--profile ci`'s fixed, hard-coded breadth set — small enough for a
/// per-PR `schema-it` smoke step (mirrors `metrics_labels::CI_CARDINALITIES`'
/// posture).
pub const CI_BREADTHS: [u32; 2] = [1_000, 10_000];
/// `--profile full`'s default breadth set when `--breadths` is not given —
/// the sweep the materiality verdict is evaluated over (`LOW_BREADTH`,
/// `HIGH_BREADTH` from [`report`]).
pub const FULL_BREADTHS: [u32; 3] = [1_000, 10_000, 50_000];

fn resolve_breadths(args: &BenchArgs) -> anyhow::Result<Vec<u32>> {
    match args.profile {
        Profile::Ci => {
            anyhow::ensure!(
                args.breadths.is_none(),
                "--breadths requires --profile full; --profile ci always uses the fixed set \
                 {CI_BREADTHS:?} (never the 50,000-breadth verdict-evaluation shape in CI)"
            );
            Ok(CI_BREADTHS.to_vec())
        }
        Profile::Full => match &args.breadths {
            Some(s) => parse_u32_csv(s),
            None => Ok(FULL_BREADTHS.to_vec()),
        },
    }
}

fn parse_u32_csv(s: &str) -> anyhow::Result<Vec<u32>> {
    s.split(',')
        .map(|tok| {
            tok.trim()
                .parse::<u32>()
                .map_err(|e| anyhow::anyhow!("--breadths: invalid integer {tok:?}: {e}"))
        })
        .collect()
}

/// Wall-clock now, nanoseconds since the Unix epoch — read **once**, here,
/// before any breadth's corpus is generated (the frozen reference instant
/// every breadth pass shares — see `dataset::load_broad_tier`'s doc
/// comment for why this is what makes the fixed result-bearing set
/// byte-identical across breadths).
fn now_unix_ns() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)
}

pub async fn run(args: BenchArgs) -> anyhow::Result<()> {
    if args.rss_probe {
        return run_rss_probe_child_mode(args).await;
    }

    let breadths = resolve_breadths(&args)?;
    anyhow::ensure!(
        !breadths.is_empty(),
        "--breadths must name at least one breadth"
    );

    let exe = std::env::current_exe()
        .map_err(|e| anyhow::anyhow!("failed to resolve the current executable path (needed to spawn RSS-probe children): {e}"))?;

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

    let schema_ctx = RenderCtx {
        db: args.database.clone(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };

    // Frozen once, for the whole run — see `now_unix_ns`'s doc comment.
    let ref_ns = now_unix_ns();

    let mut breadth_reports = Vec::with_capacity(breadths.len());
    let mut cpu_metric_source = "OSCPUVirtualTimeMicroseconds";
    // Cross-breadth correctness-gate state (code review round-2 [medium]):
    // `None` until the first breadth's gate runs, then held for the rest of
    // this invocation so every later breadth's full result envelope is
    // asserted byte-identical to the first — never reset per breadth.
    let mut reference_envelope: Option<paths::ResultEnvelope> = None;

    for &breadth in &breadths {
        eprintln!("=== logs-hydration: breadth={breadth} ===");
        admin
            .execute(
                &format!("DROP DATABASE IF EXISTS {}", args.database),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await?;
        run_init(&admin, &schema_ctx).await?;

        let mut data_cfg = admin_cfg.clone();
        data_cfg.database = args.database.clone();
        let client = ChClient::new(data_cfg).await?;

        let spec = BroadDatasetSpec {
            seed: args.seed,
            breadth,
            ref_ns,
            dist: false,
        };
        eprintln!("--- generating breadth-{breadth} corpus ---");
        let summary = dataset::load_broad_tier(&client, &spec).await?;
        eprintln!(
            "corpus: {} result streams + {} filler streams, loaded in {} ms",
            summary.result_streams, summary.filler_streams, summary.load_elapsed_ms
        );

        eprintln!(
            "--- running the three hydration paths (correctness gate + 6-round rotation + RSS probes) ---"
        );
        let tables = paths::Tables::new();
        let breadth_cfg = paths::BreadthConfig {
            client: &client,
            tables: &tables,
            exe: &exe,
            http_url: &args.http_url,
            database: &args.database,
            user: &args.user,
            password: &args.password,
        };
        let (resolved_fps, path_evidence, source) =
            paths::run_breadth(&breadth_cfg, &summary, &mut reference_envelope).await?;
        cpu_metric_source = source;

        for e in &path_evidence {
            eprintln!(
                "{:>10} breadth={:<7} client_wall_ms(median)={:.2} hydration_read_bytes(median)={} \
                 rss_delta_kib(median)={:.1}",
                e.path,
                e.breadth,
                e.client_wall_ms.median,
                e.hydration_read_bytes_median,
                e.client_rss_delta_kib.median,
            );
        }

        breadth_reports.push(report::BreadthReport {
            breadth,
            service: summary.service.clone(),
            resolved_fps,
            paths: path_evidence,
        });
    }

    let verdict = evaluate_verdict(&breadth_reports);
    if let Some(v) = &verdict {
        eprintln!(
            "=== verdict: {:?} (identity_ok={} rep_stability_ok={} storage_equality_ok={} \
             recommended_b_variant={:?}) ===",
            v.verdict,
            v.validity_gates.identity_ok,
            v.validity_gates.rep_stability_ok,
            v.validity_gates.storage_equality_ok,
            v.recommended_b_variant
        );
    } else {
        eprintln!(
            "=== no verdict: this sweep does not reach breadth {} (report::HIGH_BREADTH) ===",
            report::HIGH_BREADTH
        );
    }

    let rpt = LogsHydrationReport {
        profile: args.profile,
        seed: args.seed,
        breadths,
        cpu_metric_source: cpu_metric_source.to_string(),
        breadth_reports,
        verdict,
        closeout: CloseoutRef::default(),
    };

    if let Some(out) = &args.out {
        std::fs::write(out, serde_json::to_string_pretty(&rpt)?)?;
        eprintln!("wrote {out}");
    }
    if let Some(report_out) = &args.report_out {
        std::fs::write(report_out, report::render_markdown(&rpt))?;
        eprintln!("wrote {report_out}");
    }

    Ok(())
}

/// The `--rss-probe` child mode: connects to the already-loaded database
/// and delegates to [`rss_probe::run_rss_probe_child`], then exits (no report
/// generation, no corpus load — the parent owns both).
async fn run_rss_probe_child_mode(args: BenchArgs) -> anyhow::Result<()> {
    let variant_str = args
        .rss_variant
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--rss-probe requires --rss-variant"))?;
    let variant = Variant::parse(variant_str)?;
    let breadth = args
        .rss_breadth
        .ok_or_else(|| anyhow::anyhow!("--rss-probe requires --rss-breadth"))?;

    let (server, http_port) = parse_http_url(&args.http_url)?;
    let cfg = ChConnConfig {
        server,
        http_port,
        database: args.database.clone(),
        user: args.user.clone(),
        password: args.password.clone(),
        proto: ChProto::Http,
        pool_size: 1,
        query_timeout: Duration::from_secs(600),
        ..ChConnConfig::default()
    };
    let client = ChClient::new(cfg).await?;
    let tables = paths::Tables::new();
    rss_probe::run_rss_probe_child(&client, &tables, &args.database, variant, breadth).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_with(profile: Profile, breadths: Option<&str>) -> BenchArgs {
        BenchArgs {
            scenario: "logs-hydration".to_string(),
            profile,
            seed: 1,
            services: 1,
            streams: 1,
            lines_per_sec: 1,
            duration_secs: 1,
            http_url: "http://127.0.0.1:8123".to_string(),
            database: "pulsus_bench".to_string(),
            user: "default".to_string(),
            password: String::new(),
            dist: false,
            cluster: "c".to_string(),
            reps: 1,
            out: None,
            report_out: None,
            metric_cardinalities: None,
            activity_buckets: "1h,1d".to_string(),
            corpus_window_hours: 24,
            cache_max_series: 10_000_000,
            matcher_reps: 10,
            breadths: breadths.map(str::to_string),
            rss_probe: false,
            rss_variant: None,
            rss_breadth: None,
        }
    }

    #[test]
    fn resolve_breadths_ci_profile_ignores_no_override_and_returns_the_fixed_set() {
        let args = args_with(Profile::Ci, None);
        assert_eq!(resolve_breadths(&args).unwrap(), CI_BREADTHS.to_vec());
    }

    #[test]
    fn resolve_breadths_ci_profile_hard_errors_on_an_explicit_override() {
        let args = args_with(Profile::Ci, Some("1,2,3"));
        let err = resolve_breadths(&args).unwrap_err();
        assert!(err.to_string().contains("--profile full"));
    }

    #[test]
    fn resolve_breadths_full_profile_defaults_to_the_verdict_evaluation_set() {
        let args = args_with(Profile::Full, None);
        assert_eq!(resolve_breadths(&args).unwrap(), FULL_BREADTHS.to_vec());
    }

    #[test]
    fn resolve_breadths_full_profile_accepts_an_explicit_override() {
        let args = args_with(Profile::Full, Some("7,8,9"));
        assert_eq!(resolve_breadths(&args).unwrap(), vec![7, 8, 9]);
    }

    #[test]
    fn parse_u32_csv_rejects_a_non_integer_token() {
        assert!(parse_u32_csv("10,x,30").is_err());
    }
}
