//! `cargo xtask bench metrics-labels` — the M2 label-resolution benchmark
//! (issue #34). Closes the M2 evidence half of docs/schemas.md §2.1's
//! strategy-ladder decision gate: benchmarks the three ladder paths (cache
//! matcher, SQL fallback, prototype `metric_series_idx`) on a deterministic
//! `metric_series` corpus, across every `--metric-cardinalities` tier and
//! every `--activity-buckets` size, plus the day-bucket over-inclusion
//! ratio and the refresh-sweep cost (full + a bench-local
//! incremental-refresh prototype).
//!
//! **Two profiles, hard-bounded (architect plan amendment #2 — CI cannot
//! silently run the 5M-series shape):**
//! - `--profile ci` — [`CI_CARDINALITIES`] (`[1_000, 10_000, 50_000]`),
//!   always; an explicit `--metric-cardinalities` under `ci` is a hard
//!   error.
//! - `--profile full` — the explicit `--metric-cardinalities` override if
//!   given, else [`FULL_CARDINALITIES`] (`[10_000, 500_000, 5_000_000]`,
//!   the docs/schemas.md §9 design-target scale corpus) — manual-only,
//!   hours.
//!
//! **`metric_series_idx` is a bench-local prototype, never wired into
//! `pulsus-schema`'s migration catalog or `run_init`** (architect plan,
//! "Out of scope" / edge case 4) — the M3 milestone decides whether it
//! ships.
//!
//! Module layout: [`corpus`] (deterministic `metric_series` generator),
//! [`idx`] (the bench-local `metric_series_idx` prototype DDL + ARRAY JOIN
//! population), [`paths`] (the three path runners + the cross-path
//! correctness gate), [`report`] (JSON/markdown evidence rendering).

pub mod corpus;
pub mod idx;
pub mod paths;
pub mod report;

use std::collections::BTreeMap;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_schema::{RenderCtx, run_init};

use super::{BenchArgs, Profile, parse_http_url};

/// `--profile ci`'s fixed, hard-coded cardinality set — small enough for a
/// per-PR `schema-it` smoke step (architect plan amendment #2).
pub const CI_CARDINALITIES: [u64; 3] = [1_000, 10_000, 50_000];
/// `--profile full`'s default cardinality set when `--metric-cardinalities`
/// is not given — the docs/schemas.md §9 design-target scale corpus shape
/// (10k / 500k / 5M series per metric).
pub const FULL_CARDINALITIES: [u64; 3] = [10_000, 500_000, 5_000_000];

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;

fn resolve_cardinalities(args: &BenchArgs) -> anyhow::Result<Vec<u64>> {
    match args.profile {
        Profile::Ci => {
            anyhow::ensure!(
                args.metric_cardinalities.is_none(),
                "--metric-cardinalities requires --profile full; --profile ci always uses the \
                 fixed small set {CI_CARDINALITIES:?} (never the design-target 5M-series shape \
                 in CI)"
            );
            Ok(CI_CARDINALITIES.to_vec())
        }
        Profile::Full => match &args.metric_cardinalities {
            Some(s) => parse_u64_csv(s, "--metric-cardinalities"),
            None => Ok(FULL_CARDINALITIES.to_vec()),
        },
    }
}

fn parse_u64_csv(s: &str, flag: &str) -> anyhow::Result<Vec<u64>> {
    s.split(',')
        .map(|tok| {
            tok.trim()
                .parse::<u64>()
                .map_err(|e| anyhow::anyhow!("{flag}: invalid integer {tok:?}: {e}"))
        })
        .collect()
}

/// Parses `--activity-buckets` (e.g. `"1h,1d"`) into millisecond bucket
/// sizes — only the two `1h`/`1d` tokens docs/schemas.md §2.1 discusses are
/// accepted.
fn parse_bucket_tokens(s: &str) -> anyhow::Result<Vec<i64>> {
    s.split(',')
        .map(|tok| match tok.trim() {
            "1h" => Ok(HOUR_MS),
            "1d" => Ok(DAY_MS),
            other => anyhow::bail!(
                "--activity-buckets: unknown token {other:?} (expected \"1h\" or \"1d\")"
            ),
        })
        .collect()
}

/// Wall-clock now, milliseconds since the Unix epoch — **one of exactly
/// two** wall-clock reads anywhere in this scenario (issue #34 CODE review
/// [high] finding: "frozen reference instant"; round-3 [low] finding #3
/// swept for and confirmed no others; the other read is
/// `paths::run_all`'s guard-band drift check, `paths.rs`'s own copy of
/// `now_unix_ms` — see that function's doc comment). Every bucket-size
/// pass calls *this* one exactly once, capturing `ref_ms` before the
/// corpus is generated; `corpus::load`, `paths::run_all`'s SQL/idx bounds,
/// and the over-inclusion probe all consume that one frozen value via
/// `MetricsCorpusSummary::end_ms`/`MetricsCorpusSpec::ref_ms`, never
/// re-deriving "now" independently. See `corpus.rs`'s module doc comment
/// ("Frozen reference instant") for why this alone is not sufficient
/// on its own — paired with the corpus's guard-band bucket assignment.
fn now_unix_ms() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

pub async fn run(args: BenchArgs) -> anyhow::Result<()> {
    let cardinalities = resolve_cardinalities(&args)?;
    let bucket_list = parse_bucket_tokens(&args.activity_buckets)?;
    anyhow::ensure!(
        !bucket_list.is_empty(),
        "--activity-buckets must name at least one bucket"
    );

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

    let cluster = args.dist.then(|| args.cluster.clone());
    let schema_ctx = RenderCtx {
        db: args.database.clone(),
        cluster,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };

    let window_ms = (args.corpus_window_hours * 3_600_000) as i64;

    let mut bucket_reports = Vec::with_capacity(bucket_list.len());
    // bucket_ms -> [(cardinality, OverInclusionProbe)] for the over-inclusion
    // probe, populated once per bucket size below and combined into
    // report::OverInclusion rows after the whole loop completes (the
    // over-inclusion claim compares the *same* selector/cardinality across
    // two independently generated corpora — one per bucket size — not
    // across two live tables at once, since each bucket size's pass gets
    // its own freshly re-initialized database).
    let mut probe_by_bucket: BTreeMap<i64, Vec<(u64, paths::OverInclusionProbe)>> = BTreeMap::new();

    // `ON CLUSTER` under `--dist` (issue #34 CODE review round-2 [valid]
    // finding #4): a bare `DROP DATABASE` only drops the database on the
    // node this benchmark connects to — on a cluster the other shards keep
    // their stale tables while `run_init` (below) recreates everything
    // `ON CLUSTER`, so a re-run under `--dist` would silently mix a fresh
    // schema with leftover data on every shard but the initiator's.
    let drop_on_cluster = if args.dist {
        format!(" ON CLUSTER {}", args.cluster)
    } else {
        String::new()
    };

    for &bucket_ms in &bucket_list {
        eprintln!(
            "=== metrics-labels: bucket_ms={bucket_ms} (cardinalities={cardinalities:?}) ==="
        );
        admin
            .execute(
                &format!("DROP DATABASE IF EXISTS {}{drop_on_cluster}", args.database),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await?;
        run_init(&admin, &schema_ctx).await?;

        let mut data_cfg = admin_cfg.clone();
        data_cfg.database = args.database.clone();
        let client = ChClient::new(data_cfg).await?;

        // Frozen once, here — the single source for this pass's corpus
        // timestamps, SQL/idx bounds, and the resolve window (see
        // `now_unix_ms`'s and `corpus.rs`'s doc comments).
        let ref_ms = now_unix_ms();
        let spec = corpus::MetricsCorpusSpec {
            profile: args.profile,
            seed: args.seed,
            cardinalities: cardinalities.clone(),
            bucket_ms,
            window_ms,
            ref_ms,
            dist: args.dist,
        };
        eprintln!("--- generating metric_series corpus ---");
        let summary = corpus::load(&client, &spec).await?;
        eprintln!(
            "corpus: {} rows across {} tiers, loaded in {} ms",
            summary.total_series_rows,
            summary.tiers.len(),
            summary.load_elapsed_ms
        );

        eprintln!("--- building metric_series_idx prototype ---");
        let idx_summary = idx::build(&client, &args.database, args.dist, &args.cluster).await?;
        eprintln!(
            "idx: {} rows, {} bytes on disk, built in {} ms",
            idx_summary.rows, idx_summary.bytes_on_disk, idx_summary.build_ms
        );

        eprintln!("--- running the three-path label resolution ---");
        let paths_cfg = paths::PathsConfig {
            client: &client,
            db: &args.database,
            cluster: &args.cluster,
            dist: args.dist,
            reps: args.reps,
            matcher_reps: args.matcher_reps,
            cache_max_series: args.cache_max_series,
        };
        // A dedicated connection for the LabelCache this pass drives
        // (`ChClient` is not `Clone`; `LabelCache::new` takes ownership —
        // see `paths::run_all`'s doc comment).
        let mut cache_data_cfg = admin_cfg.clone();
        cache_data_cfg.database = args.database.clone();
        let cache_client = ChClient::new(cache_data_cfg).await?;
        let (path_evidence, refresh_evidence) =
            paths::run_all(&paths_cfg, cache_client, &summary).await?;
        for e in &path_evidence {
            eprintln!(
                "{:>13} {:>16} card={:<8} {:<14}: matched={} wall p50={:.3}ms read_rows={} \
                 selected_marks={}/{}",
                e.path,
                e.metric_name,
                e.cardinality,
                e.selector,
                e.matched_series,
                e.wall_ms_p50,
                e.read_rows,
                e.selected_marks,
                e.total_marks,
            );
        }

        eprintln!("--- probing day-bucket over-inclusion ---");
        let mut probe_rows = Vec::with_capacity(summary.tiers.len());
        for tier in &summary.tiers {
            let probe =
                paths::over_inclusion_probe(&paths_cfg, tier, bucket_ms, summary.end_ms).await?;
            eprintln!(
                "  {:>16} card={:<8} read_rows={} matched_candidates={}",
                tier.metric_name, tier.cardinality, probe.read_rows, probe.matched_candidates
            );
            probe_rows.push((tier.cardinality, probe));
        }
        probe_by_bucket.insert(bucket_ms, probe_rows);

        bucket_reports.push(report::BucketReport {
            bucket_ms,
            corpus: summary,
            idx: idx_summary,
            paths: path_evidence,
            refresh: refresh_evidence,
        });
    }

    let over_inclusion = build_over_inclusion(&probe_by_bucket);

    let rpt = report::MetricsLabelsReport {
        profile: args.profile,
        seed: args.seed,
        dist: args.dist,
        cluster: args.dist.then_some(args.cluster.clone()),
        cardinalities,
        buckets: bucket_reports,
        over_inclusion,
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

/// A ratio, `0.0` if the denominator is `0` (degenerate, should not occur
/// on a non-empty corpus — kept as a safe fallback rather than a panic).
fn safe_ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// Pairs the `1h`- and `1d`-bucket probe rows by cardinality into
/// [`report::OverInclusion`] rows — empty (no rows) unless both `1h` and
/// `1d` were actually benchmarked in this run (`--activity-buckets`
/// defaults to both). Two ratios (issue #34 CODE review round-2 [valid]
/// finding #5): `read_rows` (structural, PK-explained, ≈1.0 — see
/// `paths::OverInclusionProbe`'s doc comment) and `matched_candidates` (the
/// §2.1-faithful *semantic* over-inclusion — the ratio that is actually
/// expected to differ meaningfully from 1.0).
fn build_over_inclusion(
    probe_by_bucket: &BTreeMap<i64, Vec<(u64, paths::OverInclusionProbe)>>,
) -> Vec<report::OverInclusion> {
    let (Some(hour_rows), Some(day_rows)) =
        (probe_by_bucket.get(&HOUR_MS), probe_by_bucket.get(&DAY_MS))
    else {
        return Vec::new();
    };
    hour_rows
        .iter()
        .zip(day_rows.iter())
        .filter(|((card_h, _), (card_d, _))| card_h == card_d)
        .map(
            |((cardinality, probe_1h), (_, probe_1d))| report::OverInclusion {
                cardinality: *cardinality,
                read_rows_1h: probe_1h.read_rows,
                read_rows_1d: probe_1d.read_rows,
                ratio: safe_ratio(probe_1d.read_rows, probe_1h.read_rows),
                matched_candidates_1h: probe_1h.matched_candidates,
                matched_candidates_1d: probe_1d.matched_candidates,
                candidate_ratio: safe_ratio(
                    probe_1d.matched_candidates,
                    probe_1h.matched_candidates,
                ),
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bucket_tokens_accepts_the_default_pair() {
        assert_eq!(parse_bucket_tokens("1h,1d").unwrap(), vec![HOUR_MS, DAY_MS]);
    }

    #[test]
    fn parse_bucket_tokens_rejects_an_unknown_token() {
        assert!(parse_bucket_tokens("1h,5m").is_err());
    }

    #[test]
    fn parse_u64_csv_parses_a_comma_list() {
        assert_eq!(parse_u64_csv("10,20,30", "--x").unwrap(), vec![10, 20, 30]);
    }

    #[test]
    fn parse_u64_csv_rejects_a_non_integer_token() {
        assert!(parse_u64_csv("10,x,30", "--x").is_err());
    }

    fn args_with(profile: Profile, metric_cardinalities: Option<&str>) -> BenchArgs {
        BenchArgs {
            scenario: "metrics-labels".to_string(),
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
            metric_cardinalities: metric_cardinalities.map(str::to_string),
            activity_buckets: "1h,1d".to_string(),
            corpus_window_hours: 24,
            cache_max_series: 10_000_000,
            matcher_reps: 10,
            breadths: None,
            rss_probe: false,
            rss_variant: None,
            rss_breadth: None,
        }
    }

    #[test]
    fn resolve_cardinalities_ci_profile_ignores_no_override_and_returns_the_fixed_set() {
        let args = args_with(Profile::Ci, None);
        assert_eq!(
            resolve_cardinalities(&args).unwrap(),
            CI_CARDINALITIES.to_vec()
        );
    }

    #[test]
    fn resolve_cardinalities_ci_profile_hard_errors_on_an_explicit_override() {
        let args = args_with(Profile::Ci, Some("1,2,3"));
        let err = resolve_cardinalities(&args).unwrap_err();
        assert!(err.to_string().contains("--profile full"));
    }

    #[test]
    fn resolve_cardinalities_full_profile_defaults_to_the_design_target_set() {
        let args = args_with(Profile::Full, None);
        assert_eq!(
            resolve_cardinalities(&args).unwrap(),
            FULL_CARDINALITIES.to_vec()
        );
    }

    #[test]
    fn resolve_cardinalities_full_profile_accepts_an_explicit_override() {
        let args = args_with(Profile::Full, Some("7,8,9"));
        assert_eq!(resolve_cardinalities(&args).unwrap(), vec![7, 8, 9]);
    }

    fn probe(read_rows: u64, matched_candidates: u64) -> paths::OverInclusionProbe {
        paths::OverInclusionProbe {
            read_rows,
            matched_candidates,
        }
    }

    #[test]
    fn build_over_inclusion_pairs_matching_cardinalities_by_bucket() {
        let mut probes = BTreeMap::new();
        probes.insert(
            HOUR_MS,
            vec![(1_000u64, probe(10, 5)), (10_000, probe(100, 50))],
        );
        probes.insert(
            DAY_MS,
            vec![(1_000u64, probe(40, 100)), (10_000, probe(400, 1000))],
        );
        let rows = build_over_inclusion(&probes);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].cardinality, 1_000);
        assert_eq!(rows[0].ratio, 4.0);
        assert_eq!(rows[0].candidate_ratio, 20.0);
        assert_eq!(rows[1].ratio, 4.0);
        assert_eq!(rows[1].candidate_ratio, 20.0);
    }

    #[test]
    fn build_over_inclusion_is_empty_when_only_one_bucket_size_ran() {
        let mut probes = BTreeMap::new();
        probes.insert(HOUR_MS, vec![(1_000u64, probe(10, 5))]);
        assert!(build_over_inclusion(&probes).is_empty());
    }
}
