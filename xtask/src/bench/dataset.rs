//! Deterministic logs corpus generator for `cargo xtask bench logs-read`
//! (issue #16). Two profiles: `Profile::Ci` (minutes-scale, wired into the
//! `schema-it` CI job) and `Profile::Full` (the parameterized 1 TB/7d/
//! 50-service/5k-stream reference corpus, manual-only — see
//! docs/benchmarks/m1-logs-read-path.md's reproduction section; `--profile
//! full` only changes the eprintln banner here, the actual scale comes
//! from `--services`/`--streams`/`--lines-per-sec`/`--duration-secs`).
//!
//! **PRNG:** hand-rolled splitmix64/xorshift64* (task-manager resolution
//! #3 on issue #16) — never `rand`, so a committed CI baseline stays
//! byte-reproducible across `rand` major-version bumps (the pre-existing
//! `ch_bench/rows.rs` generator predates this convention and is left as
//! documented prior art, not migrated — issue #16 architect plan).
//!
//! **Fingerprints/labels** use `pulsus-model`'s frozen canonicalization/
//! fingerprint primitives (`LabelSet::from_normalized`,
//! `stream_fingerprint`) — the same "product fingerprinting" the CI-scale
//! and Tier-2 corpora share. **DDL** is the product's own
//! (`pulsus_schema::run_init`, run by the caller before [`load`]). **Bulk
//! load** is direct RowBinary insert (`ChClient::insert_block`); the
//! fidelity of this shortcut relative to the real OTLP ingest path is
//! licensed by `crates/pulsus-write/tests/ingest_fidelity.rs`, not
//! re-proven here (architect plan amendment, [medium] finding).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, QuerySettings, Row};
use pulsus_model::{Date, LabelSet, stream_fingerprint};

use super::Profile;

/// A single-token (no separators — ClickHouse's `tokenbf_v1` bloom index
/// rejects tokens containing whitespace/separators) needle injected into a
/// controlled fraction of bodies so the body-search shape's selectivity is
/// a known constant, not incidental to random content.
pub const NEEDLE: &str = "xtaskneedle7c91a";
/// One body in this many carries [`NEEDLE`].
pub const NEEDLE_RATE: u64 = 500;

#[derive(Debug, Clone, Copy)]
pub struct DatasetSpec {
    pub profile: Profile,
    pub seed: u64,
    pub services: u32,
    pub streams: u32,
    pub lines_per_sec: u64,
    pub duration_secs: u64,
    /// Load through the `_dist` Distributed wrappers instead of the bare
    /// local tables — required for `--dist` mode: a bare-table insert
    /// always lands on whichever single node `client` is connected to
    /// (`ReplicatedMergeTree` replicates within a shard, it does not
    /// re-shard), which would silently defeat the fan-out benchmark by
    /// putting the entire corpus on one shard. `docs/schemas.md §7`'s
    /// documented pattern (also `crates/pulsus-schema/tests/
    /// live_cluster.rs`): insert into `<table>_dist` and let the
    /// Distributed engine's `fingerprint` sharding key place each row.
    pub dist: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DatasetSummary {
    pub total_rows: u64,
    pub services: u32,
    pub streams: u32,
    pub needle_rows: u64,
    pub start_ns: i64,
    pub end_ns: i64,
    pub load_elapsed_ms: u64,
    /// The canonical stream (index 0) every canonical query shape targets.
    pub canonical_service: String,
    pub canonical_env: String,
    pub canonical_fingerprint: u64,
}

/// A cheap, deterministic 64-bit mix (splitmix64 — matches
/// `xtask/src/ch_bench/rows.rs`'s existing constant/shape, kept
/// independent rather than shared: that generator uses `rand::StdRng`
/// downstream and is left as-is per the issue #16 task-manager
/// resolution, "migrate only if the new generator naturally subsumes it").
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A tiny xorshift64* stream seeded from [`splitmix64`] — used where a
/// sequence (rather than one-shot mixes indexed by row position) is more
/// natural at the call site. Same determinism/no-`rand` rationale.
struct Xorshift64(u64);

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        Xorshift64(splitmix64(seed).max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

const ENVS: &[&str] = &["prod", "staging", "dev"];
const REGIONS: &[&str] = &["us-east-1", "us-west-2", "eu-west-1"];

/// [`build_streams`]'s exact, fixed per-stream label count
/// (`service.name`/`env`/`region`/`stream_ordinal` — four distinct keys,
/// no collisions since `stream_ordinal` is unique per row) — the expected
/// `log_streams_idx` row count per stream, once its `(key, val)` rows have
/// fully settled. Used by [`load`]'s `--dist` settle-poll, not by
/// `build_streams` itself (kept as a named constant so the two stay in
/// sync if the label set ever changes).
const LABELS_PER_STREAM: u64 = 4;

struct Stream {
    service: String,
    env: String,
    fingerprint: u64,
    labels_json: String,
}

fn build_streams(spec: &DatasetSpec) -> Vec<Stream> {
    let services = spec.services.max(1);
    (0..spec.streams)
        .map(|i| {
            let service = format!("svc-{:03}", i % services);
            let env = ENVS[(i as usize) % ENVS.len()].to_string();
            let region = REGIONS[(i as usize / ENVS.len()) % REGIONS.len()];
            let (labels, _collisions) = LabelSet::from_normalized([
                ("service.name".to_string(), service.clone()),
                ("env".to_string(), env.clone()),
                ("region".to_string(), region.to_string()),
                ("stream_ordinal".to_string(), i.to_string()),
            ]);
            let fingerprint = stream_fingerprint(&labels);
            Stream {
                service,
                env,
                fingerprint,
                labels_json: labels.to_canonical_json(),
            }
        })
        .collect()
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedStreamRow {
    month: u16,
    fingerprint: u64,
    service: String,
    labels: String,
    updated_ns: i64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSampleRow {
    service: String,
    fingerprint: u64,
    timestamp_ns: i64,
    severity: i8,
    body: String,
}

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

/// Rows per bulk `INSERT` block — large enough to amortize per-request
/// overhead, small enough to keep any one `insert_block` call's memory
/// bounded regardless of total corpus size.
const INSERT_BATCH_ROWS: usize = 100_000;

fn gen_body(i: u64, service: &str, carries_needle: bool) -> String {
    let padding = "x".repeat(96);
    if carries_needle {
        format!("service={service} idx={i} {NEEDLE} request failed with timeout {padding}")
    } else {
        format!("service={service} idx={i} request completed status=200 {padding}")
    }
}

/// Polls `count_sql` (must return one row, column `n`) until it reaches
/// `expected`, or a bounded deadline elapses — the `_dist`
/// eventual-consistency guard docs/architecture.md §9 requires
/// ("poll-until-visible, no fixed sleeps"): a `_dist` insert dispatches
/// each row to its sharding-key destination in the background, so the
/// corpus is not necessarily fully visible the instant an `insert_block`
/// call returns, and the query set that follows must observe the whole
/// corpus, not a partial one. `label` is only used in progress/error
/// messages.
async fn poll_count_until_visible(
    client: &ChClient,
    count_sql: &str,
    expected: u64,
    label: &str,
) -> anyhow::Result<()> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct CountRow {
        n: u64,
    }
    for attempt in 0..120 {
        let mut stream = client
            .query_stream::<CountRow>(count_sql, &QuerySettings::new())
            .await?;
        let n = match stream.next().await {
            Some(row) => row?.n,
            None => 0,
        };
        if n >= expected {
            return Ok(());
        }
        if attempt % 10 == 0 {
            eprintln!("waiting for {label} to settle: {n}/{expected} visible");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!(
        "{label} did not reach {expected} visible within the poll deadline (60s) — the \
         Distributed corpus failed to settle"
    )
}

/// Loads `spec`'s corpus into the database `client` is bound to (already
/// schema-initialized by the caller via `pulsus_schema::run_init`): one
/// `log_streams` row per stream, then `duration_secs * lines_per_sec`
/// `log_samples` rows round-robin across streams in
/// [`INSERT_BATCH_ROWS`]-row batches, timestamps spread evenly across
/// `[now - duration_secs, now]`. Anchored at wall-clock `now`, never a
/// fixed historical constant — the same TTL-eligibility hazard
/// `crates/pulsus-read/tests/explain_indexes.rs::now_ns` documents
/// (`log_samples`'s `ttl_only_drop_parts = 1` retention would make an
/// already-expired fixed-date fixture flaky).
pub async fn load(client: &ChClient, spec: &DatasetSpec) -> anyhow::Result<DatasetSummary> {
    if spec.profile == Profile::Full {
        eprintln!(
            "profile=full: this is the parameterized Tier-2 reference corpus shape \
             (docs/benchmarks/m1-logs-read-path.md's manual procedure, tracked by #25) — \
             scale comes entirely from --services/--streams/--lines-per-sec/--duration-secs; \
             running it at 1 TB/7d/50-service/5k-stream scale takes hours, not minutes, and \
             is not something this invocation limits or validates on your behalf."
        );
    }

    let streams_table = if spec.dist {
        "log_streams_dist"
    } else {
        "log_streams"
    };
    let streams_idx_table = if spec.dist {
        "log_streams_idx_dist"
    } else {
        "log_streams_idx"
    };
    let samples_table = if spec.dist {
        "log_samples_dist"
    } else {
        "log_samples"
    };

    let start_instant = Instant::now();
    let streams = build_streams(spec);
    anyhow::ensure!(!streams.is_empty(), "--streams must be >= 1");

    let end_ns = now_ns();
    let start_ns = end_ns - (spec.duration_secs as i64) * 1_000_000_000;
    let month = Date::start_of_month_utc(end_ns).days_since_epoch();

    let stream_rows: Vec<SeedStreamRow> = streams
        .iter()
        .map(|s| SeedStreamRow {
            month,
            fingerprint: s.fingerprint,
            service: s.service.clone(),
            labels: s.labels_json.clone(),
            updated_ns: end_ns,
        })
        .collect();
    for chunk in stream_rows.chunks(INSERT_BATCH_ROWS) {
        client.insert_block(streams_table, chunk).await?;
    }

    if spec.dist {
        // Both the base `log_streams_dist` row AND the MV-derived
        // `log_streams_idx_dist` rows (`log_streams`'s materialized view,
        // fired once a row lands on its destination shard) must settle
        // cross-shard before the query set's stage-1 resolution (which
        // reads `log_streams_idx_dist`) runs — a `_dist` insert is
        // eventually consistent (docs/architecture.md §9), and this was
        // observed live (issue #16 CODE review round 2 verification run):
        // `log_streams_dist` read only 127/500 rows on the very first
        // poll immediately after the insert loop returned. The idx check
        // is a **total row count**, not `uniqExact(fingerprint)` — a
        // stream's `(key, val)` rows can straggle in independently (e.g.
        // its `env` row visible, its `service_name` row not yet), and
        // "at least one row per fingerprint" would pass before every row
        // every fingerprint needs has actually landed. [`LABELS_PER_STREAM`]
        // is [`build_streams`]'s own exact, fixed per-stream label count.
        poll_count_until_visible(
            client,
            &format!("SELECT count() AS n FROM {streams_table}"),
            streams.len() as u64,
            streams_table,
        )
        .await?;
        poll_count_until_visible(
            client,
            &format!("SELECT count() AS n FROM {streams_idx_table}"),
            streams.len() as u64 * LABELS_PER_STREAM,
            streams_idx_table,
        )
        .await?;
    }

    let total_rows = spec.duration_secs * spec.lines_per_sec;
    let duration_ns = (spec.duration_secs as f64) * 1_000_000_000.0;
    let mut rng = Xorshift64::new(spec.seed);
    let mut needle_rows = 0u64;
    let mut batch: Vec<SeedSampleRow> = Vec::with_capacity(INSERT_BATCH_ROWS);

    for i in 0..total_rows {
        let stream = &streams[(i % streams.len() as u64) as usize];
        let frac = if total_rows > 1 {
            i as f64 / (total_rows - 1) as f64
        } else {
            0.0
        };
        let jitter_ns = (splitmix64(spec.seed ^ i) % 1_000_000) as i64;
        let timestamp_ns = start_ns + (frac * duration_ns) as i64 + jitter_ns;
        let carries_needle = rng.next_u64().is_multiple_of(NEEDLE_RATE);
        if carries_needle {
            needle_rows += 1;
        }
        batch.push(SeedSampleRow {
            service: stream.service.clone(),
            fingerprint: stream.fingerprint,
            timestamp_ns,
            severity: ((i % 24) + 1) as i8,
            body: gen_body(i, &stream.service, carries_needle),
        });
        if batch.len() == INSERT_BATCH_ROWS {
            client.insert_block(samples_table, &batch).await?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        client.insert_block(samples_table, &batch).await?;
    }

    if spec.dist {
        poll_count_until_visible(
            client,
            &format!("SELECT count() AS n FROM {samples_table}"),
            total_rows,
            samples_table,
        )
        .await?;
    }

    let canonical = &streams[0];
    Ok(DatasetSummary {
        total_rows,
        services: spec.services,
        streams: spec.streams,
        needle_rows,
        start_ns,
        end_ns,
        load_elapsed_ms: start_instant.elapsed().as_millis() as u64,
        canonical_service: canonical.service.clone(),
        canonical_env: canonical.env.clone(),
        canonical_fingerprint: canonical.fingerprint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix64_is_deterministic() {
        assert_eq!(splitmix64(42), splitmix64(42));
    }

    #[test]
    fn splitmix64_differs_across_inputs() {
        assert_ne!(splitmix64(1), splitmix64(2));
    }

    #[test]
    fn xorshift64_sequence_is_deterministic_given_the_same_seed() {
        let mut a = Xorshift64::new(7);
        let mut b = Xorshift64::new(7);
        for _ in 0..10 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn build_streams_assigns_services_round_robin() {
        let spec = DatasetSpec {
            profile: Profile::Ci,
            seed: 1,
            services: 2,
            streams: 4,
            lines_per_sec: 1,
            duration_secs: 1,
            dist: false,
        };
        let streams = build_streams(&spec);
        assert_eq!(streams[0].service, "svc-000");
        assert_eq!(streams[1].service, "svc-001");
        assert_eq!(streams[2].service, "svc-000");
        assert_eq!(streams[3].service, "svc-001");
    }

    #[test]
    fn build_streams_fingerprints_are_distinct_per_stream() {
        let spec = DatasetSpec {
            profile: Profile::Ci,
            seed: 1,
            services: 5,
            streams: 20,
            lines_per_sec: 1,
            duration_secs: 1,
            dist: false,
        };
        let streams = build_streams(&spec);
        let mut fps: Vec<u64> = streams.iter().map(|s| s.fingerprint).collect();
        fps.sort_unstable();
        fps.dedup();
        assert_eq!(
            fps.len(),
            20,
            "every stream must have a distinct fingerprint"
        );
    }

    #[test]
    fn gen_body_carries_the_needle_only_when_asked() {
        assert!(gen_body(1, "svc", true).contains(NEEDLE));
        assert!(!gen_body(1, "svc", false).contains(NEEDLE));
    }
}
