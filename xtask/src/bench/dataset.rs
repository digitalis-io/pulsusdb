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
    // `end_ns` is the current wall clock, always inside the ClickHouse `Date`
    // range — representability is an invariant of the seed timestamp, not
    // untrusted input.
    let month = Date::start_of_month_utc(end_ns)
        .expect("current wall-clock month is representable as a ClickHouse Date")
        .days_since_epoch();

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

// --- `logs-hydration` (issue #35) broad-selector corpus ---
//
// A **separate, additive** corpus generator — not a `DatasetSpec`/
// `DatasetSummary` widening (deviation from the architect plan's v1/v2
// `broad_tiers` field sketch, recorded in the issue #35 implementation
// notes): each breadth gets its own freshly-dropped-and-reinitialized
// database (mirroring `metrics_labels::run`'s per-`bucket_ms` reset), so
// [`HYDRATION_SERVICE`] never needs a breadth suffix and the R6 "identical
// fingerprints across all breadths" property (v4 architect plan) falls out
// structurally: the same `(service, env, region, stream_ordinal)`
// construction for ordinals `0..RESULT_STREAMS` yields byte-identical
// `LabelSet`s, hence byte-identical fingerprints/labels, at every breadth —
// rather than requiring a second selector branch to reunite a
// breadth-varying service name with a breadth-invariant result set. This
// keeps [`DatasetSpec`]/[`DatasetSummary`] (and therefore every committed
// `logs-read-*.json`/`metrics-labels-*.json` byte-shape) completely
// untouched.

/// The fixed, result-bearing stream count every breadth carries — equal to
/// the `logs-hydration` scenario's LIMIT (architect plan R6), so `ORDER BY
/// timestamp_ns DESC LIMIT 100` always returns exactly these streams'
/// single sample each, regardless of breadth.
pub const HYDRATION_RESULT_STREAMS: u32 = 100;
/// The single service every breadth's streams (both result-bearing and
/// filler) share — architect plan edge case 6, "single-service broad shape
/// is the deliberate isolation": `PREWHERE service = 'svc-broad'` is always
/// a singleton, so the entire eager-vs-late delta is the `labels` column.
pub const HYDRATION_SERVICE: &str = "svc-broad";
/// The fixed one-hour corpus window every breadth uses, split evenly
/// between the result/filler timestamp bands (see [`load_broad_tier`]'s
/// doc comment) — exported so the RSS-probe child (which only knows
/// `--rss-breadth`, not the parent's frozen `ref_ns`) can re-derive
/// `start_ns` from a freshly-queried `end_ns` without re-deriving this
/// constant independently.
pub const HYDRATION_WINDOW_NS: i64 = 3_600 * 1_000_000_000;

#[derive(Debug, Clone, Copy)]
pub struct BroadDatasetSpec {
    pub seed: u64,
    /// Total streams the selector resolves (`>= HYDRATION_RESULT_STREAMS`).
    pub breadth: u32,
    /// The one frozen reference instant every breadth pass in this
    /// scenario invocation shares (captured once by `logs_hydration::run`,
    /// never re-read per breadth) — this is what makes the
    /// `HYDRATION_RESULT_STREAMS` result set's samples byte-identical
    /// across breadths (same seed, same construction, same anchor).
    pub ref_ns: i64,
    pub dist: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BroadDatasetSummary {
    pub breadth: u32,
    pub service: String,
    pub result_streams: u32,
    pub filler_streams: u32,
    /// The `HYDRATION_RESULT_STREAMS` fixed fingerprints, in stream-ordinal
    /// order (`0..HYDRATION_RESULT_STREAMS`) — identical across breadths.
    pub result_fingerprints: Vec<u64>,
    pub start_ns: i64,
    pub end_ns: i64,
    /// The result/filler timestamp-band boundary (architect plan R6):
    /// result-bearing samples fall in `(t_split_ns, end_ns]`, filler
    /// samples in `(start_ns, t_split_ns]`.
    pub t_split_ns: i64,
    pub load_elapsed_ms: u64,
}

/// One broad-tier stream's construction — mirrors [`Stream`]/[`build_streams`]
/// (same `LabelSet::from_normalized`/`stream_fingerprint` primitives, same
/// `env`/`region` assignment shape) but scoped to [`HYDRATION_SERVICE`]
/// alone (a single service, not a round-robin over `spec.services`) and
/// keyed by a stream ordinal that is **breadth-independent** for the fixed
/// result-bearing set — see the module-level doc comment above.
fn build_broad_streams(breadth: u32) -> Vec<Stream> {
    (0..breadth)
        .map(|i| {
            let env = ENVS[(i as usize) % ENVS.len()].to_string();
            let region = REGIONS[(i as usize / ENVS.len()) % REGIONS.len()];
            let (labels, _collisions) = LabelSet::from_normalized([
                ("service.name".to_string(), HYDRATION_SERVICE.to_string()),
                ("env".to_string(), env.clone()),
                ("region".to_string(), region.to_string()),
                ("stream_ordinal".to_string(), i.to_string()),
            ]);
            let fingerprint = stream_fingerprint(&labels);
            Stream {
                service: HYDRATION_SERVICE.to_string(),
                env,
                fingerprint,
                labels_json: labels.to_canonical_json(),
            }
        })
        .collect()
}

/// Per-stream, non-overlapping time **slots** for the two R6 timestamp
/// bands (code review finding, issue #35: the previous `frac`-plus-jitter
/// formula's last slot could push `jitter_ns` past the slot's own
/// boundary — the top result stream's timestamp could exceed `end_ns`
/// entirely, and the top filler's could cross `t_split_ns` into the result
/// band, both of which corrupt the fixed 100-stream result set at some
/// breadths but not others). Every stream gets its own disjoint
/// `[slot_start, slot_start + slot_width)` range within its band; the
/// jitter term is reduced modulo the slot's own width before being added,
/// so it can **never** leave that slot — and disjoint slots make every
/// timestamp in the corpus globally unique (F5) without relying on jitter
/// entropy alone. [`Self::result_timestamp_ns`] sizes its slots off
/// [`HYDRATION_RESULT_STREAMS`] (the constant, never a `breadth`-derived
/// count — `load_broad_tier` asserts `breadth >= HYDRATION_RESULT_STREAMS`,
/// so the runtime `result_streams` count always equals it), so the result
/// band's slot layout — hence every result-bearing stream's timestamp — is
/// *structurally* breadth-independent, not just incidentally so; a pure,
/// no-I/O type so this property is unit-testable without a live database.
#[derive(Debug, Clone, Copy)]
struct BroadTimestampBands {
    start_ns: i64,
    t_split_ns: i64,
    end_ns: i64,
    result_slot_width: i64,
    filler_slot_width: i64,
}

impl BroadTimestampBands {
    fn new(start_ns: i64, t_split_ns: i64, end_ns: i64, filler_streams: u32) -> Self {
        let half_ns = end_ns - t_split_ns;
        let result_slot_width = (half_ns / i64::from(HYDRATION_RESULT_STREAMS)).max(1);
        let filler_slot_width = if filler_streams > 0 {
            ((half_ns - 1) / i64::from(filler_streams)).max(1)
        } else {
            1
        };
        BroadTimestampBands {
            start_ns,
            t_split_ns,
            end_ns,
            result_slot_width,
            filler_slot_width,
        }
    }

    /// Result-bearing stream `i`'s (`0..HYDRATION_RESULT_STREAMS`) timestamp
    /// — always strictly within `(t_split_ns, end_ns]`, and (since neither
    /// the inputs nor `HYDRATION_RESULT_STREAMS` depend on breadth)
    /// bit-identical for the same `(seed, i)` at every breadth.
    fn result_timestamp_ns(&self, seed: u64, i: u32) -> i64 {
        let jitter_ns = splitmix64(seed ^ u64::from(i)) as i64;
        let slot_start = self.t_split_ns + 1 + i64::from(i) * self.result_slot_width;
        let offset = jitter_ns.rem_euclid(self.result_slot_width);
        (slot_start + offset).clamp(self.t_split_ns + 1, self.end_ns)
    }

    /// Filler stream `j` (`0..filler_streams`)'s timestamp — always
    /// strictly within `(start_ns, t_split_ns)`.
    fn filler_timestamp_ns(&self, seed: u64, j: u32) -> i64 {
        // Filler stream ordinals share the result band's `0..breadth`
        // ordinal space (`i = HYDRATION_RESULT_STREAMS + j` in
        // `load_broad_tier`'s loop) — reusing that same absolute `i` as the
        // jitter key here would collide with a result stream's own jitter
        // input whenever `j` happens to equal some result `i`; offsetting
        // by `HYDRATION_RESULT_STREAMS` keeps every jitter key distinct.
        let jitter_ns = splitmix64(seed ^ u64::from(HYDRATION_RESULT_STREAMS + j)) as i64;
        let slot_start = self.start_ns + 1 + i64::from(j) * self.filler_slot_width;
        let offset = jitter_ns.rem_euclid(self.filler_slot_width);
        (slot_start + offset).clamp(self.start_ns + 1, self.t_split_ns - 1)
    }
}

/// Loads one breadth's `logs-hydration` corpus into the database `client`
/// is bound to (already freshly schema-initialized by the caller — every
/// breadth gets its own reset database, see the module-level doc comment):
/// [`HYDRATION_RESULT_STREAMS`] result-bearing streams, each carrying
/// exactly one sample in the newest timestamp band `(t_split_ns, end_ns]`,
/// plus `breadth - HYDRATION_RESULT_STREAMS` filler streams, each carrying
/// exactly one sample in the older band `(start_ns, t_split_ns]`. One
/// sample per stream (not the baseline generator's many-samples-per-stream
/// shape) keeps `ORDER BY timestamp_ns DESC LIMIT HYDRATION_RESULT_STREAMS`
/// an exact, unambiguous split between the two bands — the result
/// fingerprint set is always exactly the `HYDRATION_RESULT_STREAMS`
/// streams, never a partial mix (architect plan R6). Timestamps are drawn
/// from a per-row jitter keyed on `(spec.seed, i)` so every row's
/// `timestamp_ns` is globally unique (F5) even before the gate's total-order
/// tiebreak is applied.
pub async fn load_broad_tier(
    client: &ChClient,
    spec: &BroadDatasetSpec,
) -> anyhow::Result<BroadDatasetSummary> {
    anyhow::ensure!(
        spec.breadth >= HYDRATION_RESULT_STREAMS,
        "--breadths: {} is below the fixed result-set size ({HYDRATION_RESULT_STREAMS}) — every \
         breadth must be able to carry the full result-bearing set",
        spec.breadth
    );

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
    let streams = build_broad_streams(spec.breadth);

    let end_ns = spec.ref_ns;
    // A one-hour window, split evenly: the newest half carries the
    // result-bearing streams' samples, the oldest half the filler streams'
    // — see this function's doc comment.
    let start_ns = end_ns - HYDRATION_WINDOW_NS;
    let t_split_ns = start_ns + HYDRATION_WINDOW_NS / 2;
    // `end_ns` is the caller-supplied `spec.ref_ns`; reject (rather than
    // panic) if it resolves to a month-start outside the ClickHouse `Date`
    // range instead of assuming it is a representable wall-clock instant.
    let month = Date::start_of_month_utc(end_ns)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "broad-tier ref_ns {end_ns} resolves to a month-start outside the ClickHouse Date range"
            )
        })?
        .days_since_epoch();

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

    let result_streams = HYDRATION_RESULT_STREAMS.min(spec.breadth);
    let filler_streams = spec.breadth - result_streams;
    let bands = BroadTimestampBands::new(start_ns, t_split_ns, end_ns, filler_streams);

    let mut batch: Vec<SeedSampleRow> = Vec::with_capacity(streams.len());
    for (i, stream) in streams.iter().enumerate() {
        let i = i as u32;
        let is_result = i < result_streams;
        let timestamp_ns = if is_result {
            bands.result_timestamp_ns(spec.seed, i)
        } else {
            bands.filler_timestamp_ns(spec.seed, i - result_streams)
        };
        batch.push(SeedSampleRow {
            service: stream.service.clone(),
            fingerprint: stream.fingerprint,
            timestamp_ns,
            severity: ((i % 24) + 1) as i8,
            body: gen_body(u64::from(i), &stream.service, false),
        });
    }
    for chunk in batch.chunks(INSERT_BATCH_ROWS) {
        client.insert_block(samples_table, chunk).await?;
    }

    if spec.dist {
        poll_count_until_visible(
            client,
            &format!("SELECT count() AS n FROM {samples_table}"),
            streams.len() as u64,
            samples_table,
        )
        .await?;
    }

    let result_fingerprints = streams[..result_streams as usize]
        .iter()
        .map(|s| s.fingerprint)
        .collect();

    Ok(BroadDatasetSummary {
        breadth: spec.breadth,
        service: HYDRATION_SERVICE.to_string(),
        result_streams,
        filler_streams,
        result_fingerprints,
        start_ns,
        end_ns,
        t_split_ns,
        load_elapsed_ms: start_instant.elapsed().as_millis() as u64,
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

    #[test]
    fn build_broad_streams_all_share_the_single_hydration_service() {
        let streams = build_broad_streams(50);
        assert!(streams.iter().all(|s| s.service == HYDRATION_SERVICE));
    }

    #[test]
    fn build_broad_streams_fingerprints_are_distinct() {
        let streams = build_broad_streams(200);
        let mut fps: Vec<u64> = streams.iter().map(|s| s.fingerprint).collect();
        fps.sort_unstable();
        fps.dedup();
        assert_eq!(fps.len(), 200);
    }

    /// The architect plan's R6 corpus property: the first
    /// `HYDRATION_RESULT_STREAMS` streams' construction does not depend on
    /// `breadth` at all, so their fingerprints are byte-identical whichever
    /// breadth they were generated for.
    #[test]
    fn build_broad_streams_result_set_fingerprints_are_identical_across_breadths() {
        let small = build_broad_streams(HYDRATION_RESULT_STREAMS);
        let large = build_broad_streams(HYDRATION_RESULT_STREAMS * 500);
        for i in 0..HYDRATION_RESULT_STREAMS as usize {
            assert_eq!(
                small[i].fingerprint, large[i].fingerprint,
                "stream ordinal {i} diverged across breadths"
            );
            assert_eq!(small[i].labels_json, large[i].labels_json);
        }
    }

    /// A representative window, matching `load_broad_tier`'s own
    /// construction (`start_ns`/`t_split_ns`/`end_ns` derived from
    /// `HYDRATION_WINDOW_NS`).
    fn test_bands(filler_streams: u32) -> (i64, i64, i64, BroadTimestampBands) {
        let end_ns = 1_800_000_000_000_000_000i64;
        let start_ns = end_ns - HYDRATION_WINDOW_NS;
        let t_split_ns = start_ns + HYDRATION_WINDOW_NS / 2;
        let bands = BroadTimestampBands::new(start_ns, t_split_ns, end_ns, filler_streams);
        (start_ns, t_split_ns, end_ns, bands)
    }

    /// Code review finding (issue #35, [high]): every result-bearing
    /// stream's timestamp must land strictly within `(t_split_ns, end_ns]`
    /// — no jitter may cross either boundary — at every breadth this
    /// scenario sweeps, including the extremes.
    #[test]
    fn result_timestamps_never_cross_the_band_boundary_at_any_breadth() {
        for breadth in [HYDRATION_RESULT_STREAMS, 1_000, 10_000, 50_000, 100_000] {
            let filler_streams = breadth - HYDRATION_RESULT_STREAMS;
            let (_, t_split_ns, end_ns, bands) = test_bands(filler_streams);
            for i in 0..HYDRATION_RESULT_STREAMS {
                let ts = bands.result_timestamp_ns(42, i);
                assert!(
                    ts > t_split_ns && ts <= end_ns,
                    "breadth={breadth} i={i}: timestamp {ts} escaped (t_split_ns={t_split_ns}, \
                     end_ns={end_ns}]"
                );
            }
        }
    }

    /// Code review finding companion: every filler stream's timestamp must
    /// land strictly below `t_split_ns` (and above `start_ns`) at every
    /// breadth, including the largest filler count this scenario sweeps.
    #[test]
    fn filler_timestamps_never_cross_the_band_boundary_at_any_breadth() {
        for breadth in [HYDRATION_RESULT_STREAMS + 1, 1_000, 10_000, 50_000] {
            let filler_streams = breadth - HYDRATION_RESULT_STREAMS;
            let (start_ns, t_split_ns, _, bands) = test_bands(filler_streams);
            for j in 0..filler_streams {
                let ts = bands.filler_timestamp_ns(42, j);
                assert!(
                    ts > start_ns && ts < t_split_ns,
                    "breadth={breadth} j={j}: timestamp {ts} escaped (start_ns={start_ns}, \
                     t_split_ns={t_split_ns})"
                );
            }
        }
    }

    /// The R6 property the whole corpus design depends on: the fixed
    /// result-bearing set's timestamps (not just its fingerprints/labels —
    /// see `build_broad_streams_result_set_fingerprints_are_identical_across_breadths`)
    /// are bit-identical across breadths, since `BroadTimestampBands` sizes
    /// its result slots off the constant `HYDRATION_RESULT_STREAMS`, never
    /// off a breadth-derived count.
    #[test]
    fn result_timestamps_are_bit_identical_across_breadths() {
        let (_, _, _, small_bands) = test_bands(1_000 - HYDRATION_RESULT_STREAMS);
        let (_, _, _, large_bands) = test_bands(50_000 - HYDRATION_RESULT_STREAMS);
        for i in 0..HYDRATION_RESULT_STREAMS {
            assert_eq!(
                small_bands.result_timestamp_ns(42, i),
                large_bands.result_timestamp_ns(42, i),
                "result stream {i}'s timestamp diverged across breadths"
            );
        }
    }

    /// Every timestamp in one breadth pass — result and filler alike — is
    /// globally unique (F5): disjoint per-stream slots guarantee this by
    /// construction, not by jitter-entropy luck.
    #[test]
    fn every_timestamp_in_one_breadth_pass_is_globally_unique() {
        let breadth = 10_000u32;
        let filler_streams = breadth - HYDRATION_RESULT_STREAMS;
        let (_, _, _, bands) = test_bands(filler_streams);
        let mut seen = std::collections::HashSet::new();
        for i in 0..HYDRATION_RESULT_STREAMS {
            assert!(
                seen.insert(bands.result_timestamp_ns(7, i)),
                "duplicate result timestamp"
            );
        }
        for j in 0..filler_streams {
            assert!(
                seen.insert(bands.filler_timestamp_ns(7, j)),
                "duplicate filler timestamp"
            );
        }
    }
}
