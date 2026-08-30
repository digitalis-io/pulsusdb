//! `EXPLAIN indexes = 1` snapshot assertions against a live ClickHouse
//! (docs/schemas.md §9's regression harness: "a query silently losing its
//! primary-index prefix or skip-index usage fails the build"). Gated
//! behind `PULSUS_TEST_CLICKHOUSE=1`, reusing the #5 harness verbatim
//! (`crates/pulsus-schema/tests/live_schema.rs`'s connection/setup
//! pattern) — the CI `schema-it` job runs this after the live schema
//! tests, against the same ClickHouse 26.3 container.
//!
//! **Coverage (fix-plan amendment §4, code review FAIL):** every canonical
//! query shape from `tests/sql_snapshots.rs`'s matrix gets its own
//! `EXPLAIN indexes = 1` case here — stage 1 (single-eq / multi-eq / regex
//! / mixed positive+negative), stage 2 hydration, every stage-3 line-filter
//! op, and metric reads (rollup-served and the raw fallback, range and
//! instant). Direction/limit variants are deliberately **not** duplicated
//! here: they affect `ORDER BY`/`Sorting`, not index selection, and are
//! already exercised as pure SQL-generation snapshots in
//! `sql_snapshots.rs` — this file's job is index *usage*, not SQL text.
//!
//! **Assertion strength (round-2 review disposition):** raw `EXPLAIN` text
//! embeds volatile `Parts:`/`Granules:` counts (vary with data volume/
//! merges) and, since fixture timestamps must be wall-clock-recent (see
//! `now_ns()` below), literal nanosecond values that differ every run.
//! [`index_usage`] reduces the raw text to its stable, index-relevant lines
//! (block titles, `Keys:` + key names, `Condition:`, skip-index `Name:`)
//! and [`normalize_numbers`] collapses every digit run to `#`, producing a
//! deterministic `Vec<String>`. **Every case below `assert_eq!`s the
//! *complete* extract** against a captured expectation — not a
//! property-subset helper (a prior revision used `block_columns`/
//! `skip_index_names` picks, which can miss a real regression: a skip
//! index still listed but with `Condition: true` pruning nothing, a block
//! silently appearing/vanishing, or block order changing). A full-extract
//! `assert_eq!` catches all of those; docs/schemas.md §9's "snapshot-
//! tested" names the equality-comparison mechanism, not "capture raw
//! EXPLAIN text" (which would be non-deterministic here and fail on
//! non-regressions — see the architect's round-2 disposition on issue #11).
//!
//! Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test explain_indexes
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_logql::parse;
use pulsus_read::logql::predicate::literal;
use pulsus_read::logql::sql::{self, ScanProjection, TimeWindow};
use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec, plan};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job, so a lost `env:` block reddens the
/// build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

fn test_ctx(db: &str) -> SchemaParams {
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/explain_indexes.rs for setup)"
            );
            return;
        }
    };
}

async fn drop_database(client: &ChClient, db: &str) {
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    explain: String,
}

async fn explain_raw(client: &ChClient, sql: &str) -> String {
    // The `clickhouse` crate's own query builder treats a bare `?` in SQL
    // text as an unbound bind-argument placeholder; a regex matcher's own
    // `(?:...)` anchoring syntax (`escape::ch_regex_anchored`) always
    // contains one. Double it here exactly as `LogQlEngine::query_stream`
    // does internally — this test file calls `ChClient` directly, bypassing
    // that wrapper, so it must apply the same fix.
    let full = format!("EXPLAIN indexes = 1 {sql}").replace('?', "??");
    let mut stream = client
        .query_stream::<ExplainRow>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("explain query failed: {e}\nSQL:\n{full}"));
    let mut out = String::new();
    while let Some(row) = stream.next().await {
        out.push_str(&row.expect("decode explain row").explain);
        out.push('\n');
    }
    out
}

/// Collapses every run of ASCII digits in `s` to a single `#`, so a
/// deterministic-but-dynamic value (a fixture's wall-clock nanosecond
/// timestamp, a literal fingerprint) doesn't defeat an `assert_eq!`
/// snapshot.
fn normalize_numbers(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() {
            out.push('#');
            while matches!(chars.peek(), Some(d) if d.is_ascii_digit()) {
                chars.next();
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// ClickHouse's `EXPLAIN indexes = 1` block titles this crate's tables ever
/// produce (`MinMax`/`Partition`/`PrimaryKey` for `ORDER BY`/`PARTITION BY`
/// analysis, `Skip` per `tokenbf_v1`/`ngrambf_v1`/`minmax` secondary
/// index) — kept as an explicit allow-list so [`index_usage`]'s extract is
/// self-describing (which *kind* of index, not just position-in-list).
const INDEX_BLOCK_TITLES: &[&str] = &["MinMax", "Partition", "PrimaryKey", "Skip"];

/// The `Name:` line of the pseudo-block ClickHouse 26.x emits when a
/// filter mixes AND and OR over skip-indexed columns. It is not an index
/// the table declares, so [`index_usage`] excludes it and
/// [`combined_skip_present`] reports it separately — it can never be
/// silently absorbed into a declared-index name set.
const COMBINED_SKIP_NAME: &str = "Name: <Combined skip indexes>";

/// `true` when ClickHouse's `Name: <Combined skip indexes>` pseudo-block
/// is in `raw`.
///
/// Measured for issue #376 on the real `log_samples` DDL with a 100k-row
/// corpus: absent on 24.8.14.39 for every stage-3 shape; on 26.3.17.110
/// absent for every single-predicate shape and **present** for the shape
/// whose predicate genuinely mixes AND and OR over `body`. Issue #450
/// moved which shape that is: `!=` used to render
/// `NOT (hasToken AND hasToken AND position)` — a negated conjunction —
/// and now renders `NOT (body LIKE …)`, a single negated predicate, so it
/// no longer carries the block; the `or` group
/// (`((body LIKE …) OR (body LIKE …))`) does. Net granule selection is
/// unchanged by the block's presence, so it is a reporting addition, not
/// a plan change.
fn combined_skip_present(raw: &str) -> bool {
    raw.lines().any(|l| l.trim() == COMBINED_SKIP_NAME)
}

/// Reduces raw `EXPLAIN indexes = 1` text to its stable, index-relevant
/// lines: block titles (`PrimaryKey`/`Skip`/...), `Keys:` plus the
/// key-name lines under it, `Condition:`, and skip-index `Name:` lines.
/// Drops everything else (`Parts:`/`Granules:` row/mark counts — the
/// volatile detail docs/schemas.md §9 doesn't care about; what it cares
/// about is which columns and which skip indexes are in play) and
/// collapses digit runs via [`normalize_numbers`] so a fixture's
/// wall-clock-dependent timestamp literals don't defeat `assert_eq!`.
///
/// Two lines 26.x adds under `PrimaryKey` — `Search Algorithm:` and
/// `Ranges:` — fall out here: neither is a block title, neither starts
/// `Condition:`/`Name:`, and both carry a `:` so they also close a `Keys:`
/// run. Verified line by line against both servers (issue #376).
///
/// The `<Combined skip indexes>` pseudo-block is excluded entirely (title
/// and name), because it is not an index the table declares; ask
/// [`combined_skip_present`] for it instead.
fn index_usage(raw: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut capturing_keys = false;
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed == COMBINED_SKIP_NAME {
            // Drop the `Skip` title this pseudo-block's name belongs to,
            // so the extract holds only declared indexes.
            assert_eq!(
                out.last().map(String::as_str),
                Some("Skip"),
                "a `{COMBINED_SKIP_NAME}` line must directly follow its own `Skip` block title"
            );
            out.pop();
            capturing_keys = false;
            continue;
        }
        if INDEX_BLOCK_TITLES.contains(&trimmed) {
            out.push(trimmed.to_string());
            capturing_keys = false;
            continue;
        }
        if trimmed == "Keys:" {
            out.push(trimmed.to_string());
            capturing_keys = true;
            continue;
        }
        if capturing_keys {
            // Bare key-name lines carry no `:`; the first line that does
            // (or a blank line) ends the `Keys:` block.
            if !trimmed.is_empty() && !trimmed.contains(':') {
                out.push(normalize_numbers(trimmed));
                continue;
            }
            capturing_keys = false;
        }
        if trimmed.starts_with("Condition:") || trimmed.starts_with("Name:") {
            out.push(normalize_numbers(trimmed));
        }
    }
    out
}

async fn explain(client: &ChClient, sql: &str) -> Vec<String> {
    index_usage(&explain_raw(client, sql).await)
}

/// The FIRST `Parts: m/n` count in raw `EXPLAIN indexes = 1` text —
/// deliberately not part of [`index_usage`]'s extract, which drops these
/// counts as volatile (issue #11's round-2 disposition). Issue #399 needs
/// them for exactly one claim the extract cannot make: that a `Condition:`
/// on `bucket_ns` actually *prunes* parts rather than merely appearing.
/// The first block ClickHouse prints for these queries is `MinMax`, whose
/// `Parts:` line is the partition-level prune the assertion is about.
fn parts_selected(raw: &str) -> Option<(u64, u64)> {
    for line in raw.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("Parts: ")
            && let Some((m, n)) = rest.split_once('/')
        {
            return Some((m.trim().parse().ok()?, n.trim().parse().ok()?));
        }
    }
    None
}

/// The LAST `Granules: m/n` count in raw `EXPLAIN indexes = 1` text — the
/// net granule selection after every block has narrowed in turn, which is
/// what [`assert_prunes_at_least`] compares. Like [`parts_selected`] this
/// is deliberately outside [`index_usage`]'s extract; it is used only
/// inside a same-server comparison, never as a pinned constant.
fn last_granules(raw: &str) -> Option<(u64, u64)> {
    let mut found = None;
    for line in raw.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("Granules: ")
            && let Some((m, n)) = rest.split_once('/')
        {
            found = Some((m.trim().parse().ok()?, n.trim().parse().ok()?));
        }
    }
    found
}

/// One `Skip` block of an `EXPLAIN indexes = 1`, reduced to the two lines
/// that say WHICH index it is and WHAT it is: `Name:` and `Description:`
/// (`<type> GRANULARITY <n>`), joined with `|` and digit-normalised.
///
/// `Description:` is captured on purpose. Without it the gate would accept
/// `idx_body_tokens` silently becoming a different index type or
/// granularity.
///
/// `Condition:` is captured too, and its ABSENCE is recorded explicitly as
/// `Condition: <none>` rather than skipped — **but for the bloom-filter
/// family it is always absent, and that is a fact about ClickHouse, not an
/// omission here.** Measured on 26.3.17.110 over five index types on one
/// table:
///
/// | index type | `Condition:` under its `Skip` block |
/// |---|---|
/// | `minmax` | `Condition: (severity in [4, +Inf))` |
/// | `set` | `Condition: (fingerprint in 2-element set)` |
/// | `tokenbf_v1` | **none emitted** |
/// | `ngrambf_v1` | **none emitted** |
/// | `bloom_filter` | **none emitted** |
///
/// Stage 3's two skip indexes are `tokenbf_v1` and `ngrambf_v1`, so there
/// is no condition text to pin for them and never was — the pre-#376
/// literal expectation did not carry one either. The failure this file's
/// header describes ("a skip index still listed but with `Condition: true`
/// pruning nothing") is therefore caught for these indexes by
/// [`assert_prunes_at_least`] against a same-server control, not by
/// condition text: a bloom filter that stops ruling granules out makes the
/// gated and control granule counts equal, and the ratio collapses to 1.
/// That is why the stage-3 fixture seeds a real corpus — see
/// [`seed_line_filter_corpus`].
fn skip_blocks(raw: &str) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    let mut lines = raw.lines().map(str::trim).peekable();
    while let Some(line) = lines.next() {
        if line != "Skip" {
            continue;
        }
        let mut name = None;
        let mut description = None;
        let mut condition = None;
        // PEEK, never consume, at the boundary: a block title belongs to
        // the NEXT block, and taking it here would swallow every second
        // `Skip` block. (It did, until the corpus fixture made a
        // three-block plan visible — the two-row fixture hid it.)
        while let Some(inner) = lines.peek() {
            let inner = *inner;
            if INDEX_BLOCK_TITLES.contains(&inner) || inner.is_empty() {
                break;
            }
            lines.next();
            if inner.starts_with("Name: ") {
                name = Some(inner.to_string());
            } else if inner.starts_with("Description: ") {
                description = Some(normalize_numbers(inner));
            } else if inner.starts_with("Condition: ") {
                condition = Some(normalize_numbers(inner));
            }
            // `Parts:`/`Granules:` are volatile and are handled by the
            // pruning identity instead, so they are consumed and dropped.
        }
        let name = name.unwrap_or_else(|| "Name: <missing>".to_string());
        // 26.x's `<Combined skip indexes>` pseudo-block is not an index the
        // table declares (its own `Description:` says so — "Final set of
        // granules after AND/OR processing"), so it never enters this set.
        // [`combined_skip_present`] reports it separately and each shape
        // asserts it explicitly, which is what stops it being absorbed here.
        if name == COMBINED_SKIP_NAME {
            continue;
        }
        let description = description.unwrap_or_else(|| "Description: <missing>".to_string());
        // `Condition:` is recorded as `<none>` when the server emits none,
        // so its ABSENCE is asserted rather than merely unobserved — a
        // condition appearing where the expectation says there is none is
        // a change the gate reports.
        let condition = condition.unwrap_or_else(|| "Condition: <none>".to_string());
        out.insert(format!("{name}|{description}|{condition}"));
    }
    out
}

/// Hermetic: [`skip_blocks`] captures a skip block's `Condition:` where the
/// server emits one, records `<none>` where it does not, and never lets one
/// block swallow the next.
///
/// The input is a verbatim `EXPLAIN indexes = 1` capture from
/// 26.3.17.110 over a table carrying `minmax`, `tokenbf_v1` and
/// `ngrambf_v1` indexes. It is here because the live stage-3 shapes only
/// exercise the two bloom filters, which emit no condition at all — without
/// this, "conditions are pinned" would be a claim with no case that
/// exercises it. It also pins the block-boundary behaviour: an earlier
/// revision consumed the next block's title while scanning a block, which
/// silently dropped every second `Skip` block.
#[test]
fn skip_block_conditions_are_captured_and_blocks_do_not_swallow_each_other() {
    const RAW: &str = "\
          Indexes:\n\
            MinMax\n\
              Keys:\n\
                timestamp_ns\n\
              Condition: (timestamp_ns in [1, +Inf))\n\
              Parts: 1/1\n\
              Granules: 12/12\n\
            Skip\n\
              Name: idx_severity\n\
              Description: minmax GRANULARITY 4\n\
              Condition: (severity in [4, +Inf))\n\
              Parts: 1/1\n\
              Granules: 12/12\n\
            Skip\n\
              Name: idx_body_tokens\n\
              Description: tokenbf_v1 GRANULARITY 1\n\
              Parts: 1/1\n\
              Granules: 5/12\n\
            Skip\n\
              Name: idx_body_ngrams\n\
              Description: ngrambf_v1 GRANULARITY 1\n\
              Parts: 1/1\n\
              Granules: 5/5\n\
            Ranges: 5\n";

    let blocks = skip_blocks(RAW);
    assert_eq!(
        blocks,
        [
            "Name: idx_severity|Description: minmax GRANULARITY #|Condition: (severity in [#, +Inf))",
            "Name: idx_body_tokens|Description: tokenbf_v# GRANULARITY #|Condition: <none>",
            "Name: idx_body_ngrams|Description: ngrambf_v# GRANULARITY #|Condition: <none>",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<std::collections::BTreeSet<_>>(),
        "all three blocks, the minmax condition captured, the bloom filters' absence recorded"
    );

    // The `MinMax` block's own `Condition:` is NOT a skip block's and must
    // not be attributed to one.
    assert!(
        !blocks
            .iter()
            .any(|b| b.contains("timestamp_ns in [#, +Inf)")),
        "a prefix block's condition leaked into the skip set: {blocks:?}"
    );
}

/// The `Skip` blocks a stage-3 line filter must show — **committed here,
/// not derived from the server**.
///
/// This was briefly read out of `system.data_skipping_indices`, and code
/// review caught why that is wrong: the planner reads the same catalog, so
/// dropping `idx_body_ngrams` from the DDL moved BOTH sides and the gate
/// passed on a table that had lost an index. An expectation derived from
/// the thing it checks is not a check. The owner's rule for this bump is
/// that a moved plan may never be replaced by something that would pass on
/// a worse configuration, so the names and their types are written down
/// here, where only a human edit can move them.
///
/// What legitimately changed with the 26.3 move is the **comparison**, not
/// the source: this is a SET, because the order in which ClickHouse applies
/// two skip indexes over one column is the planner's choice. Measured both
/// ways — 24.8.14.39 follows the DDL declaration order, and 26.3.17.110
/// chooses: on a 50k fixture it reports `ngrams, tokens` where 24.8 reports
/// `tokens, ngrams` for the identical query and DDL, while on a 100k
/// fixture with a different body it reports `tokens, ngrams` like 24.8.
/// Net granule selection is identical in every one of those cases, so the
/// order was never a correctness property — but it IS unstable on 26.3, and
/// asserting it would redden on data volume alone.
fn expected_stage3_skip_blocks() -> std::collections::BTreeSet<String> {
    [
        "Name: idx_body_tokens|Description: tokenbf_v# GRANULARITY #|Condition: <none>",
        "Name: idx_body_ngrams|Description: ngrambf_v# GRANULARITY #|Condition: <none>",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

/// An extract with its `Skip` blocks removed — the `MinMax`/`Partition`/
/// `PrimaryKey` prefix, whose ORDER is asserted (those three are printed
/// in a fixed order that reflects how ClickHouse narrows, and a block
/// appearing or vanishing there IS a plan change).
fn without_skip_blocks(usage: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut in_skip = false;
    for line in usage {
        if line == "Skip" {
            in_skip = true;
            continue;
        }
        if INDEX_BLOCK_TITLES.contains(&line.as_str()) {
            in_skip = false;
        }
        if !in_skip {
            out.push(line.clone());
        }
    }
    out
}

/// The in-run pruning identity (issue #376 rule R2). Runs `sql` and
/// `control_sql` against the **same server in the same test** and asserts
/// the gated form's net granule selection is at least `k`x smaller.
///
/// Both numbers come from the server, so a pasted expectation cannot
/// satisfy this — which is what makes "regenerate until green"
/// insufficient rather than merely discouraged. `control_sql` is normally
/// `sql` with `SETTINGS use_skip_indexes = 0`.
///
/// `k = 1` is a real and sometimes the ONLY honest claim: for a negated
/// line filter (`!=`, `!~`) a bloom filter cannot rule a granule out at
/// all, so the correct assertion is "never worse than no skip index",
/// not a ratio. Callers say which they mean.
async fn assert_prunes_at_least(
    client: &ChClient,
    sql: &str,
    control_sql: &str,
    k: u64,
    what: &str,
) {
    let gated_raw = explain_raw(client, sql).await;
    let control_raw = explain_raw(client, control_sql).await;
    let (gated, _) = last_granules(&gated_raw)
        .unwrap_or_else(|| panic!("{what}: no Granules: line in the gated EXPLAIN"));
    let (control, _) = last_granules(&control_raw)
        .unwrap_or_else(|| panic!("{what}: no Granules: line in the control EXPLAIN"));
    assert!(
        gated.saturating_mul(k) <= control,
        "{what}: gated selected {gated} granules against the control's {control}; required \
         gated x {k} <= control"
    );
}

/// The rollup resolution `test_ctx` renders (`log_rollup: 5s`).
const ROLLUP_RES_NS: u64 = 5_000_000_000;

fn plan_ctx(db: &str) -> PlanCtx<'_> {
    PlanCtx {
        db,
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    }
}

// One fixture stream, `service_name="checkout", env="prod"`, plus two
// `log_samples` rows — enough for every canonical shape's EXPLAIN to run
// genuine primary-key/skip-index analysis (ClickHouse's index-usage
// analysis is query/schema-driven, not row-content-driven: it needs *some*
// data in the queried partition/time-range so the optimizer doesn't
// short-circuit to a `NullSource`, not a literal match on the query's
// specific predicate values).
const FP_PROD: u64 = 18_374_000_000_000_000_001;

/// Nanoseconds since the Unix epoch, right now. Fixture timestamps must be
/// wall-clock-recent (not a fixed historical constant): `log_samples`'s
/// `ttl_only_drop_parts = 1` retention (docs/schemas.md §3.1) makes an
/// already-expired part eligible for background deletion almost
/// immediately, which would flake a fixed-date fixture the same way
/// `live_schema.rs`'s smoke insert documents (issue #5).
fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

async fn seed(client: &ChClient, db: &str, ts_ns: i64) {
    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, updated_ns) VALUES \
                 (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({ts_ns}))), {FP_PROD}, 'checkout', \
                 '{{\"env\":\"prod\",\"service_name\":\"checkout\"}}', 0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_streams");

    client
        .execute(
            &format!(
                "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) VALUES \
                 ('checkout', {FP_PROD}, {ts_ns}, 9, 'connection refused'), \
                 ('checkout', {FP_PROD}, {ts_plus}, 0, 'request completed')",
                ts_plus = ts_ns + 1_000_000_000
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed log_samples");
}

/// Rows the line-filter fixture seeds. At the default
/// `index_granularity = 8192` this spans ~13 granules, which is what makes
/// bloom-filter pruning OBSERVABLE — the two-row fixture the rest of this
/// file uses fits in one granule, where every skip index trivially selects
/// 1/1 and a degraded one is indistinguishable from a working one.
const LINE_FILTER_CORPUS_ROWS: u64 = 100_000;

/// The needle occupies a narrow, known row range, so selectivity is a
/// controlled constant rather than incidental to the data.
const NEEDLE_START: u64 = 50_000;
const NEEDLE_COUNT: u64 = 4;

/// Seeds [`LINE_FILTER_CORPUS_ROWS`] rows into `log_samples` on top of the
/// shared fixture, server-side so no rows cross the wire.
///
/// **Why the stage-3 shapes need this and the rest of the file does not.**
/// Code review (issue #376, round 2) found that pinning a skip block's
/// name, type and granularity still admits an index that is declared,
/// correct and pruning NOTHING — the regression this file's header says it
/// exists to catch. For `tokenbf_v1`/`ngrambf_v1` there is no
/// `Condition:` text to pin (measured — see [`skip_blocks`]), so the only
/// thing that distinguishes a working bloom filter from a dead one is
/// whether it rules granules out, and that is invisible on a single
/// granule. With this corpus the gated shape selects far fewer granules
/// than a `use_skip_indexes = 0` control, and a dead index collapses the
/// ratio to 1.
async fn seed_line_filter_corpus(client: &ChClient, db: &str, ts_ns: i64) {
    let sql = format!(
        "INSERT INTO {db}.log_samples (service, fingerprint, timestamp_ns, severity, body) \
         SELECT 'checkout', {FP_PROD}, \
                toInt64({ts_ns}) - toInt64(number) * 36000000, 0, \
                if(number >= {NEEDLE_START} AND number < {NEEDLE_START} + {NEEDLE_COUNT}, \
                   concat('row ', toString(number), ' connection refused padding_', \
                          repeat('x', 120)), \
                   concat('row ', toString(number), ' routine request completed padding_', \
                          repeat('x', 120))) \
         FROM numbers({LINE_FILTER_CORPUS_ROWS})"
    );
    client
        .execute(&sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .expect("seed the line-filter corpus");
}

/// [`setup`] plus [`seed_line_filter_corpus`] — the fixture every stage-3
/// line-filter shape uses.
async fn setup_with_line_filter_corpus(db: &str, ts_ns: i64) -> ChClient {
    let client = setup(db, ts_ns).await;
    seed_line_filter_corpus(&client, db, ts_ns).await;
    client
}

/// Sets up a fresh database, seeds fixture data around `ts_ns`, and returns
/// a client bound directly to that database.
async fn setup(db: &str, ts_ns: i64) -> ChClient {
    let client = ChClient::new(test_config()).await.expect("connect");
    drop_database(&client, db).await;
    run_init(&client, &test_ctx(db)).await.expect("run_init");

    let mut data_cfg = test_config();
    data_cfg.database = db.to_string();
    let data_client = ChClient::new(data_cfg)
        .await
        .expect("connect (data client)");
    seed(&data_client, db, ts_ns).await;
    data_client
}

/// A `[now - 6h, now]` window bracketing `ts_ns` (the seeded samples'
/// timestamp), matching docs/schemas.md §3.2's canonical "last 6h" example
/// shape.
fn range_params(ts_ns: i64) -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts_ns - 6 * 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

fn streams_plan(query: &str, params: &QueryParams, db: &str) -> pulsus_read::logql::StreamsPlan {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &plan_ctx(db)).expect("plan") {
        Plan::Streams(sp) => sp,
        Plan::Metric(_) | Plan::MetricBinary(_) => panic!("expected a Streams plan"),
    }
}

fn metric_plan(query: &str, params: &QueryParams, db: &str) -> pulsus_read::logql::MetricPlan {
    let expr = parse(query).expect("parse");
    match plan(&expr, params, &plan_ctx(db)).expect("plan") {
        Plan::Metric(mp) => mp,
        Plan::Streams(_) | Plan::MetricBinary(_) => panic!("expected a Metric plan"),
    }
}

/// Qualifies a bare (unqualified) table name in `sql` with `db.` — plan-
/// generated SQL targets the connection's default database, but these
/// tests connect via a shared-port client without a fixed default.
fn qualify(sql: &str, table: &str, db: &str) -> String {
    sql.replacen(table, &format!("{db}.{table}"), 1)
}

// ---------------------------------------------------------------------
// Stage 1 — matcher normalization shapes.
// ---------------------------------------------------------------------

/// Builds a `Vec<String>` expectation literal concisely for the
/// `assert_eq!`s below.
fn v(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

async fn stage1_usage(db: &str, ts_ns: i64, client: &ChClient, query: &str) -> Vec<String> {
    let sp = streams_plan(query, &range_params(ts_ns), db);
    let sql = qualify(&sp.stage1_sql, "log_streams_idx", db);
    explain(client, &sql).await
}

#[tokio::test]
async fn stage1_single_equality_uses_the_key_val_primary_key() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_s1_single");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = stage1_usage(db, ts_ns, &client, r#"{service_name="checkout"}"#).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "Partition",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "PrimaryKey",
            "Keys:",
            "key",
            "val",
            "Condition: and((val in ['checkout', 'checkout']), (key in ['service_name', 'service_name']))",
        ])
    );
}

#[tokio::test]
async fn stage1_multi_equality_uses_the_key_val_primary_key() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_s1_multi");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = stage1_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout", env="prod"}"#,
    )
    .await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "Partition",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "PrimaryKey",
            "Keys:",
            "key",
            "val",
            "Condition: or(and((val in ['prod', 'prod']), (key in ['env', 'env'])), and((val in ['checkout', 'checkout']), (key in ['service_name', 'service_name'])))",
        ])
    );
}

#[tokio::test]
async fn stage1_regex_matcher_uses_the_key_primary_key_prefix() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_s1_regex");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    // `match(val, ...)` isn't sargable — ClickHouse's key-condition
    // analyzer can only narrow the primary-key range on the plain
    // equality (`key = 'env'`); `val`'s regex condition still applies as
    // a residual filter, just not via primary-key pruning. This is
    // exactly docs/schemas.md §3.2's "regex matchers evaluated within one
    // key's index prefix — a scan over the distinct values of that key".
    let usage = stage1_usage(db, ts_ns, &client, r#"{env=~"prod|staging"}"#).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "Partition",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "PrimaryKey",
            "Keys:",
            "key",
            "Condition: (key in ['env', 'env'])",
        ])
    );
}

#[tokio::test]
async fn stage1_mixed_positive_and_negative_matchers_uses_the_key_val_primary_key() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_s1_mixed");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = stage1_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout", team!="qa"}"#,
    )
    .await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "Partition",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "PrimaryKey",
            "Keys:",
            "key",
            "val",
            "Condition: or(and((val in ['qa', 'qa']), (key in ['team', 'team'])), and((val in ['checkout', 'checkout']), (key in ['service_name', 'service_name'])))",
        ])
    );
}

// ---------------------------------------------------------------------
// Stage 2 — hydration.
// ---------------------------------------------------------------------

#[tokio::test]
async fn stage2_hydration_uses_the_fingerprint_primary_key() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_s2");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let table = format!("{db}.log_streams");
    let sql = sql::stage2(&table, &[FP_PROD]);

    let usage = explain(&client, &sql).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Condition: true",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "fingerprint",
            "Condition: (fingerprint in #-element set)",
        ])
    );
}

// ---------------------------------------------------------------------
// Stage 3 — samples, every line-filter op. All four line-filter ops below
// produce the same index-usage extract: the `service`/`fingerprint`/
// `timestamp_ns` primary-key `Condition:` only reflects those three
// columns (not `body`, which isn't part of the primary key), and both
// `body` skip indexes are always listed as considered whenever any
// predicate references `body` — the ops differ in generated SQL
// (`sql_snapshots.rs`'s job) but not in which indexes ClickHouse consults.
//
// **How the expectation is built (issue #376).** The `PrimaryKey` key
// list comes from `system.tables.sorting_key` and the `Skip` name set
// from `system.data_skipping_indices`, so neither can be satisfied by
// pasting an EXPLAIN back in. What stays literal is only the planner's
// rendering of OUR predicate (`Condition:`), which is the thing this gate
// exists to pin and which is byte-identical across 24.8.14.39 and
// 26.3.17.110 (measured).
//
// **Skip blocks are compared as a SET, not a list.** The order in which
// ClickHouse applies two skip indexes over the same column is the
// planner's choice and provably not a correctness property — it reaches
// the same net granule count either way — so asserting it was asserting
// something the gate never meant. The prefix blocks
// (`MinMax`/`Partition`/`PrimaryKey`) keep their ordered `assert_eq!`:
// one of those appearing or vanishing IS a plan change.
// ---------------------------------------------------------------------

async fn stage3_sql(db: &str, ts_ns: i64, query: &str) -> String {
    let sp = streams_plan(query, &range_params(ts_ns), db);
    let table = format!("{db}.log_samples");
    sql::stage3(
        &table,
        &[literal("checkout")],
        &[FP_PROD],
        TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        },
        &sp.line_filters,
        sp.direction,
        sp.scan_limit,
    )
}

/// How much a stage-3 shape's skip indexes must narrow the read, relative
/// to the same query with `use_skip_indexes = 0`.
///
/// Stated per shape because it is a property of the PREDICATE, not of the
/// server: a bloom filter can rule granules out for a positive line filter
/// and cannot for a negated one, on either version. Measured on the
/// fixture this suite seeds, 26.3.17.110.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrunesBy {
    /// A positive line filter (`|=`, `|~`): the bloom filters confine the
    /// read to the granules that can hold the needle. Measured 13/13
    /// granules on the control against 1 gated, i.e. 13x; the gate
    /// requires 4x, which is the floor a dead index cannot reach — a
    /// degraded condition makes gated == control and the ratio 1.
    Strongly,
    /// A negated line filter (`!=`, `!~`): a bloom filter answers "this
    /// granule MAY contain the token", which cannot rule a granule out
    /// for `NOT (...)`. The honest claim is "never worse than no skip
    /// index at all", and it is the same on both server versions —
    /// measured 12/12 on 24.8.14.39 and 26.3.17.110 alike.
    NotAtAll,
}

impl PrunesBy {
    fn factor(self) -> u64 {
        match self {
            PrunesBy::Strongly => 4,
            PrunesBy::NotAtAll => 1,
        }
    }
}

/// Whether a stage-3 shape is expected to carry 26.x's
/// `<Combined skip indexes>` pseudo-block. Asserted per shape, never
/// folded into the index-name set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CombinedSkip {
    /// The filter is a plain conjunction over `body`.
    Absent,
    /// The filter mixes AND and OR over `body` — an `or` group
    /// (`((body LIKE …) OR (body LIKE …))`) is the shape that does this in
    /// our SQL (issue #450; before it, `!=`'s negated conjunction did).
    Present,
}

/// Runs a stage-3 shape's `EXPLAIN indexes = 1` and makes the whole
/// stage-3 judgement: the ordered prefix, the derived skip-index set, the
/// combined-block expectation, and the in-run pruning identity against a
/// same-server `use_skip_indexes = 0` control.
async fn assert_stage3_usage(
    db: &str,
    ts_ns: i64,
    client: &ChClient,
    query: &str,
    combined: CombinedSkip,
    prunes: PrunesBy,
) {
    let sql = stage3_sql(db, ts_ns, query).await;
    let raw = explain_raw(client, &sql).await;
    let usage = index_usage(&raw);

    assert_eq!(
        without_skip_blocks(&usage),
        expected_stage3_prefix(),
        "stage-3 MinMax/Partition/PrimaryKey prefix for {query}"
    );
    assert_eq!(
        skip_blocks(&raw),
        expected_stage3_skip_blocks(),
        "stage-3 Skip blocks (name + type + granularity, as a SET) for {query}"
    );
    assert_eq!(
        combined_skip_present(&raw),
        combined == CombinedSkip::Present,
        "stage-3 `{COMBINED_SKIP_NAME}` presence for {query}"
    );

    // Rule R2, and the answer to code review round 2: a skip index that
    // is still declared, still the right type and granularity, but whose
    // condition has degraded to something that rules nothing out, is a
    // read-path regression this file exists to catch — and for a bloom
    // filter there is no `Condition:` text to pin (see [`skip_blocks`]).
    // What distinguishes a working bloom filter from a dead one is
    // whether it rules granules out, so that is what is asserted, against
    // a same-server `use_skip_indexes = 0` control whose number comes
    // from the same run.
    //
    // The fixture seeds a real corpus for exactly this reason
    // ([`seed_line_filter_corpus`]): on a single granule every index
    // trivially selects 1/1 and a dead one is indistinguishable.
    let control_sql = format!("{sql} SETTINGS use_skip_indexes = 0");
    assert_prunes_at_least(client, &sql, &control_sql, prunes.factor(), query).await;
}

/// The `MinMax`/`Partition`/`PrimaryKey` prefix every stage-3 line-filter
/// case asserts — **committed, not derived**.
///
/// The `PrimaryKey` key list was briefly read from
/// `system.tables.sorting_key`; that is the same catalog the planner reads,
/// so it carried exactly the defect code review found in the skip-index
/// derivation — a DDL that lost or reordered the sorting key would move
/// both sides together and the gate would pass on the worse table. Written
/// down here instead, where only a human edit can move it.
///
/// The `Condition:` strings are the planner's rendering of OUR predicate,
/// which is what this gate exists to pin, and they are byte-identical
/// across 24.8.14.39 and 26.3.17.110 (measured).
fn expected_stage3_prefix() -> Vec<String> {
    v(&[
        "MinMax",
        "Keys:",
        "timestamp_ns",
        "Condition: and((timestamp_ns in (-Inf, #]), (timestamp_ns in [#, +Inf)))",
        "Partition",
        "Condition: true",
        "PrimaryKey",
        "Keys:",
        "service",
        "fingerprint",
        "timestamp_ns",
        "Condition: and(and((timestamp_ns in (-Inf, #]), and((timestamp_ns in [#, +Inf)), \
         (fingerprint in #-element set))), (service in ['checkout', 'checkout']))",
    ])
}

#[tokio::test]
async fn stage3_contains_line_filter_uses_the_primary_key_and_the_token_skip_index() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_s3_contains");
    let ts_ns = now_ns();
    let client = setup_with_line_filter_corpus(db, ts_ns).await;

    assert_stage3_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout"} |= "connection refused""#,
        CombinedSkip::Absent,
        PrunesBy::Strongly,
    )
    .await;
}

#[tokio::test]
async fn stage3_not_contains_line_filter_uses_the_primary_key_and_the_token_skip_index() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_s3_not_contains");
    let ts_ns = now_ns();
    let client = setup_with_line_filter_corpus(db, ts_ns).await;

    // Issue #376 recorded this as the one stage-3 shape whose extract
    // MOVED on 26.3, because `!=` then rendered
    // `NOT (hasToken AND hasToken AND position)` — a negated conjunction,
    // the AND/OR mix 26.x reports a `<Combined skip indexes>` pseudo-block
    // for. Issue #450 deleted that conjunction: `!=` now renders
    // `NOT (body LIKE '%connection refused%')`, a single negated
    // predicate, so the block is gone and the expectation below is
    // `Absent`. The block did not become untested — the shape that mixes
    // AND and OR over `body` under the new rendering is the `or` group,
    // and
    // [`stage3_or_group_line_filter_carries_the_combined_skip_block`]
    // asserts `Present` for it. What did NOT move: a negated line filter
    // still prunes nothing (12/12 granules), which `PrunesBy::NotAtAll`
    // pins, and the declared skip-index SET is unchanged.
    assert_stage3_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout"} != "connection refused""#,
        CombinedSkip::Absent,
        PrunesBy::NotAtAll,
    )
    .await;
}

/// Issue #450: the `or` group is the stage-3 shape whose pushed-down
/// predicate genuinely mixes AND and OR over `body`
/// (`((body LIKE '%a%') OR (body LIKE '%b%'))` ANDed with the PK bounds),
/// so it is where 26.x's `<Combined skip indexes>` pseudo-block lives once
/// `!=` stopped rendering a negated conjunction. The `Present` variant
/// stays a live, falsifiable expectation rather than a dead one.
///
/// `"nomatchzzzz"` is absent from the corpus, so the disjunction still
/// selects only the granules carrying `"connection refused"` — the block
/// is a reporting addition here too, not a loss of pruning, which
/// `PrunesBy::Strongly` pins against the same-run `use_skip_indexes = 0`
/// control.
#[tokio::test]
async fn stage3_or_group_line_filter_carries_the_combined_skip_block() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_s3_or_group");
    let ts_ns = now_ns();
    let client = setup_with_line_filter_corpus(db, ts_ns).await;

    assert_stage3_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout"} |= "connection refused" or "nomatchzzzz""#,
        CombinedSkip::Present,
        PrunesBy::Strongly,
    )
    .await;
}

#[tokio::test]
async fn stage3_regex_line_filter_over_a_plain_literal_uses_the_body_skip_indexes() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_s3_regex");
    let ts_ns = now_ns();
    let client = setup_with_line_filter_corpus(db, ts_ns).await;

    assert_stage3_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout"} |~ "connection refused""#,
        CombinedSkip::Absent,
        PrunesBy::Strongly,
    )
    .await;
}

#[tokio::test]
async fn stage3_not_regex_line_filter_over_a_metacharacter_pattern_still_lists_the_body_skip_indexes()
 {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_s3_not_regex");
    let ts_ns = now_ns();
    let client = setup_with_line_filter_corpus(db, ts_ns).await;

    // `!~` renders `NOT (match(body, ...))` and nothing else — no
    // prefilter of any kind is minted (issue #450).
    // ClickHouse's `EXPLAIN indexes = 1` still lists
    // both `body` skip indexes as *considered* (any predicate referencing
    // `body` surfaces every skip index declared on that column) — the
    // `Parts:`/`Granules:` counts this file deliberately drops are what
    // would show whether either one actually pruned anything.
    assert_stage3_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout"} !~ "err.*""#,
        CombinedSkip::Absent,
        PrunesBy::NotAtAll,
    )
    .await;
}

/// Issue M6-09 AC4 (Tier-1, the named perf gate): a line filter followed
/// by parser/label-filter stages keeps the stage-3 `EXPLAIN indexes = 1`
/// extract EXACTLY equal to the plain line-filter expectation — the
/// `json`/`status` stages are pure post-fetch and add nothing to the SQL,
/// so the body skip indexes stay engaged for `|= "connection refused"`.
#[tokio::test]
async fn stage3_line_filter_before_a_parser_keeps_the_exact_skip_index_usage() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_s3_parser_pushdown");
    let ts_ns = now_ns();
    let client = setup_with_line_filter_corpus(db, ts_ns).await;

    assert_stage3_usage(
        db,
        ts_ns,
        &client,
        r#"{service_name="checkout"} |= "connection refused" | json | status = "500""#,
        CombinedSkip::Absent,
        PrunesBy::Strongly,
    )
    .await;
}

// ---------------------------------------------------------------------
// Issue #90 — the fetch-until-limit keyset PAGE (a later `After` page)
// must keep the primary index engaged in BOTH directions. The composite
// tuple comparison alone does not prune granules; the redundant
// `timestamp_ns` bound (`>= ts` Forward / `<= ts` Backward) is what keeps
// `PrimaryKey` on `timestamp_ns` in play — proving no per-page full scan.
// ---------------------------------------------------------------------

async fn keyset_page_usage(
    db: &str,
    ts_ns: i64,
    client: &ChClient,
    direction: Direction,
) -> Vec<String> {
    let table = format!("{db}.log_samples");
    let sql = sql::stage3_keyset(
        &table,
        &[literal("checkout")],
        &[FP_PROD],
        TimeWindow {
            start_ns: ts_ns - 6 * 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
        },
        sql::KeysetLower::After {
            tuple: (ts_ns, FP_PROD, 42),
            offset: 1,
        },
        direction,
        &[],
        500,
    );
    explain(client, &sql).await
}

#[tokio::test]
async fn keyset_forward_page_keeps_the_primary_key_engaged_via_the_redundant_time_bound() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_keyset_fwd");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = keyset_page_usage(db, ts_ns, &client, Direction::Forward).await;
    // The PrimaryKey block must be present with `timestamp_ns` among its
    // keys and a `Condition:` that references it (granule pruning), not a
    // `Condition: true` full scan.
    assert!(
        usage.iter().any(|l| l == "PrimaryKey"),
        "forward keyset page must engage the PrimaryKey: {usage:?}"
    );
    let pk_pos = usage.iter().position(|l| l == "PrimaryKey").unwrap();
    assert!(
        usage[pk_pos..].iter().any(|l| l == "timestamp_ns"),
        "timestamp_ns must be a PrimaryKey column: {usage:?}"
    );
    assert!(
        usage[pk_pos..]
            .iter()
            .any(|l| l.starts_with("Condition:") && l.contains("timestamp_ns")),
        "the PrimaryKey Condition must prune on timestamp_ns (redundant bound): {usage:?}"
    );
}

#[tokio::test]
async fn keyset_backward_page_keeps_the_primary_key_engaged_via_the_redundant_time_bound() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_keyset_bwd");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let usage = keyset_page_usage(db, ts_ns, &client, Direction::Backward).await;
    assert!(
        usage.iter().any(|l| l == "PrimaryKey"),
        "backward keyset page must engage the PrimaryKey: {usage:?}"
    );
    let pk_pos = usage.iter().position(|l| l == "PrimaryKey").unwrap();
    assert!(
        usage[pk_pos..].iter().any(|l| l == "timestamp_ns"),
        "timestamp_ns must be a PrimaryKey column: {usage:?}"
    );
    assert!(
        usage[pk_pos..]
            .iter()
            .any(|l| l.starts_with("Condition:") && l.contains("timestamp_ns")),
        "the PrimaryKey Condition must prune on timestamp_ns (redundant bound): {usage:?}"
    );
}

// ---------------------------------------------------------------------
// Metric reads — rollup-served vs raw fallback, range vs instant.
// ---------------------------------------------------------------------

/// Issue #227 Tier-1 gate: a RANGE metric read slides raw off `log_samples`
/// (the rollup fast-path is retired for range) and its PK-ordered sliding
/// scan (`metric_raw_samples_sliding`) engages the `(service, fingerprint,
/// timestamp_ns)` primary key — the same prune as every raw `log_samples`
/// read (the `ORDER BY` change to `optimize_read_in_order` shape does not
/// alter the index prune). Never a full scan (the query-performance mandate).
#[tokio::test]
async fn metric_range_slides_raw_and_prunes_on_the_service_fingerprint_timestamp_primary_key() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_metric_range_sliding");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let mp = metric_plan(r#"rate({env="prod"}[5m])"#, &range_params(ts_ns), db);
    assert!(
        !mp.rollup,
        "issue #227: a range query slides raw, never rollup"
    );
    assert!(mp.client.is_some());
    assert_eq!(mp.table, "log_samples");
    let table = format!("{db}.log_samples");
    let sql = sql::metric_raw_samples_sliding(
        &table,
        &[literal("checkout")],
        &[FP_PROD],
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
        projection_of(&mp),
    );
    assert!(
        sql.contains("ORDER BY service ASC, fingerprint ASC, timestamp_ns ASC"),
        "PK read order (optimize_read_in_order), no body/global-ts sort: {sql}"
    );
    let usage = explain(&client, &sql).await;
    assert_eq!(usage, expected_metric_instant_raw_usage());
}

/// Issue #169 Tier-1 gate: the `/volume` rollup aggregation carries the
/// identical `(fingerprint IN, bucket_ns > s AND <= e)` predicate family
/// as the rollup metric reads, so its `EXPLAIN indexes = 1` extract must
/// equal [`expected_metric_rollup_usage`] in full — MinMax prune on
/// `bucket_ns` plus the `(fingerprint, bucket_ns)` primary key with both
/// predicates in its `Condition:` — and reference no `service`/body
/// column anywhere (primary-key pruning, never a full scan; the
/// query-performance mandate).
#[tokio::test]
async fn volume_rollup_read_uses_the_fingerprint_bucket_primary_key() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_volume_rollup");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let table = format!("{db}.log_metrics_5s");
    let sql = sql::log_volume_rollup(
        &table,
        &[FP_PROD],
        TimeWindow {
            start_ns: ts_ns - 6 * 3_600_000_000_000,
            end_ns: ts_ns + 3_600_000_000_000,
        },
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(usage, expected_metric_rollup_usage());
    assert!(!usage.iter().any(|l| l.contains("service")));
}

/// Issue #170 Tier-1 gate: the `/detected_labels` aggregation is ONE
/// `log_streams_idx` scan with the month partition pruned (MinMax +
/// Partition on `month` — the same scan class as the shipped `/labels`
/// discovery query) and never references `log_samples`/body anywhere.
/// The `(key, val, fingerprint)` primary key legitimately reports
/// `Condition: true` — the aggregation groups over every key, so the
/// pruning story is the partition, not the PK prefix.
///
/// `/detected_fields` deliberately has NO case here: it adds no new SQL
/// shape — its fast path is the byte-identical `sql::stage3` builder and
/// its paged path is `sql::stage3_keyset`, both already full-extract-
/// gated above (`stage3_*`/`keyset_*` cases); `sql_snapshots.rs` pins the
/// text and `logs_detected_live.rs` adds the endpoint-level pushdown
/// evidence.
#[tokio::test]
async fn detected_labels_aggregation_prunes_on_the_month_partition() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_detected_labels");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let params = range_params(ts_ns);
    let (start_ns, end_ns) = match params.spec {
        QuerySpec::Range {
            start_ns, end_ns, ..
        } => (start_ns, end_ns),
        QuerySpec::Instant { .. } => unreachable!("range_params builds a Range spec"),
    };
    let months = pulsus_read::logql::plan::months_overlapping(start_ns, end_ns);
    let table = format!("{db}.log_streams_idx");
    let sql = sql::detected_labels(
        &table,
        &months,
        None,
        &format!("{db}.log_metrics_5s"),
        TimeWindow { start_ns, end_ns },
        ROLLUP_RES_NS,
    );
    assert!(!sql.contains("log_samples"), "never touches log_samples");

    let usage = explain(&client, &sql).await;
    assert_eq!(
        usage,
        // The outer `log_streams_idx` scan is unchanged by issue #399 —
        // same `month` MinMax/Partition pruning — and additionally
        // engages the index primary key's TRAILING column, replacing the
        // pre-#399 `PrimaryKey / Condition: true`: the activity semi-join
        // materializes a `fingerprint` set before the outer scan reads.
        //
        // `EXPLAIN indexes = 1` reports index analysis for the OUTER
        // table only — the subquery is executed to build the set, not
        // planned inline — so the rollup's own `bucket_ns` blocks are
        // absent here (measured, ClickHouse 24.8.14.39) and are pinned
        // separately, on the subquery's standalone form, by
        // `detected_labels_activity_subquery_prunes_the_rollup_by_bucket_range`.
        v(&[
            "MinMax",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "Partition",
            "Keys:",
            "month",
            "Condition: (month in [#, #])",
            "PrimaryKey",
            "Keys:",
            "fingerprint",
            "Condition: (fingerprint in #-element set)",
        ])
    );
}

/// Issue #399 AC5 — the activity semi-join's own scan prunes the rollup
/// on `bucket_ns`, which is the partition key's only input column
/// (`PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))`), so a
/// window narrower than the corpus reads strictly fewer parts. The
/// `Parts: m/n` check is the one that carries the claim: `index_usage`
/// deliberately drops those counts, so a `Condition:` that pruned nothing
/// would still match the extract.
#[tokio::test]
async fn detected_labels_activity_subquery_prunes_the_rollup_by_bucket_range() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_activity_subquery");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    // Four distinct DAY partitions of rollup rows, one bucket each. The
    // window below covers only the newest, so at least three parts must
    // be pruned away.
    const DAY_NS: i64 = 86_400_000_000_000;
    for day in 1..=4i64 {
        let bucket = (ts_ns - day * DAY_NS) / ROLLUP_RES_NS as i64 * ROLLUP_RES_NS as i64;
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_metrics_5s (fingerprint, bucket_ns, count, bytes) \
                     VALUES ({FP_PROD}, {bucket}, 1, 10)"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed rollup day partition");
    }

    let table = format!("{db}.log_metrics_5s");
    let window = TimeWindow {
        start_ns: ts_ns - DAY_NS - 3_600_000_000_000,
        end_ns: ts_ns - DAY_NS + 3_600_000_000_000,
    };

    let unscoped = sql::active_fingerprints(&table, None, window, ROLLUP_RES_NS);
    let raw = explain_raw(&client, &unscoped).await;
    assert_eq!(
        index_usage(&raw),
        v(&[
            "MinMax",
            "Keys:",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
        ])
    );
    let (selected, total) = parts_selected(&raw).expect("a MinMax Parts: m/n line");
    assert!(
        total >= 4,
        "the fixture must offer at least four day partitions to prune from, saw {total}\n{raw}"
    );
    assert!(
        selected < total,
        "the bucket range must prune parts: {selected}/{total}\n{raw}"
    );

    // Scoped: the caller's fingerprint list is pushed inside, so the
    // primary key's LEADING column joins the condition too.
    let scoped = sql::active_fingerprints(&table, Some(&[FP_PROD]), window, ROLLUP_RES_NS);
    assert_eq!(
        explain(&client, &scoped).await,
        v(&[
            "MinMax",
            "Keys:",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "fingerprint",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), and((bucket_ns in [#, +Inf)), (fingerprint in #-element set)))",
        ])
    );
}

/// Issue #406 Part A — the two statements `/series` with no `match[]`
/// renders. The branch introduces NO new SQL shape: statement one is
/// `sql::active_fingerprints(rollup, None, …)`, already production SQL for
/// `/labels`, `/label/{name}/values` and `/detected_labels`, and statement
/// two is the byte-pinned `sql::stage2`. What this case adds over the
/// pieces' existing coverage is the PAIRING — the unmatched path engages
/// the rollup's `bucket_ns` MinMax/PrimaryKey and then `log_streams`'
/// `fingerprint` primary key, with no `log_streams_idx` stage 1 between
/// them and no `log_samples` read at all.
///
/// The `Parts: m/n` check on the first statement is what carries the
/// pruning claim: `index_usage` drops those counts, so a `Condition:` that
/// pruned nothing would still match the extract.
#[tokio::test]
async fn series_without_a_selector_prunes_the_rollup_and_hits_the_streams_primary_key() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_series_all");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    // Four distinct DAY partitions of rollup rows; the window covers one.
    const DAY_NS: i64 = 86_400_000_000_000;
    for day in 1..=4i64 {
        let bucket = (ts_ns - day * DAY_NS) / ROLLUP_RES_NS as i64 * ROLLUP_RES_NS as i64;
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_metrics_5s (fingerprint, bucket_ns, count, bytes) \
                     VALUES ({FP_PROD}, {bucket}, 1, 10)"
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed rollup day partition");
    }

    let rollup = format!("{db}.log_metrics_5s");
    let window = TimeWindow {
        start_ns: ts_ns - DAY_NS - 3_600_000_000_000,
        end_ns: ts_ns - DAY_NS + 3_600_000_000_000,
    };

    // Statement 1 — the UNSCOPED activity scan (`fingerprints: None`),
    // exactly what `all_active_fingerprints` dispatches.
    let activity = sql::active_fingerprints(&rollup, None, window, ROLLUP_RES_NS);
    let raw = explain_raw(&client, &activity).await;
    assert_eq!(
        index_usage(&raw),
        v(&[
            "MinMax",
            "Keys:",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "bucket_ns",
            "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
        ])
    );
    let (selected, total) = parts_selected(&raw).expect("a MinMax Parts: m/n line");
    assert!(
        total >= 4,
        "the fixture must offer at least four day partitions to prune from, saw {total}\n{raw}"
    );
    assert!(
        selected < total,
        "the unmatched /series activity scan must prune parts: {selected}/{total}\n{raw}"
    );

    // Statement 2 — hydration over whatever statement 1 returned, on
    // `log_streams`' `ORDER BY fingerprint` primary key.
    let streams = format!("{db}.log_streams");
    assert_eq!(
        explain(&client, &sql::stage2(&streams, &[FP_PROD])).await,
        v(&[
            "MinMax",
            "Condition: true",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "fingerprint",
            "Condition: (fingerprint in #-element set)",
        ])
    );
}

/// Issue #399 AC15 — `/labels` and `/label/{name}/values` keep their
/// month partition pruning and gain the same activity semi-join.
///
/// Issue #482 AC 3 extends it with the SCOPED forms: `query=`'s
/// fingerprints are pushed inside the activity subquery, so the outer
/// scan's `index_usage` extract must be IDENTICAL to the unscoped
/// form's, asserted as an equality against the unscoped extract rather
/// than against a second copy of the expected list.
#[tokio::test]
async fn label_discovery_scans_prune_on_the_month_partition_and_the_activity_bucket_range() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_label_discovery_window");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let params = range_params(ts_ns);
    let (start_ns, end_ns) = match params.spec {
        QuerySpec::Range {
            start_ns, end_ns, ..
        } => (start_ns, end_ns),
        QuerySpec::Instant { .. } => unreachable!("range_params builds a Range spec"),
    };
    let months = pulsus_read::logql::plan::months_overlapping(start_ns, end_ns);
    let idx = format!("{db}.log_streams_idx");
    let rollup = format!("{db}.log_metrics_5s");
    let window = TimeWindow { start_ns, end_ns };

    // `EXPLAIN indexes = 1` reports the OUTER table's analysis only (see
    // `detected_labels_aggregation_prunes_on_the_month_partition`), so
    // both extracts are month blocks plus that builder's own primary-key
    // condition. The subquery's rollup pruning is pinned on its
    // standalone form by
    // `detected_labels_activity_subquery_prunes_the_rollup_by_bucket_range`.
    let month_blocks = [
        "MinMax",
        "Keys:",
        "month",
        "Condition: (month in [#, #])",
        "Partition",
        "Keys:",
        "month",
        "Condition: (month in [#, #])",
    ];

    let names_sql = sql::label_names(&idx, &months, None, &rollup, window, ROLLUP_RES_NS);
    let mut expected: Vec<&str> = month_blocks.to_vec();
    expected.extend([
        "PrimaryKey",
        "Keys:",
        "fingerprint",
        "Condition: (fingerprint in #-element set)",
    ]);
    let names_usage = explain(&client, &names_sql).await;
    assert_eq!(names_usage, v(&expected));

    // Issue #482 AC 3 — the SCOPED form. `query=`'s stage-1 fingerprints
    // go INSIDE the activity subquery, so the OUTER scan's index usage
    // must be identical to the unscoped form's: same month MinMax and
    // Partition blocks, same primary-key condition. A fingerprint list
    // added as a second outer conjunct instead would change this extract.
    let scoped_names_sql = sql::label_names(
        &idx,
        &months,
        Some(&[7, 11]),
        &rollup,
        window,
        ROLLUP_RES_NS,
    );
    assert_eq!(
        explain(&client, &scoped_names_sql).await,
        names_usage,
        "the scoped /labels outer scan must analyse exactly as the unscoped one"
    );

    let values_sql = sql::label_values(
        &idx,
        &months,
        &literal("env"),
        None,
        &rollup,
        window,
        ROLLUP_RES_NS,
    );
    let mut expected: Vec<&str> = month_blocks.to_vec();
    expected.extend([
        "PrimaryKey",
        "Keys:",
        "key",
        "fingerprint",
        // Operand order is ClickHouse's, MEASURED (24.8.14.39), not the
        // order the predicate is written in: `label_values` renders `key
        // = 'env'` before the `fingerprint IN`, and the analyser reports
        // the reverse. The issue #399 plan predicted the written order;
        // the plan was not wrong about WHICH conditions engage the
        // primary key, only about the order the extract prints them in.
        "Condition: and((fingerprint in #-element set), (key in ['env', 'env']))",
    ]);
    let values_usage = explain(&client, &values_sql).await;
    assert_eq!(values_usage, v(&expected));

    let scoped_values_sql = sql::label_values(
        &idx,
        &months,
        &literal("env"),
        Some(&[7, 11]),
        &rollup,
        window,
        ROLLUP_RES_NS,
    );
    assert_eq!(
        explain(&client, &scoped_values_sql).await,
        values_usage,
        "the scoped /label/{{name}}/values outer scan must analyse exactly as the unscoped one"
    );

    // What embedding the semi-join must NOT cost: the outer scan's month
    // partition pruning. `index_usage` drops `Parts:` counts, so assert
    // it on the raw text.
    let raw = explain_raw(&client, &names_sql).await;
    let (selected, total) = parts_selected(&raw).expect("a MinMax Parts: m/n line");
    assert!(
        selected <= total,
        "sanity: parts selected must not exceed parts total ({selected}/{total})\n{raw}"
    );
}

/// The `(fingerprint, bucket_ns)` primary key on `log_metrics_5s` — shared
/// by the range and instant rollup cases, which differ only in `SELECT`/
/// `GROUP BY` shape (`sql_snapshots.rs`'s job), not in the `WHERE`
/// predicates over `fingerprint`/`bucket_ns` that drive index usage.
fn expected_metric_rollup_usage() -> Vec<String> {
    v(&[
        "MinMax",
        "Keys:",
        "bucket_ns",
        "Condition: and((bucket_ns in (-Inf, #]), (bucket_ns in [#, +Inf)))",
        "Partition",
        "Condition: true",
        "PrimaryKey",
        "Keys:",
        "fingerprint",
        "bucket_ns",
        "Condition: and((bucket_ns in (-Inf, #]), and((bucket_ns in [#, +Inf)), (fingerprint in #-element set)))",
    ])
}

/// Renamed from `metric_rollup_instant_read_uses_the_fingerprint_bucket_primary_key`
/// (issue #12 behaviour change from #11): an instant metric query has no
/// step to test against the rollup resolution (an unaligned `[at-range,
/// at]` window would silently diverge from raw at bucket edges —
/// task-manager resolution #1 on issue #12), so it now always routes raw.
#[tokio::test]
async fn metric_instant_read_routes_to_raw_and_uses_the_service_fingerprint_timestamp_primary_key()
{
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_metric_instant_raw");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let params = QueryParams {
        spec: QuerySpec::Instant { at_ns: ts_ns },
        limit: 100,
        direction: Direction::Backward,
    };
    let mp = metric_plan(r#"rate({env="prod"}[5m])"#, &params, db);
    assert!(!mp.rollup);
    assert_eq!(
        mp.routing.reason, "raw: instant query",
        "instant queries must name the routing reason"
    );
    assert!(mp.step_ns.is_none());
    let table = format!("{db}.log_samples");
    let sql = sql::metric_instant(
        sql::MetricSource::new(
            &table,
            mp.source_shape()
                .expect("plan::metric_plan writes both columns out of MetricShape"),
        ),
        &[literal("checkout")],
        &[FP_PROD],
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
        // Issue #249: an instant plan is always raw over `log_samples`, so
        // the pushdown aggregate reads AND groups by `structured_metadata`.
        // AC-8: the wider SELECT and the extra GROUP BY key must leave
        // `index_usage` byte-equal to the pre-#249 expectation, because that
        // is a function of the WHERE/PREWHERE predicates alone.
        ScanProjection::WithStructuredMetadata,
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(usage, expected_metric_instant_raw_usage());
}

/// The [`ScanProjection`] `LogQlEngine` would pass for this plan — derived
/// from `mp.op` exactly as `client_metric_read_sql` derives it (issue #249).
/// A wider `SELECT` list cannot move `index_usage`, which is a function of
/// the `WHERE`/`PREWHERE` predicates alone; asserting it here is what turns
/// that from an argument into a check.
fn projection_of(mp: &pulsus_read::logql::MetricPlan) -> ScanProjection {
    if matches!(mp.op, pulsus_logql::RangeAggOp::AbsentOverTime) {
        ScanProjection::Lean
    } else {
        ScanProjection::WithStructuredMetadata
    }
}

/// The `(service, fingerprint, timestamp_ns)` primary key on `log_samples`
/// — the same key condition [`expected_stage3_prefix`] asserts
/// (a `body` predicate never factors into `PrimaryKey`'s `Condition:`, only
/// into whether the `Skip` blocks are listed at all), minus the two `Skip`
/// entries: an instant metric read carries no line filter, so it never
/// references `body` and neither skip index is ever considered.
fn expected_metric_instant_raw_usage() -> Vec<String> {
    v(&[
        "MinMax",
        "Keys:",
        "timestamp_ns",
        "Condition: and((timestamp_ns in (-Inf, #]), (timestamp_ns in [#, +Inf)))",
        "Partition",
        "Condition: true",
        "PrimaryKey",
        "Keys:",
        "service",
        "fingerprint",
        "timestamp_ns",
        "Condition: and(and((timestamp_ns in (-Inf, #]), and((timestamp_ns in [#, +Inf)), (fingerprint in #-element set))), (service in ['checkout', 'checkout']))",
    ])
}

// ---------------------------------------------------------------------
// PromQL metric reads (issue #83, M6-08a) — the @-fixed and the
// subquery-widened fetch windows must keep the `(metric_name,
// fingerprint, unix_milli)` primary index on `metric_samples`: both plan
// to exactly one bounded `sample_fetch` whose `EXPLAIN indexes = 1`
// extract matches the plain raw-fetch expectation (no index loss from
// the fixed/widened bounds).
// ---------------------------------------------------------------------

const MFP: u64 = 18_374_000_000_000_000_002;

async fn seed_metric_samples(client: &ChClient, db: &str, now_ms: i64) {
    // A few samples in the last minute — enough for genuine index
    // analysis (recent so `ttl_only_drop_parts` retention can't race it,
    // the same rule as `now_ns()`'s doc).
    let values: Vec<String> = (0..6)
        .map(|k| format!("('mq', {MFP}, {}, {k}.0)", now_ms - k * 10_000))
        .collect();
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_samples (metric_name, fingerprint, unix_milli, value) \
                 VALUES {}",
                values.join(", ")
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed metric_samples");
}

/// Plans `query` (single-selector by construction), computes its fetch
/// window, and renders the real `sample_fetch` SQL against
/// `{db}.metric_samples` — the same builder `MetricsEngine` executes.
fn promql_sample_fetch_sql(query: &str, params: pulsus_promql::PlanParams, db: &str) -> String {
    let expr = pulsus_promql::parse(query).expect("parse");
    let plan = pulsus_promql::plan(&expr, params).expect("plan");
    assert_eq!(
        plan.selectors.len(),
        1,
        "{query}: one bounded sample fetch, never per-inner-step fetches"
    );
    let (lower_excl, upper_incl) = plan.selectors[0].fetch_window(&params);
    let table = format!("{db}.metric_samples");
    pulsus_read::metrics::sample_sql::sample_fetch(
        &table,
        plan.selectors[0]
            .metric_name
            .as_deref()
            .expect("these cases use concrete-name selectors"),
        &[MFP],
        lower_excl,
        upper_incl,
    )
}

/// The `(metric_name, fingerprint, unix_milli)` primary key on
/// `metric_samples` — the shared raw-fetch expectation both PromQL cases
/// below assert against: the full three-column key condition plus MinMax
/// time pruning (the `toDate(...)` partition analysis reports
/// `Condition: true` here — time-range partition pruning surfaces through
/// the MinMax block instead).
fn expected_metric_samples_fetch_usage() -> Vec<String> {
    v(&[
        "MinMax",
        "Keys:",
        "unix_milli",
        "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
        "Partition",
        "Condition: true",
        "PrimaryKey",
        "Keys:",
        "metric_name",
        "fingerprint",
        "unix_milli",
        "Condition: and(and((fingerprint in #-element set), and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))), (metric_name in ['mq', 'mq']))",
    ])
}

fn promql_params(start_ms: i64, end_ms: i64, step_ms: i64) -> pulsus_promql::PlanParams {
    pulsus_promql::PlanParams {
        start_ms,
        end_ms,
        step_ms,
        lookback_ms: pulsus_promql::DEFAULT_LOOKBACK_MS,
        experimental_functions: false,
    }
}

#[tokio::test]
async fn promql_at_fixed_metric_read_stays_on_the_metric_samples_primary_key() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_promql_at");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_samples(&client, db, now_ms).await;

    // `@` fixed at (roughly) now; the fetch window is invariant across
    // eval spans (the hermetic plan.rs gate) — asserted here against two
    // spans before the live EXPLAIN, tying AC4 to AC3.
    let at_s = now_ms / 1000;
    let query = format!("mq @ {at_s}");
    let span_a = promql_params(now_ms, now_ms, 0);
    let span_b = promql_params(now_ms - 86_400_000, now_ms, 60_000);
    let sql_a = promql_sample_fetch_sql(&query, span_a, db);
    let sql_b = promql_sample_fetch_sql(&query, span_b, db);
    assert_eq!(
        sql_a, sql_b,
        "@-fixed fetch SQL must not track the eval span"
    );

    let usage = explain(&client, &sql_a).await;
    assert_eq!(usage, expected_metric_samples_fetch_usage());
}

#[tokio::test]
async fn promql_subquery_widened_metric_read_stays_on_the_metric_samples_primary_key() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_promql_subq");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_samples(&client, db, now_ms).await;

    // One widened window for the whole inner grid — exactly one fetch,
    // lower bound widened by exactly the subquery range vs the bare
    // selector's.
    let params = promql_params(now_ms, now_ms, 0);
    let subq_sql = promql_sample_fetch_sql("max_over_time(mq[1h:5m])", params, db);
    let bare_expr = pulsus_promql::parse("mq").expect("parse");
    let bare_plan = pulsus_promql::plan(&bare_expr, params).expect("plan");
    let (bare_lower, bare_upper) = bare_plan.selectors[0].fetch_window(&params);
    assert!(subq_sql.contains(&format!("unix_milli > {}", bare_lower - 3_600_000)));
    assert!(subq_sql.contains(&format!("unix_milli <= {bare_upper}")));

    let usage = explain(&client, &subq_sql).await;
    assert_eq!(usage, expected_metric_samples_fetch_usage());
}

/// Issue #85 (M6-08c) — the name-less/regex-`__name__` fan-out gate: the
/// flat `sample_fetch_multi` SQL (one query, `PREWHERE metric_name IN
/// (…)` + `fingerprint IN (…)`) must engage BOTH components of the
/// `(metric_name, fingerprint, unix_milli)` primary key in the live
/// ClickHouse plan (round-4 adjudication item 2) — the `IN`-set prune is
/// what makes the fan-out bounded instead of a name-less full scan.
/// Concrete-name selectors' plan stays byte-identical to the existing
/// single-eq expectation (no regression from adding the multi shape).
#[tokio::test]
async fn promql_multi_metric_fanout_prunes_on_both_metric_name_and_fingerprint_keys() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_promql_multi");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    // Series across TWO metric names (`mq` via the shared seeder, `mq2`
    // here) — the fan-out shape is only meaningful over >= 2 metrics.
    seed_metric_samples(&client, db, now_ms).await;
    const MFP2: u64 = 18_374_000_000_000_000_003;
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_samples (metric_name, fingerprint, unix_milli, value) \
                 VALUES ('mq2', {MFP2}, {now_ms}, 1.0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed mq2 samples");

    // The real fetch SQL a name-less selector's resolved (name, fp) set
    // produces — the same builder `MetricsEngine::plan_multi_metric_fetch`
    // renders.
    let params = promql_params(now_ms, now_ms, 0);
    let expr = pulsus_promql::parse(r#"{__name__=~"mq.*"}"#).expect("parse");
    let plan = pulsus_promql::plan(&expr, params).expect("plan");
    assert_eq!(plan.selectors[0].metric_name, None, "name-less selector");
    let (lower_excl, upper_incl) = plan.selectors[0].fetch_window(&params);
    let table = format!("{db}.metric_samples");
    let sql = pulsus_read::metrics::sample_sql::sample_fetch_multi(
        &table,
        &["mq".to_string(), "mq2".to_string()],
        &[MFP, MFP2],
        lower_excl,
        upper_incl,
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "fingerprint",
            "unix_milli",
            "Condition: and(and((fingerprint in #-element set), and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))), (metric_name in #-element set))",
        ]),
        "both the metric_name IN and fingerprint IN components must engage the primary key"
    );

    // Control: a concrete-name selector's plan is unchanged by the multi
    // shape's existence — the exact pre-#85 extract.
    let single_sql = promql_sample_fetch_sql("mq", params, db);
    let single_usage = explain(&client, &single_sql).await;
    assert_eq!(single_usage, expected_metric_samples_fetch_usage());
}

/// Issue #82 (retroactive re-review, Finding 1) — the Tier-1 "bounded
/// info() fetch" gate: (a) `info(mq)`'s synthetic `target_info` selector
/// PK-prunes on `metric_name` in the live plan exactly like any other
/// concrete-name `metric_samples` fetch (no new/looser SQL shape — see
/// `expected_metric_samples_fetch_usage`, same shape, `target_info`
/// literal); (b) the degraded-path series-RESOLUTION probe
/// (`info_series_cardinality_probe`, `metrics/sql.rs`) both carries a
/// `LIMIT cap+1` in its rendered SQL text AND still PK-prunes on
/// `metric_series`'s leading `metric_name` component in the live plan —
/// the resolution stage is bounded BEFORE the sample fetch, not a
/// looser scan.
#[tokio::test]
async fn info_selector_fetch_prunes_on_metric_name_and_its_resolution_probe_is_limit_bounded() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_promql_info");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_samples(&client, db, now_ms).await;

    const INFO_FP: u64 = 18_374_000_000_000_000_006;
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_samples (metric_name, fingerprint, unix_milli, value) \
                 VALUES ('target_info', {INFO_FP}, {now_ms}, 1.0)"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed target_info sample");
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_series (metric_name, fingerprint, unix_milli, labels) \
                 VALUES ('target_info', {INFO_FP}, {now_ms}, '{{\"instance\":\"a\",\"job\":\"1\"}}')"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed target_info series");

    // (a) The planned info() selector: `metric_name = Some("target_info")`
    // (AC2's PK-pruned single-metric fast path), `info_family = true`.
    let params = pulsus_promql::PlanParams {
        experimental_functions: true,
        ..promql_params(now_ms, now_ms, 0)
    };
    let expr = pulsus_promql::parse("info(mq)").expect("parse");
    let plan = pulsus_promql::plan(&expr, params).expect("plan");
    assert_eq!(plan.selectors.len(), 2);
    let info_sel = &plan.selectors[1];
    assert_eq!(info_sel.metric_name.as_deref(), Some("target_info"));
    assert!(
        info_sel.info_family,
        "the synthetic selector must be marked info_family"
    );

    let (lower_excl, upper_incl) = info_sel.fetch_window(&params);
    let samples_table = format!("{db}.metric_samples");
    let fetch_sql = pulsus_read::metrics::sample_sql::sample_fetch(
        &samples_table,
        "target_info",
        &[INFO_FP],
        lower_excl,
        upper_incl,
    );
    let usage = explain(&client, &fetch_sql).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "fingerprint",
            "unix_milli",
            "Condition: and(and((fingerprint in #-element set), and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))), (metric_name in ['target_info', 'target_info']))",
        ]),
        "the info() sample fetch must PK-prune on metric_name exactly like any concrete-name fetch"
    );

    // (b) The degraded-path resolution probe: a `LIMIT cap+1` bound in
    // the rendered SQL text (the cardinality cap, checked BEFORE the
    // sample fetch above ever runs), applied over a `SELECT DISTINCT
    // fingerprint` (the #82 code-review over-count fix — the cap counts
    // distinct SERIES, never per-activity-bucket `metric_series` rows),
    // and the probe query itself still PK-prunes on `metric_series`'s
    // leading `metric_name` component.
    let series_table = format!("{db}.metric_series");
    let window = pulsus_read::metrics::DataWindow {
        start_ms: now_ms - 3_600_000,
        end_ms: now_ms,
    };
    let series_sql = pulsus_read::metrics::sql::historical_series_subquery(
        &series_table,
        "target_info",
        window,
        1,
        &[],
    );
    let cap = 100_000u64;
    let probe_sql = pulsus_read::metrics::sql::info_series_cardinality_probe(&series_sql, cap);
    assert!(
        probe_sql.starts_with("SELECT DISTINCT fingerprint"),
        "the probe must count DISTINCT series, not activity-bucket rows: {probe_sql}"
    );
    assert!(
        probe_sql.ends_with("LIMIT 100001"),
        "the probe must bound resolution at cap+1: {probe_sql}"
    );

    let probe_usage = explain(&client, &probe_sql).await;
    assert_eq!(
        probe_usage,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), and((unix_milli in [#, +Inf)), (metric_name in ['target_info', 'target_info'])))",
        ]),
        "the LIMIT-bounded resolution probe must still PK-prune on metric_name"
    );
}

// A pair of `metric_series` rows across TWO metric names sharing one
// activity bucket — the discovery-side fan-out shape is only meaningful
// over >= 2 metric names, and the flat `IN`×`IN` prune needs genuine data
// in the queried partition/time-range so the optimizer keeps a real
// `ReadFromMergeTree` (not a short-circuited `NullSource`).
const SFP1: u64 = 18_374_000_000_000_000_004;
const SFP2: u64 = 18_374_000_000_000_000_005;

async fn seed_metric_series(client: &ChClient, db: &str, now_ms: i64) {
    client
        .execute(
            &format!(
                "INSERT INTO {db}.metric_series (metric_name, fingerprint, unix_milli, labels) \
                 VALUES ('sv', {SFP1}, {now_ms}, '{{\"job\":\"api\"}}'), \
                        ('sv2', {SFP2}, {now_ms}, '{{\"job\":\"api\"}}')"
            ),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("seed metric_series");
}

/// Issue #89 (discovery-path selector parity) — the regex/negated-
/// `__name__` discovery fan-out gate: the flat `discovery_fetch_multi` SQL
/// (one query, `metric_name IN (…)` + `fingerprint IN (…)`) must engage
/// BOTH components of the `(metric_name, fingerprint, unix_milli)` primary
/// key on `metric_series` in the live ClickHouse plan — the same Tier-1
/// evidence class as #85's `sample_fetch_multi` gate on `metric_samples`,
/// carried onto the discovery table the `/series`+`/labels` name-matcher
/// selector resolves against. The `IN`-set prune is what keeps the
/// cache-resolved fan-out bounded instead of a name-less full scan.
#[tokio::test]
async fn discovery_multi_metric_fanout_prunes_on_both_metric_name_and_fingerprint_keys() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_discovery_multi");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_series(&client, db, now_ms).await;

    // The real fetch SQL a name-matcher discovery filter's resolved
    // (name, fp) set produces — the same builder
    // `MetricsEngine::discovery_multi_sql` renders. `bucket_ms = 1` floors
    // to the exact bounds (the flooring itself is unit-tested in `sql.rs`),
    // so the seeded now-stamped rows stay inside the queried window and the
    // primary-key analysis runs against a populated part.
    let window = pulsus_read::metrics::DataWindow {
        start_ms: now_ms - 3_600_000,
        end_ms: now_ms,
    };
    let table = format!("{db}.metric_series");
    let sql = pulsus_read::metrics::sql::discovery_fetch_multi(
        &table,
        &["sv".to_string(), "sv2".to_string()],
        &[SFP1, SFP2],
        window,
        1,
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "fingerprint",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), and((unix_milli in [#, +Inf)), and((fingerprint in #-element set), (metric_name in #-element set))))",
        ]),
        "both the metric_name IN and fingerprint IN components must engage the metric_series primary key"
    );
}

/// Raw `EXPLAIN PIPELINE` text — the transform chain, which
/// [`explain_raw`]'s `indexes = 1` output does not carry. Used by issue
/// #472's sorted-key DISTINCT gate below; `?`-doubling for the same reason
/// [`explain_raw`] does it.
async fn explain_pipeline_raw(client: &ChClient, sql: &str) -> String {
    let full = format!("EXPLAIN PIPELINE {sql}").replace('?', "??");
    let mut out = String::new();
    let mut stream = client
        .query_stream::<ExplainRow>(&full, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("explain pipeline failed: {e}\nSQL:\n{full}"));
    while let Some(row) = stream.next().await {
        out.push_str(&row.expect("decode explain row").explain);
        out.push('\n');
    }
    out
}

/// Lines of `raw` equal (after trimming) to `transform`.
fn transform_lines(raw: &str, transform: &str) -> usize {
    raw.lines().filter(|l| l.trim() == transform).count()
}

/// The unfiltered discovery filter both #472 gates render from — the
/// datasource's actual first `/api/v1/label/__name__/values` call, which
/// carries no `match[]` at all.
fn unfiltered_discovery_filter() -> pulsus_read::metrics::DiscoveryFilter {
    pulsus_read::metrics::DiscoveryFilter::default()
}

/// Issue #472 — **no index regression** from the narrow name projection.
///
/// `/api/v1/label/__name__/values` now renders `SELECT DISTINCT
/// metric_name` where it rendered `SELECT fingerprint, metric_name, labels
/// … LIMIT 1 BY metric_name, fingerprint`. This gate is deliberately NOT a
/// claim that the narrow form prunes better: measured on 26.3.17.110 the
/// two forms' `Indexes:` blocks are character-identical, both engaging
/// `unix_milli` through generic exclusion search, and the win is projection
/// plus a sorted-key DISTINCT (the next test), not index selection.
///
/// Asserted twice, because the halves catch different regressions: the
/// pinned literal catches a regression the two forms would **share** (a
/// dropped window bound turns `Condition` into `true`), and the equality
/// states the non-regression claim itself — the new statement engages
/// exactly what the old one did.
#[tokio::test]
async fn discovery_distinct_names_engages_the_same_indexes_as_the_wide_discovery_query() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_discovery_names_idx");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_series(&client, db, now_ms).await;

    let window = pulsus_read::metrics::DataWindow {
        start_ms: now_ms - 3_600_000,
        end_ms: now_ms,
    };
    let table = format!("{db}.metric_series");
    let filter = unfiltered_discovery_filter();
    // `bucket_ms = 1` floors to the exact bounds, so the seeded now-stamped
    // rows stay inside the queried window and the analysis runs against a
    // populated part.
    let narrow_sql =
        pulsus_read::metrics::sql::discovery_distinct_names_query(&table, &filter, window, 1);
    let wide_sql = pulsus_read::metrics::sql::discovery_query(&table, &filter, window, 1);

    let narrow = explain(&client, &narrow_sql).await;
    assert_eq!(
        narrow,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
        ]),
        "the narrow name projection must still carry both window bounds into the \
         MinMax/Partition/PrimaryKey analysis"
    );
    let wide = explain(&client, &wide_sql).await;
    assert_eq!(
        narrow, wide,
        "issue #472 must not trade the wide discovery read for a differently-indexed one: \
         the narrow statement's index usage must equal the statement it replaces"
    );
}

/// Issue #472 — the **sorted-key DISTINCT**, with a live control.
///
/// `metric_series ORDER BY (metric_name, fingerprint, unix_milli)` makes
/// `metric_name` the leading key, so `SELECT DISTINCT metric_name … ORDER
/// BY metric_name` is planned as `DistinctSortedStreamTransform` at both
/// the preliminary and the final stage — a streaming de-duplication over
/// already-sorted input, with no hash set built over the corpus. That is
/// the transform this issue's win rests on, and it is the reason the
/// builder's `ORDER BY metric_name` is not cosmetic.
///
/// **The absence half needs a witness or it passes vacuously.** "No
/// `DistinctTransform`" is only meaningful if this same server, in this
/// same test, on this same corpus, does emit one for a query that should
/// have one — otherwise a ClickHouse that renamed the transform would make
/// the gate pass while measuring nothing. `SELECT DISTINCT fingerprint`
/// (the same table, the same window bound, a **non-leading** key column) is
/// that control.
///
/// Measured on 26.3.17.110: the narrow statement gives two
/// `DistinctSortedStreamTransform` and no `DistinctTransform`; dropping the
/// `ORDER BY` turns the FINAL distinct into `DistinctTransform`, and moving
/// the projection off the leading key (`DISTINCT fingerprint`, `DISTINCT
/// labels`) turns the PRELIMINARY one into `DistinctTransform`.
#[tokio::test]
async fn discovery_distinct_names_uses_the_sorted_key_distinct_transform() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_discovery_names_pipeline");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_series(&client, db, now_ms).await;

    let window = pulsus_read::metrics::DataWindow {
        start_ms: now_ms - 3_600_000,
        end_ms: now_ms,
    };
    let table = format!("{db}.metric_series");
    let narrow_sql = pulsus_read::metrics::sql::discovery_distinct_names_query(
        &table,
        &unfiltered_discovery_filter(),
        window,
        1,
    );
    let narrow = explain_pipeline_raw(&client, &narrow_sql).await;
    assert!(
        transform_lines(&narrow, "DistinctSortedStreamTransform") >= 2,
        "both DISTINCT stages must stream off the sorted leading key:\n{narrow}"
    );
    assert_eq!(
        transform_lines(&narrow, "DistinctTransform"),
        0,
        "a hash-set DISTINCT means the projection is no longer served from the sorted \
         key:\n{narrow}"
    );

    // The live control: same server, same corpus, same window bound, a
    // non-leading key column. Without this the assertion above could pass
    // by ClickHouse having renamed the transform.
    let control_sql = format!(
        "SELECT DISTINCT fingerprint\nFROM {table}\nWHERE unix_milli >= {} AND unix_milli <= {}\n\
         ORDER BY fingerprint",
        window.start_ms, window.end_ms
    );
    let control = explain_pipeline_raw(&client, &control_sql).await;
    assert!(
        transform_lines(&control, "DistinctTransform") >= 1,
        "the control must show the hash-set DISTINCT this server still emits, or the \
         absence assertion above is vacuous:\n{control}"
    );
}

/// Issue #96 (degraded-cache discovery fallback) — the probe-derived
/// **fetch** gate: `discovery_fetch_by_names` (`metric_name IN (…)` + the
/// `unix_milli` window, label matchers in SQL, NO `fingerprint IN`) must
/// engage the leading `metric_name` component of the `(metric_name,
/// fingerprint, unix_milli)` primary key on `metric_series` in the live
/// plan — the same Tier-1 evidence class as #89's `discovery_fetch_multi`
/// gate above, minus the fingerprint component (the degraded route resolves
/// NAMES only; the label matchers narrow within each pruned metric). This
/// is what keeps the degraded fallback's dominant-cost stage PK-pruned, not
/// a name-less full scan. The PROBE itself is deliberately NOT gated here
/// (a regex `metric_name` predicate can't range-prune the leading PK
/// column — its bound, not index engagement, is the perf gate; see
/// `live_discovery_fallback.rs`).
#[tokio::test]
async fn discovery_fetch_by_names_prunes_on_the_metric_name_primary_key_component() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_discovery_by_names");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_series(&client, db, now_ms).await;

    let window = pulsus_read::metrics::DataWindow {
        start_ms: now_ms - 3_600_000,
        end_ms: now_ms,
    };
    let table = format!("{db}.metric_series");
    // The exact fetch a degraded-cache name-matcher discovery filter's
    // probe produces (`MetricsEngine::discovery_series` wave 2): the probed
    // names IN-set, with a label matcher applied in SQL. `bucket_ms = 1`
    // floors to the exact bounds so the seeded rows stay in-window.
    let sql = pulsus_read::metrics::sql::discovery_fetch_by_names(
        &table,
        &["sv".to_string(), "sv2".to_string()],
        &[pulsus_read::metrics::LabelMatcher {
            key: "job".to_string(),
            op: pulsus_read::metrics::MatchOp::Eq,
            value: "api".to_string(),
        }],
        window,
        1,
    );

    let usage = explain(&client, &sql).await;
    assert_eq!(
        usage,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), and((unix_milli in [#, +Inf)), (metric_name in #-element set)))",
        ]),
        "the metric_name IN component must engage the metric_series primary key"
    );
}

/// Issue #315 — the compile probe must be **free**. The probe is a
/// constant `match()` over an empty subject, spliced into the bucket-floored
/// lower bound (`metrics::sql::re2_compile_probe`) so ClickHouse folds it
/// during query analysis. This gate is the evidence that the folding
/// happens *before* index analysis: the `metric_series` fallback subquery
/// with a regex matcher must engage exactly the same MinMax/Partition/
/// PrimaryKey conditions as the same subquery with an `Eq` matcher, digits
/// normalised — a fold that arrived too late would drop `unix_milli` from
/// the primary-key condition and turn the fallback into a wider scan.
///
/// A standalone `AND <constant>` conjunct was rejected for the same reason
/// in reverse: it preserves these conditions but stops the matcher
/// predicate from moving *fully* into PREWHERE, leaving a `Filter` step
/// that re-evaluates `JSONExtractString` + `match` on every surviving row
/// (measured with `EXPLAIN actions=1` on 24.8.14.39).
#[tokio::test]
async fn the_re2_compile_probe_costs_the_metric_series_fallback_no_index_engagement() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_metrics_compile_probe");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;
    let now_ms = ts_ns / 1_000_000;
    seed_metric_series(&client, db, now_ms).await;

    let window = pulsus_read::metrics::DataWindow {
        start_ms: now_ms - 3_600_000,
        end_ms: now_ms,
    };
    let table = format!("{db}.metric_series");
    let matcher = |op| pulsus_read::metrics::LabelMatcher {
        key: "job".to_string(),
        op,
        value: "api".to_string(),
    };
    let subquery = |op| {
        pulsus_read::metrics::sql::historical_series_subquery(
            &table,
            "sv",
            window,
            1,
            &[matcher(op)],
        )
    };

    let with_probe = subquery(pulsus_read::metrics::MatchOp::Re);
    let without_probe = subquery(pulsus_read::metrics::MatchOp::Eq);
    // Premise: the two SQL texts genuinely differ, and only the regex one
    // carries a probe — otherwise this compares a query with itself.
    assert!(
        with_probe.contains("+ 0 * (match('', "),
        "the regex subquery must carry the probe: {with_probe}"
    );
    assert!(
        !without_probe.contains("match("),
        "the Eq subquery must carry no regex at all: {without_probe}"
    );

    assert_eq!(
        explain(&client, &with_probe).await,
        explain(&client, &without_probe).await,
        "the compile probe changed the metric_series index analysis"
    );

    // And the absolute shape, so "identical" cannot mean "both degraded".
    assert_eq!(
        explain(&client, &with_probe).await,
        v(&[
            "MinMax",
            "Keys:",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), (unix_milli in [#, +Inf)))",
            "Partition",
            "Condition: true",
            "PrimaryKey",
            "Keys:",
            "metric_name",
            "unix_milli",
            "Condition: and((unix_milli in (-Inf, #]), and((unix_milli in [#, +Inf)), (metric_name in ['sv', 'sv'])))",
        ]),
        "the bucket-floored window must still prune on unix_milli and metric_name"
    );
}

// ---------------------------------------------------------------------
// Issue M6-10 (AC3, the launch's named rollup-vs-raw gate): an un-piped
// `count_over_time` stays rollup-served (`log_metrics_<res>`); an
// unwrapped `sum_over_time` is client-aggregated and reads `log_samples`
// raw — two distinct table targets, both index-served.
// ---------------------------------------------------------------------

/// Issue #227: an un-piped range `count_over_time` slides raw (the rollup
/// fast-path is retired for range reads) and prunes on the `log_samples`
/// primary key.
#[tokio::test]
async fn m6_10_unpiped_count_over_time_range_slides_raw() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_m610_range_raw");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let mp = metric_plan(
        r#"count_over_time({env="prod"}[5m])"#,
        &range_params(ts_ns),
        db,
    );
    assert!(!mp.rollup, "issue #227: a range count slides raw");
    assert!(mp.client.is_some());
    assert_eq!(mp.table, "log_samples");
    let table = format!("{db}.log_samples");
    let sql = sql::metric_raw_samples_sliding(
        &table,
        &[literal("checkout")],
        &[FP_PROD],
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
        projection_of(&mp),
    );
    let usage = explain(&client, &sql).await;
    assert_eq!(usage, expected_metric_instant_raw_usage());
}

#[tokio::test]
async fn m6_10_unwrapped_sum_over_time_reads_log_samples_raw_on_the_primary_key() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_m610_client_raw");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let mp = metric_plan(
        r#"sum_over_time({env="prod"} | logfmt | unwrap duration(took) [5m])"#,
        &range_params(ts_ns),
        db,
    );
    assert!(!mp.rollup);
    assert!(mp.client.is_some(), "unwrap forces the client-agg mode");
    assert_eq!(mp.table, "log_samples");
    assert_eq!(
        mp.routing.reason,
        "raw: client-side pipeline/unwrap aggregation"
    );
    let table = format!("{db}.log_samples");
    let sql = sql::metric_raw_samples(
        &table,
        &[literal("checkout")],
        &[FP_PROD],
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
        projection_of(&mp),
    );
    assert!(!sql.contains("LIMIT"), "aggregations never truncate: {sql}");
    let usage = explain(&client, &sql).await;
    // Same `(service, fingerprint, timestamp_ns)` primary-key engagement
    // as every raw log_samples read; no body predicate, so no Skip
    // blocks are consulted.
    assert_eq!(usage, expected_metric_instant_raw_usage());
}

/// Issue #344 — a range-aggregation grouping costs the SCAN nothing.
///
/// The clause is a per-row label projection inside the client
/// aggregator; it never reaches SQL. So a grouped query must plan the
/// **byte-identical** statement to its ungrouped twin, engage the same
/// primary key, and issue the same ONE scan — never one scan per group,
/// and never an extra round trip to enumerate the groups. Asserted as
/// SQL equality plus the shared `EXPLAIN indexes=1` usage, so a future
/// change that pushed grouping into SQL would have to justify itself
/// here rather than land unnoticed.
#[tokio::test]
async fn a_grouped_range_aggregation_plans_the_same_single_raw_scan_as_its_ungrouped_twin() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_344_grouped_scan");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    let table = format!("{db}.log_samples");
    let build = |query: &str| {
        let mp = metric_plan(query, &range_params(ts_ns), db);
        assert!(!mp.rollup, "{query}");
        assert!(mp.client.is_some(), "{query}: unwrap forces client-agg");
        assert_eq!(mp.table, "log_samples", "{query}");
        assert_eq!(
            mp.routing.reason, "raw: client-side pipeline/unwrap aggregation",
            "{query}: grouping must not change the routing decision"
        );
        let sql = sql::metric_raw_samples(
            &table,
            &[literal("checkout")],
            &[FP_PROD],
            TimeWindow {
                start_ns: mp.start_ns,
                end_ns: mp.end_ns,
            },
            mp.scan_lower,
            &mp.extra_predicates,
            projection_of(&mp),
        );
        assert!(!sql.contains("LIMIT"), "{query}: {sql}");
        (mp, sql)
    };

    let (plain, plain_sql) =
        build(r#"max_over_time({env="prod"} | logfmt | unwrap duration(took) [5m])"#);
    for grouped in [
        r#"max_over_time({env="prod"} | logfmt | unwrap duration(took) [5m]) by (env)"#,
        r#"max_over_time({env="prod"} | logfmt | unwrap duration(took) [5m]) without (env)"#,
        r#"max_over_time({env="prod"} | logfmt | unwrap duration(took) [5m]) by ()"#,
    ] {
        let (mp, sql) = build(grouped);
        assert_eq!(sql, plain_sql, "{grouped}: the emitted SQL must not move");
        assert_eq!(
            (mp.start_ns, mp.end_ns, mp.step_ns),
            (plain.start_ns, plain.end_ns, plain.step_ns),
            "{grouped}: the scan window must not move"
        );
        assert_eq!(
            mp.probes.len(),
            plain.probes.len(),
            "{grouped}: no extra round trip"
        );
        assert!(
            mp.client.as_ref().expect("client").grouping.is_some(),
            "{grouped}: the clause must be planned, not dropped"
        );
        let usage = explain(&client, &sql).await;
        assert_eq!(usage, expected_metric_instant_raw_usage(), "{grouped}");
    }
}

#[tokio::test]
async fn metric_raw_fallback_uses_the_service_fingerprint_timestamp_primary_key() {
    skip_unless_live!();
    let db = &pulsus_testkit::test_db("pulsus_read_it_metric_raw");
    let ts_ns = now_ns();
    let client = setup(db, ts_ns).await;

    // A line filter forces the raw fallback (`plan::metric_plan`: the
    // rollup table has no `body` column to filter on).
    let mp = metric_plan(
        r#"count_over_time({env="prod"} |= "refused" [5m])"#,
        &range_params(ts_ns),
        db,
    );
    assert!(!mp.rollup);
    let table = format!("{db}.log_samples");
    // Issue #227: a range read with a line filter slides raw
    // (`metric_raw_samples_sliding`), the filter pushed down as a predicate.
    let sql = sql::metric_raw_samples_sliding(
        &table,
        &[literal("checkout")],
        &[FP_PROD],
        TimeWindow {
            start_ns: mp.start_ns,
            end_ns: mp.end_ns,
        },
        mp.scan_lower,
        &mp.extra_predicates,
        projection_of(&mp),
    );

    let raw = explain_raw(&client, &sql).await;
    let usage = index_usage(&raw);
    assert_eq!(without_skip_blocks(&usage), expected_stage3_prefix());
    assert_eq!(skip_blocks(&raw), expected_stage3_skip_blocks());
    // A positive line filter is a plain conjunction over `body`, so 26.x
    // reports no `<Combined skip indexes>` pseudo-block here (measured).
    assert!(!combined_skip_present(&raw));
}
