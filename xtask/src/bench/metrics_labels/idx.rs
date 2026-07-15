//! Bench-local `metric_series_idx` prototype (docs/schemas.md §2.1's
//! spec'd-but-not-shipped inverted label index). **Prototype only — never
//! added to `pulsus-schema`'s migration catalog or `run_init`.** This
//! module owns its own DDL string and issues it directly against the
//! benchmark database; the M3 milestone decides whether it ships (architect
//! plan, "Out of scope" / edge case 4). Single-node DDL is §2.1's verbatim
//! `ReplacingMergeTree ORDER BY (metric_name, key, val, bucket,
//! fingerprint)`; `--dist` additionally creates the `_dist` `Distributed`
//! wrapper co-sharded by `cityHash64(metric_name, fingerprint)` — the same
//! sharding expression `Family::Metrics::sharding_expr` uses for
//! `metric_series`/`metric_samples`, so a fingerprint's idx rows land on
//! the same shard as its samples (§2.1's shard-locality claim for the
//! index).
//!
//! Populated by `INSERT ... SELECT ... ARRAY JOIN
//! JSONExtractKeysAndValues(labels, 'String')` over `metric_series` — the
//! real MV-shape population `docs/schemas.md` §2.1 describes ("populated by
//! MV over metric_series"), run once here as a plain `INSERT ... SELECT`
//! rather than as an actual materialized view (a real MV is a product/M3
//! decision, out of scope for this benchmark) — this still exercises the
//! real ARRAY JOIN fan-out shape and its write/storage cost, which is what
//! the M3 write-cost input needs.
//!
//! **Resolution-SQL scope (issue #34 CODE review round-2 [adjudicated]
//! finding #2, round-3 [precision] finding #1).** `super::paths::idx_resolve_sql`'s
//! single-pass conditional aggregation is exercised, and its parity with
//! the cache/SQL-fallback paths is only *evidenced*, against the
//! benchmark's four `SelectorKind`s: bounded positive equality (`NarrowEq`,
//! `BroadEq`), regex (`Regex5xx`), and single-negative (`NegBroad`). None
//! of those is an **empty-accepting matcher** — a matcher that a
//! label-less series (zero rows for that key at all) must, under
//! Prometheus's absent-label-as-`""` semantics, either match or not match.
//! Two **opposite**, both verified, failure modes if such a matcher were
//! run against this prototype's current SQL:
//! - **`job!=""` (pure-negative form) wrongly *INCLUDES* an absent-`job`
//!   series.** The pure-negative shape's `HAVING countIf(neg_or) = 0`
//!   trivially holds when there are zero `key = 'job'` rows at all
//!   (`countIf` over an empty row-set is `0`) — but Prometheus semantics
//!   read an absent label as `""`, and `"" != ""` is **false**, so the
//!   series should **not** match `job!=""` at all. This is a false
//!   positive: the series is wrongly retained.
//! - **`job=~".*"` (positive-branch form) wrongly *EXCLUDES* an
//!   absent-`job` series.** The positive-branch shape requires at least
//!   one row matching the positive predicate to bring a fingerprint into
//!   the `GROUP BY` output at all; a label-less series has zero `key =
//!   'job'` rows, so it can never appear in the result regardless of what
//!   the pattern is — but `.*` matches the empty string `""`, so under
//!   Prometheus semantics the series **should** match `job=~".*"`. This is
//!   a false negative: the series is wrongly dropped.
//!
//! Both are **known, recorded open cases for the M3 idx design if it
//! ships** — deliberately **not** generalized here. Fixing either now
//! would mean guessing at general `metric_series_idx` resolution
//! semantics, which is the M3 ship design's decision to make, not this
//! evidence run's; a speculative general fix now risks being superseded
//! (or contradicted) by that design anyway. Honest scoping of what this
//! prototype's evidence does and does not cover is the more useful
//! artifact for the M3 gate.

use std::time::Instant;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings, Row};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IdxSummary {
    /// Wall-clock time of the `INSERT ... SELECT ... ARRAY JOIN`
    /// population — the write-cost input the M3 ship decision needs.
    pub build_ms: u64,
    /// `sum(rows)` over `system.parts` for `metric_series_idx` (`active`
    /// parts only) — one row per label pair per series per bucket, per
    /// §2.1's documented real write cost.
    pub rows: u64,
    /// `sum(bytes_on_disk)` over the same `system.parts` scope.
    pub bytes_on_disk: u64,
}

/// §2.1's verbatim single-node DDL, `{{db}}` substituted here directly
/// (this is a bench-local prototype, not rendered through
/// `pulsus_schema::render` — no `Replicated*`-engine-swap machinery needed
/// beyond the one explicit `_dist` wrapper built below). **`ON CLUSTER`
/// under `--dist`** (issue #34 CODE review [medium] finding): without it,
/// the local table is created only on the node this benchmark connects to,
/// so the `_dist` `Distributed` wrapper's remote shards have no local
/// `metric_series_idx` to write into and `--dist` population fails —
/// mirrors `pulsus_schema::render`'s own `{{on_cluster}}` token shape
/// (` ON CLUSTER '<name>'`, quoted, right after the table name).
fn create_table_sql(db: &str, dist: bool, cluster: &str) -> String {
    let on_cluster = if dist {
        format!(" ON CLUSTER '{cluster}'")
    } else {
        String::new()
    };
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.metric_series_idx{on_cluster} (\n\
             bucket       Int64,\n\
             metric_name  LowCardinality(String),\n\
             key          LowCardinality(String),\n\
             val          String,\n\
             fingerprint  UInt64\n\
         ) ENGINE = ReplacingMergeTree\n\
         PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(bucket))\n\
         ORDER BY (metric_name, key, val, bucket, fingerprint);"
    )
}

/// The `_dist` `Distributed` wrapper, co-sharded by
/// `cityHash64(metric_name, fingerprint)` — `Family::Metrics`'s own
/// sharding expression (docs/schemas.md §7), so idx fingerprint sets stay
/// shard-local to their `metric_series`/`metric_samples` counterparts
/// (architect plan edge case 5: "a mismatched key would invalidate the
/// shard-locality claim").
fn create_dist_sql(db: &str, cluster: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.metric_series_idx_dist AS {db}.metric_series_idx\n\
         ENGINE = Distributed('{cluster}', {db}, metric_series_idx, cityHash64(metric_name, fingerprint));"
    )
}

fn populate_sql(source_table: &str, target_table: &str) -> String {
    format!(
        "INSERT INTO {target_table}\n\
         SELECT unix_milli AS bucket, metric_name, kv.1 AS key, kv.2 AS val, fingerprint\n\
         FROM {source_table}\n\
         ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv"
    )
}

async fn parts_stats(
    client: &ChClient,
    db: &str,
    table: &str,
    cluster: Option<&str>,
) -> anyhow::Result<(u64, u64)> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct PartsRow {
        rows: u64,
        bytes_on_disk: u64,
    }
    let source = match cluster {
        Some(cluster) => format!("clusterAllReplicas('{cluster}', system.parts)"),
        None => "system.parts".to_string(),
    };
    let sql = format!(
        "SELECT sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk FROM {source} \
         WHERE database = '{db}' AND table = '{table}' AND active"
    );
    let mut stream = client
        .query_stream::<PartsRow>(&sql, &QuerySettings::new())
        .await?;
    Ok(match stream.next().await {
        Some(row) => {
            let row = row?;
            (row.rows, row.bytes_on_disk)
        }
        None => (0, 0),
    })
}

/// Creates the prototype table (+ `_dist` wrapper if `dist`) and populates
/// it from `metric_series`'s current contents. `db`/`dist`/`cluster` mirror
/// [`super::corpus::MetricsCorpusSpec`]'s own fields.
pub async fn build(
    client: &ChClient,
    db: &str,
    dist: bool,
    cluster: &str,
) -> anyhow::Result<IdxSummary> {
    client
        .execute(
            &create_table_sql(db, dist, cluster),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await?;

    let (source_table, target_table) = if dist {
        client
            .execute(
                &create_dist_sql(db, cluster),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await?;
        (
            "metric_series_dist".to_string(),
            "metric_series_idx_dist".to_string(),
        )
    } else {
        ("metric_series".to_string(), "metric_series_idx".to_string())
    };

    let start = Instant::now();
    client
        .execute(
            &populate_sql(&source_table, &target_table),
            &QuerySettings::new(),
            // An INSERT ... SELECT backfill is never auto-retried by the
            // client wrapper (pulsus_clickhouse::Idempotency's own
            // documented example for this exact statement shape) — a
            // retried re-run would double every row's contribution to
            // system.parts, corrupting the write-cost evidence this
            // function exists to capture.
            Idempotency::NonIdempotent,
        )
        .await?;
    let build_ms = start.elapsed().as_millis() as u64;

    let (rows, bytes_on_disk) =
        parts_stats(client, db, "metric_series_idx", dist.then_some(cluster)).await?;

    Ok(IdxSummary {
        build_ms,
        rows,
        bytes_on_disk,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_table_sql_uses_the_section_2_1_verbatim_order_by() {
        let sql = create_table_sql("pulsus_bench", false, "c");
        assert!(sql.contains("ORDER BY (metric_name, key, val, bucket, fingerprint)"));
        assert!(sql.contains("ReplacingMergeTree"));
        assert!(!sql.contains("ON CLUSTER"));
    }

    #[test]
    fn create_table_sql_appends_on_cluster_under_dist() {
        let sql = create_table_sql("pulsus_bench", true, "pulsus_bench_cluster");
        assert!(sql.contains("metric_series_idx ON CLUSTER 'pulsus_bench_cluster' ("));
    }

    #[test]
    fn create_dist_sql_uses_the_metrics_family_sharding_expression() {
        let sql = create_dist_sql("pulsus_bench", "pulsus_bench_cluster");
        assert!(sql.contains("cityHash64(metric_name, fingerprint)"));
        assert!(
            sql.contains("Distributed('pulsus_bench_cluster', pulsus_bench, metric_series_idx")
        );
    }

    #[test]
    fn populate_sql_array_joins_over_json_extract_keys_and_values() {
        let sql = populate_sql("metric_series", "metric_series_idx");
        assert!(sql.contains("ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv"));
        assert!(sql.contains("INSERT INTO metric_series_idx"));
        assert!(sql.contains("FROM metric_series"));
    }
}
