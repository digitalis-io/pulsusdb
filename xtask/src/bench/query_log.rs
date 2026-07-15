//! Shared `system.query_log` evidence-capture machinery (issue #34 plan,
//! "Open questions resolved" #1: relocated out of `queries.rs` rather than
//! duplicated, so `bench logs-read` and `bench metrics-labels` read
//! evidence through the **same** reader — never a second, divergent
//! `query_log` schema). Behaviourally identical to the pre-#34 private
//! copies in `queries.rs`; this is a mechanical relocation
//! (`pub(crate)` here, re-imported by `queries.rs` and
//! `metrics_labels::paths`), not a rewrite.

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings, Row};

/// Applies `query_id` and, when present, `log_comment` to `base` — the
/// single place every scenario's per-stage settings route `log_comment`
/// run-tagging through (issue #35 architect plan: "benefits every
/// scenario"), so `SETTINGS log_comment` A/B correlation via
/// `system.query_log` never has a second, divergent implementation.
/// `log_comment` is applied as an HTTP request setting via
/// [`QuerySettings::set`] — never inlined into SQL text (same injection
/// posture as every other setting this crate applies).
pub(crate) fn tagged_settings(
    base: QuerySettings,
    query_id: &str,
    log_comment: Option<&str>,
) -> QuerySettings {
    let settings = base.set("query_id", query_id);
    match log_comment {
        Some(comment) => settings.set("log_comment", comment),
        None => settings,
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub(crate) struct QueryLogTotals {
    pub(crate) read_rows: u64,
    pub(crate) read_bytes: u64,
    pub(crate) selected_marks: u64,
    pub(crate) memory_usage: u64,
    pub(crate) query_duration_ms: u64,
}

impl std::ops::AddAssign for QueryLogTotals {
    fn add_assign(&mut self, other: Self) {
        self.read_rows += other.read_rows;
        self.read_bytes += other.read_bytes;
        self.selected_marks += other.selected_marks;
        self.memory_usage = self.memory_usage.max(other.memory_usage);
        self.query_duration_ms += other.query_duration_ms;
    }
}

/// Flushes `system.query_log` on the connected node only. `cluster`
/// distinguishes this from [`flush_logs_before_shard_read`]: every shard's
/// `QueryFinish` row for a distributed sub-query lands in *that shard's
/// own* local `system.query_log`, which only its own periodic flush
/// interval (or its own `FLUSH LOGS`) makes queryable — flushing the
/// initiator alone leaves the other shards' rows invisible to a
/// `clusterAllReplicas` read.
pub(crate) async fn flush_logs(client: &ChClient) -> anyhow::Result<()> {
    client
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await?;
    Ok(())
}

/// Flushes `system.query_log` cluster-wide (`ON CLUSTER`) before a
/// `--dist` shard-evidence read — see [`flush_logs`]'s doc comment for why
/// a local-only flush is not enough once a caller needs every shard's own
/// rows.
pub(crate) async fn flush_logs_before_shard_read(
    client: &ChClient,
    cluster: &str,
) -> anyhow::Result<()> {
    client
        .execute(
            &format!("SYSTEM FLUSH LOGS ON CLUSTER {cluster}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await?;
    Ok(())
}

pub(crate) async fn read_query_log(
    client: &ChClient,
    query_id: &str,
) -> anyhow::Result<QueryLogTotals> {
    let sql = format!(
        "SELECT read_rows, read_bytes, ProfileEvents['SelectedMarks'] AS selected_marks, \
         memory_usage, query_duration_ms FROM system.query_log \
         WHERE query_id = '{query_id}' AND type = 'QueryFinish' \
         ORDER BY event_time_microseconds DESC LIMIT 1"
    );
    let mut stream = client
        .query_stream::<QueryLogTotals>(&sql, &QuerySettings::new())
        .await?;
    match stream.next().await {
        Some(row) => Ok(row?),
        None => Ok(QueryLogTotals::default()),
    }
}
