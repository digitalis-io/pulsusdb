//! A read of `system.query_log` that waits for the statements it reads.
//!
//! One `SYSTEM FLUSH LOGS` is not a barrier: a statement's `QueryFinish` row
//! can be queued after its client has the answer, so one flush and one read
//! can miss it. Its `QueryStart` row is queued before the statement runs, so
//! once flushed it names every statement sent. [`settle_query_log`] flushes
//! and reads until each of those statements has its ending row too.
//!
//! One execution is a `query_id` with its `query_start_time_microseconds`,
//! which its start and ending rows share. The log keeps earlier runs' rows
//! and an id can repeat, so an old run's ending row must not stand in for
//! this run's. A caller's filter must still select this run's statements
//! alone (a per-run comment, id or time marker), or an earlier run's
//! unfinished statement would hold the wait.

use std::time::{Duration, Instant};

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings, Row};

/// How long [`settle_query_log`] waits.
pub const QUERY_LOG_WAIT: Duration = Duration::from_secs(60);

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
struct Counts {
    ended: u64,
    pending: u64,
}

/// Waits until, on every one of `nodes`, each statement `filter` selects in
/// `system.query_log` that has a `QueryStart` row also has its ending row
/// (`QueryFinish` or `ExceptionWhileProcessing`), and until at least
/// `at_least` statements have ended across them, an `ExceptionBeforeStart`
/// counting as ended. `filter` is a condition on `system.query_log`'s columns
/// and must not name `type`; a statement that reads `system.query_log`,
/// this wait's own included, is never counted. Fails after [`QUERY_LOG_WAIT`] with what each
/// node still lacks.
pub async fn settle_query_log(nodes: &[&ChClient], filter: &str, at_least: u64) {
    let sql = format!(
        "SELECT toUInt64(countIf(e)) AS ended, toUInt64(countIf(s AND NOT e)) AS pending \
         FROM (SELECT query_id, max(type = 'QueryStart') AS s, \
                 max(type IN ('QueryFinish', 'ExceptionWhileProcessing', \
                              'ExceptionBeforeStart')) AS e \
               FROM system.query_log \
               WHERE ({filter}) AND query NOT LIKE '%system.query_log%' \
               GROUP BY query_id, query_start_time_microseconds)"
    );
    let deadline = Instant::now() + QUERY_LOG_WAIT;
    loop {
        let mut seen = Vec::new();
        for node in nodes {
            node.execute(
                "SYSTEM FLUSH LOGS",
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("SYSTEM FLUSH LOGS");
            let mut stream = node
                .query_stream::<Counts>(&sql, &QuerySettings::new())
                .await
                .expect("system.query_log");
            let counts = stream.next().await.expect("one row").expect("decode");
            seen.push(counts);
        }
        let ended: u64 = seen.iter().map(|c| c.ended).sum();
        if ended >= at_least && seen.iter().all(|c| c.pending == 0) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "system.query_log did not settle within {QUERY_LOG_WAIT:?} for {filter}: \
             per node (ended, started without an ending row) {:?}, at least {at_least} \
             ended statements expected",
            seen.iter()
                .map(|c| (c.ended, c.pending))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
