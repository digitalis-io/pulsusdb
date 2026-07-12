//! Pooling headroom scenario: `pool_size` concurrent inserters vs a single
//! connection, to characterize pooling headroom (issue #3 plan).

use std::time::Instant;

use super::CrateUnderTest;
use super::rows::gen_metric_rows;

#[derive(Clone, Debug, serde::Serialize)]
pub struct PoolReport {
    pub crate_name: &'static str,
    pub connections: usize,
    pub total_rows: u64,
    pub single_conn_rows_per_sec: f64,
    pub concurrent_rows_per_sec: f64,
    pub speedup: f64,
}

/// Times a single connection inserting `total_rows` in `block_rows` blocks.
pub async fn single_conn_rows_per_sec<C: CrateUnderTest>(
    c: &C,
    table: &str,
    total_rows: u64,
    block_rows: u64,
) -> anyhow::Result<f64> {
    c.execute_ddl(&super::insert::metric_table_ddl(table))
        .await?;
    c.execute_ddl(&format!("TRUNCATE TABLE {table}")).await?;
    let rows = gen_metric_rows(total_rows, 100_000, 3);
    let start = Instant::now();
    for chunk in rows.chunks(block_rows as usize) {
        c.insert_metric_block(table, chunk).await?;
    }
    let elapsed = start.elapsed();
    Ok(total_rows as f64 / elapsed.as_secs_f64())
}

/// Times `conns.len()` connections concurrently inserting `rows_per_conn`
/// rows each into `table` (each connection uses its own row range so no two
/// connections write identical fingerprints/timestamps).
pub async fn concurrent_rows_per_sec<C>(
    table: &str,
    conns: Vec<C>,
    rows_per_conn: u64,
    block_rows: u64,
) -> anyhow::Result<f64>
where
    C: CrateUnderTest + Send + 'static,
{
    let n = conns.len();
    if let Some(first) = conns.first() {
        first
            .execute_ddl(&super::insert::metric_table_ddl(table))
            .await?;
        first
            .execute_ddl(&format!("TRUNCATE TABLE {table}"))
            .await?;
    }
    let start = Instant::now();
    let mut set = tokio::task::JoinSet::new();
    for (i, c) in conns.into_iter().enumerate() {
        let table = table.to_string();
        set.spawn(async move {
            let rows = gen_metric_rows(rows_per_conn, 200_000 + i as u64 * rows_per_conn, 5);
            for chunk in rows.chunks(block_rows as usize) {
                c.insert_metric_block(&table, chunk).await?;
            }
            anyhow::Ok(())
        });
    }
    while let Some(res) = set.join_next().await {
        res??;
    }
    let elapsed = start.elapsed();
    Ok((n as u64 * rows_per_conn) as f64 / elapsed.as_secs_f64())
}
