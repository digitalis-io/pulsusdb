//! `CrateUnderTest` implementations for the two M0 candidates:
//! `clickhouse` (HTTP + RowBinary) and `klickhouse` (native TCP).
//!
//! Per-crate row types live here (not in `rows.rs`) because the two crates'
//! `Row` derive macros are distinct traits; the crate-agnostic `MetricRow` /
//! `LogRow` / `AggRow` in `rows.rs` are converted at the boundary so the
//! scenario code (`insert.rs`, `fetch.rs`, ...) never names either crate.

use futures::StreamExt;

use super::CrateUnderTest;
use super::rows::{AggRow, LogRow, MetricRow};

// ---------------------------------------------------------------------
// clickhouse (HTTP + RowBinary)
// ---------------------------------------------------------------------

pub struct ChCandidate {
    pub client: clickhouse::Client,
}

impl ChCandidate {
    pub fn connect(url: &str, database: &str, user: &str, password: &str) -> Self {
        let mut client = clickhouse::Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(user);
        if !password.is_empty() {
            client = client.with_password(password);
        }
        Self { client }
    }
}

#[derive(clickhouse::Row, serde::Serialize)]
struct ChMetricRow<'a> {
    metric_name: &'a str,
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

#[derive(clickhouse::Row, serde::Serialize)]
struct ChLogRow<'a> {
    service: &'a str,
    fingerprint: u64,
    timestamp_ns: i64,
    severity: i8,
    body: &'a str,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct ChMetricProj {
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct ChPartCount {
    n: u64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct ChAggRow {
    fingerprint: u64,
    val_count: u64,
    first_value: f64,
    last_value: f64,
}

impl CrateUnderTest for ChCandidate {
    fn name(&self) -> &'static str {
        "clickhouse"
    }

    async fn execute_ddl(&self, sql: &str) -> anyhow::Result<()> {
        self.client.query(sql).execute().await?;
        Ok(())
    }

    async fn insert_metric_block(&self, table: &str, rows: &[MetricRow]) -> anyhow::Result<()> {
        let mut insert = self.client.insert::<ChMetricRow>(table).await?;
        for r in rows {
            insert
                .write(&ChMetricRow {
                    metric_name: &r.metric_name,
                    fingerprint: r.fingerprint,
                    unix_milli: r.unix_milli,
                    value: r.value,
                })
                .await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn insert_log_block(&self, table: &str, rows: &[LogRow]) -> anyhow::Result<()> {
        let mut insert = self.client.insert::<ChLogRow>(table).await?;
        for r in rows {
            insert
                .write(&ChLogRow {
                    service: &r.service,
                    fingerprint: r.fingerprint,
                    timestamp_ns: r.timestamp_ns,
                    severity: r.severity,
                    body: &r.body,
                })
                .await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn fetch_metric_projection(
        &self,
        table: &str,
        metric_name: &str,
    ) -> anyhow::Result<(u64, u64)> {
        let sql = format!(
            "SELECT fingerprint, unix_milli, value FROM {table} \
             PREWHERE metric_name = '{metric_name}' ORDER BY fingerprint, unix_milli"
        );
        let mut cursor = self.client.query(&sql).fetch::<ChMetricProj>()?;
        let mut count = 0u64;
        let mut checksum = 0u64;
        while let Some(row) = cursor.next().await? {
            count += 1;
            checksum ^= row.fingerprint ^ (row.unix_milli as u64) ^ row.value.to_bits();
        }
        Ok((count, checksum))
    }

    async fn part_count(&self, table: &str) -> anyhow::Result<u64> {
        let sql =
            format!("SELECT count() AS n FROM system.parts WHERE table = '{table}' AND active = 1");
        let row: ChPartCount = self.client.query(&sql).fetch_one().await?;
        Ok(row.n)
    }

    async fn select_agg_rows(&self, sql: &str) -> anyhow::Result<Vec<AggRow>> {
        let rows: Vec<ChAggRow> = self.client.query(sql).fetch_all().await?;
        Ok(rows
            .into_iter()
            .map(|r| AggRow {
                fingerprint: r.fingerprint,
                val_count: r.val_count,
                first_value: r.first_value,
                last_value: r.last_value,
            })
            .collect())
    }
}

// ---------------------------------------------------------------------
// klickhouse (native TCP)
// ---------------------------------------------------------------------

pub struct KlCandidate {
    pub client: klickhouse::Client,
}

impl KlCandidate {
    pub async fn connect(
        addr: &str,
        database: &str,
        user: &str,
        password: &str,
    ) -> anyhow::Result<Self> {
        let options = klickhouse::ClientOptions {
            username: user.to_string(),
            password: password.to_string(),
            default_database: database.to_string(),
            tcp_nodelay: true,
        };
        let client = klickhouse::Client::connect(addr, options).await?;
        Ok(Self { client })
    }
}

#[derive(klickhouse::Row, serde::Serialize, serde::Deserialize)]
struct KlMetricRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

#[derive(klickhouse::Row, serde::Serialize, serde::Deserialize)]
struct KlLogRow {
    service: String,
    fingerprint: u64,
    timestamp_ns: i64,
    severity: i8,
    body: String,
}

#[derive(klickhouse::Row, serde::Serialize, serde::Deserialize)]
struct KlMetricProj {
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
}

#[derive(klickhouse::Row, serde::Serialize, serde::Deserialize)]
struct KlPartCount {
    n: u64,
}

#[derive(klickhouse::Row, serde::Serialize, serde::Deserialize)]
struct KlAggRow {
    fingerprint: u64,
    val_count: u64,
    first_value: f64,
    last_value: f64,
}

impl CrateUnderTest for KlCandidate {
    fn name(&self) -> &'static str {
        "klickhouse"
    }

    async fn execute_ddl(&self, sql: &str) -> anyhow::Result<()> {
        self.client.execute(sql).await?;
        Ok(())
    }

    async fn insert_metric_block(&self, table: &str, rows: &[MetricRow]) -> anyhow::Result<()> {
        let kl_rows: Vec<KlMetricRow> = rows
            .iter()
            .map(|r| KlMetricRow {
                metric_name: r.metric_name.clone(),
                fingerprint: r.fingerprint,
                unix_milli: r.unix_milli,
                value: r.value,
            })
            .collect();
        let query = format!("INSERT INTO {table} FORMAT native");
        self.client.insert_native_block(query, kl_rows).await?;
        Ok(())
    }

    async fn insert_log_block(&self, table: &str, rows: &[LogRow]) -> anyhow::Result<()> {
        let kl_rows: Vec<KlLogRow> = rows
            .iter()
            .map(|r| KlLogRow {
                service: r.service.clone(),
                fingerprint: r.fingerprint,
                timestamp_ns: r.timestamp_ns,
                severity: r.severity,
                body: r.body.clone(),
            })
            .collect();
        let query = format!("INSERT INTO {table} FORMAT native");
        self.client.insert_native_block(query, kl_rows).await?;
        Ok(())
    }

    async fn fetch_metric_projection(
        &self,
        table: &str,
        metric_name: &str,
    ) -> anyhow::Result<(u64, u64)> {
        let sql = format!(
            "SELECT fingerprint, unix_milli, value FROM {table} \
             PREWHERE metric_name = '{metric_name}' ORDER BY fingerprint, unix_milli"
        );
        let mut stream = self.client.query::<KlMetricProj, _>(sql).await?;
        let mut count = 0u64;
        let mut checksum = 0u64;
        while let Some(row) = stream.next().await {
            let row = row?;
            count += 1;
            checksum ^= row.fingerprint ^ (row.unix_milli as u64) ^ row.value.to_bits();
        }
        Ok((count, checksum))
    }

    async fn part_count(&self, table: &str) -> anyhow::Result<u64> {
        let sql =
            format!("SELECT count() AS n FROM system.parts WHERE table = '{table}' AND active = 1");
        let row: KlPartCount = self.client.query_one(sql).await?;
        Ok(row.n)
    }

    async fn select_agg_rows(&self, sql: &str) -> anyhow::Result<Vec<AggRow>> {
        let rows: Vec<KlAggRow> = self.client.query_collect(sql).await?;
        Ok(rows
            .into_iter()
            .map(|r| AggRow {
                fingerprint: r.fingerprint,
                val_count: r.val_count,
                first_value: r.first_value,
                last_value: r.last_value,
            })
            .collect())
    }
}
