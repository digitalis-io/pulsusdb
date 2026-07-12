//! Aggregate-state correctness gate (blocking, issue #3 amendment): a
//! deterministic dataset with a known `fingerprint > 2^63` is inserted, the
//! MV populates `metric_samples_5m` (docs/schemas.md §2.2), and the exact
//! §2.3 read shape (`finalizeAggregation(argMinMergeState(...))`, plain
//! `SimpleAggregateFunction(sum, UInt64)`) is asserted bit-correct.

use super::CrateUnderTest;
use super::rows::{AggRow, HIGH_BIT_FINGERPRINT};

#[derive(Clone, Debug, serde::Serialize)]
pub struct AggstateReport {
    pub crate_name: &'static str,
    pub ok: bool,
    pub expected: ExpectedAgg,
    pub actual: Option<AggRowOwned>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ExpectedAgg {
    pub fingerprint: u64,
    pub val_count: u64,
    pub first_value: f64,
    pub last_value: f64,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq)]
pub struct AggRowOwned {
    pub fingerprint: u64,
    pub val_count: u64,
    pub first_value: f64,
    pub last_value: f64,
}

impl From<AggRow> for AggRowOwned {
    fn from(r: AggRow) -> Self {
        Self {
            fingerprint: r.fingerprint,
            val_count: r.val_count,
            first_value: r.first_value,
            last_value: r.last_value,
        }
    }
}

const SAMPLE_COUNT: u64 = 1_000;
/// A 300s-bucket-aligned base timestamp so every sample lands in one 5m bucket.
const BASE_UNIX_MILLI: i64 = (1_700_000_000_000 / 300_000) * 300_000;

fn deterministic_dataset() -> Vec<super::rows::MetricRow> {
    (0..SAMPLE_COUNT)
        .map(|i| super::rows::MetricRow {
            metric_name: "aggstate_metric".to_string(),
            fingerprint: HIGH_BIT_FINGERPRINT,
            unix_milli: BASE_UNIX_MILLI + i as i64, // 1ms apart, all in one 300s bucket
            value: (i + 1) as f64,                  // 1.0..=1000.0, strictly increasing with ts
        })
        .collect()
}

fn expected() -> ExpectedAgg {
    ExpectedAgg {
        fingerprint: HIGH_BIT_FINGERPRINT,
        val_count: SAMPLE_COUNT,
        first_value: 1.0,                // value at the earliest unix_milli
        last_value: SAMPLE_COUNT as f64, // value at the latest unix_milli
    }
}

/// Runs the correctness gate. `raw_table` receives the deterministic
/// dataset; `tier_table`/`mv_name` are created fresh (dropped first, so
/// reruns are idempotent).
pub async fn bench_aggstate<C: CrateUnderTest>(
    c: &C,
    raw_table: &str,
    tier_table: &str,
    mv_name: &str,
) -> AggstateReport {
    let exp = expected();
    let mut report = AggstateReport {
        crate_name: c.name(),
        ok: false,
        expected: exp.clone(),
        actual: None,
        error: None,
    };

    if let Err(e) = c
        .execute_ddl(&super::insert::metric_table_ddl(raw_table))
        .await
    {
        report.error = Some(format!("create raw table: {e}"));
        return report;
    }
    if let Err(e) = c.execute_ddl(&format!("TRUNCATE TABLE {raw_table}")).await {
        report.error = Some(format!("truncate raw table: {e}"));
        return report;
    }
    let _ = c
        .execute_ddl(&format!("DROP VIEW IF EXISTS {mv_name}"))
        .await;
    let _ = c
        .execute_ddl(&format!("DROP TABLE IF EXISTS {tier_table}"))
        .await;
    if let Err(e) = c.execute_ddl(&super::ddl::tier_table_ddl(tier_table)).await {
        report.error = Some(format!("create tier table: {e}"));
        return report;
    }
    if let Err(e) = c
        .execute_ddl(&super::ddl::tier_mv_ddl(mv_name, tier_table, raw_table))
        .await
    {
        report.error = Some(format!("create mv: {e}"));
        return report;
    }

    let rows = deterministic_dataset();
    if let Err(e) = c.insert_metric_block(raw_table, &rows).await {
        report.error = Some(format!("insert dataset: {e}"));
        return report;
    }

    // docs/schemas.md §2.3 exact read shape: SimpleAggregateFunction(sum, UInt64)
    // combined with sum(), AggregateFunction states combined with
    // finalizeAggregation(argMin/argMaxMergeState(...)).
    let sql = format!(
        "SELECT fingerprint,
                sum(val_count) AS val_count,
                finalizeAggregation(argMinMergeState(first_value)) AS first_value,
                finalizeAggregation(argMaxMergeState(last_value)) AS last_value
         FROM {tier_table}
         WHERE fingerprint = {HIGH_BIT_FINGERPRINT}
         GROUP BY fingerprint"
    );
    let rows = match c.select_agg_rows(&sql).await {
        Ok(rows) => rows,
        Err(e) => {
            report.error = Some(format!("select agg rows: {e}"));
            return report;
        }
    };
    let Some(actual) = rows.into_iter().next() else {
        report.error = Some("no row returned (MV did not populate the tier table)".to_string());
        return report;
    };
    let actual: AggRowOwned = actual.into();
    report.ok = actual.fingerprint == exp.fingerprint
        && actual.val_count == exp.val_count
        && (actual.first_value - exp.first_value).abs() < 1e-9
        && (actual.last_value - exp.last_value).abs() < 1e-9;
    report.actual = Some(actual);
    report
}
