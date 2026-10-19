//! `/api/v1/query_exemplars`' pure parts (issue #501 part 2): the slice
//! width a thinned answer uses, the reference's label hash that orders a
//! series' exemplars, the answer's byte charges and budget, the hydration
//! chunks, and the assembly of rows and series labels into the answer.
//!
//! The statements are built in [`super::sql`] and sent by
//! [`super::exec::MetricsEngine::exemplars`].

use pulsus_model::Fingerprint;

use crate::logql::error::{ReadError, TooBroadReason};

use super::matcher::DataWindow;
use super::rows::{ExemplarRow, ExemplarSeriesRow};

/// The most exemplars one answer carries: the reference's whole-store
/// default capacity (`config/config.go`, `MaxExemplars: 100000`). At or
/// under it, counted in distinct exemplars, the answer is every exemplar;
/// over it, the answer is thinned to one exemplar per slice.
pub const EXEMPLAR_ANSWER_CAP: u64 = 100_000;

/// The bytes one answer may keep and write: 256 MiB, the trace search's
/// retention budget (`crate::traces::exec::HYDRATION_BYTE_BUDGET`), and
/// each statement's `max_result_bytes`.
pub const EXEMPLAR_ANSWER_BYTES: usize = 268_435_456;

/// The most series IDs one hydration statement names.
pub const HYDRATION_CHUNK_FINGERPRINTS: usize = 25_000;

/// The envelope's charge, once per answer: `{"status":"success","data":[`
/// and `]}` are 31 bytes.
pub const ENVELOPE_CHARGE: usize = 64;

/// The reference's `MinTime` in milliseconds (`web/api/v1/api.go`): a
/// request with no `start` reads from it.
pub const ALL_TIME_START_MS: i64 = -9_223_309_901_257_974_000;

/// The reference's `MaxTime` in milliseconds: a request with no `end`
/// reads to it.
pub const ALL_TIME_END_MS: i64 = 9_223_309_901_257_974_999;

/// One series of the answer: its labels, `__name__` included, sorted by
/// name, and its exemplars in the reference's order.
#[derive(Debug, Clone, PartialEq)]
pub struct ExemplarSeries {
    pub series_labels: Vec<(String, String)>,
    pub exemplars: Vec<StoredExemplar>,
}

/// One exemplar of the answer: its labels sorted by name, `trace_id` and
/// `span_id` among them when stored, its value and its time.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredExemplar {
    pub labels: Vec<(String, String)>,
    pub value: f64,
    pub unix_milli: i64,
}

/// The width in milliseconds of the slices a thinned answer keeps one
/// exemplar from.
pub fn slice_width(window: DataWindow, lo: i64, hi: i64) -> i64 {
    let _ = (window, lo, hi);
    0
}

/// The reference's label hash.
pub fn reference_label_hash(labels: &[(String, String)]) -> u64 {
    let _ = labels;
    0
}

/// The distinct IDs of `fps`, at most [`HYDRATION_CHUNK_FINGERPRINTS`] a
/// chunk, each chunk as comma-joined decimals.
pub fn hydration_chunks(fps: &[Fingerprint]) -> Vec<String> {
    let _ = fps;
    Vec::new()
}

/// What an A or T row is charged.
pub fn exemplar_row_charge(row: &ExemplarRow) -> usize {
    let _ = row;
    0
}

/// What an H row is charged.
pub fn series_row_charge(row: &ExemplarSeriesRow) -> usize {
    let _ = row;
    0
}

/// The retention counter of one exemplar answer.
#[derive(Debug)]
pub(crate) struct AnswerBudget {
    used: usize,
    cap: usize,
}

impl AnswerBudget {
    pub(crate) fn new(cap: usize) -> Self {
        AnswerBudget { used: 0, cap }
    }

    pub(crate) fn charge(&mut self, bytes: usize) -> Result<(), ReadError> {
        let _ = (
            bytes,
            self.used,
            self.cap,
            TooBroadReason::ExemplarAnswerBytes { budget_bytes: 0 },
        );
        Ok(())
    }
}

/// An A or T row as the answer keeps it.
pub fn keep_exemplar(row: ExemplarRow) -> (Fingerprint, StoredExemplar) {
    (
        row.fingerprint,
        StoredExemplar {
            labels: Vec::new(),
            value: row.value,
            unix_milli: row.unix_milli,
        },
    )
}

/// An H row as the answer keeps it.
pub fn keep_series(row: ExemplarSeriesRow) -> (Fingerprint, Vec<(String, String)>) {
    (row.fingerprint, Vec::new())
}

/// Pairs each exemplar with its series' labels, in the reference's order.
pub fn assemble(
    rows: Vec<(Fingerprint, StoredExemplar)>,
    series: Vec<(Fingerprint, Vec<(String, String)>)>,
) -> Vec<ExemplarSeries> {
    let _ = (rows, series);
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: i64 = 1_000;
    const MIN: i64 = 60 * S;
    const HOUR: i64 = 60 * MIN;
    const DAY: i64 = 24 * HOUR;

    fn w(start_ms: i64, end_ms: i64) -> DataWindow {
        DataWindow { start_ms, end_ms }
    }

    /// U3: the width the step rule gives, from the request window, and
    /// from C's `lo`/`hi` only when a bound is absent.
    #[test]
    fn slice_width_follows_the_clients_step_rule() {
        let cases: [(&str, DataWindow, i64, i64, i64); 13] = [
            ("1 h", w(0, HOUR), 0, HOUR - 1, 100),
            ("150 s, one stored exemplar", w(0, 150 * S), 5, 5, 10),
            ("2 h", w(0, 2 * HOUR), 0, 2 * HOUR - 1, 100),
            (
                "no bounds, lo/hi an hour apart",
                w(ALL_TIME_START_MS, ALL_TIME_END_MS),
                10 * DAY,
                10 * DAY + HOUR,
                100,
            ),
            (
                "no start, lo/hi an hour apart",
                w(ALL_TIME_START_MS, 30 * DAY),
                10 * DAY,
                10 * DAY + HOUR,
                100,
            ),
            (
                "no end, lo/hi an hour apart",
                w(0, ALL_TIME_END_MS),
                10 * DAY,
                10 * DAY + HOUR,
                100,
            ),
            (
                "no start, lo/hi a day apart",
                w(ALL_TIME_START_MS, 30 * DAY),
                10 * DAY,
                11 * DAY,
                5_000,
            ),
            ("1 d, lo/hi an hour apart", w(0, DAY), 0, HOUR, 5_000),
            ("1 d", w(0, DAY), 0, DAY - 1, 5_000),
            ("2 d", w(0, 2 * DAY), 0, 2 * DAY - 1, 5_000),
            ("7 d", w(0, 7 * DAY), 0, 7 * DAY - 1, 30_000),
            ("150 s, step 3", w(0, 150 * S), 0, 150 * S - 1, 10),
            ("99 s", w(0, 99 * S), 0, 99 * S - 1, 1),
        ];
        for (name, window, lo, hi, want) in cases {
            assert_eq!(slice_width(window, lo, hi), want, "{name}");
        }
    }

    /// The client's `RoundInterval`, for U3b, written from its table
    /// apart from the constants under test.
    fn round(range: i64, divisor: i64) -> i64 {
        // `interval = range / divisor`, compared against each threshold
        // as `range <= threshold × divisor`, ms.
        let ladder: [(i64, i64); 30] = [
            (10, 1),
            (15, 10),
            (35, 20),
            (75, 50),
            (150, 100),
            (350, 200),
            (750, 500),
            (1_500, 1_000),
            (3_500, 2_000),
            (7_500, 5_000),
            (12_500, 10_000),
            (17_500, 15_000),
            (25_000, 20_000),
            (45_000, 30_000),
            (90_000, 60_000),
            (210_000, 120_000),
            (450_000, 300_000),
            (750_000, 600_000),
            (1_050_000, 900_000),
            (1_500_000, 1_200_000),
            (2_700_000, 1_800_000),
            (5_400_000, 3_600_000),
            (9_000_000, 7_200_000),
            (16_200_000, 10_800_000),
            (32_400_000, 21_600_000),
            (86_400_000, 43_200_000),
            (172_800_000, 86_400_000),
            (604_800_000, 86_400_000),
            (1_814_400_000, 604_800_000),
            (63_072_000_000, 2_592_000_000),
        ];
        ladder
            .iter()
            .find(|(t, _)| range <= t * divisor)
            .map(|(_, v)| *v)
            .unwrap_or(31_536_000_000)
    }

    /// The client's rounded values, for U3b.
    const TABLE: [i64; 30] = [
        1,
        10,
        20,
        50,
        100,
        200,
        500,
        1_000,
        2_000,
        5_000,
        10_000,
        15_000,
        20_000,
        30_000,
        60_000,
        120_000,
        300_000,
        600_000,
        900_000,
        1_200_000,
        1_800_000,
        3_600_000,
        7_200_000,
        10_800_000,
        21_600_000,
        43_200_000,
        86_400_000,
        604_800_000,
        2_592_000_000,
        31_536_000_000,
    ];

    /// U3b: the client steps the width divides, on the windows the user
    /// text names, for every client range from the request window to one
    /// step longer.
    #[test]
    fn the_client_steps_the_width_divides_are_the_documented_ones() {
        let windows = [150 * S, HOUR, 6 * HOUR, DAY, 2 * DAY, 7 * DAY, 30 * DAY];
        let min_steps = [1, 5, 10, 15, 30, 60, 120, 300, 600].map(|s| s * S);
        for window in windows {
            let width = slice_width(w(0, window), 0, window - 1);
            assert!(width > 0, "{window}: width {width}");
            let step = round(window, 11_000);
            for k in 0..=16 {
                let range = window + step * k / 16;
                let safe = round(range, 11_000);
                for t in TABLE.iter().filter(|t| **t >= safe) {
                    for factor in [1, 2, 3] {
                        assert_eq!(
                            t * factor % width,
                            0,
                            "window {window}, range {range}: {t} × {factor} against {width}"
                        );
                    }
                }
                for m in min_steps.iter().filter(|m| **m > safe) {
                    assert_eq!(
                        m % width,
                        0,
                        "window {window}: min step {m} against {width}"
                    );
                }
            }
        }
        // `$__rate_interval`: max(t + scrape, 4 × scrape) for every table
        // value t at least the safe interval.
        let rate_step_divides = |window: i64, scrape: i64| -> bool {
            let width = slice_width(w(0, window), 0, window - 1);
            let safe = round(window, 11_000);
            TABLE
                .iter()
                .filter(|t| **t >= safe)
                .all(|t| (t + scrape).max(4 * scrape) % width == 0)
        };
        for window in [HOUR, DAY, 4 * DAY] {
            assert!(rate_step_divides(window, 15 * S), "15 s scrape, {window}");
        }
        assert!(!rate_step_divides(7 * DAY, 15 * S), "15 s scrape, 7 d");
        assert!(rate_step_divides(30 * DAY, 60 * S), "60 s scrape, 30 d");
    }

    /// U3c: the hash the server's `xxHash64` gives over the same
    /// length-prefixed bytes (`concat(char(8), 'trace_id', char(32),
    /// '…')`, computed on the development server).
    #[test]
    fn reference_label_hash_matches_the_server_computation() {
        let one = |v: &str| vec![("trace_id".to_string(), v.to_string())];
        assert_eq!(
            reference_label_hash(&one("0102030405060708090a0b0c0d0e0f10")),
            11_691_705_299_467_561_892
        );
        assert_eq!(
            reference_label_hash(&one("0102030405060708090a0b0c0d0e0f11")),
            10_803_734_624_230_284_826
        );
    }

    /// U2c: 60,001 distinct IDs, some given twice, are three chunks of
    /// 25,000, 25,000 and 1, each the comma-joined decimals of its IDs —
    /// exact above `2^64`.
    #[test]
    fn hydration_chunks_hold_25000_fingerprints() {
        let base: u128 = 1 << 64;
        let mut fps: Vec<Fingerprint> = (0..60_001u128)
            .map(|i| Fingerprint::from_raw(base + i * 7))
            .collect();
        fps.extend((0..100u128).map(|i| Fingerprint::from_raw(base + i * 7)));
        let chunks = hydration_chunks(&fps);
        let sizes: Vec<usize> = chunks.iter().map(|c| c.split(',').count()).collect();
        assert_eq!(sizes, vec![25_000, 25_000, 1]);
        let mut back: Vec<u128> = chunks
            .iter()
            .flat_map(|c| c.split(','))
            .map(|d| d.parse::<u128>().expect("a decimal ID"))
            .collect();
        back.sort_unstable();
        let want: Vec<u128> = (0..60_001u128).map(|i| base + i * 7).collect();
        assert_eq!(back, want);
    }

    fn row(fp: u128, ms: i64, v: f64, trace: Option<u8>, span: Option<u8>) -> ExemplarRow {
        let mut trace_id = [0u8; 16];
        if let Some(last) = trace {
            for (i, b) in trace_id.iter_mut().enumerate() {
                *b = i as u8 + 1;
            }
            trace_id[15] = last;
        }
        let mut span_id = [0u8; 8];
        if let Some(last) = span {
            span_id = [0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x07, last];
        }
        ExemplarRow {
            fingerprint: Fingerprint::from_raw(fp),
            unix_milli: ms,
            value: v,
            trace_id,
            span_id,
            labels: "{}".to_string(),
        }
    }

    /// U3d: a row whose charge would pass the cap is refused, and the
    /// refusal keeps nothing; rows whose charges reach the cap exactly are
    /// accepted.
    #[tokio::test]
    async fn the_answer_budget_refuses_past_its_cap() {
        let rows = || {
            futures::stream::iter(
                (0..3)
                    .map(|i| Ok(row(1, i, 1.0, None, None)))
                    .collect::<Vec<Result<ExemplarRow, pulsus_clickhouse::ChError>>>(),
            )
        };
        let mut budget = AnswerBudget::new(299);
        let refused = super::super::dispatch::drain_charged_bytes(
            rows(),
            &mut budget,
            |_: &ExemplarRow| 100,
            keep_exemplar,
            1 << 30,
        )
        .await;
        match refused {
            Err(ReadError::QueryTooBroad(TooBroadReason::ExemplarAnswerBytes { budget_bytes })) => {
                assert_eq!(budget_bytes, 299)
            }
            other => panic!("expected the budget's refusal, got {other:?}"),
        }
        let mut budget = AnswerBudget::new(300);
        let kept = super::super::dispatch::drain_charged_bytes(
            rows(),
            &mut budget,
            |_: &ExemplarRow| 100,
            keep_exemplar,
            1 << 30,
        )
        .await
        .expect("three rows at the cap are accepted");
        assert_eq!(kept.len(), 3);
        assert!(
            budget.charge(1).is_err(),
            "the accepted rows hold the whole cap"
        );
    }

    fn series_row(fp: u128, labels: &str) -> ExemplarSeriesRow {
        ExemplarSeriesRow {
            fingerprint: Fingerprint::from_raw(fp),
            series_name: "m".to_string(),
            series_labels: labels.to_string(),
        }
    }

    fn pairs(p: &[(&str, &str)]) -> Vec<(String, String)> {
        p.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// U4: series by their sorted labels; a series' exemplars by time,
    /// then value, then the reference's label hash; the ids as lowercase
    /// hex labels only when stored.
    ///
    /// The rows arrive in an order no rule but the right one sorts back:
    /// the later time first, the higher value first, and of the two equal
    /// in time and value the one whose labels sort first by text — the
    /// hash puts `…0f11` first, the text puts `…0f10` first.
    #[test]
    fn the_answer_is_assembled_in_the_reference_order() {
        let rows = vec![
            row(1, 3_000, 0.5, None, None),
            row(1, 2_000, 3.0, Some(0x10), None),
            row(1, 2_000, 3.0, Some(0x11), None),
            row(1, 1_000, 2.0, None, None),
            row(1, 1_000, 1.0, Some(0x10), Some(0x08)),
            row(2, 1_000, 4.0, None, None),
            row(3, 5_000, 5.0, None, None),
        ];
        let series = vec![
            series_row(1, r#"{"a":"Z"}"#),
            series_row(2, r#"{"a":"a"}"#),
            series_row(3, r#"{"A":"x"}"#),
        ];
        let answer = assemble(
            rows.into_iter().map(keep_exemplar).collect(),
            series.into_iter().map(keep_series).collect(),
        );
        let trace = |last: &str| format!("0102030405060708090a0b0c0d0e0f{last}");
        let ex = |labels: Vec<(String, String)>, value: f64, unix_milli: i64| StoredExemplar {
            labels,
            value,
            unix_milli,
        };
        assert_eq!(
            answer,
            vec![
                ExemplarSeries {
                    series_labels: pairs(&[("A", "x"), ("__name__", "m")]),
                    exemplars: vec![ex(vec![], 5.0, 5_000)],
                },
                ExemplarSeries {
                    series_labels: pairs(&[("__name__", "m"), ("a", "Z")]),
                    exemplars: vec![
                        ex(
                            pairs(&[("span_id", "a1b2c3d4e5f60708"), ("trace_id", &trace("10"))]),
                            1.0,
                            1_000
                        ),
                        ex(vec![], 2.0, 1_000),
                        ex(pairs(&[("trace_id", &trace("11"))]), 3.0, 2_000),
                        ex(pairs(&[("trace_id", &trace("10"))]), 3.0, 2_000),
                        ex(vec![], 0.5, 3_000),
                    ],
                },
                ExemplarSeries {
                    series_labels: pairs(&[("__name__", "m"), ("a", "a")]),
                    exemplars: vec![ex(vec![], 4.0, 1_000)],
                },
            ]
        );
    }
}
