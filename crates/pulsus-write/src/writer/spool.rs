//! `SpoolWriter`: dumps a poison or insert-uncertain batch to
//! `{spool_dir}/{poison|uncertain}/{table}/{ns}-{seq}.json` (task-manager
//! resolution, issue #9). Every write is atomic (a `.tmp` sibling, then
//! `rename` — a reader never observes a partial file) and carries the
//! batch's rows plus the classified error as one `serde_json` document, so
//! a human auditing a spooled file has everything needed in one place.
//!
//! **Bit-exact float fidelity (issue #26 review fix):** rows are encoded
//! via [`SpoolEncode`], not a bare `#[derive(Serialize)]` on the row type —
//! plain `serde_json` silently collapses a non-finite `f64`
//! (`Number::from_f64(NaN | Inf) -> None -> Value::Null`), which would
//! destroy a stale-NaN marker's exact bit pattern
//! (`writer::rows::MetricSampleRow`'s `0x7FF0000000000002` hazard,
//! docs/schemas.md's Gorilla-codec note) the moment it hits a poisoned or
//! insert-uncertain batch. `SpoolEncode` is a distinct trait from the row's
//! real `Serialize` impl (which drives ClickHouse RowBinary wire encoding
//! and must stay untouched): every row shape implements it explicitly (see
//! `writer::rows`), and `MetricSampleRow`'s implementation always emits a
//! `value_bits` field — the raw `f64::to_bits()`, encoded as a JSON
//! **STRING** of the decimal `u64` (e.g. `"9218868437227405314"`), not a
//! bare JSON number: `0x7FF0000000000002` exceeds `2^53`, so any
//! double-based JSON consumer (JavaScript's `JSON.parse`, `jq` arithmetic
//! by default, ...) would silently round a bare-integer `value_bits` to
//! the nearest representable double, defeating the point of the field — a
//! replay/audit tool must parse `value_bits` as a string and then as an
//! integer to reconstruct the exact bits; `value` (a plain JSON number, or
//! `null` when the original was non-finite) stays a best-effort
//! human-readable float.
//!
//! `uncertain/` gets a `README` on first use in this process, stating the
//! audit-only, never-auto-replayed semantics (task-manager resolution).
//! `poison/` needs no such note — nothing about a poison batch is
//! ambiguous (it failed deterministically, or exhausted its retry
//! budget); `uncertain/`'s danger is that a human might *assume* replay is
//! safe, which is exactly the mistake this crate exists to prevent
//! (docs/schemas.md §2.2/§8).

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde::Serialize;
use tokio::fs;

/// Bridges a row's real wire-format `Serialize` (RowBinary, ClickHouse
/// inserts) to the distinct *audit-file* JSON encoding
/// [`SpoolWriter::write`] dumps to disk — see this module's doc comment for
/// why these must be two separate encodings. Deliberately not a blanket
/// `impl<T: Serialize>` (a `MetricSampleRow`-specific override would
/// conflict with it without specialization, which stable Rust does not
/// have): every row shape ever passed to `SpoolWriter::write` implements
/// this explicitly (`writer::rows`). The default shape for a row with no
/// non-finite-float hazard is just `serde_json::to_value(self)`.
pub(crate) trait SpoolEncode {
    fn to_spool_value(&self) -> serde_json::Value;
}

/// The two counters [`SpoolWriter::write`] bumps on success — implemented
/// by every writer core's metrics struct (`WriterMetrics` for
/// `LogWriter`, `MetricWriterMetrics` for `MetricWriter`, issue #26) so one
/// `SpoolWriter` implementation serves every table-buffer-based writer
/// without duplicating the atomic-file-dump logic (architect plan: "no new
/// spool machinery" — this is the minimal parameterization letting a
/// second writer core reuse the existing one, not new spooling behavior).
pub(crate) trait SpoolCounters: Send + Sync {
    fn spool_poison_total(&self) -> &AtomicU64;
    fn spool_uncertain_total(&self) -> &AtomicU64;
}

/// Which spool subdirectory a batch is dumped to — deliberately not the
/// same type as [`crate::writer::WriteError`]: a spool write's own
/// success/failure is orthogonal to why the *insert* it is recording
/// failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpoolKind {
    Poison,
    Uncertain,
}

impl SpoolKind {
    fn dir_name(self) -> &'static str {
        match self {
            SpoolKind::Poison => "poison",
            SpoolKind::Uncertain => "uncertain",
        }
    }
}

/// States the `uncertain/` directory's audit-only, never-auto-replayed
/// semantics in place, next to the data (task-manager resolution).
const UNCERTAIN_README: &str = "\
This directory holds log/metric insert batches whose ClickHouse commit fate
is UNKNOWN (pulsus_clickhouse::ChError::InsertUncertain): the insert was
aborted by a timeout or a transient transport fault after it was already in
flight, so the server may have partially applied it.

AUDIT-ONLY. This directory's contents are NEVER automatically replayed.
Replaying a partially-committed block would duplicate rows and permanently
inflate materialized-view aggregates (docs/schemas.md sections 2.2 and 8).
A human must inspect each file and decide, per batch, whether a manual
replay is safe.

Schema note (metric_samples rows only): each row carries both a
human-readable `value` field (a plain JSON number, or `null` when the
original float was NaN/Infinity — a JSON representability limit) and a
`value_bits` field, ALWAYS PRESENT and ALWAYS A JSON STRING (e.g.
\"9218868437227405314\"), never a bare number: it holds the raw IEEE-754
bit pattern as a decimal-encoded unsigned 64-bit integer. A stale-NaN
marker or +-Infinity is NOT representable as a JSON number and would
otherwise be silently lost as `null`; a bare-integer encoding would fare no
better, since 0x7FF0000000000002 and similar bit patterns exceed 2^53 and
would silently round under any double-based JSON parser (JavaScript's
JSON.parse, jq arithmetic by default, ...). A replay/audit tool must parse
`value_bits` as a STRING, then as an integer, to reconstruct the exact
original value.
";

#[derive(Serialize)]
struct SpoolRecord<'a> {
    table: &'a str,
    error: &'a str,
    spooled_at_ns: i128,
    rows: Vec<serde_json::Value>,
}

pub struct SpoolWriter {
    root: PathBuf,
    metrics: Arc<dyn SpoolCounters>,
    uncertain_readme_written: AtomicBool,
    next_seq: AtomicU64,
}

impl SpoolWriter {
    pub fn new(root: PathBuf, metrics: Arc<dyn SpoolCounters>) -> Self {
        SpoolWriter {
            root,
            metrics,
            uncertain_readme_written: AtomicBool::new(false),
            next_seq: AtomicU64::new(0),
        }
    }

    /// Writes `rows` (with `error`'s message) to `kind`'s subdirectory for
    /// `table`. Bumps the matching `spool_total{poison,uncertain}` counter
    /// on success. Never called from a caller that treats its own failure
    /// as fatal to the batch's settlement — the batch is already gone from
    /// memory either way; a spool I/O failure is logged by the caller
    /// (`writer::table`), never silently swallowed.
    pub async fn write<R: SpoolEncode>(
        &self,
        kind: SpoolKind,
        table: &str,
        rows: &[R],
        error: &str,
    ) -> std::io::Result<()> {
        let dir = self.root.join(kind.dir_name()).join(table);
        fs::create_dir_all(&dir).await?;

        if kind == SpoolKind::Uncertain {
            self.ensure_uncertain_readme().await?;
        }

        let record = SpoolRecord {
            table,
            error,
            spooled_at_ns: now_unix_nanos(),
            rows: rows.iter().map(SpoolEncode::to_spool_value).collect(),
        };
        let body = serde_json::to_vec(&record)
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;

        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!("{}-{seq}.json", record.spooled_at_ns));
        write_atomic(&path, &body).await?;

        match kind {
            SpoolKind::Poison => self
                .metrics
                .spool_poison_total()
                .fetch_add(1, Ordering::Relaxed),
            SpoolKind::Uncertain => self
                .metrics
                .spool_uncertain_total()
                .fetch_add(1, Ordering::Relaxed),
        };
        Ok(())
    }

    async fn ensure_uncertain_readme(&self) -> std::io::Result<()> {
        if self.uncertain_readme_written.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let dir = self.root.join(SpoolKind::Uncertain.dir_name());
        fs::create_dir_all(&dir).await?;
        let readme_path = dir.join("README");
        // Best-effort: an existing README (e.g. from a prior process) is
        // left untouched rather than overwritten.
        match fs::metadata(&readme_path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                write_atomic(&readme_path, UNCERTAIN_README.as_bytes()).await
            }
            Err(e) => Err(e),
        }
    }
}

/// Atomic write: full contents to a `.tmp` sibling, then `rename` — a
/// reader never observes a partially-written spool file (crash-safe on
/// any POSIX filesystem where rename is atomic within one directory).
async fn write_atomic(path: &Path, body: &[u8]) -> std::io::Result<()> {
    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, body).await?;
    fs::rename(&tmp_path, path).await
}

fn now_unix_nanos() -> i128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i128)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use pulsus_model::STALE_NAN_BITS;

    use super::*;
    use crate::writer::metrics::WriterMetrics;
    use crate::writer::rows::MetricSampleRow;

    #[derive(Serialize)]
    struct Row {
        value: u64,
    }

    impl SpoolEncode for Row {
        fn to_spool_value(&self) -> serde_json::Value {
            serde_json::to_value(self).expect("Row contains no non-finite floats")
        }
    }

    #[tokio::test]
    async fn write_creates_a_readable_json_file_under_the_table_directory() {
        let dir = tempdir();
        let metrics = Arc::new(WriterMetrics::default());
        let spool = SpoolWriter::new(dir.clone(), metrics.clone());
        spool
            .write(
                SpoolKind::Poison,
                "log_samples",
                &[Row { value: 1 }, Row { value: 2 }],
                "boom",
            )
            .await
            .expect("spool write succeeds");

        let table_dir = dir.join("poison").join("log_samples");
        let mut entries = std::fs::read_dir(&table_dir)
            .expect("table dir exists")
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 1);
        let contents = std::fs::read_to_string(entries.remove(0).path()).unwrap();
        assert!(contents.contains("\"error\":\"boom\""));
        assert!(contents.contains("\"value\":1"));
        assert_eq!(metrics.spool_poison_total.load(Ordering::Relaxed), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn uncertain_write_creates_a_readme_exactly_once() {
        let dir = tempdir();
        let metrics = Arc::new(WriterMetrics::default());
        let spool = SpoolWriter::new(dir.clone(), metrics.clone());
        spool
            .write(
                SpoolKind::Uncertain,
                "log_streams",
                &[Row { value: 1 }],
                "e1",
            )
            .await
            .unwrap();
        spool
            .write(
                SpoolKind::Uncertain,
                "log_streams",
                &[Row { value: 2 }],
                "e2",
            )
            .await
            .unwrap();

        let readme = dir.join("uncertain").join("README");
        let contents = std::fs::read_to_string(&readme).unwrap();
        assert!(contents.contains("AUDIT-ONLY"));
        assert!(contents.contains("NEVER"));
        assert_eq!(metrics.spool_uncertain_total.load(Ordering::Relaxed), 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pulsus-write-spool-test-{}-{}",
            std::process::id(),
            now_unix_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Load-bearing regression test (issue #26 code review fix): a
    /// `MetricSampleRow` carrying the stale-NaN marker
    /// `0x7FF0000000000002`, routed through the actual spool `write` path
    /// (`InsertUncertain`'s classification), must round-trip its exact bit
    /// pattern from the written audit file — proving `SpoolEncode` closes
    /// the hazard a bare `serde_json::to_vec(&row)` would have silently
    /// collapsed to `null`.
    #[tokio::test]
    async fn stale_nan_metric_sample_round_trips_its_exact_bits_through_the_spool_file() {
        let dir = tempdir();
        let metrics = Arc::new(WriterMetrics::default());
        let spool = SpoolWriter::new(dir.clone(), metrics);

        let row = MetricSampleRow {
            metric_name: "up".to_string(),
            fingerprint: 1,
            unix_milli: 0,
            value: f64::from_bits(STALE_NAN_BITS),
        };
        spool
            .write(SpoolKind::Uncertain, "metric_samples", &[row], "boom")
            .await
            .expect("spool write succeeds");

        let table_dir = dir.join("uncertain").join("metric_samples");
        let mut entries = std::fs::read_dir(&table_dir)
            .expect("table dir exists")
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 1);
        let contents = std::fs::read_to_string(entries.remove(0).path()).unwrap();

        // Assert against the raw file TEXT (issue #26 second review-cycle
        // fix), not via `serde_json::Value` numeric handling: the whole
        // point of encoding `value_bits` as a JSON *string* is that no
        // JSON number parser (this test's own `serde_json::Value` included)
        // ever gets a chance to round it — so the test proves the string
        // shape is actually on disk, not merely that some deserializer
        // happens to reconstruct the right value.
        let expected_field = format!("\"value_bits\":\"{STALE_NAN_BITS}\"");
        assert!(
            contents.contains(&expected_field),
            "expected the quoted decimal string {expected_field:?} verbatim in the spool file, got: {contents}"
        );
        // Guard against ever regressing back to a bare (unquoted, hence
        // f64-roundable) JSON integer for this field.
        let bare_number_field = format!("\"value_bits\":{STALE_NAN_BITS}");
        assert!(
            !contents.contains(&bare_number_field),
            "value_bits must never be a bare JSON number (2^53 precision hazard): {contents}"
        );
        assert!(
            contents.contains("\"value\":null"),
            "the plain 'value' field is JSON's own null for a non-finite float \
             (readable-but-lossy, by design) — value_bits is the source of truth: {contents}"
        );

        // Also parse it, as a belt-and-braces check that the file is valid
        // JSON and the string decodes back to the exact bit pattern.
        let parsed: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");
        let spooled_bits: u64 = parsed["rows"][0]["value_bits"]
            .as_str()
            .expect("value_bits is a JSON string")
            .parse()
            .expect("value_bits parses as a u64");
        assert_eq!(spooled_bits, STALE_NAN_BITS);

        std::fs::remove_dir_all(&dir).ok();
    }
}
