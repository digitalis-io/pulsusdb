//! `SpoolWriter`: dumps a poison or insert-uncertain batch to
//! `{spool_dir}/{poison|uncertain}/{table}/{ns}-{seq}.json` (task-manager
//! resolution, issue #9). Every write is atomic (a `.tmp` sibling, then
//! `rename` — a reader never observes a partial file) and carries the
//! batch's rows plus the classified error as one `serde_json` document, so
//! a human auditing a spooled file has everything needed in one place.
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

use crate::writer::metrics::WriterMetrics;

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
This directory holds log_samples/log_streams insert batches whose ClickHouse
commit fate is UNKNOWN (pulsus_clickhouse::ChError::InsertUncertain): the
insert was aborted by a timeout or a transient transport fault after it was
already in flight, so the server may have partially applied it.

AUDIT-ONLY. This directory's contents are NEVER automatically replayed.
Replaying a partially-committed block would duplicate rows and permanently
inflate materialized-view aggregates (docs/schemas.md sections 2.2 and 8).
A human must inspect each file and decide, per batch, whether a manual
replay is safe.
";

#[derive(Serialize)]
struct SpoolRecord<'a, R> {
    table: &'a str,
    error: &'a str,
    spooled_at_ns: i128,
    rows: &'a [R],
}

pub struct SpoolWriter {
    root: PathBuf,
    metrics: Arc<WriterMetrics>,
    uncertain_readme_written: AtomicBool,
    next_seq: AtomicU64,
}

impl SpoolWriter {
    pub fn new(root: PathBuf, metrics: Arc<WriterMetrics>) -> Self {
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
    pub async fn write<R: Serialize>(
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
            rows,
        };
        let body = serde_json::to_vec(&record)
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;

        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!("{}-{seq}.json", record.spooled_at_ns));
        write_atomic(&path, &body).await?;

        match kind {
            SpoolKind::Poison => self
                .metrics
                .spool_poison_total
                .fetch_add(1, Ordering::Relaxed),
            SpoolKind::Uncertain => self
                .metrics
                .spool_uncertain_total
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

    use super::*;

    #[derive(Serialize)]
    struct Row {
        value: u64,
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
}
