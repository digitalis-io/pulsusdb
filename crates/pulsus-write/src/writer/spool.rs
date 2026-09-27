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
use tokio::io::AsyncWriteExt;

/// Bridges a row's real wire-format `Serialize` (RowBinary, ClickHouse
/// inserts) to the distinct *audit-file* JSON encoding
/// [`SpoolWriter::write`] dumps to disk — see this module's doc comment for
/// why these must be two separate encodings. Deliberately not a blanket
/// `impl<T: Serialize>` (a `MetricSampleRow`-specific override would
/// conflict with it without specialization, which stable Rust does not
/// have): every row shape ever passed to `SpoolWriter::write` implements
/// this explicitly (`writer::rows`). The default shape for a row with no
/// non-finite-float hazard is just `serde_json::to_value(self)`.
pub(crate) trait SpoolEncode: Sync {
    fn to_spool_value(&self) -> serde_json::Value;

    /// The same shape, written **into the sink** rather than returned
    /// (issue #603 code review round 8, finding 2).
    ///
    /// The default materialises [`Self::to_spool_value`] first: one row's
    /// whole value tree, and then its text in the chunk. `MetricLandingRow`
    /// overrides it and writes its fields and arrays element by element,
    /// because an accepted native histogram may carry 65,536 custom bucket
    /// bounds in one row, and the queue reservation held while the file is
    /// written covers that block's rows alone.
    ///
    /// **Scope, so the default is not read as a statement about size.** The
    /// nine shipped row shapes keep it: the log rows, the trace rows and the
    /// per-target metric rows. Their spooling is the log and trace writers'
    /// own path, which this change does not touch and whose peak is not
    /// measured by the case behind this method
    /// (`crates/pulsus-write/tests/spool_stream_alloc.rs`, the metrics
    /// landing queue's bound).
    fn write_spool_json(
        &self,
        out: &mut SpoolSink,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async move { out.put_json(&self.to_spool_value()).await }
    }
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

/// What one spool file holds — the declared shape [`write_record`] writes by
/// hand, field for field and in order.
///
/// **It exists for the case that holds the two together**
/// (`the_streamed_document_is_what_serializing_the_record_whole_would_write`)
/// and is compiled only for the tests: building one materialises every row's
/// value at once, which is the allocation `write_record` exists to avoid, so
/// production must not have it to hand.
#[cfg(test)]
#[derive(Serialize)]
struct SpoolRecord<'a> {
    table: &'a str,
    error: &'a str,
    spooled_at_ns: i128,
    rows: Vec<serde_json::Value>,
}

/// How much of the document [`SpoolSink`] holds before it writes what it
/// has. One `write_all` per chunk, so the file costs one blocking-pool
/// round trip per this many bytes rather than one per row.
const SPOOL_CHUNK_BYTES: usize = 64 * 1024;

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

        let spooled_at_ns = now_unix_nanos();
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!("{spooled_at_ns}-{seq}.json"));
        write_record(&path, table, error, spooled_at_ns, rows).await?;

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

/// A file being written in bounded pieces: what is held in memory at any
/// instant is one chunk plus whatever the last piece serialized into it.
pub(crate) struct SpoolSink {
    file: fs::File,
    buf: Vec<u8>,
}

impl SpoolSink {
    fn new(file: fs::File) -> Self {
        SpoolSink {
            file,
            buf: Vec::with_capacity(SPOOL_CHUNK_BYTES),
        }
    }

    /// Appends bytes of the document that need no encoding — the punctuation
    /// and the field names.
    async fn put(&mut self, piece: &[u8]) -> std::io::Result<()> {
        self.buf.extend_from_slice(piece);
        self.spill().await
    }

    /// Appends one value as JSON, serialized straight into the chunk rather
    /// than into a buffer of its own.
    async fn put_json<T: Serialize + ?Sized>(&mut self, value: &T) -> std::io::Result<()> {
        serde_json::to_writer(&mut self.buf, value)
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
        self.spill().await
    }

    /// Opens a JSON object, to be written field by field and closed with
    /// [`SpoolObject::end`] — how a row whose arrays are as long as the push
    /// chooses is encoded without ever holding one whole (issue #603 code
    /// review round 8, finding 2).
    pub(crate) async fn begin_object(&mut self) -> std::io::Result<SpoolObject<'_>> {
        self.put(b"{").await?;
        Ok(SpoolObject {
            sink: self,
            first: true,
        })
    }

    /// Writes the chunk out once it is full. One piece can carry the buffer
    /// past the chunk size before this is reached, so what it holds is the
    /// chunk plus at most one piece — never the whole document, and never a
    /// whole array of a row that writes its elements one at a time.
    async fn spill(&mut self) -> std::io::Result<()> {
        if self.buf.len() < SPOOL_CHUNK_BYTES {
            return Ok(());
        }
        self.flush().await
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.file.write_all(&self.buf).await?;
        self.buf.clear();
        // A piece larger than one chunk grew this; give the memory back
        // rather than keeping the high-water mark for the rest of the
        // document. A no-op while the capacity is already the chunk size.
        self.buf.shrink_to(SPOOL_CHUNK_BYTES);
        Ok(())
    }
}

/// One JSON object being written into a [`SpoolSink`]. Each field is written
/// as it is named, so nothing accumulates: the comma before every field but
/// the first is this type's whole state.
pub(crate) struct SpoolObject<'a> {
    sink: &'a mut SpoolSink,
    first: bool,
}

impl SpoolObject<'_> {
    /// `"name":<value>`, the value serialized straight into the chunk.
    pub(crate) async fn field<T: Serialize + ?Sized>(
        &mut self,
        name: &str,
        value: &T,
    ) -> std::io::Result<()> {
        self.key(name).await?;
        self.sink.put_json(value).await
    }

    /// `"name":[…]`, **one element at a time**, so an array of any length
    /// holds one element's text and not its own.
    pub(crate) async fn array<T: Serialize>(
        &mut self,
        name: &str,
        items: impl IntoIterator<Item = T>,
    ) -> std::io::Result<()> {
        self.key(name).await?;
        self.sink.put(b"[").await?;
        for (i, item) in items.into_iter().enumerate() {
            if i > 0 {
                self.sink.put(b",").await?;
            }
            self.sink.put_json(&item).await?;
        }
        self.sink.put(b"]").await
    }

    /// Closes the object.
    pub(crate) async fn end(self) -> std::io::Result<()> {
        self.sink.put(b"}").await
    }

    /// The comma, then the quoted field name and its colon. The name goes
    /// through `serde_json` like any other string, so it is escaped exactly as
    /// a derived `Serialize` would escape it.
    async fn key(&mut self, name: &str) -> std::io::Result<()> {
        if !self.first {
            self.sink.put(b",").await?;
        }
        self.first = false;
        self.sink.put_json(name).await?;
        self.sink.put(b":").await
    }
}

/// Writes one spool document — the `SpoolRecord` shape, field for field — to
/// a `.tmp` sibling and renames it into place.
///
/// **It is streamed, not built** (issue #603 code review round 7, finding 3).
/// A failed block's queue reservation covers its rows and is held until this
/// returns, so anything this holds on top of them is memory the ingest byte
/// bound does not know about: an encoder that made a value per row and then
/// the whole serialized body would put two further copies of the push beside
/// the rows, and enough blocks failing at once would exceed
/// `PULSUS_INGEST_QUEUE_BYTES` by a multiple of the data admitted. What this
/// holds instead is one chunk and one piece of one row — a row that writes
/// its fields and arrays straight into the sink never has a value or a text
/// of its own at all ([`SpoolEncode::write_spool_json`]).
///
/// The rename is what makes the file atomic to a reader — it never observes a
/// partially written spool file — exactly as [`write_atomic`] does for the
/// README.
async fn write_record<R: SpoolEncode>(
    path: &Path,
    table: &str,
    error: &str,
    spooled_at_ns: i128,
    rows: &[R],
) -> std::io::Result<()> {
    let tmp_path = path.with_extension("tmp");
    let mut out = SpoolSink::new(fs::File::create(&tmp_path).await?);
    out.put(b"{\"table\":").await?;
    out.put_json(table).await?;
    out.put(b",\"error\":").await?;
    out.put_json(error).await?;
    out.put(b",\"spooled_at_ns\":").await?;
    out.put_json(&spooled_at_ns).await?;
    out.put(b",\"rows\":[").await?;
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            out.put(b",").await?;
        }
        row.write_spool_json(&mut out).await?;
    }
    out.put(b"]}").await?;
    out.flush().await?;
    fs::rename(&tmp_path, path).await
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
    use pulsus_model::Fingerprint;
    use serde::Serialize;

    use pulsus_model::STALE_NAN_BITS;

    use super::*;
    use crate::writer::metrics::WriterMetrics;
    use crate::writer::rows::{MetricLandingRow, MetricSampleRow};

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

    /// **The streamed document is byte for byte what serializing the record
    /// whole would have produced.** `write_record` writes the shape by hand
    /// so that it never holds the whole of it, and the shape it writes is
    /// [`SpoolRecord`]'s; nothing else keeps the two together, and a spool
    /// file whose shape drifted is an audit trail no replay tool can read.
    ///
    /// The fixture crosses the chunk boundary several times over and the
    /// strings carry the escaping serde owns, so the flushes fall inside
    /// values and between them rather than only at the end.
    #[tokio::test]
    async fn the_streamed_document_is_what_serializing_the_record_whole_would_write() {
        let dir = tempdir();
        let rows: Vec<Row> = (0..30_000).map(|value| Row { value }).collect();
        let table = "log_\"samples\"";
        let error = "boom:\n\tone \"quoted\" \\ thing";
        let path = dir.join("streamed.json");

        write_record(&path, table, error, 1_700_000_000_123_456_789, &rows)
            .await
            .expect("the document is written");

        let streamed = std::fs::read(&path).expect("read the document back");
        assert!(
            streamed.len() > 4 * SPOOL_CHUNK_BYTES,
            "the fixture must be several chunks long, not {} bytes",
            streamed.len()
        );
        let whole = serde_json::to_vec(&SpoolRecord {
            table,
            error,
            spooled_at_ns: 1_700_000_000_123_456_789,
            rows: rows.iter().map(SpoolEncode::to_spool_value).collect(),
        })
        .expect("the record serializes");
        assert_eq!(
            String::from_utf8(streamed).expect("the document is UTF-8"),
            String::from_utf8(whole).expect("the record is UTF-8"),
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// **Each landing row kind streams the shape that kind declares.** A row
    /// whose arrays are written element by element into the sink (issue #603
    /// code review round 8, finding 2) must put the same document on disk as
    /// the declared [`SpoolEncode::to_spool_value`] shape every other case in
    /// `writer::rows` asserts against — the audit record a replay tool reads.
    /// Nothing else holds the two together once the encoder stops going
    /// through a value.
    ///
    /// All four kinds, and the histogram twice: once with no custom bounds and
    /// once with the widest array the ingest seam admits, so the
    /// element-at-a-time path is what the comparison covers rather than the
    /// empty case.
    #[tokio::test]
    async fn every_landing_row_kind_streams_the_shape_it_declares() {
        let dir = tempdir();
        for (name, row) in landing_rows_of_every_kind() {
            let path = dir.join(format!("{name}.json"));
            let whole = serde_json::to_vec(&SpoolRecord {
                table: "metric_landing",
                error: "boom",
                spooled_at_ns: 1_700_000_000_123_456_789,
                rows: vec![row.to_spool_value()],
            })
            .expect("the record serializes");
            write_record(
                &path,
                "metric_landing",
                "boom",
                1_700_000_000_123_456_789,
                &[row],
            )
            .await
            .expect("the document is written");
            let streamed = std::fs::read(&path).expect("read the document back");
            assert_eq!(
                String::from_utf8(streamed).expect("the document is UTF-8"),
                String::from_utf8(whole).expect("the record is UTF-8"),
                "the streamed {name} row is not the shape it declares"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// One row of each landing kind, the histogram twice: an empty-array one
    /// and one carrying `MAX_BUCKETS_PER_HISTOGRAM_SIDE` custom bounds, which
    /// is the widest single row the ingest seam admits.
    fn landing_rows_of_every_kind() -> Vec<(&'static str, MetricLandingRow)> {
        use crate::ingest::metrics::{HistogramPoint, MetricMetadata, MetricPoint, SeriesRef};
        use pulsus_model::{CUSTOM_BUCKETS_SCHEMA, CounterResetHint, NativeHistogram, Span};

        let (labels, _) =
            pulsus_model::LabelSet::from_normalized([("job".to_string(), "checkout".to_string())]);
        let point = MetricPoint {
            metric_name: Arc::from("http_requests_total"),
            fingerprint: Fingerprint::from_raw(7),
            unix_milli: 1_000,
            value: f64::from_bits(STALE_NAN_BITS),
        };
        let series = SeriesRef {
            metric_name: Arc::from("http_requests_total"),
            fingerprint: Fingerprint::from_raw(7),
            labels,
        };
        let meta = MetricMetadata {
            metric_name: Arc::from("http_requests_total"),
            metric_type: "counter".to_string(),
            help: "requests \"served\"\n".to_string(),
            unit: "1".to_string(),
            updated_ns: 42,
        };
        let hist = |bounds: usize| HistogramPoint {
            metric_name: Arc::from("http_request_duration_seconds"),
            fingerprint: Fingerprint::from_raw(11),
            unix_milli: 2_000,
            histogram: NativeHistogram {
                counter_reset_hint: CounterResetHint::Unknown,
                schema: CUSTOM_BUCKETS_SCHEMA,
                zero_threshold: 0.0,
                zero_count: 0,
                count: 3,
                sum: f64::INFINITY,
                positive_spans: vec![Span {
                    offset: 0,
                    length: 2,
                }],
                negative_spans: Vec::new(),
                positive_buckets: vec![1, 1],
                negative_buckets: Vec::new(),
                custom_values: (0..bounds).map(|i| 0.5 + i as f64 / 1024.0).collect(),
            },
        };
        vec![
            ("float", MetricLandingRow::float_sample(5, &point)),
            ("hist-empty", MetricLandingRow::hist_sample(5, &hist(0))),
            ("hist-wide", MetricLandingRow::hist_sample(5, &hist(65_536))),
            ("series", MetricLandingRow::series(5, &series, 3_600_000, 1)),
            ("metadata", MetricLandingRow::metadata(5, &meta)),
        ]
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
            fingerprint: Fingerprint::from_raw(1),
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
