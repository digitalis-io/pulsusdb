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

use pulsus_model::Fingerprint;
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
    /// The default materialises [`Self::to_spool_value`] first — one row's
    /// whole value tree — and then walks it into the sink, which holds one
    /// chunk whatever the value's size ([`SpoolSink::put_value`]). **Both
    /// landing rows override it** and write their fields, arrays and text
    /// straight into the sink, so neither has a value tree of its own: an
    /// accepted native histogram may carry 65,536 custom bucket bounds in one
    /// metrics row, and one logs row carries a log line of any length admission
    /// accepts. The queue reservation held while the file is written covers
    /// that block's rows alone, so a copy beside them is memory
    /// `PULSUS_INGEST_QUEUE_BYTES` never knew about.
    ///
    /// **What the default still costs, so it is not read as a statement about
    /// size.** The per-target row shapes keep it: the trace rows and the
    /// per-target log and metric rows. Their value tree is a copy of the row,
    /// held while the row is written, and nothing here removes it; what it no
    /// longer holds is the serialized text on top of that tree. Only the two
    /// landing queues' bounds are measured, by
    /// `crates/pulsus-write/tests/spool_stream_alloc.rs`.
    fn write_spool_json(
        &self,
        out: &mut SpoolSink,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async move { out.put_value(&self.to_spool_value()).await }
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
///
/// Public because the case that measures what spooling one block costs is an
/// integration test, and a field whose encoded text crosses this boundary is
/// the thing it has to build — a literal there would be this figure said
/// twice (`crates/pulsus-write/tests/spool_stream_alloc.rs`).
pub const SPOOL_CHUNK_BYTES: usize = 64 * 1024;

/// How much of a string [`SpoolSink::put_str`] escapes at a time. Any value
/// from 4 up works; this one makes the escaped fragment a few hundred bytes.
const SPOOL_STR_FRAGMENT_BYTES: usize = 64;

/// **The largest piece anything may hand [`SpoolSink`] in one call, and the
/// reason the chunk never grows.**
///
/// A JSON string escape is at most six bytes of output per byte of input
/// (`\u00XX`, for a control character), so one fragment escapes to at most
/// six times its length, plus the two quotes `serde_json` writes around it and
/// [`SpoolSink::put_str`] strips. Every scalar's JSON text is far inside the
/// same figure: the widest is a 39-digit `u128`.
const SPOOL_MAX_PIECE_BYTES: usize = 6 * SPOOL_STR_FRAGMENT_BYTES + 2;

/// **The seal.** Nameable only inside this module, so every
/// [`SpoolScalar`] implementation is here, beside the invariant it belongs to —
/// a row shape in `writer::rows` cannot declare a type bounded by writing its
/// own impl.
mod sealed {
    pub trait BoundedJson {}
}

/// A value whose JSON text fits in one piece of at most
/// [`SPOOL_MAX_PIECE_BYTES`] bytes, **whatever the push contains** — a number,
/// a flag, a fingerprint. Not a string, not a sequence, not a struct.
///
/// This is the type-level half of [`SpoolSink`]'s invariant: `field` and
/// `array` take only these, so a field of a type whose text grows with the
/// input does not compile through them and has to go through the streamed
/// route instead. Adding a scalar type means adding an impl below, inside the
/// seal, and `every_spool_scalar_fits_one_bounded_piece` then holds its widest
/// value to the piece bound.
pub(crate) trait SpoolScalar: Serialize + sealed::BoundedJson {}

macro_rules! spool_scalar {
    ($($t:ty),* $(,)?) => { $(
        impl sealed::BoundedJson for $t {}
        impl SpoolScalar for $t {}
    )* };
}

// Every scalar type a row shape writes today. `i128` is the spool document's
// own `spooled_at_ns`; `Fingerprint` is `#[serde(transparent)]` over a `u128`,
// so its text is at most 39 digits.
spool_scalar!(
    bool,
    i8,
    i32,
    i64,
    i128,
    u8,
    // `LogLandingRow::month`, the days-since-epoch a `Date` column takes.
    u16,
    u32,
    u64,
    Fingerprint,
    FiniteOrNull,
    serde_json::Number,
);

/// A reference to a bounded value is bounded — so `array` takes an iterator of
/// references (`&Vec<i64>`) as readily as one of values.
impl<T: sealed::BoundedJson + ?Sized> sealed::BoundedJson for &T {}
impl<T: SpoolScalar + ?Sized> SpoolScalar for &T {}

/// A finite `f64` as a JSON number, and a non-finite one as JSON `null` —
/// NaN and ±∞ are not JSON-representable, and the exact bits travel in the
/// paired `*_bits` string field (this module's doc comment).
///
/// The one definition of that rule: `writer::rows`' `finite_or_null` builds its
/// [`serde_json::Value`] form from this impl rather than restating it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FiniteOrNull(pub(crate) f64);

impl Serialize for FiniteOrNull {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if self.0.is_finite() {
            s.serialize_f64(self.0)
        } else {
            s.serialize_none()
        }
    }
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

/// A file being written in bounded pieces.
///
/// **The invariant: nothing in the encoding path holds a copy that grows with
/// the input.** What this type holds at any instant is one chunk of at most
/// [`SPOOL_CHUNK_BYTES`] bytes, and the chunk's allocation never grows past
/// that for the sink's whole life. Every byte of every document reaches the
/// file through [`Self::put_bounded`], which takes at most
/// [`SPOOL_MAX_PIECE_BYTES`] at a time and writes the chunk out first when the
/// piece would not fit, so the chunk is never asked to reallocate.
///
/// There are exactly four routes in, and each one is bounded:
///
/// | route | what it takes | how it stays bounded |
/// |---|---|---|
/// | [`Self::put_bounded`] | punctuation | the caller's literal, checked against the piece bound |
/// | [`Self::put_scalar`] | a [`SpoolScalar`] | the type says its text fits one piece, and the seal keeps the set of such types in this module |
/// | [`Self::put_str`] | any `&str` | escaped and written [`SPOOL_STR_FRAGMENT_BYTES`] of input at a time |
/// | [`Self::put_value`] | any [`serde_json::Value`] | walked with an explicit stack, every leaf through one of the three above |
///
/// **So a field added to a row shape cannot break the bound without saying so.**
/// A `String` field does not compile through `field`; it has to go through
/// `str_field`, which streams. A `Vec` field has to go through `array` or
/// `str_array`, which write one element at a time. A nested value has to go
/// through `put_value`. And a scalar of a type with no impl here does not
/// compile at all until someone adds one inside the seal. Two checks back the
/// statement up: `put_bounded` refuses an over-long piece in every build, not
/// just a debug one, and the cases in this module assert
/// [`Self::chunk_high_water`] after writing megabyte strings and values.
///
/// **What is not bounded, and is not this type's to bound:** the value a caller
/// hands in. `SpoolEncode::to_spool_value` builds a whole value tree for the
/// row shapes that use the default encoder, and that tree is the caller's
/// memory, charged or not by whatever queued the row. This type adds nothing
/// to it.
pub(crate) struct SpoolSink {
    file: fs::File,
    /// The chunk. Allocated once at [`SPOOL_CHUNK_BYTES`] and never grown —
    /// see the invariant above.
    buf: Vec<u8>,
    /// The most [`Self::buf`] has ever held at once — the figure the bound is
    /// about, so a case can assert it directly instead of inferring it from a
    /// process-wide allocation high-water mark. Test-only: nothing in
    /// production reads it.
    #[cfg(test)]
    chunk_high_water: usize,
}

impl SpoolSink {
    fn new(file: fs::File) -> Self {
        SpoolSink {
            file,
            buf: Vec::with_capacity(SPOOL_CHUNK_BYTES),
            #[cfg(test)]
            chunk_high_water: 0,
        }
    }

    /// Appends one piece of at most [`SPOOL_MAX_PIECE_BYTES`] bytes — the
    /// punctuation, one scalar's text, one fragment of a string's escaping.
    /// Writes the chunk out first when the piece would not fit in what is left
    /// of it, so the chunk never has to grow.
    ///
    /// **The over-long piece is refused rather than asserted against**, in
    /// every build: a scalar type whose text outgrew its bound would otherwise
    /// silently reintroduce the copy this bound exists to remove. A refusal
    /// fails the spool write, which the caller logs (`writer::table`).
    async fn put_bounded(&mut self, piece: &[u8]) -> std::io::Result<()> {
        if piece.len() > SPOOL_MAX_PIECE_BYTES {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "a spool document piece of {} bytes is past the {} byte \
                     bound: see SpoolSink's invariant",
                    piece.len(),
                    SPOOL_MAX_PIECE_BYTES
                ),
            ));
        }
        if self.buf.len() + piece.len() > SPOOL_CHUNK_BYTES {
            self.flush().await?;
        }
        self.buf.extend_from_slice(piece);
        self.note_chunk();
        Ok(())
    }

    /// Records what the chunk holds, right after the append that grew it.
    fn note_chunk(&mut self) {
        #[cfg(test)]
        {
            self.chunk_high_water = self.chunk_high_water.max(self.buf.len());
        }
    }

    /// One [`SpoolScalar`]'s JSON text, formed in a fixed slot on the stack and
    /// copied into the chunk. A type whose text does not fit the slot fails
    /// here, which is the run-time half of the seal's promise.
    async fn put_scalar<T: SpoolScalar + ?Sized>(&mut self, value: &T) -> std::io::Result<()> {
        let mut slot = [0u8; SPOOL_MAX_PIECE_BYTES];
        let mut cursor: &mut [u8] = &mut slot;
        serde_json::to_writer(&mut cursor, value)
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
        let written = SPOOL_MAX_PIECE_BYTES - cursor.len();
        self.put_bounded(&slot[..written]).await
    }

    /// Writes one `&str` as a JSON string, **one bounded fragment at a time**:
    /// the quotes, then `serde_json`'s own escaping of at most
    /// [`SPOOL_STR_FRAGMENT_BYTES`] input bytes per piece.
    ///
    /// `serde_json` escapes a string byte by byte off a lookup table and
    /// carries no state between bytes, and a byte of a multi-byte character is
    /// never escaped, so escaping the fragments of a string and concatenating
    /// the results gives exactly what escaping it whole gives. The fragments
    /// are cut on character boundaries because `&str` demands it, not because
    /// the escaping does — `a_streamed_string_escapes_exactly_as_serde_json_does`
    /// holds the two forms together over every character width and either side
    /// of a fragment boundary.
    pub(crate) async fn put_str(&mut self, s: &str) -> std::io::Result<()> {
        self.put_bounded(b"\"").await?;
        let mut rest = s;
        while !rest.is_empty() {
            // Back off to a character boundary. A character is at most four
            // bytes and a fragment is 64, so this never reaches zero.
            let mut cut = SPOOL_STR_FRAGMENT_BYTES.min(rest.len());
            while !rest.is_char_boundary(cut) {
                cut -= 1;
            }
            let (head, tail) = rest.split_at(cut);
            rest = tail;
            let mut slot = [0u8; SPOOL_MAX_PIECE_BYTES];
            let mut cursor: &mut [u8] = &mut slot;
            serde_json::to_writer(&mut cursor, head)
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
            let written = SPOOL_MAX_PIECE_BYTES - cursor.len();
            // `to_writer` wrote `"<escaped>"`; the quotes are this method's.
            self.put_bounded(&slot[1..written - 1]).await?;
        }
        self.put_bounded(b"\"").await
    }

    /// Writes one [`serde_json::Value`] of any shape, every leaf through a
    /// bounded route — the encoding [`SpoolEncode::write_spool_json`]'s default
    /// uses, and so the one every row shape but the metrics landing row's goes
    /// through.
    ///
    /// Walked with an explicit stack rather than by recursion: a recursive
    /// `async fn` boxes a future per level and would box one per array element,
    /// where this holds one frame per level of nesting and nothing per element.
    /// Nesting is a property of the shape the row declares, never of the push.
    pub(crate) async fn put_value(&mut self, value: &serde_json::Value) -> std::io::Result<()> {
        use serde_json::Value;

        /// One container being walked, and whether its next member is its first.
        enum Frame<'a> {
            Array(std::slice::Iter<'a, Value>),
            Object(serde_json::map::Iter<'a>),
        }
        /// What advancing the innermost frame asks for next. Its borrows are
        /// the walked value's, not the stack's, so the stack is free again.
        enum Step<'a> {
            Item(bool, &'a Value),
            Entry(bool, &'a str, &'a Value),
            EndArray,
            EndObject,
        }

        let mut stack: Vec<(Frame<'_>, bool)> = Vec::new();
        let mut next = Some(value);
        loop {
            if let Some(v) = next.take() {
                match v {
                    Value::Null => self.put_bounded(b"null").await?,
                    Value::Bool(true) => self.put_bounded(b"true").await?,
                    Value::Bool(false) => self.put_bounded(b"false").await?,
                    Value::Number(n) => self.put_scalar(n).await?,
                    Value::String(s) => self.put_str(s).await?,
                    Value::Array(items) => {
                        self.put_bounded(b"[").await?;
                        stack.push((Frame::Array(items.iter()), true));
                    }
                    Value::Object(map) => {
                        self.put_bounded(b"{").await?;
                        stack.push((Frame::Object(map.iter()), true));
                    }
                }
            }
            let step = match stack.last_mut() {
                None => return Ok(()),
                Some((Frame::Array(items), first)) => match items.next() {
                    Some(item) => {
                        let was_first = *first;
                        *first = false;
                        Step::Item(was_first, item)
                    }
                    None => Step::EndArray,
                },
                Some((Frame::Object(entries), first)) => match entries.next() {
                    Some((key, val)) => {
                        let was_first = *first;
                        *first = false;
                        Step::Entry(was_first, key, val)
                    }
                    None => Step::EndObject,
                },
            };
            match step {
                Step::Item(first, item) => {
                    if !first {
                        self.put_bounded(b",").await?;
                    }
                    next = Some(item);
                }
                Step::Entry(first, key, val) => {
                    if !first {
                        self.put_bounded(b",").await?;
                    }
                    self.put_str(key).await?;
                    self.put_bounded(b":").await?;
                    next = Some(val);
                }
                Step::EndArray => {
                    stack.pop();
                    self.put_bounded(b"]").await?;
                }
                Step::EndObject => {
                    stack.pop();
                    self.put_bounded(b"}").await?;
                }
            }
        }
    }

    /// Opens a JSON object, to be written field by field and closed with
    /// [`SpoolObject::end`] — how a row whose arrays and text are as long as
    /// the push chooses is encoded without ever holding one whole (issue #603
    /// code review rounds 8 and 9).
    pub(crate) async fn begin_object(&mut self) -> std::io::Result<SpoolObject<'_>> {
        self.put_bounded(b"{").await?;
        Ok(SpoolObject {
            sink: self,
            first: true,
        })
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.file.write_all(&self.buf).await?;
        self.buf.clear();
        Ok(())
    }

    /// The last chunk, and then the file's own buffer.
    ///
    /// The second half is not optional: `tokio::fs::File::write_all` hands the
    /// bytes to a blocking task and returns before that task has run, so a
    /// `rename` issued straight after it is a second blocking task with no
    /// ordering against the first — a reader could then open the renamed file
    /// and find it short. Flushing here waits for the write, so the rename
    /// publishes a whole document.
    async fn finish(&mut self) -> std::io::Result<()> {
        self.flush().await?;
        self.file.flush().await
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
    /// `"name":<value>` for a value whose JSON text fits one bounded piece.
    ///
    /// **A `String`, a `Vec` or a nested value does not compile here** — that
    /// is [`SpoolSink`]'s invariant doing its work. Use [`Self::str_field`],
    /// [`Self::array`] or [`Self::str_array`].
    pub(crate) async fn field<T: SpoolScalar + ?Sized>(
        &mut self,
        name: &str,
        value: &T,
    ) -> std::io::Result<()> {
        self.key(name).await?;
        self.sink.put_scalar(value).await
    }

    /// `"name":"<text>"`, the text escaped and written in bounded fragments
    /// ([`SpoolSink::put_str`]), so a string of any length holds one fragment
    /// and not its own text.
    pub(crate) async fn str_field(&mut self, name: &str, value: &str) -> std::io::Result<()> {
        self.key(name).await?;
        self.sink.put_str(value).await
    }

    /// `"name":[…]`, **one element at a time**, so an array of any length
    /// holds one element's text and not its own.
    pub(crate) async fn array<T: SpoolScalar>(
        &mut self,
        name: &str,
        items: impl IntoIterator<Item = T>,
    ) -> std::io::Result<()> {
        self.key(name).await?;
        self.sink.put_bounded(b"[").await?;
        for (i, item) in items.into_iter().enumerate() {
            if i > 0 {
                self.sink.put_bounded(b",").await?;
            }
            self.sink.put_scalar(&item).await?;
        }
        self.sink.put_bounded(b"]").await
    }

    /// [`Self::array`] for strings: one element at a time and each element in
    /// bounded fragments, so neither the array's length nor any element's
    /// length is held.
    pub(crate) async fn str_array<S: AsRef<str>>(
        &mut self,
        name: &str,
        items: impl IntoIterator<Item = S>,
    ) -> std::io::Result<()> {
        self.key(name).await?;
        self.sink.put_bounded(b"[").await?;
        for (i, item) in items.into_iter().enumerate() {
            if i > 0 {
                self.sink.put_bounded(b",").await?;
            }
            self.sink.put_str(item.as_ref()).await?;
        }
        self.sink.put_bounded(b"]").await
    }

    /// Closes the object.
    pub(crate) async fn end(self) -> std::io::Result<()> {
        self.sink.put_bounded(b"}").await
    }

    /// The comma, then the quoted field name and its colon. The name goes
    /// through the same string path as any other string, so it is escaped
    /// exactly as a derived `Serialize` would escape it.
    async fn key(&mut self, name: &str) -> std::io::Result<()> {
        if !self.first {
            self.sink.put_bounded(b",").await?;
        }
        self.first = false;
        self.sink.put_str(name).await?;
        self.sink.put_bounded(b":").await
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
/// holds instead is one chunk — [`SpoolSink`]'s invariant, which covers this
/// document's own `table` and `error` text as well as the rows'.
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
    out.put_bounded(b"{\"table\":").await?;
    out.put_str(table).await?;
    out.put_bounded(b",\"error\":").await?;
    out.put_str(error).await?;
    out.put_bounded(b",\"spooled_at_ns\":").await?;
    out.put_scalar(&spooled_at_ns).await?;
    out.put_bounded(b",\"rows\":[").await?;
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            out.put_bounded(b",").await?;
        }
        row.write_spool_json(&mut out).await?;
    }
    out.put_bounded(b"]}").await?;
    out.finish().await?;
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

    /// **Every [`SpoolScalar`] type's widest value fits one bounded piece.**
    /// The trait's promise, held against each type that carries it — the one
    /// place a new scalar type has to be added, next to its impl.
    #[tokio::test]
    async fn every_spool_scalar_fits_one_bounded_piece() {
        let widest: Vec<(&str, String)> = vec![
            ("bool", serde_json::to_string(&false).unwrap()),
            ("i8", serde_json::to_string(&i8::MIN).unwrap()),
            ("i32", serde_json::to_string(&i32::MIN).unwrap()),
            ("i64", serde_json::to_string(&i64::MIN).unwrap()),
            ("i128", serde_json::to_string(&i128::MIN).unwrap()),
            ("u8", serde_json::to_string(&u8::MAX).unwrap()),
            ("u16", serde_json::to_string(&u16::MAX).unwrap()),
            ("u32", serde_json::to_string(&u32::MAX).unwrap()),
            ("u64", serde_json::to_string(&u64::MAX).unwrap()),
            (
                "Fingerprint",
                serde_json::to_string(&Fingerprint::from_raw(u128::MAX)).unwrap(),
            ),
            (
                "FiniteOrNull",
                serde_json::to_string(&FiniteOrNull(f64::MIN)).unwrap(),
            ),
            (
                "serde_json::Number",
                serde_json::to_string(&serde_json::json!(f64::MIN)).unwrap(),
            ),
        ];
        for (name, text) in widest {
            assert!(
                text.len() <= SPOOL_MAX_PIECE_BYTES,
                "{name}'s widest JSON text is {} bytes, past the \
                 {SPOOL_MAX_PIECE_BYTES} byte piece bound: it cannot carry \
                 SpoolScalar",
                text.len()
            );
        }
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

    /// **T40.** Each LOGS landing row kind streams the shape that kind
    /// declares. A row written field by field into the sink must put the same
    /// document on disk as the declared [`SpoolEncode::to_spool_value`] shape,
    /// which is the audit record a reader reads; nothing else holds the two
    /// together once the encoder stops going through a value.
    ///
    /// **What this cannot detect is a MISSING override**: the default
    /// `write_spool_json` walks `to_spool_value` itself, so the comparison is
    /// trivially true with no override at all. It fails on an override that
    /// drops a field or misspells a key.
    /// `spooling_a_log_block_holds_no_copy_of_the_push`
    /// (`crates/pulsus-write/tests/spool_stream_alloc.rs`) is the sibling that
    /// detects a missing override, and neither case covers the other.
    #[tokio::test]
    async fn every_log_landing_row_kind_streams_the_shape_it_declares() {
        let dir = tempdir();
        for (name, row) in log_landing_rows_of_every_kind() {
            let path = dir.join(format!("{name}.json"));
            let whole = serde_json::to_vec(&SpoolRecord {
                table: "log_landing",
                error: "boom",
                spooled_at_ns: 1_700_000_000_123_456_789,
                rows: vec![row.to_spool_value()],
            })
            .expect("the record serializes");
            write_record(
                &path,
                "log_landing",
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

    /// One row of each logs kind, each carrying text long enough that the
    /// string path spills the chunk rather than fitting one piece.
    fn log_landing_rows_of_every_kind() -> Vec<(&'static str, crate::writer::rows::LogLandingRow)> {
        use crate::protocols::otlp_logs::{LogRow, StreamRow};
        use crate::writer::rows::{LogLandingRow, LogPatternRow};
        use pulsus_model::{Date, UnixNano};

        const TS: i64 = 1_700_000_000_000_000_000;
        let (labels, _) = pulsus_model::LabelSet::from_normalized([
            ("service_name".to_string(), "checkout".to_string()),
            ("env".to_string(), "\"quoted\"\n".to_string()),
        ]);
        let line = LogRow {
            service: "checkout".to_string(),
            fingerprint: Fingerprint::from_raw(7),
            timestamp_ns: UnixNano(TS),
            severity: -3,
            body: "x".repeat(STRING_PAST_CHUNK),
            structured_metadata: "{\"scope_name\":\"tracer\"}".to_string(),
        };
        let stream = StreamRow {
            month: Date::start_of_month_utc(TS).expect("a representable month"),
            fingerprint: Fingerprint::from_raw(9),
            service: "checkout".to_string(),
            labels,
            updated_ns: TS,
        };
        let pattern = LogPatternRow {
            fingerprint: Fingerprint::from_raw(11),
            bucket_ns: TS - 7,
            pattern: "y".repeat(STRING_PAST_CHUNK),
            count: 4_242,
        };
        vec![
            ("kind0-line", LogLandingRow::line(5, &line)),
            ("kind1-stream", LogLandingRow::stream(5, &stream)),
            ("kind2-pattern", LogLandingRow::pattern(5, pattern)),
        ]
    }

    /// How long a string a case gives a row's `labels`, `help` or `unit` when
    /// it wants the encoder's string path to spill several times inside one
    /// value rather than at a field boundary.
    const STRING_PAST_CHUNK: usize = 5 * SPOOL_CHUNK_BYTES / 2;

    /// **What the sink holds does not grow with the string it is given**
    /// (issue #603 code review round 9, finding 1). A field whose text is
    /// megabytes has to leave the chunk the size it started: the reservation
    /// the queue is holding covers the row, so a serialized copy of that
    /// string beside it is memory `PULSUS_INGEST_QUEUE_BYTES` never knew
    /// about.
    ///
    /// The document is compared with what `serde_json` writes for the same
    /// field, so the bound is not bought by changing the bytes.
    #[tokio::test]
    async fn a_string_field_of_any_length_leaves_the_chunk_the_size_it_was() {
        let dir = tempdir();
        let path = dir.join("long-string.json");
        let value = "x".repeat(64 * SPOOL_CHUNK_BYTES);
        let mut out = SpoolSink::new(fs::File::create(&path).await.expect("create the file"));
        let mut o = out.begin_object().await.expect("open the object");
        o.str_field("labels", &value)
            .await
            .expect("write the field");
        o.end().await.expect("close the object");
        assert!(
            out.chunk_high_water <= SPOOL_CHUNK_BYTES,
            "a {} byte string had the sink holding {} bytes at once, past the \
             {SPOOL_CHUNK_BYTES} byte chunk: the encoder keeps a copy of the \
             field that grows with it",
            value.len(),
            out.chunk_high_water
        );
        let written = finish(out, &path).await;
        let whole = serde_json::json!({ "labels": value });
        assert_eq!(
            written,
            serde_json::to_string(&whole).expect("the object serializes"),
            "the streamed field is not the bytes serde_json writes"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// **The same, for a value of any shape** — the encoding every row shape
    /// but the landing row's uses ([`SpoolEncode::write_spool_json`]'s
    /// default). A nested value's strings and arrays go through the sink the
    /// way a landing row's fields do, so what is held is a chunk and one piece
    /// whatever the shape.
    #[tokio::test]
    async fn a_value_of_any_shape_leaves_the_chunk_the_size_it_was() {
        let dir = tempdir();
        let path = dir.join("long-value.json");
        let value = serde_json::json!({
            "body": "y".repeat(64 * SPOOL_CHUNK_BYTES),
            "empty_array": [],
            "empty_object": {},
            "escapes": "a\"b\\c\nd\te\u{0}f",
            "flags": [true, false, null],
            "nested": [{"k": [1, -2, 3.5]}, {"k": []}],
            "numbers": [0, -1, 1.5, 1e300],
            "strings": ["p".repeat(SPOOL_CHUNK_BYTES), "q"],
        });
        let mut out = SpoolSink::new(fs::File::create(&path).await.expect("create the file"));
        out.put_value(&value).await.expect("write the value");
        assert!(
            out.chunk_high_water <= SPOOL_CHUNK_BYTES,
            "the value had the sink holding {} bytes at once, past the \
             {SPOOL_CHUNK_BYTES} byte chunk: the default encoder keeps a copy \
             of it",
            out.chunk_high_water
        );
        assert_eq!(
            finish(out, &path).await,
            serde_json::to_string(&value).expect("the value serializes"),
            "the streamed value is not the bytes serde_json writes"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Finishes the sink ([`SpoolSink::finish`]), closes it, and answers what is
    /// on disk.
    async fn finish(mut out: SpoolSink, path: &Path) -> String {
        out.finish().await.expect("flush the sink and the file");
        drop(out);
        std::fs::read_to_string(path).expect("read the document back")
    }

    /// **A string written in fragments is escaped exactly as `serde_json`
    /// escapes it whole.** Every byte value that can appear in a Rust `str`,
    /// every character width, and lengths either side of the fragment the sink
    /// escapes at a time — the one thing a streamed escaper can get wrong that
    /// a whole-value encoder cannot.
    #[tokio::test]
    async fn a_streamed_string_escapes_exactly_as_serde_json_does() {
        let dir = tempdir();
        let every_char: String = (0u32..=0x2FF)
            .filter_map(char::from_u32)
            .chain(['\u{1F600}', '\u{10FFFF}', '\u{FFFD}'])
            .collect();
        let cases: Vec<String> = vec![
            String::new(),
            "\"".to_string(),
            "\u{0}".repeat(4 * SPOOL_CHUNK_BYTES / 3),
            "\\".repeat(200),
            "é".repeat(200),
            "\u{1F600}".repeat(200),
            every_char.clone(),
            every_char.repeat(40),
            format!("{}{}", "z".repeat(63), "é"),
            format!("{}{}", "z".repeat(64), "\u{1F600}"),
            format!("{}{}", "z".repeat(65), "\n"),
        ];
        for (i, case) in cases.iter().enumerate() {
            let path = dir.join(format!("escape-{i}.json"));
            let mut out = SpoolSink::new(fs::File::create(&path).await.expect("create the file"));
            out.put_str(case).await.expect("write the string");
            assert_eq!(
                finish(out, &path).await,
                serde_json::to_string(case).expect("the string serializes"),
                "case {i} ({} bytes) is not escaped the way serde_json escapes it",
                case.len()
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// One row of each landing kind, the histogram twice: an empty-array one
    /// and one carrying `MAX_BUCKETS_PER_HISTOGRAM_SIDE` custom bounds, which
    /// is the widest single row the ingest seam admits. The kind-2 and kind-3
    /// rows appear twice as well — once with short text and once with text
    /// several chunks long, so the comparison covers a string the encoder has
    /// to write in fragments (issue #603 code review round 9, finding 1).
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
        // The same two kinds with text several chunks long, and with a
        // character needing an escape every few bytes so the fragments the
        // encoder writes fall inside escapes as well as between them.
        let long_text = "a\"b\nc\\d\u{0}e".repeat(STRING_PAST_CHUNK / 8);
        let (long_labels, _) =
            pulsus_model::LabelSet::from_normalized([("job".to_string(), long_text.clone())]);
        let long_series = SeriesRef {
            metric_name: Arc::from("http_requests_total"),
            fingerprint: Fingerprint::from_raw(7),
            labels: long_labels,
        };
        let long_meta = MetricMetadata {
            metric_name: Arc::from("http_requests_total"),
            metric_type: "counter".to_string(),
            help: long_text.clone(),
            unit: long_text,
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
            (
                "series-long-labels",
                MetricLandingRow::series(5, &long_series, 3_600_000, 1),
            ),
            ("metadata", MetricLandingRow::metadata(5, &meta)),
            (
                "metadata-long-text",
                MetricLandingRow::metadata(5, &long_meta),
            ),
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
