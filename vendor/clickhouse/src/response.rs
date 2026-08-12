use bstr::ByteSlice;
use bytes::{BufMut, Bytes};
use futures_util::stream::{self, Stream, TryStreamExt};
use http_body_util::BodyExt as _;
use hyper::{
    StatusCode,
    body::{Body as _, Incoming},
};
use hyper_util::client::legacy::ResponseFuture as HyperResponseFuture;
use std::{
    future::{self, Future},
    pin::{Pin, pin},
    task::{Context, Poll},
};

#[cfg(feature = "lz4")]
use crate::compression::lz4::Lz4Decoder;
#[cfg(feature = "zstd")]
use crate::compression::zstd::ZstdHttpDecoder;
use crate::{
    compression::Compression,
    error::{Error, Result},
    query_summary::QuerySummary,
};
use tracing::Instrument;

// === Response ===

pub(crate) enum Response {
    // Headers haven't been received yet.
    // `Box<_>` improves performance by reducing the size of the whole future.
    Waiting(ResponseFuture),
    // Headers have been received, streaming the body.
    Loading(Chunks),
}

pub(crate) type ResponseFuture =
    Pin<Box<dyn Future<Output = Result<(Chunks, Option<Box<QuerySummary>>)>> + Send>>;

impl Response {
    pub(crate) fn new(response: HyperResponseFuture, compression: Compression) -> Self {
        let span = tracing::info_span!(
            "response",
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            error.type = tracing::field::Empty,
            db.response_code = tracing::field::Empty,
        );

        Self::Waiting(Box::pin(
            collect_response(response, compression).instrument(span),
        ))
    }

    pub(crate) fn into_future(self) -> ResponseFuture {
        match self {
            Self::Waiting(future) => future,
            Self::Loading(_) => panic!("response is already streaming"),
        }
    }

    pub(crate) async fn finish(&mut self) -> Result<()> {
        let chunks = loop {
            match self {
                Self::Waiting(future) => {
                    let (chunks, _summary) = future.await?;
                    *self = Self::Loading(chunks);
                }
                Self::Loading(chunks) => break chunks,
            }
        };

        while chunks.try_next().await?.is_some() {}
        Ok(())
    }
}

async fn collect_response(
    response: HyperResponseFuture,
    compression: Compression,
) -> Result<(Chunks, Option<Box<QuerySummary>>)> {
    let response = response.await?;

    let status = response.status();
    let exception_code = response.headers().get("X-ClickHouse-Exception-Code");

    tracing::record_all!(
        tracing::Span::current(),
        // Note: not supposed to set `otel.status_code` unless an error occurs
        db.response.status_code = status.as_u16(),
    );

    if status == StatusCode::OK && exception_code.is_none() {
        let tag = response
            .headers()
            .get("X-ClickHouse-Exception-Tag")
            .map(|value| value.as_bytes().into());

        let summary = response
            .headers()
            .get("X-ClickHouse-Summary")
            .and_then(|v| v.to_str().ok())
            .and_then(QuerySummary::from_header)
            .map(Box::new); // More likely to be successful, start streaming.
        // It still can fail, but we'll handle it in `DetectDbException`.
        Ok((Chunks::new(response.into_body(), compression, tag), summary))
    } else {
        // An instantly failed request.
        let error = collect_bad_response(
            status,
            exception_code
                .and_then(|value| value.to_str().ok())
                .map(|code| format!("Code: {code}")),
            response.into_body(),
            compression,
        )
        .await;

        error.record_in_current_span("response error");

        Err(error)
    }
}

#[cold]
#[inline(never)]
async fn collect_bad_response(
    status: StatusCode,
    exception_code: Option<String>,
    body: Incoming,
    compression: Compression,
) -> Error {
    // Collect the whole body into one contiguous buffer to simplify handling.
    // Only network errors can occur here and we return them instead of status code
    // because it means the request can be repeated to get a more detailed error.
    //
    // TODO: we don't implement any length checks and a malicious peer (e.g. MITM)
    //       might make us consume arbitrary amounts of memory.
    let raw_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        // If we can't collect the body, return standardised reason for the status code.
        Err(_) => return Error::BadResponse(reason(status, exception_code)),
    };
    if raw_bytes.is_empty() {
        return Error::BadResponse(reason(status, exception_code));
    }

    // Try to decompress the body, because CH uses compression even for errors.
    let stream = stream::once(future::ready(Result::<_>::Ok(raw_bytes.slice(..))));
    let stream = Decompress::new(stream, compression).map_ok(|chunk| chunk.data);

    // We're collecting already fetched chunks, thus only decompression errors can
    // be here. If decompression is failed, we should try the raw body because
    // it can be sent without any compression if some proxy is used, which
    // typically know nothing about CH params.
    let bytes = collect_bytes(stream).await.unwrap_or(raw_bytes);

    // PULSUSDB PATCH (issue #382) -- see ../PATCHES.md.
    //
    // Upstream discards `exception_code` here whenever the body decodes, and
    // the body is `<already-written result bytes><exception>` when the query
    // had produced output before failing. Callers that need the numeric code
    // are then left parsing it out of text that includes query results and an
    // exception description echoing the SQL -- both of which can contain a
    // forged `Code: N. DB::Exception:`, so no parse of the text is sound.
    //
    // `exception_code` comes from the `X-ClickHouse-Exception-Code` response
    // header and nothing in the body can influence it, so put it at byte 0.
    // The decoded body is kept whole after it: callers still read the
    // description (PulsusDB's 427 handling parses `cannot compile re2: ...`
    // out of it) and operators still see the full message.
    //
    // The prefix is added only when the body does not already start with the
    // same `Code: N`, so every response whose exception was NOT preceded by
    // output -- the overwhelmingly common case, and every response on the
    // `reason()` fallback paths above -- is byte-identical to upstream.
    let decoded = String::from_utf8(bytes.into())
        .map(|reason| reason.trim().to_string())
        .ok();
    let reason = match (decoded, exception_code) {
        (Some(body), Some(code)) if !body.starts_with(&code) => format!("{code}\n{body}"),
        (Some(body), _) => body,
        // If we have a unreadable response, return standardised reason for the status code.
        (None, exception_code) => reason(status, exception_code),
    };

    Error::BadResponse(reason)
}

async fn collect_bytes(stream: impl Stream<Item = Result<Bytes>>) -> Result<Bytes> {
    let mut stream = pin!(stream);

    let mut bytes = Vec::new();

    // TODO: avoid extra copying if there is only one chunk in the stream.
    while let Some(chunk) = stream.try_next().await? {
        bytes.put(chunk);
    }

    Ok(bytes.into())
}

fn reason(status: StatusCode, exception_code: Option<String>) -> String {
    exception_code.unwrap_or_else(|| {
        format!(
            "{} {}",
            status.as_str(),
            status.canonical_reason().unwrap_or("<unknown>"),
        )
    })
}

// === Chunks ===

pub(crate) struct Chunk {
    pub(crate) data: Bytes,
    pub(crate) net_size: usize,
}

// * Uses `Option<_>` to make this stream fused.
// * Uses `Box<_>` in order to reduce the size of cursors.
pub(crate) struct Chunks {
    inner: Option<Box<DetectDbException<Decompress<IncomingStream>>>>,
}

impl Chunks {
    fn new(stream: Incoming, compression: Compression, exception_tag: Option<Box<[u8]>>) -> Self {
        let stream = IncomingStream(stream);
        let stream = Decompress::new(stream, compression);
        // PULSUSDB PATCH (issue #412): the anchor is built once per response,
        // never per chunk.
        let anchor = exception_tag.as_deref().map(|tag| {
            let mut anchor = Vec::with_capacity(EXC_OPEN.len() + tag.len());
            anchor.extend_from_slice(EXC_OPEN);
            anchor.extend_from_slice(tag);
            anchor.into_boxed_slice()
        });
        let stream = DetectDbException {
            stream,
            exception_tag,
            anchor,
            pos: FramePos::default(),
            eos: false,
        };
        Self {
            inner: Some(Box::new(stream)),
        }
    }

    pub(crate) fn empty() -> Self {
        Self { inner: None }
    }

    #[cfg(feature = "futures03")]
    pub(crate) fn is_terminated(&self) -> bool {
        self.inner.is_none()
    }
}

impl Stream for Chunks {
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // We use `take()` to make the stream fused, including the case of panics.
        if let Some(mut stream) = self.inner.take() {
            let res = Pin::new(&mut stream).poll_next(cx);

            if matches!(res, Poll::Pending | Poll::Ready(Some(Ok(_)))) {
                self.inner = Some(stream);
            }

            res
        } else {
            Poll::Ready(None)
        }
    }

    // `size_hint()` is unimplemented because unused.
}

// === IncomingStream ===

// * Produces bytes from incoming data frames.
// * Skips trailer frames (CH doesn't use them for now).
// * Converts hyper errors to our own.
struct IncomingStream(Incoming);

impl Stream for IncomingStream {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut incoming = Pin::new(&mut self.get_mut().0);

        loop {
            break match incoming.as_mut().poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(bytes) => Poll::Ready(Some(Ok(bytes))),
                    Err(_frame) => continue,
                },
                Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

// === Decompress ===

enum Decompress<S> {
    Plain(S),
    #[cfg(feature = "lz4")]
    Lz4(Lz4Decoder<S>),
    #[cfg(feature = "zstd")]
    Zstd(ZstdHttpDecoder<S>),
}

impl<S> Decompress<S> {
    fn new(stream: S, compression: Compression) -> Self {
        match compression {
            Compression::None => Self::Plain(stream),
            #[cfg(feature = "lz4")]
            #[allow(deprecated)]
            Compression::Lz4 | Compression::Lz4Hc(_) => Self::Lz4(Lz4Decoder::new(stream)),
            #[cfg(feature = "zstd")]
            Compression::Zstd(_) => Self::Zstd(ZstdHttpDecoder::new(stream)),
        }
    }
}

impl<S> Stream for Decompress<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream)
                .poll_next(cx)
                .map_ok(|bytes| Chunk {
                    net_size: bytes.len(),
                    data: bytes,
                })
                .map_err(Into::into),
            #[cfg(feature = "lz4")]
            Self::Lz4(stream) => Pin::new(stream).poll_next(cx),
            #[cfg(feature = "zstd")]
            Self::Zstd(stream) => Pin::new(stream).poll_next(cx),
        }
    }
}

// === DetectDbException ===

// PULSUSDB PATCH (issue #412) -- see ../PATCHES.md §2.
//
// The opening of a tagged exception frame, without the tag. ClickHouse writes
// the frame with its two ends in OPPOSITE field orders -- captured from a live
// 26.3.17.110 HTTP-200 stream (body 15,960,999 bytes, tag `zgnglmkjouifsqby`):
//
// ```text
// opening   \r\n__exception__\r\nzgnglmkjouifsqby\r\nCode: 395. DB::Exception: ...
// closing   ...0 (official build))\n288 zgnglmkjouifsqby\r\n__exception__\r\n
// ```
//
// `extract_exception_new`'s `strip_suffix` chain parses the CLOSING sequence
// only, so it says nothing about the opening; reassembly has to detect where a
// frame STARTS, which is `EXC_OPEN ++ tag`.
const EXC_OPEN: &[u8] = b"\r\n__exception__\r\n";

// The closing marker, which is also what upstream's `ends_with` arm keys on.
const EXC_CLOSE: &[u8] = b"__exception__\r\n";

// A memory bound on a frame that never terminates -- NOT a claim about any
// size ClickHouse guarantees. The largest exception body measured on
// 26.3.17.110 was 100,334 bytes (`SELECT throwIf(1, repeat('x', 100000))`), so
// this is roughly 167x that. Crossing it fails the stream with `Error::Other`
// and drops the buffered bytes; that path was not reproducible against a real
// server.
const EXC_FRAME_CAP: usize = 16 * 1024 * 1024;

/// Where the reassembler is inside a tagged exception frame.
///
/// Only reachable when the server declared `X-ClickHouse-Exception-Tag`; an
/// untagged response never leaves `Idle`.
#[derive(Default)]
enum FramePos {
    /// No frame open. Chunks are result data unless they carry the anchor.
    #[default]
    Idle,
    /// The tail of the previous chunk is a proper prefix of the anchor and
    /// nothing else yet, so the anchor may be straddling the boundary. At most
    /// `anchor.len() - 1` bytes.
    Maybe(Vec<u8>),
    /// The anchor matched; accumulating until the closing marker.
    Frame(Vec<u8>),
}

/// What `poll_next` should do with the chunk it was just handed.
enum Action {
    /// Deliver these bytes to the caller as result data.
    Data(Chunk),
    /// The bytes were withheld (buffered); poll the inner stream again.
    Buffer,
    /// Fail the stream.
    Fail(Error),
}

struct DetectDbException<S> {
    stream: S,
    exception_tag: Option<Box<[u8]>>,
    // PULSUSDB PATCH (issue #412): `EXC_OPEN ++ tag`, built ONCE in
    // `Chunks::new` -- never per chunk, which would be a 33-byte allocation on
    // the hot read path. `None` exactly when `exception_tag` is `None`.
    anchor: Option<Box<[u8]>>,
    pos: FramePos,
    // Set when the inner stream has ended, so a final `Maybe` flush can be
    // delivered as one more `Ready(Some(Ok(_)))` without re-polling an
    // exhausted stream on the following call.
    eos: bool,
}

impl<S> Stream for DetectDbException<S>
where
    S: Stream<Item = Result<Chunk>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.eos {
                return Poll::Ready(None);
            }

            let res = Pin::new(&mut self.stream).poll_next(cx);

            let chunk = match res {
                Poll::Ready(Some(Ok(chunk))) => chunk,
                Poll::Ready(None) => {
                    // PULSUSDB PATCH (issue #412): settle whatever was withheld.
                    self.eos = true;
                    let this = &mut *self;
                    let Some(action) = on_end_of_stream(&mut this.pos, this.anchor.as_deref())
                    else {
                        return Poll::Ready(None);
                    };
                    return match action {
                        Action::Data(chunk) => Poll::Ready(Some(Ok(chunk))),
                        Action::Fail(err) => {
                            err.record_in_current_span("response error");
                            Poll::Ready(Some(Err(err)))
                        }
                        Action::Buffer => Poll::Ready(None),
                    };
                }
                other => return other,
            };

            let this = &mut *self;
            // PULSUSDB PATCH (issue #412): the untagged path is upstream's,
            // byte for byte. It is the only signal a pre-25.11 server (or a
            // header-stripping proxy) gives, and it is reachable on `main` --
            // `PULSUS_SKIP_DDL` skips the version gate in `run_init`
            // (`crates/pulsus-server/src/serve.rs:645-685`,
            // `crates/pulsus-schema/src/controller.rs:84-100`).
            let (Some(tag), Some(anchor)) = (this.exception_tag.as_deref(), this.anchor.as_deref())
            else {
                if let Some(err) = extract_exception(&chunk.data, None) {
                    err.record_in_current_span("response error");
                    return Poll::Ready(Some(Err(err)));
                }
                return Poll::Ready(Some(Ok(chunk)));
            };

            match consume(&mut this.pos, chunk, anchor, tag) {
                Action::Data(chunk) => return Poll::Ready(Some(Ok(chunk))),
                Action::Buffer => continue,
                Action::Fail(err) => {
                    err.record_in_current_span("response error");
                    return Poll::Ready(Some(Err(err)));
                }
            }
        }
    }
}

// PULSUSDB PATCH (issue #412) -- see ../PATCHES.md §2.
//
// Decides what to do with one decompressed chunk on a TAGGED response.
//
// A free function, not a method: `self.exception_tag` / `self.anchor` and
// `&mut self.pos` cannot be borrowed together through a `Pin<&mut Self>`.
fn consume(pos: &mut FramePos, chunk: Chunk, anchor: &[u8], tag: &[u8]) -> Action {
    match pos {
        FramePos::Frame(buf) => {
            buf.extend_from_slice(&chunk.data);
            close_or_buffer(pos, tag)
        }
        FramePos::Maybe(prev) => {
            // The withheld tail is at most `anchor.len() - 1` bytes and is a
            // proper prefix of the anchor, so joining is the only way to see
            // an anchor that straddles the boundary. The copy costs one
            // chunk-sized memcpy and happens only when the previous chunk
            // ended with `\r`, `\r\n`, `\r\n_`, ... -- rare, and bounded.
            let mut joined = std::mem::take(prev);
            joined.extend_from_slice(&chunk.data);
            *pos = FramePos::Idle;
            scan(pos, Bytes::from(joined), chunk.net_size, anchor, tag)
        }
        FramePos::Idle => {
            // Upstream's arm, unchanged and still first: a chunk that ENDS
            // with the closing marker carries the whole frame (or its tail),
            // and `extract_exception_new` slices it by the server-declared
            // length. Only when that finds nothing do we look for an opening.
            if chunk.data.ends_with(EXC_CLOSE)
                && let Some(err) = extract_exception_new(&chunk.data, tag)
            {
                return Action::Fail(err);
            }
            scan(pos, chunk.data, chunk.net_size, anchor, tag)
        }
    }
}

// PULSUSDB PATCH (issue #412): the anchor may begin at ANY offset in `data`.
//
// Checking only offset 0 carries the same shape as the defect being fixed -- a
// check that inspects one position -- and measured on the hermetic mock, a
// frame opening appended after result data in one chunk then closing in the
// next was NOT recognised by a `starts_with` design (4 garbage rows, then
// `not enough data...`).
//
// `net_size` is the SOURCE block's compressed size, so when a chunk is split
// the whole of it is attributed to the emitted prefix; a chunk withheld in
// full contributes nothing. It feeds `RawCursor::received_bytes`, which is
// tracing only and has no caller in this workspace.
fn scan(pos: &mut FramePos, data: Bytes, net_size: usize, anchor: &[u8], tag: &[u8]) -> Action {
    if let Some(i) = data.find(anchor) {
        *pos = FramePos::Frame(data[i..].to_vec());
        let act = close_or_buffer(pos, tag);
        // Rows that arrived ahead of the frame are delivered, not discarded.
        // (The `ends_with` arm above does discard its whole chunk; that
        // asymmetry is upstream's and is left alone.)
        if i > 0 && matches!(act, Action::Buffer) {
            return Action::Data(Chunk {
                data: data.slice(..i),
                net_size,
            });
        }
        return act;
    }

    // Longest suffix of `data` that is a proper prefix of `anchor`: at most
    // `anchor.len() - 1` comparisons, so the anchor cannot hide in a straddle.
    let k = straddle_len(&data, anchor);
    if k > 0 {
        let split = data.len() - k;
        *pos = FramePos::Maybe(data[split..].to_vec());
        return if split > 0 {
            Action::Data(Chunk {
                data: data.slice(..split),
                net_size,
            })
        } else {
            Action::Buffer
        };
    }

    Action::Data(Chunk { data, net_size })
}

// PULSUSDB PATCH (issue #412): close the buffered frame, or keep buffering.
fn close_or_buffer(pos: &mut FramePos, tag: &[u8]) -> Action {
    let FramePos::Frame(buf) = pos else {
        // Unreachable: every caller sets `Frame` first. Treated as "keep
        // going" rather than panicking inside a stream poll.
        return Action::Buffer;
    };

    if buf.ends_with(EXC_CLOSE) {
        let err = extract_exception_new(buf, tag).unwrap_or_else(|| {
            Error::Other(
                format!(
                    "found a tagged exception frame in response but could not parse it (frame len: {})",
                    buf.len()
                )
                .into(),
            )
        });
        *pos = FramePos::Idle;
        return Action::Fail(err);
    }

    if buf.len() > EXC_FRAME_CAP {
        let len = buf.len();
        *pos = FramePos::Idle;
        return Action::Fail(Error::Other(
            format!(
                "a tagged exception frame exceeded {EXC_FRAME_CAP} bytes without terminating \
                 (buffered: {len})"
            )
            .into(),
        ));
    }

    Action::Buffer
}

// PULSUSDB PATCH (issue #412): the longest `k` in `1..anchor.len()` with
// `data` ending in `anchor[..k]`. `0` when no suffix of `data` is a proper
// prefix of `anchor`.
fn straddle_len(data: &[u8], anchor: &[u8]) -> usize {
    let max = anchor.len().saturating_sub(1).min(data.len());
    (1..=max)
        .rev()
        .find(|&k| data.ends_with(&anchor[..k]))
        .unwrap_or(0)
}

// PULSUSDB PATCH (issue #412): what happens to bytes still withheld when the
// stream ends. Three outcomes, all deliberate -- see ../PATCHES.md §2.
fn on_end_of_stream(pos: &mut FramePos, anchor: Option<&[u8]>) -> Option<Action> {
    match std::mem::take(pos) {
        FramePos::Idle => None,
        // A straddle that never became an anchor is result data.
        FramePos::Maybe(buf) => {
            let net_size = buf.len();
            Some(Action::Data(Chunk {
                data: Bytes::from(buf),
                net_size,
            }))
        }
        // A started frame NEVER ends as `Ok`: the anchor matched, so the tag
        // matched, and a truncated exception is the only shape that produces
        // one on a healthy server. The anchor and the `\r\n` after it are
        // stripped so byte 0 is the server's own `Code: N` and the caller's
        // code parse classifies it correctly. The tail of a partial trailer
        // may still be attached to the description -- lossy there, exact in
        // the code, and preferable to dropping the error.
        FramePos::Frame(buf) => {
            let msg = match anchor {
                Some(anchor) => buf
                    .strip_prefix(anchor)
                    .map(|rest| rest.strip_prefix(b"\r\n".as_slice()).unwrap_or(rest))
                    .unwrap_or(&buf),
                None => &buf,
            };
            Some(Action::Fail(Error::BadResponse(
                String::from_utf8_lossy(msg).trim().into(),
            )))
        }
    }
}

fn extract_exception(chunk: &[u8], tag: Option<&[u8]>) -> Option<Error> {
    // 25.11 introduced a new exception tagging format that's incompatible with the previous
    // https://github.com/ClickHouse/clickhouse-rs/issues/359
    //
    // PULSUSDB PATCH (issue #412): the tag, not the shape of the current
    // chunk, decides which channel is believed. Upstream falls through to
    // `extract_exception_old` on ANY chunk ending `))\n`, including on a
    // tagged response -- and result bytes are tenant data, so a stored
    // ClickHouse error message ending `))\n` fabricates a `Code: 210` on a
    // query that SUCCEEDED (measured: `rows=0` where the server returned
    // 200 000 rows). 210 is retryable, so the fabricated failure is retried
    // and demotes a healthy endpoint via `report_transport_failure`.
    match tag {
        // The server declared an exception tag, so every exception it raises
        // arrives inside `<open><tag>\r\n<msg>\n<len> <tag><close>`, sliced by
        // declared length. Result bytes are never searched.
        Some(tag) if chunk.ends_with(EXC_CLOSE) => extract_exception_new(chunk, tag),
        Some(_) => None,
        // No tag: pre-25.11 server, or a proxy that dropped the header. The
        // text search is then the only signal there is -- see ../PATCHES.md.
        None if chunk.ends_with(b"))\n") => {
            // `))\n` is very rare in real data, so it's fast dirty check.
            // In random data, it occurs with a probability of ~6*10^-8 only.
            extract_exception_old(chunk)
        }
        None => None,
    }
}

// Format:
// ```
//   <data>Code: <code>. DB::Exception: <desc> (version <version> (official build))\n
// ```
#[cold]
#[inline(never)]
fn extract_exception_old(chunk: &[u8]) -> Option<Error> {
    let index = chunk.rfind(b"Code:")?;

    if !(chunk[index..].contains_str(b"DB::") && chunk[index..].contains_str(b"Exception:")) {
        return None;
    }

    let exception = String::from_utf8_lossy(&chunk[index..chunk.len() - 1]);
    Some(Error::BadResponse(exception.into()))
}

// https://github.com/ClickHouse/ClickHouse/blob/4eaa92852bac117e95f28abe61237b0257d939d6/src/Server/HTTP/WriteBufferFromHTTPServerResponse.cpp#L347-L357
#[cold]
#[inline(never)]
fn extract_exception_new(chunk: &[u8], tag: &[u8]) -> Option<Error> {
    // Strip the chunk backwards until we get to the `<message length>`
    let rem = chunk
        .strip_suffix(b"\r\n__exception__\r\n")?
        .strip_suffix(tag)?
        .strip_suffix(b" ")?;

    // `<message length>` is *NOT* 8 bytes, because it's actually an integer formatted as text:
    // https://github.com/ClickHouse/ClickHouse/blob/4eaa92852bac117e95f28abe61237b0257d939d6/src/Server/HTTP/WriteBufferFromHTTPServerResponse.cpp#L376
    //
    // This means we actually need to search for the `\n` that's added to terminate the message:
    // https://github.com/ClickHouse/ClickHouse/blob/4eaa92852bac117e95f28abe61237b0257d939d6/src/Server/HTTP/WriteBufferFromHTTPServerResponse.cpp#L373-L374
    let msg_len_start = rem.rfind(b"\n")? + 1;

    // `msg_len_start` should always be either in-bounds or just past the end
    let msg_len = match parse_msg_len(&rem[msg_len_start..]) {
        Ok(msg_len) => msg_len,
        // At this point we can be fairly certain we've found the exception tag,
        // so it's better to fail with an error than continue.
        Err(e) => return Some(e),
    };

    // Note: checked operations in case `msg_len` is incorrect
    let Some(msg) = msg_len_start
        .checked_sub(msg_len)
        .and_then(|msg_start| rem.get(msg_start..msg_len_start))
    else {
        return Some(Error::Other(
            format!("found exception tag in response but message length was invalid: {msg_len} (chunk len: {})", chunk.len())
                .into(),
        ));
    };

    // We shouldn't discard the exception message if it fails to validate as UTF-8
    Some(Error::BadResponse(
        String::from_utf8_lossy(msg).trim().into(),
    ))
}

// FIXME: this can be replaced with `usize::from_ascii()` when stable
// https://github.com/rust-lang/rust/issues/134821
fn parse_msg_len(len_bytes: &[u8]) -> Result<usize, Error> {
    let len_utf8 = str::from_utf8(len_bytes).map_err(|e| {
        Error::Other(
            format!("found exception tag in response but failed to parse message length: {e}")
                .into(),
        )
    })?;

    len_utf8.parse().map_err(|e| {
        Error::Other(
            format!("found exception tag in response but failed to parse message length {len_utf8:?}: {e}")
                .into(),
        )
    })
}

#[test]
fn it_extracts_exception_old() {
    let errors = [
        "Code: 159. DB::Exception: Timeout exceeded: elapsed 1.2 seconds, maximum: 0.1. (TIMEOUT_EXCEEDED) (version 24.10.1.2812 (official build))",
        "Code: 210. DB::NetException: I/O error: Broken pipe, while writing to socket (127.0.0.1:9000 -> 127.0.0.1:54646). (NETWORK_ERROR) (version 23.8.8.20 (official build))",
    ];

    for error in errors {
        let chunk = format!("{error}\n");
        let err = extract_exception(chunk.as_bytes(), None).expect("failed to extract exception");
        assert_eq!(err.to_string(), format!("bad response: {error}"));
    }
}

#[test]
fn it_extracts_exception_new() {
    let tag = b"rnywyenlaeqynhmu";
    let chunk = b"\r\n__exception__\r\nrnywyenlaeqynhmu\r\nCode: 159. DB::Exception: Timeout exceeded: elapsed 126.147987 ms, maximum: 100 ms. (TIMEOUT_EXCEEDED) (version 25.12.1.649 (official build))\n142 rnywyenlaeqynhmu\r\n__exception__\r\n";
    let error = "Code: 159. DB::Exception: Timeout exceeded: elapsed 126.147987 ms, maximum: 100 ms. (TIMEOUT_EXCEEDED) (version 25.12.1.649 (official build))";

    let err = extract_exception(chunk, Some(tag)).expect("failed to extract exception");
    assert_eq!(err.to_string(), format!("bad response: {error}"));
}
