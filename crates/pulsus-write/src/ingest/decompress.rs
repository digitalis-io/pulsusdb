//! Request-body decompression for `/v1/logs` (architect plan): hand-rolled
//! gzip/zstd/snappy/identity handling rather than `tower-http`'s
//! compression layer, which covers gzip only — not snappy
//! (docs/architecture.md's compression dependency table: `snap` for
//! "snappy for remote write/OTLP").

use std::io::Read;

use crate::error::LogsIngestError;

/// Decompressed-body cap (architect plan amendment 2, task-manager
/// resolution): a documented constant, not configurable — the zip-bomb
/// guard applied to every `Content-Encoding`, including `identity`.
/// Promote to a config variable only when a deployment actually needs a
/// different limit.
pub const MAX_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

/// The `Content-Encoding` values this receiver understands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Identity,
    Gzip,
    Zstd,
    Snappy,
}

impl Encoding {
    /// Maps a non-empty `Content-Encoding` header value (case-insensitive,
    /// per RFC 9110) to the [`Encoding`] it names. An absent header /
    /// empty value is the caller's responsibility to treat as `identity`
    /// — this only parses a value that is actually present.
    pub fn from_header_value(value: &str) -> Result<Encoding, LogsIngestError> {
        let trimmed = value.trim();
        if trimmed.eq_ignore_ascii_case("identity") {
            Ok(Encoding::Identity)
        } else if trimmed.eq_ignore_ascii_case("gzip") {
            Ok(Encoding::Gzip)
        } else if trimmed.eq_ignore_ascii_case("zstd") {
            Ok(Encoding::Zstd)
        } else if trimmed.eq_ignore_ascii_case("snappy") {
            Ok(Encoding::Snappy)
        } else {
            Err(LogsIngestError::UnsupportedEncoding(trimmed.to_string()))
        }
    }
}

/// Decompresses `body` per `encoding`, rejecting anything that would
/// exceed [`MAX_DECOMPRESSED_BYTES`] once decompressed (the zip-bomb
/// guard) rather than allocating an unbounded buffer first.
pub fn decompress(encoding: Encoding, body: &[u8]) -> Result<Vec<u8>, LogsIngestError> {
    match encoding {
        Encoding::Identity => {
            if body.len() > MAX_DECOMPRESSED_BYTES {
                return Err(LogsIngestError::OversizeBody {
                    limit: MAX_DECOMPRESSED_BYTES,
                });
            }
            Ok(body.to_vec())
        }
        Encoding::Gzip => read_capped(flate2::read::GzDecoder::new(body), "gzip"),
        Encoding::Zstd => {
            let decoder = zstd::stream::read::Decoder::new(body).map_err(|source| {
                LogsIngestError::Decompress {
                    encoding: "zstd",
                    reason: source.to_string(),
                }
            })?;
            read_capped(decoder, "zstd")
        }
        Encoding::Snappy => decompress_snappy(body),
    }
}

/// Reads `reader` to the end, capped at [`MAX_DECOMPRESSED_BYTES`] + 1
/// bytes: gzip/zstd streams do not declare their decompressed size up
/// front, so the only safe zip-bomb guard is bounding the read itself
/// rather than the eventual buffer length — `Read::take` stops pulling
/// bytes from the (potentially still-expanding) decoder the instant the
/// cap is exceeded.
fn read_capped(reader: impl Read, encoding: &'static str) -> Result<Vec<u8>, LogsIngestError> {
    let mut limited = reader.take(MAX_DECOMPRESSED_BYTES as u64 + 1);
    let mut out = Vec::new();
    limited
        .read_to_end(&mut out)
        .map_err(|source| LogsIngestError::Decompress {
            encoding,
            reason: source.to_string(),
        })?;
    if out.len() > MAX_DECOMPRESSED_BYTES {
        return Err(LogsIngestError::OversizeBody {
            limit: MAX_DECOMPRESSED_BYTES,
        });
    }
    Ok(out)
}

/// Raw (unframed) Snappy block decompression — the format OTLP/remote-
/// write-style snappy encoding uses. Unlike the streaming gzip/zstd path,
/// a raw Snappy block declares its uncompressed length up front
/// ([`snap::raw::decompress_len`]), so the zip-bomb guard checks the
/// declared length *before* allocating the output buffer.
fn decompress_snappy(body: &[u8]) -> Result<Vec<u8>, LogsIngestError> {
    let declared_len =
        snap::raw::decompress_len(body).map_err(|source| LogsIngestError::Decompress {
            encoding: "snappy",
            reason: source.to_string(),
        })?;
    if declared_len > MAX_DECOMPRESSED_BYTES {
        return Err(LogsIngestError::OversizeBody {
            limit: MAX_DECOMPRESSED_BYTES,
        });
    }
    let mut decoder = snap::raw::Decoder::new();
    decoder
        .decompress_vec(body)
        .map_err(|source| LogsIngestError::Decompress {
            encoding: "snappy",
            reason: source.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn zstd_compress(data: &[u8]) -> Vec<u8> {
        zstd::stream::encode_all(data, 1).unwrap()
    }

    fn snappy(data: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(data).unwrap()
    }

    #[test]
    fn from_header_value_is_case_insensitive() {
        assert_eq!(Encoding::from_header_value("GZIP").unwrap(), Encoding::Gzip);
        assert_eq!(Encoding::from_header_value("Zstd").unwrap(), Encoding::Zstd);
        assert_eq!(
            Encoding::from_header_value("SNAPPY").unwrap(),
            Encoding::Snappy
        );
        assert_eq!(
            Encoding::from_header_value("Identity").unwrap(),
            Encoding::Identity
        );
    }

    #[test]
    fn from_header_value_rejects_unknown_encodings() {
        let err = Encoding::from_header_value("br").unwrap_err();
        assert!(matches!(err, LogsIngestError::UnsupportedEncoding(v) if v == "br"));
    }

    #[test]
    fn identity_passthrough_returns_body_unchanged() {
        let body = b"hello world";
        assert_eq!(decompress(Encoding::Identity, body).unwrap(), body);
    }

    #[test]
    fn identity_rejects_a_body_already_over_the_cap() {
        let body = vec![0u8; MAX_DECOMPRESSED_BYTES + 1];
        let err = decompress(Encoding::Identity, &body).unwrap_err();
        assert!(matches!(err, LogsIngestError::OversizeBody { .. }));
    }

    #[test]
    fn gzip_round_trips() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let compressed = gzip(data);
        assert_eq!(decompress(Encoding::Gzip, &compressed).unwrap(), data);
    }

    #[test]
    fn gzip_rejects_corrupt_input() {
        let err = decompress(Encoding::Gzip, b"not a gzip stream").unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::Decompress {
                encoding: "gzip",
                ..
            }
        ));
    }

    #[test]
    fn gzip_rejects_a_stream_that_decompresses_past_the_cap() {
        let data = vec![0u8; MAX_DECOMPRESSED_BYTES + 1024];
        let compressed = gzip(&data);
        let err = decompress(Encoding::Gzip, &compressed).unwrap_err();
        assert!(matches!(err, LogsIngestError::OversizeBody { .. }));
    }

    #[test]
    fn zstd_round_trips() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let compressed = zstd_compress(data);
        assert_eq!(decompress(Encoding::Zstd, &compressed).unwrap(), data);
    }

    #[test]
    fn zstd_rejects_corrupt_input() {
        let err = decompress(Encoding::Zstd, b"not a zstd frame").unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::Decompress {
                encoding: "zstd",
                ..
            }
        ));
    }

    #[test]
    fn zstd_rejects_a_stream_that_decompresses_past_the_cap() {
        let data = vec![0u8; MAX_DECOMPRESSED_BYTES + 1024];
        let compressed = zstd_compress(&data);
        let err = decompress(Encoding::Zstd, &compressed).unwrap_err();
        assert!(matches!(err, LogsIngestError::OversizeBody { .. }));
    }

    #[test]
    fn snappy_round_trips() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let compressed = snappy(data);
        assert_eq!(decompress(Encoding::Snappy, &compressed).unwrap(), data);
    }

    #[test]
    fn snappy_rejects_corrupt_input() {
        let err = decompress(Encoding::Snappy, b"\xFF\xFF\xFF\xFF garbage").unwrap_err();
        assert!(matches!(
            err,
            LogsIngestError::Decompress {
                encoding: "snappy",
                ..
            }
        ));
    }

    #[test]
    fn snappy_rejects_a_declared_length_past_the_cap_without_allocating() {
        let data = vec![0u8; MAX_DECOMPRESSED_BYTES + 1024];
        let compressed = snappy(&data);
        let err = decompress(Encoding::Snappy, &compressed).unwrap_err();
        assert!(matches!(err, LogsIngestError::OversizeBody { .. }));
    }
}
