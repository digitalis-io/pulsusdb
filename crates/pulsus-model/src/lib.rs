//! Data model fundamentals: labels, fingerprints, samples, series, and time
//! types shared across ingestion and query engines. See
//! docs/architecture.md §2.

mod canonical;
mod fingerprint;
mod labels;
mod matcher;
mod sample;
mod time;

pub use canonical::{METRIC_NAME_LABEL, SERVICE_NAME_LABEL, canonicalize_label_key};
pub use fingerprint::{
    build_metric_buffer, build_stream_buffer, metric_fingerprint, raw_cityhash64,
    stream_fingerprint,
};
pub use labels::{LabelError, LabelSet};
pub use matcher::{LabelMatcher, MatchOp};
pub use sample::{LogSample, MetricSample, STALE_NAN_BITS, Series};
pub use time::{
    DEFAULT_ACTIVITY_BUCKET_MS, Date, Fingerprint, UnixMilli, UnixNano, floor_to_activity_bucket,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
