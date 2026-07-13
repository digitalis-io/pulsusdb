//! Data model fundamentals: labels, fingerprints, samples, series, and time
//! types shared across ingestion and query engines. See
//! docs/architecture.md §2.

mod canonical;
mod fingerprint;
mod labels;
mod sample;
mod time;

pub use canonical::{METRIC_NAME_LABEL, SERVICE_NAME_LABEL, canonicalize_label_key};
pub use fingerprint::{
    build_metric_buffer, build_stream_buffer, metric_fingerprint, raw_cityhash64,
    stream_fingerprint,
};
pub use labels::{LabelError, LabelSet};
pub use sample::{LogSample, MetricSample, Series};
pub use time::{Date, Fingerprint, UnixMilli, UnixNano};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
