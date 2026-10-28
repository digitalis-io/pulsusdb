//! The setup form's generator (issue #746 part C).
//!
//! [`Inputs::from_pairs`] reads the form's fields, [`validate`] applies the
//! form's own rules and then `pulsus_schema::ctx_from_vars` for every rule
//! the schema decides, and [`bundle`] writes the files. Nothing here reads
//! a file, opens a connection or holds a secret.

use std::path::Path;

use serde::Serialize;

mod inputs;
mod server_xml;
mod steps;
mod vars;

pub use inputs::{FieldError, Inputs, LevelRow, Tls, Totals, Validated, validate};

/// One generated file: its path inside the bundle, and its text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct File {
    pub path: String,
    pub content: String,
}

/// Every file of the bundle, in the order the page lists them.
pub fn bundle(_v: &Validated) -> Vec<File> {
    Vec::new()
}

/// Writes `files` under `dir`, creating the directories they need.
pub fn write_bundle(_dir: &Path, _files: &[File]) -> std::io::Result<()> {
    Ok(())
}

/// The page's lines for what the schema keeps: raw data, each level, then
/// the due-time ceiling.
pub fn total_lines(_v: &Validated) -> Vec<String> {
    Vec::new()
}

/// The figures the server derives from the hardware, shown and not
/// written: (label, value).
pub fn sizing(_v: &Validated) -> Vec<(&'static str, String)> {
    Vec::new()
}

/// The issue's example as form pairs: six hosts in two shards of three,
/// three Keeper hosts, TLS, object storage, raw data 7 days local and 90 on
/// object storage, four levels kept 365 days on local disk.
pub fn example_pairs() -> Vec<(String, String)> {
    let mut pairs = vec![
        ("hosts", "ch1\nch2\nch3\nch4\nch5\nch6"),
        ("keepers", "kp1\nkp2\nkp3"),
        ("shards", "2"),
        ("replicas", "3"),
        ("cluster", "pulsus_cluster"),
        ("database", "pulsus"),
        ("tls_cert", "/etc/ch/server.crt"),
        ("tls_key", "/etc/ch/server.key"),
        ("tls_ca", "/etc/ch/ca.crt"),
        ("local_path", "/var/lib/clickhouse/"),
        ("remote_url", "https://bucket.example/pulsus/"),
        ("cores", "32"),
        ("memory_gib", "128"),
        ("disk_gib", "2048"),
        ("raw_local_days", "7"),
        ("raw_remote_days", "90"),
        ("l1_width", "60"),
        ("l1_local", "365"),
        ("l2_width", "720"),
        ("l2_from", "86400"),
        ("l2_local", "365"),
        ("l3_width", "7200"),
        ("l3_from", "604800"),
        ("l3_local", "365"),
        ("l4_width", "86400"),
        ("l4_from", "7776000"),
        ("l4_local", "365"),
    ];
    pairs.sort_by_key(|(k, _)| *k);
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
