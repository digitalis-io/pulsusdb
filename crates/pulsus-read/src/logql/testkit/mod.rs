//! Test-only helpers shared by more than one of the `logql` region's
//! `#[cfg(test)] mod tests` (issue #299).
//!
//! A SUBDIRECTORY, never a flat `.rs`: both directory censuses over
//! `src/logql/` are non-recursive and filter on the `.rs` extension, so a
//! flat test-only file would be walked as production source while a
//! subdirectory is invisible to them by construction.

use super::rows::{SampleRow, StreamMetaRow};
use pulsus_logql::VectorAggOp;
use std::collections::HashMap;

use super::agg::LabelSet;
use super::window::ClientWindow;

// ---- Issue #227: sliding-window range engine ----

pub(in crate::logql) fn slide_meta(fp: u64, labels_json: &str) -> HashMap<u64, StreamMetaRow> {
    let mut m = HashMap::new();
    m.insert(
        fp,
        StreamMetaRow {
            fingerprint: fp,
            service: "svc".to_string(),
            labels: labels_json.to_string(),
        },
    );
    m
}

/// Builds a RANGE `ClientWindow` through the real validation funnel —
/// tests cannot fabricate an unvalidated duration either (issue #227
/// review round 3).
pub(in crate::logql) fn slide_window(
    start_ns: i64,
    end_ns: i64,
    step_ns: u64,
    range_ns: u64,
) -> ClientWindow {
    ClientWindow::Range {
        grid_start_ns: start_ns,
        end_ns,
        step_ns: super::params::validate_duration_ns(step_ns, "step").expect("valid step"),
        range_ns: super::params::validate_duration_ns(range_ns, "range selector")
            .expect("valid range"),
    }
}

pub(in crate::logql) fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
    list.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

pub(in crate::logql) fn sample(fp: u64, ts: i64, body: &str) -> SampleRow {
    SampleRow {
        fingerprint: fp,
        timestamp_ns: ts,
        body: body.to_string(),
        structured_metadata: String::new(),
    }
}

// ---- Issue #238: reserved structured-metadata routing (`Add`,
// `labels.go:392-412`) and the no-pipeline fast-path gate. Rows carry
// their Delta C''.3 ids; every expected set is a literal reference
// capture (grafana/loki:3.7.4, `discover_log_levels: false`). The
// pipeline-path C-rows live in `pipeline.rs`'s tests against these
// exact (merged base, ctx) pairs. ----

pub(in crate::logql) fn owned_pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    out.sort();
    out
}

pub(in crate::logql) const VSEC: i64 = 1_000_000_000;

// -----------------------------------------------------------------
// Issue #236 Part B — the streaming fold.
// -----------------------------------------------------------------

pub(in crate::logql) const REDUCING_OPS: [VectorAggOp; 7] = [
    VectorAggOp::Sum,
    VectorAggOp::Avg,
    VectorAggOp::Min,
    VectorAggOp::Max,
    VectorAggOp::Count,
    VectorAggOp::Stddev,
    VectorAggOp::Stdvar,
];

/// A leaf series as the fold receives it: labels plus grid-aligned
/// `(timestamp, value)` points.
pub(in crate::logql) type FoldInput = (LabelSet, Vec<(i64, f64)>);

pub(in crate::logql) fn fold_labels(pairs: &[(&str, &str)]) -> LabelSet {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
