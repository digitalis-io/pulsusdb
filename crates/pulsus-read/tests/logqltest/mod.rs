//! Issue #220 (LogQL value-differential corpus, Batch 0): the shared
//! `logqltest` driver — a promqltest-style native replayer for LogQL
//! `.test` files against the pure LogQL value evaluator in `pulsus-read`
//! (`CompiledPipeline`, `run_client_agg_rows`, `apply_vector_aggs`,
//! `combine_binary`, `plan`). Mirrors `crates/pulsus-promql/tests/promqltest`
//! structurally: a `load` dataset block plus `eval instant at T <query>`
//! assertions, replayed hermetically with EXACT-f64 equality (the #218
//! lesson — no tolerance).
//!
//! No ClickHouse ever. The pinned reference container
//! (`grafana/loki:3.7.4`) is touched only ONCE per new case to CAPTURE the
//! expected value — never at test time. See `PROVENANCE.md` for the
//! capture procedure.
//!
//! Shared-test-module convention: like `pulsus-promql`'s `promqltest`
//! module, this is compiled into a test binary (`logqltest_corpus.rs`)
//! that uses a subset of it, so `dead_code` is allowed here for the
//! extensible surface later batches (B1–B6) build on, not to hide
//! genuinely unused logic.
#![allow(dead_code)]

pub mod runner;

use std::path::{Path, PathBuf};

/// `crates/pulsus-read/tests/logqltest` — the corpus root.
pub fn base_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("logqltest")
}

/// The `.test` corpus directory.
pub fn corpus_dir() -> PathBuf {
    base_dir().join("corpus")
}

/// Reads a corpus file, panicking with a legible path on error.
pub fn read_file(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}
