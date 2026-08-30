//! The issue #474 byte-frozen reference capture, loaded by both legs of
//! the nullable-submessage differential.
//!
//! Included via `#[path = "support/wire_capture.rs"] mod wire_capture;` —
//! a `tests/` subdirectory, so cargo never builds this file as its own
//! test binary (same layout as `support/live_db.rs` and
//! `support/manifest.rs`).
//!
//! ## Why this is shared and nothing else is
//!
//! Two legs drive two different servers: the oracle leg replays the
//! capture against the pinned reference build over `curl`, and the
//! PulsusDB leg replays it against a spawned `pulsusdb` from inside
//! `traces_api_live.rs`, where the raw-socket helpers it needs already
//! live. Two drivers is weaker than one, so it matters that **neither leg
//! compares itself to the other** — both compare to the hex in this
//! artifact, so a driver that mangled bytes fails its own leg rather than
//! agreeing with the opposite one.
//!
//! ## What the artifact holds
//!
//! `push_body_hex` is the exact `ExportTraceServiceRequest` pushed to BOTH
//! stores. Each probe carries the trace id, the reference's `GET
//! /api/traces/{id}` protobuf body, and — for the one probe fetched
//! through v2 as well — the reference's `trace` FIELD bytes and the
//! absent-trace envelope. The reference's `metrics` field is deliberately
//! absent from this file: its counter is not stable between two fetches of
//! the same trace, so there is nothing to freeze (ledger row
//! `traces-v2-fetch-metrics-not-populated`).

#![allow(dead_code)]

use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Provenance {
    /// The digest-pinned reference image, as spelled in
    /// `.github/workflows/ci.yml`.
    pub image: String,
    /// The repo-relative config the container was booted on.
    pub config: String,
    pub captured: String,
}

#[derive(Debug, Deserialize)]
pub struct Probe {
    /// `T1`..`T4`.
    pub name: String,
    /// 32 lowercase hex characters.
    pub trace_id: String,
    /// What the sender left absent, for the failure message.
    pub absent: String,
    /// The reference's `GET /api/traces/{id}` body under
    /// `Accept: application/protobuf`.
    pub v1_protobuf_hex: String,
    /// The reference's v2 `trace` field CONTENTS (field 1's submessage,
    /// without its key or length prefix), or `null` when this probe is not
    /// fetched through v2.
    pub v2_trace_field_hex: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Capture {
    pub provenance: Provenance,
    /// The `ExportTraceServiceRequest` pushed to both stores.
    pub push_body_hex: String,
    /// The reference's whole `GET /api/v2/traces/{absent}` protobuf body.
    pub v2_absent_protobuf_hex: String,
    /// The reference's whole `GET /api/v2/traces/{absent}` JSON body.
    pub v2_absent_json: String,
    /// A trace id no probe uses, for the absent-trace cells.
    pub absent_trace_id: String,
    pub probes: Vec<Probe>,
}

impl Capture {
    pub fn probe(&self, name: &str) -> &Probe {
        self.probes
            .iter()
            .find(|p| p.name == name)
            .unwrap_or_else(|| panic!("capture has no probe named {name}"))
    }
}

/// `crates/pulsus-server/tests/fixtures/trace_nullable_wire/capture.json`.
pub fn capture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/trace_nullable_wire/capture.json")
}

pub fn load() -> Capture {
    let path = capture_path();
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// Bytes of a lowercase hex string.
pub fn from_hex(hex: &str) -> Vec<u8> {
    assert!(hex.len().is_multiple_of(2), "odd-length hex in the capture");
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex in the capture"))
        .collect()
}

/// Lowercase hex of `bytes` — used only to build failure messages.
pub fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
