//! Issue #471 M2: the request-deadline path partition, pinned as a
//! visible, checkable list.
//!
//! Hermetic — no server, no ClickHouse. Runs under the plain `ci` job's
//! `cargo test --workspace` / `cargo nextest run --workspace`, not behind
//! `PULSUS_TEST_CLICKHOUSE`.
//!
//! ## Why a list rather than a mechanism
//!
//! A request deadline is answered two ways: the PromQL query surface gets
//! `503` plus the three-field JSON error envelope, and everything else
//! keeps the bare `408` it has always had. That is a claim about a **set**,
//! and its failure mode is a route added later landing silently on the
//! wrong side — which no assertion about one path can see. So the set is
//! written out here and compared against the route manifest in **both**
//! directions: no unclassified path, no extra path.
//!
//! `middleware::deadline_class` itself is not reachable from an
//! integration test (`pulsus-server` has no library target), so the link
//! between this list and production is `DEADLINE_BARE_PATHS`, which is
//! source-scanned out of `middleware.rs` below and compared with the
//! manifest's own answer. The predicate that reads it is unit-tested
//! inside the crate by
//! `middleware::tests::deadline_class_partitions_every_mounted_api_v1_path`.
//!
//! ## Two things this cannot see, recorded rather than closed
//!
//! 1. **`route_manifest()` carries planned routes as well as mounted
//!    ones** — `/api/v1/rules` is `Planned` right now — so every filter
//!    here is on `RouteStatus::Mounted` and the tables describe **mounted**
//!    routes only.
//! 2. **A macro-generated route registration evades the source scanner by
//!    construction**, exactly as `route_inventory.rs`'s own threat model
//!    already records for itself. A route added that way is absent from
//!    the manifest, absent from these tables, and lands on the default
//!    side (bare `408`) with every gate green. That residual is written
//!    down deliberately instead of being chased with a further mechanism.

#[path = "support/manifest.rs"]
mod manifest;
#[path = "support/source_scan.rs"]
mod source_scan;

use std::collections::BTreeSet;

use manifest::{RouteStatus, Surface, route_manifest};
use source_scan::{preprocess_views, workspace_root};

/// The twelve mounted `/api/v1/*` query routes, which answer a deadline
/// breach with `503` + the PromQL error envelope.
const ENVELOPE_PATHS: &[&str] = &[
    "/api/v1/query",
    "/api/v1/query_range",
    "/api/v1/labels",
    "/api/v1/label/{name}/values",
    "/api/v1/series",
    "/api/v1/metadata",
    "/api/v1/query_exemplars",
    "/api/v1/status/buildinfo",
    "/api/v1/status/config",
    "/api/v1/status/flags",
    "/api/v1/status/runtimeinfo",
    "/api/v1/status/tsdb",
];

/// The mounted `/api/v1/*` paths that KEEP the bare `408`. Exactly one,
/// and it is the reason the rule cannot be a bare prefix test:
/// `/api/v1/write` is remote-write **ingest**, not a query surface.
const BARE_PATHS: &[&str] = &["/api/v1/write"];

fn set(paths: &[&str]) -> BTreeSet<String> {
    paths.iter().map(|p| (*p).to_string()).collect()
}

fn mounted_api_v1() -> Vec<&'static manifest::RouteSpec> {
    route_manifest()
        .iter()
        .filter(|r| r.status == RouteStatus::Mounted)
        .filter(|r| r.path.starts_with("/api/v1/"))
        .collect()
}

/// The string literals inside `middleware.rs`'s `DEADLINE_BARE_PATHS`,
/// read out of the source rather than re-declared here. Comments are
/// blanked first by the shared lexer, so a commented-out entry cannot be
/// counted.
fn scanned_bare_paths() -> BTreeSet<String> {
    let file = workspace_root().join("crates/pulsus-server/src/middleware.rs");
    let src = std::fs::read_to_string(&file).unwrap_or_else(|e| panic!("read {file:?}: {e}"));
    let (stripped, _) = preprocess_views(&src);
    let decl = stripped
        .find("DEADLINE_BARE_PATHS")
        .expect("middleware.rs must declare DEADLINE_BARE_PATHS");
    // Anchor on the `=` first: the declaration's TYPE is `&[&str]`, so a
    // naive search for the next `[` lands inside the type and reads an
    // empty body — which is what the emptiness assertion below caught.
    let eq = stripped[decl..]
        .find('=')
        .expect("DEADLINE_BARE_PATHS must be initialised")
        + decl;
    let open = stripped[eq..]
        .find('[')
        .expect("DEADLINE_BARE_PATHS must be a slice literal")
        + eq;
    let close = stripped[open..]
        .find(']')
        .expect("DEADLINE_BARE_PATHS's slice literal must close")
        + open;
    let body = &stripped[open + 1..close];
    let mut out = BTreeSet::new();
    let mut rest = body;
    while let Some(a) = rest.find('"') {
        let tail = &rest[a + 1..];
        let b = tail
            .find('"')
            .expect("unterminated literal in DEADLINE_BARE_PATHS");
        out.insert(tail[..b].to_string());
        rest = &tail[b + 1..];
    }
    assert!(
        !out.is_empty(),
        "the scan found no literals in DEADLINE_BARE_PATHS — it has stopped reading the constant"
    );
    out
}

/// (a) Every mounted `Surface::PromApi` route is in the envelope list, and
/// every path in the envelope list is a mounted `Surface::PromApi` route.
#[test]
fn the_envelope_paths_are_exactly_the_mounted_prom_api_routes() {
    let from_manifest: BTreeSet<String> = mounted_api_v1()
        .iter()
        .filter(|r| r.surface == Surface::PromApi)
        .map(|r| r.path.to_string())
        .collect();
    assert_eq!(from_manifest, set(ENVELOPE_PATHS));
    assert_eq!(from_manifest.len(), 12);
}

/// (b) Every mounted non-PromQL route under `/api/v1/` is in the bare
/// list, and vice versa. This is the direction that catches an ingest
/// route added under the prefix without being excluded.
#[test]
fn the_bare_paths_are_exactly_the_mounted_non_prom_api_routes_under_the_prefix() {
    let from_manifest: BTreeSet<String> = mounted_api_v1()
        .iter()
        .filter(|r| r.surface != Surface::PromApi)
        .map(|r| r.path.to_string())
        .collect();
    assert_eq!(from_manifest, set(BARE_PATHS));
}

/// (c) The production constant agrees with (b). Without this the two
/// tables above would be a private opinion about a set that production
/// never reads.
#[test]
fn the_production_exclusion_constant_matches_the_manifest() {
    assert_eq!(scanned_bare_paths(), set(BARE_PATHS));
}

/// The partition is total and disjoint over the mounted `/api/v1/` set,
/// classified with production's own rule — prefix test minus the scanned
/// exclusion constant — rather than with a hand list.
#[test]
fn the_two_classes_partition_the_mounted_prefix_exactly_once() {
    let bare = scanned_bare_paths();
    let mut envelope = BTreeSet::new();
    let mut kept_bare = BTreeSet::new();
    for route in mounted_api_v1() {
        if bare.contains(route.path) {
            kept_bare.insert(route.path.to_string());
        } else {
            envelope.insert(route.path.to_string());
        }
    }
    assert_eq!(envelope, set(ENVELOPE_PATHS));
    assert_eq!(kept_bare, set(BARE_PATHS));
    assert!(envelope.is_disjoint(&kept_bare));
    assert_eq!(envelope.len() + kept_bare.len(), mounted_api_v1().len());
}
