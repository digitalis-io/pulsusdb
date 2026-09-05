//! Issue #381 — which of several structured-metadata pairs sharing a stored
//! name wins, held by a committed raw capture of the pinned reference.
//!
//! **Where the expectations come from.** No reference VALUE is written in
//! this file. Every expectation is DERIVED, at test time, from
//! `tests/fixtures/structured_metadata_collisions/capture.json` — the raw
//! `/loki/api/v1/query_range` response bodies of the pinned reference
//! container (`grafana/loki:3.7.4`, booted on `ci/logql/config.yaml`), each
//! probe pushed through `/loki/api/v1/push` under a per-run nonce'd `app`
//! stream label and read back with
//! `X-Loki-Response-Encoding-Flags: categorize-labels`. The artifact also
//! records the container's `/loki/api/v1/status/buildinfo` answer.
//! Refreshing it CANNOT be done by hand: the only writer is the regeneration
//! mode of [`the_committed_capture_matches_the_live_reference`], which
//! requires a live container reporting exactly the pinned version AND
//! revision:
//!
//! ```text
//! podman run -d --name pulsus-sm-cap -p 13381:3100 \
//!     -v $PWD/ci/logql/config.yaml:/etc/loki/local-config.yaml:ro \
//!     grafana/loki:3.7.4 -config.file=/etc/loki/local-config.yaml
//! PULSUSDB_LOGQL_DIFF_URL=http://localhost:13381 \
//!     PULSUS_REGEN_SM_COLLISION_CAPTURE=1 \
//!     cargo test -p pulsus-write --test structured_metadata_collisions -- --nocapture
//! ```
//!
//! then review the diff. The same test WITHOUT the regen variable is the
//! drift leg, which CI runs against the digest-pinned differential oracle.
//! [`artifact_probe_set_is_exactly_the_source_probe_set`] pins the artifact's
//! probes (id, pushed pairs, line, query) to the table below.
//!
//! **The rule the capture agrees with.** Loki's distributor runs an entry's
//! structured metadata through Prometheus' `labels.Builder`
//! (`pkg/distributor/distributor.go:697-722 @ v3.7.4`); the primitives and
//! their source lines are transcribed at
//! `pulsus_model::resolve_structured_metadata`, which is the function under
//! test here. In one sentence: a pair that was `Set` — renamed, or carrying
//! `utf8.RuneError` — beats a pair that was not, wherever either sits in wire
//! order; among pairs `Set` onto one name the last wins; an empty value is a
//! `Del`. It is NOT plain last-write-wins, which cannot explain why both wire
//! orders of `{a.b="x", a_b="keep"}` store `a_b="x"`.
//!
//! **The transport is the JSON push encoding**, whose structured-metadata
//! object keeps duplicate keys as raw pairs on both sides
//! (`pkg/loghttp/query.go:181-196 @ v3.7.4`, and `BoundedStructuredMetadata`
//! here) — so the probe bodies are assembled as text rather than through a
//! map. The snappy-protobuf encoding's agreement with this one is asserted
//! hermetically by `loki_push.rs`'s
//! `both_push_encodings_resolve_a_metadata_collision_identically`.
//!
//! **The `was` column** records what PulsusDB stored at `b872855`, the commit
//! before this fix. It is never asserted as an expectation — only as
//! DIFFERING from (or agreeing with) the derived reference answer, by
//! [`the_table_discriminates_the_pre_fix_resolution`], which pins how many
//! rows each way so the table cannot quietly go vacuous.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pulsus_write::{LevelDiscovery, parse_loki_json};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// ---------------------------------------------------------------------
// The probe table — the SOURCE of what is probed, never of what to expect.
// ---------------------------------------------------------------------

/// Whether the reference can SERVE the entry it accepted. `f04` is the one
/// row where it cannot: the push is a 204 and the read is a 500 (residual B
/// of this issue, docs/benchmarks/logs-differential-ledger.md).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Readable {
    Yes,
    ReferenceReadFails,
}

struct SourceProbe {
    id: &'static str,
    /// The entry's structured metadata, in WIRE order, raw names.
    sm: &'static [(&'static str, &'static str)],
    /// The canonical JSON PulsusDB stored at `b872855`. Asserted only as a
    /// discriminator, never as an expectation.
    was: &'static str,
    readable: Readable,
}

const fn probe(
    id: &'static str,
    sm: &'static [(&'static str, &'static str)],
    was: &'static str,
) -> SourceProbe {
    SourceProbe {
        id,
        sm,
        was,
        readable: Readable::Yes,
    }
}

/// U+FFFD, `utf8.RuneError` — the rune the distributor rewrites to a space
/// (`removeInvalidUtf`, `pkg/distributor/distributor.go:75-80 @ v3.7.4`).
const R: &str = "\u{FFFD}";

/// The `c` rows cross the collision axes: renamed-vs-not, one-vs-both-
/// renamed, wire order, a repeated canonical name, three-way collisions, an
/// empty value in the group, and an untouched bystander. The `f` rows add the
/// U+FFFD `Set`, which is what decides `f01`/`f02` and which cannot be
/// omitted without resolving them to the wrong pair.
#[rustfmt::skip]
fn source_probes() -> Vec<SourceProbe> {
    vec![
        probe("c01", &[("a.b", "x"), ("a_b", "keep")],                 r#"{"a_b":"keep"}"#),
        probe("c02", &[("a_b", "keep"), ("a.b", "x")],                 r#"{"a_b":"keep"}"#),
        probe("c03", &[("a.b", "1"), ("a-b", "2")],                    r#"{"a_b":"1"}"#),
        probe("c04", &[("a-b", "2"), ("a.b", "1")],                    r#"{"a_b":"1"}"#),
        probe("c05", &[("a_b", "1"), ("a_b", "2")],                    r#"{"a_b":"2"}"#),
        probe("c06", &[("a_b", "2"), ("a_b", "1")],                    r#"{"a_b":"2"}"#),
        probe("c07", &[("a_b", "1"), ("a_b", "2"), ("a.b", "9")],      r#"{"a_b":"2"}"#),
        probe("c08", &[("a.b", "1"), ("a_b", "2"), ("a.b", "3")],      r#"{"a_b":"2"}"#),
        probe("c09", &[("a.b", "1"), ("a_b", "2"), ("a-b", "3")],      r#"{"a_b":"2"}"#),
        probe("c10", &[("a-b", "3"), ("a_b", "2"), ("a.b", "1")],      r#"{"a_b":"2"}"#),
        probe("c11", &[("a.b", "1"), ("a.b", "2")],                    r#"{"a_b":"2"}"#),
        probe("c16", &[("a_b", ""), ("a.b", "x"), ("a-b", "y")],       r#"{"a_b":"x"}"#),
        probe("c17", &[("a.b", "x"), ("a_b", "keep"), ("z", "1")],     r#"{"a_b":"keep","z":"1"}"#),
        probe("c18", &[("a.b", "9"), ("a_b", "1"), ("a_b", "2")],      r#"{"a_b":"2"}"#),
        probe("f01", &[("a.b", "x"), ("a_b", "p\u{FFFD}")],            r#"{"a_b":"p�"}"#),
        probe("f02", &[("a_b", "p\u{FFFD}"), ("a.b", "x")],            r#"{"a_b":"p�"}"#),
        probe("f03", &[("a_b", "p\u{FFFD}q")],                         r#"{"a_b":"p�q"}"#),
        SourceProbe {
            id: "f04",
            sm: &[("a_b", "1"), ("a_b", "p\u{FFFD}")],
            was: r#"{"a_b":"p�"}"#,
            readable: Readable::ReferenceReadFails,
        },
    ]
}

/// Every probe pushes the same line, so the response's echo binds it.
const PROBE_LINE: &str = "line";

// ---------------------------------------------------------------------
// The committed artifact.
// ---------------------------------------------------------------------

const ARTIFACT_IMAGE: &str = "grafana/loki:3.7.4";
const ARTIFACT_VERSION: &str = "3.7.4";
/// The reference commit this repo's conformance suites are pinned to.
const ARTIFACT_REVISION: &str = "b318f282";

/// Structured-metadata names the container ADDS at ingest — `ci/logql/
/// config.yaml` leaves `discover_log_levels` at its shipped default, which
/// appends `detected_level` to every entry.
///
/// **This list no longer filters the derived expectation** (issue #483).
/// PulsusDB now appends the same pair, so the full stored string is
/// comparable and [`the_stored_string_reproduces_the_reference_capture`]
/// compares it — `"detected_level":"unknown"` included, which every probe
/// line here answers. The list survives as the extractor's PRESENCE check
/// (its absence would mean the response is not an ingest capture) and as
/// what [`no_probe_name_collides_with_an_elided_ingest_label`] asserts no
/// probe name canonicalizes onto, and it is still applied to the
/// pre-fix-discrimination count — see
/// [`the_table_discriminates_the_pre_fix_resolution`] for why that one
/// comparison must stay on the collision-relevant subset.
const ELIDED_SM_NAMES: &[&str] = &["detected_level"];

/// Which comparison a derived expectation is for: the whole stored string,
/// or the collision-relevant subset alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Elision {
    /// Compare everything the reference stored, `detected_level` included.
    None,
    /// Drop the names the container adds at ingest.
    IngestAdded,
}

#[derive(Serialize, Deserialize)]
struct Artifact {
    image: String,
    config: String,
    push_endpoint: String,
    query_endpoint: String,
    read_headers: Vec<String>,
    /// The container's `/loki/api/v1/status/buildinfo` response.
    buildinfo: Value,
    captured_at_unix: u64,
    probes: Vec<ArtifactProbe>,
}

#[derive(Serialize, Deserialize)]
struct ArtifactProbe {
    id: String,
    /// The `app` stream label the probe was pushed under (nonce'd).
    app: String,
    /// The structured metadata as pushed, in wire order — duplicate names
    /// included, which is why this is a list and not a map.
    sm: Vec<(String, String)>,
    line: String,
    query: String,
    push_status: u16,
    read_status: u16,
    /// The raw `query_range` response body. A body that does not parse as
    /// JSON — the reference's 500 for `f04` is plain text — is recorded as a
    /// JSON string.
    response: Value,
}

fn artifact_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/structured_metadata_collisions/capture.json")
}

fn load_artifact() -> Artifact {
    let path = artifact_path();
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

// ---------------------------------------------------------------------
// Deriving the reference's answer from a raw response.
// ---------------------------------------------------------------------

/// The canonical JSON PulsusDB would store for a `(name, value)` map — sorted
/// keys, `serde_json` escaping, and `""` for the empty map (the seam's
/// no-structured-metadata sentinel). Spelled out here rather than borrowed
/// from `LabelSet::to_canonical_json` so the expectation's SHAPE is
/// independent of the code under test; the two agreeing is what
/// [`the_stored_string_reproduces_the_reference_capture`] asserts.
fn canonical_json(map: &BTreeMap<String, String>) -> String {
    if map.is_empty() {
        return String::new();
    }
    let mut out = String::from("{");
    for (i, (k, v)) in map.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&serde_json::to_string(k).expect("string"));
        out.push(':');
        out.push_str(&serde_json::to_string(v).expect("string"));
    }
    out.push('}');
    out
}

/// The reference's structured metadata for one probe, DERIVED from its raw
/// response: exactly one stream with exactly one entry echoing the probe's
/// line, timestamped in wall-clock nanoseconds, carrying the ingest transport
/// labels and the engine's execution-stats block, filtered per `elision`.
fn reference_structured_metadata(sp: &SourceProbe, ap: &ArtifactProbe, elision: Elision) -> String {
    let id = sp.id;
    assert_eq!(
        sp.readable,
        Readable::Yes,
        "{id}: the reference cannot serve this row — it has no reference answer"
    );
    assert_eq!(ap.read_status, 200, "{id}: read status");
    let r = &ap.response;
    assert_eq!(r["status"], "success", "{id}: response status");
    assert_eq!(r["data"]["resultType"], "streams", "{id}: result type");
    assert_eq!(
        r["data"]["encodingFlags"][0], "categorize-labels",
        "{id}: the response must be the categorized encoding, or structured \
         metadata would be merged into the stream labels"
    );
    let result = r["data"]["result"]
        .as_array()
        .unwrap_or_else(|| panic!("{id}: no result array"));
    assert_eq!(result.len(), 1, "{id}: expected exactly one stream");
    let stream = result[0]["stream"]
        .as_object()
        .unwrap_or_else(|| panic!("{id}: no stream labels"));
    // Binds the response to the push that produced it: the container
    // discovers `service_name` from `app`, so in a real capture the two agree
    // and both are THIS probe's nonce.
    assert_eq!(
        stream["app"].as_str(),
        Some(ap.app.as_str()),
        "{id}: the response must come from the probe's own stream"
    );
    assert_eq!(
        stream["service_name"], stream["app"],
        "{id}: service discovery"
    );
    let values = result[0]["values"]
        .as_array()
        .unwrap_or_else(|| panic!("{id}: no values"));
    assert_eq!(values.len(), 1, "{id}: expected exactly one entry");
    assert_eq!(
        values[0][1].as_str(),
        Some(PROBE_LINE),
        "{id}: the response must echo the probe's line"
    );
    // Local sanity, not provenance: a wall-clock NANOSECOND timestamp. The
    // range (2020..2100) rejects seconds, millis, micros and zero without
    // flaking on a re-capture.
    let ts = values[0][0]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("{id}: entry timestamp is not a decimal string"));
    assert!(
        (1_577_836_800_000_000_000..4_102_444_800_000_000_000).contains(&ts),
        "{id}: entry timestamp {ts} is not a plausible wall-clock nanosecond value"
    );
    // Local sanity: the engine's execution-stats block, present in every real
    // query_range response. Structural presence only — the values vary per
    // capture — except the structured-metadata byte counter, which must be
    // positive or the entry carried none and this probe measured nothing.
    let stats = &r["data"]["stats"];
    for path in [
        &["summary", "execTime"][..],
        &["summary", "queueTime"],
        &["summary", "totalBytesProcessed"],
        &["querier", "store", "chunkRefsFetchTime"],
    ] {
        let mut v = stats;
        for seg in path {
            v = &v[*seg];
        }
        assert!(
            v.is_number(),
            "{id}: stats.{} is missing — not a plausible engine response",
            path.join(".")
        );
    }
    assert!(
        stats["summary"]["totalStructuredMetadataBytesProcessed"]
            .as_u64()
            .is_some_and(|n| n > 0),
        "{id}: the entry carried no structured metadata at all"
    );

    let sm = values[0][2]["structuredMetadata"]
        .as_object()
        .unwrap_or_else(|| panic!("{id}: no structuredMetadata in the categorized entry"));
    for elided in ELIDED_SM_NAMES {
        assert!(
            sm.contains_key(*elided),
            "{id}: captured entry lost the ingest-time metadata {elided:?}"
        );
    }
    let derived: BTreeMap<String, String> = sm
        .iter()
        .filter(|(k, _)| elision == Elision::None || !ELIDED_SM_NAMES.contains(&k.as_str()))
        .map(|(k, v)| {
            (
                k.clone(),
                v.as_str()
                    .unwrap_or_else(|| panic!("{id}: non-string metadata value"))
                    .to_string(),
            )
        })
        .collect();
    canonical_json(&derived)
}

/// The reference's read FAILURE for a row it accepted but cannot serve.
fn reference_read_failure(sp: &SourceProbe, ap: &ArtifactProbe) -> (u16, String) {
    assert_eq!(
        sp.readable,
        Readable::ReferenceReadFails,
        "{}: this row is readable — it has an answer, not a failure",
        sp.id
    );
    let body = ap
        .response
        .as_str()
        .unwrap_or_else(|| panic!("{}: expected a plain-text error body", sp.id))
        .trim()
        .to_string();
    (ap.read_status, body)
}

// ---------------------------------------------------------------------
// Our side: the real receiver, over the real JSON push body.
// ---------------------------------------------------------------------

/// The structured-metadata JSON object of a push body, assembled as TEXT so a
/// repeated name survives — a `serde_json::Map` would collapse it, and the
/// repetition is half of what is under test.
fn sm_object(sm: &[(&str, &str)]) -> String {
    let inner: Vec<String> = sm
        .iter()
        .map(|(k, v)| {
            format!(
                "{}:{}",
                serde_json::to_string(k).expect("string"),
                serde_json::to_string(v).expect("string")
            )
        })
        .collect();
    format!("{{{}}}", inner.join(","))
}

fn push_body(app: &str, sp: &SourceProbe, ts_ns: u128) -> String {
    format!(
        r#"{{"streams":[{{"stream":{{"app":{}}},"values":[[{},{},{}]]}}]}}"#,
        serde_json::to_string(app).expect("string"),
        serde_json::to_string(&ts_ns.to_string()).expect("string"),
        serde_json::to_string(PROBE_LINE).expect("string"),
        sm_object(sp.sm),
    )
}

/// What PulsusDB stores for a probe, through the real receiver over the real
/// wire body — not a direct call to the resolution function.
fn stored(sp: &SourceProbe) -> String {
    let body = push_body("probe", sp, 1_700_000_000_000_000_000u128);
    // Issue #483: level discovery ON, which is the product default and the
    // configuration `ci/logql/config.yaml` leaves the reference container in
    // — so the derived expectation and our stored string are compared on the
    // same terms, `detected_level` included.
    let out = parse_loki_json(body.as_bytes(), 0, LevelDiscovery::On)
        .unwrap_or_else(|e| panic!("{}: PulsusDB rejected the probe body: {e}", sp.id));
    assert_eq!(out.rows.len(), 1, "{}: expected one row", sp.id);
    out.rows[0].structured_metadata.clone()
}

// ---------------------------------------------------------------------
// Hermetic.
// ---------------------------------------------------------------------

/// The artifact's probe set is EXACTLY the source table — id, pushed pairs,
/// line and query — so the capture cannot silently cover a different
/// experiment than the one described here.
#[test]
fn artifact_probe_set_is_exactly_the_source_probe_set() {
    let art = load_artifact();
    assert_eq!(art.image, ARTIFACT_IMAGE);
    assert_eq!(art.config, "ci/logql/config.yaml");
    assert_eq!(art.push_endpoint, "/loki/api/v1/push");
    assert_eq!(art.query_endpoint, "/loki/api/v1/query_range");
    assert_eq!(
        art.read_headers,
        vec!["X-Loki-Response-Encoding-Flags: categorize-labels".to_string()]
    );
    assert_eq!(
        art.buildinfo["version"].as_str(),
        Some(ARTIFACT_VERSION),
        "the recorded buildinfo must be the pinned reference's"
    );
    assert_eq!(
        art.buildinfo["revision"].as_str(),
        Some(ARTIFACT_REVISION),
        "the recorded buildinfo must be the pinned reference's"
    );
    let sources = source_probes();
    assert_eq!(art.probes.len(), sources.len(), "probe count");
    for (ap, sp) in art.probes.iter().zip(&sources) {
        assert_eq!(ap.id, sp.id, "probe order");
        assert_eq!(ap.line, PROBE_LINE, "{}: line", sp.id);
        let pushed: Vec<(String, String)> = sp
            .sm
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(ap.sm, pushed, "{}: pushed structured metadata", sp.id);
        assert_eq!(
            ap.query,
            format!(r#"{{app="{}"}}"#, ap.app),
            "{}: the query sent must be the probe's own",
            sp.id
        );
        assert_eq!(ap.push_status, 204, "{}: the push must be accepted", sp.id);
    }
}

/// No probe name can canonicalize onto a name the extractor elides, so the
/// elision cannot hide a probe's own answer.
#[test]
fn no_probe_name_collides_with_an_elided_ingest_label() {
    for sp in source_probes() {
        for (name, _) in sp.sm {
            let canonical = pulsus_model::canonicalize_label_key(name);
            assert!(
                !ELIDED_SM_NAMES.contains(&canonical.as_str()),
                "{}: probe name {name:?} canonicalizes onto an elided name",
                sp.id
            );
        }
    }
}

/// **The deliverable.** Every readable row's stored string equals the
/// reference's answer, derived from the committed capture.
#[test]
fn the_stored_string_reproduces_the_reference_capture() {
    let art = load_artifact();
    let by_id: BTreeMap<&str, &ArtifactProbe> =
        art.probes.iter().map(|p| (p.id.as_str(), p)).collect();
    for sp in source_probes() {
        if sp.readable != Readable::Yes {
            continue;
        }
        let expected = reference_structured_metadata(&sp, by_id[sp.id], Elision::None);
        assert_eq!(stored(&sp), expected, "{}: from {:?}", sp.id, sp.sm);
    }
}

/// Residual B: the one row the reference accepts (204) and cannot serve — its
/// `Labels()` emits two `a_b` entries, one of them still carrying the invalid
/// rune, and its own read path fails. PulsusDB accepts the same push and
/// serves it; the value it serves is OUR choice, asserted here as such and
/// recorded in docs/benchmarks/logs-differential-ledger.md.
#[test]
fn the_row_the_reference_cannot_serve_is_stored_by_us_as_the_last_pair() {
    let art = load_artifact();
    let by_id: BTreeMap<&str, &ArtifactProbe> =
        art.probes.iter().map(|p| (p.id.as_str(), p)).collect();
    let sources = source_probes();
    let sp = sources
        .iter()
        .find(|p| p.readable == Readable::ReferenceReadFails)
        .expect("f04 is the unreadable row");
    let (status, body) = reference_read_failure(sp, by_id[sp.id]);
    assert_eq!(status, 500, "{}: the reference's read status", sp.id);
    assert!(
        body.contains("invalid UTF-8 rune"),
        "{}: unexpected reference failure {body:?}",
        sp.id
    );
    // Our own side, asserted on our own side: the duplicate collapse keeps
    // the last pair, which is the one the builder did not rewrite. The
    // `detected_level` pair beside it is the ingest-time level for the probe
    // line `line` (issue #483) and is not part of this row's residual.
    assert_eq!(
        stored(sp),
        format!(r#"{{"a_b":"p{R}","detected_level":"unknown"}}"#)
    );
}

/// Non-vacuity, and honest about it: the table must contain rows the pre-fix
/// resolution got WRONG, or it would pass against `b872855` and prove
/// nothing. Both counts are pinned, so a row silently changing side fails.
///
/// The five rows that AGREE — `c04`, `c05`, `c07`, `c11`, `c18` — pin the
/// rule without discriminating this fix: the frozen greatest-original-key
/// rule and the builder happen to elect the same pair there.
#[test]
fn the_table_discriminates_the_pre_fix_resolution() {
    let art = load_artifact();
    let by_id: BTreeMap<&str, &ArtifactProbe> =
        art.probes.iter().map(|p| (p.id.as_str(), p)).collect();
    let mut differ: Vec<&str> = Vec::new();
    let mut agree: Vec<&str> = Vec::new();
    for sp in source_probes() {
        if sp.readable != Readable::Yes {
            continue;
        }
        // Issue #483 keeps this ONE comparison on the collision-relevant
        // subset. `sp.was` is the pre-fix stored string, captured before
        // ingest-time level detection existed; comparing it against a
        // full expectation that now carries `detected_level` would make
        // every row differ and the count vacuous.
        let expected = reference_structured_metadata(&sp, by_id[sp.id], Elision::IngestAdded);
        if sp.was == expected {
            agree.push(sp.id);
        } else {
            differ.push(sp.id);
        }
    }
    assert_eq!(
        agree,
        vec!["c04", "c05", "c07", "c11", "c18"],
        "the non-discriminating rows have moved"
    );
    assert_eq!(differ.len(), 12, "differing rows: {differ:?}");
    // And the fix is what closes them: every differing row's pre-fix answer
    // is NOT what we store now.
    for sp in source_probes() {
        if sp.readable == Readable::Yes && differ.contains(&sp.id) {
            assert_ne!(stored(&sp), sp.was, "{}: still the pre-fix answer", sp.id);
        }
    }
}

// ---------------------------------------------------------------------
// Live: the only writer of the artifact, and the drift check.
// ---------------------------------------------------------------------

fn curl(args: &[&str]) -> String {
    let out = Command::new("curl")
        .args(["-s", "--max-time", "20"])
        .args(args)
        .output()
        .expect("curl must be on PATH");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn now_unix() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("clock")
}

/// Pushes one probe under a nonce'd `app` stream and reads it back, polling
/// until the entry is visible or the read fails outright.
fn capture_probe(base_url: &str, nonce: u64, sp: &SourceProbe) -> ArtifactProbe {
    let app = format!("smc{nonce}-{}", sp.id);
    let ts_ns = now_unix().as_nanos();
    let body = push_body(&app, sp, ts_ns);
    let push_status = curl(&[
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        "-H",
        "Content-Type: application/json",
        "-X",
        "POST",
        "--data-binary",
        &body,
        &format!("{base_url}/loki/api/v1/push"),
    ])
    .trim()
    .parse::<u16>()
    .unwrap_or_else(|e| panic!("{}: unparseable push status: {e}", sp.id));
    assert_eq!(push_status, 204, "{}: push rejected", sp.id);

    let query = format!(r#"{{app="{app}"}}"#, app = app);
    let start = (ts_ns / 1_000_000_000).saturating_sub(3600).to_string();
    let end = (ts_ns / 1_000_000_000 + 3600).to_string();
    let mut read_status = 0u16;
    let mut response = Value::Null;
    for _ in 0..40 {
        let raw = curl(&[
            "-w",
            "\n%{http_code}",
            "-G",
            "-H",
            "X-Loki-Response-Encoding-Flags: categorize-labels",
            "--data-urlencode",
            &format!("query={query}"),
            "--data-urlencode",
            &format!("start={start}"),
            "--data-urlencode",
            &format!("end={end}"),
            "--data-urlencode",
            "limit=10",
            &format!("{base_url}/loki/api/v1/query_range"),
        ]);
        let (body, status) = raw
            .rsplit_once('\n')
            .unwrap_or_else(|| panic!("{}: curl wrote no status line", sp.id));
        read_status = status
            .trim()
            .parse::<u16>()
            .unwrap_or_else(|e| panic!("{}: unparseable read status {status:?}: {e}", sp.id));
        response = serde_json::from_str(body).unwrap_or_else(|_| json!(body));
        let visible = read_status == 200
            && response["data"]["result"]
                .as_array()
                .is_some_and(|r| !r.is_empty());
        if visible || read_status >= 500 {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    ArtifactProbe {
        id: sp.id.to_string(),
        app,
        sm: sp
            .sm
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        line: PROBE_LINE.to_string(),
        query,
        push_status,
        read_status,
        response,
    }
}

/// Drift mode (default): re-captures every probe against the live container
/// and asserts the fresh responses derive the SAME answers as the committed
/// artifact. Regen mode (`PULSUS_REGEN_SM_COLLISION_CAPTURE=1`) rewrites the
/// artifact instead, and refuses any container that does not report the
/// pinned version AND revision.
#[test]
fn the_committed_capture_matches_the_live_reference() {
    let Some(base_url) = pulsus_testkit::live_endpoint("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset; skipping the sm collision capture leg");
        return;
    };
    let buildinfo: Value = serde_json::from_str(&curl(&[&format!(
        "{base_url}/loki/api/v1/status/buildinfo"
    )]))
    .expect("buildinfo must parse — is the reference container up?");

    let nonce = now_unix().as_secs();
    let sources = source_probes();
    let fresh: Vec<ArtifactProbe> = sources
        .iter()
        .map(|sp| capture_probe(&base_url, nonce, sp))
        .collect();

    if std::env::var("PULSUS_REGEN_SM_COLLISION_CAPTURE").as_deref() == Ok("1") {
        assert_eq!(
            buildinfo["version"].as_str(),
            Some(ARTIFACT_VERSION),
            "regeneration requires the pinned reference ({ARTIFACT_IMAGE}); refusing to \
             capture from {buildinfo}"
        );
        assert_eq!(
            buildinfo["revision"].as_str(),
            Some(ARTIFACT_REVISION),
            "regeneration requires the pinned reference revision; refusing to capture from \
             {buildinfo}"
        );
        let artifact = Artifact {
            image: ARTIFACT_IMAGE.to_string(),
            config: "ci/logql/config.yaml".to_string(),
            push_endpoint: "/loki/api/v1/push".to_string(),
            query_endpoint: "/loki/api/v1/query_range".to_string(),
            read_headers: vec!["X-Loki-Response-Encoding-Flags: categorize-labels".to_string()],
            buildinfo,
            captured_at_unix: nonce,
            probes: fresh,
        };
        let path = artifact_path();
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        let text = serde_json::to_string_pretty(&artifact).expect("serialize") + "\n";
        std::fs::write(&path, text).unwrap_or_else(|e| panic!("write {path:?}: {e}"));
        eprintln!("regenerated {path:?} from {base_url} — review the diff");
        return;
    }

    let committed = load_artifact();
    let by_id: BTreeMap<&str, &ArtifactProbe> = committed
        .probes
        .iter()
        .map(|p| (p.id.as_str(), p))
        .collect();
    for (ap, sp) in fresh.iter().zip(&sources) {
        match sp.readable {
            Readable::Yes => assert_eq!(
                reference_structured_metadata(sp, ap, Elision::None),
                reference_structured_metadata(sp, by_id[sp.id], Elision::None),
                "{}: the live reference answers differently than the committed capture — if \
                 the reference genuinely changed, regenerate with \
                 PULSUS_REGEN_SM_COLLISION_CAPTURE=1 against {ARTIFACT_IMAGE} and review the \
                 diff",
                sp.id
            ),
            Readable::ReferenceReadFails => assert_eq!(
                reference_read_failure(sp, ap),
                reference_read_failure(sp, by_id[sp.id]),
                "{}: the live reference's read failure differs from the committed capture",
                sp.id
            ),
        }
    }
    eprintln!(
        "sm collision capture drift: {} probes re-captured from {base_url} (reference {}), all \
         agree with the committed artifact",
        sources.len(),
        buildinfo["version"]
    );
}
