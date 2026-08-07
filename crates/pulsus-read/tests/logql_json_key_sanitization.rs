//! Issue #287 — `| json`'s flattened label NAMES are the reference's,
//! byte for byte, held by a committed raw capture.
//!
//! **Where the expectations come from (fix rounds 3–5).** Every
//! assertion in this file about what a probe YIELDS — a label or field
//! name or value we expect to observe — is DERIVED, at test time, from
//! `tests/fixtures/json_key_sanitization/capture.json` — the raw
//! `/loki/api/v1/query_range` response bodies of the pinned reference
//! container (`grafana/loki:3.7.4`, booted on `ci/logql/config.yaml`,
//! each probe pushed through `/loki/api/v1/push` under a per-run
//! nonce'd `app` stream label and read back with its pipeline). The
//! artifact also records the container's `/loki/api/v1/status/buildinfo`
//! answer. Refreshing it CANNOT be done by hand: the only writer is the
//! regeneration mode of [`the_committed_capture_matches_the_live_reference`],
//! which requires a live container reporting exactly the pinned version:
//!
//! ```text
//! podman run -d --name pulsus-json-cap -p 13102:3100 \
//!     -v $PWD/ci/logql/config.yaml:/etc/loki/local-config.yaml:ro \
//!     grafana/loki:3.7.4 -config.file=/etc/loki/local-config.yaml
//! PULSUSDB_LOGQL_DIFF_URL=http://localhost:13102 \
//!     PULSUS_REGEN_JSON_KEY_CAPTURE=1 \
//!     cargo test -p pulsus-read --test logql_json_key_sanitization -- --nocapture
//! ```
//!
//! then review the diff. The same test WITHOUT the regen variable is the
//! drift leg: it re-pushes every probe to the live container and asserts
//! the fresh responses derive the same labels as the committed artifact
//! (CI runs it against the digest-pinned differential oracle, so the
//! artifact is re-validated against a real reference on every push —
//! that leg now runs the SAME v3.7.4 image this artifact was captured
//! from, so the status oracle and this value capture no longer speak for
//! different versions).
//! [`artifact_probe_set_is_exactly_the_source_probe_set`] pins the
//! artifact's probes (id, pipeline, stream, structured metadata, line,
//! query shape) to the tables below. PROVENANCE — that the committed
//! responses really are container output — is attested by the drift
//! leg: CI re-captures every probe from the digest-pinned oracle on
//! every run and asserts the fresh responses derive the same labels as
//! the committed artifact, a live attestation no hermetic assertion
//! can substitute for. The extractor's own checks — the probe's line
//! echoed back, the container's ingest-time transport labels, a
//! plausible wall-clock NANOSECOND entry timestamp, the engine's
//! execution-stats block (`summary`, cache and ingester timings) —
//! are cheap local sanity that catches an obviously synthetic file;
//! they establish schema plausibility, not provenance.
//!
//! The literals that REMAIN in this file fall into FIVE classes plus
//! message prose — an inventory of every string literal outside
//! comments, classified with nothing left over (fix round 6: the list
//! is machine-derived, not curated):
//!
//! 1. probe INPUTS — ids, lines, stream labels, structured metadata,
//!    pipeline and query shapes, the capture nonce format;
//! 2. the container's response-schema names and constants —
//!    `status`/`success`, `resultType`/`streams`, the
//!    result/stream/values/stats envelope and stats field paths,
//!    buildinfo `version`, and the transport-label names in
//!    [`ELIDED_INGEST_LABELS`] — asserted about CAPTURES, elided from
//!    every expectation, never asserted as our output;
//! 3. pins and plumbing — the committed artifact's path, the
//!    image/version pin, the config and endpoint paths, env-var names
//!    and curl/HTTP mechanics: these say where the artifact lives and
//!    what may write it, are cross-checked against the artifact's own
//!    recorded fields, and name no label or field we expect to emit;
//! 4. the reference's `_extracted` rename suffix, a rule constant
//!    appearing only in relations BETWEEN two derived values;
//! 5. the `was` column with its `same` sentinel — pre-fix behaviour
//!    records, asserted only as DIFFERING from derived expectations,
//!    never as expected output;
//!
//! plus assertion/panic message text and join separators, which name
//! nothing.
//!
//! **The rules the capture agrees with** — the source they mirror is
//! `pkg/logql/log/parser.go`'s `buildSanitizedPrefixFromBuffer` over
//! `pkg/logql/log/util.go:42`'s `appendSanitized`:
//!
//! - each path part is whitespace-trimmed (`bytes.TrimSpace`);
//! - a part that trims EMPTY contributes nothing at all — no
//!   characters and no `_` separator;
//! - the `_` separator appears only when something has already been
//!   emitted;
//! - a leading ASCII digit gains a `_` only when nothing has been
//!   emitted yet, so it fires on the first surviving part and never
//!   deeper;
//! - every rune outside `[A-Za-z0-9_]` becomes exactly ONE `_`,
//!   whatever its UTF-8 width;
//! - a top-level key that sanitizes to nothing DROPS the field, rather
//!   than emitting a label with an empty name;
//! - a leaf whose built key collides with an existing label is renamed
//!   with `_extracted`, and the suffix lands depth-awarely: on the
//!   SANITIZED key for a top-level field (`parser.go:152-153`), on the
//!   RAW final path part — rebuilt through the same trim + rune map —
//!   for a nested one (`parser.go:183-187`).
//!
//! Before this issue PulsusDB joined RAW keys and sanitized the result
//! afterwards, which agrees for keys needing no trimming and diverges
//! otherwise — the `was` column records what each row used to emit.
//!
//! **The collision matrix's coverage, stated as a product.** The
//! collision axes are depth of the colliding leaf {0, nested} × the
//! colliding label's category {base/stream, structured metadata} × its
//! state when `| json` runs {live, dropped by an earlier stage}, crossed
//! against the construction rules above. The eight core cells and where
//! each lives:
//!
//! | depth  | category | state   | probe        | outcome            |
//! |--------|----------|---------|--------------|--------------------|
//! | 0      | base     | live    | `c16` (+more)| parity             |
//! | 0      | base     | dropped | `d00`        | parity (since #334)|
//! | 0      | sm       | live    | `c13`        | parity             |
//! | 0      | sm       | dropped | `c15`        | parity             |
//! | nested | base     | live    | `c10` (+more)| parity             |
//! | nested | base     | dropped | `d01`        | parity (since #334)|
//! | nested | sm       | live    | `c14`        | parity             |
//! | nested | sm       | dropped | `c18`        | parity             |
//!
//! The full four-axis product is SAMPLED, by this rule: the construction
//! rules are crossed with a live collision at each depth where the rule
//! is expressible (the rest of the `c` table); the drop axis crosses
//! only a plain untrimmed-key representative of each depth × category
//! cell, because `drop` edits label-set MEMBERSHIP before `| json` runs
//! while the construction rules edit the key TEXT — the collision check
//! is a single membership test of (built key, label state), so the two
//! axes couple only through name equality, which each representative
//! already pins. Genuinely unreachable combinations, named rather than
//! omitted: a collision with an EMPTY-NAMED label cannot exist at any
//! depth (the push API rejects empty label names, and a top-level key
//! that sanitizes to nothing is dropped before any collision check
//! runs), and the structured-metadata category has no depth of its own —
//! depth is a property of the JSON leaf, so sm × {0, nested} is fully
//! covered by `c13`–`c15`/`c18`.
//!
//! **`d00`–`d03` were the four DIVERGENT cells** this file recorded for
//! issue #334, all of them the same missing mechanism — the reference's
//! category-aware builder state. #334 built it, so all four are now
//! ordinary collision rows, asserted against the SAME committed capture
//! by `collision_renames_reproduce_the_reference_capture` (the artifact
//! never changed: it already held the reference's answers, and it was
//! our side that moved). What each cell now agrees on:
//!
//! - `d00`/`d01` — `drop <base-label>` then a recolliding key: the
//!   reference renames at both depths because its `BaseHas` reads the
//!   ORIGINAL stream labels, which `Del` never edits. The
//!   `drop <sm-label>` mirrors (`c15`/`c18`) do NOT rename, and by the
//!   same token: `HasInCategory(…, StructuredMetadataLabel)` reads the
//!   LIVE category, which the drop emptied.
//! - `d02`/`d03` — parsed-versus-parsed collisions: the FIRST extraction
//!   of a name wins and the later one is dropped entirely
//!   (`ParserHint.Extracted`), at depth 0 and across the nested/flat
//!   boundary.

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pulsus_read::logql::StructuredMetadataCtx;
use pulsus_read::logql::pipeline::CompiledPipeline;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// ---------------------------------------------------------------------
// Probe tables — the SOURCE of what is probed, never of what to expect.
// ---------------------------------------------------------------------

type Labels = &'static [(&'static str, &'static str)];

/// One probe: everything needed to BUILD its capture. The expected
/// output deliberately does not appear here — it is derived from the
/// committed artifact (see the module docs).
struct SourceProbe {
    id: &'static str,
    /// The pipeline after the selector, shared verbatim by both sides.
    pipeline: &'static str,
    /// Stream labels beside the per-run `app` routing label.
    base: Labels,
    /// Per-entry structured metadata. Reaches OUR pipeline merged into
    /// the base slice — the same place the reference's
    /// `BaseHas || HasInCategory(StructuredMetadataLabel)` check reads
    /// it for ordinary (non-reserved) entries.
    sm: Labels,
    line: &'static str,
    /// What PulsusDB emitted BEFORE the fix (label names joined by
    /// `,`), `same` when it already agreed — the non-vacuity tests'
    /// discriminator against the pre-fix join.
    was: &'static str,
}

const fn probe(
    id: &'static str,
    pipeline: &'static str,
    base: Labels,
    sm: Labels,
    line: &'static str,
    was: &'static str,
) -> SourceProbe {
    SourceProbe {
        id,
        pipeline,
        base,
        sm,
        line,
        was,
    }
}

/// Key construction (no collision): the trim/transparent-part/rune-map/
/// digit-guard rules, one group per rule.
#[rustfmt::skip]
const CONSTRUCTION_PROBES: &[SourceProbe] = &[
    // -- whitespace-edged parts: trimmed by the reference ---------------
    probe("k00", "| json", &[], &[], r#"{" a ": 1}"#,                     "_a_"),
    probe("k01", "| json", &[], &[], r#"{"x": {" b ": 1}}"#,              "x__b_"),
    probe("k02", "| json", &[], &[], "{\"\\ta\\n\": {\"b\": 1}}",         "_a__b"),
    probe("k03", "| json", &[], &[], r#"{" 1a ": 1}"#,                    "__1a_"),
    probe("k04", "| json", &[], &[], r#"{"x": {" 1a ": 1}}"#,             "x__1a_"),
    probe("k05", "| json", &[], &[], r#"{"a  b": {"  c  d  ": 1}}"#,      "a__b___c__d__"),
    // -- parts that trim EMPTY: transparent, separator included ---------
    probe("k06", "| json", &[], &[], r#"{"  ": {"b": 1}}"#,               "___b"),
    probe("k07", "| json", &[], &[], r#"{"  ": {"2b": 1}}"#,              "___2b"),
    probe("k08", "| json", &[], &[], r#"{"x": {"  ": 1}}"#,               "x___"),
    probe("k09", "| json", &[], &[], r#"{"x": {"": 1}}"#,                 "x_"),
    probe("k10", "| json", &[], &[], r#"{"": {"b": 1}}"#,                 "b"),
    probe("k11", "| json", &[], &[], r#"{"x": {" ": {"y": 1}}}"#,         "x___y"),
    probe("k12", "| json", &[], &[], r#"{"a": {"b": {" ": {"c": 1}}}}"#,  "a_b___c"),
    probe("k13", "| json", &[], &[], "{\"x\": {\"\\t\": {\"\\t\": {\"y\": 1}}}}", "x_____y"),
    probe("k14", "| json", &[], &[], r#"{"a": {"": {"": 1}}}"#,           "a__"),
    probe("k15", "| json", &[], &[], r#"{"": {"": {"z": 1}}}"#,           "z"),
    // -- a top-level key that sanitizes to nothing is DROPPED -----------
    probe("k16", "| json", &[], &[], r#"{"": 1}"#,                        "<empty label name>"),
    probe("k17", "| json", &[], &[], "{\"\\t\": 1}",                      "_"),
    // -- rows that already agreed: the digit guard, the rune map --------
    probe("k18", "| json", &[], &[], r#"{"2a": {"2b": 1}}"#,              "_2a_2b"),
    probe("k19", "| json", &[], &[], r#"{"x": {"2b": 1}}"#,               "x_2b"),
    probe("k20", "| json", &[], &[], r#"{"9": {"9": 1}}"#,                "_9_9"),
    probe("k21", "| json", &[], &[], r#"{"a b": {"c d": 1}}"#,            "a_b_c_d"),
    probe("k22", "| json", &[], &[], r#"{"a.b": {"c-d": 1}}"#,            "a_b_c_d"),
    probe("k23", "| json", &[], &[], r#"{"x": {"a.b": {"c": 1}}}"#,       "x_a_b_c"),
    probe("k24", "| json", &[], &[], r#"{"__": {"__": 1}}"#,              "_____"),
    probe("k25", "| json", &[], &[], r#"{"é": {"ü": 1}}"#,                "___"),
    probe("k26", "| json", &[], &[], r#"{"日本": {"語": 1}}"#,             "____"),
    probe("k27", "| json", &[], &[], r#"{"level":"info","http":{"status":200}}"#, "same"),
];

/// The collision matrix (fix rounds 2–3): the rename rule crossed with
/// each construction rule and with the depth × category × state product
/// enumerated in the module docs.
#[rustfmt::skip]
const COLLISION_PROBES: &[SourceProbe] = &[
    // -- colliding part that trims EMPTY --------------------------------
    probe("c00", "| json", &[("x", "s")], &[], r#"{"x":{"":1}}"#,          "x_extracted"),
    probe("c01", "| json", &[("x", "s")], &[], r#"{"x":{"  ":1}}"#,        "x_extracted"),
    // -- depth 0 trims BEFORE suffixing (sanitize-then-suffix) ----------
    probe("c02", "| json", &[("x", "s")], &[], r#"{" x ":1}"#,             "same"),
    // -- nested suffixes the RAW part (suffix-then-sanitize) ------------
    probe("c03", "| json", &[("x_y", "s")], &[], r#"{"x":{" y ":1}}"#,     "x_y_extracted"),
    // -- all-rejected-runes parts: the two orders agree -----------------
    probe("c04", "| json", &[("__", "s")], &[], r#"{"@#":1}"#,             "same"),
    probe("c05", "| json", &[("x___", "s")], &[], r#"{"x":{"@#":1}}"#,     "same"),
    // -- digit-initial parts: the guard survives the rebuild ------------
    probe("c06", "| json", &[("_9x", "s")], &[], r#"{"9x":1}"#,            "same"),
    probe("c07", "| json", &[("a_9x", "s")], &[], r#"{"a":{"9x":1}}"#,     "same"),
    probe("c08", "| json", &[("_9b", "s")], &[], r#"{"  ":{"9b":1}}"#,     "same"),
    // -- transparent ancestor: nested rule even with an empty prefix ----
    probe("c09", "| json", &[("b", "s")], &[], r#"{"  ":{" b ":1}}"#,      "b_extracted"),
    // -- nested collisions: plain, and through a transparent middle -----
    probe("c10", "| json", &[("a_b_c", "s")], &[], r#"{"a":{"b":{"c":1}}}"#, "same"),
    probe("c11", "| json", &[("x_a", "s")], &[], r#"{"x":{"":{"a":1}}}"#,  "same"),
    // -- a collision at a PREFIX only never renames ---------------------
    probe("c12", "| json", &[("x", "s")], &[], r#"{"x":{"y":1}}"#,         "same"),
    // -- structured-metadata labels collide exactly like base labels ----
    probe("c13", "| json", &[], &[("m", "sv")], r#"{"m":1}"#,              "same"),
    probe("c14", "| json", &[], &[("x_a", "sv")], r#"{"x":{" a ":1}}"#,    "x_a_extracted"),
    // -- an sm label DROPPED before `| json` no longer collides ---------
    probe("c15", "| drop m | json", &[], &[("m", "sv")], r#"{"m":1}"#,     "same"),
    // -- depth-0 plain collision (regression) ---------------------------
    probe("c16", "| json", &[("x", "s")], &[], r#"{"x":1}"#,               "same"),
    // -- the renamed key lands in an already-taken slot: overwritten ----
    probe("c17", "| json", &[("x", "s"), ("x__extracted", "t")], &[], r#"{"x":{"":1}}"#, "x_extracted"),
    // -- nested × sm × dropped (fix round 3): the nested mirror of c15.
    //    `was` is derived, not measured: no trimming is involved (the
    //    pre-fix join built the same `x_m`) and neither side sees a
    //    collision, so 19d02fb emitted the same labels ------------------
    probe("c18", "| drop x_m | json", &[], &[("x_m", "sv")], r#"{"x":{"m":1}}"#, "same"),
    // -- issue #334's four cells, PARITY since that fix ------------------
    //    They were recorded here as divergences (`d00`-`d03`) with their
    //    reference answers captured like every other row; the fix made
    //    them agree, and the SAME committed capture now asserts it. Ids
    //    and artifact positions are unchanged, so the capture needs no
    //    regeneration. `was` stays `same` — none of the four changed in
    //    #287, which is why they were filed against #334 rather than
    //    fixed there.
    //
    //    depth 0 / nested × base × DROPPED: `BaseHas` reads the ORIGINAL
    //    stream labels, so a dropped stream label still forces the
    //    rename (`labels.go:278-280 @ v3.7.4`).
    probe("d00", "| drop x | json", &[("x", "s")], &[], r#"{"x":1}"#, "same"),
    probe("d01", "| drop x_y | json", &[("x_y", "s")], &[], r#"{"x":{"y":1}}"#, "same"),
    //    parsed-versus-parsed at both depths: the FIRST extraction of a
    //    name wins and the second vanishes
    //    (`ParserHint.Extracted`/`parser.go:156-158`, `:190-194`).
    probe("d02", "| json", &[], &[], r#"{"a-b":1,"a.b":2}"#, "same"),
    probe("d03", "| json", &[], &[], r#"{"x":{"y":1},"x_y":2}"#, "same"),
];

/// Every probe, in artifact order.
fn all_probes() -> Vec<&'static SourceProbe> {
    CONSTRUCTION_PROBES
        .iter()
        .chain(COLLISION_PROBES.iter())
        .collect()
}

// ---------------------------------------------------------------------
// The committed artifact.
// ---------------------------------------------------------------------

/// The image the artifact must have been captured from — enforced at
/// REGENERATION time against the live container's buildinfo, and pinned
/// hermetically against the recorded buildinfo.
const ARTIFACT_IMAGE: &str = "grafana/loki:3.7.4";
const ARTIFACT_VERSION: &str = "3.7.4";

/// Ingest-time transport labels the container adds to every stream —
/// `service_name` (discovered from `app`) and `detected_level`
/// (`"unknown"` when the line carries no level) — plus the per-run
/// `app` routing label. Elided from the DERIVED expectation on both
/// sides; the extractor asserts all three are PRESENT in every captured
/// response (their absence would mean the response is not an ingest
/// capture), and the comparison helper asserts OUR output never emits
/// one of these names, so the elision cannot mask a real difference.
/// No probe key sanitizes to an elided name.
const ELIDED_INGEST_LABELS: &[&str] = &["app", "service_name", "detected_level"];

#[derive(Serialize, Deserialize)]
struct Artifact {
    image: String,
    config: String,
    push_endpoint: String,
    query_endpoint: String,
    /// The container's `/loki/api/v1/status/buildinfo` response.
    buildinfo: Value,
    captured_at_unix: u64,
    probes: Vec<ArtifactProbe>,
}

#[derive(Serialize, Deserialize)]
struct ArtifactProbe {
    id: String,
    pipeline: String,
    /// The stream labels as pushed, `app` (nonce'd) first.
    stream: Vec<(String, String)>,
    sm: Vec<(String, String)>,
    line: String,
    /// The query as sent to the container.
    query: String,
    /// The raw `query_range` response body.
    response: Value,
}

fn artifact_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/json_key_sanitization/capture.json")
}

fn load_artifact() -> Artifact {
    let path = artifact_path();
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

/// The reference's labels for one probe, DERIVED from its raw response:
/// exactly one stream with exactly one entry echoing the probe's line,
/// timestamped in wall-clock nanoseconds, carrying the ingest
/// transport labels and the engine's execution-stats block, minus the
/// elide list, sorted.
fn reference_labels(sp: &SourceProbe, ap: &ArtifactProbe) -> Vec<(String, String)> {
    let id = sp.id;
    let r = &ap.response;
    assert_eq!(r["status"], "success", "{id}: response status");
    assert_eq!(r["data"]["resultType"], "streams", "{id}: result type");
    let result = r["data"]["result"]
        .as_array()
        .unwrap_or_else(|| panic!("{id}: no result array"));
    assert_eq!(result.len(), 1, "{id}: expected exactly one stream");
    let values = result[0]["values"]
        .as_array()
        .unwrap_or_else(|| panic!("{id}: no values"));
    assert_eq!(values.len(), 1, "{id}: expected exactly one entry");
    assert_eq!(
        values[0][1].as_str(),
        Some(sp.line),
        "{id}: the response must echo the probe's line"
    );
    // Local sanity, not provenance (module docs): the entry timestamp
    // must be wall-clock NANOSECONDS. The range (2020..2100) rejects
    // seconds, millis, micros and zero without flaking on a
    // re-capture.
    let ts = values[0][0]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("{id}: entry timestamp is not a decimal string"));
    assert!(
        (1_577_836_800_000_000_000..4_102_444_800_000_000_000).contains(&ts),
        "{id}: entry timestamp {ts} is not a plausible wall-clock nanosecond value"
    );
    // Local sanity, not provenance (module docs): the engine's
    // execution-stats block is present in every real query_range
    // response, so its absence marks an obviously synthetic file.
    // Structural presence only — the values vary per capture.
    let stats = &r["data"]["stats"];
    for path in [
        &["summary", "execTime"][..],
        &["summary", "queueTime"],
        &["summary", "totalBytesProcessed"],
        &["cache", "chunk", "downloadTime"],
        &["ingester", "recvWaitTime"],
        &["ingester", "store", "chunkRefsFetchTime"],
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
    let stream = result[0]["stream"]
        .as_object()
        .unwrap_or_else(|| panic!("{id}: no stream labels"));
    for transport in ELIDED_INGEST_LABELS {
        assert!(
            stream.contains_key(*transport),
            "{id}: captured stream lost the ingest transport label {transport:?}"
        );
    }
    // The container discovers `service_name` from `app`, so in a real
    // capture the two agree — and both must be THIS probe's nonce'd
    // stream, binding the response to the push that produced it.
    assert_eq!(
        stream["service_name"], stream["app"],
        "{id}: service discovery"
    );
    assert_eq!(
        stream["app"].as_str(),
        Some(ap.stream[0].1.as_str()),
        "{id}: the response must come from the probe's own stream"
    );
    let mut labels: Vec<(String, String)> = stream
        .iter()
        .filter(|(k, _)| !ELIDED_INGEST_LABELS.contains(&k.as_str()))
        .map(|(k, v)| {
            (
                k.clone(),
                v.as_str()
                    .unwrap_or_else(|| panic!("{id}: non-string label"))
                    .to_string(),
            )
        })
        .collect();
    labels.sort();
    labels
}

// ---------------------------------------------------------------------
// Our side.
// ---------------------------------------------------------------------

fn compiled(query: &str) -> CompiledPipeline {
    let expr = pulsus_logql::parse(query).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query: {query}");
    };
    CompiledPipeline::compile(&log.pipeline).expect("compile")
}

/// PulsusDB's labels for one probe, sorted. Structured metadata reaches
/// the pipeline merged into the base slice (see [`SourceProbe::sm`]).
fn emitted(p: &SourceProbe) -> Vec<(String, String)> {
    let base: Vec<(String, String)> = p
        .base
        .iter()
        .chain(p.sm.iter())
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    // The merged base is stream labels then ordinary structured metadata,
    // and the pipeline is TOLD where the split is — the reference reads
    // the two categories from different places, so a probe that hides the
    // split cannot reach its LIVE-category rule (issue #334; `c15`/`c18`
    // are exactly that cell). No probe's base and sm collide, so this
    // concatenation is what the real merge would produce.
    let sm_ctx = StructuredMetadataCtx {
        err: String::new(),
        details: String::new(),
        has_ordinary: !p.sm.is_empty(),
        stream_label_count: Some(p.base.len()),
    };
    let query = format!(r#"{{app="a"}} {}"#, p.pipeline);
    let compiled = compiled(&query);
    let mut labels = Vec::new();
    compiled
        .run_into_with_sm(p.line, &base, 0, &sm_ctx, &mut labels)
        .expect("well inside the key budget")
        .expect("no probe pipeline drops its line");
    let mut pairs: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    pairs.sort();
    for (k, _) in &pairs {
        assert!(
            !ELIDED_INGEST_LABELS.contains(&k.as_str()),
            "{}: emitted an elided transport-label name {k:?} — the elision would mask it",
            p.id
        );
    }
    pairs
}

fn owned(labels: Labels) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = labels
        .iter()
        .map(|(k, val)| (k.to_string(), val.to_string()))
        .collect();
    v.sort();
    v
}

fn artifact_by_id(art: &Artifact) -> std::collections::BTreeMap<&str, &ArtifactProbe> {
    art.probes.iter().map(|p| (p.id.as_str(), p)).collect()
}

// ---------------------------------------------------------------------
// Hermetic: the artifact is bound to the tables, and we reproduce it.
// ---------------------------------------------------------------------

/// The artifact's probe set is EXACTLY the source tables — id, pipeline,
/// stream shape, structured metadata, line and query — so the capture
/// cannot silently cover a different experiment than the one described
/// here, and a stale artifact fails loudly instead of sampling.
#[test]
fn artifact_probe_set_is_exactly_the_source_probe_set() {
    let art = load_artifact();
    assert_eq!(art.image, ARTIFACT_IMAGE);
    assert_eq!(art.config, "ci/logql/config.yaml");
    assert_eq!(art.push_endpoint, "/loki/api/v1/push");
    assert_eq!(art.query_endpoint, "/loki/api/v1/query_range");
    assert_eq!(
        art.buildinfo["version"].as_str(),
        Some(ARTIFACT_VERSION),
        "the recorded buildinfo must be the pinned reference's"
    );
    let sources = all_probes();
    assert_eq!(art.probes.len(), sources.len(), "probe count");
    for (ap, sp) in art.probes.iter().zip(&sources) {
        assert_eq!(ap.id, sp.id, "probe order");
        assert_eq!(ap.pipeline, sp.pipeline, "{}: pipeline", sp.id);
        assert_eq!(ap.line, sp.line, "{}: line", sp.id);
        let (app_name, app_value) = &ap.stream[0];
        assert_eq!(app_name, "app", "{}: stream must lead with app", sp.id);
        let rest: Vec<(String, String)> = ap.stream[1..].to_vec();
        assert_eq!(rest, owned(sp.base), "{}: base labels", sp.id);
        assert_eq!(ap.sm, owned(sp.sm), "{}: structured metadata", sp.id);
        assert_eq!(
            ap.query,
            format!(r#"{{app="{app_value}"}} {}"#, sp.pipeline),
            "{}: the query sent must be the probe's own",
            sp.id
        );
    }
}

/// Every construction row reproduces the reference capture exactly.
#[test]
fn flattened_key_names_reproduce_the_reference_capture() {
    let art = load_artifact();
    let by_id = artifact_by_id(&art);
    for sp in CONSTRUCTION_PROBES {
        let ap = by_id[sp.id];
        assert_eq!(
            emitted(sp),
            reference_labels(sp, ap),
            "{}: line {}",
            sp.id,
            sp.line
        );
    }
}

/// Every collision row reproduces the reference capture exactly.
#[test]
fn collision_renames_reproduce_the_reference_capture() {
    let art = load_artifact();
    let by_id = artifact_by_id(&art);
    for sp in COLLISION_PROBES {
        let ap = by_id[sp.id];
        assert_eq!(
            emitted(sp),
            reference_labels(sp, ap),
            "{}: line {} over base {:?} sm {:?}",
            sp.id,
            sp.line,
            sp.base,
            sp.sm
        );
    }
}

/// Non-vacuity: the construction table must actually contain rows the
/// pre-fix join got WRONG, or it would pass against the old code and
/// prove nothing. Also pins the specific defect class — an emitted
/// label whose name is the empty string, which no Prometheus-compatible
/// consumer accepts.
#[test]
fn the_table_covers_rows_the_pre_fix_join_got_wrong() {
    let art = load_artifact();
    let by_id = artifact_by_id(&art);
    let changed = CONSTRUCTION_PROBES
        .iter()
        .filter(|sp| {
            let joined = reference_labels(sp, by_id[sp.id])
                .iter()
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>()
                .join(",");
            joined != sp.was
        })
        .count();
    assert!(
        changed >= 16,
        "only {changed} rows discriminate the rewrite — the table has gone vacuous"
    );
    assert!(
        emitted(&CONSTRUCTION_PROBES[16]).is_empty(),
        "a top-level key that sanitizes to nothing must be DROPPED, never emitted \
         under an empty label name"
    );
}

/// Non-vacuity, same shape: the collision matrix must contain rows
/// where the two suffix orders DISAGREE, or it would pass against
/// 19d02fb's sanitize-then-suffix rename and prove nothing about the
/// ordering.
#[test]
fn the_matrix_discriminates_the_suffix_order() {
    let art = load_artifact();
    let by_id = artifact_by_id(&art);
    let changed = COLLISION_PROBES
        .iter()
        .filter(|sp| {
            sp.was != "same"
                && reference_labels(sp, by_id[sp.id])
                    .iter()
                    .all(|(k, _)| k != sp.was)
        })
        .count();
    assert!(
        changed >= 6,
        "only {changed} rows discriminate the rename order — the matrix has gone vacuous"
    );
}

/// The names are built by the flatten, so they are what the auto-parse
/// probe behind `/detected_fields` observes too — the surface that runs
/// `| json` on every sampled line whether the query asks for it or not.
/// The probe is fed k01's line, so the expected field set is k01's,
/// derived from the capture like every other expectation (fix round 5:
/// this test used to hand-type `x_b`).
#[test]
fn the_detected_fields_probe_sees_the_same_names() {
    use pulsus_read::logql::DetectedFieldsProbe;
    let art = load_artifact();
    let by_id = artifact_by_id(&art);
    let sp = CONSTRUCTION_PROBES
        .iter()
        .find(|sp| sp.id == "k01")
        .expect("k01 is a construction probe");
    let expected: Vec<String> = reference_labels(sp, by_id[sp.id])
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert!(
        !expected.is_empty(),
        "k01 must derive at least one field, or this test asserts nothing"
    );
    let mut probe = DetectedFieldsProbe::new(10, 100);
    probe.add_stream(1, &[("app".to_string(), "a".to_string())]);
    probe
        .feed_row(&compiled(r#"{app="a"}"#), 1, 0, sp.line, "")
        .expect("no budget breach");
    let (fields, _capped) = probe.finish();
    let mut fields: Vec<String> = fields
        .into_iter()
        .map(|f| f.label)
        .filter(|l| l != "app")
        .collect();
    fields.sort();
    assert_eq!(fields, expected);
}

// ---------------------------------------------------------------------
// Live: the only writer of the artifact, and the drift check that
// re-validates it against a real container (CI: the differential
// oracle). Gated like `logql_differential` on PULSUSDB_LOGQL_DIFF_URL.
// ---------------------------------------------------------------------

fn curl_body(args: &[&str]) -> String {
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

/// Pushes one probe under a nonce'd `app` stream and polls `query_range`
/// until its single entry is visible, returning the raw response.
fn capture_probe(base_url: &str, nonce: u64, sp: &SourceProbe) -> ArtifactProbe {
    let app = format!("cap{nonce}-{}", sp.id);
    let mut stream = serde_json::Map::new();
    stream.insert("app".into(), json!(app));
    for (k, v) in owned(sp.base) {
        stream.insert(k, json!(v));
    }
    let ts_ns = now_unix().as_nanos();
    let mut entry = vec![json!(ts_ns.to_string()), json!(sp.line)];
    if !sp.sm.is_empty() {
        let sm: serde_json::Map<String, Value> = owned(sp.sm)
            .into_iter()
            .map(|(k, v)| (k, json!(v)))
            .collect();
        entry.push(Value::Object(sm));
    }
    let body = json!({"streams": [{"stream": Value::Object(stream), "values": [entry]}]});
    let status = curl_body(&[
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        "-H",
        "Content-Type: application/json",
        "-X",
        "POST",
        "--data-binary",
        &body.to_string(),
        &format!("{base_url}/loki/api/v1/push"),
    ]);
    assert_eq!(status.trim(), "204", "{}: push rejected", sp.id);

    let query = format!(r#"{{app="{app}"}} {}"#, sp.pipeline);
    let start = (ts_ns / 1_000_000_000).saturating_sub(3600).to_string();
    let end = (ts_ns / 1_000_000_000 + 3600).to_string();
    let mut response = Value::Null;
    for _ in 0..40 {
        let body = curl_body(&[
            "-G",
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
        let parsed: Value = serde_json::from_str(&body)
            .unwrap_or_else(|e| panic!("{}: unparseable query_range body {body:?}: {e}", sp.id));
        if parsed["data"]["result"]
            .as_array()
            .is_some_and(|r| !r.is_empty())
        {
            response = parsed;
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    assert!(
        !response.is_null(),
        "{}: pushed entry never became visible to {query:?}",
        sp.id
    );
    ArtifactProbe {
        id: sp.id.to_string(),
        pipeline: sp.pipeline.to_string(),
        stream: std::iter::once(("app".to_string(), app))
            .chain(owned(sp.base))
            .collect(),
        sm: owned(sp.sm),
        line: sp.line.to_string(),
        query,
        response,
    }
}

/// Drift mode (default): re-captures every probe against the live
/// container and asserts the fresh responses derive the SAME labels as
/// the committed artifact. Regen mode (`PULSUS_REGEN_JSON_KEY_CAPTURE=1`)
/// rewrites the artifact instead — and refuses any container that does
/// not report the pinned version, so the artifact can only ever be
/// produced by the reference it names. Drift deliberately does NOT pin
/// the version: it runs against whatever `PULSUSDB_LOGQL_DIFF_URL`
/// serves, and in CI that is the differential oracle.
///
/// **That used to be a SECOND reference build.** Before the oracle was
/// re-pinned, CI ran drift against a v3.7.3 container while this artifact
/// was captured at v3.7.4, so the check incidentally spanned two
/// versions; both sides are v3.7.4 now and it is same-version. The
/// cross-version property is gone, and it is not being reconstructed: it
/// was incidental rather than designed, this leg's job is drift against
/// the pinned oracle, and it would not have caught anything we have
/// actually hit — issue #350's byte-size spellings are rejected by both
/// versions, so a cross-version check was blind to them too. Standing up
/// a second container to preserve it would be apparatus for a property
/// nobody chose.
#[test]
fn the_committed_capture_matches_the_live_reference() {
    let Ok(base_url) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset; skipping the json key capture leg");
        return;
    };
    let buildinfo: Value = serde_json::from_str(&curl_body(&[&format!(
        "{base_url}/loki/api/v1/status/buildinfo"
    )]))
    .expect("buildinfo must parse — is the reference container up?");

    let nonce = now_unix().as_secs();
    let sources = all_probes();
    let fresh: Vec<ArtifactProbe> = sources
        .iter()
        .map(|sp| capture_probe(&base_url, nonce, sp))
        .collect();

    if std::env::var("PULSUS_REGEN_JSON_KEY_CAPTURE").as_deref() == Ok("1") {
        assert_eq!(
            buildinfo["version"].as_str(),
            Some(ARTIFACT_VERSION),
            "regeneration requires the pinned reference ({ARTIFACT_IMAGE}); refusing to \
             capture from {buildinfo}"
        );
        let artifact = Artifact {
            image: ARTIFACT_IMAGE.to_string(),
            config: "ci/logql/config.yaml".to_string(),
            push_endpoint: "/loki/api/v1/push".to_string(),
            query_endpoint: "/loki/api/v1/query_range".to_string(),
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
    let by_id = artifact_by_id(&committed);
    for (ap, sp) in fresh.iter().zip(&sources) {
        let live = reference_labels(sp, ap);
        let recorded = reference_labels(sp, by_id[sp.id]);
        assert_eq!(
            live, recorded,
            "{}: the live reference derives different labels than the committed capture — \
             if the reference genuinely changed, regenerate with \
             PULSUS_REGEN_JSON_KEY_CAPTURE=1 against {ARTIFACT_IMAGE} and review the diff",
            sp.id
        );
    }
    eprintln!(
        "json key capture drift: {} probes re-captured from {base_url} (reference {}), all \
         agree with the committed artifact",
        sources.len(),
        buildinfo["version"]
    );
}
