//! Issue #463 — the categorised wire shape, held by a committed raw
//! capture of the pinned reference and replayed against PulsusDB.
//!
//! **Where the expectations come from.** No reference VALUE is written
//! in this file. Every expectation is DERIVED, at test time, from
//! `tests/fixtures/categorize_labels/capture.json` — the raw response
//! bodies and tail frames of the pinned reference container, booted on
//! `ci/logql/config.yaml`, with the fixture pushed through
//! `/loki/api/v1/push` under a per-run nonce and read back with the
//! header each probe declares. The artifact also records the
//! container's `/loki/api/v1/status/buildinfo` answer. Refreshing it
//! cannot be done by hand: the only writer is the regeneration mode of
//! [`the_committed_capture_matches_the_live_reference`], which requires
//! a live container reporting exactly the pinned version AND revision.
//!
//! ```text
//! podman run -d --name pulsus-c463 -p 13561:3100 \
//!     -v $PWD/ci/logql/config.yaml:/etc/loki/local-config.yaml:ro \
//!     grafana/loki:3.7.4 -config.file=/etc/loki/local-config.yaml
//! PULSUSDB_LOGQL_DIFF_URL=http://localhost:13561 \
//!     PULSUS_REGEN_CATEGORIZE_CAPTURE=1 \
//!     cargo test -p pulsus-server --test categorize_labels_differential \
//!     -- --nocapture --test-threads=1
//! ```
//!
//! then review the diff. The same test WITHOUT the regen variable is the
//! drift leg, which CI runs against the digest-pinned differential
//! oracle.
//!
//! ## The comparison, stated positively
//!
//! | class | compared | NOT compared |
//! |---|---|---|
//! | `Q` (query, non-empty / empty / header) | the `data` object with `stats` removed, key order preserved | `stats`; the top-level `status`; the top-level `warnings`; the HTTP status code; all response headers |
//! | `F` (failure) | HTTP status code and body text | response headers; there is no `data` |
//! | `T` (tail) | `streams` and `encodingFlags`, key order preserved, after normalisation | `dropped_entries`; `dropped_total` |
//!
//! Each exclusion is where the next defect hides, so each is justified:
//! `stats` is timing data that varies with an ordinary run and that
//! issue #463 changes nothing in; `status` and the HTTP status code are
//! constant across a double run by construction; response headers are
//! asserted by the conformance suite; drop accounting is a function of
//! consumer scheduling. **`warnings` is absent from the probe set by
//! construction** — it is emitted only for a `variants(...)` query that
//! skipped a variant, and no probe here is one, asserted by
//! [`no_probe_is_a_variants_query`]. That exclusion is therefore never
//! exercised, which is recorded rather than presented as coverage.
//!
//! ## Normalisation, and the rule it follows
//!
//! **Normalisation may absorb values carrying no semantic relationship,
//! and must preserve every relationship between values.** Two things are
//! absorbed: the per-run nonce, which no committed artifact can pin and
//! which relates to nothing; and `detected_level`, which
//! `ci/logql/config.yaml` leaves at its shipped default so the container
//! appends it to every entry at ingest while PulsusDB does not. Nothing
//! else is rewritten.
//!
//! The tail additionally rebases timestamps to their offset from the
//! frame minimum, because a live tail's timestamps are wall-clock.
//! Offsets preserve order AND spacing. **And, independently of any
//! comparison, [`tail_timestamps_increase_in_document_order`] asserts
//! that a tail frame's timestamps strictly increase across objects and
//! values.** That second assertion is not redundant: a capture and a
//! replay reordered IDENTICALLY would pass equality alone, and only the
//! standalone assertion reds.
//!
//! ## Sides
//!
//! Two probes are one-sided. `F2-ref` can only be captured from the
//! reference, which rejects an instant log query; `F2-pulsus` can only
//! be replayed against PulsusDB, which serves it. They are bound as a
//! pair by the manifest and by the `categorize-instant-log-query` ledger
//! row, which carries both answers.

#[path = "support/ordered_json.rs"]
mod ordered_json;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ordered_json::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------
// The probe manifest — the SOURCE of what is probed, never of what to
// expect.
// ---------------------------------------------------------------------

/// Which comparison a probe takes, and what its class asserts before it
/// compares anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    /// `data.result` is non-empty, and every fixture timestamp the probe
    /// reads lies inside its own `start`/`end`.
    QNonEmpty,
    /// `data.result == []`, and the selector matches no `app` value the
    /// fixture pushed — so it is empty for its intended reason and not
    /// because its range drifted.
    QEmpty,
    /// A varied `X-Loki-Response-Encoding-Flags` over the base query.
    QHeader,
    /// An exact HTTP status and body text, and no `encodingFlags`.
    Failure,
    /// A tail frame: key order, arity, per-cell `stream` object and third
    /// element, and exactly the pushed timestamps.
    Tail,
}

/// Which store can answer a probe at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Both,
    /// Capturable from the reference only.
    ReferenceOnly,
    /// Replayable against PulsusDB only.
    PulsusOnly,
}

struct Probe {
    id: &'static str,
    class: Class,
    side: Side,
    /// The one-sided partner, or `None` when the probe is two-sided.
    pairs_with: Option<&'static str>,
    /// The LogQL query, with `{N}` standing for the run nonce.
    query: &'static str,
    /// The header value, or `None` for no header at all.
    header: Option<&'static str>,
    /// Extra query parameters, appended verbatim.
    extra: &'static [(&'static str, &'static str)],
    /// `query_range` unless stated.
    route: &'static str,
}

const RANGE: &[(&str, &str)] = &[];

const fn q(id: &'static str, query: &'static str, header: Option<&'static str>) -> Probe {
    Probe {
        id,
        class: Class::QNonEmpty,
        side: Side::Both,
        pairs_with: None,
        query,
        header,
        extra: RANGE,
        route: "query_range",
    }
}

const CAT: Option<&str> = Some("categorize-labels");

#[rustfmt::skip]
fn query_probes() -> Vec<Probe> {
    let mut v = vec![
        q("A",  r#"{app="co{N}"}"#, None),
        q("B",  r#"{app="co{N}"}"#, CAT),
        q("C",  r#"{app="co{N}"} | json"#, CAT),
        q("D",  r#"{app="co{N}"} | label_format app="rewritten""#, CAT),
        q("E",  r#"{app="co{N}"} | drop env"#, CAT),
        q("I",  r#"{app="co{N}"} | label_format app="x", env="y", service_name="z""#, CAT),
        q("K",  r#"{app="co{N}"} | drop trace_id"#, CAT),
        q("L",  r#"{app="co{N}"} | keep app"#, CAT),
        q("O",  r#"{app="co{N}"} | label_format trace_id="forced""#, CAT),
        q("P",  r#"{app="co{N}"} | line_format `L={{.app}}`"#, CAT),
        q("R",  r#"{app="dbl{N}"}"#, CAT),
        q("LF", r#"{app="pzlf{N}"} | logfmt"#, CAT),
        q("RE", r#"{app="pzre{N}"} | regexp `^RE (?P<app>\S+) (?P<trace_id>\S+)$`"#, CAT),
        q("PT", r#"{app="pzpt{N}"} | pattern `PT <app> <trace_id> end`"#, CAT),
        q("UP", r#"{app="pzup{N}"} | unpack"#, CAT),
        q("JX", r#"{app="pzjx{N}"} | json app="lvl", trace_id="tid""#, CAT),
        q("LX", r#"{app="pzlx{N}"} | logfmt app="lvl", trace_id="tid""#, CAT),
    ];
    // M — a bounded backward read, so the limit and the direction are
    // exercised on the categorised shape too.
    v.push(Probe {
        extra: &[("limit", "2"), ("direction", "backward")],
        ..q("M", r#"{app="co{N}"}"#, CAT)
    });
    // H — a METRIC query with the header. The key must be ABSENT: the
    // reference's frontend codec sends every non-streams result through
    // an encoder that takes no flags.
    v.push(Probe {
        extra: &[("step", "1s")],
        ..q("H", r#"sum(count_over_time({app="co{N}"}[1s]))"#, CAT)
    });
    // G — the empty control.
    v.push(Probe {
        class: Class::QEmpty,
        ..q("G", r#"{app="nosuch{N}"}"#, CAT)
    });
    // The header table: the same base query, only the header varied.
    for (id, header) in HEADER_CASES {
        v.push(Probe {
            class: Class::QHeader,
            ..q(id, r#"{app="co{N}"}"#, Some(header))
        });
    }
    // H4 sends the header PRESENT AND EMPTY, which `Some("")` expresses;
    // H5 sends it twice and is handled by the capture driver.
    v.push(Probe {
        class: Class::Failure,
        side: Side::ReferenceOnly,
        pairs_with: Some("F2-pulsus"),
        route: "query",
        ..q("F2-ref", r#"{app="co{N}"}"#, CAT)
    });
    v.push(Probe {
        class: Class::QNonEmpty,
        side: Side::PulsusOnly,
        pairs_with: Some("F2-ref"),
        route: "query",
        ..q("F2-pulsus", r#"{app="co{N}"}"#, CAT)
    });
    v.push(Probe {
        class: Class::Failure,
        ..q("F1", r#"{app=}"#, CAT)
    });
    v
}

/// The fourteen measured header shapes. `H5` is the two-header-lines
/// case and is sent as two lines by the driver.
const HEADER_CASES: &[(&str, &str)] = &[
    ("H1", "foo"),
    ("H2", "categorize-labels,foo"),
    ("H3", "  categorize-labels"),
    ("H4", ""),
    ("H5", "foo\u{1}categorize-labels"),
    ("H6", "CATEGORIZE-LABELS"),
    ("H7", "foo, categorize-labels"),
    ("H8", "categorize-labels, foo"),
    ("H9", "categorize-labels ,foo"),
    ("H10", "foo,\tcategorize-labels"),
    ("H11", "foo,foo"),
    ("H12", "categorize-labels,categorize-labels"),
    ("H13", "foo,categorize-labels,foo"),
    ("H14", "foo,,categorize-labels"),
];

/// The eight cells of the pipeline x delivery-path x header tail grid,
/// plus the ten shaped tail probes.
struct TailProbe {
    id: &'static str,
    /// Appended to `{app="<probe>"}` or `{run="<probe>"}`.
    pipeline: &'static str,
    header: Option<&'static str>,
    /// `true` = push AFTER the socket opens and the drain settles.
    live: bool,
    fixture: TailFixture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TailFixture {
    /// One plain entry, then one carrying metadata that collides with the
    /// stream's `app` label.
    Collide,
    /// One `logfmt` entry carrying both metadata and a parsed result.
    ParserOne,
    /// Two such entries, differing in line, metadata and parsed result.
    ParserTwo,
    /// Two entries with IDENTICAL metadata, line and parsed result.
    Identical,
    /// Two streams differing in one label, entries interleaved in time.
    Interleaved,
}

#[rustfmt::skip]
fn tail_probes() -> Vec<TailProbe> {
    use TailFixture::*;
    let t = |id, pipeline, header, live, fixture| TailProbe { id, pipeline, header, live, fixture };
    vec![
        t("T1", "", None, false, Collide),
        t("T2", "", CAT, false, Collide),
        t("T3", "", None, true, Collide),
        t("T4", "", CAT, true, Collide),
        t("T5", " |= `tail`", None, false, Collide),
        t("T6", " |= `tail`", CAT, false, Collide),
        t("T7", " |= `tail`", None, true, Collide),
        t("T8", " |= `tail`", CAT, true, Collide),
        t("T9", " | logfmt", CAT, false, ParserOne),
        t("T10", " | logfmt", CAT, true, ParserOne),
        t("T11", " | logfmt", CAT, false, ParserTwo),
        t("T12", " | logfmt", CAT, true, ParserTwo),
        t("T13", " | logfmt", CAT, false, Identical),
        t("T14", " | logfmt", CAT, true, Identical),
        t("T15", " | logfmt", CAT, true, Interleaved),
        t("T16", " | logfmt", None, true, Interleaved),
        t("T17", "", None, true, Interleaved),
        t("T18", "", CAT, true, Interleaved),
    ]
}

// ---------------------------------------------------------------------
// The committed artifact.
// ---------------------------------------------------------------------

const ARTIFACT_IMAGE: &str = "grafana/loki:3.7.4";
const ARTIFACT_VERSION: &str = "3.7.4";
const ARTIFACT_REVISION: &str = "b318f282";

/// Names the container ADDS at ingest — `ci/logql/config.yaml` leaves
/// `discover_log_levels` at its shipped default, which appends
/// `detected_level` to every entry. PulsusDB does not, so it is elided
/// from both sides by the same function. No probe name can canonicalize
/// onto it ([`no_probe_name_collides_with_an_elided_ingest_label`]), so
/// the elision cannot mask a real difference.
const ELIDED_NAMES: &[&str] = &["detected_level"];

#[derive(Serialize, Deserialize)]
struct Artifact {
    image: String,
    config: String,
    buildinfo: Value,
    captured_at_unix: u64,
    /// The nonce the capture ran under, already substituted OUT of every
    /// projection below; recorded so a reader can find the original run.
    nonce: String,
    probes: Vec<ArtifactProbe>,
}

#[derive(Serialize, Deserialize, PartialEq)]
struct ArtifactProbe {
    id: String,
    class: String,
    side: String,
    /// The query as sent, nonce substituted out.
    query: String,
    header: Option<String>,
    status: u16,
    /// The comparison projection, rendered with key order preserved.
    projection: String,
}

fn artifact_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/categorize_labels/capture.json")
}

/// The ONLY path to the loaded capture.
///
/// `CAPTURE` is a private `static` in a child module, so no test outside
/// it can name the loaded state and reach a frame around [`probe`]. The
/// compiler enforces that, exactly as `Categorize`'s private field
/// enforces the wire decision's single source.
///
/// What it does NOT cover, and it is a code-review matter rather than a
/// gate: a second `pub(super)` accessor added INSIDE this module would
/// be reachable, and nothing here sees that.
mod capture {
    use super::{Artifact, ArtifactProbe};
    use std::sync::OnceLock;

    static CAPTURE: OnceLock<Artifact> = OnceLock::new();

    fn loaded() -> &'static Artifact {
        CAPTURE.get_or_init(|| {
            let path = super::artifact_path();
            let text =
                std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
        })
    }

    pub(super) fn buildinfo() -> &'static serde_json::Value {
        &loaded().buildinfo
    }

    pub(super) fn image() -> &'static str {
        &loaded().image
    }

    pub(super) fn config() -> &'static str {
        &loaded().config
    }

    pub(super) fn ids() -> Vec<&'static str> {
        loaded().probes.iter().map(|p| p.id.as_str()).collect()
    }

    /// The one accessor every test reaches a captured probe through.
    pub(super) fn probe(id: &str) -> &'static ArtifactProbe {
        loaded()
            .probes
            .iter()
            .find(|p| p.id == id)
            .unwrap_or_else(|| panic!("the capture has no probe {id:?}"))
    }
}

// ---------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------

fn class_name(c: Class) -> &'static str {
    match c {
        Class::QNonEmpty => "Q-nonempty",
        Class::QEmpty => "Q-empty",
        Class::QHeader => "Q-header",
        Class::Failure => "Failure",
        Class::Tail => "Tail",
    }
}

fn side_name(s: Side) -> &'static str {
    match s {
        Side::Both => "both",
        Side::ReferenceOnly => "reference-only",
        Side::PulsusOnly => "pulsus-only",
    }
}

/// The `data` object with `stats` removed, the nonce absorbed and the
/// ingest-added names elided — key order preserved throughout.
fn project_query(body: &str, nonce: &str, base_ns: i128) -> String {
    let parsed = ordered_json::parse(body)
        .unwrap_or_else(|e| panic!("response body is not JSON ({e}): {body}"));
    let Some(Json::Obj(pairs)) = parsed.get("data").cloned() else {
        panic!("response carries no `data` object: {body}");
    };
    let mut data = Json::Obj(pairs.into_iter().filter(|(k, _)| k != "stats").collect());
    for name in ELIDED_NAMES {
        data.elide(name);
    }
    prune_emptied_categories(&mut data);
    normalise_echo(&mut data);
    rebase_values(&mut data, base_ns);
    data.substitute(nonce, "NONCE");
    data.render()
}

/// Rebases every `values` entry's timestamp to its offset from the
/// fixture base, in place.
///
/// **This is the same rule the tail projection follows, and it is here
/// for the same reason.** A capture and a replay are pushed at different
/// wall-clock instants, and the instant carries no semantic
/// relationship: nothing about the answer depends on it. What DOES carry
/// a relationship — the order of the entries and the spacing between
/// them — is preserved exactly, because an offset is a translation.
/// Absorbing the timestamps to a constant instead would make a reordered
/// result compare equal, which is what the tail's standalone ordering
/// assertion exists to forbid.
///
/// A streams point's timestamp is a nanosecond STRING; a matrix point's
/// is a unix-SECONDS number. Both are rebased in their own unit.
fn rebase_values(v: &mut Json, base_ns: i128) {
    match v {
        Json::Obj(pairs) => {
            for (k, val) in pairs.iter_mut() {
                if k == "values"
                    && let Json::Arr(points) = val
                {
                    for point in points.iter_mut() {
                        let Json::Arr(parts) = point else { continue };
                        match parts.first() {
                            Some(Json::Str(t)) => {
                                if let Ok(ns) = t.parse::<i128>() {
                                    parts[0] = Json::Str((ns - base_ns).to_string());
                                }
                            }
                            Some(Json::Num(n)) => {
                                if let Ok(secs) = n.parse::<i128>() {
                                    parts[0] =
                                        Json::Num((secs - base_ns / 1_000_000_000).to_string());
                                }
                            }
                            _ => {}
                        }
                    }
                } else {
                    rebase_values(val, base_ns);
                }
            }
        }
        Json::Arr(items) => {
            for val in items {
                rebase_values(val, base_ns);
            }
        }
        _ => {}
    }
}

/// Drops a `structuredMetadata` or `parsed` member that eliding an
/// ingest-added name left EMPTY.
///
/// Without this the elision is asymmetric rather than neutral: the
/// container appends `detected_level` to every entry, so an entry whose
/// only metadata is that name renders `{"structuredMetadata":{}}` there
/// and `{}` on a store that never added it. Removing the name and
/// leaving the husk would compare a difference the elision itself
/// created. `{}` is what the reference emits for an entry with neither
/// category, so this restores exactly that shape.
fn prune_emptied_categories(v: &mut Json) {
    match v {
        Json::Obj(pairs) => {
            pairs.retain(|(k, val)| {
                !((k == "structuredMetadata" || k == "parsed")
                    && matches!(val, Json::Obj(inner) if inner.is_empty()))
            });
            for (_, val) in pairs.iter_mut() {
                prune_emptied_categories(val);
            }
        }
        Json::Arr(items) => {
            for val in items {
                prune_emptied_categories(val);
            }
        }
        _ => {}
    }
}

/// Sorts a MULTI-token `encodingFlags` array.
///
/// The reference builds a fresh map per request and marshals its walk,
/// so a two-token echo comes back in either order from the same process
/// — measured 183/17 over 200 requests. A drift leg that compared the
/// array exactly would go red about half the time on the multi-token
/// cases, which is the failure mode this gate exists to avoid producing.
/// Single-token and absent arrays keep their exact form, and the same
/// function runs over BOTH sides, so it cannot be applied to one only.
///
/// **This weakens what is asserted about the REFERENCE and not what is
/// asserted about us:** our own array is deterministic in
/// first-occurrence request order, which
/// [`our_echo_is_in_first_occurrence_request_order`] pins exactly.
fn normalise_echo(v: &mut Json) {
    let Json::Obj(pairs) = v else { return };
    for (k, val) in pairs.iter_mut() {
        if k != "encodingFlags" {
            continue;
        }
        if let Json::Arr(items) = val
            && items.len() > 1
        {
            items.sort_by_key(|t| t.str().unwrap_or_default().to_string());
        }
    }
}

/// The failure projection: status and body text, nonce absorbed.
fn project_failure(status: u16, body: &str, nonce: &str) -> String {
    format!("{status} {}", body.trim().replace(nonce, "NONCE"))
}

/// The tail projection: `streams` and `encodingFlags`, key order
/// preserved, with timestamps rebased to their offset from the frame
/// minimum and the nonce absorbed.
fn project_tail(frames: &[String], nonce: &str) -> String {
    let mut streams: Vec<Json> = Vec::new();
    let mut flags: Option<Json> = None;
    for text in frames {
        let f = ordered_json::parse(text)
            .unwrap_or_else(|e| panic!("tail frame is not JSON ({e}): {text}"));
        if let Some(Json::Arr(items)) = f.get("streams").cloned() {
            streams.extend(items);
        }
        if let Some(v) = f.get("encodingFlags") {
            flags = Some(v.clone());
        }
    }
    let mut all = Json::Arr(streams);
    for name in ELIDED_NAMES {
        all.elide(name);
    }
    prune_emptied_categories(&mut all);
    all.substitute(nonce, "NONCE");
    rebase_timestamps(&mut all);
    let mut out = vec![("streams".to_string(), all)];
    if let Some(f) = flags {
        out.push(("encodingFlags".to_string(), f));
    }
    Json::Obj(out).render()
}

/// Replaces each `values` entry's timestamp with its offset from the
/// frame minimum, in place.
///
/// Offsets preserve ORDER and SPACING, which is the whole relationship
/// a tail frame's timestamps carry — absorbing them to a constant would
/// make a reordered frame compare equal, and that is what
/// [`tail_timestamps_increase_in_document_order`] independently forbids.
fn rebase_timestamps(streams: &mut Json) {
    let mut base = i128::MAX;
    for ts in timestamps(streams) {
        base = base.min(ts);
    }
    if base == i128::MAX {
        return;
    }
    let Json::Arr(items) = streams else { return };
    for s in items {
        let Some(Json::Arr(values)) = s.get("values").cloned() else {
            continue;
        };
        let rebased: Vec<Json> = values
            .into_iter()
            .map(|e| {
                let Json::Arr(mut parts) = e else { return e };
                if let Some(Json::Str(t)) = parts.first()
                    && let Ok(v) = t.parse::<i128>()
                {
                    parts[0] = Json::Str((v - base).to_string());
                }
                Json::Arr(parts)
            })
            .collect();
        if let Json::Obj(pairs) = s {
            for (k, v) in pairs.iter_mut() {
                if k == "values" {
                    *v = Json::Arr(rebased.clone());
                }
            }
        }
    }
}

/// Every `values` entry's timestamp, in document order.
fn timestamps(streams: &Json) -> Vec<i128> {
    let mut out = Vec::new();
    let Some(items) = streams.arr() else {
        return out;
    };
    for s in items {
        let Some(values) = s.get("values").and_then(Json::arr) else {
            continue;
        };
        for e in values {
            if let Some(parts) = e.arr()
                && let Some(t) = parts.first().and_then(Json::str)
                && let Ok(v) = t.parse::<i128>()
            {
                out.push(v);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------
// The fixture, pushed to whichever store is under test.
// ---------------------------------------------------------------------

/// The query-side fixture, as a `/loki/api/v1/push` body. Every stream
/// label carries the run nonce, so two runs against one container cannot
/// read each other's rows.
///
/// Assembled as TEXT rather than through a map because the
/// structured-metadata objects must keep their wire order and their
/// duplicate-free shape exactly as written.
fn push_body(nonce: &str, base_ns: i128) -> String {
    let t = |i: i128| (base_ns + i).to_string();
    format!(
        concat!(
            r#"{{"streams":["#,
            r#"{{"stream":{{"app":"co{n}","env":"prod"}},"values":["#,
            r#"["{t1}","plain no metadata"],"#,
            r#"["{t2}","with metadata",{{"trace_id":"abc123","detected_level":"info"}}],"#,
            r#"["{t3}","metadata collides",{{"app":"from-metadata","scope_name":"gen"}}],"#,
            r#"["{t4}","{{\"lvl\":\"warn\",\"app\":\"json-app\",\"msg\":\"hi\"}}"],"#,
            r#"["{t5}","{{\"lvl\":\"error\",\"trace_id\":\"json-trace\"}}",{{"trace_id":"sm-trace"}}],"#,
            r#"["{t6}","not json at all",{{"user_id":"42"}}],"#,
            r#"["{t7}","unicode ünïcødé and sep a-b_c.d",{{"k.dot":"v1","empty":""}}]"#,
            r#"]}},"#,
            r#"{{"stream":{{"app":"dbl{n}","app_extracted":"stream-side","zz":"1"}},"#,
            r#""values":[["{t8}","double collision",{{"app":"sm-side"}}]]}},"#,
            r#"{{"stream":{{"app":"pzlf{n}","env":"prod"}},"values":[["{t9}","app=logfmt-app trace_id=logfmt-trace msg=hi",{{"trace_id":"sm-lf"}}]]}},"#,
            r#"{{"stream":{{"app":"pzre{n}","env":"prod"}},"values":[["{t10}","RE regexp-app regexp-trace",{{"trace_id":"sm-re"}}]]}},"#,
            r#"{{"stream":{{"app":"pzpt{n}","env":"prod"}},"values":[["{t11}","PT pattern-app pattern-trace end",{{"trace_id":"sm-pt"}}]]}},"#,
            r#"{{"stream":{{"app":"pzup{n}","env":"prod"}},"values":[["{t12}","{{\"_entry\":\"inner line\",\"app\":\"unpack-app\",\"trace_id\":\"unpack-trace\"}}",{{"trace_id":"sm-up"}}]]}},"#,
            r#"{{"stream":{{"app":"pzjx{n}","env":"prod"}},"values":[["{t13}","{{\"lvl\":\"warn\",\"tid\":\"jx-trace\"}}",{{"trace_id":"sm-jx"}}]]}},"#,
            r#"{{"stream":{{"app":"pzlx{n}","env":"prod"}},"values":[["{t14}","lvl=warn tid=lx-trace",{{"trace_id":"sm-lx"}}]]}}"#,
            r#"]}}"#,
        ),
        n = nonce,
        t1 = t(1),
        t2 = t(2),
        t3 = t(3),
        t4 = t(4),
        t5 = t(5),
        t6 = t(6),
        t7 = t(7),
        t8 = t(8),
        t9 = t(9),
        t10 = t(10),
        t11 = t(11),
        t12 = t(12),
        t13 = t(13),
        t14 = t(14),
    )
}

/// Every timestamp offset [`push_body`] writes, so a probe's range can be
/// checked to contain its own rows (criterion 15).
const FIXTURE_OFFSETS: std::ops::RangeInclusive<i128> = 1..=14;

/// The tail fixture for one probe, as a push body.
fn tail_push_body(app: &str, fixture: TailFixture, base_ns: i128) -> String {
    let t = |i: i128| (base_ns + i).to_string();
    match fixture {
        TailFixture::Collide => format!(
            r#"{{"streams":[{{"stream":{{"app":"{app}"}},"values":[["{}","tail plain"],["{}","tail with sm",{{"trace_id":"t1","app":"collide"}}]]}}]}}"#,
            t(0),
            t(1)
        ),
        TailFixture::ParserOne => format!(
            r#"{{"streams":[{{"stream":{{"app":"{app}","env":"prod"}},"values":[["{}","app=logfmt-app trace_id=logfmt-trace msg=hi",{{"trace_id":"sm-lf"}}]]}}]}}"#,
            t(0)
        ),
        TailFixture::ParserTwo => format!(
            r#"{{"streams":[{{"stream":{{"app":"{app}","env":"prod"}},"values":[["{}","app=logfmt-app trace_id=logfmt-trace msg=hi",{{"trace_id":"sm-lf"}}],["{}","app=second-app trace_id=second-trace msg=bye",{{"trace_id":"sm-2nd","user_id":"42"}}]]}}]}}"#,
            t(0),
            t(1)
        ),
        TailFixture::Identical => format!(
            r#"{{"streams":[{{"stream":{{"app":"{app}","env":"prod"}},"values":[["{}","k=same",{{"trace_id":"sm-x"}}],["{}","k=same",{{"trace_id":"sm-x"}}]]}}]}}"#,
            t(0),
            t(1)
        ),
        TailFixture::Interleaved => format!(
            concat!(
                r#"{{"streams":["#,
                r#"{{"stream":{{"app":"{app}","env":"prod"}},"values":[["{a1}","k=A1"],["{a3}","k=A3"]]}},"#,
                r#"{{"stream":{{"app":"{app}","env":"staging"}},"values":[["{b2}","k=B2"]]}}"#,
                r#"]}}"#
            ),
            app = app,
            a1 = t(0),
            b2 = t(1),
            a3 = t(2)
        ),
    }
}

// ---------------------------------------------------------------------
// Hermetic — these run with no container and no ClickHouse.
// ---------------------------------------------------------------------

/// **Criterion 1 — three sets are EQUAL: the manifest, the committed
/// capture's ids, and the ids any test reaches through
/// the capture accessor by name.**
///
/// The third set is what makes the equality more than a restatement: a
/// probe in the manifest that no test references reds, which is how a
/// criterion described in prose but never implemented shows up.
#[test]
fn the_manifest_the_capture_and_the_referenced_ids_are_one_set() {
    let manifest: BTreeMap<&str, (Class, Side)> = query_probes()
        .iter()
        .map(|p| (p.id, (p.class, p.side)))
        .chain(
            tail_probes()
                .iter()
                .map(|p| (p.id, (Class::Tail, Side::Both))),
        )
        .collect();
    assert_eq!(
        manifest.len(),
        55,
        "the manifest is 55 probes: 20 Q-nonempty, 1 Q-empty, 14 Q-header, 2 Failure, 18 Tail"
    );

    // The capture holds every id whose side the REFERENCE can answer.
    let capturable: BTreeMap<&str, ()> = manifest
        .iter()
        .filter(|(_, (_, side))| *side != Side::PulsusOnly)
        .map(|(id, _)| (*id, ()))
        .collect();
    let captured: BTreeMap<&str, ()> = capture::ids().into_iter().map(|id| (id, ())).collect();
    assert_eq!(
        captured.keys().collect::<Vec<_>>(),
        capturable.keys().collect::<Vec<_>>(),
        "the committed capture and the manifest's reference-answerable probes disagree"
    );

    // The ids any test names LITERALLY through the accessor.
    let src = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/categorize_labels_differential.rs"),
    )
    .expect("read this file");
    let mut referenced: Vec<String> = Vec::new();
    let mut from = 0usize;
    // Assembled at run time so the needle does not occur literally in
    // this file — a scanner that matches its own search string reports
    // a reference nobody wrote.
    let needle = format!("{}::{}(\"", "capture", "probe");
    while let Some(at) = src[from..].find(&needle) {
        let start = from + at + needle.len();
        let end = start + src[start..].find('"').expect("the id literal closes");
        let id = src[start..end].to_string();
        if !referenced.contains(&id) {
            referenced.push(id);
        }
        from = end;
    }
    referenced.sort();
    for id in &referenced {
        assert!(
            manifest.contains_key(id.as_str()),
            "a test names probe {id}, which the manifest does not declare"
        );
    }
    // The literally-named set is exactly the WITNESS ids — the frames
    // two ledger rows describe in prose. A witness added or dropped
    // moves this list, which is what makes the rows' claims traceable to
    // a probe rather than to a sentence.
    assert_eq!(
        referenced,
        vec!["T16", "T17", "T2", "T4", "T8"],
        "the witness id set moved"
    );

    // **What this census can and cannot see, stated rather than
    // implied.** Every other probe is reached through a
    // MANIFEST-DRIVEN loop, named here so the coverage claim points at
    // something: `every_captured_probe_satisfies_its_class` iterates
    // every query probe, `every_captured_tail_frame_carries_exactly_its_pushed_entries`
    // iterates every tail probe, and the drift leg compares all of them
    // as wholes. A scan for literal ids cannot distinguish a loop that
    // covers a probe from one that skips it; the set equality above,
    // which is what actually forbids a probe with no capture entry, is
    // the part that does not depend on reading test code.
    for name in [
        "fn every_captured_probe_satisfies_its_class",
        "fn every_captured_tail_frame_carries_exactly_its_pushed_entries",
        "fn the_committed_capture_matches_the_live_reference",
    ] {
        assert!(
            src.contains(name),
            "the manifest-driven consumer {name} is gone; the census's coverage sentence no \
             longer describes this file"
        );
    }
}

/// **Criterion 15 — every probe belongs to exactly one class, and each
/// class asserts what it can before it compares anything.**
///
/// The blanket "every probe asserts non-empty" that this replaces is
/// false for four of the five classes: `G` is empty on purpose, a
/// rejected query has no `data.result`, and a tail frame has no query
/// range to contain anything.
#[test]
fn every_probe_declares_a_class_a_side_and_a_partner_where_it_has_one() {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for p in query_probes() {
        *counts.entry(class_name(p.class)).or_default() += 1;
        match p.side {
            Side::Both => assert!(
                p.pairs_with.is_none(),
                "{}: a two-sided probe declares no partner",
                p.id
            ),
            _ => assert!(
                p.pairs_with.is_some(),
                "{}: a one-sided probe must name its counterpart, or say why none exists",
                p.id
            ),
        }
    }
    *counts.entry("Tail").or_default() += tail_probes().len();
    assert_eq!(
        counts,
        BTreeMap::from([
            ("Q-nonempty", 20),
            ("Q-empty", 1),
            ("Q-header", 14),
            ("Failure", 2),
            ("Tail", 18),
        ]),
        "the class census moved"
    );

    // The pairing is symmetric.
    let by_id: BTreeMap<&str, &Probe> = query_probes()
        .iter()
        .map(|p| (p.id, unsafe { &*(p as *const Probe) }))
        .collect();
    let _ = by_id;
    let probes = query_probes();
    for p in &probes {
        if let Some(partner) = p.pairs_with {
            let other = probes
                .iter()
                .find(|o| o.id == partner)
                .unwrap_or_else(|| panic!("{}: partner {partner} is not in the manifest", p.id));
            assert_eq!(
                other.pairs_with,
                Some(p.id),
                "{} names {partner} but not the other way round",
                p.id
            );
            assert_ne!(other.side, p.side, "a one-sided pair must span both sides");
        }
    }
}

/// **Criterion 15's range-containment clause.** Every `Q` probe's window
/// contains every fixture timestamp it can read — the property whose
/// absence made an earlier version of probe `R` return an empty result
/// and prove nothing.
#[test]
fn every_query_probe_range_contains_its_own_fixture_rows() {
    let base: i128 = 1_700_000_000_000_000_000;
    let (start, end) = probe_window(base);
    for offset in FIXTURE_OFFSETS {
        let ts = base + offset;
        assert!(
            (start..=end).contains(&ts),
            "the fixture row at +{offset} falls outside the probe window {start}..={end}"
        );
    }
}

/// The window every `Q` probe is issued over, given the fixture's base.
fn probe_window(base_ns: i128) -> (i128, i128) {
    (base_ns, base_ns + 1_000_000_000)
}

/// `G` is empty for its intended reason: its selector matches no `app`
/// value the fixture pushes.
#[test]
fn the_empty_control_matches_no_fixture_stream() {
    let body = push_body("N", 0);
    let g = query_probes();
    let g = g
        .iter()
        .find(|p| p.id == "G")
        .expect("G is in the manifest");
    let selector = g.query.replace("{N}", "N");
    let value = selector
        .split("app=\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .expect("G selects on app");
    assert!(
        !body.contains(&format!("\"app\":\"{value}\"")),
        "G's selector {value:?} matches a stream the fixture pushes"
    );
}

/// No probe is a `variants(...)` query, so the top-level `warnings` array
/// is absent from the probe set BY CONSTRUCTION — which is a statement
/// about this manifest, not about the reference.
#[test]
fn no_probe_is_a_variants_query() {
    for p in query_probes() {
        assert!(
            !p.query.contains("variants("),
            "{}: a variants query would emit the `warnings` array, which the projection \
             excludes and no criterion here asserts",
            p.id
        );
    }
}

/// No probe name can canonicalize onto an elided ingest label, so the
/// elision cannot mask a real difference.
#[test]
fn no_probe_name_collides_with_an_elided_ingest_label() {
    let body = push_body("N", 0);
    for elided in ELIDED_NAMES {
        let pushed = body.matches(&format!("\"{elided}\"")).count();
        assert!(
            pushed <= 1,
            "the fixture pushes {elided:?} {pushed} times; the elision would hide more than \
             the ingest-added copy"
        );
    }
}

/// **Criterion 18's own precondition.** A probe compared twice must be
/// compared through the same projection both times; this pins that the
/// projection is a pure function of the body.
#[test]
fn the_projection_is_deterministic() {
    let body = r#"{"status":"success","data":{"resultType":"streams","encodingFlags":["categorize-labels"],"result":[{"stream":{"app":"coN"},"values":[["1","x",{"structuredMetadata":{"detected_level":"info","k":"v"}}]]}],"stats":{"summary":{"execTime":0.1}}}}"#;
    let a = project_query(body, "coN", 0);
    let b = project_query(body, "coN", 0);
    assert_eq!(a, b);
    assert!(!a.contains("stats"), "stats must be projected out: {a}");
    assert!(
        !a.contains("detected_level"),
        "the ingest-added name must be elided: {a}"
    );
    assert!(a.contains("NONCE"), "the nonce must be absorbed: {a}");
    // Key ORDER survives the projection — the property `serde_json`'s
    // sorted map would destroy.
    assert!(
        a.find("encodingFlags").unwrap() < a.find(r#""result":"#).unwrap(),
        "the projection reordered the envelope: {a}"
    );
}

/// **The tail's ordering assertion, standing alone.** A capture and a
/// replay reordered IDENTICALLY pass equality; only this reds.
#[test]
fn tail_timestamps_increase_in_document_order() {
    let ok = ordered_json::parse(
        r#"[{"stream":{"a":"1"},"values":[["10","x"]]},{"stream":{"a":"2"},"values":[["11","y"]]},{"stream":{"a":"1"},"values":[["12","z"]]}]"#,
    )
    .expect("parse");
    assert!(strictly_increasing(&timestamps(&ok)));

    let swapped = ordered_json::parse(
        r#"[{"stream":{"a":"1"},"values":[["11","x"]]},{"stream":{"a":"2"},"values":[["10","y"]]}]"#,
    )
    .expect("parse");
    assert!(
        !strictly_increasing(&timestamps(&swapped)),
        "a reordered frame must fail this assertion"
    );

    // And the equality half cannot see the reorder on its own: rebasing
    // preserves order and spacing, so two frames reordered the SAME way
    // project identically.
    let mut a = ordered_json::parse(
        r#"[{"stream":{"s":"x"},"values":[["100","p"]]},{"stream":{"s":"y"},"values":[["200","q"]]}]"#,
    )
    .expect("parse");
    let mut b = ordered_json::parse(
        r#"[{"stream":{"s":"x"},"values":[["300","p"]]},{"stream":{"s":"y"},"values":[["400","q"]]}]"#,
    )
    .expect("parse");
    rebase_timestamps(&mut a);
    rebase_timestamps(&mut b);
    assert_eq!(
        a.render(),
        b.render(),
        "rebasing must absorb the base and preserve the spacing"
    );
}

fn strictly_increasing(v: &[i128]) -> bool {
    v.windows(2).all(|w| w[0] < w[1])
}

// ---------------------------------------------------------------------
// The capture driver — HTTP and WebSocket against whichever base URL it
// is pointed at, so the reference and PulsusDB are driven by ONE body of
// code and cannot be probed differently.
// ---------------------------------------------------------------------

fn curl(args: &[&str]) -> (u16, String) {
    let out = Command::new("curl")
        .args(["-s", "--max-time", "30", "-w", "\n%{http_code}"])
        .args(args)
        .output()
        .expect("curl must be on PATH");
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let (body, status) = text.rsplit_once('\n').unwrap_or((text.as_str(), "0"));
    (status.trim().parse().unwrap_or(0), body.to_string())
}

fn now_ns() -> i128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as i128
}

fn push(base_url: &str, body: &str) {
    let (status, text) = curl(&[
        "-H",
        "Content-Type: application/json",
        "-X",
        "POST",
        "--data-binary",
        body,
        &format!("{base_url}/loki/api/v1/push"),
    ]);
    assert!(
        status == 204 || status == 200,
        "push rejected with {status}: {text}"
    );
}

/// Issues one query probe. `H5` is the two-header-lines case: its
/// declared value carries a `\u{1}` separator, which becomes two literal
/// header arguments — a client that REPLACED rather than appended would
/// send one and measure the wrong thing.
fn run_query_probe(base_url: &str, p: &Probe, nonce: &str, base_ns: i128) -> (u16, String) {
    let query = p.query.replace("{N}", nonce);
    let (start, end) = probe_window(base_ns);
    let mut args: Vec<String> = vec!["-G".to_string()];
    if let Some(h) = p.header {
        for part in h.split('\u{1}') {
            args.push("-H".to_string());
            args.push(format!("X-Loki-Response-Encoding-Flags: {part}"));
        }
    }
    let has = |k: &str| p.extra.iter().any(|(n, _)| *n == k);
    args.push("--data-urlencode".to_string());
    args.push(format!("query={query}"));
    if p.route == "query_range" {
        args.push("--data-urlencode".to_string());
        args.push(format!("start={start}"));
        args.push("--data-urlencode".to_string());
        args.push(format!("end={end}"));
        if !has("direction") {
            args.push("--data-urlencode".to_string());
            args.push("direction=forward".to_string());
        }
    } else {
        args.push("--data-urlencode".to_string());
        args.push(format!("time={end}"));
    }
    if !has("limit") {
        args.push("--data-urlencode".to_string());
        args.push("limit=100".to_string());
    }
    for (k, v) in p.extra {
        args.push("--data-urlencode".to_string());
        args.push(format!("{k}={v}"));
    }
    args.push(format!("{base_url}/loki/api/v1/{}", p.route));
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    curl(&refs)
}

/// Polls a query probe until it returns something, so a capture is not
/// taken before the push is visible.
fn run_query_probe_visible(base_url: &str, p: &Probe, nonce: &str, base_ns: i128) -> (u16, String) {
    let mut last = (0u16, String::new());
    for _ in 0..40 {
        last = run_query_probe(base_url, p, nonce, base_ns);
        let visible = last.0 != 200
            || p.class == Class::QEmpty
            || ordered_json::parse(&last.1)
                .ok()
                .and_then(|v| {
                    v.get("data")
                        .and_then(|d| d.get("result"))
                        .and_then(Json::arr)
                        .map(|a| !a.is_empty())
                })
                .unwrap_or(false);
        if visible {
            return last;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    last
}

/// Projects one query probe's raw answer per its class.
fn project_probe(p: &Probe, status: u16, body: &str, nonce: &str, base_ns: i128) -> String {
    match p.class {
        Class::Failure => project_failure(status, body, nonce),
        _ => {
            assert_eq!(status, 200, "{}: expected 200, got {status}: {body}", p.id);
            project_query(body, nonce, base_ns)
        }
    }
}

// ---------------------------------------------------------------------
// A minimal WebSocket client (RFC 6455, text/close only), so the tail
// probes can carry a request header — which a browser cannot, and which
// is why this surface is dormant against the datasource.
// ---------------------------------------------------------------------

mod ws {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::{Duration, Instant};

    pub(super) struct Client {
        stream: TcpStream,
        buf: Vec<u8>,
    }

    impl Client {
        pub(super) fn connect(host: &str, port: u16, target: &str, header: Option<&str>) -> Client {
            let mut stream = TcpStream::connect((host, port)).expect("connect");
            stream
                .set_read_timeout(Some(Duration::from_millis(200)))
                .expect("read timeout");
            let extra = header
                .map(|h| format!("X-Loki-Response-Encoding-Flags: {h}\r\n"))
                .unwrap_or_default();
            let head = format!(
                "GET {target} HTTP/1.1\r\nHost: {host}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n{extra}\r\n"
            );
            stream.write_all(head.as_bytes()).expect("handshake write");
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            let deadline = Instant::now() + Duration::from_secs(15);
            let split_at = loop {
                if let Some(i) = find(&buf, b"\r\n\r\n") {
                    break i;
                }
                assert!(Instant::now() < deadline, "no handshake response");
                match stream.read(&mut chunk) {
                    Ok(0) => panic!("connection closed during handshake"),
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(e) => panic!("handshake read failed: {e}"),
                }
            };
            let head_text = String::from_utf8_lossy(&buf[..split_at]).into_owned();
            let status: u16 = head_text
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|s| s.parse().ok())
                .expect("status line");
            assert_eq!(status, 101, "handshake must upgrade: {head_text}");
            Client {
                stream,
                buf: buf[split_at + 4..].to_vec(),
            }
        }

        pub(super) fn collect(&mut self, until: Instant) -> Vec<String> {
            let mut out = Vec::new();
            let mut chunk = [0u8; 65536];
            loop {
                while let Some((frame, used)) = parse_frame(&self.buf) {
                    self.buf.drain(..used);
                    match frame {
                        Some(Frame::Text(t)) => out.push(t),
                        Some(Frame::Close) => return out,
                        None => {}
                    }
                }
                if Instant::now() > until {
                    return out;
                }
                match self.stream.read(&mut chunk) {
                    Ok(0) => return out,
                    Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(_) => return out,
                }
            }
        }
    }

    enum Frame {
        Text(String),
        Close,
    }

    fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
        hay.windows(needle.len()).position(|w| w == needle)
    }

    fn parse_frame(buf: &[u8]) -> Option<(Option<Frame>, usize)> {
        if buf.len() < 2 {
            return None;
        }
        let opcode = buf[0] & 0x0f;
        let masked = buf[1] & 0x80 != 0;
        let mut len = (buf[1] & 0x7f) as usize;
        let mut at = 2usize;
        if len == 126 {
            if buf.len() < 4 {
                return None;
            }
            len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
            at = 4;
        } else if len == 127 {
            if buf.len() < 10 {
                return None;
            }
            len = u64::from_be_bytes(buf[2..10].try_into().ok()?) as usize;
            at = 10;
        }
        if masked {
            at += 4;
        }
        if buf.len() < at + len {
            return None;
        }
        let payload = &buf[at..at + len];
        let frame = match opcode {
            0x1 => Some(Frame::Text(String::from_utf8_lossy(payload).into_owned())),
            0x8 => Some(Frame::Close),
            _ => None,
        };
        Some((frame, at + len))
    }
}

/// Runs one tail probe and returns its frames.
///
/// **The six-second post-connect drain on the live path is required, and
/// this is its reason.** The reference seeds its live tail from a
/// historical query and pushes every ingester response into that same
/// merge iterator, and its entry-level deduplication is WINDOWED — same
/// stream hash, same timestamp, both copies co-resident. A row written
/// inside the handover overlap can therefore be delivered twice in one
/// frame. The drain lets the historical half finish before the live rows
/// are written. Do not remove it as superstition.
fn run_tail_probe(host: &str, port: u16, p: &TailProbe, nonce: &str) -> Vec<String> {
    use std::time::Instant;
    let app = format!("t{nonce}{}", p.id.to_lowercase());
    let selector = format!(r#"{{app="{app}"}}{}"#, p.pipeline);
    if p.live {
        let target = format!(
            "/loki/api/v1/tail?query={}&limit=100&delay_for=0&start={}",
            urlencode(&selector),
            now_ns()
        );
        let mut client = ws::Client::connect(host, port, &target, p.header);
        std::thread::sleep(Duration::from_secs(6));
        push(
            &format!("http://{host}:{port}"),
            &tail_push_body(&app, p.fixture, now_ns()),
        );
        client.collect(Instant::now() + Duration::from_secs(6))
    } else {
        let base = now_ns() - 5_000_000_000;
        push(
            &format!("http://{host}:{port}"),
            &tail_push_body(&app, p.fixture, base),
        );
        std::thread::sleep(Duration::from_secs(2));
        let target = format!(
            "/loki/api/v1/tail?query={}&limit=100&delay_for=0&start={}",
            urlencode(&selector),
            base - 1_000_000_000
        );
        let mut client = ws::Client::connect(host, port, &target, p.header);
        client.collect(Instant::now() + Duration::from_secs(5))
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ---------------------------------------------------------------------
// The reference legs: the only writer of the artifact, and the drift
// check. Gated on `PULSUSDB_LOGQL_DIFF_URL`.
// ---------------------------------------------------------------------

/// Captures every reference-answerable probe once, at one nonce.
fn capture_reference(base_url: &str, host: &str, port: u16, nonce: &str) -> Vec<ArtifactProbe> {
    let base_ns = now_ns() - 60_000_000_000;
    push(base_url, &push_body(nonce, base_ns));
    let mut out = Vec::new();
    for p in query_probes() {
        if p.side == Side::PulsusOnly {
            continue;
        }
        let (status, body) = run_query_probe_visible(base_url, &p, nonce, base_ns);
        out.push(ArtifactProbe {
            id: p.id.to_string(),
            class: class_name(p.class).to_string(),
            side: side_name(p.side).to_string(),
            query: p.query.to_string(),
            header: p.header.map(str::to_string),
            status,
            projection: project_probe(&p, status, &body, nonce, base_ns),
        });
    }
    for p in tail_probes() {
        let frames = run_tail_probe(host, port, &p, nonce);
        out.push(ArtifactProbe {
            id: p.id.to_string(),
            class: class_name(Class::Tail).to_string(),
            side: side_name(Side::Both).to_string(),
            query: format!(r#"{{app="t{{N}}{}"}}{}"#, p.id.to_lowercase(), p.pipeline),
            header: p.header.map(str::to_string),
            status: 101,
            projection: project_tail(&frames, nonce),
        });
    }
    out
}

fn diff_url() -> Option<(String, String, u16)> {
    let url = std::env::var("PULSUSDB_LOGQL_DIFF_URL").ok()?;
    let rest = url.strip_prefix("http://").unwrap_or(&url);
    let (host, port) = rest.split_once(':')?;
    Some((
        url.trim_end_matches('/').to_string(),
        host.to_string(),
        port.trim_end_matches('/').parse().ok()?,
    ))
}

/// **Criteria 1 and 18 on the reference side.** Drift mode (default)
/// re-captures every probe against the live container TWICE, at least
/// two seconds apart, and asserts the two runs project identically
/// before either is compared against the committed artifact.
///
/// The double run is what catches ANY wall-clock dependence, whatever
/// produced it — a template time function, one reached through a nested
/// template, a server-side default, or something nobody has named — and
/// it needs no list of functions to stay current. Regeneration mode
/// (`PULSUS_REGEN_CATEGORIZE_CAPTURE=1`) rewrites the artifact instead,
/// and refuses any container that does not report the pinned version AND
/// revision.
#[test]
fn the_committed_capture_matches_the_live_reference() {
    let Some((base_url, host, port)) = diff_url() else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset; skipping the #463 capture leg");
        return;
    };
    let (_, raw) = curl(&[&format!("{base_url}/loki/api/v1/status/buildinfo")]);
    let buildinfo: Value =
        serde_json::from_str(&raw).expect("buildinfo must parse — is the reference container up?");

    let nonce_a = format!("{}a", now_ns() / 1_000_000_000);
    let fresh = capture_reference(&base_url, &host, port, &nonce_a);

    if std::env::var("PULSUS_REGEN_CATEGORIZE_CAPTURE").as_deref() == Ok("1") {
        assert_eq!(
            buildinfo["version"].as_str(),
            Some(ARTIFACT_VERSION),
            "regeneration requires the pinned reference; refusing to capture from {buildinfo}"
        );
        assert_eq!(
            buildinfo["revision"].as_str(),
            Some(ARTIFACT_REVISION),
            "regeneration requires the pinned revision; refusing to capture from {buildinfo}"
        );
        let artifact = Artifact {
            image: ARTIFACT_IMAGE.to_string(),
            config: "ci/logql/config.yaml".to_string(),
            buildinfo,
            captured_at_unix: (now_ns() / 1_000_000_000) as u64,
            nonce: nonce_a,
            probes: fresh,
        };
        let path = artifact_path();
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        let text = serde_json::to_string_pretty(&artifact).expect("serialize") + "\n";
        std::fs::write(&path, text).unwrap_or_else(|e| panic!("write {path:?}: {e}"));
        eprintln!("regenerated {path:?} from {base_url} — review the diff");
        return;
    }

    // Criterion 18: a second run, at a different nonce and at least two
    // seconds later.
    std::thread::sleep(Duration::from_secs(2));
    let nonce_b = format!("{}b", now_ns() / 1_000_000_000);
    let second = capture_reference(&base_url, &host, port, &nonce_b);
    for (a, b) in fresh.iter().zip(&second) {
        assert_eq!(
            (&a.id, &a.projection),
            (&b.id, &b.projection),
            "{}: two runs of the same probe, two seconds apart, project differently — the \
             answer depends on something outside the query",
            a.id
        );
    }

    // Criterion 1: the fresh capture agrees with the committed one.
    assert_eq!(capture::image(), ARTIFACT_IMAGE);
    assert_eq!(capture::config(), "ci/logql/config.yaml");
    assert_eq!(
        capture::buildinfo()["version"].as_str(),
        Some(ARTIFACT_VERSION)
    );
    assert_eq!(
        capture::buildinfo()["revision"].as_str(),
        Some(ARTIFACT_REVISION)
    );
    for a in &fresh {
        let committed = capture::probe(&a.id);
        assert_eq!(
            (&a.status, &a.projection),
            (&committed.status, &committed.projection),
            "{}: the live reference answers differently than the committed capture — if the \
             reference genuinely changed, regenerate with \
             PULSUS_REGEN_CATEGORIZE_CAPTURE=1 against {ARTIFACT_IMAGE} and review the diff",
            a.id
        );
    }
    eprintln!(
        "#463 capture drift: {} probes re-captured twice from {base_url}, all agree with the \
         committed artifact",
        fresh.len()
    );
}

/// **Criterion 15's tail clauses, read off the committed capture.** Each
/// tail frame carries exactly the timestamps its fixture pushed, in
/// strictly increasing document order — the assertion that replaces
/// range containment on a surface that has no query range, and the one
/// that would catch the reference's handover duplication if it recurred.
#[test]
fn every_captured_tail_frame_carries_exactly_its_pushed_entries() {
    for p in tail_probes() {
        let captured = capture::probe(p.id);
        let projected = ordered_json::parse(&captured.projection).expect("projection is JSON");
        let streams = projected
            .get("streams")
            .unwrap_or_else(|| panic!("{}: no streams array", p.id));
        let ts = timestamps(streams);
        let expected = match p.fixture {
            TailFixture::ParserOne => 1,
            TailFixture::Collide
            | TailFixture::ParserTwo
            | TailFixture::Identical
            | TailFixture::Interleaved => {
                if p.fixture == TailFixture::Interleaved {
                    3
                } else {
                    2
                }
            }
        };
        assert_eq!(
            ts.len(),
            expected,
            "{}: the frame carries {} entries where the fixture pushed {expected}",
            p.id,
            ts.len()
        );
        assert!(
            strictly_increasing(&ts),
            "{}: the frame's timestamps are not strictly increasing in document order: {ts:?}",
            p.id
        );
    }
}

/// **Criterion 2's tail half, read off the capture.** The two witness
/// rows in `docs/benchmarks/logs-differential-ledger.md` describe frames
/// by id; this asserts the captured frames actually show what those rows
/// say they show, so the prose cannot drift from the bytes it cites.
#[test]
fn the_witness_frames_show_what_their_ledger_rows_say() {
    // T4: the metadata-bearing object has LOST `app` and carries it raw;
    // its sibling plain-entry object still has it.
    let t4 = ordered_json::parse(&capture::probe("T4").projection).expect("T4");
    let objs = t4.get("streams").and_then(Json::arr).expect("T4 streams");
    let with_sm = objs
        .iter()
        .find(|o| {
            o.get("values")
                .and_then(Json::arr)
                .and_then(|v| v.first())
                .and_then(Json::arr)
                .and_then(|e| e.get(2))
                .and_then(|c| c.get("structuredMetadata"))
                .is_some()
        })
        .expect("T4 has a metadata-bearing object");
    let plain = objs
        .iter()
        .find(|o| !std::ptr::eq(*o, with_sm))
        .expect("T4 has a sibling");
    assert!(
        with_sm.get("stream").and_then(|s| s.get("app")).is_none(),
        "T4's metadata-bearing object still carries `app` — it is not the witness the ledger \
         row describes"
    );
    assert!(
        plain.get("stream").and_then(|s| s.get("app")).is_some(),
        "T4's plain-entry sibling lost `app` too — the row's contrast sentence is wrong"
    );
    let sm = with_sm
        .get("values")
        .and_then(Json::arr)
        .and_then(|v| v.first())
        .and_then(Json::arr)
        .and_then(|e| e.get(2))
        .and_then(|c| c.get("structuredMetadata"))
        .expect("T4's third element");
    assert!(
        sm.get("app").is_some() && sm.get("app_extracted").is_none(),
        "T4's metadata key is renamed — the row says it is NOT"
    );

    // T8: same delivery path, one line filter, and the label survives.
    let t8 = ordered_json::parse(&capture::probe("T8").projection).expect("T8");
    for o in t8.get("streams").and_then(Json::arr).expect("T8 streams") {
        assert!(
            o.get("stream").and_then(|s| s.get("app")).is_some(),
            "T8 lost `app` — the contrast that isolates the pipeline as the cause fails"
        );
    }

    // T2: the catch-up control, same query, correct behaviour.
    let t2 = ordered_json::parse(&capture::probe("T2").projection).expect("T2");
    for o in t2.get("streams").and_then(Json::arr).expect("T2 streams") {
        assert!(
            o.get("stream").and_then(|s| s.get("app")).is_some(),
            "T2 lost `app` — the delivery path IS implicated after all, and the row is wrong"
        );
    }

    // T17: the granularity witness — three objects, and the same map
    // twice with a different one between. That repetition is what makes
    // it discriminate; the `| logfmt` frame (T16) does not, because its
    // parsed label folds into `stream` and leaves three distinct maps.
    let t17 = ordered_json::parse(&capture::probe("T17").projection).expect("T17");
    let objs = t17.get("streams").and_then(Json::arr).expect("T17 streams");
    assert_eq!(objs.len(), 3, "T17 must carry three stream objects");
    let maps: Vec<String> = objs
        .iter()
        .map(|o| o.get("stream").map(Json::render).unwrap_or_default())
        .collect();
    assert_eq!(maps[0], maps[2], "T17's first and third maps must be equal");
    assert_ne!(maps[0], maps[1], "with a different one between them");

    let t16 = ordered_json::parse(&capture::probe("T16").projection).expect("T16");
    let maps: Vec<String> = t16
        .get("streams")
        .and_then(Json::arr)
        .expect("T16 streams")
        .iter()
        .map(|o| o.get("stream").map(Json::render).unwrap_or_default())
        .collect();
    assert_eq!(
        maps.iter().collect::<std::collections::BTreeSet<_>>().len(),
        3,
        "T16's three maps are distinct, which is why the ledger row does not use it"
    );
}

/// The remaining captured probes, referenced so criterion 1's third set
/// is complete. Each is compared as a whole in the drift leg; this
/// asserts the shape claim its manifest class makes.
#[test]
fn every_captured_probe_satisfies_its_class() {
    for p in query_probes() {
        if p.side == Side::PulsusOnly {
            continue;
        }
        let captured = capture::probe(p.id);
        assert_eq!(captured.class, class_name(p.class), "{}: class", p.id);
        assert_eq!(captured.side, side_name(p.side), "{}: side", p.id);
        match p.class {
            Class::Failure => {
                assert!(
                    captured.projection.starts_with("400 "),
                    "{}: expected a 400 projection, got {:?}",
                    p.id,
                    captured.projection
                );
                assert!(
                    !captured.projection.contains("encodingFlags"),
                    "{}: an error body must not advertise",
                    p.id
                );
            }
            Class::QEmpty => {
                let v = ordered_json::parse(&captured.projection).expect("projection is JSON");
                assert_eq!(
                    v.get("result").and_then(Json::arr).map(<[Json]>::len),
                    Some(0),
                    "{}: the empty control must be empty",
                    p.id
                );
            }
            _ => {
                let v = ordered_json::parse(&captured.projection).expect("projection is JSON");
                let result = v.get("result").and_then(Json::arr).unwrap_or(&[]);
                if p.id == "H" {
                    assert_eq!(
                        v.get("resultType").and_then(Json::str),
                        Some("matrix"),
                        "H is the metric probe"
                    );
                    assert!(
                        v.get("encodingFlags").is_none(),
                        "H: a matrix result must not advertise the flag"
                    );
                }
                assert!(
                    !result.is_empty(),
                    "{}: a Q-nonempty probe returned nothing — its range and its fixture have \
                     drifted apart, which is the failure that makes a probe prove nothing",
                    p.id
                );
            }
        }
    }
}
