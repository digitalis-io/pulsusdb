//! Issue #478, criterion 18: the TraceQL tag lookback is exposed in the
//! Helm chart, at the same value the binary defaults to.
//!
//! **Presence is not the property.** A chart that carries the key at a
//! different default silently gives a chart-deployed instance different
//! behaviour from a bare one, so the value is asserted against
//! `Config::default()` rather than against a literal — a drift on either
//! side fails.
//!
//! Four artifacts, read INDEPENDENTLY and each named in its own
//! assertion, so removing the option from exactly one of them names which
//! one: `values.yaml`, `values.schema.json` (whose `reader` block is
//! `additionalProperties: false`, so a key absent there cannot be set
//! through `pulsusdb.config.reader` at all) and the two rendered chart
//! goldens.
//!
//! The chart's rule is "expose user-facing settings, keep tuning caps
//! out": `promql_lookback` — the analogous knob — is in, while
//! `traceql_scan_budget_rows` and `traceql_max_series` are not. A lookback
//! that decides what a dropdown shows is the first kind.

use std::path::{Path, PathBuf};

use pulsus_config::Config;

/// The chart key this test is about.
const KEY: &str = "traceql_tag_lookback";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repo root")
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

/// serde_norway parses JSON as well as YAML (JSON is a YAML subset), so
/// the schema and the value files are read with one parser and no extra
/// dependency.
fn parse_doc(text: &str, what: &str) -> serde_norway::Value {
    serde_norway::from_str(text).unwrap_or_else(|e| panic!("parsing {what}: {e}"))
}

fn get<'a>(doc: &'a serde_norway::Value, path: &[&str]) -> &'a serde_norway::Value {
    let mut cur = doc;
    for step in path {
        cur = cur
            .get(step)
            .unwrap_or_else(|| panic!("missing {step} in {path:?}"));
    }
    cur
}

/// The configured default, as a `Duration`, so the comparison is on the
/// meaning of the string rather than on its spelling.
fn parse_duration(raw: &str, what: &str) -> std::time::Duration {
    raw.parse::<pulsus_config::HumanDuration>()
        .unwrap_or_else(|e| panic!("{what} carries {raw:?}, which is not a duration: {e}"))
        .0
}

#[test]
fn the_lookback_is_present_in_all_four_chart_artifacts() {
    let want = Config::default().reader.traceql_tag_lookback.0;

    // 1. values.yaml — the operator-visible default.
    let values = parse_doc(&read("deploy/charts/pulsusdb/values.yaml"), "values.yaml");
    let raw = get(&values, &["pulsusdb", "config", "reader", KEY])
        .as_str()
        .expect("values.yaml: the lookback must be a duration string");
    assert_eq!(
        parse_duration(raw, "values.yaml"),
        want,
        "values.yaml's {KEY} must equal the binary default"
    );

    // 2. values.schema.json — without the property, `additionalProperties:
    //    false` makes the key unsettable through `pulsusdb.config.reader`.
    //    The `$ref` matters as well as the presence: without it the key
    //    would be schema-valid as any JSON type, so `traceql_tag_lookback:
    //    12` would render where the config expects a duration string.
    let schema = parse_doc(
        &read("deploy/charts/pulsusdb/values.schema.json"),
        "values.schema.json",
    );
    // `pulsusdb.config` is a `$ref` to `pulsusdbConfig`, so the reader
    // block lives under `definitions`, not inline.
    let reader = get(
        &schema,
        &["definitions", "pulsusdbConfig", "properties", "reader"],
    );
    assert_eq!(
        reader.get("additionalProperties").and_then(|v| v.as_bool()),
        Some(false),
        "the reader block is closed, which is why a missing property makes the key unsettable"
    );
    let property = get(reader, &["properties", KEY]);
    assert_eq!(
        property.get("$ref").and_then(|v| v.as_str()),
        Some("#/definitions/humanDuration"),
        "values.schema.json: {KEY} must carry the same $ref its sibling promql_lookback does"
    );
    // The sibling is read rather than assumed, so a rename of the shared
    // definition fails here instead of leaving two names that agree by
    // coincidence.
    assert_eq!(
        get(reader, &["properties", "promql_lookback"])
            .get("$ref")
            .and_then(|v| v.as_str()),
        Some("#/definitions/humanDuration"),
    );

    // 3 and 4. Both rendered goldens, each named.
    for golden in ["single", "cluster"] {
        let rendered = read(&format!(
            "deploy/charts/pulsusdb/tests/golden/{golden}.yaml"
        ));
        let line = rendered
            .lines()
            .find(|l| l.trim_start().starts_with(&format!("{KEY}:")))
            .unwrap_or_else(|| panic!("{golden}.yaml renders no {KEY} line"));
        let raw = line
            .split_once(':')
            .map(|(_, v)| v.trim())
            .unwrap_or_default();
        assert_eq!(
            parse_duration(raw, &format!("{golden}.yaml")),
            want,
            "{golden}.yaml renders {KEY} at {raw}, not the binary default"
        );
    }
}

/// The convention this plan followed, asserted so the next reader sees it
/// was a decision: the two TraceQL tuning caps stay OUT of the chart, and
/// the analogous lookback is IN.
#[test]
fn the_chart_exposes_the_lookback_and_not_the_tuning_caps() {
    let schema = parse_doc(
        &read("deploy/charts/pulsusdb/values.schema.json"),
        "values.schema.json",
    );
    let reader = get(
        &schema,
        &[
            "definitions",
            "pulsusdbConfig",
            "properties",
            "reader",
            "properties",
        ],
    );
    assert!(reader.get(KEY).is_some(), "{KEY} must be exposed");
    for cap in ["traceql_scan_budget_rows", "traceql_max_series"] {
        assert!(
            reader.get(cap).is_none(),
            "{cap} is a tuning cap and is deliberately not in the chart"
        );
    }
}
