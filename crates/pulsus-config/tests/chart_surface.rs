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

mod support;

use std::collections::BTreeSet;
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
    // Presence only: which values the property accepts is
    // `the_chart_schema_accepts_no_value_the_binary_refuses`.
    get(reader, &["properties", KEY]);

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

/// The key paths of the chart schema's `pulsusdbConfig`, read through
/// nested `properties`.
fn schema_key_paths(node: &serde_norway::Value, prefix: &str, out: &mut BTreeSet<String>) {
    if let Some(props) = node.get("properties").and_then(|p| p.as_mapping()) {
        for (k, child) in props {
            let k = k.as_str().expect("schema property names are strings");
            let path = if prefix.is_empty() {
                k.to_string()
            } else {
                format!("{prefix}.{k}")
            };
            out.insert(path.clone());
            schema_key_paths(child, &path, out);
        }
    }
}

fn chart_config() -> serde_norway::Value {
    let values = parse_doc(&read("deploy/charts/pulsusdb/values.yaml"), "values.yaml");
    get(&values, &["pulsusdb", "config"]).clone()
}

/// `values.yaml` and `values.schema.json` carry the same configuration
/// keys, section by section: a key only in the schema has no documented
/// default, and a key only in `values.yaml` fails the chart's own
/// `additionalProperties: false`.
#[test]
fn the_chart_values_and_schema_carry_the_same_config_keys() {
    let schema = parse_doc(
        &read("deploy/charts/pulsusdb/values.schema.json"),
        "values.schema.json",
    );
    let mut in_schema = BTreeSet::new();
    schema_key_paths(
        get(&schema, &["definitions", "pulsusdbConfig"]),
        "",
        &mut in_schema,
    );
    let in_values = support::key_paths(&chart_config());
    let only_schema: Vec<_> = in_schema.difference(&in_values).collect();
    let only_values: Vec<_> = in_values.difference(&in_schema).collect();
    assert!(
        only_schema.is_empty() && only_values.is_empty(),
        "only in values.schema.json: {only_schema:?}; only in values.yaml: {only_values:?}"
    );
}

/// The keys whose chart value is deliberately not the binary default, each
/// with the value `values.yaml` gives it. `clickhouse.server` is empty there
/// and filled at render time (`_helpers.tpl`'s `pulsusdb.clickhouseServer`).
/// Asserted by equality, path and value.
const CHART_OWNED_DEFAULTS: &[(&str, &str)] = &[("clickhouse.server", "")];

/// Every configuration key the chart carries is a key the binary accepts,
/// at the binary's default unless it is listed above. Each key is parsed on
/// its own, so a drifted key is named.
#[test]
fn every_chart_config_key_is_a_binary_key_at_the_binary_default() {
    fn leaves(prefix: &str, v: &serde_norway::Value, out: &mut Vec<(String, serde_norway::Value)>) {
        match v.as_mapping() {
            Some(m) if !m.is_empty() => {
                for (k, child) in m {
                    let k = k.as_str().expect("string key");
                    let path = if prefix.is_empty() {
                        k.to_string()
                    } else {
                        format!("{prefix}.{k}")
                    };
                    leaves(&path, child, out);
                }
            }
            _ => out.push((prefix.to_string(), v.clone())),
        }
    }
    let mut all = Vec::new();
    leaves("", &chart_config(), &mut all);
    let mut differs = Vec::new();
    for (path, leaf) in &all {
        let mut doc = leaf.clone();
        for step in path.rsplit('.') {
            let mut m = serde_norway::Mapping::new();
            m.insert(serde_norway::Value::String(step.to_string()), doc);
            doc = serde_norway::Value::Mapping(m);
        }
        let cfg: Config = serde_norway::from_value(doc).unwrap_or_else(|e| {
            panic!("values.yaml pulsusdb.config.{path}: the binary rejects it: {e}")
        });
        if cfg != Config::default() {
            differs.push((path.clone(), leaf.clone()));
        }
    }
    let want: Vec<(String, serde_norway::Value)> = CHART_OWNED_DEFAULTS
        .iter()
        .map(|(p, v)| (p.to_string(), serde_norway::Value::String(v.to_string())))
        .collect();
    assert_eq!(
        differs, want,
        "values.yaml keys whose default is not the binary default, with their values"
    );
}

/// The README values-table cells that are words rather than a value, with
/// the exact cell. Asserted by equality.
const README_WORD_DEFAULTS: &[(&str, &str)] = &[("pulsusdb.config.*", "see `values.yaml`")];

/// The chart README's "Values reference" table and `values.yaml`, both ways:
/// every key the table names exists in `values.yaml`, every `values.yaml`
/// leaf lies under a key the table names, and every default the table
/// states is the `values.yaml` value exactly. A default written in words is
/// listed above, with its cell.
#[test]
fn the_chart_readme_values_table_matches_values_yaml() {
    let values = parse_doc(&read("deploy/charts/pulsusdb/values.yaml"), "values.yaml");
    let readme = read("deploy/charts/pulsusdb/README.md");
    let table = readme
        .split_once("\n## Values reference\n")
        .expect("the chart README has a Values reference section")
        .1;
    let table = table.split("\n## ").next().unwrap_or_default();
    let mut named: Vec<String> = Vec::new();
    let mut wrong = Vec::new();
    let mut words = Vec::new();
    for line in table.lines().filter(|l| l.starts_with("| `")) {
        let cells: Vec<&str> = line.split(" | ").map(str::trim).collect();
        let first = cells[0].trim_start_matches('|').trim();
        let names: Vec<&str> = first.split(" / ").map(|k| k.trim_matches('`')).collect();
        let parent = names[0].rsplit_once('.').map(|(p, _)| p).unwrap_or("");
        let keys: Vec<String> = names
            .iter()
            .map(|k| match k.strip_prefix('.') {
                Some(rest) => format!("{parent}.{rest}"),
                None => k.to_string(),
            })
            .collect();
        let parts: Vec<&str> = cells[2].split(" / ").collect();
        for (i, key) in keys.iter().enumerate() {
            named.push(key.clone());
            let part = if parts.len() == keys.len() {
                parts[i]
            } else {
                cells[2]
            };
            let literal = part
                .strip_prefix('`')
                .and_then(|p| p.strip_suffix('`'))
                .filter(|p| !p.contains('`'));
            let Some(literal) = literal else {
                words.push((key.clone(), part.to_string()));
                continue;
            };
            let cur = key
                .split('.')
                .try_fold(&values, |v, s| v.get(s))
                .unwrap_or_else(|| panic!("README names {key}, which values.yaml does not have"));
            let shown: serde_norway::Value = serde_norway::from_str(literal)
                .unwrap_or_else(|e| panic!("README default for {key}, {literal:?}: {e}"));
            if &shown != cur {
                wrong.push(format!("{key}: README `{literal}`, values.yaml {cur:?}"));
            }
        }
    }
    let want: Vec<(String, String)> = README_WORD_DEFAULTS
        .iter()
        .map(|(k, c)| (k.to_string(), c.to_string()))
        .collect();
    assert_eq!(
        words, want,
        "README defaults written in words, with their cells"
    );
    assert!(
        wrong.is_empty(),
        "chart README defaults that are not values.yaml's: {wrong:?}"
    );
    // Every values.yaml leaf lies under a named key (`a.*` names a's subtree).
    let mut leaves = Vec::new();
    fn walk(prefix: &str, v: &serde_norway::Value, out: &mut Vec<String>) {
        match v.as_mapping() {
            Some(m) if !m.is_empty() => {
                for (k, child) in m {
                    let k = k.as_str().unwrap();
                    let p = if prefix.is_empty() {
                        k.to_string()
                    } else {
                        format!("{prefix}.{k}")
                    };
                    walk(&p, child, out);
                }
            }
            _ => out.push(prefix.to_string()),
        }
    }
    walk("", &values, &mut leaves);
    let roots: Vec<&str> = named.iter().map(|k| k.trim_end_matches(".*")).collect();
    let unnamed: Vec<&String> = leaves
        .iter()
        .filter(|l| {
            !roots
                .iter()
                .any(|r| *l == r || l.starts_with(&format!("{r}.")))
        })
        .collect();
    assert!(
        unnamed.is_empty(),
        "values.yaml keys the README table does not name: {unnamed:?}"
    );
}

/// Every `--mode` the chart passes to the binary is a mode the binary
/// accepts. Read from both goldens (all-mode) and the chart unit specs,
/// which pin the split-mode arguments the render produces.
#[test]
fn every_mode_the_chart_passes_is_one_the_binary_accepts() {
    let mut modes = Vec::new();
    for golden in ["single", "cluster"] {
        let text = read(&format!(
            "deploy/charts/pulsusdb/tests/golden/{golden}.yaml"
        ));
        let lines: Vec<&str> = text.lines().map(str::trim).collect();
        for pair in lines.windows(2) {
            if pair[0] == "- --mode" {
                let m = pair[1].strip_prefix("- ").expect("a mode follows --mode");
                modes.push((format!("{golden}.yaml"), m.to_string()));
            }
        }
    }
    let unit_dir = repo_root().join("deploy/charts/pulsusdb/tests/unit");
    for entry in std::fs::read_dir(&unit_dir).expect("chart unit specs") {
        let path = entry.expect("dir entry").path();
        if !path.to_string_lossy().ends_with("_test.yaml") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("read spec");
        let file = path.file_name().unwrap().to_string_lossy().into_owned();
        for (_, rest) in text.split("\"--mode\", \"").enumerate().skip(1) {
            let m = rest.split('"').next().unwrap_or_default();
            modes.push((file.clone(), m.to_string()));
        }
    }
    for (file, m) in &modes {
        m.parse::<pulsus_config::Mode>()
            .unwrap_or_else(|e| panic!("{file} passes --mode {m}, which the binary rejects: {e}"));
    }
    let seen: BTreeSet<&str> = modes.iter().map(|(_, m)| m.as_str()).collect();
    assert_eq!(
        seen,
        BTreeSet::from(["all", "reader", "writer"]),
        "the chart's modes, as found: {modes:?}"
    );
}

// ---------------------------------------------------------------------------
// The chart schema against the binary's parser (issue #506).

/// The JSON-schema keywords `pulsusdbConfig` uses, evaluated on one value.
/// Any other keyword fails the test, so a schema that grows one is not
/// silently half-read.
fn schema_accepts(
    root: &serde_norway::Value,
    node: &serde_norway::Value,
    v: &serde_norway::Value,
) -> bool {
    use serde_norway::Value as V;
    let map = node.as_mapping().expect("a schema node is a mapping");
    for (k, rule) in map {
        let ok = match k.as_str().expect("keyword") {
            "description" | "$comment" => true,
            "$ref" => {
                let name = rule
                    .as_str()
                    .and_then(|r| r.strip_prefix("#/definitions/"))
                    .expect("a local definition");
                schema_accepts(root, get(root, &["definitions", name]), v)
            }
            "type" => {
                let types: Vec<&str> = match rule {
                    V::String(t) => vec![t.as_str()],
                    V::Sequence(ts) => ts.iter().filter_map(|t| t.as_str()).collect(),
                    _ => panic!("type is a string or a list"),
                };
                types.iter().any(|t| match *t {
                    "null" => v.is_null(),
                    "boolean" => v.is_bool(),
                    "string" => v.is_string(),
                    "integer" => v.is_i64() || v.is_u64(),
                    "number" => v.is_number(),
                    "array" => v.is_sequence(),
                    "object" => v.is_mapping(),
                    other => panic!("type {other}"),
                })
            }
            "enum" => rule.as_sequence().expect("enum list").contains(v),
            "const" => rule == v,
            "minItems" => v
                .as_sequence()
                .is_none_or(|xs| xs.len() as u64 >= rule.as_u64().unwrap()),
            "if" => {
                !schema_accepts(root, rule, v)
                    || node.get("then").is_none_or(|t| schema_accepts(root, t, v))
            }
            "then" => true,
            "minimum" => v.as_f64().is_none_or(|x| x >= rule.as_f64().unwrap()),
            "maximum" => v.as_f64().is_none_or(|x| x <= rule.as_f64().unwrap()),
            "minLength" => v
                .as_str()
                .is_none_or(|s| s.chars().count() as u64 >= rule.as_u64().unwrap()),
            "pattern" => v.as_str().is_none_or(|s| {
                thread_local! {
                    static COMPILED: std::cell::RefCell<std::collections::HashMap<String, regex::Regex>> =
                        Default::default();
                }
                let text = rule.as_str().unwrap();
                COMPILED.with(|c| {
                    c.borrow_mut()
                        .entry(text.to_string())
                        .or_insert_with(|| regex::Regex::new(text).expect("pattern"))
                        .is_match(s)
                })
            }),
            "items" => v
                .as_sequence()
                .is_none_or(|xs| xs.iter().all(|x| schema_accepts(root, rule, x))),
            "required" => v.as_mapping().is_none_or(|m| {
                rule.as_sequence()
                    .unwrap()
                    .iter()
                    .all(|r| m.contains_key(r))
            }),
            "properties" => v.as_mapping().is_none_or(|m| {
                m.iter().all(|(key, x)| match rule.get(key) {
                    Some(p) => schema_accepts(root, p, x),
                    None => true,
                })
            }),
            "additionalProperties" => {
                assert_eq!(rule.as_bool(), Some(false), "only a closed object is read");
                v.as_mapping().is_none_or(|m| {
                    let props = node.get("properties").and_then(|p| p.as_mapping());
                    m.keys()
                        .all(|key| props.is_some_and(|p| p.contains_key(key)))
                })
            }
            other => panic!("schema keyword {other} is not evaluated by this test"),
        };
        if !ok {
            return false;
        }
    }
    true
}

/// Every leaf property of `pulsusdbConfig` with its dotted path. A list's
/// element properties are reached as `<list>[].<field>`.
fn schema_leaves<'a>(
    node: &'a serde_norway::Value,
    prefix: &str,
    out: &mut Vec<(String, &'a serde_norway::Value)>,
) {
    let props = node.get("properties").and_then(|p| p.as_mapping());
    for (k, child) in props.into_iter().flatten() {
        let k = k.as_str().unwrap();
        let path = if prefix.is_empty() {
            k.to_string()
        } else {
            format!("{prefix}.{k}")
        };
        if child.get("properties").is_some() {
            schema_leaves(child, &path, out);
        } else {
            out.push((path.clone(), child));
            if let Some(items) = child.get("items") {
                schema_leaves(items, &format!("{path}[]"), out);
            }
        }
    }
}

/// A valid tier, so an element field can be probed inside a valid list.
fn base_tier() -> serde_norway::Mapping {
    serde_norway::from_str(
        "{name: r5m, resolution: 5m, table: metric_samples_5m, retention: 30d, min_step: 5m}",
    )
    .unwrap()
}

/// The values every property is probed with: each JSON type, and the string
/// forms a duration, a byte size or a name can take.
fn probes() -> Vec<serde_norway::Value> {
    let text = r#"[null, true, false, 0, 1, -1, 1.5, 65535, 65536, "", "x", "0", "5", "-5",
        "0s", "5s", "5m", "1h", "7d", "2w", "500ms", "5x", "5 s", " 5s", "1.5s", "s",
        "99999999999999999999s", "0B", "16MiB", "16MB", "16mib", "1KiB", "16 MiB",
        "http", "https", "native", "sync", "async", "exact", "fast", "info", "INFO",
        "9999999999999w", "99999TiB", [], {}, ["a"], {a: 1}]"#;
    serde_norway::from_str(text).unwrap()
}

/// `doc` with the value at dotted `path` replaced; `<list>[].<field>` sets
/// that one field in a one-element list holding `base_tier()`.
fn with_value(
    doc: &serde_norway::Value,
    path: &str,
    v: &serde_norway::Value,
) -> serde_norway::Value {
    let (path, v) = match path.split_once("[].") {
        Some((list, field)) => {
            let mut tier = base_tier();
            tier.insert(field.into(), v.clone());
            (
                list.to_string(),
                serde_norway::Value::Sequence(vec![tier.into()]),
            )
        }
        None => (path.to_string(), v.clone()),
    };
    let mut doc = doc.clone();
    let steps: Vec<&str> = path.split('.').collect();
    let mut cur = &mut doc;
    for step in &steps[..steps.len() - 1] {
        cur = cur
            .as_mapping_mut()
            .expect("a mapping")
            .entry((*step).into())
            .or_insert_with(|| serde_norway::Mapping::new().into());
    }
    cur.as_mapping_mut()
        .expect("a mapping")
        .insert((*steps.last().unwrap()).into(), v);
    doc
}

/// The server's verdict on one changed key: its defaults with that key set,
/// parsed and validated, with the server's message when it refuses.
fn server_verdict(path: &str, v: &serde_norway::Value) -> Result<(), String> {
    let doc = with_value(&serde_norway::Value::Mapping(Default::default()), path, v);
    let cfg = serde_norway::from_value::<Config>(doc).map_err(|e| e.to_string())?;
    pulsus_config::validate(&cfg).map_err(|e| e.to_string())
}

fn server_accepts(path: &str, v: &serde_norway::Value) -> bool {
    server_verdict(path, v).is_ok()
}

/// The one server rule the schema is not held to: a tier's `min_step` must
/// be at least its `resolution`. It relates two fields, and so do the rules
/// across tiers (durations strictly increasing, names and tables unique),
/// which a one-tier probe never reaches. The schema does not encode them:
/// the server refuses them at startup with a message naming the fields
/// (`a_tier_relation_the_schema_does_not_check_is_refused_at_startup`), and
/// the `downsampling` section is due for removal. A refusal carrying this
/// text, at a tier field, is not counted by the sweep.
const TIER_RELATION_REFUSAL: &str = "must be >= resolution";

/// The values the server accepts and the schema refuses by decision, with
/// the probe that finds each. Asserted by equality.
/// * `clickhouse.server` with surrounding whitespace: the template renders
///   the value as given, so the schema refuses it rather than trim it.
/// * `clickhouse.database` empty: no database has an empty name.
/// * `05s` and `016MiB`, leading zeros: stricter by decision. The positive
///   duration and byte-size patterns refuse them; `humanDuration` accepts
///   them, so its keys are not listed. A tier's `min_step` is probed with
///   `05m` (`TIER_MIN_STEP_LEADING_ZERO`), because `05s` is below the probe
///   tier's 5m `resolution` and the server refuses it for that.
const SCHEMA_STRICTER: &[(&str, &str)] = &[
    ("query_timeout", "05s"),
    ("log_rollup_resolution", "05s"),
    ("clickhouse.server", " 5s"),
    ("clickhouse.database", ""),
    ("writer.batch_bytes", "016MiB"),
    ("writer.ingest_queue_bytes", "016MiB"),
    ("reader.logql_scan_budget_bytes", "016MiB"),
    ("reader.traceql_tag_lookback", "05s"),
    ("downsampling.raw_retention", "05s"),
    ("downsampling.tiers[].resolution", "05s"),
    ("downsampling.tiers[].retention", "05s"),
    ("downsampling.tiers[].min_step", "05m"),
    ("ruler.poll_interval", "05s"),
    ("ruler.max_result_bytes", "016MiB"),
];

/// A duration and a byte size with a leading zero, probed at every key in
/// the reverse direction.
const LEADING_ZERO_PROBES: &[&str] = &["05s", "016MiB"];

/// A leading-zero `min_step` that fits the probe tier: `05m` equals its
/// `resolution`, so only the leading zero is in question.
const TIER_MIN_STEP_LEADING_ZERO: (&str, &str) = ("downsampling.tiers[].min_step", "05m");

/// The definitions whose values the server trims before parsing: durations
/// and byte sizes (`units.rs`). Their patterns accept surrounding ASCII
/// whitespace, and the sweep probes them padded.
const TRIMMED_DEFINITIONS: &[&str] = &[
    "humanDuration",
    "positiveHumanDuration",
    "nullablePositiveHumanDuration",
    "byteSize",
    "batchBytes",
    "ingestQueueBytes",
    "logqlScanBudgetBytes",
];

/// The surrounding whitespace a duration or byte-size pattern accepts: the
/// ASCII characters `str::trim` removes. The server also trims the other
/// Unicode white space; the schema refuses it, which is the stricter side.
const PAD: &str = r"[\t\n\v\f\r ]*";

fn int(x: i128) -> serde_norway::Value {
    if x < 0 {
        (x as i64).into()
    } else {
        (x as u64).into()
    }
}

/// The smallest and largest integers the server accepts at `path`, by
/// bisection from one it accepts towards each end of the integer range.
fn integer_bounds(path: &str, seed: i128) -> (i128, i128) {
    let ok = |x: i128| server_accepts(path, &int(x));
    let search = |mut good: i128, mut bad: i128| {
        if ok(bad) {
            return bad;
        }
        while (good - bad).abs() > 1 {
            let mid = good + (bad - good) / 2;
            if ok(mid) { good = mid } else { bad = mid }
        }
        good
    };
    (
        search(seed, i64::MIN as i128),
        search(seed, u64::MAX as i128),
    )
}

/// The units a duration or a byte size can carry; `""` is a bare count.
const UNITS: &[&str] = &[
    "ms", "s", "m", "h", "d", "w", "", "B", "KB", "MB", "GB", "TB", "KiB", "MiB", "GiB", "TiB",
];

/// The largest `n` for which the server accepts the string `"<n><unit>"` at
/// `path`, by bisection over `1..=u64::MAX`; `None` when it refuses `1<unit>`.
fn largest_count(path: &str, unit: &str) -> Option<u128> {
    let ok = |n: u128| server_accepts(path, &serde_norway::Value::String(format!("{n}{unit}")));
    if !ok(1) {
        return None;
    }
    let (mut good, mut bad) = (1u128, u64::MAX as u128 + 1);
    if ok(bad) {
        return Some(bad);
    }
    while bad - good > 1 {
        let mid = good + (bad - good) / 2;
        if ok(mid) { good = mid } else { bad = mid }
    }
    Some(good)
}

/// The schema node at dotted `path` under `pulsusdbConfig` (`<list>[].<f>`
/// reaches a list entry's field).
fn leaf_node<'a>(config_schema: &'a serde_norway::Value, path: &str) -> &'a serde_norway::Value {
    let mut node = config_schema;
    for step in path.split('.') {
        let (name, item) = match step.strip_suffix("[]") {
            Some(n) => (n, true),
            None => (step, false),
        };
        node = get(node, &["properties", name]);
        if item {
            node = get(node, &["items"]);
        }
    }
    node
}

/// A node with its local `$ref` followed.
fn resolve<'a>(
    root: &'a serde_norway::Value,
    node: &'a serde_norway::Value,
) -> &'a serde_norway::Value {
    match node.get("$ref").and_then(|r| r.as_str()) {
        Some(r) => get(
            root,
            &[
                "definitions",
                r.strip_prefix("#/definitions/").expect("local ref"),
            ],
        ),
        None => node,
    }
}

/// No value the chart schema accepts at a configuration key is one the
/// server refuses. Each key is probed with every JSON type and the string
/// forms a duration, a byte size or a name can take, and every key that
/// takes integers with the server's own smallest and largest accepted
/// integer and the ones beyond them. The schema is evaluated over
/// `values.yaml` with that one key changed, which is what the chart checks.
#[test]
fn the_chart_schema_accepts_no_value_the_binary_refuses() {
    let schema = parse_doc(
        &read("deploy/charts/pulsusdb/values.schema.json"),
        "values.schema.json",
    );
    let config_schema = get(&schema, &["definitions", "pulsusdbConfig"]);
    let values = chart_config();
    let mut leaves = Vec::new();
    schema_leaves(config_schema, "", &mut leaves);
    // Every `enum` and `const` member the configuration schema holds, so a
    // spelling added to an enumeration is probed at every key.
    fn members(
        root: &serde_norway::Value,
        node: &serde_norway::Value,
        out: &mut Vec<serde_norway::Value>,
    ) {
        let node = resolve(root, node);
        if let Some(m) = node.as_mapping() {
            for (k, child) in m {
                match k.as_str() {
                    Some("enum") => out.extend(child.as_sequence().into_iter().flatten().cloned()),
                    Some("const") => out.push(child.clone()),
                    _ => members(root, child, out),
                }
            }
        } else if let Some(xs) = node.as_sequence() {
            for x in xs {
                members(root, x, out);
            }
        }
    }
    let mut enumerated = Vec::new();
    members(&schema, config_schema, &mut enumerated);
    assert!(
        enumerated.len() >= 10,
        "the member finder found only {enumerated:?}"
    );
    let mut refused = Vec::new();
    let mut rejected = Vec::new();
    let mut padded_keys = 0;
    for (path, _) in &leaves {
        let mut candidates = probes();
        candidates.extend(enumerated.iter().cloned());
        let seed = [1i128, 0, 1000, 65536].into_iter().find(|x| {
            schema_accepts(&schema, config_schema, &with_value(&values, path, &int(*x)))
                && server_accepts(path, &int(*x))
        });
        if let Some(seed) = seed {
            let (lo, hi) = integer_bounds(path, seed);
            for x in [lo - 1, lo, hi, hi + 1] {
                if x >= i64::MIN as i128 && x <= u64::MAX as i128 {
                    candidates.push(int(x));
                }
            }
        }
        // The schema's own integer bounds, and the values one beyond them.
        let node = resolve(&schema, leaf_node(config_schema, path));
        for key in ["minimum", "maximum"] {
            if let Some(m) = node
                .get(key)
                .and_then(|m| m.as_i64().map(i128::from).or(m.as_u64().map(i128::from)))
            {
                for x in [m - 1, m, m + 1] {
                    if x >= i64::MIN as i128 && x <= u64::MAX as i128 {
                        candidates.push(int(x));
                    }
                }
            }
        }
        // Every unit a duration or a byte size takes: the largest count the
        // server accepts with it, and the one beyond.
        let unit_probes_from = candidates.len();
        for unit in UNITS {
            if let Some(n) = largest_count(path, unit) {
                for x in [n, n + 1] {
                    candidates.push(serde_norway::Value::String(format!("{x}{unit}")));
                }
            }
        }
        // The reverse direction: every probe above that is not one of the
        // per-unit largest counts (durations allow 13 digits and `byteSize`
        // 2^53 - 1, deliberately stricter than the server), and at a key
        // whose value the server trims, each string probe and the key's own
        // value padded with whitespace.
        let mut reverse = candidates[..unit_probes_from].to_vec();
        let trimmed = leaf_node(config_schema, path)
            .get("$ref")
            .and_then(|r| r.as_str())
            .and_then(|r| r.strip_prefix("#/definitions/"))
            .is_some_and(|d| TRIMMED_DEFINITIONS.contains(&d));
        if trimmed {
            let own = match path.split_once("[].") {
                Some((_, field)) => base_tier().get(field).cloned(),
                None => path.split('.').try_fold(&values, |v, s| v.get(s)).cloned(),
            };
            let strings: Vec<String> = reverse
                .iter()
                .chain(own.iter())
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect();
            for s in strings {
                for padded in [format!(" {s}"), format!("{s} "), format!("\t{s}\n")] {
                    reverse.push(serde_norway::Value::String(padded));
                }
            }
            padded_keys += 1;
        }
        // Leading zeros, which the server accepts and the schema refuses by
        // decision (`SCHEMA_STRICTER`). Not padded.
        for probe in LEADING_ZERO_PROBES {
            reverse.push(serde_norway::Value::String(probe.to_string()));
        }
        if path == TIER_MIN_STEP_LEADING_ZERO.0 {
            reverse.push(serde_norway::Value::String(
                TIER_MIN_STEP_LEADING_ZERO.1.to_string(),
            ));
        }
        for v in candidates {
            let verdict = server_verdict(path, &v);
            let relation = path.contains("[].")
                && verdict
                    .as_ref()
                    .is_err_and(|e| e.contains(TIER_RELATION_REFUSAL));
            if schema_accepts(&schema, config_schema, &with_value(&values, path, &v))
                && verdict.is_err()
                && !relation
            {
                refused.push(format!(
                    "{path} = {}",
                    serde_norway::to_string(&v).unwrap().trim()
                ));
            }
        }
        for v in reverse {
            // `null` never reaches the schema: the chart tool deletes a key
            // set to `null` before it validates. A byte size above 2^53 - 1
            // is refused by decision (`byteSize`).
            let over_safe = serde_norway::from_value::<pulsus_config::ByteSize>(v.clone())
                .is_ok_and(|b| b.0 > JSON_SAFE_MAX);
            if !v.is_null()
                && !over_safe
                && server_accepts(path, &v)
                && !schema_accepts(&schema, config_schema, &with_value(&values, path, &v))
            {
                rejected.push((path.clone(), v));
            }
        }
    }
    assert!(
        leaves.len() >= 40,
        "the leaf finder found only {}",
        leaves.len()
    );
    assert!(
        schema_accepts(&schema, config_schema, &values),
        "values.yaml's own configuration must pass the schema"
    );
    assert!(
        padded_keys >= 15,
        "the padded probes reached only {padded_keys} keys"
    );
    assert!(
        refused.is_empty(),
        "values the schema accepts and the server refuses: {refused:#?}"
    );
    let want: Vec<(String, serde_norway::Value)> = SCHEMA_STRICTER
        .iter()
        .map(|(p, v)| (p.to_string(), serde_norway::Value::String(v.to_string())))
        .collect();
    assert_eq!(
        rejected, want,
        "values the server accepts and the schema refuses, beyond the listed ones"
    );
}

/// The limit `TIER_RELATION_REFUSAL` states, held true: a tier whose
/// `min_step` is below its `resolution`, with downsampling enabled, passes
/// the schema, and the server refuses it at startup with a message naming
/// both fields.
#[test]
fn a_tier_relation_the_schema_does_not_check_is_refused_at_startup() {
    let schema = parse_doc(
        &read("deploy/charts/pulsusdb/values.schema.json"),
        "values.schema.json",
    );
    let config_schema = get(&schema, &["definitions", "pulsusdbConfig"]);
    let tiers: serde_norway::Value = serde_norway::from_str(
        "[{name: r10m, resolution: 10m, table: metric_samples_10m, retention: 30d, min_step: 5m}]",
    )
    .unwrap();
    let values = with_value(
        &with_value(&chart_config(), "downsampling.enabled", &true.into()),
        "downsampling.tiers",
        &tiers,
    );
    assert!(
        schema_accepts(&schema, config_schema, &values),
        "the schema refuses the tier, so the limit no longer holds"
    );
    let cfg: Config = serde_norway::from_value(values).expect("the tier parses");
    let err = pulsus_config::validate(&cfg)
        .expect_err("the server must refuse min_step below resolution")
        .to_string();
    assert!(
        err.contains("min_step")
            && err.contains("resolution")
            && err.contains(TIER_RELATION_REFUSAL),
        "the refusal must name both fields: {err}"
    );
}

/// The chart's tier entry names exactly the binary's tier fields, and
/// requires exactly the ones the binary requires: a field is required when
/// a tier without it does not parse.
#[test]
fn the_chart_tier_entry_matches_the_binary_tier() {
    let schema = parse_doc(
        &read("deploy/charts/pulsusdb/values.schema.json"),
        "values.schema.json",
    );
    let items = get(
        &schema,
        &[
            "definitions",
            "pulsusdbConfig",
            "properties",
            "downsampling",
            "properties",
            "tiers",
            "items",
        ],
    );
    let in_schema: BTreeSet<String> = items
        .get("properties")
        .and_then(|p| p.as_mapping())
        .expect("tier properties")
        .keys()
        .map(|k| k.as_str().unwrap().to_string())
        .collect();
    let required_in_schema: BTreeSet<String> = items
        .get("required")
        .and_then(|r| r.as_sequence())
        .expect("tier required")
        .iter()
        .map(|k| k.as_str().unwrap().to_string())
        .collect();
    let tier = base_tier();
    let in_binary: BTreeSet<String> = support::key_paths(
        &serde_norway::to_value(
            serde_norway::from_value::<pulsus_config::Tier>(tier.clone().into()).unwrap(),
        )
        .unwrap(),
    );
    let required_in_binary: BTreeSet<String> = in_binary
        .iter()
        .filter(|k| {
            let mut t = tier.clone();
            t.remove(k.as_str());
            serde_norway::from_value::<pulsus_config::Tier>(t.into()).is_err()
        })
        .cloned()
        .collect();
    assert_eq!(in_schema, in_binary, "the tier entry's fields");
    assert_eq!(
        required_in_schema, required_in_binary,
        "the tier entry's required fields"
    );
}

/// The keys the ConfigMap template rewrites on the way from `values.yaml`
/// to the rendered `config.yaml`, per golden, with the rendered value.
/// `auth_user`, added from `pulsusdb.auth.user`, is not set in either golden
/// and is pinned by the chart unit specs.
const RENDER_REWRITES: &[(&str, &[(&str, &str)])] = &[
    ("single", &[("clickhouse.server", "pulsusdb-clickhouse")]),
    (
        "cluster",
        &[
            ("clickhouse.server", "pulsusdb-clickhouse-shard-0"),
            ("cluster", "pulsus_cluster"),
        ],
    ),
];

fn leaf_map(v: &serde_norway::Value) -> std::collections::BTreeMap<String, serde_norway::Value> {
    fn walk(
        prefix: &str,
        v: &serde_norway::Value,
        out: &mut std::collections::BTreeMap<String, serde_norway::Value>,
    ) {
        match v.as_mapping() {
            Some(m) if !m.is_empty() => {
                for (k, child) in m {
                    let k = k.as_str().unwrap();
                    let p = if prefix.is_empty() {
                        k.to_string()
                    } else {
                        format!("{prefix}.{k}")
                    };
                    walk(&p, child, out);
                }
            }
            _ => {
                out.insert(prefix.to_string(), v.clone());
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    walk("", v, &mut out);
    out
}

/// The `config.yaml` each golden renders is one the server starts with, and
/// it differs from `values.yaml`'s `pulsusdb.config` in exactly the listed
/// keys, to exactly the listed values.
#[test]
fn the_rendered_config_is_values_yaml_with_the_listed_rewrites() {
    let values = leaf_map(&chart_config());
    for (golden, rewrites) in RENDER_REWRITES {
        let text = read(&format!(
            "deploy/charts/pulsusdb/tests/golden/{golden}.yaml"
        ));
        let config_text = serde_norway::Deserializer::from_str(&text)
            .map(|d| {
                <serde_norway::Value as serde::Deserialize>::deserialize(d)
                    .expect("a YAML document")
            })
            .find(|d| {
                d.get("kind").and_then(|k| k.as_str()) == Some("ConfigMap")
                    && d.get("metadata")
                        .and_then(|m| m.get("name"))
                        .and_then(|n| n.as_str())
                        == Some("pulsusdb-config")
            })
            .and_then(|d| {
                d.get("data")
                    .and_then(|x| x.get("config.yaml"))
                    .and_then(|c| c.as_str())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| panic!("{golden}.yaml renders no pulsusdb-config config.yaml"));
        let cfg: Config = serde_norway::from_str(&config_text)
            .unwrap_or_else(|e| panic!("{golden}.yaml's config.yaml: the server refuses it: {e}"));
        pulsus_config::validate(&cfg)
            .unwrap_or_else(|e| panic!("{golden}.yaml's config.yaml fails validation: {e}"));
        let rendered = leaf_map(&serde_norway::from_str(&config_text).unwrap());
        let keys: BTreeSet<&String> = values.keys().chain(rendered.keys()).collect();
        let differs: Vec<(String, Option<serde_norway::Value>)> = keys
            .into_iter()
            .filter(|k| values.get(*k) != rendered.get(*k))
            .map(|k| (k.clone(), rendered.get(k).cloned()))
            .collect();
        let want: Vec<(String, Option<serde_norway::Value>)> = rewrites
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    Some(serde_norway::Value::String(v.to_string())),
                )
            })
            .collect();
        assert_eq!(
            differs, want,
            "{golden}.yaml: rendered keys that differ from values.yaml"
        );
    }
}

/// The chart's prose: `values.yaml`'s comments (the text after each `#`),
/// the chart README, `templates/NOTES.txt`, and every template's comments
/// (`{{/* … */}}` blocks and `#` lines). Every file in `templates/` is read,
/// so a template added later is swept too.
fn chart_prose() -> Vec<(String, String)> {
    let mut out = vec![
        (
            "values.yaml comments".to_string(),
            read("deploy/charts/pulsusdb/values.yaml")
                .lines()
                .filter_map(|l| l.split_once('#').map(|(_, c)| c))
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        (
            "README.md".to_string(),
            read("deploy/charts/pulsusdb/README.md"),
        ),
    ];
    let dir = repo_root().join("deploy/charts/pulsusdb/templates");
    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    entries.sort();
    for path in entries.into_iter().filter(|p| p.is_file()) {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let text = std::fs::read_to_string(&path).unwrap();
        let prose = if name == "NOTES.txt" {
            text
        } else {
            let mut prose = String::new();
            for block in text.split("{{/*").skip(1) {
                prose.push_str(block.split("*/}}").next().unwrap_or_default());
                prose.push('\n');
            }
            for line in text.lines() {
                if let Some((_, c)) = line.split_once("# ")
                    && line.trim_start().starts_with('#')
                {
                    prose.push_str(c);
                    prose.push('\n');
                }
            }
            prose
        };
        out.push((format!("templates/{name}"), prose));
    }
    out
}

/// Every backticked name in the chart's prose resolves (issue #506):
///
/// * a mode — `--mode <word>` — is one the binary accepts;
/// * a file — a token ending in a file extension (no space, not starting
///   `/`, not a bare `.ext`), or any token starting with a repository or
///   chart directory (`tests/`, `templates/`, `docs/`, …) — exists under the
///   repository root, the chart or `templates/`, is the end of a tracked
///   path (`users.d/network.xml`), or is a data key a template renders
///   (`interserver-credentials.xml: |`); a `dir/*…` glob needs its
///   directory;
/// * a value path — a token whose first segment is a top-level `values.yaml`
///   key and which has a dot, cut at `=`, `[`, a space or `:`, with a
///   trailing `.*` dropped — exists in `values.yaml` or under
///   `pulsusdb.config` (a key of the rendered `config.yaml`);
/// * otherwise such a token is a template name, a `define` in `templates/`.
///
/// A token a comment wraps across lines is read joined. A name added to any
/// of these files later is checked when it is added.
#[test]
fn every_name_the_chart_prose_uses_resolves() {
    let values = parse_doc(&read("deploy/charts/pulsusdb/values.yaml"), "values.yaml");
    let config = get(&values, &["pulsusdb", "config"]).clone();
    let top: BTreeSet<String> = values
        .as_mapping()
        .unwrap()
        .keys()
        .filter_map(|k| k.as_str().map(str::to_string))
        .collect();
    let mut defines = BTreeSet::new();
    for entry in std::fs::read_dir(repo_root().join("deploy/charts/pulsusdb/templates")).unwrap() {
        let text = std::fs::read_to_string(entry.unwrap().path()).unwrap_or_default();
        for rest in text.split("define \"").skip(1) {
            defines.insert(rest.split('"').next().unwrap_or_default().to_string());
        }
    }
    let bases = [
        repo_root(),
        repo_root().join("deploy/charts/pulsusdb"),
        repo_root().join("deploy/charts/pulsusdb/templates"),
    ];
    let listing = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_root())
        .args(["ls-files", "-z"])
        .output()
        .expect("git ls-files must run");
    let tracked: Vec<String> = String::from_utf8(listing.stdout)
        .unwrap()
        .split('\0')
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .collect();
    let rendered_keys: String =
        std::fs::read_dir(repo_root().join("deploy/charts/pulsusdb/templates"))
            .unwrap()
            .map(|e| std::fs::read_to_string(e.unwrap().path()).unwrap_or_default())
            .collect();
    let file_exists = |token: &str| match token.split_once("/*") {
        Some((dir, _)) => bases.iter().any(|b| b.join(dir).is_dir()),
        None => {
            bases.iter().any(|b| b.join(token).exists())
                || tracked.iter().any(|t| t.ends_with(&format!("/{token}")))
                || rendered_keys.contains(&format!("  {token}: |"))
        }
    };
    let roots = [
        "tests/",
        "templates/",
        "deploy/",
        "docs/",
        "crates/",
        "ci/",
        ".github/",
        "schema/",
        "packaging/",
    ];
    let extensions = [
        ".yaml", ".yml", ".tpl", ".md", ".rs", ".sh", ".sql", ".xml", ".txt", ".json", ".py",
        ".feature",
    ];
    let mut counts = [0usize; 4];
    let mut unresolved = Vec::new();
    for (what, text) in chart_prose() {
        for raw in text.split('`').skip(1).step_by(2) {
            let token: String = raw.split('\n').map(str::trim_start).collect();
            if let Some(mode) = token.strip_prefix("--mode ") {
                counts[0] += 1;
                if mode.parse::<pulsus_config::Mode>().is_err() {
                    unresolved.push(format!("{what}: mode `{token}`"));
                }
                continue;
            }
            let named_file = !token.starts_with('/')
                && !token.contains(' ')
                && (token.contains('/') || !token.starts_with('.'))
                && extensions.iter().any(|e| token.ends_with(e));
            let named_dir = !token.contains(' ') && roots.iter().any(|r| token.starts_with(r));
            if named_file || named_dir {
                counts[1] += 1;
                if !file_exists(&token) {
                    unresolved.push(format!("{what}: file `{token}`"));
                }
                continue;
            }
            let path = token.split(['=', '[', ' ', ':']).next().unwrap_or_default();
            let path = path.trim_end_matches(".*");
            let Some((first, _)) = path.split_once('.') else {
                continue;
            };
            if !top.contains(first) {
                continue;
            }
            let resolves = |root: &serde_norway::Value| {
                path.split('.').try_fold(root, |v, s| v.get(s)).is_some()
            };
            if resolves(&values) || resolves(&config) {
                counts[2] += 1;
            } else if defines.contains(path) {
                counts[3] += 1;
            } else {
                unresolved.push(format!("{what}: `{token}`"));
            }
        }
    }
    assert!(
        counts.iter().all(|c| *c > 0),
        "a finder found nothing: {counts:?}"
    );
    assert!(
        unresolved.is_empty(),
        "names the chart prose uses that resolve to nothing: {unresolved:#?}"
    );
}

/// The maximum safe integer for readers based on binary64 numbers (2^53 - 1). No
/// schema `maximum` may exceed it, and the byte size with no binary ceiling
/// is bounded by it.
const JSON_SAFE_MAX: u64 = 9_007_199_254_740_991;

/// A regex (no anchors, no leading zeros) for the decimal integers `1..=hi`.
fn count_regex(hi: u64) -> String {
    // Equal-length digit strings a <= b (leading zeros allowed).
    fn same_len(a: &str, b: &str) -> Vec<String> {
        if a == b {
            return vec![a.to_string()];
        }
        let i = a.bytes().zip(b.bytes()).take_while(|(x, y)| x == y).count();
        let (p, ra, rb) = (&a[..i], &a[i..], &b[i..]);
        let n = ra.len();
        if n == 1 {
            return vec![format!("{p}[{ra}-{rb}]")];
        }
        if ra.bytes().all(|c| c == b'0') && rb.bytes().all(|c| c == b'9') {
            return vec![format!("{p}[0-9]{{{n}}}")];
        }
        let tail = n - 1;
        let any = |t: usize| {
            if t == 1 {
                "[0-9]".to_string()
            } else {
                format!("[0-9]{{{t}}}")
            }
        };
        let mut out = Vec::new();
        let (fa, fb) = (ra.as_bytes()[0] - b'0', rb.as_bytes()[0] - b'0');
        let lo_first = if ra[1..].bytes().all(|c| c == b'0') {
            fa
        } else {
            for x in same_len(&ra[1..], &"9".repeat(tail)) {
                out.push(format!("{p}{}{x}", &ra[..1]));
            }
            fa + 1
        };
        let hi_first = if rb[1..].bytes().all(|c| c == b'9') {
            fb
        } else {
            fb - 1
        };
        if lo_first <= hi_first {
            let class = if lo_first == hi_first {
                lo_first.to_string()
            } else {
                format!("[{lo_first}-{hi_first}]")
            };
            out.push(format!("{p}{class}{}", any(tail)));
        }
        if !rb[1..].bytes().all(|c| c == b'9') {
            for x in same_len(&"0".repeat(tail), &rb[1..]) {
                out.push(format!("{p}{}{x}", &rb[..1]));
            }
        }
        out
    }
    let mut parts = Vec::new();
    let mut lo = 1u64;
    let mut digits = 1u32;
    while lo <= hi {
        let top = hi.min(10u64.saturating_pow(digits).saturating_sub(1));
        let a = lo.to_string();
        let b = top.to_string();
        // A range of one digit count whose first digits differ splits as for
        // equal-length strings; `lo` has no leading zero by construction.
        parts.extend(same_len(&a, &b));
        lo = top + 1;
        digits += 1;
    }
    format!("(?:{})", parts.join("|"))
}

/// The pattern a positive byte size with `ceiling` must carry: the canonical
/// forms (no leading zero) of the values `1..=ceiling`, in every unit the
/// parser takes, plus the surrounding whitespace the parser trims. The
/// parser also accepts leading zeros; the pattern refuses them by decision
/// (`SCHEMA_STRICTER`).
fn byte_size_pattern(ceiling: u64) -> String {
    let units: [(&str, u64); 10] = [
        ("", 1),
        ("B", 1),
        ("KB", 1_000),
        ("MB", 1_000_000),
        ("GB", 1_000_000_000),
        ("TB", 1_000_000_000_000),
        ("KiB", 1 << 10),
        ("MiB", 1 << 20),
        ("GiB", 1 << 30),
        ("TiB", 1 << 40),
    ];
    let alts: Vec<String> = units
        .iter()
        .filter(|(_, m)| ceiling / m >= 1)
        .map(|(u, m)| format!("{}{u}", count_regex(ceiling / m)))
        .collect();
    format!("^{PAD}(?:{}){PAD}$", alts.join("|"))
}

/// Each byte-size definition is bounded by its key's ceiling in both forms:
/// `maximum` is the ceiling, and the pattern is generated from it. The
/// ceilings are the binary's own constants, so a ceiling that moves fails
/// here until the schema moves with it; the message carries the pattern.
#[test]
fn every_byte_size_definition_is_bounded_by_its_ceiling() {
    let schema = parse_doc(
        &read("deploy/charts/pulsusdb/values.schema.json"),
        "values.schema.json",
    );
    for (name, ceiling) in [
        ("batchBytes", pulsus_config::BATCH_BYTES_CEILING),
        (
            "ingestQueueBytes",
            pulsus_config::INGEST_QUEUE_BYTES_CEILING,
        ),
        (
            "logqlScanBudgetBytes",
            pulsus_config::LOGQL_SCAN_BUDGET_BYTES_CEILING,
        ),
        ("byteSize", JSON_SAFE_MAX),
    ] {
        let def = schema.get("definitions").and_then(|d| d.get(name));
        assert!(def.is_some(), "the schema has no {name} definition");
        let def = def.unwrap();
        assert_eq!(
            def.get("minimum").and_then(|m| m.as_u64()),
            Some(1),
            "{name} minimum"
        );
        assert_eq!(
            def.get("maximum").and_then(|m| m.as_u64()),
            Some(ceiling),
            "{name} maximum"
        );
        let want = byte_size_pattern(ceiling);
        assert_eq!(
            def.get("pattern").and_then(|p| p.as_str()),
            Some(want.as_str()),
            "{name}: the pattern must be generated from {ceiling}"
        );
    }
}

/// No `maximum` anywhere in the schema exceeds 2^53 - 1.
#[test]
fn every_schema_maximum_is_a_json_safe_integer() {
    fn walk(v: &serde_norway::Value, path: &str, out: &mut Vec<String>) {
        if let Some(m) = v.as_mapping() {
            for (k, child) in m {
                let k = k.as_str().unwrap_or("?");
                if k == "maximum" && child.as_u64().is_none_or(|x| x > JSON_SAFE_MAX) {
                    out.push(format!("{path}: {child:?}"));
                }
                walk(child, &format!("{path}/{k}"), out);
            }
        } else if let Some(xs) = v.as_sequence() {
            for (i, x) in xs.iter().enumerate() {
                walk(x, &format!("{path}/{i}"), out);
            }
        }
    }
    let schema = parse_doc(
        &read("deploy/charts/pulsusdb/values.schema.json"),
        "values.schema.json",
    );
    let mut over = Vec::new();
    walk(&schema, "", &mut over);
    assert!(over.is_empty(), "maximums above 2^53 - 1: {over:?}");
}

/// Every object of the configuration — the root, each section, the tier
/// entry, and any object reached through a local `$ref` — is closed (`additionalProperties: false`), and one extra member
/// in each is refused by the schema and by the server.
#[test]
fn every_config_object_in_the_schema_is_closed() {
    let schema = parse_doc(
        &read("deploy/charts/pulsusdb/values.schema.json"),
        "values.schema.json",
    );
    let config_schema = get(&schema, &["definitions", "pulsusdbConfig"]);
    let values = chart_config();
    // (path to the object, its schema node); a tier entry is `<list>[]`.
    fn objects<'a>(
        root: &'a serde_norway::Value,
        node: &'a serde_norway::Value,
        path: &str,
        out: &mut Vec<(String, &'a serde_norway::Value)>,
    ) {
        let node = resolve(root, node);
        if node.get("properties").is_some() {
            out.push((path.to_string(), node));
            for (k, child) in node.get("properties").unwrap().as_mapping().unwrap() {
                let k = k.as_str().unwrap();
                let p = if path.is_empty() {
                    k.to_string()
                } else {
                    format!("{path}.{k}")
                };
                objects(root, child, &p, out);
                if let Some(items) = child.get("items") {
                    objects(root, items, &format!("{p}[]"), out);
                }
            }
        }
    }
    let mut found = Vec::new();
    objects(&schema, config_schema, "", &mut found);
    let paths: Vec<&str> = found.iter().map(|(p, _)| p.as_str()).collect();
    assert_eq!(
        paths,
        [
            "",
            "clickhouse",
            "writer",
            "reader",
            "downsampling",
            "downsampling.tiers[]",
            "ruler"
        ],
        "the configuration's objects"
    );
    for (path, node) in &found {
        assert_eq!(
            node.get("additionalProperties").and_then(|a| a.as_bool()),
            Some(false),
            "{path:?} must be closed"
        );
        let member = if path.is_empty() {
            "zz_unknown".to_string()
        } else {
            format!("{path}.zz_unknown")
        };
        let v = serde_norway::Value::from(1u64);
        assert!(
            !schema_accepts(&schema, config_schema, &with_value(&values, &member, &v)),
            "the schema accepts an extra member in {path:?}"
        );
        assert!(
            !server_accepts(&member, &v),
            "the server accepts an extra member in {path:?}"
        );
    }
}

/// The README's "Rollouts on config/secret change": every Deployment and
/// StatefulSet pod template carries a `checksum/*` annotation, and the
/// schema Job's carries none. Read from both goldens, so a workload added
/// later is checked when the goldens are regenerated.
#[test]
fn every_rolled_workload_carries_a_checksum_and_the_job_none() {
    let mut seen = 0;
    for golden in ["single", "cluster"] {
        let text = read(&format!(
            "deploy/charts/pulsusdb/tests/golden/{golden}.yaml"
        ));
        for doc in serde_norway::Deserializer::from_str(&text) {
            let doc = <serde_norway::Value as serde::Deserialize>::deserialize(doc).expect("YAML");
            let kind = doc
                .get("kind")
                .and_then(|k| k.as_str())
                .unwrap_or_default()
                .to_string();
            if !["Deployment", "StatefulSet", "Job"].contains(&kind.as_str()) {
                continue;
            }
            seen += 1;
            let name = doc["metadata"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let checksums = doc["spec"]["template"]["metadata"]
                .get("annotations")
                .and_then(|a| a.as_mapping())
                .map(|a| {
                    a.keys()
                        .filter(|k| k.as_str().is_some_and(|k| k.starts_with("checksum/")))
                        .count()
                })
                .unwrap_or(0);
            if kind == "Job" {
                assert_eq!(
                    checksums, 0,
                    "{golden}: Job {name} carries a checksum annotation"
                );
            } else {
                assert!(
                    checksums > 0,
                    "{golden}: {kind} {name} carries no checksum annotation"
                );
            }
        }
    }
    assert!(seen >= 8, "found only {seen} workloads");
}
