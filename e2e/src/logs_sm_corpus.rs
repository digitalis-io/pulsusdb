//! Deterministic Loki-push structured-metadata (SM) corpus for the
//! `logs_structured_metadata_differential` scenario (issue #102), the SM
//! sibling of `logs_corpus.rs`. Unlike the M6-09 OTLP corpus — pushed
//! through the collector, which carries NO per-entry structured metadata —
//! this corpus is pushed as native Loki JSON `[ts, line, {sm}]` bodies
//! (the #97 wire shape) directly to BOTH stores' `/loki/api/v1/push`
//! endpoints, so the SM surfacing/collision behavior #97 shipped is
//! exercised against `grafana/loki:3.4.2` as identical wire bytes.
//!
//! Every value is a pure function of a fixed index, so the corpus — and
//! each case's expected result set — is byte-reproducible from an
//! [`SmCorpusSpec`] alone (no PRNG; unit-tested).
//!
//! **The circularity breaker.** [`merge_sm_into_labels`] is an INDEPENDENT
//! re-derivation of the oracle-pinned SM→labels merge (colliding SM key →
//! `<key>_extracted`, last-write-wins upsert; a double collision overwrites
//! the single `_extracted` slot rather than emitting `_extracted_extracted`)
//! — the `naive_matches` convention. It NEVER calls into `pulsus-read`'s own
//! merge; a hermetic test asserts it against the committed expected
//! projection so a projection typo fails hermetically, not at nightly
//! runtime. The behavior itself was pinned by a one-time live probe against
//! `grafana/loki:3.4.2` under `deploy/e2e/loki.yaml`
//! (`allow_structured_metadata: true`, `discover_log_levels: false`); the
//! nightly lane is the permanent pin.
//!
//! **Three SM behaviors, in dedicated records:** (a) non-colliding SM keys
//! (`trace_id`, `user_id`) fan verbatim into response stream labels; (b) a
//! single-collision SM key (`env`, equal to a base label) surfaces as
//! `env_extracted`, last-write-wins; (c) a double collision (base carries
//! both `env` AND `env_extracted`) overwrites the single `_extracted` slot —
//! no `env_extracted_extracted`, no surviving base `env_extracted` value.

use std::collections::{BTreeMap, BTreeSet};

use crate::logs_corpus::{ExpectedResult, RUN_ATTR};

/// Every committed SM differential case id, in fixture order —
/// `test/fixtures/logs/sm_differential.json` `cases[]` must match exactly
/// (hermetic id-lock in `logs.rs`). Own list, own fixture, own `run_id`:
/// the OTLP `CASE_IDS`/`differential.json` lock is left untouched.
pub const SM_CASE_IDS: &[&str] = &[
    // (a) SM fans verbatim into response labels — the full merged label set
    // is compared, so a silent SM drop or a spurious `_extracted` on either
    // store is caught (incl. the double-collision record's labels).
    "sm_surfacing",
    // (b) `| trace_id="keep"` selects on a non-colliding SM key.
    "sm_label_filter",
    // (c) `| env_extracted="stg"` selects on a single-collision key's
    // `_extracted` slot.
    "sm_collision_extracted",
];

/// The single service every SM record carries (`service_name` base label).
pub const SVC_SM: &str = "svc-sm";

/// Fixed record count and inter-record spacing — the corpus span is
/// `STEP_NS * (ENTRY_COUNT - 1)`, anchored near "now" by the harness.
pub const ENTRY_COUNT: usize = 8;
pub const STEP_NS: i64 = 1_000_000_000;

/// The base label a single-collision record carries, colliding with the SM
/// `env` key so it surfaces as `env_extracted`.
const ENV_BASE: &str = "prod";
/// A double-collision record's pre-existing `env_extracted` base value,
/// which the SM `env` (→ `env_extracted`) must OVERWRITE.
const ENV_EXTRACTED_BASE: &str = "baseval";
/// The single-collision SM value the `sm_collision_extracted` filter selects.
const COLLISION_KEEP_VALUE: &str = "stg";
/// The non-colliding SM `trace_id` value the `sm_label_filter` case selects.
const TRACE_KEEP_VALUE: &str = "keep";

/// Generation parameters for one SM corpus.
#[derive(Debug, Clone)]
pub struct SmCorpusSpec {
    pub base_ns: i64,
    pub run_id: String,
}

/// One generated SM log record: its `service_name`, timestamp, line, any
/// EXTRA base (stream) labels beyond `service_name`/`run_id`, and its
/// per-entry structured metadata (ordered — last-write-wins depends on it).
#[derive(Debug, Clone, PartialEq)]
pub struct SmEntry {
    pub service: &'static str,
    pub ts_ns: i64,
    pub line: String,
    pub base_extra: Vec<(String, String)>,
    pub sm: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SmCorpus {
    pub run_id: String,
    pub entries: Vec<SmEntry>,
    pub first_ts_ns: i64,
    pub last_ts_ns: i64,
}

/// Generates the corpus: `ENTRY_COUNT` records at `base_ns + STEP_NS·i`
/// (globally-distinct timestamps), covering the three SM behaviors in
/// dedicated records (see the module doc comment).
pub fn generate(spec: &SmCorpusSpec) -> SmCorpus {
    let ts = |i: usize| spec.base_ns + STEP_NS * i as i64;
    let env_base = vec![("env".to_string(), ENV_BASE.to_string())];
    let env_double = vec![
        ("env".to_string(), ENV_BASE.to_string()),
        ("env_extracted".to_string(), ENV_EXTRACTED_BASE.to_string()),
    ];
    let non_colliding = |trace: &str| {
        vec![
            ("trace_id".to_string(), trace.to_string()),
            ("user_id".to_string(), "u-42".to_string()),
        ]
    };
    let entries = vec![
        // (a) non-colliding SM; `trace_id=keep` twice (one stream, two
        // entries) + one `trace_id=drop`, so the filter is a strict subset.
        SmEntry {
            service: SVC_SM,
            ts_ns: ts(0),
            line: "sm surfacing keep 0".to_string(),
            base_extra: vec![],
            sm: non_colliding(TRACE_KEEP_VALUE),
        },
        SmEntry {
            service: SVC_SM,
            ts_ns: ts(1),
            line: "sm surfacing keep 1".to_string(),
            base_extra: vec![],
            sm: non_colliding(TRACE_KEEP_VALUE),
        },
        SmEntry {
            service: SVC_SM,
            ts_ns: ts(2),
            line: "sm surfacing drop 2".to_string(),
            base_extra: vec![],
            sm: non_colliding("drop"),
        },
        // (b) single collision: base `env=prod` + SM `env=stg`/`dev` →
        // `env_extracted`. `stg` twice + `dev` once, so `env_extracted="stg"`
        // is a strict subset.
        SmEntry {
            service: SVC_SM,
            ts_ns: ts(3),
            line: "sm collision 3".to_string(),
            base_extra: env_base.clone(),
            sm: vec![("env".to_string(), COLLISION_KEEP_VALUE.to_string())],
        },
        SmEntry {
            service: SVC_SM,
            ts_ns: ts(4),
            line: "sm collision 4".to_string(),
            base_extra: env_base.clone(),
            sm: vec![("env".to_string(), COLLISION_KEEP_VALUE.to_string())],
        },
        SmEntry {
            service: SVC_SM,
            ts_ns: ts(5),
            line: "sm collision 5".to_string(),
            base_extra: env_base,
            sm: vec![("env".to_string(), "dev".to_string())],
        },
        // (c) double collision: base `env=prod` AND `env_extracted=baseval`
        // + SM `env=qa` → the renamed `env_extracted` OVERWRITES `baseval`
        // (last-write-wins). No `env_extracted_extracted`; `baseval` gone.
        SmEntry {
            service: SVC_SM,
            ts_ns: ts(6),
            line: "sm double 6".to_string(),
            base_extra: env_double.clone(),
            sm: vec![("env".to_string(), "qa".to_string())],
        },
        SmEntry {
            service: SVC_SM,
            ts_ns: ts(7),
            line: "sm double 7".to_string(),
            base_extra: env_double,
            sm: vec![("env".to_string(), "qa".to_string())],
        },
    ];
    debug_assert_eq!(entries.len(), ENTRY_COUNT);
    let first_ts_ns = entries.first().map_or(spec.base_ns, |e| e.ts_ns);
    let last_ts_ns = entries.last().map_or(spec.base_ns, |e| e.ts_ns);
    SmCorpus {
        run_id: spec.run_id.clone(),
        entries,
        first_ts_ns,
        last_ts_ns,
    }
}

/// The base (stream) labels a store exposes for one entry before SM merges:
/// `service_name` + `run_id` + the entry's extra base labels.
fn base_labels(run_id: &str, e: &SmEntry) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::from([
        ("service_name".to_string(), e.service.to_string()),
        (RUN_ATTR.to_string(), run_id.to_string()),
    ]);
    for (k, v) in &e.base_extra {
        labels.insert(k.clone(), v.clone());
    }
    labels
}

/// INDEPENDENT re-derivation of the oracle-pinned SM→labels merge (NOT a
/// `pulsus-read` call — the `naive_matches` circularity-breaker convention):
///
///  - a SM key colliding with an ORIGINAL base label is renamed
///    `<key>_extracted` (collision is judged against the base keys only, a
///    snapshot taken before any SM is merged);
///  - the (possibly renamed) key is then UPSERTED — last-write-wins — so a
///    second colliding SM value overwrites the first `_extracted` slot, and a
///    base that already carries `<key>_extracted` is overwritten in place
///    (no `<key>_extracted_extracted`).
///
/// SM pairs are consumed in order, so last-write-wins is order-dependent —
/// matching the shipped engine's reuse of the wire order.
pub fn merge_sm_into_labels(
    base: &BTreeMap<String, String>,
    sm: &[(String, String)],
) -> BTreeMap<String, String> {
    let base_keys: BTreeSet<String> = base.keys().cloned().collect();
    let mut out = base.clone();
    for (k, v) in sm {
        let key = if base_keys.contains(k) {
            format!("{k}_extracted")
        } else {
            k.clone()
        };
        out.insert(key, v.clone());
    }
    out
}

/// One entry's final (merged) response label set.
fn merged_labels(c: &SmCorpus, e: &SmEntry) -> BTreeMap<String, String> {
    merge_sm_into_labels(&base_labels(&c.run_id, e), &e.sm)
}

/// The by-construction expected result set for one committed SM case.
pub fn expected_case_result(c: &SmCorpus, case_id: &str) -> ExpectedResult {
    let mut out = ExpectedResult::new();
    for e in &c.entries {
        let labels = merged_labels(c, e);
        let selected = match case_id {
            // Bare selector: every SM record surfaces under its merged labels.
            "sm_surfacing" => true,
            // `| trace_id="keep"`: the merged `trace_id` label equals `keep`.
            "sm_label_filter" => {
                labels.get("trace_id").map(String::as_str) == Some(TRACE_KEEP_VALUE)
            }
            // `| env_extracted="stg"`: the merged `env_extracted` slot.
            "sm_collision_extracted" => {
                labels.get("env_extracted").map(String::as_str) == Some(COLLISION_KEEP_VALUE)
            }
            other => panic!("expected_case_result: unknown SM case id {other:?}"),
        };
        if selected {
            out.entry(labels)
                .or_default()
                .insert((e.ts_ns, e.line.clone()));
        }
    }
    out
}

/// Every record under its merged label set — the run-scoped completeness
/// query's (`{run_id="R"}`, no pipeline) expectation.
pub fn expected_all_records(c: &SmCorpus) -> ExpectedResult {
    expected_case_result(c, "sm_surfacing")
}

/// One Loki JSON push body per distinct base-label stream:
/// `{"streams":[{"stream":{base labels}, "values":[[ts,line,{sm}],…]}]}`.
/// Byte-identical to both stores. Grouped by the full base label set (not
/// just service), deterministic in `BTreeMap` key order.
pub fn to_loki_push_json(c: &SmCorpus) -> Vec<serde_json::Value> {
    let mut by_stream: BTreeMap<BTreeMap<String, String>, Vec<&SmEntry>> = BTreeMap::new();
    for e in &c.entries {
        by_stream
            .entry(base_labels(&c.run_id, e))
            .or_default()
            .push(e);
    }
    by_stream
        .into_iter()
        .map(|(labels, entries)| {
            let values: Vec<serde_json::Value> = entries
                .iter()
                .map(|e| {
                    let sm: serde_json::Map<String, serde_json::Value> =
                        e.sm.iter()
                            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                            .collect();
                    serde_json::json!([e.ts_ns.to_string(), e.line, sm])
                })
                .collect();
            serde_json::json!({ "streams": [ { "stream": labels, "values": values } ] })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> SmCorpusSpec {
        SmCorpusSpec {
            base_ns: 1_700_000_000_000_000_000,
            run_id: "e2e-sm-test-run".to_string(),
        }
    }

    #[test]
    fn generate_is_deterministic_and_covers_the_fixed_count() {
        let a = generate(&spec());
        let b = generate(&spec());
        assert_eq!(a, b);
        assert_eq!(a.entries.len(), ENTRY_COUNT);
        assert_eq!(to_loki_push_json(&a), to_loki_push_json(&b));
    }

    #[test]
    fn all_entry_timestamps_are_globally_distinct() {
        let corpus = generate(&spec());
        let distinct: BTreeSet<i64> = corpus.entries.iter().map(|e| e.ts_ns).collect();
        assert_eq!(distinct.len(), corpus.entries.len());
        assert_eq!(corpus.first_ts_ns, corpus.entries.first().unwrap().ts_ns);
        assert_eq!(corpus.last_ts_ns, corpus.entries.last().unwrap().ts_ns);
    }

    /// The circularity breaker (AC6): the independent merge re-derivation
    /// produces the exact oracle-pinned collision labels, asserted against
    /// literal expected maps so a projection typo fails hermetically.
    #[test]
    fn merge_sm_into_labels_reproduces_the_three_oracle_pinned_behaviors() {
        let base = BTreeMap::from([
            ("service_name".to_string(), SVC_SM.to_string()),
            (RUN_ATTR.to_string(), "R".to_string()),
        ]);
        // (a) non-colliding keys fan verbatim.
        assert_eq!(
            merge_sm_into_labels(
                &base,
                &[
                    ("trace_id".to_string(), "keep".to_string()),
                    ("user_id".to_string(), "u-42".to_string()),
                ],
            ),
            BTreeMap::from([
                ("service_name".to_string(), SVC_SM.to_string()),
                (RUN_ATTR.to_string(), "R".to_string()),
                ("trace_id".to_string(), "keep".to_string()),
                ("user_id".to_string(), "u-42".to_string()),
            ]),
        );
        // (b) single collision: SM `env` -> `env_extracted`, base `env` kept.
        let mut env_base = base.clone();
        env_base.insert("env".to_string(), "prod".to_string());
        assert_eq!(
            merge_sm_into_labels(&env_base, &[("env".to_string(), "stg".to_string())]),
            BTreeMap::from([
                ("service_name".to_string(), SVC_SM.to_string()),
                (RUN_ATTR.to_string(), "R".to_string()),
                ("env".to_string(), "prod".to_string()),
                ("env_extracted".to_string(), "stg".to_string()),
            ]),
        );
        // (c) double collision: base env + env_extracted; SM env overwrites
        // the `_extracted` slot once (no env_extracted_extracted, baseval gone).
        let mut env_double = env_base.clone();
        env_double.insert("env_extracted".to_string(), "baseval".to_string());
        let merged = merge_sm_into_labels(&env_double, &[("env".to_string(), "qa".to_string())]);
        assert_eq!(
            merged,
            BTreeMap::from([
                ("service_name".to_string(), SVC_SM.to_string()),
                (RUN_ATTR.to_string(), "R".to_string()),
                ("env".to_string(), "prod".to_string()),
                ("env_extracted".to_string(), "qa".to_string()),
            ]),
        );
        assert!(!merged.contains_key("env_extracted_extracted"));
    }

    #[test]
    fn expected_case_results_are_non_empty_strict_subsets_below_the_limit() {
        const LIMIT: usize = 1_000;
        let corpus = generate(&spec());
        let all = expected_all_records(&corpus);
        let all_entries: usize = all.values().map(BTreeSet::len).sum();
        assert_eq!(all_entries, ENTRY_COUNT);
        for case in SM_CASE_IDS {
            let expected = expected_case_result(&corpus, case);
            let entries: usize = expected.values().map(BTreeSet::len).sum();
            assert!(entries > 0, "SM case {case:?} is vacuous");
            assert!(entries < LIMIT, "SM case {case:?} not below the limit");
        }
        // The two filter cases must be STRICT subsets (proving they filter).
        for case in ["sm_label_filter", "sm_collision_extracted"] {
            let entries: usize = expected_case_result(&corpus, case)
                .values()
                .map(BTreeSet::len)
                .sum();
            assert!(
                entries < all_entries,
                "SM case {case:?} selected every record — it does not filter"
            );
        }
    }

    /// The double-collision record surfaces exactly one `env_extracted` with
    /// the SM value and drops the base `baseval` — the (c) invariant, via the
    /// full expected projection (what the nightly surfacing case compares).
    #[test]
    fn double_collision_record_has_a_single_overwritten_extracted_slot() {
        let corpus = generate(&spec());
        let qa_stream = expected_all_records(&corpus)
            .into_keys()
            .find(|labels| labels.get("env_extracted").map(String::as_str) == Some("qa"))
            .expect("the double-collision stream surfaces env_extracted=qa");
        assert_eq!(qa_stream.get("env").map(String::as_str), Some("prod"));
        assert!(!qa_stream.contains_key("env_extracted_extracted"));
        assert_ne!(
            qa_stream.get("env_extracted").map(String::as_str),
            Some("baseval")
        );
    }

    #[test]
    fn push_bodies_carry_three_element_values_with_the_sm_object() {
        let corpus = generate(&spec());
        let bodies = to_loki_push_json(&corpus);
        assert!(!bodies.is_empty());
        let mut total = 0usize;
        for body in &bodies {
            let streams = body["streams"].as_array().unwrap();
            assert_eq!(streams.len(), 1);
            for value in streams[0]["values"].as_array().unwrap() {
                let arr = value.as_array().unwrap();
                assert_eq!(arr.len(), 3, "each value is [ts, line, {{sm}}]");
                assert!(arr[0].is_string() && arr[1].is_string() && arr[2].is_object());
                total += 1;
            }
        }
        assert_eq!(total, ENTRY_COUNT);
    }
}
