//! Env-gated by()/coalesce() VALUE differential (issue #193) — a REAL
//! two-system differential proving grouped / coalesced spanSet-array
//! parity (not just parse acceptance).
//!
//! For each fixture it ingests the SAME spans into both systems and reads
//! the grouped response back live:
//!
//!   * **PulsusDB side** — the fixture spans are written to a live
//!     ClickHouse (a throwaway DB) and the `by()`/`coalesce()` response is
//!     read through this crate's REAL two-phase search executor
//!     ([`TraceEngine::search`]) — genuine engine output (the
//!     `TraceSearchResult.groups` layer #193 builds), never a constant.
//!   * **Tempo side** — the same spans are pushed to the pinned
//!     `grafana/tempo:3.0.2` OTLP receiver and the grouped spanSets are
//!     read back from its live `/api/search` with the identical `q=`.
//!
//! The gate compares, per trace: the SET of group key-tuples, the per-group
//! span-id membership, and the group `attributes` TYPING — the value is a
//! TYPE-TAGGED token (`stringValue=…`/`intValue=…`/`doubleValue=…`/
//! `boolValue=…`), so a wire-type mismatch (e.g. Tempo `stringValue "error"`
//! vs an `intValue 2`) fails the gate, not just a value mismatch. A
//! `coalesce()` fixture asserts the groups collapse to a single flat
//! spanSet on BOTH sides.
//!
//! **Type coverage (flag-5).** One representative case of EACH by-key type
//! so a single CI pass reveals any remaining wire-type divergence: `name`
//! (string), `status`/`kind` (lowercase keyword `stringValue`), `duration`
//! (Go `time.Duration.String()` `stringValue`), and `nestedSetParent`
//! (`intValue`). Numeric-attribute `doubleValue` and the `-0.0`/NaN
//! `canonical_double_bits` folding are pinned by the HERMETIC
//! `search_eval::tests::float_by_key_collapses_signed_zero_and_all_nan`
//! and `..::grouped_charges_equal_retained_plus_counter_exactly` units
//! (OTLP/JSON cannot even carry a NaN attribute), so those need no live
//! oracle.
//!
//! Gate: skips unless `PULSUS_TEST_CLICKHOUSE=1` AND
//! `PULSUSDB_GROUPING_DIFF_URL` (Tempo search API base, e.g.
//! `http://localhost:3200`) AND `PULSUSDB_GROUPING_OTLP_URL` (Tempo OTLP
//! HTTP base, e.g. `http://localhost:4318`) are all set. Run locally:
//!
//! ```text
//! # ClickHouse 26.3 on 19124, Tempo 3.0.2 on 3200 (API) / 4318 (OTLP)
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=19124 \
//!   PULSUSDB_GROUPING_DIFF_URL=http://localhost:3200 \
//!   PULSUSDB_GROUPING_OTLP_URL=http://localhost:4318 \
//!   cargo test -p pulsus-read --test traces_search_grouping_differential -- --nocapture
//! ```
//!
//! Clean-room: no Tempo/Grafana source, grammar, or test corpus is read —
//! the fixtures are our own authorship and the Tempo values are read back
//! as black-box runtime output.
//!
//! **Known wiring hole, recorded not fixed** (issue #458): this suite's
//! endpoint URL variables are read with a bare `env::var` and taken as a
//! skip when absent, while `PULSUS_TEST_CLICKHOUSE` is fail-closed. Drop
//! only the URLs from a live step and it reports GREEN having compared
//! nothing — ledger entry
//! `traceql-differential-legs-skip-green-on-a-missing-endpoint`, which
//! names the two-line fix (`pulsus_testkit::require_live_endpoint_gate`).

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_read::traces::search_plan::{SearchParams, plan_search};
use pulsus_read::{GroupValue, TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, run_init};

// ---------------------------------------------------------------------------
// ClickHouse setup
// ---------------------------------------------------------------------------

fn ch_config(database: &str) -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: database.to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

fn engine_config() -> TraceReadConfig {
    TraceReadConfig {
        // Issue #398: the per-query ClickHouse memory ceiling; the
        // production default, so this fixture keeps today's behaviour.
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        spans_table: "trace_spans".to_string(),
        attrs_table: "trace_attrs_idx".to_string(),
        edges_table: "trace_edges".to_string(),
        max_candidates: 100_000,
        scan_budget_rows: 50_000_000,
        max_series: 1_000,
        generator_max_memory_bytes: 536_870_912,
        distributed: false,
        skip_unavailable_shards: false,
    }
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

async fn init_db(bootstrap: &ChClient, db: &str) {
    exec(bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
    let params = RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    };
    run_init(bootstrap, &params).await.expect("run_init");
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SpanDef {
    id: u8,
    /// 0 = root (no `parentSpanId`).
    parent: u8,
    name: &'static str,
    ts_ns: i64,
    duration_ns: i64,
    /// OTLP StatusCode (0 unset / 1 ok / 2 error).
    status: i32,
    /// OTLP SpanKind (1 internal / 2 server / 3 client / 4 producer /
    /// 5 consumer).
    kind: i32,
}

impl SpanDef {
    fn new(id: u8, name: &'static str, ts_ns: i64) -> Self {
        SpanDef {
            id,
            parent: 0,
            name,
            ts_ns,
            duration_ns: 1_000,
            status: 0,
            kind: 1,
        }
    }
    fn status(mut self, status: i32) -> Self {
        self.status = status;
        self
    }
    fn kind(mut self, kind: i32) -> Self {
        self.kind = kind;
        self
    }
    fn duration(mut self, duration_ns: i64) -> Self {
        self.duration_ns = duration_ns;
        self
    }
    fn parent(mut self, parent: u8) -> Self {
        self.parent = parent;
        self
    }
}

struct Fixture {
    /// The differential name.
    name: &'static str,
    /// The TraceQL query.
    q: &'static str,
    /// The `resource.service.name` this fixture's spans carry. The issue
    /// #492 ordering fixtures scope their queries to it, so a reference
    /// instance holding another suite's corpus cannot enter their answer.
    service: &'static str,
    /// `true` when the query coalesces back to a single flat spanSet.
    coalesced: bool,
    spans: Vec<SpanDef>,
}

fn fixtures(base: i64) -> Vec<Fixture> {
    let sec = 1_000_000_000i64;
    vec![
        Fixture {
            name: "by_name_string_groups",
            q: "{} | by(name)",
            service: "svc",
            coalesced: false,
            spans: vec![
                SpanDef::new(1, "gold", base),
                SpanDef::new(2, "gold", base + sec),
                SpanDef::new(3, "silver", base + 2 * sec),
            ],
        },
        Fixture {
            name: "by_name_then_coalesce_collapses",
            q: "{} | by(name) | coalesce()",
            service: "svc",
            coalesced: true,
            spans: vec![
                SpanDef::new(1, "gold", base),
                SpanDef::new(2, "silver", base + sec),
            ],
        },
        // Flag-5 coverage: one representative case of EACH by-key TYPE so a
        // single CI pass reveals any remaining wire-type divergence.
        // `status` renders its lowercase keyword as `stringValue`.
        Fixture {
            name: "by_status_keyword_string",
            q: "{} | by(status)",
            service: "svc",
            coalesced: false,
            spans: vec![
                SpanDef::new(1, "s", base).status(2),           // error
                SpanDef::new(2, "s", base + sec).status(2),     // error
                SpanDef::new(3, "s", base + 2 * sec).status(1), // ok
            ],
        },
        // `kind` renders its lowercase keyword as `stringValue`.
        Fixture {
            name: "by_kind_keyword_string",
            q: "{} | by(kind)",
            service: "svc",
            coalesced: false,
            spans: vec![
                SpanDef::new(1, "s", base).kind(2),           // server
                SpanDef::new(2, "s", base + sec).kind(2),     // server
                SpanDef::new(3, "s", base + 2 * sec).kind(3), // client
            ],
        },
        // `duration` renders Go's `time.Duration.String()` as `stringValue`.
        Fixture {
            name: "by_duration_go_string",
            q: "{} | by(duration)",
            service: "svc",
            coalesced: false,
            spans: vec![
                SpanDef::new(1, "s", base).duration(1_500_000_000), // 1.5s
                SpanDef::new(2, "s", base + sec).duration(1_500_000_000),
                SpanDef::new(3, "s", base + 2 * sec).duration(2_000_000_000), // 2s
            ],
        },
        // A nested-set (COUNT/numbering) intrinsic renders as `intValue`.
        // A simple root -> single-child tree (no siblings, in-window) has
        // an unambiguous numbering both systems agree on: root
        // nestedSetParent = -1, child nestedSetParent = root's left (1).
        Fixture {
            name: "by_nested_set_parent_int",
            q: "{} | by(nestedSetParent)",
            service: "svc",
            coalesced: false,
            spans: vec![
                SpanDef::new(1, "root", base),
                SpanDef::new(2, "child", base + sec).parent(1),
            ],
        },
        // -- issue #492 item 2: the WRITTEN order of the stages ---------
        //
        // Six orderings whose SQL is byte-identical in pairs, so only the
        // evaluator can tell them apart. Two of them must NOT move
        // (`count() > 2 | by(name)` and `by(name) | coalesce() | count()`),
        // which is the half a fix for the other four can silently break.
        //
        // Corpus C-ORD1 (`grp492`): three spans named `a` with durations
        // 0.5s / 2s / 3s, one named `b` with 0.4s, one second apart.
        // Corpus C-ORD2 (`grp492b`): cross-cutting keys — (a, server),
        // (a, client), (b, server), (b, server) — so neither key alone
        // separates what the other separates.
        //
        // Each is scoped to its own service so the answer cannot depend on
        // what else the shared reference instance holds.
        Fixture {
            name: "ord_by_then_count_filters_the_groups",
            q: r#"{ resource.service.name = "grp492" } | by(name) | count() > 2"#,
            service: "grp492",
            coalesced: false,
            spans: ord1_spans(base, sec),
        },
        Fixture {
            name: "ord_count_then_by_is_unchanged",
            q: r#"{ resource.service.name = "grp492" } | count() > 2 | by(name)"#,
            service: "grp492",
            coalesced: false,
            spans: ord1_spans(base, sec),
        },
        Fixture {
            name: "ord_coalesce_merges_the_survivors",
            q: r#"{ resource.service.name = "grp492" } | by(name) | count() > 2 | coalesce()"#,
            service: "grp492",
            coalesced: true,
            spans: ord1_spans(base, sec),
        },
        Fixture {
            name: "ord_coalesce_before_the_filter_is_unchanged",
            q: r#"{ resource.service.name = "grp492" } | by(name) | coalesce() | count() > 2"#,
            service: "grp492",
            coalesced: true,
            spans: ord1_spans(base, sec),
        },
        Fixture {
            name: "ord_nested_by_name_then_kind",
            q: r#"{ resource.service.name = "grp492b" } | by(name) | by(kind)"#,
            service: "grp492b",
            coalesced: false,
            spans: ord2_spans(base, sec),
        },
        Fixture {
            name: "ord_nested_by_kind_then_name",
            q: r#"{ resource.service.name = "grp492b" } | by(kind) | by(name)"#,
            service: "grp492b",
            coalesced: false,
            spans: ord2_spans(base, sec),
        },
    ]
}

/// Corpus C-ORD1 (issue #492 item 2): three spans named `a` (0.5s / 2s /
/// 3s) and one named `b` (0.4s), starting one second apart, all children
/// of the first. `count() > 2`, `max(duration) > 1s`, `avg(duration) > 1s`
/// and `sum(duration) > 5s` all separate the two groups the same way.
fn ord1_spans(base: i64, sec: i64) -> Vec<SpanDef> {
    vec![
        SpanDef::new(1, "a", base).duration(sec / 2),
        SpanDef::new(2, "a", base + sec).duration(2 * sec).parent(1),
        SpanDef::new(3, "a", base + 2 * sec)
            .duration(3 * sec)
            .parent(1),
        SpanDef::new(4, "b", base + 3 * sec)
            .duration(2 * sec / 5)
            .parent(1),
    ]
}

/// Corpus C-ORD2 (issue #492 item 2): cross-cutting `name` / `kind` keys,
/// so a second `by()` that sub-divides gives three groups and one that
/// rebuilds from the matched set gives two.
fn ord2_spans(base: i64, sec: i64) -> Vec<SpanDef> {
    vec![
        SpanDef::new(1, "a", base).kind(2),
        SpanDef::new(2, "a", base + sec).kind(3),
        SpanDef::new(3, "b", base + 2 * sec).kind(2),
        SpanDef::new(4, "b", base + 3 * sec).kind(2),
    ]
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn sid_bytes(id: u8) -> [u8; 8] {
    let mut b = [0u8; 8];
    b[7] = id;
    b
}

// ---------------------------------------------------------------------------
// PulsusDB side
// ---------------------------------------------------------------------------

async fn pulsus_insert(
    client: &ChClient,
    db: &str,
    trace: &[u8; 16],
    spans: &[SpanDef],
    service: &str,
) {
    let mut rows = Vec::new();
    for s in spans {
        let parent = if s.parent == 0 {
            "0000000000000000".to_string()
        } else {
            hex(&sid_bytes(s.parent))
        };
        rows.push(format!(
            "(toFixedString(unhex('{tid}'),16), toFixedString(unhex('{sid}'),8), \
             toFixedString(unhex('{parent}'),8), '{name}', '{service}', {ts}, {dur}, {status}, \
             {kind}, 1, 'x')",
            tid = hex(trace),
            sid = hex(&sid_bytes(s.id)),
            name = s.name,
            ts = s.ts_ns,
            dur = s.duration_ns,
            status = s.status,
            kind = s.kind,
        ));
    }
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_spans \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) VALUES {}",
            rows.join(", ")
        ),
    )
    .await;
}

/// One side's grouped answer (issue #492 item 2 widened this from a
/// single attribute to the ordered `by(...)` SEQUENCE): the map from a
/// spanSet's ordered `by(...)` `key=typed-value` tokens to its member
/// span-id low bytes, plus the number of spanSets. A coalesced (flat)
/// spanSet — one carrying no `by(...)` attribute — maps under the
/// sentinel `"<flat>"` key.
///
/// The map is compared as a MAP and never as a sequence: the reference
/// iterates its groups out of a hash map, so its spanSets array order is
/// unstable between runs. The COUNT is compared separately, because a map
/// alone cannot see two spanSets that reduce to the same key.
#[derive(Debug, Default, PartialEq, Eq)]
struct GroupedAnswer {
    groups: BTreeMap<Vec<String>, BTreeSet<u8>>,
    span_sets: usize,
}

/// The grouped spanSets PulsusDB returns for one single-trace fixture.
async fn pulsus_groups(engine: &TraceEngine, q: &str, window: (i64, i64)) -> GroupedAnswer {
    let query = pulsus_traceql::parse(q).unwrap_or_else(|e| panic!("parse {q:?}: {e}"));
    let plan = plan_search(
        &query,
        &SearchParams {
            start_ns: window.0,
            end_ns: window.1,
            limit: 100,
            spss: 100,
        },
        &engine.search_ctx(),
    )
    .unwrap_or_else(|e| panic!("plan {q:?}: {e}"));
    let out = engine
        .search(&plan)
        .await
        .unwrap_or_else(|e| panic!("search {q:?}: {e}"));
    let mut answer = GroupedAnswer::default();
    for t in &out.traces {
        match &t.groups {
            Some(groups) => {
                for g in groups {
                    // Only `by(...)`-keyed attributes take part: the
                    // reference also emits the aggregate's own
                    // `count()` / `max(duration)` attribute, which we do
                    // not (issue #510), so it is excluded on BOTH sides.
                    let key: Vec<String> = g
                        .attributes
                        .iter()
                        .filter(|(display, _)| display.starts_with("by("))
                        .map(|(display, value)| format!("{display}={}", group_value_typed(value)))
                        .collect();
                    let key = if key.is_empty() {
                        vec!["<flat>".to_string()]
                    } else {
                        key
                    };
                    let members: BTreeSet<u8> = g.spans.iter().map(|s| s.span_id[7]).collect();
                    answer.span_sets += 1;
                    answer.groups.entry(key).or_default().extend(members);
                }
            }
            None => {
                let members: BTreeSet<u8> = t.spans.iter().map(|s| s.span_id[7]).collect();
                answer.span_sets += 1;
                answer
                    .groups
                    .entry(vec!["<flat>".to_string()])
                    .or_default()
                    .extend(members);
            }
        }
    }
    answer
}

/// PulsusDB's group value as a TYPE-TAGGED token: the wire-type tag PLUS
/// the value, so `intValue 2` never compares equal to `doubleValue 2.0`
/// or `stringValue "2"`. This is what makes the differential genuinely
/// pin the reference's exact rendering (finding: a type-blind string
/// reduction cannot distinguish int/double/string wire types).
fn group_value_typed(value: &GroupValue) -> String {
    match value {
        GroupValue::Str(s) => format!("stringValue={s}"),
        GroupValue::Int(i) => format!("intValue={i}"),
        GroupValue::Double(bits) => format!("doubleValue={}", f64::from_bits(*bits)),
        GroupValue::Bool(b) => format!("boolValue={b}"),
        GroupValue::Nil => "null".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tempo side
// ---------------------------------------------------------------------------

fn otlp_push(otlp_base: &str, trace: &[u8; 16], spans: &[SpanDef], service: &str) {
    let otlp_spans: Vec<serde_json::Value> = spans
        .iter()
        .map(|s| {
            let mut span = serde_json::json!({
                "traceId": hex(trace),
                "spanId": hex(&sid_bytes(s.id)),
                "name": s.name,
                "startTimeUnixNano": s.ts_ns.to_string(),
                "endTimeUnixNano": (s.ts_ns + s.duration_ns).to_string(),
                "kind": s.kind,
                "status": {"code": s.status},
            });
            if s.parent != 0 {
                span["parentSpanId"] = serde_json::Value::String(hex(&sid_bytes(s.parent)));
            }
            span
        })
        .collect();
    let body = serde_json::json!({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": service}}
            ]},
            "scopeSpans": [{"spans": otlp_spans}],
        }]
    });
    let url = format!("{}/v1/traces", otlp_base.trim_end_matches('/'));
    let out = Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "--max-time",
            "20",
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "-d",
            &body.to_string(),
        ])
        .arg(&url)
        .output()
        .expect("curl on PATH");
    let code = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        code.trim(),
        "200",
        "OTLP push to {url} failed (http {code})"
    );
}

/// Polls Tempo's search API until the pushed trace's grouped spanSets are
/// queryable. Indexing is not instantaneous, so a single empty answer is
/// inconclusive — this polls rather than reading once.
fn tempo_groups(api_base: &str, q: &str, trace: &[u8; 16], window: (i64, i64)) -> GroupedAnswer {
    let trace_hex = hex(trace);
    for _ in 0..60 {
        if let Some(answer) = tempo_query_once(api_base, q, &trace_hex, window)
            && !answer.groups.is_empty()
        {
            return answer;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    panic!("Tempo never returned grouped spanSets for trace {trace_hex} within the poll budget");
}

fn tempo_query_once(
    api_base: &str,
    q: &str,
    trace_hex: &str,
    window: (i64, i64),
) -> Option<GroupedAnswer> {
    // The window is derived from the CORPUS BASE instant, never from a
    // fresh clock reading: a query window that drifts away from the data
    // it queries turns a passing case into a red one at some hour of the
    // day (issue #492 item 2, plan v2 D7).
    let start = (window.0 / 1_000_000_000).to_string();
    let end = (window.1 / 1_000_000_000).to_string();
    let url = format!("{}/api/search", api_base.trim_end_matches('/'));
    let out = Command::new("curl")
        .args(["-s", "-G", "--max-time", "20"])
        .args(["--data-urlencode", &format!("q={q}")])
        .args(["--data-urlencode", &format!("start={start}")])
        .args(["--data-urlencode", &format!("end={end}")])
        .args(["--data-urlencode", "limit=100"])
        .args(["--data-urlencode", "spss=100"])
        .arg(&url)
        .output()
        .expect("curl on PATH");
    let body: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    let traces = body.get("traces")?.as_array()?;
    let mut answer = GroupedAnswer::default();
    for t in traces {
        // Tempo strips leading zero bytes from the traceID; match on the
        // trimmed hex suffix.
        let tid = t.get("traceID")?.as_str().unwrap_or("");
        if !trace_hex.trim_start_matches('0').ends_with(tid) && tid != trace_hex {
            continue;
        }
        let span_sets = t
            .get("spanSets")
            .and_then(|s| s.as_array())
            .map(|v| v.as_slice());
        let flat = t.get("spanSet").map(std::slice::from_ref);
        let sets = span_sets.or(flat)?;
        for set in sets {
            // A grouped spanSet carries at least one `by(...)` attribute;
            // a flat one carries none — it may still carry the
            // aggregate's own `count()` attribute, which is excluded here
            // exactly as it is on the PulsusDB side (issue #510).
            let key: Vec<String> = set
                .get("attributes")
                .and_then(|a| a.as_array())
                .map(|attrs| {
                    attrs
                        .iter()
                        .filter(|a| {
                            a.get("key")
                                .and_then(|k| k.as_str())
                                .is_some_and(|k| k.starts_with("by("))
                        })
                        .filter_map(|a| {
                            let key = a.get("key")?.as_str()?;
                            Some(format!("{key}={}", tempo_attr_typed(a)?))
                        })
                        .collect()
                })
                .unwrap_or_default();
            let key = if key.is_empty() {
                vec!["<flat>".to_string()]
            } else {
                key
            };
            let members: BTreeSet<u8> = set
                .get("spans")
                .and_then(|s| s.as_array())
                .map(|spans| {
                    spans
                        .iter()
                        .filter_map(|s| s.get("spanID").and_then(|v| v.as_str()))
                        .filter_map(|h| {
                            u8::from_str_radix(&h[h.len().saturating_sub(2)..], 16).ok()
                        })
                        .collect()
                })
                .unwrap_or_default();
            answer.span_sets += 1;
            answer.groups.entry(key).or_default().extend(members);
        }
    }
    Some(answer)
}

/// A Tempo group `attributes[0]` value as the SAME TYPE-TAGGED token
/// [`group_value_typed`] produces: WHICH `value:{…}` field is populated is
/// the wire type, and it is compared alongside the value. So a Tempo
/// `by(status)` rendered `stringValue "error"` will NOT match PulsusDB's
/// `intValue 2` — the differential fails on the exact int-vs-double-vs-
/// string typing question, as intended.
fn tempo_attr_typed(attr: &serde_json::Value) -> Option<String> {
    let value = attr.get("value")?;
    if let Some(s) = value.get("stringValue").and_then(|v| v.as_str()) {
        return Some(format!("stringValue={s}"));
    }
    // protojson renders 64-bit ints as strings; tolerate a bare number too.
    if let Some(s) = value.get("intValue").and_then(|v| v.as_str()) {
        return Some(format!("intValue={s}"));
    }
    if let Some(n) = value.get("intValue").and_then(|v| v.as_i64()) {
        return Some(format!("intValue={n}"));
    }
    if let Some(f) = value.get("doubleValue").and_then(|v| v.as_f64()) {
        return Some(format!("doubleValue={f}"));
    }
    if let Some(b) = value.get("boolValue").and_then(|v| v.as_bool()) {
        return Some(format!("boolValue={b}"));
    }
    None
}

// ---------------------------------------------------------------------------
// The differential
// ---------------------------------------------------------------------------

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

#[tokio::test(flavor = "multi_thread")]
async fn traces_search_grouping_differential() {
    let (Ok(api_base), Ok(otlp_base), true) = (
        std::env::var("PULSUSDB_GROUPING_DIFF_URL"),
        std::env::var("PULSUSDB_GROUPING_OTLP_URL"),
        pulsus_testkit::live_clickhouse_enabled(),
    ) else {
        eprintln!(
            "skipping the by()/coalesce() grouping differential — set PULSUS_TEST_CLICKHOUSE=1, \
             PULSUSDB_GROUPING_DIFF_URL (Tempo API) and PULSUSDB_GROUPING_OTLP_URL (Tempo OTLP)."
        );
        return;
    };

    let bootstrap = ChClient::new(ch_config("default"))
        .await
        .expect("connect bootstrap");
    // ONE clock reading, to anchor the corpus. Every window on BOTH sides
    // is derived from it, so no case can drift away from the data it
    // queries (issue #492 item 2, plan v2 D7).
    let base = now_ns();
    let sec = 1_000_000_000i64;
    let window = (base - 60 * sec, base + 60 * sec);
    // The reference's own window, from the same corpus instant.
    let reference_window = (base - 3600 * sec, base + 300 * sec);
    let mut diverged: Vec<String> = Vec::new();

    for fx in fixtures(base) {
        let trace = *uuid::Uuid::new_v4().as_bytes();

        // Tempo: push first so it has the whole poll window to index.
        otlp_push(&otlp_base, &trace, &fx.spans, fx.service);

        // PulsusDB: throwaway DB, real ingest + real grouped search readback.
        let db = pulsus_testkit::test_db(&format!("pulsus_grpdiff_it_{}", hex(&trace)));
        init_db(&bootstrap, &db).await;
        let client = ChClient::new(ch_config(&db)).await.expect("connect db");
        pulsus_insert(&client, &db, &trace, &fx.spans, fx.service).await;
        let engine = TraceEngine::new(
            ChClient::new(ch_config(&db)).await.expect("connect engine"),
            engine_config(),
        );
        let pulsus = pulsus_groups(&engine, fx.q, window).await;
        let tempo = tempo_groups(&api_base, fx.q, &trace, reference_window);

        let flat_key = vec!["<flat>".to_string()];
        let mut mism: Vec<String> = Vec::new();
        if fx.coalesced {
            // BOTH sides must present a single flat spanSet (no groups).
            if pulsus.groups.keys().collect::<Vec<_>>() != vec![&flat_key] {
                mism.push(format!(
                    "pulsus did not collapse: {:?}",
                    pulsus.groups.keys()
                ));
            }
            if !tempo.groups.contains_key(&flat_key) || tempo.groups.len() != 1 {
                mism.push(format!("tempo did not collapse: {:?}", tempo.groups.keys()));
            }
        }
        if pulsus.groups != tempo.groups {
            mism.push(format!(
                "group map mismatch: pulsus {:?} != tempo {:?}",
                pulsus.groups, tempo.groups
            ));
        }
        if pulsus.span_sets != tempo.span_sets {
            mism.push(format!(
                "spanSet count mismatch: pulsus {} != tempo {}",
                pulsus.span_sets, tempo.span_sets
            ));
        }

        if mism.is_empty() {
            eprintln!(
                "[{}] AGREES — {} spanSet(s), {} group key(s)",
                fx.name,
                pulsus.span_sets,
                pulsus.groups.len()
            );
        } else {
            eprintln!("[{}] DIVERGES:\n  {}", fx.name, mism.join("\n  "));
            diverged.push(fx.name.to_string());
        }

        exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;
    }

    assert!(
        diverged.is_empty(),
        "by()/coalesce() grouped spanSet-array value parity divergence in {diverged:?} \
         (from REAL PulsusDB + Tempo output)."
    );
}
