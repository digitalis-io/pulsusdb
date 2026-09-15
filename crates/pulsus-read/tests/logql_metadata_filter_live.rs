//! Issue #544 — the live behaviour of the structured-metadata label
//! filter, against a real ClickHouse.
//!
//! What it asserts, one test per acceptance criterion:
//!
//! - **the lowered route executes exactly one sample statement**, counted
//!   in the database's own statement log under a structural filter, over a
//!   quiet period;
//! - **the lowered queries answer what the reference answers**, over the
//!   design record's corpora, with the client pipeline removing zero of
//!   the rows the statement returned;
//! - **the refused filters answer as they do today, on today's route** —
//!   the page loop, more than one sample statement;
//! - **the metric answers do not move**, and the fragment is in the
//!   statement;
//! - **a fragment past the budget falls back**, on the streams route and
//!   on both database-aggregated metric routes, with a `200` and the same
//!   answer rather than a rejection or a wrong number.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-read --test logql_metadata_filter_live
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_logql::parse;
use pulsus_read::logql::{Direction, QueryParams, QuerySpec};
use pulsus_read::{EngineConfig, LogQlEngine, QueryResult};
use pulsus_schema::{RenderCtx, SchemaParams, run_init};

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping
/// when the gate is absent in a live CI job, so a lost `env:` block
/// reddens the build instead of reporting green (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-read/tests/logql_metadata_filter_live.rs for setup)"
            );
            return;
        }
    };
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    }
}

fn schema_params(db: &str) -> SchemaParams {
    RenderCtx {
        db: db.to_string(),
        cluster: None,
        dist_suffix: "_dist".to_string(),
        storage_policy: None,
        retention_days: 7,
        log_rollup: Duration::from_secs(5),
    }
}

fn engine_config(db: &str) -> EngineConfig {
    EngineConfig {
        read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
        db: db.to_string(),
        streams_idx: "log_streams_idx".to_string(),
        streams: "log_streams".to_string(),
        samples: "log_samples".to_string(),
        rollup_table: "log_metrics_5s".to_string(),
        patterns_table: "log_patterns".to_string(),
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
        distributed: false,
    }
}

async fn admin_client() -> ChClient {
    ChClient::new(test_config()).await.expect("connect admin")
}

async fn data_client(db: &str) -> ChClient {
    let mut cfg = test_config();
    cfg.database = db.to_string();
    ChClient::new(cfg).await.expect("connect data client")
}

/// Wall-clock now, in nanoseconds. Fixture timestamps must be recent:
/// `log_samples` carries a TTL and `ttl_only_drop_parts = 1`.
fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SeedSampleRow {
    service: String,
    fingerprint: u64,
    timestamp_ns: i64,
    severity: i8,
    body: String,
    structured_metadata: String,
}

/// One seeded stream: its fingerprint, its `service` column and the
/// canonical-JSON label set stage 2 returns.
struct Stream {
    fingerprint: u64,
    service: &'static str,
    labels: &'static str,
}

/// One seeded row: its stream, its body and its stored metadata text.
///
/// **The metadata texts are canonical**: sorted unique keys, string
/// values, `serde_json` escaping — what `render_structured_metadata`
/// produces. They are written out rather than pushed through the receiver
/// because this suite is about what the READ does with a stored text;
/// `crates/pulsus-server/tests/logs_metadata_filter_live.rs` is the one
/// that puts them through the real write path.
struct Seed {
    stream: usize,
    body: &'static str,
    metadata: &'static str,
}

async fn create_schema(db: &str) -> ChClient {
    let admin = admin_client().await;
    admin
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
    run_init(&admin, &schema_params(db))
        .await
        .expect("run_init");
    data_client(db).await
}

async fn seed(client: &ChClient, db: &str, ts_ns: i64, streams: &[Stream], rows: &[Seed]) {
    for s in streams {
        client
            .execute(
                &format!(
                    "INSERT INTO {db}.log_streams (month, fingerprint, service, labels, \
                     updated_ns) VALUES \
                     (toStartOfMonth(fromUnixTimestamp64Nano(toInt64({ts_ns}))), {}, '{}', '{}', \
                     0)",
                    s.fingerprint,
                    s.service,
                    s.labels.replace('\'', "\\'")
                ),
                &QuerySettings::new(),
                Idempotency::Idempotent,
            )
            .await
            .expect("seed log_streams");
    }
    let block: Vec<SeedSampleRow> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let s = &streams[r.stream];
            SeedSampleRow {
                service: s.service.to_string(),
                fingerprint: s.fingerprint,
                // One nanosecond apart, ascending in seed order.
                timestamp_ns: ts_ns + i as i64,
                severity: 0,
                body: r.body.to_string(),
                structured_metadata: r.metadata.to_string(),
            }
        })
        .collect();
    client
        .insert_block("log_samples", &block)
        .await
        .expect("seed log_samples");
}

fn window(ts_ns: i64) -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts_ns - 60_000_000_000,
            end_ns: ts_ns + 60_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Forward,
    }
}

/// The number of entries a log query returns.
async fn entries(engine: &LogQlEngine, query: &str, params: &QueryParams) -> usize {
    let expr = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
    let (result, _) = engine
        .query(&expr, params)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    let QueryResult::Streams { items, .. } = result else {
        panic!("{query}: a stream selector must return Streams");
    };
    items.iter().map(|s| s.entries.len()).sum()
}

// =====================================================================
// The corpora — the design record's, written out
// =====================================================================

/// Corpus A — `{service_name="checkout", env="prod"}`, twelve entries.
/// The stored metadata of entries 4 and 5 holds the real code points; the
/// query text spells them with the JSON escapes, which is what a client
/// sends.
const STREAM_A: Stream = Stream {
    fingerprint: 18_374_000_000_000_000_101,
    service: "checkout",
    labels: r#"{"env":"prod","service_name":"checkout"}"#,
};

const CORPUS_A: &[Seed] = &[
    Seed {
        stream: 0,
        body: "GET /api/v1/orders/1 status=200",
        metadata: r#"{"pod":"checkout-7d9f6c5b4-xk2pq","trace_id":"740e2a1b9c3d4f5061728394a5b6c7d8"}"#,
    },
    Seed {
        stream: 0,
        body: "GET /api/v1/orders/2 status=200",
        metadata: r#"{"pod":"checkout-7d9f6c5b4-xk2pq","trace_id":"0000000000000000000000000000beef"}"#,
    },
    Seed {
        stream: 0,
        body: "GET /api/v1/orders/3 status=500",
        metadata: r#"{"pod":"checkout-7d9f6c5b4-aaaaa","trace_id":"740e2a1b9c3d4f5061728394a5b6c7d9"}"#,
    },
    // The backspace and form-feed values: the two the reader could not
    // decode until issue #539, written here as the escapes our own
    // renderer emits for U+0008 and U+000C.
    Seed {
        stream: 0,
        body: "line with backspace metadata",
        metadata: r#"{"note":"a\bb"}"#,
    },
    Seed {
        stream: 0,
        body: "line with formfeed metadata",
        metadata: r#"{"note":"a\fb"}"#,
    },
    Seed {
        stream: 0,
        body: "line with quote metadata",
        metadata: r#"{"note":"he said \"hi\""}"#,
    },
    Seed {
        stream: 0,
        body: "line with no metadata at all",
        metadata: "",
    },
    // An empty-valued pair is stripped at ingest, so the stored text is
    // the column default — the same text the row above holds.
    Seed {
        stream: 0,
        body: "line whose only metadata value was empty",
        metadata: "",
    },
    Seed {
        stream: 0,
        body: "line with emoji metadata",
        metadata: "{\"note\":\"ok \u{1F600}\"}",
    },
    // The ingest namer rewrites `a.b` to `a_b`, so that is what is
    // stored and what the filter names.
    Seed {
        stream: 0,
        body: "line whose metadata name needs sanitising",
        metadata: r#"{"a_b":"x"}"#,
    },
    Seed {
        stream: 0,
        body: "line with a colliding metadata name",
        metadata: r#"{"service_name":"other"}"#,
    },
    Seed {
        stream: 0,
        body: "line with json-shaped metadata value",
        metadata: r#"{"raw":"{\"trace_id\":\"abc\"}"}"#,
    },
];

/// Corpus B — `{service_name="probe3", env="fromStream"}`, the four
/// metadata shapes the `_extracted` rule turns on.
const STREAM_B: Stream = Stream {
    fingerprint: 18_374_000_000_000_000_202,
    service: "probe3",
    labels: r#"{"env":"fromStream","service_name":"probe3"}"#,
};

const CORPUS_B: &[Seed] = &[
    Seed {
        stream: 1,
        body: "both",
        metadata: r#"{"env":"fromSM","env_extracted":"fromSMx"}"#,
    },
    Seed {
        stream: 1,
        body: "only_sm",
        metadata: r#"{"env":"fromSM2"}"#,
    },
    Seed {
        stream: 1,
        body: "only_x",
        metadata: r#"{"env_extracted":"fromSMx2"}"#,
    },
    Seed {
        stream: 1,
        body: "nocollide",
        metadata: r#"{"pod_extracted":"px"}"#,
    },
];

/// Corpus C — `{app="mix"}` selects three streams at once, so ONE
/// statement carries fingerprints in different name-resolution classes.
/// Corpora A and B each select one stream, so their class partitions are
/// all-or-nothing and cannot distinguish the encodings at all.
///
/// `s1c` is the hazard row: its stream carries `env` as a label AND its
/// metadata carries `env="fromSM2"`, so the merge renames the metadata
/// pair and the label wins. A complement arm whose `NOT IN` list is built
/// from the RENDERED arms rather than from the classes in full admits it
/// into the metadata arm and returns it — one row too many.
const STREAM_C1: Stream = Stream {
    fingerprint: 18_374_000_000_000_000_301,
    service: "s1",
    labels: r#"{"app":"mix","env":"fromStream","service_name":"s1"}"#,
};
const STREAM_C2: Stream = Stream {
    fingerprint: 18_374_000_000_000_000_302,
    service: "s2",
    labels: r#"{"app":"mix","service_name":"s2","zone":"z2"}"#,
};
const STREAM_C3: Stream = Stream {
    fingerprint: 18_374_000_000_000_000_303,
    service: "s3",
    labels: r#"{"app":"mix","env":"stream3","service_name":"s3"}"#,
};
/// Two more streams with no `env` label at all, so the ordinary class is
/// LARGER than the stream-label class. That is what makes the renderer
/// choose the complement encoding over the one with no complement — with
/// three streams the explicit form is shorter and the complement's `NOT
/// IN` list is never rendered, so the rule AC17 states is not exercised.
const STREAM_C4: Stream = Stream {
    fingerprint: 18_374_000_000_000_000_304,
    service: "s4",
    labels: r#"{"app":"mix","service_name":"s4","zone":"z4"}"#,
};
const STREAM_C5: Stream = Stream {
    fingerprint: 18_374_000_000_000_000_305,
    service: "s5",
    labels: r#"{"app":"mix","service_name":"s5","zone":"z5"}"#,
};

const CORPUS_C: &[Seed] = &[
    Seed {
        stream: 2,
        body: "s1a",
        metadata: r#"{"env":"fromSM1"}"#,
    },
    Seed {
        stream: 2,
        body: "s1b",
        metadata: r#"{"env_extracted":"litX"}"#,
    },
    Seed {
        stream: 2,
        body: "s1c",
        metadata: r#"{"env":"fromSM2"}"#,
    },
    Seed {
        stream: 3,
        body: "s2a",
        metadata: r#"{"env":"fromSM2"}"#,
    },
    Seed {
        stream: 3,
        body: "s2b",
        metadata: r#"{"other":"x"}"#,
    },
    Seed {
        stream: 4,
        body: "s3a",
        metadata: r#"{"env":"fromSM3","env_extracted":"litY"}"#,
    },
    // The second hazard row, and the one the complement's `NOT IN` list
    // is measured against. Its stream carries `env` as a LABEL whose
    // value is not `fromStream`, and its metadata `env` IS `fromStream`
    // — which the merge renames, so the evaluator never compares it. A
    // complement arm whose `NOT IN` list is built from the RENDERED arms
    // excludes only the PASSING stream-label fingerprints, admits this
    // one into the metadata arm, and returns it: 4 rows where the answer
    // is 3.
    Seed {
        stream: 4,
        body: "s3b",
        metadata: r#"{"env":"fromStream"}"#,
    },
    Seed {
        stream: 5,
        body: "s4a",
        metadata: r#"{"other":"y"}"#,
    },
    Seed {
        stream: 6,
        body: "s5a",
        metadata: r#"{"other":"z"}"#,
    },
];

/// Corpus D — the numeric refusal controls. A value that does not convert
/// keeps the line and sets the error label, so a numeric comparison
/// returns the `abc` line too. That is why the construct is not a
/// comparison and wins no `LIMIT`.
const STREAM_D: Stream = Stream {
    fingerprint: 18_374_000_000_000_000_404,
    service: "numctl",
    labels: r#"{"service_name":"numctl"}"#,
};

const CORPUS_D: &[Seed] = &[
    Seed {
        stream: 7,
        body: "num 700",
        metadata: r#"{"dur":"700"}"#,
    },
    Seed {
        stream: 7,
        body: "num 20",
        metadata: r#"{"dur":"20"}"#,
    },
    Seed {
        stream: 7,
        body: "num abc",
        metadata: r#"{"dur":"abc"}"#,
    },
    Seed {
        stream: 7,
        body: "num 7e2",
        metadata: r#"{"dur":"7e2"}"#,
    },
];

/// The DOUBLE collision: a stream carrying both `env` and
/// `env_extracted` as labels. A metadata pair named `env` is renamed to
/// `env_extracted` and then **overwrites the stream label of that name**,
/// so the stream label does not win — which is the one case the three
/// name-resolution classes do not cover. The filter does not lower on
/// such a stream and the query takes the route it takes today; the
/// answers below are the reference's either way.
const STREAM_DOUBLE: Stream = Stream {
    fingerprint: 18_374_000_000_000_000_606,
    service: "sm-double",
    labels: r#"{"env":"prod","env_extracted":"baseval","service_name":"sm-double"}"#,
};

const CORPUS_DOUBLE: &[Seed] = &[
    Seed {
        stream: 8,
        body: "double collision",
        metadata: r#"{"env":"smval"}"#,
    },
    Seed {
        stream: 8,
        body: "no metadata at all",
        metadata: "",
    },
];

const STREAMS: &[Stream] = &[
    STREAM_A,
    STREAM_B,
    STREAM_C1,
    STREAM_C2,
    STREAM_C3,
    STREAM_C4,
    STREAM_C5,
    STREAM_D,
    STREAM_DOUBLE,
];

fn all_rows() -> Vec<Seed> {
    let mut out: Vec<Seed> = Vec::new();
    for group in [CORPUS_A, CORPUS_B, CORPUS_C, CORPUS_D, CORPUS_DOUBLE] {
        for s in group {
            out.push(Seed {
                stream: s.stream,
                body: s.body,
                metadata: s.metadata,
            });
        }
    }
    out
}

// =====================================================================
// The answers
// =====================================================================

/// `(query, entries)` — the answer the reference gives and the answer we
/// must give. Corpora A and B are the design record's, with its recorded
/// numbers; corpus C's rows are written out above and its numbers are
/// derived from the name-resolution rule, row by row, in the comments.
const LOWERED: &[(&str, usize)] = &[
    // Corpus A.
    (
        r#"{service_name="checkout"} | trace_id="740e2a1b9c3d4f5061728394a5b6c7d8""#,
        1,
    ),
    (
        r#"{service_name="checkout"} | trace_id!="740e2a1b9c3d4f5061728394a5b6c7d8""#,
        11,
    ),
    (r#"{service_name="checkout"} | trace_id="""#, 9),
    // The two escape values, under both spellings. Before issue #539 our
    // reader decoded `\b` as the letter `b`, so the first returned
    // nothing and the second returned this line.
    (r#"{service_name="checkout"} | note="a\bb""#, 1),
    (r#"{service_name="checkout"} | note="abb""#, 0),
    (r#"{service_name="checkout"} | note="a\fb""#, 1),
    (r#"{service_name="checkout"} | note="afb""#, 0),
    (r#"{service_name="checkout"} | note="he said \"hi\"""#, 1),
    (r#"{service_name="checkout"} | note="""#, 8),
    (r#"{service_name="checkout"} | a_b="x""#, 1),
    // The collision rule: a metadata `service_name` is renamed, so the
    // stream label answers the first and the renamed pair the second.
    (r#"{service_name="checkout"} | service_name="other""#, 0),
    (
        r#"{service_name="checkout"} | service_name_extracted="other""#,
        1,
    ),
    (
        r#"{service_name="checkout"} | raw="{\"trace_id\":\"abc\"}""#,
        1,
    ),
    // Corpus B.
    (r#"{service_name="probe3"} | env="fromStream""#, 4),
    (r#"{service_name="probe3"} | env="fromSM""#, 0),
    (r#"{service_name="probe3"} | env_extracted="fromSM""#, 0),
    (r#"{service_name="probe3"} | env_extracted="fromSMx""#, 1),
    (r#"{service_name="probe3"} | env_extracted="fromSMx2""#, 1),
    (r#"{service_name="probe3"} | env_extracted="fromSM2""#, 1),
    (r#"{service_name="probe3"} | pod_extracted="px""#, 1),
    (r#"{service_name="probe3"} | env_extracted="""#, 1),
    // Corpus C. `A` is the streams carrying the name as a LABEL, `C` the
    // rest; `B` is the streams the name un-suffixes onto.
    //
    // A={s1,s3} A_true={s1} C={s2}: every s1 row, and no s2 row.
    (r#"{app="mix"} | env="fromStream""#, 3),
    // A_true={} C={s2}: s2a's metadata `env`. s1c's is RENAMED, so it
    // is not this — that is the hazard row.
    (r#"{app="mix"} | env="fromSM2""#, 1),
    // A_true={s3}: s3a and s3b, both on the stream whose label is
    // `stream3`.
    (r#"{app="mix"} | env="stream3""#, 2),
    // A_true={s3}: s3a, s3b. C={s2,s4,s5}: every row, since none of
    // their metadata `env` is `fromStream`.
    (r#"{app="mix"} | env!="fromStream""#, 6),
    // B={s1,s3} C={s2}: s1a's renamed `env`.
    (r#"{app="mix"} | env_extracted="fromSM1""#, 1),
    // s1b's literal `env_extracted`.
    (r#"{app="mix"} | env_extracted="litX""#, 1),
    // s3a carries BOTH; the literal wins, so this matches and the next
    // one does not.
    (r#"{app="mix"} | env_extracted="litY""#, 1),
    (r#"{app="mix"} | env_extracted="fromSM3""#, 0),
    // C={s2,s4,s5}: s2b alone carries the value `x`.
    (r#"{app="mix"} | other="x""#, 1),
    // B={s1,s3} none empty; C={s2,s4,s5} all four rows empty.
    (r#"{app="mix"} | env_extracted="""#, 4),
    // A_true={} C={s2} none: nothing.
    (r#"{app="mix"} | env="fromSM1""#, 0),
    // The double collision. The renamed metadata pair overwrites the
    // stream label of the same name, so `smval` is the value and
    // `baseval` is not — the answer the reference gives, and the answer
    // the query takes today's route to produce.
    (r#"{service_name="sm-double"} | env_extracted="smval""#, 1),
    // …and the row with NO metadata keeps the stream label, so the base
    // value answers for it and for nothing else.
    (r#"{service_name="sm-double"} | env_extracted="baseval""#, 1),
    (r#"{service_name="sm-double"} | env_extracted="""#, 0),
    (r#"{service_name="sm-double"} | env="prod""#, 2),
];

/// The four the metadata cell refuses. Same answers, today's route.
const REFUSED: &[(&str, usize)] = &[
    (
        r#"{service_name="checkout"} | pod=~"checkout-7d9f6c5b4-.*""#,
        3,
    ),
    (r#"{service_name="numctl"} | dur > 500"#, 3),
    (r#"{service_name="numctl"} | dur = 700"#, 3),
    (r#"{service_name="numctl"} | dur =~ "7.*""#, 2),
];

/// Issue #544 — **every lowered query answers what the reference
/// answers.**
///
/// The pair that used to differ is here twice each: `| note="a\bb"`
/// against `| note="abb"`, and the form-feed pair. Before the shared
/// decoder those two returned each other's answers.
#[tokio::test]
async fn every_lowered_metadata_filter_answers_the_recorded_answer() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("logql_md_filter_answers");
    let client = create_schema(&db).await;
    let ts = now_ns();
    seed(&client, &db, ts, STREAMS, &all_rows()).await;
    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db));
    let params = window(ts);

    let mut wrong: Vec<String> = Vec::new();
    for (query, want) in LOWERED {
        let got = entries(&engine, query, &params).await;
        if got != *want {
            wrong.push(format!("{query}: want {want}, got {got}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} lowered queries answer differently:\n  {}",
        wrong.len(),
        LOWERED.len(),
        wrong.join("\n  ")
    );
    assert_eq!(
        LOWERED.len(),
        36,
        "the design record's 31 lowered queries — corpus A's 13, corpus B's 8 and corpus C's \
         10 — plus corpus F's second query, `| env=\"fromSM1\"`, whose answer the hazard row \
         makes worth asserting beside its first, and the four over the double collision, \
         which is the one shape the three name-resolution classes do not cover"
    );

    // The refusals answer the same as they do today, on the same corpus,
    // so the block above is about the operator rather than about the
    // data.
    for (query, want) in REFUSED {
        assert_eq!(
            entries(&engine, query, &params).await,
            *want,
            "{query}: a refused filter must answer exactly as it does today"
        );
    }
    drop_db(&db).await;
}

async fn drop_db(db: &str) {
    admin_client()
        .await
        .execute(
            &format!("DROP DATABASE IF EXISTS {db}"),
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("drop test database");
}

// =====================================================================
// The statement count
// =====================================================================

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CountRow {
    n: u64,
}

async fn flush_logs(admin: &ChClient) {
    admin
        .execute(
            "SYSTEM FLUSH LOGS",
            &QuerySettings::new(),
            Idempotency::Idempotent,
        )
        .await
        .expect("flush logs");
}

async fn now_micros(admin: &ChClient) -> i64 {
    let mut stream = admin
        .query_stream::<CountRow>(
            "SELECT toUInt64(toUnixTimestamp64Micro(now64(6))) AS n",
            &QuerySettings::new(),
        )
        .await
        .expect("now64");
    i64::try_from(stream.next().await.expect("a row").expect("decode").n).expect("fits i64")
}

/// How many SAMPLE statements the database executed that carry `needle`,
/// since `since_us`.
///
/// **Structural, not textual.** Three of the six constraints ask what a
/// statement READ rather than what its text spells, because text is the
/// wrong thing to match: a statement naming the sample table in an alias
/// inflates a textual count, and excluding the statement log by NAME
/// fails a correct build whose per-case literal happens to spell it.
///
/// **The needle is assembled at query time**, from two halves, so this
/// counting statement's own text never contains it — a counting query
/// that names the literal counts itself, and climbs by one on every read
/// without ever settling.
async fn sample_statements(admin: &ChClient, db: &str, needle: &str, since_us: i64) -> u64 {
    let (first, second) = needle.split_at(needle.len() / 2);
    let sql = format!(
        "SELECT count() AS n FROM system.query_log \
         WHERE type = 'QueryFinish' \
           AND query_kind = 'Select' \
           AND toUInt64(toUnixTimestamp64Micro(event_time_microseconds)) >= {since_us} \
           AND position(query, concat('{first}', '{second}')) > 0 \
           AND has(tables, '{db}.log_samples') \
           AND NOT has(tables, 'system.query_log')"
    );
    let mut stream = admin
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .expect("count sample statements");
    stream.next().await.expect("a row").expect("decode").n
}

/// The count, read across a one-second quiet period after the request has
/// returned.
///
/// The flush is not a barrier, so an early read can legitimately be 0 or
/// short. The window opens only after an explicit flush has RETURNED, the
/// assertion is on the terminal reading, and a count still moving over the
/// final half of the window fails rather than being accepted at whatever
/// it last read. Measured on this machine, the time from a statement
/// finishing to its being countable is 0.057–0.069 s over five probes, so
/// a one-second window is more than twelve times the slowest reading.
async fn settled_sample_statements(admin: &ChClient, db: &str, needle: &str, since_us: i64) -> u64 {
    flush_logs(admin).await;
    let opened = std::time::Instant::now();
    let window = Duration::from_secs(1);
    let mut readings: Vec<(Duration, u64)> = Vec::new();
    loop {
        flush_logs(admin).await;
        let n = sample_statements(admin, db, needle, since_us).await;
        let at = opened.elapsed();
        readings.push((at, n));
        if at >= window {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let tail: Vec<u64> = readings
        .iter()
        .filter(|(at, _)| *at >= window / 2)
        .map(|(_, n)| *n)
        .collect();
    assert!(
        !tail.is_empty() && tail.windows(2).all(|w| w[0] == w[1]),
        "the statement count was still moving over the final half of the window: {readings:?}"
    );
    *tail.last().expect("a terminal reading")
}

/// Sixteen rows on one stream, each carrying a metadata value literal
/// unique to it and **drawn from lowercase hexadecimal only** — so it
/// cannot spell a table name, whatever the exclusion does.
const COUNT_CASES: usize = 16;

fn count_needle(i: usize) -> String {
    format!("aa{i:02}bb{i:02}cc{i:02}dd{i:02}")
}

/// Issue #544 AC6 — **the lowered route executes exactly one sample
/// statement.**
///
/// The count comes from the database's own statement log, not from the
/// execution trace: that trace records the sample read ONCE, before the
/// branch that chooses between one scan and the keyset loop, so it reads
/// `1` whether one statement ran or three thousand did.
///
/// What it discriminates, in all three directions: with the filter
/// compiled the count is **1**; with the paging flag left set while the
/// fragment is still emitted it is the page count; and on a build where
/// the filter never reaches SQL the literal appears in no sample
/// statement at all and the count is **0**.
#[tokio::test]
async fn every_lowered_metadata_filter_executes_one_sample_statement() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("logql_md_filter_statements");
    let client = create_schema(&db).await;
    let admin = admin_client().await;
    let ts = now_ns();
    let stream = Stream {
        fingerprint: 18_374_000_000_000_000_501,
        service: "counted",
        labels: r#"{"service_name":"counted"}"#,
    };
    let metadata: Vec<String> = (0..COUNT_CASES)
        .map(|i| format!(r#"{{"trace_id":"{}"}}"#, count_needle(i)))
        .collect();
    let bodies: Vec<String> = (0..COUNT_CASES).map(|i| format!("row {i}")).collect();
    let rows: Vec<Seed> = (0..COUNT_CASES)
        .map(|i| Seed {
            stream: 0,
            body: Box::leak(bodies[i].clone().into_boxed_str()),
            metadata: Box::leak(metadata[i].clone().into_boxed_str()),
        })
        .collect();
    seed(&client, &db, ts, std::slice::from_ref(&stream), &rows).await;

    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db));
    // **What this count discriminates, measured in both directions.**
    // With the fragment in the statement the count is 1. With the
    // fragment absent — the route this change replaces — no sample
    // statement carries the case's literal at all and the count is 0,
    // which is what the break below reports.
    //
    // **The other break does not work, and saying so is the point.**
    // Leaving the paging flag set while still EMITTING the fragment
    // leaves the count at 1, measured at `limit` 1, 2 and 100: an exact
    // predicate makes the first page either fill the limit or come back
    // short, and a short page is what the loop reads as an exhausted
    // window. A page loop over an exact predicate is one statement, so
    // the flag alone is not observable here.
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts - 60_000_000_000,
            end_ns: ts + 60_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 2,
        direction: Direction::Forward,
    };
    for i in 0..COUNT_CASES {
        let needle = count_needle(i);
        let query = format!(r#"{{service_name="counted"}} | trace_id="{needle}""#);
        let since = now_micros(&admin).await;
        assert_eq!(
            entries(&engine, &query, &params).await,
            1,
            "{query}: one row carries this value"
        );
        let n = settled_sample_statements(&admin, &db, &needle, since).await;
        assert_eq!(
            n, 1,
            "{query}: the lowered route must execute exactly one sample statement, not {n}"
        );
    }
    drop_db(&db).await;
}

/// Issue #544 AC7 — **the four refused filters answer as they do today,
/// on today's route.**
///
/// The route is the page loop, so the sample statement runs more than
/// once. That is what makes the assertion above about the LOWERING rather
/// than about a corpus small enough to fit in one page either way.
#[tokio::test]
async fn the_four_refused_filters_answer_unchanged_on_the_page_loop() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("logql_md_filter_refused");
    let client = create_schema(&db).await;
    let admin = admin_client().await;
    let ts = now_ns();
    seed(&client, &db, ts, STREAMS, &all_rows()).await;
    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db));
    // A `limit` of 1 with the shipped scan factor pages one row at a
    // time, so a route that pages issues more than one statement on a
    // corpus of four matching rows.
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts - 60_000_000_000,
            end_ns: ts + 60_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Forward,
    };
    for (query, want) in REFUSED {
        assert_eq!(
            entries(&engine, query, &params).await,
            *want,
            "{query}: the answer must not move"
        );
    }
    // …and the route, read off the statement the database executed. A
    // page carries the OVERSAMPLED row bound — `limit x
    // pipeline_scan_factor`, 1000 here — where the single-statement
    // route carries the request's own `LIMIT 100`; and a refused filter
    // renders no extraction over the metadata column at all.
    let since = now_micros(&admin).await;
    let paged = r#"{service_name="checkout"} | pod=~"checkout-7d9f6c5b4-.*""#;
    assert_eq!(entries(&engine, paged, &params).await, 3);
    flush_logs(&admin).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    flush_logs(&admin).await;
    let sample_statements_since = |text: &'static str| {
        let sql = format!(
            "SELECT count() AS n FROM system.query_log \
             WHERE type = 'QueryFinish' AND query_kind = 'Select' \
               AND toUInt64(toUnixTimestamp64Micro(event_time_microseconds)) >= {since} \
               AND has(tables, '{db}.log_samples') \
               AND NOT has(tables, 'system.query_log') \
               AND position(query, '{text}') > 0"
        );
        let admin = &admin;
        async move {
            let mut stream = admin
                .query_stream::<CountRow>(&sql, &QuerySettings::new())
                .await
                .expect("count statements");
            stream.next().await.expect("a row").expect("decode").n
        }
    };
    assert!(
        sample_statements_since("LIMIT 1000").await >= 1,
        "a refused filter must stay on the page loop, whose row bound is the oversample"
    );
    assert_eq!(
        sample_statements_since("JSONExtractString(structured_metadata").await,
        0,
        "a refused filter must render no extraction over the metadata column"
    );
    drop_db(&db).await;
}

// =====================================================================
// The metric routes, and the budget fallback
// =====================================================================

fn instant(ts_ns: i64) -> QueryParams {
    QueryParams {
        spec: QuerySpec::Instant { at_ns: ts_ns + 1 },
        limit: 100,
        direction: Direction::Forward,
    }
}

/// `(labels, value)` per series, sorted, for a vector answer.
async fn vector(engine: &LogQlEngine, query: &str, params: &QueryParams) -> Vec<(String, f64)> {
    let expr = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
    let (result, _) = engine
        .query(&expr, params)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    let QueryResult::Vector(samples) = result else {
        panic!("{query}: an instant metric query must return a Vector, got {result:?}");
    };
    let mut out: Vec<(String, f64)> = samples
        .into_iter()
        .map(|s| {
            let mut pairs: Vec<String> = s.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
            pairs.sort();
            (format!("{{{}}}", pairs.join(",")), s.value)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// The same, for a range answer: one entry per series, carrying its
/// points' values.
async fn matrix(
    engine: &LogQlEngine,
    query: &str,
    params: &QueryParams,
) -> Vec<(String, Vec<f64>)> {
    let expr = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
    let (result, _) = engine
        .query(&expr, params)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    let QueryResult::Matrix(series) = result else {
        panic!("{query}: a range metric query must return a Matrix, got {result:?}");
    };
    let mut out: Vec<(String, Vec<f64>)> = series
        .into_iter()
        .map(|s| {
            let mut pairs: Vec<String> = s.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
            pairs.sort();
            (
                format!("{{{}}}", pairs.join(",")),
                s.points.into_iter().map(|(_, v)| v).collect(),
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

const TRACE_1: &str = "740e2a1b9c3d4f5061728394a5b6c7d8";

/// Issue #544 AC8 — **the metric answer is the same and the predicate is
/// in the statement.**
///
/// Queries 36 and 37 are the ones that change route: today they are a
/// full raw scan with no predicate and no aggregation, client-aggregated;
/// after, they are one database-aggregated statement. **The answer must
/// not move**, and if the fragment is ever omitted on that route the
/// answer becomes one series per distinct metadata text the window
/// admits instead of one series with value 1 — two on this window,
/// measured, and eleven series over twelve entries on the design
/// record's own.
#[tokio::test]
async fn a_metric_query_over_a_metadata_filter_answers_the_same_from_one_statement() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("logql_md_filter_metric");
    let client = create_schema(&db).await;
    let admin = admin_client().await;
    let ts = now_ns();
    seed(&client, &db, ts, STREAMS, &all_rows()).await;
    let engine = LogQlEngine::new(data_client(&db).await, engine_config(&db));

    let since = now_micros(&admin).await;
    // 36: one series, value 1.
    let count =
        format!(r#"count_over_time({{service_name="checkout"}} | trace_id="{TRACE_1}" [5m])"#);
    assert_eq!(
        vector(&engine, &count, &instant(ts)).await,
        // The matching row's metadata merges into the series identity
        // (issue #249), so the one series carries it.
        vec![("{env=prod,pod=checkout-7d9f6c5b4-xk2pq,service_name=checkout,trace_id=740e2a1b9c3d4f5061728394a5b6c7d8}".to_string(), 1.0)],
        "the instant count must be ONE series with value 1. An unguarded omission of the \
         fragment counts every row the window admits instead, one series per distinct \
         metadata text: two here, and eleven series over twelve entries on the design \
         record's own window"
    );
    // 37: one series, value 31 — the matching line's body is 31 bytes.
    let bytes =
        format!(r#"bytes_over_time({{service_name="checkout"}} | trace_id="{TRACE_1}" [5m])"#);
    assert_eq!(
        vector(&engine, &bytes, &instant(ts)).await,
        vec![("{env=prod,pod=checkout-7d9f6c5b4-xk2pq,service_name=checkout,trace_id=740e2a1b9c3d4f5061728394a5b6c7d8}".to_string(), 31.0)],
    );
    // 38: the parent sum collapses the labels.
    let summed =
        format!(r#"sum(count_over_time({{service_name="checkout"}} | trace_id="{TRACE_1}" [5m]))"#);
    assert_eq!(
        vector(&engine, &summed, &instant(ts)).await,
        vec![("{}".to_string(), 1.0)],
    );
    // 39: the bucketed range route, `[1m]` against a 60s step.
    let ranged =
        format!(r#"sum(count_over_time({{service_name="checkout"}} | trace_id="{TRACE_1}" [1m]))"#);
    let range_params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts,
            end_ns: ts + 60_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Forward,
    };
    let got = matrix(&engine, &ranged, &range_params).await;
    assert_eq!(got.len(), 1, "one series: {got:?}");
    assert_eq!(got[0].0, "{}");
    assert!(
        got[0].1.iter().any(|v| (*v - 1.0).abs() < f64::EPSILON),
        "the range answer must carry the value 1: {got:?}"
    );

    // …and the fragment really is in the statement the database ran.
    flush_logs(&admin).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    flush_logs(&admin).await;
    let sql = format!(
        "SELECT count() AS n FROM system.query_log \
         WHERE type = 'QueryFinish' AND query_kind = 'Select' \
           AND toUInt64(toUnixTimestamp64Micro(event_time_microseconds)) >= {since} \
           AND has(tables, '{db}.log_samples') \
           AND NOT has(tables, 'system.query_log') \
           AND position(query, concat('JSONExtractString(structured_metadata', \
                                      ', ''trace_id'')')) > 0"
    );
    let mut stream = admin
        .query_stream::<CountRow>(&sql, &QuerySettings::new())
        .await
        .expect("count metric statements");
    let with_fragment = stream.next().await.expect("a row").expect("decode").n;
    assert!(
        with_fragment >= 4,
        "each of the four metric queries must carry the metadata fragment in its statement; \
         {with_fragment} did"
    );
    drop_db(&db).await;
}

/// Issue #544 AC12, AC15 and AC16 — **a fragment past the budget falls
/// back, on all three routes that can meet it.**
///
/// The budget is lowered to one byte for this test, which makes every
/// render exceed it on a three-stream corpus; the shipped constant needs
/// about 48,000 selected streams to reach the same state, which is a
/// corpus, not a test.
///
/// What each route must do:
///
/// ```text
/// streams                      omit the fragment, page — a 200 and the same rows
/// metric, instant, database-   swap the RESTORED client stage in — one series, value 1.
///   aggregated                 Omitting the fragment with no client stage returns
///                              a series per distinct metadata text
/// metric, range, bucketed      the same restored stage, NOT the empty-pipeline
///                              fallback, which filters nothing
/// ```
#[tokio::test]
async fn an_oversized_fragment_falls_back_on_every_route_that_can_meet_it() {
    skip_unless_live!();
    let db = pulsus_testkit::test_db("logql_md_filter_budget");
    let client = create_schema(&db).await;
    let ts = now_ns();
    seed(&client, &db, ts, STREAMS, &all_rows()).await;
    let full = LogQlEngine::new(data_client(&db).await, engine_config(&db));
    // One byte: no fragment this corpus can render fits.
    let starved = LogQlEngine::new(data_client(&db).await, engine_config(&db))
        .with_metadata_fragment_budget(1);

    // AC12 — the streams route. The same answer, and never a rejection.
    for (query, want) in LOWERED {
        let got = entries(&starved, query, &window(ts)).await;
        assert_eq!(
            got, *want,
            "{query}: the budget fallback must answer exactly what the lowered route answers"
        );
    }

    // AC15 — the instant, database-aggregated route. Omitting the
    // fragment with no client stage counts every row the window admits,
    // one series per distinct metadata text; the restored stage gives one
    // series with value 1.
    let count =
        format!(r#"count_over_time({{service_name="checkout"}} | trace_id="{TRACE_1}" [5m])"#);
    let want = vector(&full, &count, &instant(ts)).await;
    assert_eq!(
        want,
        vec![("{env=prod,pod=checkout-7d9f6c5b4-xk2pq,service_name=checkout,trace_id=740e2a1b9c3d4f5061728394a5b6c7d8}".to_string(), 1.0)],
        "the control: the lowered route's answer"
    );
    assert_eq!(
        vector(&starved, &count, &instant(ts)).await,
        want,
        "the instant database-aggregated route must restore its client stage, not omit the \
         fragment"
    );

    // AC16 — the bucketed range route, `[1m]` against a 60s step.
    let ranged =
        format!(r#"count_over_time({{service_name="checkout"}} | trace_id="{TRACE_1}" [1m])"#);
    let range_params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: ts,
            end_ns: ts + 60_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Forward,
    };
    let want = matrix(&full, &ranged, &range_params).await;
    assert_eq!(want.len(), 1, "the control: one series, {want:?}");
    assert_eq!(
        matrix(&starved, &ranged, &range_params).await,
        want,
        "the bucketed range route must restore the same stage — the empty-pipeline fallback \
         filters nothing, so it would count every line in the window"
    );
    drop_db(&db).await;
}
