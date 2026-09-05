//! Env-gated spanSet-`attributes` differential (issues #193, #510) — a
//! REAL two-system comparison of what a grouped, coalesced or aggregated
//! response carries.
//!
//! For each fixture it ingests the SAME OTLP/JSON bytes into both systems
//! and reads the response back live:
//!
//!   * **PulsusDB side** — the corpus bytes go through OUR OWN ingest
//!     parser (`pulsus_write::protocols::otlp_traces::decode_json` +
//!     `parse_traces`) into the real trace writer against a throwaway
//!     ClickHouse database, and the answer comes from this crate's REAL
//!     two-phase search executor ([`TraceEngine::search`]). A differential
//!     whose two sides are written separately compares two authors, not
//!     two systems — and here the STORED TYPE is the thing under test, so
//!     hand-writing `trace_attrs_idx` rows would let the fixture choose
//!     the answer.
//!   * **Reference side** — the same bytes are pushed to the pinned
//!     reference's OTLP receiver and the spanSets are read back from its
//!     live search route with the identical `q=`.
//!
//! ## What is compared
//!
//! Per query, the SET of span-set shapes. One shape is:
//!
//! ```text
//! <attributes> | m<matched> | <member span ids>
//! ```
//!
//! where `<attributes>` is the ORDERED list of `key=arm=value` tokens, or
//! `-` when the span set carries no `attributes` key at all. The arm is
//! the wire arm — `stringValue` / `intValue` / `doubleValue` /
//! `boolValue` — so `intValue=3` never compares equal to `doubleValue=3`
//! or `stringValue=3`. Our side's token is built from
//! [`pulsus_read::wire_arm`], the SAME decision the response encoder
//! renders from, so this leg cannot pass on an arm the wire does not
//! carry.
//!
//! **The ORDER within a span set is compared; the order BETWEEN span sets
//! is not.** The contributor sequence is the query's own stage order on
//! both systems, so it is asserted. The reference's grouping iterates a
//! hash map, so the group order is not a specification and the shapes are
//! compared as a SET. The reference's matched-span attribute order varies
//! between reads too, so per-span attributes are SORTED before comparing.
//!
//! ## Where the two systems deliberately differ
//!
//! A fixture carrying `theirs` is a pinned divergence: BOTH answers are
//! literal, and the fixture fails if the two ever AGREE — which is the
//! signal that the owning work landed and the pin must retire with it.
//! Two causes, both recorded in
//! `docs/benchmarks/traces-differential-ledger.md`:
//!
//!   * an attribute aggregate computes on `val_num`, so an integer past
//!     2^53 is one digit out (a by-key renders from the exact stored text
//!     and is not);
//!   * the reference's mixed-type `sum`/`avg` is order-dependent and we
//!     compute the true sum (entry 24 of
//!     `docs/reference-defects-we-do-not-copy.md`).
//!
//! A third cause retired with this change: our aggregate used to run over
//! the whole trace BEFORE grouping, so a `by()`-then-aggregate query
//! reported the trace total. The ordered pipeline fold (#492 item 2)
//! fixed it, `by_then_count`, `by_then_count_then_coalesce` and
//! `by_agg_by` are parity fixtures again, and the ledger row is a
//! withdrawal record.
//!
//! ## Corpus isolation on a shared oracle
//!
//! This suite runs against the container the syntax and `compare()` legs
//! share, AFTER them in the job. Every corpus below carries its own
//! random trace id and its own `resource.service.name`, and every query
//! is scoped to that service, so a fixture can only see its own spans and
//! nothing this suite pushes can be picked out of another leg's answer by
//! value.
//!
//! ## The window comes from the corpus, never from a fresh clock
//!
//! Both sides are asked for a window derived from the ONE base instant
//! the corpus is written at. A window taken from a second `now` read can
//! land on a different UTC day from the data it queries, which reddens
//! for an hour a day and passes the rest.
//!
//! Gate: skips unless `PULSUS_TEST_CLICKHOUSE=1` AND
//! `PULSUSDB_GROUPING_DIFF_URL` (the reference's search API base) AND
//! `PULSUSDB_GROUPING_OTLP_URL` (its OTLP HTTP base) are all set. Run
//! locally:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=19124 \
//!   PULSUSDB_GROUPING_DIFF_URL=http://localhost:3200 \
//!   PULSUSDB_GROUPING_OTLP_URL=http://localhost:4318 \
//!   cargo test -p pulsus-read --test traces_search_grouping_differential -- --nocapture
//! ```
//!
//! Clean-room: no reference source, grammar or test corpus is read — the
//! fixtures are our own authorship and the reference's values are read
//! back as black-box runtime output.
//!
//! **Fail-closed on all three gates** (issue #458 recorded the hole,
//! issue #492 part 3 closed it here). Both endpoint URLs go through
//! `pulsus_testkit::require_live_endpoint_gate`, not the boolean gate: a
//! URL-valued variable read by the boolean rule looks "not set" while the
//! `env:` block is right there in the log. Before this, the URLs were read
//! with a bare `env::var` and taken as a skip, so dropping only them from
//! a live step reported GREEN having compared nothing. Ledger entry
//! `traceql-differential-legs-skip-green-on-a-missing-endpoint` records
//! the class and which suites still carry it.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings};
use pulsus_config::WriterConfig;
use pulsus_read::traces::search_plan::{SearchParams, plan_search};
use pulsus_read::{GroupValue, TraceEngine, TraceReadConfig, wire_arm};
use pulsus_schema::{RenderCtx, run_init};
use pulsus_write::TraceSink;
use pulsus_write::writer::{ChBlockInserter, TraceWriter, TraceWriterTables};

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
// The corpora
// ---------------------------------------------------------------------------

/// One span of one corpus. `attrs` are literal OTLP/JSON `AnyValue`
/// objects, so the fixture states the WIRE form the sender used and both
/// systems decode the identical bytes — which is the whole point when the
/// stored type is what is under test.
struct SpanDef {
    /// The span id's low byte; `01`, `02`, … on the wire.
    id: u8,
    /// The parent span's low byte; `0` = a root, which emits no
    /// `parentSpanId` at all.
    parent: u8,
    name: &'static str,
    duration_ns: i64,
    /// OTLP StatusCode (0 unset / 1 ok / 2 error).
    status: i32,
    /// OTLP SpanKind (1 internal / 2 server / 3 client / …). The issue
    /// #492 ordering corpus groups on it, so it must reach the wire.
    kind: i32,
    /// `(key, AnyValue JSON)` pairs, in PUSH ORDER — which is what the
    /// mixed-type fixtures vary.
    attrs: &'static [(&'static str, &'static str)],
}

const fn span(id: u8, name: &'static str) -> SpanDef {
    SpanDef {
        id,
        parent: 0,
        name,
        duration_ns: 1_000_000_000,
        status: 0,
        kind: 1,
        attrs: &[],
    }
}

impl SpanDef {
    const fn dur(mut self, duration_ns: i64) -> Self {
        self.duration_ns = duration_ns;
        self
    }
    const fn status(mut self, status: i32) -> Self {
        self.status = status;
        self
    }
    const fn kind(mut self, kind: i32) -> Self {
        self.kind = kind;
        self
    }
    const fn attrs(mut self, attrs: &'static [(&'static str, &'static str)]) -> Self {
        self.attrs = attrs;
        self
    }
    const fn parent(mut self, parent: u8) -> Self {
        self.parent = parent;
        self
    }
}

/// One corpus: a single trace under its own `resource.service.name`.
struct Corpus {
    /// The `resource.service.name` every query below scopes on, and the
    /// per-corpus isolation this suite relies on when the oracle is
    /// shared.
    service: &'static str,
    spans: &'static [SpanDef],
}

const SEC: i64 = 1_000_000_000;

/// The base corpus: two `alpha` spans and one `beta`, carrying an int and
/// a double attribute each. Q1–Q5 and Q7 of the plan run on it.
const A: &[SpanDef] = &[
    span(1, "alpha").dur(3 * SEC / 2).attrs(&[
        ("n", r#"{"intValue":"3"}"#),
        ("f", r#"{"doubleValue":1.5}"#),
    ]),
    span(2, "alpha").dur(2 * SEC).attrs(&[
        ("n", r#"{"intValue":"5"}"#),
        ("f", r#"{"doubleValue":2.5}"#),
    ]),
    span(3, "beta").dur(3 * SEC).attrs(&[
        ("n", r#"{"intValue":"7"}"#),
        ("f", r#"{"doubleValue":0.5}"#),
    ]),
];

/// One span carries NO value for the aggregated key: the divisor of
/// `avg(.n)` must be 2 and the group value of `by(.n)` must include the
/// reference's `nil` bucket.
const B: &[SpanDef] = &[
    span(1, "g").attrs(&[("n", r#"{"intValue":"10"}"#)]),
    span(2, "g"),
    span(3, "h").attrs(&[("n", r#"{"intValue":"4"}"#)]),
];

/// Sub-millisecond durations: `avg(duration)` must truncate to
/// `666.666µs`, with the non-ASCII `µ`.
const C: &[SpanDef] = &[
    span(1, "z").dur(1),
    span(2, "z").dur(999_999),
    span(3, "z").dur(1_000_000),
];

/// A string and a bool attribute.
const E: &[SpanDef] = &[
    span(1, "k").attrs(&[
        ("s", r#"{"stringValue":"apple"}"#),
        ("b", r#"{"boolValue":true}"#),
    ]),
    span(2, "k").attrs(&[
        ("s", r#"{"stringValue":"pear"}"#),
        ("b", r#"{"boolValue":false}"#),
    ]),
];

/// A STRING that parses as a number — the case `val_type` exists for —
/// beside a span carrying no value at all.
const H: &[SpanDef] = &[
    span(1, "p").attrs(&[("sn", r#"{"stringValue":"8080"}"#)]),
    span(2, "p"),
];

/// Signed zero: `-0.0` and `+0.0` are different groups.
const J: &[SpanDef] = &[
    span(1, "q").attrs(&[("f", r#"{"doubleValue":-0.0}"#)]),
    span(2, "q").attrs(&[("f", r#"{"doubleValue":0.0}"#)]),
    span(3, "q").attrs(&[("f", r#"{"doubleValue":-0.0}"#)]),
];

/// Non-finite doubles in their protojson string form — the values whose
/// `val_num` is NULL in our store, so the key falls to the string branch
/// unless the stored TYPE is read.
const K: &[SpanDef] = &[
    span(1, "r").attrs(&[("f", r#"{"doubleValue":"NaN"}"#)]),
    span(2, "r").attrs(&[("f", r#"{"doubleValue":"Infinity"}"#)]),
    span(3, "r").attrs(&[("f", r#"{"doubleValue":"-Infinity"}"#)]),
];

/// An integer past the point where a double is exact.
const L: &[SpanDef] = &[
    span(1, "t").attrs(&[("big", r#"{"intValue":"9007199254740993"}"#)]),
    span(2, "t").attrs(&[("big", r#"{"intValue":"-5"}"#)]),
];

/// Two grouping keys either side of an aggregate: three `a` spans with
/// two statuses, one `b` span.
const M: &[SpanDef] = &[
    span(1, "a").status(1),
    span(2, "a").status(2),
    span(3, "a").status(2),
    span(4, "b").status(1),
];

/// A root and its child, so the query-time nested-set numbering has an
/// unambiguous answer both systems agree on.
const P: &[SpanDef] = &[span(1, "root"), span(2, "child").parent(1)];

/// Corpus C-ORD1 (issue #492 item 2): three spans named `a` with
/// durations 0.5s / 2s / 3s and one named `b` with 0.4s, every one a
/// child of the first. `count() > 2` keeps the `a` group and drops the
/// `b` one, so the written position of the aggregate decides the answer.
const ORD1: &[SpanDef] = &[
    span(1, "a").dur(SEC / 2),
    span(2, "a").dur(2 * SEC).parent(1),
    span(3, "a").dur(3 * SEC).parent(1),
    span(4, "b").dur(2 * SEC / 5).parent(1),
];

/// Corpus C-ORD2 (issue #492 item 2): CROSS-CUTTING `name` / `kind` keys
/// — (a, server), (a, client), (b, server), (b, server). Neither key
/// alone separates what the other separates, which is what makes a
/// second `by()` that sub-divides distinguishable from one that rebuilds
/// from the matched set.
const ORD2: &[SpanDef] = &[
    span(1, "a").kind(2),
    span(2, "a").kind(3),
    span(3, "b").kind(2),
    span(4, "b").kind(2),
];

/// The mixed-type pair: identical values, OPPOSITE push order. One order
/// cannot show order dependence, which is the entire content of the
/// reference defect these two record.
const N: &[SpanDef] = &[
    span(1, "u").attrs(&[("v", r#"{"intValue":"2"}"#)]),
    span(2, "u").attrs(&[("v", r#"{"doubleValue":3.5}"#)]),
];
const O: &[SpanDef] = &[
    span(1, "u").attrs(&[("v", r#"{"doubleValue":3.5}"#)]),
    span(2, "u").attrs(&[("v", r#"{"intValue":"2"}"#)]),
];

fn corpora() -> Vec<Corpus> {
    vec![
        Corpus {
            service: "s510a",
            spans: A,
        },
        Corpus {
            service: "s510b",
            spans: B,
        },
        Corpus {
            service: "s510c",
            spans: C,
        },
        Corpus {
            service: "s510e",
            spans: E,
        },
        Corpus {
            service: "s510h",
            spans: H,
        },
        Corpus {
            service: "s510j",
            spans: J,
        },
        Corpus {
            service: "s510k",
            spans: K,
        },
        Corpus {
            service: "s510l",
            spans: L,
        },
        Corpus {
            service: "s510m",
            spans: M,
        },
        Corpus {
            service: "s510p",
            spans: P,
        },
        Corpus {
            service: "s510n",
            spans: N,
        },
        Corpus {
            service: "s510o",
            spans: O,
        },
        Corpus {
            service: "grp492",
            spans: ORD1,
        },
        Corpus {
            service: "grp492b",
            spans: ORD2,
        },
    ]
}

// ---------------------------------------------------------------------------
// The fixtures
// ---------------------------------------------------------------------------

/// One differential case.
struct Fixture {
    name: &'static str,
    /// The corpus's `resource.service.name`; the query is scoped to it.
    service: &'static str,
    /// The pipeline written AFTER the service scope, e.g. `| count() > 1`.
    pipeline: &'static str,
    /// Compare the per-span `attributes` too (the matched-span projection
    /// surface). Off elsewhere so a span-set fixture's literals stay
    /// about the span set.
    span_attrs: bool,
    /// `true` when a `coalesce()` is the last grouping-affecting stage,
    /// so the answer is ONE span set carrying no `by(...)` attribute.
    /// Carried on the fixture rather than derived from the query, because
    /// it is one of the four fields
    /// `the_six_ordering_fixtures_are_exactly_the_frozen_records` freezes
    /// (issue #492 item 2).
    coalesced: bool,
    /// Our REQUIRED answer, as span-set shapes.
    ours: &'static [&'static str],
    /// `None` — the reference must give the same shapes. `Some(..)` — a
    /// pinned divergence: the reference's own shapes, which must differ
    /// from `ours`.
    theirs: Option<&'static [&'static str]>,
    /// Why the two differ, and what retires the pin. Empty for parity.
    divergence: &'static str,
}

const fn parity(
    name: &'static str,
    service: &'static str,
    pipeline: &'static str,
    ours: &'static [&'static str],
) -> Fixture {
    Fixture {
        name,
        service,
        pipeline,
        span_attrs: false,
        coalesced: false,
        ours,
        theirs: None,
        divergence: "",
    }
}

/// A parity fixture whose answer is one flat span set because a
/// `coalesce()` is its last grouping-affecting stage.
const fn parity_coalesced(
    name: &'static str,
    service: &'static str,
    pipeline: &'static str,
    ours: &'static [&'static str],
) -> Fixture {
    Fixture {
        name,
        service,
        pipeline,
        span_attrs: false,
        coalesced: true,
        ours,
        theirs: None,
        divergence: "",
    }
}

const fn diverges(
    name: &'static str,
    service: &'static str,
    pipeline: &'static str,
    ours: &'static [&'static str],
    theirs: &'static [&'static str],
    divergence: &'static str,
) -> Fixture {
    Fixture {
        name,
        service,
        pipeline,
        span_attrs: false,
        coalesced: false,
        ours,
        theirs: Some(theirs),
        divergence,
    }
}

const fn projected(
    name: &'static str,
    service: &'static str,
    pipeline: &'static str,
    ours: &'static [&'static str],
) -> Fixture {
    Fixture {
        name,
        service,
        pipeline,
        span_attrs: true,
        coalesced: false,
        ours,
        theirs: None,
        divergence: "",
    }
}

/// The divergence texts, written once so a fixture and its ledger row
/// cannot drift apart.
///
/// `traceql-spanset-aggregate-precedes-grouping` is gone from this list:
/// the ordered fold landed, the three fixtures it covered now AGREE, and
/// the ledger row is a withdrawal record.
const PRECISION_LIMIT: &str = "an attribute aggregate computes on val_num — ledger row \
     traceql-attribute-aggregate-float64-precision";
const MIXED_TYPE: &str = "the reference's mixed-type sum/avg is order-dependent — ledger row \
     traceql-spanset-aggregate-mixed-type-attribute, entry 24 of \
     docs/reference-defects-we-do-not-copy.md";

#[rustfmt::skip]
fn fixtures() -> Vec<Fixture> {
    vec![
        // ---- every aggregate contributes one attribute, flat ----------
        parity("flat_count", "s510a", "| count() > 1",
            &["count()=intValue=3 | m3 | 01,02,03"]),
        parity("flat_min_duration", "s510a", "| min(duration) > 1ms",
            &["min(duration)=stringValue=1.5s | m3 | 01,02,03"]),
        parity("flat_max_duration", "s510a", "| max(duration) > 1ms",
            &["max(duration)=stringValue=3s | m3 | 01,02,03"]),
        parity("flat_sum_duration", "s510a", "| sum(duration) > 1ms",
            &["sum(duration)=stringValue=6.5s | m3 | 01,02,03"]),
        // The truncating integer nanosecond division: 6 500 000 000 / 3.
        // A `2.1666666666666665s` is the f64 division and fails.
        parity("flat_avg_duration", "s510a", "| avg(duration) > 1ms",
            &["avg(duration)=stringValue=2.166666666s | m3 | 01,02,03"]),
        // Sub-millisecond, with the non-ASCII µ: 2 000 000 / 3.
        parity("flat_avg_duration_submilli", "s510c", "| avg(duration) > 0",
            &["avg(duration)=stringValue=666.666µs | m3 | 01,02,03"]),
        parity("flat_min_attr_int", "s510a", "| min(.n) > 0",
            &["min(.n)=intValue=3 | m3 | 01,02,03"]),
        parity("flat_max_attr_int", "s510a", "| max(.n) > 0",
            &["max(.n)=intValue=7 | m3 | 01,02,03"]),
        parity("flat_sum_attr_int", "s510a", "| sum(.n) > 0",
            &["sum(.n)=intValue=15 | m3 | 01,02,03"]),
        // `avg` is a double even over an int attribute.
        parity("flat_avg_attr_int", "s510a", "| avg(.n) > 0",
            &["avg(.n)=doubleValue=5 | m3 | 01,02,03"]),
        parity("flat_min_attr_float", "s510a", "| min(.f) > 0",
            &["min(.f)=doubleValue=0.5 | m3 | 01,02,03"]),
        parity("flat_sum_attr_float", "s510a", "| sum(.f) > 0",
            &["sum(.f)=doubleValue=4.5 | m3 | 01,02,03"]),
        parity("flat_avg_attr_float", "s510a", "| avg(.f) > 0",
            &["avg(.f)=doubleValue=1.5 | m3 | 01,02,03"]),
        // The divisor is 2, not 3: the span carrying no `n` is skipped by
        // the aggregate and stays in the span set. A `4.666…` means it
        // was counted.
        parity("flat_avg_attr_absent_contributor", "s510b", "| avg(.n) > 0",
            &["avg(.n)=doubleValue=7 | m3 | 01,02,03"]),

        // ---- the list is the ordered record of the STAGES --------------
        parity("agg_then_by", "s510a", "| count() > 1 | by(name)", &[
            "count()=intValue=3,by(name)=stringValue=alpha | m2 | 01,02",
            "count()=intValue=3,by(name)=stringValue=beta | m1 | 03",
        ]),
        parity("max_duration_then_by", "s510a", "| max(duration) > 1ms | by(name)", &[
            "max(duration)=stringValue=3s,by(name)=stringValue=alpha | m2 | 01,02",
            "max(duration)=stringValue=3s,by(name)=stringValue=beta | m1 | 03",
        ]),
        // TWO entries, both `count()`. A deduplicated single entry fails.
        parity("duplicate_count", "s510a", "| count() > 1 | count() > 2",
            &["count()=intValue=3,count()=intValue=3 | m3 | 01,02,03"]),
        // `coalesce()` clears the list: a flat span set with NO
        // `attributes` key at all, holding the spans that SURVIVED the
        // filter rather than every matched one. This was a pinned
        // divergence until the ordered fold landed; both sides now say
        // `m2`.
        parity_coalesced("by_then_count_then_coalesce", "s510a",
            "| by(name) | count() > 1 | coalesce()",
            &["- | m2 | 01,02"]),
        // …and an aggregate AFTER the `coalesce()` contributes again.
        parity_coalesced("coalesce_then_count", "s510a", "| by(name) | coalesce() | count() > 1",
            &["count()=intValue=3 | m3 | 01,02,03"]),
        // The aggregate at its WRITTEN position filters the GROUPS: the
        // `beta` group holds one span and is dropped, and the surviving
        // group's `count()` is its OWN two members, not the trace's
        // three. Both were a pinned divergence before the ordered fold.
        parity("by_then_count", "s510a", "| by(name) | count() > 1",
            &["by(name)=stringValue=alpha,count()=intValue=2 | m2 | 01,02"]),
        // A later `by()` SUB-DIVIDES: three span sets, both grouping
        // contributors, in written order. Two span sets, or a first entry
        // keyed `by(status)`, is the walk that replaces instead of
        // extending. The `count()` is each `by(name)` group's own size —
        // 3 for `a` and 1 for `b` — which is what the whole-trace
        // aggregate got wrong.
        parity("by_agg_by", "s510m", "| by(name) | count() > 0 | by(status)",
            &[
                "by(name)=stringValue=a,count()=intValue=3,by(status)=stringValue=ok | m1 | 01",
                "by(name)=stringValue=a,count()=intValue=3,by(status)=stringValue=error | m2 | 02,03",
                "by(name)=stringValue=b,count()=intValue=1,by(status)=stringValue=ok | m1 | 04",
            ]),

        // ---- issue #492 item 2: the WRITTEN order of the stages -------
        //
        // Six orderings whose SQL is byte-identical in pairs, so only the
        // evaluator can tell them apart. Two of them must NOT move
        // (`count() > 2 | by(name)` and `by(name) | coalesce() | count()`),
        // which is the half a fix for the other four can silently break.
        parity("ord_by_then_count_filters_the_groups", "grp492",
            "| by(name) | count() > 2",
            &["by(name)=stringValue=a,count()=intValue=3 | m3 | 01,02,03"]),
        parity("ord_count_then_by_is_unchanged", "grp492",
            "| count() > 2 | by(name)",
            &[
                "count()=intValue=4,by(name)=stringValue=a | m3 | 01,02,03",
                "count()=intValue=4,by(name)=stringValue=b | m1 | 04",
            ]),
        parity_coalesced("ord_coalesce_merges_the_survivors", "grp492",
            "| by(name) | count() > 2 | coalesce()",
            &["- | m3 | 01,02,03"]),
        parity_coalesced("ord_coalesce_before_the_filter_is_unchanged", "grp492",
            "| by(name) | coalesce() | count() > 2",
            &["count()=intValue=4 | m4 | 01,02,03,04"]),
        parity("ord_nested_by_name_then_kind", "grp492b",
            "| by(name) | by(kind)",
            &[
                "by(name)=stringValue=a,by(kind)=stringValue=server | m1 | 01",
                "by(name)=stringValue=a,by(kind)=stringValue=client | m1 | 02",
                "by(name)=stringValue=b,by(kind)=stringValue=server | m2 | 03,04",
            ]),
        parity("ord_nested_by_kind_then_name", "grp492b",
            "| by(kind) | by(name)",
            &[
                "by(kind)=stringValue=server,by(name)=stringValue=a | m1 | 01",
                "by(kind)=stringValue=client,by(name)=stringValue=a | m1 | 02",
                "by(kind)=stringValue=server,by(name)=stringValue=b | m2 | 03,04",
            ]),

        // ---- an attribute by-key renders in the STORED type's arm ------
        parity("by_attr_int", "s510a", "| by(.n)", &[
            "by(.n)=intValue=3 | m1 | 01",
            "by(.n)=intValue=5 | m1 | 02",
            "by(.n)=intValue=7 | m1 | 03",
        ]),
        parity("by_attr_float", "s510a", "| by(.f)", &[
            "by(.f)=doubleValue=1.5 | m1 | 01",
            "by(.f)=doubleValue=2.5 | m1 | 02",
            "by(.f)=doubleValue=0.5 | m1 | 03",
        ]),
        parity("by_attr_bool", "s510e", "| by(.b)", &[
            "by(.b)=boolValue=true | m1 | 01",
            "by(.b)=boolValue=false | m1 | 02",
        ]),
        parity("by_attr_string", "s510e", "| by(.s)", &[
            "by(.s)=stringValue=apple | m1 | 01",
            "by(.s)=stringValue=pear | m1 | 02",
        ]),
        // The adversarial one: a STRING that parses as a number, beside
        // the absent-key bucket.
        parity("by_attr_numeric_string_and_absent", "s510h", "| by(.sn)", &[
            "by(.sn)=stringValue=8080 | m1 | 01",
            "by(.sn)=stringValue=nil | m1 | 02",
        ]),
        parity("by_attr_absent", "s510b", "| by(.n)", &[
            "by(.n)=intValue=10 | m1 | 01",
            "by(.n)=stringValue=nil | m1 | 02",
            "by(.n)=intValue=4 | m1 | 03",
        ]),
        // `-0.0` and `+0.0` are DIFFERENT groups.
        parity("by_attr_signed_zero", "s510j", "| by(.f)", &[
            "by(.f)=doubleValue=0 | m1 | 02",
            "by(.f)=doubleValue=-0 | m2 | 01,03",
        ]),
        // The `-Infinity` member is adversarial twice over: our stored
        // text is `-inf` and our numeric column is NULL, so a wrong arm
        // AND a wrong spelling are both reachable.
        parity("by_attr_non_finite", "s510k", "| by(.f)", &[
            "by(.f)=doubleValue=NaN | m1 | 01",
            "by(.f)=doubleValue=Infinity | m1 | 02",
            "by(.f)=doubleValue=-Infinity | m1 | 03",
        ]),
        // Rendered from the exact stored text, so the digits survive.
        parity("by_attr_int_beyond_2_53", "s510l", "| by(.big)", &[
            "by(.big)=intValue=9007199254740993 | m1 | 01",
            "by(.big)=intValue=-5 | m1 | 02",
        ]),
        // …and the AGGREGATE path is one digit out, deliberately.
        diverges("max_attr_beyond_2_53", "s510l", "| max(.big) > 0",
            &["max(.big)=intValue=9007199254740992 | m2 | 01,02"],
            &["max(.big)=intValue=9007199254740993 | m2 | 01,02"],
            PRECISION_LIMIT),

        // ---- the reference's order-dependent mixed-type arithmetic ----
        diverges("mixed_type_int_first_sum", "s510n", "| sum(.v) > 0",
            &["sum(.v)=doubleValue=5.5 | m2 | 01,02"],
            &["sum(.v)=intValue=2 | m2 | 01,02"],
            MIXED_TYPE),
        diverges("mixed_type_float_first_sum", "s510o", "| sum(.v) > 0",
            &["sum(.v)=doubleValue=5.5 | m2 | 01,02"],
            &["sum(.v)=doubleValue=3.5 | m2 | 01,02"],
            MIXED_TYPE),
        diverges("mixed_type_int_first_avg", "s510n", "| avg(.v) > 0",
            &["avg(.v)=doubleValue=2.75 | m2 | 01,02"],
            &["avg(.v)=doubleValue=1 | m2 | 01,02"],
            MIXED_TYPE),
        diverges("mixed_type_float_first_avg", "s510o", "| avg(.v) > 0",
            &["avg(.v)=doubleValue=2.75 | m2 | 01,02"],
            &["avg(.v)=doubleValue=1.75 | m2 | 01,02"],
            MIXED_TYPE),

        // ---- the matched-span projection, the third surface ------------
        // The condition's own field is projected beside `service.name`,
        // which the service scope collects on both systems.
        projected("projected_int", "s510a", "&& .n = 3",
            &["- | m1 | 01{n=intValue=3,service.name=stringValue=s510a}"]),
        projected("projected_double", "s510a", "&& .f = 1.5",
            &["- | m1 | 01{f=doubleValue=1.5,service.name=stringValue=s510a}"]),
        projected("projected_bool", "s510e", "&& .b = true",
            &["- | m1 | 01{b=boolValue=true,service.name=stringValue=s510e}"]),
        // The CONTROL: a string attribute is a `stringValue` on both
        // systems before and after this change, so it stays green when
        // the renderer hard-codes the string arm. It is here beside the
        // typed cases to show the typed ones are what discriminate.
        projected("projected_string", "s510h", "&& .sn = \"8080\"",
            &["- | m1 | 01{service.name=stringValue=s510h,sn=stringValue=8080}"]),
        projected("projected_int_beyond_2_53", "s510l", "&& .big = 9007199254740993",
            &["- | m1 | 01{big=intValue=9007199254740993,service.name=stringValue=s510l}"]),
        // A query-time NUMBER: the same `intValue` arm `by(nestedSetLeft)`
        // resolves it into, which the projection rendered as a string
        // before issue #510.
        projected("projected_nested_set", "s510p", "&& nestedSetLeft > 0", &[
            "- | m2 | \
             01{nestedSetLeft=intValue=1,service.name=stringValue=s510p},\
             02{nestedSetLeft=intValue=2,service.name=stringValue=s510p}",
        ]),
        projected("projected_non_finite", "s510k", "| select(.f)", &[
            "- | m3 | \
             01{f=doubleValue=NaN,service.name=stringValue=s510k},\
             02{f=doubleValue=Infinity,service.name=stringValue=s510k},\
             03{f=doubleValue=-Infinity,service.name=stringValue=s510k}",
        ]),
    ]
}

/// The full TraceQL text of a fixture: the service scope, then whatever
/// the fixture writes after it (a `&&` conjunct or a pipeline stage).
fn query_of(fx: &Fixture) -> String {
    let service = fx.service;
    if let Some(conjunct) = fx.pipeline.strip_prefix("&&") {
        format!(r#"{{resource.service.name="{service}" &&{conjunct}}}"#)
    } else {
        format!(r#"{{resource.service.name="{service}"}} {}"#, fx.pipeline)
    }
}

// ---------------------------------------------------------------------------
// The comparison shape
// ---------------------------------------------------------------------------

/// One span set reduced to its comparable shape. `Ord` so a whole
/// response is a `BTreeSet`: the reference's group ORDER is a hash-map
/// iteration order and is not a specification, while the contributor
/// order WITHIN a span set is the query's own and is compared.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SetShape(String);

impl SetShape {
    /// `<attrs> | m<matched> | <members>`; `-` for a span set carrying no
    /// `attributes` key at all, which is a DIFFERENT body from an empty
    /// array and is what the reference answers after a `coalesce()`.
    ///
    /// **The members are SORTED, the attributes are not.** The
    /// contributor sequence is the query's own stage order on both
    /// systems, so it is compared. The span order WITHIN a span set is
    /// not a specification on the reference side: measured, one
    /// `coalesce()`d span set came back `03,01,02` on one read and
    /// `01,02,03` on another over the same corpus. Ours is deterministic
    /// (ascending `(timestamp_ns, span_id)`, `search_eval.rs`); asserting
    /// the reference's would be asserting an artefact, and it reddened a
    /// break test for a reason unrelated to the break.
    ///
    /// **The sort is a HARNESS NORMALISATION, not a behaviour of ours,
    /// and it is applied to both sides.** No production path sorts a span
    /// set's members into this order — ours are already in
    /// `(timestamp_ns, span_id)` order when the fold hands them over —
    /// so there is nothing here for a test to cover and no assertion to
    /// add. A hermetic check that `SetShape::new` given `02,01` renders
    /// `01,02` would restate this line, not test anything.
    ///
    /// **Deleting it does not make a test fail; it makes one FLAKY.** The
    /// order it absorbs varies between reads of the same corpus, so a
    /// single run with the sort removed comes back green most of the
    /// time, and that green says nothing. What established that the sort
    /// is load-bearing is repetition: after it was added, three
    /// consecutive full runs of this suite reported 34 fixtures agreeing
    /// and 8 pinned divergences, each time. Before it, a run reddened
    /// `coalesce_then_count` on the member order alone while the code
    /// under test had not moved.
    fn new(attrs: Option<Vec<String>>, matched: u64, members: Vec<String>) -> Self {
        let attrs = match attrs {
            None => "-".to_string(),
            Some(list) => list.join(","),
        };
        let mut members = members;
        // The normalisation described above: both sides, every span set.
        // Removing this produces flakiness, not a failure — see the doc
        // comment before repeating that break.
        members.sort();
        SetShape(format!("{attrs} | m{matched} | {}", members.join(",")))
    }
}

fn render(shapes: &BTreeSet<SetShape>) -> String {
    shapes
        .iter()
        .map(|s| format!("\n    {}", s.0))
        .collect::<String>()
}

fn expected(list: &[&str]) -> BTreeSet<SetShape> {
    list.iter().map(|s| SetShape(s.to_string())).collect()
}

// ---------------------------------------------------------------------------
// PulsusDB side
// ---------------------------------------------------------------------------

/// Our typed value as the comparison token, through
/// [`pulsus_read::wire_arm`] — the SAME arm decision the response encoder
/// renders from, so this leg cannot agree on an arm the wire does not
/// carry.
fn our_token(value: &GroupValue) -> String {
    let (arm, text) = wire_arm(value);
    format!("{arm}={text}")
}

async fn ours(
    engine: &TraceEngine,
    q: &str,
    window: (i64, i64),
    span_attrs: bool,
) -> BTreeSet<SetShape> {
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
    let mut shapes = BTreeSet::new();
    for t in &out.traces {
        let member = |s: &pulsus_read::SpanSummary| {
            let id = format!("{:02x}", s.span_id[7]);
            if !span_attrs {
                return id;
            }
            // SORTED: the reference's matched-span attribute order varies
            // between reads (ledger row
            // `traceql-matched-span-attribute-order`), so order is not a
            // parity claim on this axis.
            let mut pairs: Vec<String> = s
                .attributes
                .iter()
                .map(|a| format!("{}={}", a.key(), our_token(a.value())))
                .collect();
            pairs.sort();
            format!("{id}{{{}}}", pairs.join(","))
        };
        match &t.groups {
            Some(groups) => {
                for g in groups {
                    shapes.insert(SetShape::new(
                        Some(
                            g.attributes
                                .iter()
                                .map(|(k, v)| format!("{k}={}", our_token(v)))
                                .collect(),
                        ),
                        u64::from(g.matched),
                        g.spans.iter().map(&member).collect(),
                    ));
                }
            }
            // A span set carrying NO attributes at all — the shape the
            // reference answers after a `coalesce()`. Every span set that
            // does carry attributes arrives through `groups`, including
            // the one-entry list a flat aggregate produces.
            None => {
                shapes.insert(SetShape::new(
                    None,
                    u64::from(t.matched),
                    t.spans.iter().map(&member).collect(),
                ));
            }
        }
    }
    shapes
}

// ---------------------------------------------------------------------------
// Reference side
// ---------------------------------------------------------------------------

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn sid_hex(id: u8) -> String {
    let mut b = [0u8; 8];
    b[7] = id;
    hex(&b)
}

/// One corpus as OTLP/JSON, built ONCE: the reference receives these
/// bytes and our own ingest parser decodes the same ones.
fn corpus_body(corpus: &Corpus, trace: &[u8; 16], base_ns: i64) -> Vec<u8> {
    let spans: Vec<serde_json::Value> = corpus
        .spans
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let start = base_ns + i as i64 * 1_000_000;
            let attrs: Vec<serde_json::Value> = s
                .attrs
                .iter()
                .map(|(k, v)| {
                    serde_json::json!({
                        "key": k,
                        "value": serde_json::from_str::<serde_json::Value>(v)
                            .expect("the fixture's AnyValue literal is JSON"),
                    })
                })
                .collect();
            let mut span = serde_json::json!({
                "traceId": hex(trace),
                "spanId": sid_hex(s.id),
                "name": s.name,
                "startTimeUnixNano": start.to_string(),
                "endTimeUnixNano": (start + s.duration_ns).to_string(),
                "kind": s.kind,
                "status": {"code": s.status},
                "attributes": attrs,
            });
            if s.parent != 0 {
                span["parentSpanId"] = serde_json::Value::String(sid_hex(s.parent));
            }
            span
        })
        .collect();
    serde_json::to_vec(&serde_json::json!({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": corpus.service}}
            ]},
            "scopeSpans": [{"spans": spans}],
        }]
    }))
    .expect("serialise the corpus")
}

fn otlp_push(otlp_base: &str, body: &[u8], service: &str) {
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
            "--data-binary",
            "@-",
        ])
        .arg(&url)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .expect("stdin")
                .write_all(body)
                .and_then(|()| child.wait_with_output())
        })
        .expect("curl on PATH");
    let code = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        code.trim(),
        "200",
        "OTLP push of corpus {service} to {url} failed (http {code})"
    );
}

/// The reference's answer to one query, reduced to the same shapes.
/// `None` when the response carries no `traces` array at all.
fn reference(
    api_base: &str,
    q: &str,
    window_s: (i64, i64),
    span_attrs: bool,
    trace: &[u8; 16],
) -> Option<BTreeSet<SetShape>> {
    let url = format!("{}/api/search", api_base.trim_end_matches('/'));
    let out = Command::new("curl")
        .args(["-s", "-G", "--max-time", "20"])
        .args(["--data-urlencode", &format!("q={q}")])
        .args(["--data-urlencode", &format!("start={}", window_s.0)])
        .args(["--data-urlencode", &format!("end={}", window_s.1)])
        .args(["--data-urlencode", "limit=100"])
        .args(["--data-urlencode", "spss=100"])
        .arg(&url)
        .output()
        .expect("curl on PATH");
    let body: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    let traces = body.get("traces")?.as_array()?;
    let mut shapes = BTreeSet::new();
    let trace_hex = hex(trace);
    for t in traces {
        // THIS run's trace only. A long-lived oracle keeps every corpus
        // ever pushed to it, and the service names here are stable across
        // runs while the trace ids are not — without this filter a second
        // run against the same instance compares the union of both runs.
        // The reference strips leading zero bytes from the id it returns.
        let tid = t.get("traceID").and_then(|v| v.as_str()).unwrap_or("");
        if tid != trace_hex && !trace_hex.trim_start_matches('0').ends_with(tid) {
            continue;
        }
        let sets = t
            .get("spanSets")
            .and_then(|s| s.as_array())
            .map(|v| v.as_slice())
            .or_else(|| t.get("spanSet").map(std::slice::from_ref))
            .unwrap_or_default();
        for set in sets {
            let attrs = set.get("attributes").and_then(|a| a.as_array()).map(|a| {
                a.iter()
                    .map(|kv| {
                        let key = kv.get("key").and_then(|v| v.as_str()).unwrap_or_default();
                        format!("{key}={}", reference_token(kv.get("value")))
                    })
                    .collect::<Vec<_>>()
            });
            let members: Vec<String> = set
                .get("spans")
                .and_then(|s| s.as_array())
                .map(|spans| {
                    spans
                        .iter()
                        .map(|s| {
                            let hexid =
                                s.get("spanID").and_then(|v| v.as_str()).unwrap_or_default();
                            let id = hexid[hexid.len().saturating_sub(2)..].to_string();
                            if !span_attrs {
                                return id;
                            }
                            let mut pairs: Vec<String> = s
                                .get("attributes")
                                .and_then(|a| a.as_array())
                                .map(|a| {
                                    a.iter()
                                        .map(|kv| {
                                            let key = kv
                                                .get("key")
                                                .and_then(|v| v.as_str())
                                                .unwrap_or_default();
                                            format!("{key}={}", reference_token(kv.get("value")))
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();
                            pairs.sort();
                            format!("{id}{{{}}}", pairs.join(","))
                        })
                        .collect()
                })
                .unwrap_or_default();
            let matched = set.get("matched").and_then(|v| v.as_u64()).unwrap_or(0);
            shapes.insert(SetShape::new(attrs, matched, members));
        }
    }
    Some(shapes)
}

/// The reference's `value:{…}` object as the SAME `arm=text` token
/// [`our_token`] builds. WHICH key is populated is the wire arm, so a
/// reference `stringValue "3"` can never match our `intValue 3`.
///
/// A `doubleValue` arrives either as a JSON number or — for a non-finite
/// value — as one of protojson's three literal strings; an `intValue` as
/// a JSON string, with the bare-number form tolerated.
fn reference_token(value: Option<&serde_json::Value>) -> String {
    let Some(value) = value else {
        return "<no value>".to_string();
    };
    for arm in ["stringValue", "intValue", "doubleValue"] {
        if let Some(s) = value.get(arm).and_then(|v| v.as_str()) {
            return format!("{arm}={s}");
        }
    }
    if let Some(n) = value.get("intValue").and_then(|v| v.as_i64()) {
        return format!("intValue={n}");
    }
    if let Some(f) = value.get("doubleValue").and_then(|v| v.as_f64()) {
        return format!("doubleValue={f}");
    }
    if let Some(b) = value.get("boolValue").and_then(|v| v.as_bool()) {
        return format!("boolValue={b}");
    }
    format!("<unrecognised arm {value}>")
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
    // FAIL-CLOSED on all three: dropping any `env:` block from this
    // suite's CI step PANICS rather than skipping green. On a developer
    // machine with no reference container it still skips cleanly — the
    // guard fires only when the gate is missing inside a CI job that
    // exists to supply it.
    let api_gate = pulsus_testkit::require_live_endpoint_gate("PULSUSDB_GROUPING_DIFF_URL");
    let otlp_gate = pulsus_testkit::require_live_endpoint_gate("PULSUSDB_GROUPING_OTLP_URL");
    if !(api_gate.is_running()
        && otlp_gate.is_running()
        && pulsus_testkit::live_clickhouse_enabled())
    {
        eprintln!(
            "skipping the spanSet-attributes differential — set PULSUS_TEST_CLICKHOUSE=1, \
             PULSUSDB_GROUPING_DIFF_URL (the reference's search API) and \
             PULSUSDB_GROUPING_OTLP_URL (its OTLP receiver)."
        );
        return;
    }
    let api_base = std::env::var("PULSUSDB_GROUPING_DIFF_URL").expect("gate is running");
    let otlp_base = std::env::var("PULSUSDB_GROUPING_OTLP_URL").expect("gate is running");

    // ONE base instant for the whole run. Both windows are derived from
    // it, so no comparison can straddle a day boundary the corpus does
    // not.
    let base = now_ns();
    let window = (base - 3600 * SEC, base + 3600 * SEC);
    let window_s = (window.0 / SEC, window.1 / SEC);

    let bootstrap = ChClient::new(ch_config("default"))
        .await
        .expect("connect bootstrap");
    let db = pulsus_testkit::test_db("pulsus_grpdiff_it");
    init_db(&bootstrap, &db).await;
    let client = Arc::new(ChClient::new(ch_config(&db)).await.expect("connect db"));
    let mut cfg = WriterConfig::default();
    cfg.batch_bytes.0 = 1;
    let writer = TraceWriter::with_inserters_with_tables(
        Arc::new(ChBlockInserter::new(client.clone())),
        Arc::new(ChBlockInserter::new(client.clone())),
        &cfg,
        TraceWriterTables::traces_default(),
    );

    // Every corpus carries its own random trace id and its own
    // `resource.service.name`, so nothing this suite pushes can be picked
    // out of another leg's answer by value on the shared oracle.
    let mut trace_of: BTreeMap<&'static str, [u8; 16]> = BTreeMap::new();
    for corpus in corpora() {
        let trace = *uuid::Uuid::new_v4().as_bytes();
        trace_of.insert(corpus.service, trace);
        let body = corpus_body(&corpus, &trace, base);
        // The reference first, so it has the whole poll window to index.
        otlp_push(&otlp_base, &body, corpus.service);
        let req = pulsus_write::protocols::otlp_traces::decode_json(&body)
            .expect("our own ingest decodes the same body the reference got");
        let parsed =
            pulsus_write::parse_traces(&req, base).expect("our own ingest parses the same body");
        assert_eq!(
            parsed.spans.len(),
            corpus.spans.len(),
            "corpus {} lost a span in our own ingest",
            corpus.service
        );
        let wait = writer.admit_flush(parsed).expect("queue has room");
        tokio::time::timeout(Duration::from_secs(20), wait)
            .await
            .expect("flush settles")
            .expect("the corpus commits");
    }

    let engine = TraceEngine::new(
        ChClient::new(ch_config(&db)).await.expect("connect engine"),
        engine_config(),
    );

    // ---- the validity gate, BEFORE any comparison ---------------------
    // Two empty answers are equal, so a fixture issued before either side
    // has indexed its corpus would compare nothing and pass green. The
    // reference serves a trace by ID while its SEARCH route still answers
    // `{"traces":[]}`, so the SEARCH route is what is polled — once per
    // corpus, because "some corpus is visible" does not mean this one is.
    for (service, trace) in &trace_of {
        let q = format!(r#"{{resource.service.name="{service}"}}"#);
        let mut ready = false;
        for _ in 0..60 {
            if reference(&api_base, &q, window_s, false, trace).is_some_and(|m| !m.is_empty()) {
                ready = true;
                break;
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        assert!(
            ready,
            "the reference never returned corpus {service} on its search route within the poll \
             budget — every fixture on it would have compared two empty answers and passed"
        );
        let mut ours_ready = false;
        for _ in 0..60 {
            if !ours(&engine, &q, window, false).await.is_empty() {
                ours_ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(ours_ready, "our own side never returned corpus {service}");
    }

    let mut diverged: Vec<String> = Vec::new();
    let mut agreed = 0usize;
    let mut pinned = 0usize;
    for fx in fixtures() {
        let q = query_of(&fx);
        let mine = ours(&engine, &q, window, fx.span_attrs).await;
        let trace = trace_of
            .get(fx.service)
            .unwrap_or_else(|| panic!("[{}] no corpus for {}", fx.name, fx.service));
        let theirs = reference(&api_base, &q, window_s, fx.span_attrs, trace)
            .unwrap_or_else(|| panic!("[{}] the reference returned no traces array", fx.name));
        let want_ours = expected(fx.ours);

        let mut problems: Vec<String> = Vec::new();
        if mine != want_ours {
            problems.push(format!(
                "OUR answer is not the required one.\n  required:{}\n  got:{}",
                render(&want_ours),
                render(&mine)
            ));
        }
        match fx.theirs {
            None => {
                if theirs != want_ours {
                    problems.push(format!(
                        "the reference does not agree.\n  required:{}\n  reference:{}",
                        render(&want_ours),
                        render(&theirs)
                    ));
                }
            }
            Some(pin) => {
                let want_theirs = expected(pin);
                if theirs != want_theirs {
                    problems.push(format!(
                        "the reference's PINNED answer moved.\n  pinned:{}\n  reference:{}",
                        render(&want_theirs),
                        render(&theirs)
                    ));
                }
                // A pinned divergence that has become an agreement is the
                // signal the owning work landed: retire the pin and its
                // ledger row in the same change, do not relax this.
                if theirs == mine {
                    problems.push(format!(
                        "the two sides now AGREE, so this pin and its ledger row must retire: {}",
                        fx.divergence
                    ));
                }
            }
        }

        if problems.is_empty() {
            if fx.theirs.is_some() {
                pinned += 1;
                eprintln!("[{}] PINNED DIVERGENCE — {}", fx.name, fx.divergence);
            } else {
                agreed += 1;
                eprintln!("[{}] AGREES — {} span set(s)", fx.name, mine.len());
            }
        } else {
            eprintln!("[{}] q = {q}\n  {}", fx.name, problems.join("\n  "));
            diverged.push(fx.name.to_string());
        }
    }

    exec(&bootstrap, &format!("DROP DATABASE IF EXISTS {db}")).await;

    eprintln!("{agreed} fixture(s) agree, {pinned} pinned divergence(s)");
    assert!(
        diverged.is_empty(),
        "spanSet-attributes parity divergence in {diverged:?} (from REAL PulsusDB + reference \
         output; see the per-fixture lines above)"
    );
}

// ---------------------------------------------------------------------------
// Hermetic guards on the comparison itself
// ---------------------------------------------------------------------------

/// The reduction carries EVERY attribute of a span set, in order — not
/// just the first.
///
/// *RED when:* the reduction goes back to `attributes[0]`. The pre-#510
/// version of this suite reduced each span set to its first contributor,
/// so a second contributor, a wrong arm on a later one, and a wrong
/// ORDER were all invisible to it; a round-1 review confirmed that by
/// returning a wrong attribute type and watching the suite stay green.
#[test]
fn the_group_map_key_carries_every_attribute() {
    let shape = SetShape::new(
        Some(vec![
            format!("by(name)={}", our_token(&GroupValue::Str("a".to_string()))),
            format!("count()={}", our_token(&GroupValue::Int(3))),
        ]),
        2,
        vec!["01".to_string(), "02".to_string()],
    );
    assert_eq!(
        shape.0, "by(name)=stringValue=a,count()=intValue=3 | m2 | 01,02",
        "the shape must carry every contributor, in order, with its arm"
    );
    // A span set with NO attributes key is a different shape from one
    // with an empty list — the reference's answer after a `coalesce()`.
    assert_eq!(
        SetShape::new(None, 3, vec!["01".to_string()]).0,
        "- | m3 | 01"
    );
    assert_ne!(
        SetShape::new(None, 3, vec!["01".to_string()]),
        SetShape::new(Some(vec![]), 3, vec!["01".to_string()])
    );
}

/// Our token and the reference's are the same shape for the same value,
/// arm included — so the two sides of every fixture are comparable, and a
/// wire-type difference cannot be normalised away.
#[test]
fn the_two_sides_tokenise_a_value_the_same_way() {
    let cases: [(GroupValue, serde_json::Value); 8] = [
        (
            GroupValue::Str("apple".to_string()),
            serde_json::json!({"stringValue": "apple"}),
        ),
        (
            GroupValue::Str("8080".to_string()),
            serde_json::json!({"stringValue": "8080"}),
        ),
        (GroupValue::Int(3), serde_json::json!({"intValue": "3"})),
        (
            GroupValue::Int(9_007_199_254_740_993),
            serde_json::json!({"intValue": "9007199254740993"}),
        ),
        (
            GroupValue::Double(1.5_f64.to_bits()),
            serde_json::json!({"doubleValue": 1.5}),
        ),
        (
            GroupValue::Double((-0.0_f64).to_bits()),
            serde_json::json!({"doubleValue": -0.0}),
        ),
        (
            GroupValue::Double(f64::NEG_INFINITY.to_bits()),
            serde_json::json!({"doubleValue": "-Infinity"}),
        ),
        (
            GroupValue::Bool(true),
            serde_json::json!({"boolValue": true}),
        ),
    ];
    for (ours, theirs) in cases {
        assert_eq!(
            our_token(&ours),
            reference_token(Some(&theirs)),
            "{ours:?} against {theirs}"
        );
    }
    // …and a wire-type difference is NOT normalised away.
    assert_ne!(
        our_token(&GroupValue::Int(3)),
        reference_token(Some(&serde_json::json!({"doubleValue": 3.0})))
    );
    assert_ne!(
        our_token(&GroupValue::Str("3".to_string())),
        reference_token(Some(&serde_json::json!({"intValue": "3"})))
    );
    // The absent-key bucket is the reference's literal marker.
    assert_eq!(our_token(&GroupValue::Nil), "stringValue=nil");
}

/// Every pinned divergence names the ledger row that records it, and the
/// mixed-type defect is exercised in BOTH push orders.
///
/// *RED when:* a fixture is pinned as a divergence without saying what
/// retires it, or one of the mixed-type pair is deleted. A single-order
/// fixture cannot show order dependence, which is the entire content of
/// that defect, so the PAIR is the criterion and both names are asserted.
#[test]
fn every_pinned_divergence_names_its_ledger_row_and_the_pair_is_complete() {
    let all = fixtures();
    for fx in &all {
        if fx.theirs.is_some() {
            assert!(
                fx.divergence.contains("ledger row"),
                "[{}] a pinned divergence must name the ledger row that records it",
                fx.name
            );
        } else {
            assert!(
                fx.divergence.is_empty(),
                "[{}] a parity fixture carries no divergence text",
                fx.name
            );
        }
    }
    for name in [
        "mixed_type_int_first_sum",
        "mixed_type_float_first_sum",
        "mixed_type_int_first_avg",
        "mixed_type_float_first_avg",
    ] {
        assert!(
            all.iter().any(|f| f.name == name),
            "{name} is missing: the mixed-type defect is order dependence, and one order cannot \
             show it"
        );
    }
    // The two corpora are the same values in opposite push order — the
    // property the pair rests on, asserted rather than assumed.
    let flip: Vec<(&str, &str)> = O.iter().map(|s| s.attrs[0]).collect();
    let straight: Vec<(&str, &str)> = N.iter().map(|s| s.attrs[0]).collect();
    assert_eq!(straight.len(), 2);
    assert_eq!(
        straight,
        flip.iter().rev().copied().collect::<Vec<_>>(),
        "the mixed-type corpora must be the same two values in opposite order"
    );
}

/// Issue #492 item 2's six ordering fixtures, frozen as
/// `(name, query, service, coalesced)`. The merge that brought issue #510
/// here rewrote this file around a different fixture type, so a fixture
/// that is dropped, renamed, or given another fixture's query must fail
/// here. A count of the names cannot see any of those three.
///
/// The query is the text `query_of` sends, transcribed by hand from the
/// six records the merge plan froze.
const ORD_FIXTURES: [(&str, &str, &str, bool); 6] = [
    (
        "ord_by_then_count_filters_the_groups",
        r#"{resource.service.name="grp492"} | by(name) | count() > 2"#,
        "grp492",
        false,
    ),
    (
        "ord_count_then_by_is_unchanged",
        r#"{resource.service.name="grp492"} | count() > 2 | by(name)"#,
        "grp492",
        false,
    ),
    (
        "ord_coalesce_merges_the_survivors",
        r#"{resource.service.name="grp492"} | by(name) | count() > 2 | coalesce()"#,
        "grp492",
        true,
    ),
    (
        "ord_coalesce_before_the_filter_is_unchanged",
        r#"{resource.service.name="grp492"} | by(name) | coalesce() | count() > 2"#,
        "grp492",
        true,
    ),
    (
        "ord_nested_by_name_then_kind",
        r#"{resource.service.name="grp492b"} | by(name) | by(kind)"#,
        "grp492b",
        false,
    ),
    (
        "ord_nested_by_kind_then_name",
        r#"{resource.service.name="grp492b"} | by(kind) | by(name)"#,
        "grp492b",
        false,
    ),
];

/// Issue #492 item 2's six ordering fixtures survive this merge
/// unchanged: each `(name, query, service, coalesced)` record, in this
/// order.
///
/// *RED when:* a fixture is dropped, renamed, or given another fixture's
/// query. A count of the names — `git grep -c 'ord_'` returning 6 — sees
/// none of those three, which is why the records are frozen instead.
#[test]
fn the_six_ordering_fixtures_are_exactly_the_frozen_records() {
    let built = fixtures();
    let got: Vec<(&str, String, &str, bool)> = built
        .iter()
        .filter(|f| f.name.starts_with("ord_"))
        .map(|f| (f.name, query_of(f), f.service, f.coalesced))
        .collect();
    let want: Vec<(&str, String, &str, bool)> = ORD_FIXTURES
        .iter()
        .map(|(name, q, service, coalesced)| (*name, (*q).to_string(), *service, *coalesced))
        .collect();
    assert_eq!(
        got, want,
        "issue #492 item 2's six ordering fixtures must survive the merge unchanged: each \
         (name, query, service, coalesced) record, in this order. A count of the names cannot \
         see a dropped, renamed, or duplicated query."
    );
}

/// Every fixture's query is scoped to its own corpus's service, and every
/// service named by a fixture exists.
///
/// *RED when:* a fixture is added with an unscoped query — which would
/// see every other corpus in the shared database and, on the shared
/// oracle, every other leg's spans too.
#[test]
fn every_fixture_is_scoped_to_a_corpus_that_exists() {
    let services: BTreeSet<&str> = corpora().into_iter().map(|c| c.service).collect();
    for fx in fixtures() {
        assert!(
            services.contains(fx.service),
            "[{}] names service {:?}, which no corpus provides",
            fx.name,
            fx.service
        );
        let q = query_of(&fx);
        assert!(
            q.starts_with(&format!(r#"{{resource.service.name="{}""#, fx.service)),
            "[{}] must scope on its own service: {q}",
            fx.name
        );
        pulsus_traceql::parse(&q).unwrap_or_else(|e| panic!("[{}] {q:?} must parse: {e}", fx.name));
    }
}
