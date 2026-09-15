//! Issue #544 — **every stored metadata text is its own render**, and
//! every ingest constraint the render does not re-apply has a row.
//!
//! # Why this is the writer-side half of the lowered filter
//!
//! The lowered predicate reads a key out of `log_samples.structured_metadata`
//! with `JSONExtractString`, and our own decoder reads the same column.
//! The two agree on every byte string a WRITER can produce; they disagree
//! on thirteen recorded shapes of arbitrary JSON, which is why the claim
//! is scoped to writers and why the set of writers has to stay closed
//! (`crates/pulsus-read/tests/logql_metadata_filter_gates.rs` keeps it
//! closed; this file checks what those writers produce).
//!
//! # The property, as an equation
//!
//! For every stored text `T`, with the writer named for its column:
//!
//! ```text
//! structured_metadata   T == render_structured_metadata(what the database reads out of T)
//! log_streams.labels    T == LabelSet::to_canonical_json(what the database reads out of T)
//! ```
//!
//! **One function per column, each named, each including its own empty
//! case.** The two writer-side functions disagree on the empty input —
//! `render_structured_metadata(vec![])` is `""` and
//! `LabelSet::from_verbatim(vec![]).to_canonical_json()` is `{}` — and the
//! empty string is what the metadata column holds for every entry with no
//! metadata, which is the commonest input there is.
//!
//! **The re-render is done in Rust, not in SQL.** The database's own JSON
//! renderer escapes a solidus where our writer leaves it raw, so a
//! pure-SQL identity fails on every value holding one, a URL path being
//! the case a reader will actually meet.
//!
//! # Where the identity does not reach, and what closes it instead
//!
//! The identity accepts exactly the fixed points of render-after-read. A
//! writer emits `render(P)` for pair lists ingest has already shaped, so
//! the texts it accepts and no writer produces are in one-to-many
//! correspondence with the constraints INGEST applies that the RENDER does
//! not re-apply. [`BLIND_SPOTS`] is that table, and
//! [`the_blind_spot_rows_match_the_ingest_sites`] is what keeps it honest:
//! a digest over the ingest function's body, so a constraint added inside
//! it cannot be added silently, and a per-row behavioural check that
//! executes both halves.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`. Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-write --test metadata_text_identity_live
//! podman rm -f pulsus-ch-test
//! ```

use std::time::Duration;

use futures::StreamExt;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, QuerySettings, Row};
use pulsus_model::LabelSet;
use pulsus_write::{LogIngestSettings, ParsedLogs, render_structured_metadata};

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this \
                 test (see crates/pulsus-write/tests/metadata_text_identity_live.rs)"
            );
            return;
        }
    };
}

async fn client() -> ChClient {
    ChClient::new(ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    })
    .await
    .expect("connect")
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct PairsRow {
    pairs: Vec<(String, String)>,
}

/// What the database reads out of `text`, as string-valued pairs — the
/// same expression the lowered predicate's projection reads.
async fn database_reads(ch: &ChClient, text: &str) -> Vec<(String, String)> {
    let sql = format!(
        "SELECT JSONExtractKeysAndValues({}, 'String') AS pairs",
        quote(text)
    );
    // The client reads `?` as a query-argument placeholder; a doubled
    // one is a literal question mark, which a URL value carries.
    let sql = sql.replace('?', "??");
    let mut stream = ch
        .query_stream::<PairsRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("read {text:?}: {e}"));
    stream.next().await.expect("a row").expect("decode").pairs
}

/// A ClickHouse string literal.
fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    out.push('\'');
    out
}

fn attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

fn otlp_settings(on: bool) -> LogIngestSettings {
    LogIngestSettings {
        discover_log_levels: on,
    }
}

/// One entry through the **scope-attribute** transport. Its metadata comes
/// from the SCOPE's attributes, not from the log record's: a record
/// attribute reaches the level rule and nothing else.
fn otlp_push(pairs: &[(&str, &str)], on: bool) -> Result<ParsedLogs, String> {
    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![attr("service.name", "identity")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: String::new(),
                    version: String::new(),
                    attributes: pairs.iter().map(|(k, v)| attr(k, v)).collect(),
                    ..Default::default()
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_788_099_000_000_000_000,
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("a line".to_string())),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    pulsus_write::parse(&req, 1_788_099_000_000_000_000, otlp_settings(on))
        .map_err(|e| e.to_string())
}

/// One entry through the **push** transport, as the JSON body a client
/// sends.
fn loki_push(pairs: &[(&str, &str)], on: bool) -> Result<ParsedLogs, String> {
    let meta: serde_json::Map<String, serde_json::Value> = pairs
        .iter()
        .map(|(k, v)| {
            (
                (*k).to_string(),
                serde_json::Value::String((*v).to_string()),
            )
        })
        .collect();
    let body = serde_json::json!({
        "streams": [{
            "stream": {"service_name": "identity"},
            "values": [["1788099000000000000", "a line", meta]],
        }]
    });
    let discovery = if on {
        pulsus_write::LevelDiscovery::On
    } else {
        pulsus_write::LevelDiscovery::Off
    };
    pulsus_write::parse_loki_json(
        serde_json::to_string(&body).expect("json").as_bytes(),
        1_788_099_000_000_000_000,
        discovery,
    )
    .map_err(|e| e.to_string())
}

/// The adversarial corpus: value shapes a client would choose to try to
/// produce a text the two readers disagree about.
const VALUES: &[(&str, &str)] = &[
    ("plain", "v"),
    ("ending_comma", "v,"),
    ("close_brace", "}"),
    ("open_brace", "{"),
    ("spelled_u", r"a\uZZZZb"),
    ("spelled_pair", r"a😀b"),
    ("object", r#"{"n":"v"}"#),
    ("array", r#"["v"]"#),
    ("astral", "a\u{1F600}b"),
    ("dup_key", "\",\"k\":\"second"),
    ("backspace", "a\u{8}b"),
    ("formfeed", "a\u{c}b"),
    ("quote", "he said \"hi\""),
    ("solidus", "a/b"),
    ("url", "https://example.test/a/b?c=d"),
    ("tab_nl_cr", "a\tb\nc\rd"),
    ("nul", "a\u{0}b"),
    ("nbsp", "a\u{a0}b"),
    ("zwsp", "a\u{200b}b"),
    ("combining", "e\u{301}"),
    ("text_before", "before{\"k\":\"v\"}"),
    ("trailing_comma", "{\"k\":\"v\",}"),
];

/// Issue #544 AC19, the identity — **every stored text is its own render,
/// under the writer named for its column.**
///
/// Both metadata writers are exercised: the push transport's renderer and
/// the other transport's, at both discovery settings. The corpus carries
/// the two ways the EMPTY string arises in production — an entry with no
/// metadata at the discovery-off setting, and a row written before the
/// column existed, which reads back as its `DEFAULT ''`.
#[tokio::test]
async fn every_stored_text_is_its_own_render() {
    skip_unless_live!();
    let ch = client().await;

    let mut texts: Vec<(String, String)> = Vec::new(); // (what produced it, the text)
    let mut labels: Vec<(String, String)> = Vec::new();
    for (name, value) in VALUES {
        for on in [false, true] {
            for (transport, parsed) in [
                ("push", loki_push(&[("k", value)], on)),
                ("scope", otlp_push(&[("k", value)], on)),
            ] {
                let parsed =
                    parsed.unwrap_or_else(|e| panic!("{transport}/{name}/discovery={on}: {e}"));
                for row in &parsed.rows {
                    texts.push((
                        format!("{transport}/{name}/discovery={on}"),
                        row.structured_metadata.clone(),
                    ));
                }
                for s in &parsed.streams {
                    labels.push((
                        format!("{transport}/{name}/discovery={on}"),
                        s.labels.to_canonical_json(),
                    ));
                }
            }
        }
    }
    // Multi-pair entries, so a text with more than one member is covered.
    for on in [false, true] {
        for (transport, parsed) in [
            (
                "push",
                loki_push(&[("a", "1"), ("b", "2"), ("c", "a/b")], on),
            ),
            (
                "scope",
                otlp_push(&[("a", "1"), ("b", "2"), ("c", "a/b")], on),
            ),
        ] {
            let parsed = parsed.unwrap_or_else(|e| panic!("{transport}/multi: {e}"));
            for row in &parsed.rows {
                texts.push((
                    format!("{transport}/multi/discovery={on}"),
                    row.structured_metadata.clone(),
                ));
            }
        }
    }
    // An entry with NO metadata, discovery off: the empty string, through
    // the ordinary receiver path.
    let bare = loki_push(&[], false).expect("no metadata, discovery off");
    for row in &bare.rows {
        texts.push((
            "push/none/discovery=false".to_string(),
            row.structured_metadata.clone(),
        ));
    }
    // …and the column's own `DEFAULT ''`, which is what every row written
    // before migration id 21 reads back as.
    texts.push(("column default".to_string(), String::new()));

    assert!(
        texts.iter().any(|(_, t)| t.is_empty()),
        "the corpus must contain the empty string, which is the commonest stored text of all"
    );
    let distinct: std::collections::BTreeSet<&str> =
        texts.iter().map(|(_, t)| t.as_str()).collect();
    assert!(
        distinct.len() >= 40,
        "the corpus is too narrow to say anything: {} distinct stored texts",
        distinct.len()
    );

    let mut broken: Vec<String> = Vec::new();
    for (whence, text) in &texts {
        let pairs = database_reads(&ch, text).await;
        let rendered = render_structured_metadata(pairs.clone());
        if &rendered != text {
            broken.push(format!(
                "{whence}: stored {text:?}, the database reads {pairs:?}, the writer \
                 re-renders {rendered:?}"
            ));
        }
    }
    for (whence, text) in &labels {
        let pairs = database_reads(&ch, text).await;
        assert!(
            !text.is_empty(),
            "{whence}: a stream always carries at least `service_name`, so no stored labels \
             text is empty"
        );
        let rendered = LabelSet::from_verbatim(pairs.clone()).to_canonical_json();
        if &rendered != text {
            broken.push(format!(
                "{whence} (labels): stored {text:?}, the database reads {pairs:?}, the writer \
                 re-renders {rendered:?}"
            ));
        }
    }
    assert!(
        broken.is_empty(),
        "{} of {} stored texts are not their own render:\n  {}",
        broken.len(),
        texts.len() + labels.len(),
        broken.join("\n  ")
    );
    println!(
        "identity: {} stored texts, {} distinct, 0 failures",
        texts.len() + labels.len(),
        distinct.len()
    );
}

// =====================================================================
// The blind spots, derived from the ingest function rather than listed
// =====================================================================

/// One class the identity accepts and no writer produces.
struct BlindSpot {
    /// The lines of the ingest function the constraint sits on.
    site: &'static str,
    class: &'static str,
    /// Stored texts the identity ACCEPTS — that is what makes the class a
    /// blind spot rather than something the identity already catches.
    witnesses: &'static [&'static str],
    /// The client inputs whose ingest is what closes the class, and what
    /// each transport must do with them. `None` means nothing closes it:
    /// the text is producible on the scope transport, so it is corpus
    /// rather than a blind spot.
    closure: Option<Closure>,
}

/// What every transport must do with the inputs of a closed class.
#[derive(Clone, Copy)]
enum Closure {
    /// The receiver refuses the request.
    Refused(&'static [(&'static str, &'static str)]),
    /// The receiver accepts it and stores something else — the pair is
    /// stripped, or the name or value rewritten.
    Rewritten(&'static [(&'static str, &'static str, &'static str)]),
}

/// **One row per constraint SITE, carrying the classes it contributes** —
/// which may be three, or one, or none.
///
/// The derivation, so a later reader can re-run it rather than
/// rediscovering a member: the identity accepts exactly the fixed points
/// of render-after-read, and a writer emits `render(P)` for pair lists
/// ingest has already shaped, so a text the identity accepts and no writer
/// produces is `render(P)` for a `P` that INGEST would have refused or
/// altered and that RENDER does not itself refuse or alter. Reading the
/// ingest function and asking, of each constraint in it, whether the
/// render repeats it, enumerates them.
///
/// **Two ways that goes wrong, both of which have happened here.** A
/// single call can apply several constraints — the pair resolver was read
/// as one alteration, then two, then three. And an alteration the render
/// "repeats" may be repeated only in PART: the render's canonicaliser
/// agrees with the ingest namer on `a.b` and not on `a__b`, so a row must
/// say on WHICH inputs it repeats, not that it repeats.
const BLIND_SPOTS: &[BlindSpot] = &[
    BlindSpot {
        site: "the pair-count cap",
        class: "a pair count over the cap",
        witnesses: &[],
        closure: None,
    },
    BlindSpot {
        site: "the byte-count cap",
        class: "a byte count over the cap",
        witnesses: &[],
        closure: None,
    },
    BlindSpot {
        site: "the name check",
        class: "the empty name, and any name of underscores only",
        witnesses: &[r#"{"":"v"}"#, r#"{"_":"v"}"#, r#"{"__":"v"}"#],
        closure: Some(Closure::Refused(&[("", "v"), ("_", "v"), ("__", "v")])),
    },
    BlindSpot {
        site: "the pair resolver, its empty-value strip",
        class: "an empty-valued pair",
        witnesses: &[r#"{"k":""}"#],
        // Accepted, then stripped: nothing named `k` is stored.
        closure: Some(Closure::Rewritten(&[("k", "", "")])),
    },
    BlindSpot {
        site: "the pair resolver, its replacement-character rewrite",
        class: "a value carrying the replacement character",
        witnesses: &["{\"k\":\"a\u{fffd}b\"}"],
        closure: Some(Closure::Rewritten(&[("k", "a\u{fffd}b", "a b")])),
    },
    BlindSpot {
        site: "the pair resolver, its namer",
        class: "a name the two namers disagree about",
        witnesses: &[
            r#"{"1x":"v"}"#,
            r#"{"9":"v"}"#,
            r#"{"a__b":"v"}"#,
            r#"{"a___b":"v"}"#,
        ],
        // The ingest namer prefixes `key_` when a name does not begin
        // with a letter or an underscore, and collapses an interior run
        // of underscores; the render's canonicaliser does neither.
        closure: Some(Closure::Rewritten(&[
            ("1x", "v", "v"),
            ("9", "v", "v"),
            ("a__b", "v", "v"),
            ("a___b", "v", "v"),
        ])),
    },
];

/// The ingest function whose constraints the table above is derived from.
const INGEST_FILE: &str = "src/protocols/loki_push.rs";
const INGEST_FN: &str = "fn canonical_structured_metadata<'a>(";

/// The digest of that function's body, from its `fn` line to its closing
/// brace.
///
/// **A tripwire, not a classifier.** It fires on ANY edit to the body — a
/// rename and a comment included — so it names the function and not the
/// constraint. That is the price of detecting something that is, by
/// definition, not yet described: a constraint ADDED inside the function
/// changes neither the table nor any set built from it, so nothing
/// narrower than "this function changed, re-derive the table" can see it.
/// Anything narrower would have to parse the body, and a parser that
/// decides what a "constraint" is would be a second place for this
/// question to be answered wrongly.
const INGEST_BODY_SHA256: &str = "523f7488cb27d7fb520c4bf646fc63f8a4158ea84c2b28b3f8d6cfe993f0030a";

fn ingest_function_body() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(INGEST_FILE);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let mut out = String::new();
    let mut inside = false;
    for line in text.lines() {
        if line.starts_with(INGEST_FN) {
            inside = true;
        }
        if inside {
            out.push_str(line);
            out.push('\n');
            if line == "}" {
                return out;
            }
        }
    }
    panic!("{INGEST_FN} not found in {INGEST_FILE}");
}

/// Issue #544 AC19, the inventory — **every ingest constraint has a row,
/// and every row is still true.**
///
/// Two mechanisms, neither of which a comment can satisfy, each covering
/// the direction the other cannot:
///
/// ```text
/// a constraint is ADDED             the digest over the ingest function's body
/// a row stops being true            the row's witness and its closure, both executed
///   (removed, renamed, weakened)
/// ```
#[tokio::test]
async fn the_blind_spot_rows_match_the_ingest_sites() {
    skip_unless_live!();
    let ch = client().await;

    // The tripwire.
    let body = ingest_function_body();
    let digest = format!(
        "{:x}",
        <sha2::Sha256 as sha2::Digest>::digest(body.as_bytes())
    );
    assert_eq!(
        digest,
        INGEST_BODY_SHA256,
        "the metadata ingest function changed. Re-derive the blind-spot table in this file — \
         of each constraint in that body, ask whether the render re-applies it, and on WHICH \
         inputs — then update `INGEST_BODY_SHA256`. The body is {} lines.",
        body.lines().count()
    );

    for row in BLIND_SPOTS {
        println!("blind spot: {} ({})", row.class, row.site);
        // The identity half: a witness the identity does NOT accept is
        // not a blind spot and should not be in the table.
        for witness in row.witnesses {
            let pairs = database_reads(&ch, witness).await;
            let rendered = render_structured_metadata(pairs.clone());
            assert_eq!(
                &rendered, witness,
                "{}: the witness {witness:?} is not a blind spot — the identity already \
                 catches it (the database reads {pairs:?})",
                row.class
            );
        }
        // The closure half, on EVERY transport that reaches the column.
        match row.closure {
            None => {}
            Some(Closure::Refused(inputs)) => {
                for (name, value) in inputs {
                    for (transport, parsed) in [
                        ("push", loki_push(&[(name, value)], false)),
                        ("scope", otlp_push(&[(name, value)], false)),
                    ] {
                        assert!(
                            parsed.is_err(),
                            "{}: {transport} must REFUSE the name {name:?}; it stored {:?}",
                            row.class,
                            parsed.map(|p| p
                                .rows
                                .iter()
                                .map(|r| r.structured_metadata.clone())
                                .collect::<Vec<_>>())
                        );
                    }
                }
            }
            Some(Closure::Rewritten(inputs)) => {
                for (name, value, _stored_value) in inputs {
                    for (transport, parsed) in [
                        ("push", loki_push(&[(name, value)], false)),
                        ("scope", otlp_push(&[(name, value)], false)),
                    ] {
                        let parsed = parsed.unwrap_or_else(|e| {
                            panic!("{}: {transport} refused {name:?}: {e}", row.class)
                        });
                        for stored in parsed.rows.iter().map(|r| &r.structured_metadata) {
                            assert!(
                                !stored.contains(&format!(r#""{name}":"{value}""#)),
                                "{}: {transport} stored {name:?} unchanged as {stored:?}; the \
                                 rewrite that closes this class did not run",
                                row.class
                            );
                        }
                    }
                }
            }
        }
    }

    // The two cap rows are NOT blind spots, and this is what says so:
    // the push transport refuses them and the scope transport stores
    // them, so a text past either cap is ordinary stored data.
    let many: Vec<(String, String)> = (0..257)
        .map(|i| (format!("k{i:03}"), "v".to_string()))
        .collect();
    let many_ref: Vec<(&str, &str)> = many.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    assert!(
        loki_push(&many_ref, false).is_err(),
        "the push transport refuses a pair count over its cap"
    );
    let wide = otlp_push(&many_ref, false).expect("the scope transport stores it");
    let long_value = "x".repeat(70_000);
    assert!(
        loki_push(&[("k", &long_value)], false).is_err(),
        "the push transport refuses a byte count over its cap"
    );
    let long = otlp_push(&[("k", &long_value)], false).expect("the scope transport stores it");
    for (what, parsed) in [("257 pairs", wide), ("a 70,000-byte value", long)] {
        for stored in parsed.rows.iter().map(|r| &r.structured_metadata) {
            let pairs = database_reads(&ch, stored).await;
            assert_eq!(
                &render_structured_metadata(pairs),
                stored,
                "{what}: a wide or long metadata text is a canonical text like any other, and \
                 the identity holds on it"
            );
        }
    }
}
