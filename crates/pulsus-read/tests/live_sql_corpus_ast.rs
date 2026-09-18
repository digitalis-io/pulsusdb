//! ADR 0008 D2, as amended by issue #549: **no statement in the
//! committed corpus binds a relational subquery through a common table
//! expression** — asked of ClickHouse's own parser rather than of a
//! regular expression.
//!
//! # Why this is not a text matcher
//!
//! Seven attempts at this check asserted a property of the TEXT — a
//! keyword's position, a statement count, that a part parses, that a part
//! carries no binding node — and each was beaten by text with a different
//! property. The last was beaten by a mutation that truncated a nested
//! binding to a bare `SELECT *`, which ClickHouse accepts with no source
//! clause: the truncation landed on valid SQL, parsed cleanly, carried
//! zero binding nodes, and left every pinned literal holding.
//!
//! So the check asks the thing that defines the language:
//!
//! ```text
//!   for each statement in the committed corpus
//!       EXPLAIN AST <statement>          <- ClickHouse's own parser
//!       assert the tree contains no `WithElement` node
//! ```
//!
//! Measured on 26.3, across the forms: `name AS (SELECT …)` and
//! `AS MATERIALIZED (SELECT …)` produce a `WithElement`; a scalar alias,
//! an array alias, `(SELECT …) AS name`, a trailing-alias subquery,
//! `ORDER BY … WITH FILL` and `GROUP BY … WITH TOTALS` produce none.
//! `EXPLAIN AST` parses without the referenced tables existing, so this
//! needs a server and not a fixture — and it creates no database, because
//! it reads nothing.
//!
//! # Three assertions, in this order, and the order is the point
//!
//! **1. Coverage, first, because it cannot be argued with.** Every byte
//! of every corpus file is either inside a declared statement or on a
//! STRUCTURAL line — a marker, a `== ` case header, a `-- ` comment, a
//! `params`/`selectors` line, or blank. This is not a statement about
//! statements, so there is no cleverer statement that defeats it: a
//! truncation leaves SQL bytes on a line that is not structural and the
//! failure names the file and the byte.
//!
//! **2. The parse, second**, over parts step 1 has already shown are
//! whole. `EXPLAIN AST` over each part; no part carries a binding node.
//!
//! **3. The semantic control, third**, as the thing that reddens if
//! either of the first two slips: `golden/with_binding_control.txt`
//! holds ONE statement carrying EXACTLY ONE binding node. Exactly one,
//! not "a binding node" — a wording that permitted more would pass a
//! fixture that had grown a second.
//!
//! # Where the boundaries come from
//!
//! **Every boundary is writer-emitted or format-defined; none is
//! inferred from a keyword.**
//!
//! ```text
//! golden/**/*.sql
//!     a file with no `== ` line holds exactly one statement; otherwise
//!     each `== ` line begins one, and it runs to the next `== ` line.
//!
//! golden/promql_statements.txt, golden/with_binding_control.txt
//!     the writer emits `-- statement[i] offset=… len=…` before each
//!     statement, and the split is at those DECLARED spans. It is our
//!     generator, so the format is ours to make unambiguous rather than
//!     to guess at.
//! ```
//!
//! A keyword rule would be wrong here and measurably so: the four
//! grouped statements issue #549 adds BEGIN with the binding keyword, so
//! "a line starting `SELECT ` in column 0" finds their inner `SELECT`
//! and feeds the parser only the tail — under which a prohibited binding
//! becomes a clean tree:
//!
//! ```text
//!                                    whole statement      tail only
//!   the grouped statement            no WithElement       no WithElement
//!   a controlled relational binding  WithElement          no WithElement   <- the hole
//!      WITH q AS (SELECT 1 AS n)
//!      SELECT n FROM q
//! ```
//!
//! # Both empty inputs
//!
//! Coverage that is vacuously true is the defect this check already
//! removed a count assertion for.
//!
//! * **An empty file** would make the coverage walk vacuously green with
//!   nothing parsed. Every corpus file must therefore declare AT LEAST
//!   ONE statement, and a file that declares none is a failure naming the
//!   file — not a pass.
//! * **One marked empty statement** covers its file byte for byte and
//!   then fails the parse, because ClickHouse rejects empty statement
//!   text. That is the right outcome and it is the PARSE that produces
//!   it, not the coverage; the failure carries the offending part's
//!   offset so the message names where.
//!
//! # Where it stops
//!
//! It guarantees: *every statement in the committed corpus, as the
//! database parses it, binds no relational CTE.* It does **not** cover a
//! statement absent from the corpus, and it does **not** cover text built
//! at run time that never lands in a golden. The hermetic freeze keeps
//! its digest and cannot see this property at all — a stated limit, not a
//! gate that implies otherwise.
//!
//! **And this check is the only protection there is.** Measured: with a
//! relational binding prefixed onto a generated statement and both the
//! golden and its digest regenerated, both freeze suites go green.
//! Nothing else in the tree sees it.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1` through
//! `pulsus_testkit::require_live_gate`, which **fails closed** in a CI job
//! that exists to supply the dependency, so the check cannot skip green
//! there. To run it:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=$PORT_CH \
//!   cargo test -p pulsus-read --test live_sql_corpus_ast
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, QuerySettings, Row};

/// The marker prefix the two `.txt` corpora carry, and the fixed width of
/// one whole marker line — both mirroring
/// `tests/promql_statement_freeze.rs`'s writer.
const STATEMENT_MARKER: &str = "-- statement[";
const MARKER_LEN: usize = "-- statement[000] offset=00000000 len=000000\n".len();

/// The control, excluded from the no-binding assertion by path — ONE
/// exclusion, named here and nowhere else, and carrying its own opposite
/// assertion below.
const CONTROL: &str = "with_binding_control.txt";

/// The corpus, and what it holds. The counts are a cheap tripwire for a
/// corpus gaining or losing an entry; **they are not the protection**.
///
/// Issue #557 moved `SQL_STATEMENTS` from 446 to 396. The 45 committed
/// `traces_search` goldens that carried a `== phase2 membership[i] ==`
/// section — 50 sections between them — lost it: the attribute condition
/// is a predicate column on the hydration statement now and sends no
/// statement of its own. No `.sql` FILE was added or removed, so
/// `SQL_FILES` stays at 126.
///
/// The same change puts a `WITH` clause on 45 of those goldens'
/// hydration statements, which is the first `WITH` in the `traces_search`
/// corpus. Every one of them is a scalar/array alias
/// (`arrayFirstIndex(…) AS pi0`), so the parse below still reports ZERO
/// `WithElement` binding nodes for them — which is what ADR 0008 D2 asks
/// and what the loop above measures rather than assumes.
const SQL_FILES: usize = 126;
const SQL_STATEMENTS: usize = 396;
const PROMQL_ENTRIES: usize = 30;
const PROMQL_STATEMENTS: usize = 56;
const CONTROL_STATEMENTS: usize = 1;

fn golden_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
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
        query_timeout: Duration::from_secs(30),
        ..ChConnConfig::default()
    }
}

/// One row of `EXPLAIN AST`'s single `String` column.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ExplainRow {
    explain: String,
}

/// One declared statement: where it starts in its file and how long it is.
#[derive(Debug, Clone, Copy)]
struct Part {
    offset: usize,
    len: usize,
}

/// A byte range the format defines as structure rather than SQL.
///
/// Deliberately narrow: a line has to be blank, a case header, a comment,
/// or one of the freeze's two header keywords. A truncated statement
/// leaves `FROM metric_samples` on a line that is none of those.
fn structural(line: &str) -> bool {
    line.is_empty()
        || line.starts_with("== ")
        || line.starts_with("--")
        || line.starts_with("params ")
        || line.starts_with("selectors ")
}

/// The `.sql` rule: one statement per `== ` line, or the whole file when
/// there is none.
fn split_sql(text: &str) -> Vec<Part> {
    let mut starts: Vec<usize> = Vec::new();
    let mut at = 0usize;
    for line in text.split_inclusive('\n') {
        if line.starts_with("== ") {
            starts.push(at + line.len());
        }
        at += line.len();
    }
    if starts.is_empty() {
        return if text.is_empty() {
            Vec::new()
        } else {
            vec![Part {
                offset: 0,
                len: text.len(),
            }]
        };
    }
    // Each statement runs to the start of the NEXT `== ` line, which is
    // that line's own recorded start minus its header line.
    let mut header_starts: Vec<usize> = Vec::new();
    let mut at = 0usize;
    for line in text.split_inclusive('\n') {
        if line.starts_with("== ") {
            header_starts.push(at);
        }
        at += line.len();
    }
    starts
        .iter()
        .enumerate()
        .map(|(i, &s)| Part {
            offset: s,
            len: header_starts.get(i + 1).copied().unwrap_or(text.len()) - s,
        })
        .collect()
}

/// The marker rule: the split is at the DECLARED spans, and the
/// declaration is checked against where the marker line ends.
fn split_marked(rel: &str, text: &str) -> Vec<Part> {
    let mut parts = Vec::new();
    let mut at = 0usize;
    for line in text.split_inclusive('\n') {
        if line.starts_with(STATEMENT_MARKER) {
            assert_eq!(
                line.len(),
                MARKER_LEN,
                "{rel}: a marker line of the wrong width at byte {at}: {line:?}"
            );
            let offset: usize = line[line.find("offset=").expect("offset=") + 7..][..8]
                .parse()
                .unwrap_or_else(|e| panic!("{rel}: offset digits at byte {at}: {e}"));
            let len: usize = line[line.find(" len=").expect(" len=") + 5..][..6]
                .parse()
                .unwrap_or_else(|e| panic!("{rel}: len digits at byte {at}: {e}"));
            assert_eq!(
                offset,
                at + MARKER_LEN,
                "{rel}: a marker at byte {at} declares a span that does not start after it"
            );
            assert!(
                offset + len <= text.len(),
                "{rel}: the span declared at byte {at} runs past the end of the file"
            );
            parts.push(Part { offset, len });
        }
        at += line.len();
    }
    parts
}

/// **Assertion one.** Every byte of `text` is inside a declared statement
/// or on a structural line, and the file declares at least one statement.
///
/// The failure names the file and the byte, which is what makes it
/// actionable rather than "a corpus statement did not parse".
fn assert_covered(rel: &str, text: &str, parts: &[Part]) {
    assert!(
        !parts.is_empty(),
        "{rel}: the file declares no statement. An empty corpus file makes this check \
         vacuously green with nothing parsed, so it is a failure naming the file."
    );
    let mut cursor = 0usize;
    let check_gap = |from: usize, to: usize| {
        let mut at = from;
        for line in text[from..to].split_inclusive('\n') {
            let bare = line.strip_suffix('\n').unwrap_or(line);
            assert!(
                structural(bare),
                "{rel}: byte {at} is outside every declared statement and is not structure: \
                 {bare:?}"
            );
            at += line.len();
        }
    };
    for part in parts {
        assert!(
            part.offset >= cursor,
            "{rel}: the span at byte {} overlaps the one before it",
            part.offset
        );
        check_gap(cursor, part.offset);
        cursor = part.offset + part.len;
    }
    check_gap(cursor, text.len());
}

/// Every file in the golden tree that this check reads, with its relative
/// path and its declared statements.
fn corpus() -> Vec<(String, String, Vec<Part>)> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, String, Vec<Part>)>) {
        let mut entries: Vec<_> = fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
            .map(|e| e.expect("dir entry"))
            .collect();
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let name = entry.file_name().to_string_lossy().to_string();
            let rel = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            let meta = entry.metadata().expect("metadata");
            if meta.is_dir() {
                walk(&entry.path(), &rel, out);
                continue;
            }
            let is_sql = rel.ends_with(".sql");
            let is_marked = rel.ends_with("promql_statements.txt") || rel.ends_with(CONTROL);
            if !is_sql && !is_marked {
                continue;
            }
            let text = fs::read_to_string(entry.path()).expect("read");
            let parts = if is_sql {
                split_sql(&text)
            } else {
                split_marked(&rel, &text)
            };
            out.push((rel, text, parts));
        }
    }
    let mut out = Vec::new();
    walk(&golden_root(), "", &mut out);
    out
}

/// `EXPLAIN AST` over one statement, as the database's own parser sees it.
///
/// **The `?` doubling is transport, not a transform of what is parsed.**
/// The `clickhouse` crate's `SqlBuilder` reads a bare `?` anywhere in the
/// query text as an unbound bind placeholder and refuses the query; `??`
/// collapses back to one literal `?` *before the text reaches the
/// server*, so ClickHouse's parser still sees the committed bytes. This
/// is the same fix `logql::exec::escape_query_placeholders` applies at
/// every production execution boundary, for the same reason: the corpus
/// is full of `(?:…)` non-capturing groups.
async fn explain_ast(client: &ChClient, sql: &str) -> Result<String, String> {
    let sql = sql.replace('?', "??");
    let mut stream = match client
        .query_stream::<ExplainRow>(&format!("EXPLAIN AST {sql}"), &QuerySettings::new())
        .await
    {
        Ok(s) => s,
        Err(e) => return Err(format!("{e:?}")),
    };
    let mut tree = String::new();
    while let Some(row) = stream.next().await {
        match row {
            Ok(r) => {
                tree.push_str(&r.explain);
                tree.push('\n');
            }
            Err(e) => return Err(format!("{e:?}")),
        }
    }
    Ok(tree)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_committed_statement_binds_no_relational_cte() {
    // Fails closed: in a CI job that exists to supply ClickHouse, an
    // absent gate is a wiring failure and not a skip.
    if !pulsus_testkit::require_live_gate("PULSUS_TEST_CLICKHOUSE").is_running() {
        eprintln!("skipping: set PULSUS_TEST_CLICKHOUSE=1");
        return;
    }
    let client = ChClient::new(test_config()).await.expect("connect");
    let corpus = corpus();

    // --- 1. coverage, first -----------------------------------------
    let mut sql_files = 0usize;
    let mut sql_statements = 0usize;
    let mut promql_statements = 0usize;
    let mut control_statements = 0usize;
    for (rel, text, parts) in &corpus {
        assert_covered(rel, text, parts);
        if rel.ends_with(".sql") {
            sql_files += 1;
            sql_statements += parts.len();
        } else if rel.ends_with(CONTROL) {
            control_statements += parts.len();
        } else {
            promql_statements += parts.len();
        }
    }

    // --- 2. the parse, over parts coverage has shown are whole ------
    let mut parsed = 0usize;
    for (rel, text, parts) in &corpus {
        for part in parts {
            let sql = &text[part.offset..part.offset + part.len];
            let tree = explain_ast(&client, sql).await.unwrap_or_else(|e| {
                panic!("{rel}: the part at byte {} did not parse: {e}", part.offset)
            });
            parsed += 1;
            let bindings = tree.matches("WithElement").count();
            if rel.ends_with(CONTROL) {
                // The one exclusion, in the same function that scans, and
                // with its own opposite assertion.
                assert_eq!(
                    bindings, 1,
                    "{rel}: the control must carry EXACTLY ONE binding node, or this check has \
                     stopped seeing the thing it exists to forbid"
                );
                continue;
            }
            assert_eq!(
                bindings, 0,
                "{rel}: the part at byte {} binds a relational subquery through a common table \
                 expression, which ADR 0008 D2 forbids. The measured alternative is a scalar or \
                 array alias, which the parser gives no binding node.",
                part.offset
            );
        }
    }

    // --- 3. the tripwire counts, which are NOT the protection -------
    assert_eq!(sql_files, SQL_FILES, "the `.sql` corpus file count");
    assert_eq!(
        sql_statements, SQL_STATEMENTS,
        "the `.sql` corpus statement count"
    );
    assert_eq!(
        promql_statements, PROMQL_STATEMENTS,
        "the PromQL freeze's statement count"
    );
    assert_eq!(
        control_statements, CONTROL_STATEMENTS,
        "the control's statement count"
    );
    assert_eq!(
        parsed,
        SQL_STATEMENTS + PROMQL_STATEMENTS + CONTROL_STATEMENTS,
        "every declared statement reached the parser"
    );
    // A second entry in the PromQL freeze's `== ` headers would mean the
    // two formats had drifted apart.
    let entries = corpus
        .iter()
        .find(|(rel, _, _)| rel.ends_with("promql_statements.txt"))
        .map(|(_, text, _)| text.matches("\n== ").count() + 1)
        .expect("the PromQL freeze is in the corpus");
    assert_eq!(entries, PROMQL_ENTRIES, "the PromQL freeze's entry count");
    eprintln!(
        "[549] parsed {parsed} statements: {sql_statements} across {sql_files} .sql files, \
         {promql_statements} across {entries} PromQL entries, {control_statements} control"
    );
}
