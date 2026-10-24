//! The tenant every trace read is scoped to (issue #738).
//!
//! Every read of a trace table carries [`TenantTerm`] as the first conjunct
//! of the `WHERE` that follows its `FROM`, except the service graph's marker
//! reads, which carry [`MARKER_TERM`]. [`check`] holds that rule over every
//! statement [`super::dispatch::TraceDispatch`] sends.

use pulsus_model::Tenant;

use crate::metrics::TenantSql;

/// `org_id = <tenant>`, the term a trace read's `WHERE` begins with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantTerm(String);

impl TenantTerm {
    /// The term for `tenant`, its text through the crate's one escaper.
    pub fn of(tenant: &Tenant) -> Self {
        TenantTerm(format!("org_id = {}", tenant.sql_literal()))
    }

    /// The term's text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TenantTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The empty tenant's term, for the crate's own unit tests.
#[cfg(test)]
pub(crate) fn empty_term() -> &'static TenantTerm {
    static TERM: std::sync::LazyLock<TenantTerm> = std::sync::LazyLock::new(|| {
        TenantTerm::of(&Tenant::from_header(None, false).expect("the empty tenant"))
    });
    &TERM
}

/// The service graph's marker reads: the job's markers are one set per run,
/// stored under the reserved empty tenant.
pub const MARKER_TERM: &str = "org_id = '' AND conn_type = ''";

/// One lexical token of a statement, at its parenthesis depth: a bracket
/// carries the depth outside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tok {
    Word,
    Quoted,
    Open,
    Close,
    Dot,
    Other,
}

struct Token {
    kind: Tok,
    start: usize,
    end: usize,
    depth: usize,
}

/// `sql` as tokens. A quoted text — `'…'`, `"…"` or `` `…` ``, with
/// backslash escapes — is one token, so nothing inside a literal is read as
/// a keyword or a bracket.
fn tokens(sql: &str) -> Vec<Token> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let (mut i, mut depth) = (0, 0usize);
    while i < b.len() {
        let c = b[i];
        let start = i;
        let kind = match c {
            b'\'' | b'"' | b'`' => {
                i += 1;
                while i < b.len() && b[i] != c {
                    i += if b[i] == b'\\' { 2 } else { 1 };
                }
                i = (i + 1).min(b.len());
                Tok::Quoted
            }
            b'(' => {
                i += 1;
                out.push(Token {
                    kind: Tok::Open,
                    start,
                    end: i,
                    depth,
                });
                depth += 1;
                continue;
            }
            b')' => {
                i += 1;
                depth = depth.saturating_sub(1);
                Tok::Close
            }
            b'.' => {
                i += 1;
                Tok::Dot
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1;
                }
                Tok::Word
            }
            c if c.is_ascii_whitespace() => {
                i += 1;
                continue;
            }
            _ => {
                i += 1;
                Tok::Other
            }
        };
        out.push(Token {
            kind,
            start,
            end: i,
            depth,
        });
    }
    out
}

/// How a table source is introduced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// After `FROM`: a trace table here is scoped by the `WHERE` that
    /// follows at the same depth.
    From,
    /// After `JOIN` (any form), or after a comma in a `FROM` clause: the
    /// `WHERE` that follows is the whole statement's, so a trace table
    /// here must be a subquery with its own.
    Joined,
    /// After `IN` (and `GLOBAL IN`, `NOT IN`): a bare name is a table set.
    InSet,
}

/// Table functions that read nothing a request stores.
const HARMLESS_TABLE_FUNCTIONS: [&str; 6] = [
    "numbers",
    "numbers_mt",
    "zeros",
    "zeros_mt",
    "generateseries",
    "generate_series",
];

/// Table functions whose argument is a query: the read inside is checked
/// as any other.
const QUERY_TABLE_FUNCTIONS: [&str; 2] = ["view", "viewifpermitted"];

/// Table functions that run a query, or read a table, on other servers:
/// accepted only when their argument is a [`QUERY_TABLE_FUNCTIONS`] call.
const REMOTE_TABLE_FUNCTIONS: [&str; 4] =
    ["cluster", "clusterallreplicas", "remote", "remotesecure"];

/// Words that end a `FROM` clause's list of sources at their depth.
const CLAUSE_WORDS: [&str; 18] = [
    "WHERE",
    "PREWHERE",
    "GROUP",
    "ORDER",
    "LIMIT",
    "HAVING",
    "SETTINGS",
    "UNION",
    "ON",
    "USING",
    "SELECT",
    "WINDOW",
    "QUALIFY",
    "FORMAT",
    "INTERSECT",
    "EXCEPT",
    "SAMPLE",
    "WITH",
];

/// Whether `sql` reads every trace table under `term` (issue #738).
///
/// Every table source is inspected: what follows `FROM`, every `JOIN`
/// form, each comma-joined source of a `FROM` clause, and a bare name
/// after `IN`. A CTE's body and every subquery are read as statements of
/// their own, so their sources are inspected the same way.
///
/// * A trace table after `FROM` — a name whose last dot-separated part is
///   one of `tables` — must have, at that `FROM`'s depth, a first `WHERE`
///   (before the depth closes or a `UNION`, `GROUP BY`, `ORDER BY`,
///   `LIMIT`, `HAVING` or `SETTINGS` at that depth) beginning `<term> AND`
///   (or, on `service_edges`, [`MARKER_TERM`]), with no `OR` at that depth
///   inside it.
/// * A trace table as a join target, a comma-joined source or an `IN` set
///   is refused: it must be a subquery whose own `WHERE` carries the term.
/// * A table function is refused unless it reads nothing a request stores
///   ([`HARMLESS_TABLE_FUNCTIONS`]), takes a query whose reads are checked
///   ([`QUERY_TABLE_FUNCTIONS`]), or runs such a query remotely
///   ([`REMOTE_TABLE_FUNCTIONS`] with a `view(…)` argument). One the check
///   cannot see into is refused.
///
/// The refusal names the source and its byte offset.
pub fn check(sql: &str, term: &TenantTerm, tables: &[String]) -> Result<(), String> {
    let toks = tokens(sql);
    let text = |t: &Token| &sql[t.start..t.end];
    let word = |t: &Token, w: &str| t.kind == Tok::Word && text(t).eq_ignore_ascii_case(w);
    let refuse =
        |what: &str, at: usize| Err(format!("statement not tenant-scoped: {what} at byte {at}"));
    // The clause at `depth` ends at `toks[k]`.
    let ends = |k: usize, depth: usize| -> bool {
        let t = &toks[k];
        if t.kind == Tok::Close && t.depth < depth {
            return true;
        }
        if t.depth != depth || t.kind != Tok::Word {
            return false;
        }
        let next_by = toks.get(k + 1).is_some_and(|n| word(n, "BY"));
        ["UNION", "LIMIT", "HAVING", "SETTINGS"]
            .iter()
            .any(|w| word(t, w))
            || ((word(t, "GROUP") || word(t, "ORDER")) && next_by)
    };

    // Every source: where it starts, how it was introduced, and the byte
    // of the word or comma that introduced it.
    let mut sources: Vec<(usize, Source, usize)> = Vec::new();
    let mut in_from: Vec<bool> = vec![false];
    for (i, t) in toks.iter().enumerate() {
        let d = t.depth;
        if in_from.len() < d + 2 {
            in_from.resize(d + 2, false);
        }
        if t.kind == Tok::Open {
            in_from[d + 1] = false;
            continue;
        }
        if word(t, "FROM") {
            in_from[d] = true;
            sources.push((i + 1, Source::From, t.start));
        } else if word(t, "JOIN") {
            let array = i > 0 && word(&toks[i - 1], "ARRAY");
            if array {
                // An array join's list names arrays, not tables.
                in_from[d] = false;
            } else {
                in_from[d] = true;
                sources.push((i + 1, Source::Joined, t.start));
            }
        } else if t.kind == Tok::Other && text(t) == "," && in_from[d] {
            sources.push((i + 1, Source::Joined, t.start));
        } else if word(t, "IN") {
            sources.push((i + 1, Source::InSet, t.start));
        } else if t.kind == Tok::Word && CLAUSE_WORDS.iter().any(|w| word(t, w)) {
            in_from[d] = false;
        }
    }

    for (start, how, at) in sources {
        // The name: parts joined by dots, each a word or a quoted name.
        let mut j = start;
        let mut last = None;
        while let Some(t) = toks.get(j) {
            match t.kind {
                Tok::Word => last = Some(text(t).to_string()),
                Tok::Quoted if text(t).starts_with('`') => {
                    last = Some(text(t).trim_matches('`').to_string())
                }
                _ => break,
            }
            j += 1;
            if toks.get(j).is_some_and(|d| d.kind == Tok::Dot) {
                j += 1;
            } else {
                break;
            }
        }
        // A subquery, a tuple or a literal: its own reads are sources of
        // their own.
        let Some(name) = last else {
            continue;
        };
        if toks.get(j).is_some_and(|t| t.kind == Tok::Open) {
            let f = name.to_ascii_lowercase();
            let known = HARMLESS_TABLE_FUNCTIONS.contains(&f.as_str())
                || QUERY_TABLE_FUNCTIONS.contains(&f.as_str())
                || REMOTE_TABLE_FUNCTIONS.contains(&f.as_str());
            if how == Source::InSet && !known && !f.starts_with("merge") {
                // After `IN` the server evaluates a call as a scalar function
                // (`x IN tuple(…)`, `x IN arrayConcat(…)`): a value, not a
                // table. The names that read tables are refused below.
                continue;
            }
            if HARMLESS_TABLE_FUNCTIONS.contains(&f.as_str())
                || QUERY_TABLE_FUNCTIONS.contains(&f.as_str())
            {
                continue;
            }
            if REMOTE_TABLE_FUNCTIONS.contains(&f.as_str()) {
                let inner = toks[j].depth + 1;
                let mut k = j + 1;
                let mut viewed = false;
                while k + 1 < toks.len() && !(toks[k].kind == Tok::Close && toks[k].depth < inner) {
                    if toks[k].depth == inner
                        && QUERY_TABLE_FUNCTIONS.iter().any(|q| word(&toks[k], q))
                        && toks[k + 1].kind == Tok::Open
                    {
                        viewed = true;
                    }
                    k += 1;
                }
                if viewed {
                    continue;
                }
            }
            return refuse(&format!("table function {name}"), at);
        }
        let table = name;
        if !tables.contains(&table) {
            continue;
        }
        if how != Source::From {
            return refuse(&format!("{table} read as a join target or a set"), at);
        }
        let refused = || refuse(&format!("FROM {table}"), at);
        let depth = toks[start - 1].depth;
        let mut k = j;
        let where_at = loop {
            if k >= toks.len() || ends(k, depth) {
                break None;
            }
            if toks[k].depth == depth && word(&toks[k], "WHERE") {
                break Some(k);
            }
            k += 1;
        };
        let Some(w) = where_at else {
            return refused();
        };
        let body = sql[toks[w].end..].trim_start();
        let leads = |lead: &str| {
            body.strip_prefix(lead)
                .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_whitespace()))
        };
        let marker = table == "service_edges"
            && body
                .strip_prefix(MARKER_TERM)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with([' ', '\n', ')']));
        let scoped = leads(&format!("{term} AND")) || marker;
        if !scoped {
            return refused();
        }
        let mut k = w + 1;
        while k < toks.len() && !ends(k, depth) {
            if toks[k].depth == depth && word(&toks[k], "OR") {
                return refused();
            }
            k += 1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn term(name: &str) -> TenantTerm {
        let value = http::HeaderValue::from_str(name).expect("a header value");
        TenantTerm::of(&Tenant::from_header(Some(&value), false).expect("a valid tenant"))
    }

    fn tables() -> Vec<String> {
        [
            "resources",
            "spans",
            "tag_names",
            "tag_values",
            "trace_landing",
            "traces",
            "service_edges",
            "tag_bucket_values",
            "error_spans",
            "service_bucket_resources",
            "spans_dist",
            "error_spans_dist",
            "traces_dist",
        ]
        .iter()
        .map(|t| t.to_string())
        .collect()
    }

    /// **T-G (issue #738): the check refuses every unscoped read.** Each
    /// refused statement reads a trace table without the request's term
    /// leading its `WHERE`, or with an `OR` beside it; each accepted one
    /// carries the term at every site, or reads no trace table there.
    #[test]
    fn the_check_refuses_every_unscoped_read() {
        let a = term("a");
        let tables = tables();
        for sql in [
            "SELECT 1 FROM spans WHERE x",
            "SELECT 1 FROM spans_dist WHERE x",
            "SELECT 1 FROM db.traces WHERE x",
            "SELECT k FROM tag_names ORDER BY k",
            "SELECT 1 FROM spans WHERE org_id = 'b' AND x",
            "SELECT 1 FROM spans WHERE org_id = 'a' AND x OR y",
            "SELECT 1 FROM spans WHERE org_id = '' AND conn_type = ''",
        ] {
            assert!(
                check(sql, &a, &tables).is_err(),
                "refused, as tenant 'a': {sql}"
            );
        }
        for sql in [
            "SELECT 1 FROM spans WHERE org_id = 'a' AND x \
             AND trace_id IN (SELECT trace_id FROM traces_dist WHERE org_id = 'a' AND y) \
             UNION ALL SELECT 2 FROM db.resources WHERE org_id = 'a' AND z",
            "SELECT 1 FROM spans WHERE org_id = 'a' AND (x OR y)",
            "SELECT 'FROM spans' FROM system.one",
            "SELECT 1 FROM service_edges FINAL WHERE org_id = '' AND conn_type = ''",
            "SELECT 1 FROM db.service_edges FINAL WHERE org_id = 'a' AND conn_type IN ('rpc')",
            "SELECT engine_full FROM system.tables WHERE database = currentDatabase()",
        ] {
            assert_eq!(
                check(sql, &a, &tables),
                Ok(()),
                "accepted, as tenant 'a': {sql}"
            );
        }
    }

    /// **T-G, every table source (issue #738).** A trace table read as a
    /// join target, a comma-joined table, an `IN` set, inside a CTE, or
    /// through a table function is refused unscoped and accepted only
    /// when the read is a subquery whose own `WHERE` leads with the term.
    /// A table function the check cannot see into is refused.
    #[test]
    fn the_check_refuses_an_unscoped_read_in_every_table_source() {
        let a = term("a");
        let tables = tables();
        let scoped_spans = "(SELECT trace_id FROM spans WHERE org_id = 'a' AND x)";
        let unscoped_spans = "(SELECT trace_id FROM spans WHERE x)";
        let joins = [
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "LEFT OUTER JOIN",
            "RIGHT JOIN",
            "FULL OUTER JOIN",
            "CROSS JOIN",
            "ANY LEFT JOIN",
            "ALL INNER JOIN",
            "ASOF LEFT JOIN",
            "LEFT SEMI JOIN",
            "GLOBAL INNER JOIN",
            "GLOBAL ANY LEFT JOIN",
        ];
        let base = "SELECT 1 FROM system.one AS o";
        let mut refused: Vec<String> = Vec::new();
        let mut accepted: Vec<String> = Vec::new();
        for j in joins {
            refused.push(format!("{base} {j} spans AS s ON o.dummy = s.kind WHERE x"));
            refused.push(format!("{base} {j} db.spans_dist AS s USING (trace_id)"));
            refused.push(format!("{base} {j} {unscoped_spans} AS s USING (trace_id)"));
            accepted.push(format!("{base} {j} {scoped_spans} AS s USING (trace_id)"));
        }
        // Comma joins.
        refused.push(format!("{base}, spans AS s WHERE org_id = 'a' AND x"));
        refused.push(format!("{base}, {unscoped_spans} AS s"));
        accepted.push(format!("{base}, {scoped_spans} AS s"));
        // `IN` and `GLOBAL IN` sets.
        for op in ["IN", "NOT IN", "GLOBAL IN", "GLOBAL NOT IN"] {
            refused.push(format!("{base} WHERE x {op} spans"));
            refused.push(format!("{base} WHERE x {op} db.traces_dist"));
            refused.push(format!("{base} WHERE x {op} {unscoped_spans}"));
            accepted.push(format!("{base} WHERE x {op} {scoped_spans}"));
        }
        // CTE bodies and references.
        refused.push("WITH c AS (SELECT * FROM spans WHERE x) SELECT * FROM c".to_string());
        refused.push(format!(
            "WITH c AS {unscoped_spans} SELECT * FROM c JOIN spans USING (trace_id)"
        ));
        accepted.push(format!("WITH c AS {scoped_spans} SELECT * FROM c"));
        // Table functions: refused unless the read is a scoped one inside.
        for f in [
            "merge('db', '^spans$')",
            "merge(db, '^(spans|traces)$')",
            "remote('h', 'db', 'spans')",
            "remote('h', 'db.spans')",
            "remoteSecure('h', 'db', 'traces')",
            "cluster('c', 'db', 'spans')",
            "cluster('c', db.spans)",
            "clusterAllReplicas('c', 'db', 'spans')",
            "url('http://h/x', 'TSV')",
            "file('x.tsv')",
        ] {
            refused.push(format!("SELECT 1 FROM {f} WHERE org_id = 'a' AND x"));
            refused.push(format!("{base} JOIN {f} AS s USING (trace_id)"));
            // After `IN` the server evaluates a call as a scalar function, and
            // knows none of these names; the check refuses them anyway.
            if !f.starts_with("url") && !f.starts_with("file") {
                refused.push(format!("{base} WHERE x IN {f}"));
            }
        }
        // A scalar function after `IN` is a value.
        accepted.push(format!("{base} WHERE x IN arrayConcat([1], [2])"));
        accepted.push(format!("{base} WHERE (x, y) IN tuple(1, 2)"));
        for f in [
            "view",
            "cluster('c', view",
            "clusterAllReplicas('c', view",
            "remote('h', view",
        ] {
            let close = if f == "view" { ")" } else { "))" };
            refused.push(format!(
                "SELECT 1 FROM {f}(SELECT 1 FROM db.spans WHERE x){close}"
            ));
            accepted.push(format!(
                "SELECT 1 FROM {f}(SELECT 1 FROM db.spans WHERE org_id = 'a' AND x){close}"
            ));
        }
        accepted.push("SELECT number FROM numbers(10)".to_string());
        // Not tables: an array join over a column, a column named like a
        // table, a scalar function.
        accepted.push(
            "SELECT 1 FROM spans ARRAY JOIN events AS e, links AS l WHERE org_id = 'a' AND x"
                .to_string(),
        );
        accepted.push(
            "SELECT m.spans AS spans, arrayJoin(spans) AS s FROM (SELECT 1 AS spans) AS m"
                .to_string(),
        );
        let mut wrong = Vec::new();
        for sql in &refused {
            if check(sql, &a, &tables).is_ok() {
                wrong.push(format!("accepted, want refused: {sql}"));
            }
        }
        for sql in &accepted {
            if let Err(e) = check(sql, &a, &tables) {
                wrong.push(format!("refused ({e}), want accepted: {sql}"));
            }
        }
        assert!(
            wrong.is_empty(),
            "{} of {} cases wrong:\n{}",
            wrong.len(),
            refused.len() + accepted.len(),
            wrong.join("\n")
        );
    }
}
