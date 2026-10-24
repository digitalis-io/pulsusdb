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

/// Whether `sql` reads every trace table under `term` (issue #738).
///
/// For every `FROM` followed by a name whose last dot-separated part is one
/// of `tables`, the first `WHERE` at that `FROM`'s depth — before the depth
/// closes or a `UNION`, `GROUP BY`, `ORDER BY`, `LIMIT`, `HAVING` or
/// `SETTINGS` at that depth — must begin `<term> AND` (or, on
/// `service_edges`, [`MARKER_TERM`]), and no `OR` may stand at that depth
/// inside it. The refusal names the `FROM` and its byte offset.
pub fn check(sql: &str, term: &TenantTerm, tables: &[String]) -> Result<(), String> {
    let toks = tokens(sql);
    let text = |t: &Token| &sql[t.start..t.end];
    let word = |t: &Token, w: &str| t.kind == Tok::Word && text(t).eq_ignore_ascii_case(w);
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
    for (i, from) in toks.iter().enumerate() {
        if !word(from, "FROM") {
            continue;
        }
        // The name: parts joined by dots, each a word or a quoted name.
        let mut j = i + 1;
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
        let Some(table) = last.filter(|name| tables.iter().any(|t| t == name)) else {
            continue;
        };
        let refused = || {
            format!(
                "statement not tenant-scoped: FROM {table} at byte {}",
                from.start
            )
        };
        let depth = from.depth;
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
            return Err(refused());
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
            return Err(refused());
        }
        let mut k = w + 1;
        while k < toks.len() && !ends(k, depth) {
            if toks[k].depth == depth && word(&toks[k], "OR") {
                return Err(refused());
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
}
