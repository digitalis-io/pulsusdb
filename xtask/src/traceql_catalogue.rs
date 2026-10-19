//! `xtask traceql-catalogue` (issue #600): every catalogue query through the
//! shipped planner and engine, one JSON line per query.

/// The command line.
#[derive(clap::Parser)]
pub struct Args {
    /// The ClickHouse server's host.
    #[arg(long)]
    host: String,
    /// Its HTTP port.
    #[arg(long)]
    port: u16,
    /// The database holding the fixture.
    #[arg(long)]
    database: String,
    /// `crates/pulsus-traceql/tests/corpus`.
    #[arg(long)]
    corpus: String,
    /// `docs/TraceQL/measure/catalogue-extra.tsv`.
    #[arg(long)]
    extra: String,
    /// The window's start, inclusive, in Unix nanoseconds.
    #[arg(long)]
    start_ns: i64,
    /// The window's end, exclusive, in Unix nanoseconds.
    #[arg(long)]
    end_ns: i64,
}

/// The most statements one request may issue (`T-Q2`).
const MAX_STATEMENTS: usize = 100;

/// The first `MAX_STATEMENTS` of a request's statements, and whether the
/// log held more: the read asks for one row past the ceiling.
fn cap_statements(_rows: Vec<String>) -> (Vec<String>, bool) {
    (Vec::new(), false)
}

pub async fn run(_args: Args) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("statement {i}")).collect()
    }

    /// `T-Q2`'s ceiling: a request with exactly `MAX_STATEMENTS` statements
    /// keeps them all and is not over.
    #[test]
    fn a_request_at_the_ceiling_keeps_every_statement() {
        let (kept, over) = cap_statements(rows(MAX_STATEMENTS));
        assert_eq!(kept, rows(MAX_STATEMENTS));
        assert!(!over);
    }

    /// The sentinel row: one more than the ceiling keeps the first
    /// `MAX_STATEMENTS` and reports the request over.
    #[test]
    fn the_sentinel_row_marks_the_request_over() {
        let (kept, over) = cap_statements(rows(MAX_STATEMENTS + 1));
        assert_eq!(kept, rows(MAX_STATEMENTS));
        assert!(over);
    }
}
