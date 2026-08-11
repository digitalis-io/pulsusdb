//! `cargo xtask bench match-flag-head` — the issue #331 remedy
//! benchmark, committed as a runnable artifact (fix round 2) because
//! two unshared benchmarks of the same remedies disagreed about the
//! anchored rewrite's bucket and nobody could adjudicate them.
//!
//! One command, printing only, asserting nothing:
//!
//! ```text
//! podman run -d --name pulsus-ch-bench -p 19134:8123 \
//!     clickhouse/clickhouse-server:26.3
//! cargo run -p xtask -- bench match-flag-head \
//!     --http-url http://127.0.0.1:19134 --database pulsus_match_bench \
//!     --reps 9
//! ```
//!
//! **Data**: [`ROWS`] rows, fully deterministic server-side (every
//! string is a function of `number` through `cityHash64`, no RNG, no
//! seed to mismatch): one row in [`SELECTIVITY`] is
//! `ERROR-<n>-timeout-tail-<hex>`, the rest `log line <n> <hex>`.
//!
//! **Protocol**: one discarded warm pass per query, then `--reps`
//! timed passes taken ROUND-ROBIN across the whole work list — not
//! per-query batches — so slow drift in machine load lands evenly on
//! every row of the table (review evidence: a non-interleaved run of
//! this same comparison inverted the bucket ordering entirely).
//! `min` is the headline number (best-of-N), `med` is printed beside
//! it.
//!
//! **Calibration first, conclusions second**: the three calibration
//! rows establish what this machine's prefilter looks like — an
//! ABSENT extracted-literal pattern must time like the pure substring
//! scan and well below the no-literal full-RE2 scan, or the machine
//! cannot separate the buckets and the remedy rows below prove
//! nothing. Every remedy row is then read by which bucket its
//! zero-candidate (`ABSENT` core) variant lands in.
//!
//! **Shapes**: the three render templates the read path emits —
//! unanchored (LogQL line filters), `^(?:…)$` (LogQL/TraceQL
//! matchers), `(?-s)^(?:…)$` (PromQL matchers) — composed here exactly
//! as `pulsus-read`'s `logql::escape` composes them (its unit tests
//! pin the byte shapes), with the rewrite text taken from the
//! PRODUCTION classifier `pulsus_re2::clickhouse_match_strategy`,
//! never hand-spelled.

use std::time::Instant;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, Idempotency, QuerySettings, Row};
use pulsus_re2::{
    ClickhouseMatchStrategy, clickhouse_match_head_rewrite, clickhouse_match_strategy,
};

use super::BenchArgs;

#[derive(Row, serde::Serialize, serde::Deserialize)]
struct CountRow {
    n: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize)]
struct VersionRow {
    v: String,
}

/// Doubles literal `?` before dispatch — the execution-boundary
/// contract for raw SQL through the `clickhouse` crate's `SqlBuilder`
/// (a bare `?` is a bind placeholder), mirrored from the live test
/// suites. Load-bearing here: every rendered pattern carries `(?`.
fn double_placeholders(sql: &str) -> String {
    sql.replace('?', "??")
}

async fn one_row<R: pulsus_clickhouse::ChRow>(client: &ChClient, sql: &str) -> anyhow::Result<R> {
    let sql = double_placeholders(sql);
    let mut stream = client
        .query_stream::<R>(&sql, &QuerySettings::new())
        .await?;
    let mut out = None;
    while let Some(row) = stream.next().await {
        out = Some(row?);
    }
    out.ok_or_else(|| anyhow::anyhow!("no row from {sql:?}"))
}

/// Row count. Large enough that the prefilter-vs-full-RE2 gap dwarfs
/// per-query overhead, small enough to build in seconds.
const ROWS: u64 = 5_000_000;
/// One row in this many carries the `ERROR-…-timeout` payload.
const SELECTIVITY: u64 = 1_000;

/// The pattern cores, chosen to probe the regimes that decided (and
/// un-decided) earlier rounds:
/// * `SELECTIVE` — literal present in 1/[`SELECTIVITY`] rows (the
///   ordinary observability filter);
/// * `ABSENT` — literal in no row: a working prefilter means RE2 runs
///   on ~zero rows, so this is the bucket classifier;
/// * `NONSELECTIVE` — literal in ~every row: the prefilter prunes
///   nothing and can only add its scan cost.
///
/// Each core runs in TWO spellings (fix round 3): literal-leading, and
/// wrapped in leading/trailing `.*` — because the round-1/2 dispute
/// resolved to exactly this: a leading `.*` alone leaves the baseline
/// in the prefilter bucket, but a leading `.*` INSIDE the rewritten
/// flag group (`(?s-i:.*ERROR…)`) demotes the rewrite to the full-RE2
/// bucket (isolated under review). `=~".*foo.*"` is an everyday label
/// matcher, so the table must carry the shape that reverses the
/// ordering, not only the shapes that flatter it.
const SELECTIVE: &str = "ERROR.*timeout";
const ABSENT: &str = "ZQWXJKVYNOPE.*x";
const NONSELECTIVE: &str = "log.*line";

/// `.*`-wraps a core — the everyday `=~".*foo.*"` spelling.
fn dotstar(core: &str) -> String {
    format!(".*{core}.*")
}

/// A ClickHouse string literal (same escapes as the test suites').
fn sql_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

/// The affected-pattern core: the user wrote `(?s:CORE)`.
fn affected(core: &str) -> String {
    format!("(?s:{core})")
}

/// The rewrite text for `(?s:CORE)`, from the production
/// ROUTING-INDEPENDENT seam — never hand-spelled, and deliberately not
/// the routed strategy: the bench measures BOTH remedies at every
/// shape-point precisely so the routing can be judged against the
/// table (fix round 4: taking the text from the routed strategy broke
/// the bench the moment the shape gate landed, because the dotstar
/// cores correctly stopped classifying `RewriteHeads`).
fn rewrite_of(core: &str) -> String {
    clickhouse_match_head_rewrite(&affected(core))
        .expect("premise: every bench core is a no-i affected pattern (gated by unit test)")
}

/// What the SHIPPED strategy routes this core's affected pattern to —
/// printed beside the table so the reader can see which measured row
/// production actually takes.
fn routed(core: &str) -> &'static str {
    match clickhouse_match_strategy(&affected(core)) {
        ClickhouseMatchStrategy::RewriteHeads(_) => "rewrite",
        ClickhouseMatchStrategy::NeverMatchArm => "defeat-arm",
        ClickhouseMatchStrategy::Verbatim => "verbatim (unexpected)",
    }
}

/// One render template. Compositions mirror `logql::escape`
/// byte-for-byte (pinned by that module's unit tests).
#[derive(Clone, Copy)]
enum Shape {
    Unanchored,
    LogqlAnchored,
    Promql,
}

impl Shape {
    fn baseline(self, core: &str) -> String {
        match self {
            Shape::Unanchored => core.to_string(),
            Shape::LogqlAnchored => format!("^(?:{core})$"),
            Shape::Promql => format!("(?-s)^(?:{core})$"),
        }
    }
    /// Today's broken rendering of the affected pattern (pre-#331).
    fn broken(self, core: &str) -> String {
        self.baseline(&affected(core))
    }
    /// The `-i` rewrite rendering.
    fn rewrite(self, core: &str) -> String {
        self.baseline(&rewrite_of(core))
    }
    /// The never-matching-arm rendering.
    fn defeat(self, core: &str) -> String {
        match self {
            Shape::Unanchored => format!("(?:{})|$.", affected(core)),
            Shape::LogqlAnchored => format!("^(?:{})$|$.", affected(core)),
            Shape::Promql => format!("(?-s)^(?:{})$|$.", affected(core)),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Shape::Unanchored => "unanchored",
            Shape::LogqlAnchored => "logql-anch",
            Shape::Promql => "promql",
        }
    }
}

pub async fn run(args: BenchArgs) -> anyhow::Result<()> {
    let (server, http_port) = super::parse_http_url(&args.http_url)?;
    let client = ChClient::new(ChConnConfig {
        server,
        http_port,
        database: "default".to_string(),
        user: args.user.clone(),
        password: args.password.clone(),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: std::time::Duration::from_secs(600),
        ..ChConnConfig::default()
    })
    .await?;
    let db = &args.database;
    let table = format!("{db}.match_flag_head");
    let settings = QuerySettings::new();
    let exec = |sql: String| {
        let client = &client;
        let settings = &settings;
        async move {
            client
                .execute(
                    &double_placeholders(&sql),
                    settings,
                    Idempotency::Idempotent,
                )
                .await
        }
    };

    exec(format!("CREATE DATABASE IF NOT EXISTS {db}")).await?;
    exec(format!("DROP TABLE IF EXISTS {table}")).await?;
    exec(format!(
        "CREATE TABLE {table} (s String) ENGINE = MergeTree ORDER BY tuple() AS \
         SELECT if(number % {SELECTIVITY} = 0, \
                   concat('ERROR-', toString(number), '-timeout-tail-', hex(cityHash64(number))), \
                   concat('log line ', toString(number), ' ', hex(cityHash64(number + {ROWS})))) \
                AS s \
         FROM numbers({ROWS})"
    ))
    .await?;

    // The work list: calibration first, then the remedy grid.
    let mut work: Vec<(String, String)> = vec![
        (
            "CAL pure substring scan (trivial)".into(),
            "ZQWXJKVYNOPE".into(),
        ),
        ("CAL absent literal + .* (prefilter)".into(), ABSENT.into()),
        (
            "CAL no literal (full RE2)".into(),
            "[ZQ][QW][WX][XJ][JK][KV]".into(),
        ),
    ];
    for (regime, base_core) in [
        ("ABSENT", ABSENT),
        ("SELECTIVE", SELECTIVE),
        ("NONSEL", NONSELECTIVE),
    ] {
        for (spelling, core) in [("lit", base_core.to_string()), (".*", dotstar(base_core))] {
            for shape in [Shape::Unanchored, Shape::LogqlAnchored, Shape::Promql] {
                let n = shape.name();
                let tag = format!("{regime}/{spelling} {n}");
                work.push((format!("{tag} baseline"), shape.baseline(&core)));
                work.push((format!("{tag} broken-today"), shape.broken(&core)));
                work.push((format!("{tag} rewrite"), shape.rewrite(&core)));
                work.push((format!("{tag} defeat-arm"), shape.defeat(&core)));
            }
        }
    }

    let count_sql = |pattern: &str| {
        format!(
            "SELECT count() AS n FROM {table} WHERE match(s, {})",
            sql_literal(pattern)
        )
    };

    // Warm pass, and record match counts (correctness context — the
    // broken-today rows are the silent zeros this issue fixed).
    let mut counts: Vec<u64> = Vec::with_capacity(work.len());
    for (_, pattern) in &work {
        counts.push(one_row::<CountRow>(&client, &count_sql(pattern)).await?.n);
    }

    // Timed passes, ROUND-ROBIN across the whole list.
    let reps = args.reps.max(1);
    let mut times: Vec<Vec<f64>> = vec![Vec::with_capacity(reps); work.len()];
    for _ in 0..reps {
        for (i, (_, pattern)) in work.iter().enumerate() {
            let t0 = Instant::now();
            exec(count_sql(pattern)).await?;
            times[i].push(t0.elapsed().as_secs_f64() * 1e3);
        }
    }

    let version = one_row::<VersionRow>(&client, "SELECT version() AS v")
        .await?
        .v;
    println!(
        "issue #331 match() remedy benchmark — clickhouse {version}, {ROWS} rows, \
         selectivity 1/{SELECTIVITY}, best-of-{reps} round-robin (min; med beside)"
    );
    println!("{:-<100}", "");
    for (i, (name, pattern)) in work.iter().enumerate() {
        let mut sorted = times[i].clone();
        sorted.sort_by(|a, b| a.total_cmp(b));
        let min = sorted.first().copied().unwrap_or(f64::NAN);
        let med = sorted[sorted.len() / 2];
        println!(
            "{name:36} min={min:8.1}ms med={med:8.1}ms count={:>8}  {pattern}",
            counts[i]
        );
    }
    println!("{:-<100}", "");
    for (name, core) in [
        ("SELECTIVE/lit", SELECTIVE.to_string()),
        ("ABSENT/lit", ABSENT.to_string()),
        ("NONSEL/lit", NONSELECTIVE.to_string()),
        ("SELECTIVE/.*", dotstar(SELECTIVE)),
        ("ABSENT/.*", dotstar(ABSENT)),
        ("NONSEL/.*", dotstar(NONSELECTIVE)),
    ] {
        println!(
            "shipped routing: {name:14} (?s:{core}) -> {}",
            routed(&core)
        );
    }
    println!(
        "read the table via the calibration rows: a remedy whose ABSENT variant times like \
         'CAL absent literal' kept the prefilter on this machine; one that times like \
         'CAL no literal' runs full RE2. Both remedies are measured at every shape-point \
         regardless of the shipped routing above. No numbers are asserted."
    );
    exec(format!("DROP TABLE IF EXISTS {table}")).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fix round 4 (review observation): nothing gated the bench, so a
    /// classifier change broke it while the whole suite stayed green.
    /// This pins the bench's one premise — every core, in both
    /// spellings, is a no-i affected pattern, so BOTH remedies exist to
    /// measure — and reddens the moment either seam stops answering.
    #[test]
    fn every_bench_core_supports_both_remedies() {
        for core in [SELECTIVE, ABSENT, NONSELECTIVE] {
            for spelled in [core.to_string(), dotstar(core)] {
                // Exercise the bench's OWN construction path for every
                // rendering — `shape.rewrite` panics through
                // `rewrite_of` if the premise breaks, which is exactly
                // the round-3 failure this gate exists to catch.
                for shape in [Shape::Unanchored, Shape::LogqlAnchored, Shape::Promql] {
                    let _ = shape.baseline(&spelled);
                    let _ = shape.broken(&spelled);
                    let _ = shape.rewrite(&spelled);
                    let _ = shape.defeat(&spelled);
                }
                let _ = routed(&spelled);
                let pattern = affected(&spelled);
                assert!(
                    !matches!(
                        clickhouse_match_strategy(&pattern),
                        ClickhouseMatchStrategy::Verbatim
                    ),
                    "{pattern:?}: no longer classified affected — the bench premise moved"
                );
            }
        }
    }
}
