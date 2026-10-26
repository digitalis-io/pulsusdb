//! `schema/schema.sql` and `schema/schema.sh` — the file the schema is
//! written in and the script that applies it.
//!
//! The binary does not create schema. These cases hold the properties the
//! deleted controller used to hold structurally:
//!
//! - every `{{token}}` renders, in both modes;
//! - ClickHouse's own `{shard}` and `{replica}` macros reach the server as
//!   those exact characters (a macro replaced by a constant is accepted by
//!   the server and silently gives every shard one replica set);
//! - every replication path names the table of the `CREATE` it sits in;
//! - the file holds only `CREATE` statements: it is the schema, not a
//!   script that walks one forward, and `schema/schema.sh` drops the
//!   database before applying it;
//! - a statement ends at a line ending in `;` and nowhere else, which is
//!   what lets the script send one statement per request;
//! - the script and this crate render the identical text, so the two
//!   renderers cannot drift.

use std::process::Command;
use std::time::Duration;

use pulsus_schema::{RenderCtx, SCHEMA_SQL, rendered, rendered_statements};

fn single() -> RenderCtx {
    RenderCtx::for_tests("pulsus", 7)
}

fn clustered() -> RenderCtx {
    RenderCtx {
        cluster: Some("prod".to_string()),
        ..RenderCtx::for_tests("pulsus", 7)
    }
}

/// Statements beginning with `what`, in the rendered output.
fn starting_with(stmts: &[String], what: &str) -> usize {
    stmts
        .iter()
        .filter(|s| s.trim_start().starts_with(what))
        .count()
}

/// The repository root, from this test binary's own crate directory.
fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the crate directory resolves")
}

/// The inventory, single-node: one database, 29 tables, 26 views, and
/// nothing else. No `Replicated*` engine, no `Distributed` wrapper, no `ON
/// CLUSTER`.
#[test]
fn the_single_node_render_is_the_whole_inventory_with_every_token_resolved() {
    let stmts = rendered_statements(&single());
    assert_eq!(stmts.len(), 56, "statement count");
    assert_eq!(starting_with(&stmts, "CREATE DATABASE"), 1);
    assert_eq!(starting_with(&stmts, "CREATE TABLE"), 29);
    assert_eq!(starting_with(&stmts, "CREATE MATERIALIZED VIEW"), 26);

    let text = rendered(&single());
    assert!(
        !text.contains("{{"),
        "an unrendered token reached the output"
    );
    assert!(
        !text.contains("Replicated"),
        "single-node renders no replicated engine"
    );
    assert!(
        !text.contains("Distributed"),
        "single-node renders no routing wrapper"
    );
    assert!(
        !text.contains("ON CLUSTER"),
        "single-node renders no ON CLUSTER"
    );
    assert!(!text.contains("{shard}") && !text.contains("{replica}"));
    // The deduplication-window setting follows the engine: the replicated
    // name applies to a `Replicated*` table only.
    assert!(
        !text.contains(" replicated_deduplication_window"),
        "single-node must not carry the replicated engines' setting name"
    );
    assert!(text.contains("non_replicated_deduplication_window = 10000"));
}

/// The inventory, clustered: the same statements plus 18 `_dist` wrappers,
/// `ON CLUSTER` on every one, and the macros intact.
#[test]
fn the_clustered_render_adds_the_wrappers_and_keeps_the_server_macros() {
    let stmts = rendered_statements(&clustered());
    assert_eq!(stmts.len(), 74, "statement count");
    assert_eq!(
        starting_with(&stmts, "CREATE TABLE"),
        47,
        "29 tables + 18 wrappers"
    );
    assert_eq!(starting_with(&stmts, "CREATE MATERIALIZED VIEW"), 26);

    let text = rendered(&clustered());
    assert!(
        !text.contains("{{"),
        "an unrendered token reached the output"
    );
    assert_eq!(
        text.matches("ON CLUSTER 'prod'").count(),
        74,
        "every statement carries ON CLUSTER"
    );

    // `{shard}` and `{replica}` are the server's own macros and must arrive
    // literally. 23 tables take a per-shard replica set and 6 take the
    // cluster-wide one, so 29 paths name `{replica}` and 23 name `{shard}`.
    // The four rollup levels add four per-shard replica sets.
    assert_eq!(
        text.matches("{shard}").count(),
        23,
        "per-shard replica sets"
    );
    assert_eq!(text.matches("{replica}").count(), 29, "replicated tables");
    assert_eq!(
        text.matches("/clickhouse/tables/all/").count(),
        6,
        "cluster-wide replica sets"
    );
    assert!(
        !text.contains("non_replicated_deduplication_window"),
        "clustered must not carry the non-replicated setting name"
    );
    assert!(text.contains(&format!(
        "replicated_deduplication_window_seconds = {}",
        pulsus_schema::DEDUP_WINDOW_SECONDS
    )));
}

/// **Every replication path names the table of its own `CREATE`.**
///
/// The paths are written out per table rather than derived, so a wrong one
/// is a per-table error. A checksum cannot see it — a path that is
/// consistently wrong is consistently current — and the server cannot
/// either: it accepts any path string. This is what catches it.
#[test]
fn every_replication_path_names_the_table_of_its_own_create() {
    let mut table = String::new();
    let mut checked = 0usize;
    // The level block names its tables through `{{level}}`, so the file is
    // read with the default levels expanded (issue #18).
    let expanded = pulsus_schema::expand_levels_for_tests(pulsus_schema::DEFAULT_ROLLUP_LEVELS);
    for line in expanded.lines() {
        // The file's own header explains the two path shapes, so a comment
        // line would be read as a statement.
        if line.starts_with("--") && !line.starts_with("--@") {
            continue;
        }
        let bare = line.trim_start_matches("--@cluster ").trim_start();
        if let Some(rest) = bare.strip_prefix("CREATE TABLE IF NOT EXISTS {{db}}.") {
            // The name may itself carry a token (`log_metrics_{{...}}`),
            // which the path on the other side carries too, so neither is
            // resolved before the comparison.
            table = rest
                .split_whitespace()
                .next()
                .expect("a name follows the database")
                .trim_end_matches("{{on_cluster}}")
                .to_string();
        }
        if let Some(at) = bare.find("/clickhouse/tables/") {
            let path = &bare[at + "/clickhouse/tables/".len()..];
            let (_scope, rest) = path.split_once('/').expect("a scope then the object");
            let named = rest
                .trim_start_matches("{{db}}.")
                .split('\'')
                .next()
                .expect("the path is a quoted literal");
            assert_eq!(
                named, table,
                "replication path names {named} inside {table}'s CREATE"
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 29, "every replicated table's path was checked");
}

/// The splitter the script relies on: a statement ends at a line whose last
/// character is `;`, and no other character in the file is one.
#[test]
fn a_semicolon_ends_a_line_and_occurs_nowhere_else() {
    for ctx in [single(), clustered()] {
        let text = rendered(&ctx);
        let ending = text.lines().filter(|l| l.ends_with(';')).count();
        assert!(ending > 0, "the render carries statements at all");
        assert_eq!(
            ending,
            rendered_statements(&ctx).len(),
            "one statement per line ending in a semicolon"
        );
        for line in text.lines() {
            let found = line.matches(';').count();
            assert!(
                found == 0 || (found == 1 && line.ends_with(';')),
                "a semicolon that does not end its line would cut a statement \
                 in half: {line}"
            );
        }
    }
}

/// No token may be a single brace pair: a substitution expression matching
/// one would eat `{shard}`/`{replica}` and produce a cluster where every
/// shard shares one replica set, which the server accepts without comment.
#[test]
fn the_only_single_brace_names_in_the_file_are_the_two_server_macros() {
    assert!(
        SCHEMA_SQL.lines().count() > 500,
        "the file carries the schema"
    );
    let mut offenders = Vec::new();
    for (n, line) in SCHEMA_SQL.lines().enumerate() {
        let bytes: Vec<char> = line.chars().collect();
        for (i, c) in bytes.iter().enumerate() {
            if *c != '{' || (i > 0 && bytes[i - 1] == '{') {
                continue;
            }
            if bytes.get(i + 1) == Some(&'{') {
                continue;
            }
            let rest: String = bytes[i..].iter().collect();
            let name: String = rest
                .trim_start_matches('{')
                .chars()
                .take_while(|c| c.is_ascii_lowercase() || *c == '_')
                .collect();
            if !matches!(name.as_str(), "shard" | "replica") {
                offenders.push(format!("{}: {line}", n + 1));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "single-brace names other than the server macros: {offenders:?}"
    );
}

/// The file holds only `CREATE` statements, in both variants. It is the
/// schema, not a script that walks a database forward: `schema/schema.sh`
/// drops the database and applies the file to an empty one.
#[test]
fn the_file_holds_only_create_statements() {
    for ctx in [single(), clustered()] {
        let not_create: Vec<String> = rendered_statements(&ctx)
            .iter()
            .map(|s| {
                s.lines()
                    .map(str::trim)
                    .find(|l| !l.is_empty() && !l.starts_with("--"))
                    .unwrap_or("")
                    .to_string()
            })
            .filter(|first| !first.starts_with("CREATE "))
            .collect();
        assert!(
            not_create.is_empty(),
            "statements other than CREATE (cluster {:?}): {not_create:?}",
            ctx.cluster
        );
    }
    let drops: Vec<&str> = SCHEMA_SQL
        .lines()
        .filter(|l| !l.trim_start().starts_with("-- "))
        .filter(|l| l.to_ascii_uppercase().contains("DROP "))
        .collect();
    assert!(drops.is_empty(), "DROP in schema/schema.sql: {drops:?}");
}

/// A family's routing wrappers all shard on one expression: a series'
/// rollups must land on the shard its samples do.
///
/// `metric_labels` is in the metrics family (issue #623): one label row
/// per series, keyed like the series, so its wrapper shards on the same
/// `(metric_name, fingerprint)`. No write goes through it — the view fills
/// it on the node that received the push.
#[test]
fn every_wrapper_in_a_family_shards_on_the_same_expression() {
    let text = rendered(&clustered());
    let mut by_family: std::collections::BTreeMap<&str, std::collections::BTreeSet<String>> =
        Default::default();
    for s in rendered_statements(&clustered()) {
        let Some(at) = s.find("ENGINE = Distributed(") else {
            continue;
        };
        let engine_line = s[at..]
            .lines()
            .next()
            .expect("the engine clause is one line");
        let expr = engine_line
            .trim_end()
            .trim_end_matches(';')
            .trim_end_matches(')')
            .rsplit_once(", ")
            .expect("a sharding expression ends the argument list")
            .1
            .to_string();
        let name = s
            .trim()
            .strip_prefix("CREATE TABLE IF NOT EXISTS pulsus.")
            .expect("a wrapper names its database")
            .to_string();
        let family = if name.starts_with("metric") {
            "metrics"
        } else if name.starts_with("log") {
            "logs"
        } else {
            "traces"
        };
        by_family.entry(family).or_default().insert(expr);
    }
    assert_eq!(by_family.len(), 3, "three families have wrappers");
    for (family, exprs) in &by_family {
        assert_eq!(
            exprs.len(),
            1,
            "{family} wrappers must share one sharding expression: {exprs:?}"
        );
    }
    assert!(text.contains("cityHash64(trace_id)"));
    // F7 (issue #623): the metrics family shards by the series ID alone.
    for table in [
        "metric_samples",
        "metric_hist_samples",
        "metric_exemplars",
        "metric_series",
        "metric_labels",
        "metric_samples_60s",
        "metric_samples_720s",
        "metric_samples_7200s",
        "metric_samples_86400s",
    ] {
        assert!(
            text.contains(&format!("'{table}', cityHash64(fingerprint))")),
            "{table} routes by cityHash64(fingerprint)"
        );
    }
}

/// The storage policy reaches every table's own `SETTINGS` and nothing
/// else: `Distributed` does not accept it and a view has no settings.
#[test]
fn the_storage_policy_renders_into_every_table_and_no_wrapper_or_view() {
    let ctx = RenderCtx {
        storage_policy: Some("hot_cold".to_string()),
        ..clustered()
    };
    let mut on_tables = 0;
    for s in rendered_statements(&ctx) {
        let has = s.contains("storage_policy = 'hot_cold'");
        let is_table =
            s.trim_start().starts_with("CREATE TABLE") && !s.contains("ENGINE = Distributed(");
        if is_table {
            assert!(has, "a table without the configured storage policy: {s}");
            on_tables += 1;
        } else {
            assert!(!has, "the storage policy reached a wrapper or a view: {s}");
        }
    }
    assert_eq!(on_tables, 29);

    assert!(
        !rendered(&clustered()).contains("storage_policy"),
        "no policy configured renders no setting"
    );
}

/// The configured values reach the statements: a hard-coded literal in the
/// file passes at the default and fails here.
#[test]
fn the_configured_values_reach_the_statements() {
    let ctx = RenderCtx {
        retention_days: 31,
        log_rollup: Duration::from_secs(120),
        metrics_landing_retention_hours: 11,
        log_landing_retention_hours: 12,
        trace_landing_retention_hours: 13,
        metrics_dedup_window: 21,
        log_dedup_window: 22,
        trace_dedup_window: 23,
        trace_indexed_attributes: Vec::new(),
        dist_suffix: "_routed".to_string(),
        rollup_levels: vec![120, 720],
        ..clustered()
    };
    let text = rendered(&ctx);
    assert!(text.contains("(31 * 86400)"), "retention days");
    // Issue #18: the configured levels name the tables, and the timers
    // follow them: `p = 120`, `d_1 = 240`, `d_2 = 300`.
    assert!(
        text.contains("CREATE TABLE IF NOT EXISTS pulsus.metric_samples_120s ON CLUSTER 'prod'"),
        "the configured first level names its table"
    );
    assert!(
        text.contains(
            "REFRESH EVERY 12 MINUTE OFFSET 300 SECOND APPEND TO pulsus.metric_samples_720s"
        ),
        "the configured second level's timer"
    );
    assert!(
        text.contains("pulsus.log_metrics_2m"),
        "rollup suffix names a table"
    );
    assert!(
        text.contains("120000000000"),
        "rollup resolution in nanoseconds"
    );
    assert!(text.contains("(11 * 3600)"), "metrics landing retention");
    assert!(text.contains("(12 * 3600)"), "logs landing retention");
    assert!(text.contains("(13 * 3600)"), "traces landing retention");
    assert!(text.contains("replicated_deduplication_window = 21"));
    assert!(text.contains("replicated_deduplication_window = 22"));
    assert!(text.contains("replicated_deduplication_window = 23"));
    // No table may hard-code the default window instead of the token: a
    // configured window would never reach it.
    assert!(
        !text.contains("deduplication_window = 10000"),
        "a table hard-codes the default deduplication window: {}",
        text.lines()
            .filter(|l| l.contains("deduplication_window = 10000"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        text.contains("pulsus.spans_routed"),
        "the configured suffix"
    );
    // No identifier `pulsus.<name>_dist`: the default suffix is not baked
    // in. Settings such as `fallback_to_stale_replicas_for_distributed_queries`
    // contain `_dist` and are not names, so each name is read whole.
    let baked: Vec<&str> = text
        .match_indices("pulsus.")
        .map(|(at, _)| {
            text[at + "pulsus.".len()..]
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .next()
                .expect("split yields at least one piece")
        })
        .filter(|name| name.ends_with("_dist"))
        .collect();
    assert!(
        baked.is_empty(),
        "the default suffix is baked into {baked:?}"
    );
}

/// **The script and this crate render the identical text.** Two renderers
/// read one file; this is what stops them diverging.
#[test]
fn the_script_renders_exactly_what_this_crate_renders() {
    let root = repo_root();
    let script = root.join("schema/schema.sh");

    // Composed rather than written as a literal: nothing here reaches a
    // server, but `live_db_naming` reads every literal assigned to a `db`
    // field and cannot tell the difference.
    let other_db = format!("{}_elsewhere", single().db);
    let cases: Vec<(&str, RenderCtx)> = vec![
        ("default single-node", single()),
        ("default clustered", clustered()),
        (
            "every value moved",
            RenderCtx {
                db: other_db,
                cluster: Some("c1".to_string()),
                dist_suffix: "_routed".to_string(),
                storage_policy: Some("hot_cold".to_string()),
                retention_days: 31,
                log_rollup: Duration::from_millis(500),
                metrics_landing_retention_hours: 11,
                log_landing_retention_hours: 12,
                trace_landing_retention_hours: 13,
                metrics_dedup_window: 21,
                log_dedup_window: 22,
                trace_dedup_window: 23,
                trace_indexed_attributes: vec![
                    "span.app.request.id".to_string(),
                    "event.exception.type".to_string(),
                    "span.http-x.a_b".to_string(),
                ],
                // The script renders the production cap and has no input
                // for it (issue #599), so this case keeps it.
                graph_job_max_rows: pulsus_schema::GRAPH_JOB_MAX_ROWS,
                rollup_levels: vec![15, 60, 720],
                rollup_ranges: vec![3600, 86400],
                rollup_counter_buckets: 7,
            },
        ),
        (
            "no levels",
            RenderCtx {
                rollup_levels: Vec::new(),
                ..single()
            },
        ),
        (
            "whole minutes",
            RenderCtx {
                log_rollup: Duration::from_secs(120),
                ..single()
            },
        ),
        (
            "whole seconds given in milliseconds",
            RenderCtx {
                log_rollup: Duration::from_millis(2000),
                ..single()
            },
        ),
    ];

    for (what, ctx) in cases {
        let mut cmd = Command::new("sh");
        cmd.arg(&script).arg("--print").current_dir(&root);
        cmd.env("CLICKHOUSE_DB", &ctx.db);
        cmd.env("PULSUS_DIST_SUFFIX", &ctx.dist_suffix);
        cmd.env("PULSUS_RETENTION_DAYS", ctx.retention_days.to_string());
        cmd.env(
            "PULSUS_LOG_ROLLUP_RESOLUTION",
            format!("{}ms", ctx.log_rollup.as_millis()),
        );
        cmd.env(
            "PULSUS_METRICS_LANDING_RETENTION_HOURS",
            ctx.metrics_landing_retention_hours.to_string(),
        );
        cmd.env(
            "PULSUS_LOG_LANDING_RETENTION_HOURS",
            ctx.log_landing_retention_hours.to_string(),
        );
        cmd.env(
            "PULSUS_TRACE_LANDING_RETENTION_HOURS",
            ctx.trace_landing_retention_hours.to_string(),
        );
        cmd.env(
            "PULSUS_METRICS_DEDUP_WINDOW",
            ctx.metrics_dedup_window.to_string(),
        );
        cmd.env("PULSUS_LOG_DEDUP_WINDOW", ctx.log_dedup_window.to_string());
        cmd.env(
            "PULSUS_TRACE_DEDUP_WINDOW",
            ctx.trace_dedup_window.to_string(),
        );
        cmd.env(
            "PULSUS_TRACEQL_INDEXED_ATTRIBUTES",
            ctx.trace_indexed_attributes.join(","),
        );
        cmd.env(
            "PULSUS_ROLLUP_LEVELS",
            ctx.rollup_levels
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
        cmd.env(
            "PULSUS_ROLLUP_RANGES",
            ctx.rollup_ranges
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
        cmd.env(
            "PULSUS_ROLLUP_COUNTER_BUCKETS",
            ctx.rollup_counter_buckets.to_string(),
        );
        match &ctx.cluster {
            Some(name) => cmd.env("PULSUS_CLUSTER", name),
            None => cmd.env_remove("PULSUS_CLUSTER"),
        };
        match &ctx.storage_policy {
            Some(p) => cmd.env("PULSUS_STORAGE_POLICY", p),
            None => cmd.env_remove("PULSUS_STORAGE_POLICY"),
        };

        let out = cmd.output().expect("the script runs");
        assert!(
            out.status.success(),
            "{what}: the script refused: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let from_script = String::from_utf8(out.stdout).expect("the render is text");
        let from_crate = rendered(&ctx);
        assert_eq!(
            from_script.trim_end(),
            from_crate.trim_end(),
            "{what}: the script and this crate must render the same text"
        );
    }
}

/// Each view's projection reads its landing table under that table's
/// discriminator — `row_kind` for traces, `kind` for metrics — and a
/// table's declared column list, which the trace fetch derives its
/// projection from, is read out of the same file.
#[test]
fn each_view_reads_its_landing_table_under_its_discriminator() {
    let ctx = single();

    for mv in [
        "spans_mv",
        "traces_mv",
        "resources_mv",
        "tag_names_mv",
        "tag_values_mv",
        "error_spans_mv",
        "tag_bucket_values_mv",
        "service_bucket_resources_mv",
    ] {
        let projection =
            pulsus_schema::mv_projection(mv, &ctx).unwrap_or_else(|| panic!("{mv} is in the file"));
        assert!(projection.starts_with("SELECT"), "{mv}: {projection}");
        assert!(
            projection.contains("WHERE row_kind = "),
            "{mv} must scan the landing table under a row_kind predicate, \
             which is the one landing-row shape it takes: {projection}"
        );
        assert!(!projection.ends_with(';'));
    }
    // Issue #601 parts 4b and 4c: the three derived tables read the span
    // rows alone.
    for mv in [
        "error_spans_mv",
        "tag_bucket_values_mv",
        "service_bucket_resources_mv",
    ] {
        let projection =
            pulsus_schema::mv_projection(mv, &ctx).unwrap_or_else(|| panic!("{mv} is in the file"));
        assert!(
            projection.contains("FROM pulsus.trace_landing")
                && projection.contains("WHERE row_kind = 0"),
            "{mv} must read the landing table's span rows: {projection}"
        );
    }
    for mv in [
        "metric_samples_mv",
        "metric_series_mv",
        "metric_metadata_mv",
        "metric_hist_samples_mv",
        "metric_exemplars_mv",
        "metric_labels_mv",
        "metric_label_index_mv",
        "metric_label_values_mv",
    ] {
        let projection =
            pulsus_schema::mv_projection(mv, &ctx).unwrap_or_else(|| panic!("{mv} is in the file"));
        assert!(
            projection.contains("WHERE kind = "),
            "{mv} must read the landing table under a kind predicate: {projection}"
        );
    }
    assert!(pulsus_schema::mv_projection("not_a_view", &ctx).is_none());

    let spans = pulsus_schema::table_column_names("spans").expect("spans is in the file");
    assert_eq!(&spans[..2], &["org_id", "trace_id"]);
    assert_eq!(spans.last(), Some(&"service_type"));
    assert_eq!(spans.len(), 29);
    let resources = pulsus_schema::table_column_names("resources").expect("resources");
    assert_eq!(
        resources,
        vec![
            "org_id",
            "day",
            "resource_id",
            "service",
            "attrs",
            "attrs_other",
            "dropped_attrs",
            "schema_url",
            "entity_refs",
        ]
    );
    let traces = pulsus_schema::table_column_names("traces").expect("traces");
    assert_eq!(&traces[..2], &["org_id", "day"]);
    assert!(traces.contains(&"buckets"));
    assert!(pulsus_schema::table_column_names("not_a_table").is_none());

    // An index, a constraint and a projection are not columns.
    let log_samples = pulsus_schema::table_column_names("log_samples").expect("log_samples");
    assert_eq!(
        log_samples,
        vec![
            "org_id",
            "service",
            "fingerprint",
            "timestamp_ns",
            "severity",
            "body",
            "structured_metadata",
        ]
    );
    let spans = pulsus_schema::table_column_names("spans").expect("spans");
    assert!(!spans.iter().any(|c| c.starts_with(char::is_uppercase)));
}

// ---------------------------------------------------------------------------
// The file against the documents, and the two structural invariants nothing
// else holds.
//
// The deleted catalogue's test block pinned each statement's text against a
// retyped copy of itself; the file IS the DDL now, so those are circular and
// gone. What is kept is what no other instrument answers: the passages in
// three documents that name which tables have no routing wrapper, and two
// shapes a silent edit could break.
// ---------------------------------------------------------------------------

/// One `CREATE TABLE` the file declares.
struct Table {
    /// The unresolved name, so `log_metrics_{{log_rollup_suffix}}` keeps
    /// its token rather than being compared against a resolved one.
    name: String,
    /// `true` when the statement is a clustered-only routing wrapper.
    wrapper: bool,
    /// `true` when the clustered engine joins the shard-less, cluster-wide
    /// replica set (`/clickhouse/tables/all/`).
    cluster_wide: bool,
}

/// Every `CREATE TABLE` in the file, read from the unrendered text with the
/// default rollup levels expanded, so each level table is here under its
/// default name (issue #18).
fn tables() -> Vec<Table> {
    let expanded = pulsus_schema::expand_levels_for_tests(pulsus_schema::DEFAULT_ROLLUP_LEVELS);
    let mut out: Vec<Table> = Vec::new();
    for line in expanded.lines() {
        if line.starts_with("--") && !line.starts_with("--@") {
            continue;
        }
        let wrapper = line.starts_with("--@cluster ");
        let bare = line.trim_start_matches("--@cluster ");
        if let Some(rest) = bare.strip_prefix("CREATE TABLE IF NOT EXISTS {{db}}.") {
            let mut name = rest
                .split_whitespace()
                .next()
                .expect("a name follows the database")
                .trim_end_matches("{{on_cluster}}")
                .to_string();
            let is_wrapper = name.ends_with("{{dist_suffix}}");
            name = name.trim_end_matches("{{dist_suffix}}").to_string();
            out.push(Table {
                name,
                wrapper: wrapper && is_wrapper,
                cluster_wide: false,
            });
            continue;
        }
        if bare.contains("/clickhouse/tables/all/")
            && let Some(last) = out.last_mut()
        {
            last.cluster_wide = true;
        }
    }
    out
}

/// Every base table with no routing wrapper: the landing tables and the
/// cluster-wide ones. These are the exceptions three documents name.
fn tables_without_a_wrapper() -> Vec<String> {
    let all = tables();
    let wrapped: Vec<&String> = all.iter().filter(|t| t.wrapper).map(|t| &t.name).collect();
    all.iter()
        .filter(|t| !t.wrapper && !wrapped.contains(&&t.name))
        .map(|t| t.name.clone())
        .collect()
}

/// The document, read from the repository root.
fn doc(name: &str) -> String {
    let path = repo_root().join("docs").join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The one line of `name` containing `anchor`, which must occur once.
fn doc_line(name: &str, anchor: &str) -> String {
    let text = doc(name);
    let hits: Vec<&str> = text.lines().filter(|l| l.contains(anchor)).collect();
    assert_eq!(
        hits.len(),
        1,
        "docs/{name} must carry exactly one line containing {anchor:?}"
    );
    hits[0].to_string()
}

/// The one line of `name` containing `anchor`, **cut off at the anchor**.
///
/// The cut is the point. A passage that lists tables and then explains
/// itself can satisfy a bare `contains` with its explanation rather than
/// with its list: dropping `spans` from the sharding-key table's first
/// column left the old case green, because the third column still said the
/// word. The anchor is the column boundary, so only the list is read.
fn doc_line_before(name: &str, anchor: &str) -> String {
    let line = doc_line(name, anchor);
    let at = line.find(anchor).expect("the line contains the anchor");
    line[..at].to_string()
}

/// **The wrapper sentences name every table that has no routing sibling,
/// and no table that has one.**
///
/// Both directions matter. Without the first the sentence keeps a false
/// absolute — which is what it carried; without the second the fix is "list
/// every table", which says nothing.
///
/// A name holding a token (`log_metrics_{{log_rollup_suffix}}`) is skipped
/// in the second direction, because the documents print its resolved name
/// and this case does not resolve one.
#[test]
fn the_wrapper_sentences_name_every_table_without_a_routing_sibling() {
    let no_wrapper = tables_without_a_wrapper();
    assert_eq!(
        no_wrapper.len(),
        11,
        "three landing tables, the label values, `service_edges` and six \
         cluster-wide ones: \
         {no_wrapper:?}"
    );
    let all = tables();
    let wrapped: Vec<&String> = all.iter().filter(|t| t.wrapper).map(|t| &t.name).collect();
    for (name, anchor) in [
        ("architecture.md", "**Sharded** (`PULSUS_CLUSTER` set):"),
        ("schemas.md", "Enabled by `PULSUS_CLUSTER`."),
    ] {
        let line = doc_line(name, anchor);
        for table in &no_wrapper {
            assert!(
                line.contains(&format!("`{table}`")),
                "docs/{name}'s clustering sentence must name `{table}`, which \
                 has no routing sibling: {line}"
            );
        }
        for table in &wrapped {
            if table.contains("{{") {
                continue;
            }
            assert!(
                !line.contains(&format!("`{table}`")),
                "docs/{name}'s clustering sentence names `{table}` among the \
                 tables with no routing sibling, and it has one: {line}"
            );
        }
    }
}

/// **The trace sharding-key passages name every routed trace table.**
/// `spans` and `traces` are routed on `cityHash64(trace_id)` and neither
/// passage named them until this case was written.
///
/// Only the names column of each passage is read, for the reason
/// [`doc_line_before`] gives.
#[test]
fn the_trace_sharding_key_passage_names_every_routed_trace_table() {
    let routed: Vec<String> = tables()
        .into_iter()
        .filter(|t| t.wrapper)
        .map(|t| t.name)
        .filter(|n| !n.starts_with("metric") && !n.starts_with("log") || n.starts_with("trace"))
        .collect();
    assert_eq!(
        routed.len(),
        3,
        "every trace base table but the landing one is routed: {routed:?}"
    );
    for (name, anchor) in [
        (
            "architecture.md",
            "`cityHash64(trace_id)` — a trace is whole on one shard",
        ),
        (
            "schemas.md",
            "| `cityHash64(trace_id)` | a trace is whole on one shard",
        ),
    ] {
        let names_column = doc_line_before(name, anchor);
        for table in &routed {
            assert!(
                names_column.contains(&format!("`{table}`")),
                "docs/{name}'s trace sharding-key passage must name `{table}` \
                 among the tables that key, and its names column is: \
                 {names_column}"
            );
        }
    }
}

/// **The three landing tables, `metric_label_values` and `service_edges`
/// are the only per-shard tables with no routing wrapper.** The label
/// values are named locally inside every statement that reads them (issue
/// #635); `service_edges` is read only per shard, through `cluster(…,
/// view(…))`, by the one statement that also pairs that shard's spans
/// (issue #599, part A D9). Four passages across three documents restrict their claim to
/// per-shard tables, and this is the fact that restriction rests on: a
/// per-shard table added without a wrapper makes all four false at once,
/// and reddens here rather than in a document nobody reads.
#[test]
fn only_the_landing_tables_the_label_values_and_the_service_edges_are_per_shard_without_a_wrapper()
{
    let cluster_wide: Vec<String> = tables()
        .into_iter()
        .filter(|t| t.cluster_wide)
        .map(|t| t.name)
        .collect();
    let mut without: Vec<String> = tables_without_a_wrapper()
        .into_iter()
        .filter(|n| !cluster_wide.contains(n))
        .collect();
    without.sort();
    assert_eq!(
        without,
        vec![
            "log_landing",
            "metric_label_values",
            "metric_landing",
            "service_edges",
            "trace_landing"
        ],
        "a per-shard table with no routing wrapper that is not a landing \
         table, the label values or `service_edges` (read only per shard, issue #599 part A \
         D9) falsifies the clustering claim in docs/architecture.md §7, \
         docs/schemas.md §7, docs/schemas.md's conventions preamble and \
         docs/features.md's clustering row"
    );
}

/// **Every passage saying which tables are written under their bare name
/// names all three landing tables.**
#[test]
fn the_bare_name_passages_name_all_three_landing_tables() {
    let landing: Vec<String> = tables_without_a_wrapper()
        .into_iter()
        .filter(|n| n.ends_with("_landing"))
        .collect();
    assert_eq!(landing.len(), 3, "three landing tables: {landing:?}");
    for (name, anchor) in [
        ("architecture.md", "The landing tables have no wrapper"),
        ("schemas.md", "Conventions used below:"),
        ("features.md", "| Clustered ClickHouse |"),
    ] {
        let line = doc_line(name, anchor);
        for table in &landing {
            assert!(
                line.contains(&format!("`{table}`")),
                "docs/{name}'s bare-name passage must name `{table}`: {line}"
            );
        }
    }
}

/// **The two passages that carry the claim without the list say where the
/// list is.** `docs/architecture.md` §7 and `docs/schemas.md` §7 own the
/// exceptions, read by
/// [`the_wrapper_sentences_name_every_table_without_a_routing_sibling`];
/// the conventions preamble and the features row state the claim and cite
/// them instead of repeating it.
#[test]
fn the_citing_clustering_passages_name_the_section_holding_the_list() {
    // The documents spell the number out, so the count is spelled too —
    // otherwise the citation could be corrected to a digit and still pass
    // while the number itself went stale.
    let count = match tables_without_a_wrapper().len() {
        7 => "seven",
        8 => "eight",
        9 => "nine",
        10 => "ten",
        11 => "eleven",
        12 => "twelve",
        n => panic!("spell {n} out here before the count can be cited"),
    };
    assert!(
        doc("schemas.md").contains("\n## 7. Distributed layout"),
        "docs/schemas.md must still hold the section the other two cite"
    );
    let preamble = doc_line("schemas.md", "Conventions used below:");
    assert!(
        preamble.contains(&format!("§7 lists all {count}")),
        "docs/schemas.md's conventions preamble must cite its own §7 for the \
         {count}: {preamble}"
    );
    let row = doc_line("features.md", "| Clustered ClickHouse |");
    assert!(
        row.contains(&format!("[schemas.md §7](schemas.md) lists all {count}")),
        "docs/features.md's clustering row must cite schemas.md §7 for the \
         {count}: {row}"
    );
    // The two wrapper passages that state the rule without the list.
    let schema_dir = doc_line(
        "architecture.md",
        "When `PULSUS_CLUSTER` is set, `MergeTree` families",
    );
    assert!(
        schema_dir.contains(&format!("for every table but the {count} §7 lists")),
        "docs/architecture.md's schema paragraph must cite §7 for the {count} \
         tables with no wrapper: {schema_dir}"
    );
    let shape = doc_line("schemas.md", "gets a Distributed wrapper of this shape");
    assert!(
        shape.contains(&format!(
            "Every table but the {count} listed at the start of this section gets"
        )),
        "docs/schemas.md's wrapper-shape sentence must cite the {count} tables \
         with no wrapper: {shape}"
    );
}

/// **Every passage saying the clustered tables are replicated names the
/// tables that are not.** Since issue #18 part A there are none: every
/// clustered table is `Replicated*`, and no passage may still except
/// `metric_rollup_plan`, which part A removed.
#[test]
fn the_replication_passages_name_every_unreplicated_table() {
    let mut local: Vec<String> = rendered_statements(&clustered())
        .into_iter()
        .filter(|s| s.starts_with("CREATE TABLE IF NOT EXISTS pulsus."))
        .filter(|s| !s.contains("ENGINE = Distributed(") && !s.contains("ENGINE = Replicated"))
        .map(|s| {
            s["CREATE TABLE IF NOT EXISTS pulsus.".len()..]
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .next()
                .expect("a table name")
                .to_string()
        })
        .collect();
    local.sort();
    assert_eq!(
        local,
        Vec::<String>::new(),
        "the clustered tables that are not replicated"
    );
    for (name, anchor) in [
        (
            "architecture.md",
            "When `PULSUS_CLUSTER` is set, `MergeTree` families",
        ),
        ("architecture.md", "**Sharded** (`PULSUS_CLUSTER` set):"),
        ("schemas.md", "Conventions used below:"),
        ("schemas.md", "Enabled by `PULSUS_CLUSTER`."),
    ] {
        let line = doc_line(name, anchor);
        for table in &local {
            assert!(
                line.contains(&format!("but `{table}`"))
                    || line.contains(&format!("except `{table}`")),
                "docs/{name}'s replication claim must except `{table}`, which \
                 is not replicated: {line}"
            );
        }
        assert!(
            !line.contains("metric_rollup_plan"),
            "docs/{name}'s replication claim names a removed table: {line}"
        );
    }
}

/// **No insert-triggered view reads a table another view writes.**
/// Chaining one such view onto another's target would count every row
/// twice, and the server would accept it without comment.
///
/// The sources are pinned as a set too: every insert-triggered view reads a
/// landing table, which the writers fill directly and no view targets.
///
/// **V1 (issue #599): the refreshable views are named, not exempted.**
/// `service_edges_mv` runs on a schedule rather than on an insert, and it
/// reads `spans` (another view's target) and its own target, `service_edges`
/// (its markers). Its sources are read through `merge('pulsus', '^<table>$')`
/// and through nothing else (part C D6): a direct `FROM pulsus.` read takes
/// its part snapshot before the barrier runs. Issue #18's level views: each
/// reads its own target (its run rows) and its source, `metric_landing` for
/// the first level and the level below for every other. Each is pinned by
/// name, target and exact sources, in both variants, so another refreshable
/// view, or one of these reading anything else, fails here instead of
/// passing under a rule for every refreshable view.
#[test]
fn no_insert_triggered_view_reads_a_table_another_view_writes() {
    for base in [single(), clustered()] {
        let variant = base.cluster.clone();
        let mut targets = Vec::new();
        let mut sources = Vec::new();
        let mut refreshable = Vec::new();
        for stmt in rendered_statements(&base) {
            let Some(head) = stmt.strip_prefix("CREATE MATERIALIZED VIEW pulsus.") else {
                continue;
            };
            let head_line = head.lines().next().expect("a view has a head line");
            let name = head_line
                .split_whitespace()
                .next()
                .expect("the view's name")
                .to_string();
            let target = head_line
                .split_once(" TO pulsus.")
                .expect("a view names its target on its head line")
                .1
                .trim()
                .to_string();
            targets.push(target.clone());
            // Every `FROM`, not just the first: a view with a subquery has
            // more than one, and each is a source. A clustered
            // `clusterAllReplicas('<cluster>', pulsus.<table>)` names
            // `<table>`.
            let mut own: Vec<String> = Vec::new();
            for marker in ["FROM pulsus.", "clusterAllReplicas('prod', pulsus."] {
                for (at, _) in stmt.match_indices(marker) {
                    let table = stmt[at + marker.len()..]
                        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                        .next()
                        .expect("a table follows FROM")
                        .to_string();
                    own.push(table);
                }
            }
            // `merge('pulsus', '^<table>$')`: one table, read when the query
            // reaches it.
            let mut merged: Vec<String> = Vec::new();
            for (at, _) in stmt.match_indices("merge('pulsus', '^") {
                let table = stmt[at + "merge('pulsus', '^".len()..]
                    .split_once("$')")
                    .expect("a merge() source ends with $')")
                    .0
                    .to_string();
                merged.push(table);
            }
            assert!(
                !own.is_empty() || !merged.is_empty(),
                "a view that names no source: {stmt}"
            );
            if head_line.contains(" REFRESH EVERY ") {
                own.sort();
                own.dedup();
                merged.sort();
                merged.dedup();
                refreshable.push((name, target, own, merged));
            } else {
                assert!(
                    merged.is_empty(),
                    "an insert-triggered view reads through merge(): {stmt}"
                );
                sources.extend(own);
            }
        }
        assert_eq!(targets.len(), 26, "{variant:?}: twenty-six views");
        refreshable.sort();
        let strings = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            refreshable,
            vec![
                (
                    "metric_samples_60s_mv".to_string(),
                    "metric_samples_60s".to_string(),
                    strings(&["metric_landing", "metric_samples_60s"]),
                    Vec::<String>::new(),
                ),
                // Sorted as strings: `7200s` before `720s`.
                (
                    "metric_samples_7200s_mv".to_string(),
                    "metric_samples_7200s".to_string(),
                    strings(&["metric_samples_7200s", "metric_samples_720s"]),
                    Vec::<String>::new(),
                ),
                (
                    "metric_samples_720s_mv".to_string(),
                    "metric_samples_720s".to_string(),
                    strings(&["metric_samples_60s", "metric_samples_720s"]),
                    Vec::<String>::new(),
                ),
                (
                    "metric_samples_86400s_mv".to_string(),
                    "metric_samples_86400s".to_string(),
                    strings(&["metric_samples_7200s", "metric_samples_86400s"]),
                    Vec::<String>::new(),
                ),
                (
                    "service_edges_mv".to_string(),
                    "service_edges".to_string(),
                    Vec::<String>::new(),
                    strings(&["service_edges", "spans"]),
                ),
            ],
            "{variant:?}: the five refreshable views, their targets, their direct \
             sources and their sources read through merge()"
        );
        let mut distinct: Vec<String> = sources.clone();
        distinct.sort();
        distinct.dedup();
        assert_eq!(
            distinct,
            vec!["log_landing", "metric_landing", "trace_landing"],
            "{variant:?}: an insert-triggered view reads a table outside the pinned source set"
        );
        for source in &sources {
            assert!(
                !targets.contains(source),
                "a view reads {source}, which another view writes: every row \
                 would be counted twice"
            );
        }
    }
}

/// **The cluster-wide replica set is exactly these six tables.** A data
/// table given an `/all/` path would make every shard a replica of one
/// table and silently collapse the cluster to one shard's worth of data.
#[test]
fn the_cluster_wide_replica_set_is_exactly_the_catalog_tables() {
    let mut names: Vec<String> = tables()
        .into_iter()
        .filter(|t| t.cluster_wide)
        .map(|t| t.name)
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "metric_metadata",
            "resources",
            "service_bucket_resources",
            "tag_bucket_values",
            "tag_names",
            "tag_values",
        ],
        "the shard-less `/clickhouse/tables/all/` path belongs to the catalog \
         tables alone (docs/architecture.md §3)"
    );
}

// ---------------------------------------------------------------------------
// Issue #623: the metrics tables' storage shape.
// ---------------------------------------------------------------------------

/// The rendered `CREATE TABLE` of base table `table` under `ctx`.
fn create_of(ctx: &RenderCtx, table: &str) -> String {
    let head = format!("CREATE TABLE IF NOT EXISTS pulsus.{table}");
    rendered_statements(ctx)
        .into_iter()
        .find(|s| {
            s.trim_start()
                .strip_prefix(&head)
                .is_some_and(|rest| rest.starts_with([' ', '\n']) && !rest.contains("Distributed("))
        })
        .unwrap_or_else(|| panic!("no CREATE TABLE for {table}"))
}

/// The one line of `stmt` that starts with `prefix`.
fn line_of<'a>(stmt: &'a str, prefix: &str) -> &'a str {
    let hits: Vec<&str> = stmt
        .lines()
        .filter(|l| l.trim_start().starts_with(prefix))
        .collect();
    assert_eq!(hits.len(), 1, "one {prefix:?} line in {stmt}");
    hits[0]
}

/// **`metric_landing` carries no `event_id`.** Nothing outside tests read it,
/// and it was most of the table's bytes.
#[test]
fn metric_landing_carries_no_event_id() {
    let columns = pulsus_schema::table_column_names("metric_landing").expect("in the file");
    assert!(
        !columns.contains(&"event_id"),
        "metric_landing still declares event_id: {columns:?}"
    );
    assert_eq!(columns.len(), 28, "the 28 columns the writer sends");
    // Issue #635 part 4: the tenant leads, as it leads the key.
    assert_eq!(columns.first(), Some(&"org_id"));
    // The log and trace landing tables keep theirs, after the tenant
    // (issues #653, #738).
    let log = pulsus_schema::table_column_names("log_landing").expect("in the file");
    assert_eq!(&log[..2], &["org_id", "event_id"], "log_landing");
    let trace = pulsus_schema::table_column_names("trace_landing").expect("in the file");
    assert_eq!(&trace[..2], &["org_id", "event_id"], "trace_landing");
}

/// **F7 (issue #623): `metric_series` is one row per series per UTC day**,
/// its active hours a 24-bit mask OR'd on merge: `day`, `fingerprint`,
/// `metric_name`, `hours`, partitioned by `day`, keyed by the series ID, kept
/// until the day's last sample has expired. The view folds each kind-2 row to
/// its day and its hour's bit.
#[test]
fn metric_series_is_one_row_per_series_day() {
    assert_eq!(
        pulsus_schema::table_column_names("metric_series").expect("in the file"),
        vec!["org_id", "day", "fingerprint", "metric_name", "hours"],
    );
    let single_create = create_of(&single(), "metric_series");
    assert!(
        single_create.contains("hours SimpleAggregateFunction(groupBitOr, UInt32)"),
        "{single_create}"
    );
    assert!(
        single_create.contains("ENGINE = AggregatingMergeTree\n"),
        "{single_create}"
    );
    for ctx in [single(), clustered()] {
        let series = create_of(&ctx, "metric_series");
        assert_eq!(line_of(&series, "PARTITION BY"), "PARTITION BY day");
        assert_eq!(
            line_of(&series, "ORDER BY"),
            "ORDER BY (org_id, fingerprint)"
        );
        assert_eq!(
            line_of(&series, "TTL"),
            "TTL toDateTime(least((toUInt64(toUInt16(day)) + 1 + 7) * 86400, 4294967295))"
        );
        assert!(
            line_of(&series, "SETTINGS").contains("ttl_only_drop_parts = 1"),
            "{series}"
        );
    }
    assert!(
        create_of(&clustered(), "metric_series").contains(
            "ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/pulsus.metric_series', '{replica}')"
        )
    );
    let view = pulsus_schema::mv_projection("metric_series_mv", &single())
        .expect("the view is in the file");
    for want in [
        "toDate(fromUnixTimestamp64Milli(unix_milli), 'UTC') AS day",
        "fingerprint AS fingerprint",
        "metric_name AS metric_name",
        "toUInt32(bitShiftLeft(toUInt32(1), toHour(fromUnixTimestamp64Milli(unix_milli), 'UTC'))) AS hours",
        "WHERE kind = 2",
    ] {
        assert!(view.contains(want), "{want}: {view}");
    }
    assert!(!view.contains("labels"), "{view}");
}

/// **K1 (issue #623): `metric_labels` is the lookup, one row per series**,
/// keyed `(metric_name, fingerprint)` so a named matcher is a key range, with
/// the hours it was first and last registered folded by `min` and `max`. An
/// aggregating engine filled by a view from the kind-2 rows, with a routing
/// wrapper and a per-shard replica set. Its expiry (issue #496) is
/// `every_table_expires_exactly_as_documented`'s.
#[test]
fn metric_labels_holds_one_label_row_per_series() {
    assert_eq!(
        pulsus_schema::table_column_names("metric_labels").expect("in the file"),
        vec![
            "org_id",
            "metric_name",
            "fingerprint",
            "labels",
            "first_seen",
            "last_seen"
        ],
    );
    let single_create = create_of(&single(), "metric_labels");
    assert!(
        single_create.contains("ENGINE = AggregatingMergeTree\n"),
        "{single_create}"
    );
    for want in [
        "first_seen SimpleAggregateFunction(min, Int64)",
        "last_seen SimpleAggregateFunction(max, Int64)",
    ] {
        assert!(single_create.contains(want), "{want}: {single_create}");
    }
    assert_eq!(
        line_of(&single_create, "ORDER BY"),
        "ORDER BY (org_id, metric_name, fingerprint)"
    );
    let clustered_create = create_of(&clustered(), "metric_labels");
    assert!(
        clustered_create.contains(
            "ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/pulsus.metric_labels', '{replica}')"
        ),
        "{clustered_create}"
    );
    assert!(
        rendered(&clustered()).contains("CREATE TABLE IF NOT EXISTS pulsus.metric_labels_dist"),
        "metric_labels has a routing wrapper"
    );
    let projection = pulsus_schema::mv_projection("metric_labels_mv", &single())
        .expect("the view is in the file");
    assert!(projection.ends_with("WHERE kind = 2"), "{projection}");
    for want in [
        "metric_name AS metric_name",
        "labels AS labels",
        "unix_milli AS first_seen",
        "unix_milli AS last_seen",
        "FROM pulsus.metric_landing",
    ] {
        assert!(projection.contains(want), "{want}: {projection}");
    }
}

/// **F7 (issue #623): the sample tables are keyed by the series ID.** No
/// `metric_name` column, `ORDER BY (fingerprint, unix_milli)`, and their
/// views project no name.
#[test]
fn the_sample_tables_are_keyed_by_the_series_id() {
    assert_eq!(
        pulsus_schema::table_column_names("metric_samples").expect("in the file"),
        vec!["org_id", "fingerprint", "unix_milli", "value"],
    );
    let hist = pulsus_schema::table_column_names("metric_hist_samples").expect("in the file");
    assert_eq!(&hist[..3], &["org_id", "fingerprint", "unix_milli"]);
    assert!(!hist.contains(&"metric_name"), "{hist:?}");
    for ctx in [single(), clustered()] {
        for table in ["metric_samples", "metric_hist_samples"] {
            let create = create_of(&ctx, table);
            assert_eq!(
                line_of(&create, "ORDER BY"),
                "ORDER BY (org_id, fingerprint, unix_milli)",
                "{table}"
            );
            let view = pulsus_schema::mv_projection(&format!("{table}_mv"), &ctx)
                .expect("the view is in the file");
            assert!(!view.contains("metric_name"), "{table}: {view}");
        }
    }
}

/// **Both sample tables keep the whole sorting key in memory.** At the
/// default ratio the engine drops `fingerprint` and `unix_milli` from the
/// in-memory index of a part where `metric_name` is nearly unique per
/// granule, and a one-series read stops pruning.
#[test]
fn both_sample_tables_keep_the_whole_key_in_memory() {
    for ctx in [single(), clustered()] {
        for table in ["metric_samples", "metric_hist_samples", "metric_exemplars"] {
            let create = create_of(&ctx, table);
            assert!(
                line_of(&create, "SETTINGS").contains(
                    "primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1,"
                ),
                "{table}: {create}"
            );
        }
    }
}

/// **Issue #501 part 1: `metric_exemplars` is a sample table with two
/// ids.** Its key, partition, TTL and windows are `metric_samples`' own;
/// its granule is 1,024 rows; its view copies kind 4 of the landing table,
/// and its wrapper shards with the rest of the metrics family.
#[test]
fn the_exemplar_table_is_a_sample_table_with_two_ids() {
    assert_eq!(
        pulsus_schema::table_column_names("metric_exemplars").expect("in the file"),
        vec![
            "org_id",
            "fingerprint",
            "unix_milli",
            "value",
            "trace_id",
            "span_id",
            "labels"
        ],
    );
    for ctx in [single(), clustered()] {
        let create = create_of(&ctx, "metric_exemplars");
        let samples = create_of(&ctx, "metric_samples");
        assert_eq!(
            line_of(&create, "ORDER BY"),
            "ORDER BY (org_id, fingerprint, unix_milli)"
        );
        for prefix in ["PARTITION BY", "TTL"] {
            assert_eq!(
                line_of(&create, prefix),
                line_of(&samples, prefix),
                "{prefix}"
            );
        }
        assert!(
            line_of(&create, "SETTINGS").contains("index_granularity = 1024,"),
            "{create}"
        );
        // Every other setting is `metric_samples`' own.
        assert_eq!(
            line_of(&create, "SETTINGS").replace("index_granularity = 1024,", ""),
            line_of(&samples, "SETTINGS").replace("index_granularity = 8192,", ""),
        );
    }
    let view = pulsus_schema::mv_projection("metric_exemplars_mv", &single())
        .expect("the view is in the file");
    for pair in [
        "org_id AS org_id",
        "fingerprint AS fingerprint",
        "unix_milli AS unix_milli",
        "value AS value",
        "ex_trace_id AS trace_id",
        "ex_span_id AS span_id",
        "labels AS labels",
    ] {
        assert!(view.contains(pair), "{pair}: {view}");
    }
    assert!(view.trim_end().ends_with("WHERE kind = 4"), "{view}");
    assert!(
        rendered(&clustered()).contains("'metric_exemplars', cityHash64(fingerprint))"),
        "the wrapper shards by the series ID"
    );
}

/// Issue #595 part 2: the script refuses, with exit status 2, every item
/// `pulsus_config::parse_indexed_attribute` refuses
/// (`an_indexed_attribute_is_span_or_event_and_a_plain_dotted_key`), a
/// repeat, an empty item and a seventeenth item.
#[test]
fn the_script_refuses_the_attribute_lists_the_configuration_refuses() {
    let root = repo_root();
    let seventeen: Vec<String> = (0..17).map(|i| format!("span.k{i}")).collect();
    let seventeen = seventeen.join(",");
    let too_long = format!("span.{}", "a.".repeat(64) + "a");
    for list in [
        "resource.k8s.pod.name",
        "link.x",
        "instrumentation.x",
        ".k",
        "SPAN.k",
        "span.",
        "span..a",
        "span.a.",
        "span.a..b",
        "span.a b",
        "span.a`b",
        "span.a%b",
        too_long.as_str(),
        "span.a,span.a",
        "span.a,",
        ",span.a",
        "span.a,,span.b",
        seventeen.as_str(),
    ] {
        let out = Command::new("sh")
            .arg(root.join("schema/schema.sh"))
            .arg("--print")
            .current_dir(&root)
            .env_remove("PULSUS_CLUSTER")
            .env("PULSUS_RETENTION_DAYS", "7")
            .env("PULSUS_TRACEQL_INDEXED_ATTRIBUTES", list)
            .output()
            .expect("the script runs");
        assert_eq!(out.status.code(), Some(2), "{list:?} must be refused");
        if list.starts_with("resource.") {
            let err = String::from_utf8_lossy(&out.stderr);
            assert!(
                err.contains("resource.k8s.pod.name") && err.contains("resource_id"),
                "the refusal names the item and the resource_id index: {err}"
            );
        }
    }
    // A key of exactly 128 bytes renders.
    let at = format!("span.{}", "a.".repeat(63) + "ab");
    let out = Command::new("sh")
        .arg(root.join("schema/schema.sh"))
        .arg("--print")
        .current_dir(&root)
        .env_remove("PULSUS_CLUSTER")
        .env("PULSUS_RETENTION_DAYS", "7")
        .env("PULSUS_TRACEQL_INDEXED_ATTRIBUTES", &at)
        .output()
        .expect("the script runs");
    assert!(out.status.success(), "a 128-byte key is accepted");
}

/// Issue #595 part 2: a refused list stops the script before it reaches the
/// server, so it can never drop a database it will not rebuild. The
/// server named here does not answer: had the script got as far as
/// waiting for it, it would exit 1, not 2.
#[test]
fn a_refused_list_stops_the_script_before_it_touches_the_server() {
    let root = repo_root();
    let out = Command::new("sh")
        .arg(root.join("schema/schema.sh"))
        .current_dir(&root)
        .env_remove("PULSUS_CLUSTER")
        .env("PULSUS_RETENTION_DAYS", "7")
        .env("CLICKHOUSE_SERVER", "127.0.0.1")
        .env("CLICKHOUSE_HTTP_PORT", "1")
        .env("PULSUS_SCHEMA_WAIT_SECONDS", "1")
        .env(
            "PULSUS_TRACEQL_INDEXED_ATTRIBUTES",
            "span.a,resource.k8s.pod.name",
        )
        .output()
        .expect("the script runs");
    let err = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{err}");
    assert!(err.contains("PULSUS_TRACEQL_INDEXED_ATTRIBUTES"), "{err}");
}

/// Issue #595 part 2, D8: `spans` grows each column stream's write buffers
/// as needed instead of allocating them whole, in both modes. The
/// clustered statement has no live run, so this is its only check.
#[test]
fn the_span_table_writes_with_adaptive_buffers_in_both_modes() {
    for (mode, ctx) in [("single", single()), ("clustered", clustered())] {
        let create = create_of(&ctx, "spans");
        let settings = line_of(&create, "SETTINGS");
        assert!(
            settings.contains("min_columns_to_activate_adaptive_write_buffer = 1"),
            "{mode}: {settings}"
        );
    }
}

/// **T4 (issue #624, part 3d-3): the logs and metrics write-path tables
/// grow their write buffers as needed**, as `spans` does, in both modes.
#[test]
fn the_write_path_tables_write_with_adaptive_buffers_in_both_modes() {
    for table in [
        "log_landing",
        "log_samples",
        "metric_landing",
        "metric_samples",
        "metric_hist_samples",
        "metric_exemplars",
        "metric_labels",
        "metric_label_index",
    ] {
        for (mode, ctx) in [("single", single()), ("clustered", clustered())] {
            let create = create_of(&ctx, table);
            let settings = line_of(&create, "SETTINGS");
            assert!(
                settings.contains("min_columns_to_activate_adaptive_write_buffer = 1"),
                "{table}, {mode}: {settings}"
            );
        }
    }
}

/// **T-S2 (issue #653): the log tables lead with the tenant, in both
/// forms.** In the single-node and the clustered rendering, each of the six
/// log tables is created once, keyed with `org_id` first, and neither
/// aggregating table carries the tests-first seam's
/// `allow_dimensions_outside_sorting_key`. One assertion per rendering,
/// naming every table that fails.
#[test]
fn the_log_tables_lead_with_the_tenant_in_both_forms() {
    let keys = [
        (
            "log_landing",
            "(org_id, kind, service, fingerprint, timestamp_ns)",
        ),
        ("log_metrics_5s", "(org_id, fingerprint, bucket_ns)"),
        (
            "log_patterns",
            "(org_id, fingerprint, bucket_ns, pattern, level)",
        ),
        (
            "log_samples",
            "(org_id, service, fingerprint, timestamp_ns)",
        ),
        ("log_streams", "(org_id, fingerprint)"),
        ("log_streams_idx", "(org_id, key, val, fingerprint)"),
    ];
    let clustered = RenderCtx {
        cluster: Some("c1".to_string()),
        ..RenderCtx::for_tests("pulsus", 7)
    };
    for (form, ctx) in [("single-node", single()), ("clustered", clustered)] {
        let stmts = rendered_statements(&ctx);
        let mut failures = Vec::new();
        for (table, key) in keys {
            let head = format!("CREATE TABLE IF NOT EXISTS pulsus.{table}");
            let creates: Vec<&String> = stmts
                .iter()
                .filter(|s| {
                    s.trim_start()
                        .strip_prefix(&head)
                        .is_some_and(|rest| rest.starts_with([' ', '\n']))
                })
                .collect();
            if creates.len() != 1 {
                failures.push(format!("{table}: created {} times", creates.len()));
                continue;
            }
            let order: Vec<&str> = creates[0]
                .lines()
                .filter(|l| l.trim_start().starts_with("ORDER BY"))
                .collect();
            let want = format!("ORDER BY {key}");
            if order != [want.as_str()] {
                failures.push(format!("{table}: {order:?}, expected {want:?}"));
            }
            if creates[0].contains("allow_dimensions_outside_sorting_key") {
                failures.push(format!(
                    "{table}: carries allow_dimensions_outside_sorting_key"
                ));
            }
        }
        assert!(failures.is_empty(), "{form}: {failures:#?}");
    }
}

/// **T-S (issue #738): the trace tables lead with the tenant, in both
/// forms.** In the single-node and the clustered rendering, each of the ten
/// trace tables is created once, keyed with `org_id` first, and none carries
/// the tests-first seam's `allow_dimensions_outside_sorting_key`; the
/// service-graph job's marker read reads the markers under the empty tenant,
/// and its pairing groups by tenant. One assertion per rendering, naming
/// every failure.
#[test]
fn the_trace_tables_lead_with_the_tenant_in_both_forms() {
    let spans_key = "(org_id, intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)";
    let keys = [
        ("resources", "(org_id, service, resource_id)"),
        ("spans", spans_key),
        ("tag_names", "(org_id, scope, key)"),
        ("tag_values", "(org_id, scope, key, value, val_type)"),
        (
            "trace_landing",
            "(org_id, row_kind, trace_id, start_ns, span_id, kind, tag_key, tag_value)",
        ),
        ("traces", "(org_id, trace_id)"),
        (
            "service_edges",
            "(org_id, conn_type, minute, client_minute, client, server)",
        ),
        ("tag_bucket_values", "(org_id, service, scope, key, bucket)"),
        ("error_spans", spans_key),
        (
            "service_bucket_resources",
            "(org_id, service, bucket, resource_id)",
        ),
    ];
    let clustered = RenderCtx {
        cluster: Some("c1".to_string()),
        ..RenderCtx::for_tests("pulsus", 7)
    };
    for (form, ctx) in [("single-node", single()), ("clustered", clustered)] {
        let stmts = rendered_statements(&ctx);
        let mut failures = Vec::new();
        for (table, key) in keys {
            let head = format!("CREATE TABLE IF NOT EXISTS pulsus.{table}");
            let creates: Vec<&String> = stmts
                .iter()
                .filter(|s| {
                    s.trim_start()
                        .strip_prefix(&head)
                        .is_some_and(|rest| rest.starts_with([' ', '\n']))
                })
                .collect();
            if creates.len() != 1 {
                failures.push(format!("{table}: created {} times", creates.len()));
                continue;
            }
            let order: Vec<&str> = creates[0]
                .lines()
                .filter(|l| l.trim_start().starts_with("ORDER BY"))
                .collect();
            let want = format!("ORDER BY {key}");
            if order != [want.as_str()] {
                failures.push(format!("{table}: {order:?}, expected {want:?}"));
            }
            if creates[0].contains("allow_dimensions_outside_sorting_key") {
                failures.push(format!(
                    "{table}: carries allow_dimensions_outside_sorting_key"
                ));
            }
        }
        let jobs: Vec<&String> = stmts
            .iter()
            .filter(|s| {
                s.trim_start()
                    .starts_with("CREATE MATERIALIZED VIEW pulsus.service_edges_mv")
            })
            .collect();
        if let [job] = jobs.as_slice() {
            let markers = job
                .matches("FROM merge('pulsus', '^service_edges$') FINAL WHERE ")
                .count();
            let scoped = job
                .matches(
                    "FROM merge('pulsus', '^service_edges$') FINAL WHERE org_id = '' AND conn_type = ''",
                )
                .count();
            if markers != 1 || scoped != 1 {
                failures.push(format!(
                    "service_edges_mv: {scoped} of {markers} marker reads under org_id = ''"
                ));
            }
            if !job.contains("GROUP BY org_id, trace_id, k\n") {
                failures.push("service_edges_mv: pairing not GROUP BY org_id, trace_id, k".into());
            }
        } else {
            failures.push(format!("service_edges_mv: created {} times", jobs.len()));
        }
        assert!(failures.is_empty(), "{form}: {failures:#?}");
    }
}

/// Issue #614, part 2: the rollup keeps a bucket exactly as long as
/// `log_samples` keeps the lines it counts, so a window past retention
/// answers alike on the rollup route and the raw route.
#[test]
fn the_rollup_is_kept_as_long_as_the_lines_it_counts() {
    let stmts = rendered_statements(&single());
    let create = |table: &str| {
        stmts
            .iter()
            .find(|s| s.contains(&format!("CREATE TABLE IF NOT EXISTS pulsus.{table}\n")))
            .unwrap_or_else(|| panic!("no CREATE for {table}"))
            .clone()
    };
    let ttl = |stmt: &str| {
        stmt.lines()
            .find(|l| l.starts_with("TTL "))
            .map(str::to_string)
    };
    let samples = create("log_samples");
    let rollup = create("log_metrics_5s");
    assert_eq!(
        ttl(&rollup),
        ttl(&samples).map(|l| l.replace("timestamp_ns", "bucket_ns")),
        "the rollup's TTL is log_samples' over bucket_ns"
    );
    for stmt in [&samples, &rollup] {
        assert!(stmt.contains("ttl_only_drop_parts = 1"), "{stmt}");
    }
}

// ---------------------------------------------------------------------------
// Issue #599: the service-graph totals and the job that appends them.
// ---------------------------------------------------------------------------

/// `service_edges` as the design writes it (part A §3.1), rendered with
/// `db = pulsus`, `retention_days = 7` and no storage policy.
const SERVICE_EDGES_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS pulsus.service_edges
(
    org_id LowCardinality(String),
    minute DateTime('UTC') CODEC(Delta, ZSTD(1)),
    client_minute DateTime('UTC') CODEC(Delta, ZSTD(1)),
    client LowCardinality(String) CODEC(ZSTD(1)),
    server LowCardinality(String) CODEC(ZSTD(1)),
    conn_type LowCardinality(String) CODEC(ZSTD(1)),
    calls UInt64 CODEC(ZSTD(1)),
    failed UInt64 CODEC(ZSTD(1)),
    q AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Int64) CODEC(ZSTD(1)),
    ver UInt64 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY toDate(minute)
ORDER BY (org_id, conn_type, minute, client_minute, client, server)
TTL toDateTime(least(toUInt32(minute) - if(conn_type = '', 240, 0) + (7 * 86400), 4294967295))
SETTINGS index_granularity = 1024, ttl_only_drop_parts = 1;"#;

/// `service_edges_mv` as the design writes it (part A §3.2, part C §3),
/// rendered the same way, with the production row ceiling. Its first line
/// and its `hi` line are the single-node variant's (`--@single`), which the
/// render strips with the indentation after the prefix. Both tables are
/// read through `merge()`, so each is read when the scalars before it have
/// run (part C D6).
const SERVICE_EDGES_VIEW: &str = r#"CREATE MATERIALIZED VIEW pulsus.service_edges_mv REFRESH EVERY 1 MINUTE APPEND TO pulsus.service_edges
AS WITH (SELECT arraySort(groupArray(minute)) FROM merge('pulsus', '^service_edges$') FINAL WHERE org_id = '' AND conn_type = '') AS ms,
     ms[arrayFirstIndex(i -> i = length(ms) OR ms[i + 1] - ms[i] != 300, arrayEnumerate(ms))] AS wm,
     toStartOfInterval(now('UTC') - INTERVAL 3 MINUTE, INTERVAL 5 MINUTE) AS ready,
     (SELECT toDateTime(min(toDate(parseDateTimeBestEffort(partition))), 'UTC') FROM system.parts WHERE database = 'pulsus' AND table = 'spans' AND active) AS oldest,
     if(wm = 0, if(oldest = 0, ready - INTERVAL 5 MINUTE, oldest), wm + INTERVAL 1 MINUTE) AS lo,
least(ready, lo + INTERVAL 5 MINUTE) AS hi,
     toInt64(toUnixTimestamp(lo)) * 1000000000 AS slo,
     toInt64(toUnixTimestamp(hi)) * 1000000000 AS shi,
     toUInt64(toUnixTimestamp64Milli(now64())) AS v
SELECT * FROM (
SELECT org_id, toDateTime(intDiv(s.6, 60000000000) * 60, 'UTC') AS minute, toDateTime(intDiv(c.4, 60000000000) * 60, 'UTC') AS client_minute,
       c.1 AS client, s.2 AS server, if(c.3 = 3, 'rpc', 'messaging') AS conn_type,
       count() AS calls, countIf(s.3 = 2 OR c.2 = 2) AS failed,
       quantilesTDigestState(0.5, 0.95, 0.99)(s.4) AS q, v AS ver
FROM (SELECT org_id, trace_id,
        if(kind IN (3, 4) OR attrs.`zipkin%2Eshared`.:String = 'true', span_id, parent_span_id) AS k,
        anyIf((service, status_code, kind, start_ns), kind IN (3, 4)) AS c,
        countIf(kind IN (3, 4)) AS nc,
        groupArrayIf((span_id, service, status_code, duration_ns, kind, start_ns), kind IN (2, 5)) AS ss
 FROM merge('pulsus', '^spans$')
 WHERE start_ns >= slo - 60000000000 AND start_ns < shi + 60000000000
   AND intDiv(start_ns, 300000000000) BETWEEN intDiv(slo - 60000000000, 300000000000) AND intDiv(shi + 59999999999, 300000000000)
   AND ((kind IN (2, 5) AND start_ns >= slo AND start_ns < shi) OR kind IN (3, 4))
 GROUP BY org_id, trace_id, k
 HAVING nc > 0)
ARRAY JOIN arrayDistinct(ss) AS s
WHERE ((c.3 = 3 AND s.5 = 2) OR (c.3 = 4 AND s.5 = 5)) AND shi > slo
GROUP BY org_id, minute, client_minute, client, server, conn_type
UNION ALL
SELECT '', hi - INTERVAL 1 MINUTE, hi - INTERVAL 1 MINUTE, '', '', '', 0, 0, quantilesTDigestState(0.5, 0.95, 0.99)(toInt64(0)), v
FROM system.one
HAVING hi > lo)
ORDER BY conn_type = '', org_id, minute, client_minute, client, server, conn_type
SETTINGS max_threads = 4, max_memory_usage = 4294967296, max_rows_to_read = 60000000, read_overflow_mode = 'throw', max_execution_time = 50,
         max_insert_threads = 1, max_insert_block_size = 100000000, min_insert_block_size_rows = 100000000, min_insert_block_size_bytes = 10000000000;"#;

/// The clustered view's first line (part C §3), in place of the
/// single-node one: the replication log copied (for `spans`, or its
/// uncopied entries counted and listed, at most 10,000) and the replica not
/// under repair for both tables, then each table's queue read entry by entry from
/// Keeper, and the watermark read only when `service_edges`' queue holds
/// nothing but merges and fetches or attaches of parts the replica holds.
/// Its lines carry the `--@cluster` prefix, which the
/// render strips with the indentation after it.
const SERVICE_EDGES_HEAD: &str = r#"AS WITH (SELECT groupArray((replica_path, log_pointer > log_max_index OR (log_pointer = 0 AND log_max_index = 0), zookeeper_path, log_pointer)) FROM system.replicas WHERE database = 'pulsus' AND table IN ('spans', 'service_edges')) AS logs,
(SELECT groupArray((name, numChildren, value)) FROM system.zookeeper WHERE path IN (SELECT arrayJoin(arrayMap(x -> x.1, logs))) AND name IN ('queue', 'is_lost')) AS nodes,
(SELECT groupArray((path, numChildren)) FROM system.zookeeper WHERE path IN (SELECT arrayJoin(arrayMap(x -> x.3, arrayFilter(x -> position(x.1, '.spans/') > 0 AND NOT x.2, logs)))) AND name = 'log') AS loglen,
(SELECT groupArray((toUInt64OrZero(substring(name, 5)), ctime, extract(value, '^format version: \\d+\ncreate_time: [^\n]*\nsource replica: [^\n]*\nblock_id: [^\n]*\n(?:log_entry_id: [^\n]*\n)?([A-Za-z_]+)\n'), extract(value, '\nsource replica: ([^\n]*)\n'))) FROM (SELECT name, ctime, value FROM system.zookeeper WHERE path IN (SELECT arrayJoin(arrayMap(x -> x.3 || '/log', arrayFilter(x -> position(x.1, '.spans/') > 0 AND NOT x.2 AND length(loglen) = 1 AND loglen[1].2 <= 10000, logs)))) LIMIT 10001) SETTINGS max_execution_time = 10) AS logents,
length(logs) = 2 AND arrayAll(x -> x.2 OR (position(x.1, '.spans/') > 0 AND length(loglen) = 1 AND loglen[1].2 <= 10000 AND length(logents) <= 10000), logs) AND arrayCount(n -> n.1 = 'is_lost' AND n.3 = '0', nodes) = 2 AND arrayCount(n -> n.1 = 'queue' AND n.2 <= 1000, nodes) = 2 AS listable,
(SELECT groupArray((position(path, '.service_edges/') > 0, extract(value, '^format version: \\d+\ncreate_time: [^\n]*\nsource replica: [^\n]*\nblock_id: [^\n]*\n(?:log_entry_id: [^\n]*\n)?([A-Za-z_]+)\n'), parseDateTimeBestEffortOrZero(extract(value, '\ncreate_time: ([^\n]*)\n'), serverTimezone()), extract(value, '\n(?:get|attach)\n([^\n]+)'), extract(value, '\nsource replica: ([^\n]*)\n'))) FROM system.zookeeper WHERE path IN (SELECT arrayJoin(arrayMap(x -> x.1 || if(listable, '/queue', '/log_pointer'), logs))) SETTINGS max_execution_time = 10) AS entries,
(SELECT groupArray((partition_id, min_block_number, max_block_number)) FROM system.parts WHERE database = 'pulsus' AND table = 'service_edges' AND active AND has(arrayMap(e -> extract(e.4, '^([^_]+)_[0-9]+_[0-9]+_[0-9]+(?:_[0-9]+)?$'), arrayFilter(e -> e.1, entries)), partition_id)) AS local_edges,
listable AND arrayCount(e -> e.1, entries) <= 1000 AND arrayCount(e -> NOT e.1, entries) <= 1000 AND arrayAll(e -> NOT e.1 OR e.2 = 'merge' OR (e.2 IN ('get', 'attach') AND e.5 != '' AND arrayExists(p -> p.1 = extract(e.4, '^([^_]+)_[0-9]+_[0-9]+_[0-9]+(?:_[0-9]+)?$') AND p.2 <= toInt64OrZero(extract(e.4, '^[^_]+_([0-9]+)_')) AND toInt64OrZero(extract(e.4, '^[^_]+_[0-9]+_([0-9]+)_')) <= p.3, local_edges)), entries) AS edges_ok,
(SELECT arraySort(groupArray(minute)) FROM merge('pulsus', '^service_edges$') FINAL WHERE org_id = '' AND conn_type = '' AND edges_ok) AS ms,
"#;

/// The clustered view's replica barrier (part C §3), in place of the
/// single-node `hi` line: a run pairs an empty range and writes no marker
/// unless every `spans` entry queued on this replica is a merge, a fetch
/// or an attach of rows it already holds — in the same part or in one that
/// has since merged it — or a fetch an insert queued at
/// least 10 s after the interval became eligible, and every uncopied
/// `spans` log entry is a merge or an insert's fetch committed to the log
/// at least 10 s after that eligibility. Its lines carry the
/// `--@cluster` prefix, which the render strips with the indentation after
/// it.
const SERVICE_EDGES_BARRIER: &str = r#"
least(ready, lo + INTERVAL 5 MINUTE) AS hi0,
(SELECT groupArray((partition_id, min_block_number, max_block_number)) FROM system.parts WHERE database = 'pulsus' AND table = 'spans' AND active AND has(arrayMap(e -> extract(e.4, '^([^_]+)_[0-9]+_[0-9]+_[0-9]+(?:_[0-9]+)?$'), arrayFilter(e -> NOT e.1, entries)), partition_id)) AS local_named,
edges_ok AND arrayAll(x -> x.2 OR arrayAll(e -> e.1 < x.4 OR e.3 = 'merge' OR (e.3 = 'get' AND e.4 != '' AND e.2 >= hi0 + INTERVAL 3 MINUTE + INTERVAL 10 SECOND), logents), logs) AND arrayAll(e -> e.1 OR e.2 = 'merge' OR (e.2 IN ('get', 'attach') AND e.5 != '' AND e.4 != '' AND toUnixTimestamp(e.3) > 0 AND (arrayExists(p -> p.1 = extract(e.4, '^([^_]+)_[0-9]+_[0-9]+_[0-9]+(?:_[0-9]+)?$') AND p.2 <= toInt64OrZero(extract(e.4, '^[^_]+_([0-9]+)_')) AND toInt64OrZero(extract(e.4, '^[^_]+_[0-9]+_([0-9]+)_')) <= p.3, local_named) OR (e.2 = 'get' AND e.3 >= hi0 + INTERVAL 3 MINUTE + INTERVAL 10 SECOND))), entries) AS caught_up,
if(caught_up, hi0, lo) AS hi,
"#;

/// The rendered statement beginning `head`, exactly one of it.
fn statement_starting(ctx: &RenderCtx, head: &str) -> String {
    let found: Vec<String> = rendered_statements(ctx)
        .into_iter()
        .filter(|s| s.starts_with(head))
        .collect();
    assert_eq!(found.len(), 1, "one statement begins {head:?}: {found:?}");
    found.into_iter().next().expect("one statement")
}

/// **U3: the two statements are the design's, byte for byte, in both
/// variants.** The live cases cannot see most of what is here — a codec, the
/// partition key, the 3-minute wait, the 60 s margins, the marker's `240`, a
/// ceiling or an insert setting — so the whole text is the assertion.
#[test]
fn the_service_edges_statements_are_the_plan_s() {
    let table_head = "CREATE TABLE IF NOT EXISTS pulsus.service_edges\n";
    let view_head = "CREATE MATERIALIZED VIEW pulsus.service_edges_mv ";
    assert_eq!(
        statement_starting(&single(), table_head),
        SERVICE_EDGES_TABLE,
        "single-node service_edges"
    );
    assert_eq!(
        statement_starting(&single(), view_head),
        SERVICE_EDGES_VIEW,
        "single-node service_edges_mv"
    );

    let clustered_table = SERVICE_EDGES_TABLE
        .replacen(
            "pulsus.service_edges\n",
            "pulsus.service_edges ON CLUSTER 'prod'\n",
            1,
        )
        .replacen(
            "ENGINE = ReplacingMergeTree(ver)",
            "ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/pulsus.service_edges', '{replica}', ver)",
            1,
        );
    let hi = "\nleast(ready, lo + INTERVAL 5 MINUTE) AS hi,\n";
    assert!(SERVICE_EDGES_VIEW.contains(hi), "the single-node `hi` line");
    let head = "AS WITH (SELECT arraySort(groupArray(minute)) FROM merge('pulsus', \
                '^service_edges$') FINAL WHERE org_id = '' AND conn_type = '') AS ms,\n";
    assert!(
        SERVICE_EDGES_VIEW.contains(head),
        "the single-node first line"
    );
    let clustered_view = SERVICE_EDGES_VIEW
        .replacen(
            "pulsus.service_edges_mv REFRESH",
            "pulsus.service_edges_mv ON CLUSTER 'prod' REFRESH",
            1,
        )
        .replacen(hi, SERVICE_EDGES_BARRIER, 1)
        .replacen(head, SERVICE_EDGES_HEAD, 1);
    assert_eq!(
        statement_starting(
            &clustered(),
            "CREATE TABLE IF NOT EXISTS pulsus.service_edges ON CLUSTER 'prod'\n"
        ),
        clustered_table,
        "clustered service_edges"
    );
    assert_eq!(
        statement_starting(&clustered(), view_head),
        clustered_view,
        "clustered service_edges_mv"
    );
}

// ---------------------------------------------------------------------------
// Issue #497: every table's expiry, exactly.
// ---------------------------------------------------------------------------

/// Each table the file creates, with the exact `TTL` line it renders at
/// `retention_days = 17` and landing hours 3 (metrics), 4 (logs) and 5
/// (traces), whether it drops whole parts (`ttl_only_drop_parts = 1`), and its
/// `merge_with_ttl_timeout` when set. `None` is a table with no TTL. The
/// literals are written from docs/configuration.md §7, not read back from the
/// renderer.
const EXPIRY: &[(&str, Option<&str>, bool, Option<u32>)] = &[
    (
        "log_landing",
        Some("TTL toDateTime(least(intDiv(received_ms, 1000) + (4 * 3600), 4294967295))"),
        true,
        Some(3600),
    ),
    (
        "log_metrics_5s",
        Some("TTL toDateTime(least(intDiv(bucket_ns, 1000000000) + (17 * 86400), 4294967295))"),
        true,
        None,
    ),
    (
        "log_patterns",
        Some("TTL toDateTime(least(intDiv(bucket_ns, 1000000000) + (17 * 86400), 4294967295))"),
        true,
        None,
    ),
    (
        "log_samples",
        Some("TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + (17 * 86400), 4294967295))"),
        true,
        None,
    ),
    (
        "log_streams",
        Some(
            "TTL toDateTime(least((toUInt32(addMonths(month, 1)) * 86400) + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "log_streams_idx",
        Some(
            "TTL toDateTime(least((toUInt32(addMonths(month, 1)) * 86400) + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "metric_exemplars",
        Some("TTL toDateTime(least(intDiv(unix_milli, 1000) + (17 * 86400), 4294967295))"),
        true,
        None,
    ),
    (
        "metric_hist_samples",
        Some("TTL toDateTime(least(intDiv(unix_milli, 1000) + (17 * 86400), 4294967295))"),
        true,
        None,
    ),
    (
        "metric_label_index",
        Some(
            "TTL toDateTime(least(((intDiv(last_seen, 86400000) + 1) * 86400) + (17 * 86400), 4294967295))",
        ),
        false,
        None,
    ),
    (
        "metric_label_values",
        Some(
            "TTL toDateTime(least(((intDiv(last_seen, 86400000) + 1) * 86400) + (17 * 86400), 4294967295))",
        ),
        false,
        None,
    ),
    (
        "metric_labels",
        Some(
            "TTL toDateTime(least(((intDiv(last_seen, 86400000) + 1) * 86400) + (17 * 86400), 4294967295))",
        ),
        false,
        None,
    ),
    (
        "metric_landing",
        Some("TTL toDateTime(least(intDiv(received_ms, 1000) + (3 * 3600), 4294967295))"),
        true,
        Some(3600),
    ),
    (
        "metric_metadata",
        Some("TTL toDateTime(least(intDiv(updated_ns, 1000000000) + (17 * 86400), 4294967295))"),
        false,
        None,
    ),
    (
        "metric_samples",
        Some("TTL toDateTime(least(intDiv(unix_milli, 1000) + (17 * 86400), 4294967295))"),
        true,
        None,
    ),
    // Issue #18: the rollup levels, one row each, due at the start of the
    // UTC day of `t_end − 1 ms` plus the retention.
    (
        "metric_samples_60s",
        Some(
            "TTL toDateTime(least(intDiv(t_end - 1, 86400000) * 86400 + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "metric_samples_720s",
        Some(
            "TTL toDateTime(least(intDiv(t_end - 1, 86400000) * 86400 + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "metric_samples_7200s",
        Some(
            "TTL toDateTime(least(intDiv(t_end - 1, 86400000) * 86400 + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "metric_samples_86400s",
        Some(
            "TTL toDateTime(least(intDiv(t_end - 1, 86400000) * 86400 + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "metric_series",
        Some("TTL toDateTime(least((toUInt64(toUInt16(day)) + 1 + 17) * 86400, 4294967295))"),
        true,
        None,
    ),
    (
        "resources",
        Some("TTL toDateTime(least(((toUInt32(day) + 1) * 86400) + (17 * 86400), 4294967295))"),
        true,
        None,
    ),
    (
        "service_bucket_resources",
        Some(
            "TTL toDateTime(least(intDiv(bucket * 300000000000, 1000000000) + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "service_edges",
        Some(
            "TTL toDateTime(least(toUInt32(minute) - if(conn_type = '', 240, 0) + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "spans",
        Some("TTL toDateTime(least(intDiv(start_ns, 1000000000) + (17 * 86400), 4294967295))"),
        true,
        None,
    ),
    (
        "error_spans",
        Some("TTL toDateTime(least(intDiv(start_ns, 1000000000) + (17 * 86400), 4294967295))"),
        true,
        None,
    ),
    (
        "tag_bucket_values",
        Some(
            "TTL toDateTime(least(intDiv(bucket * 300000000000, 1000000000) + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "tag_names",
        Some(
            "TTL toDateTime(least(((intDiv(last_start_ns, 86400000000000) + 1) * 86400) + (17 * 86400), 4294967295))",
        ),
        false,
        None,
    ),
    (
        "tag_values",
        Some(
            "TTL toDateTime(least(((intDiv(last_start_ns, 86400000000000) + 1) * 86400) + (17 * 86400), 4294967295))",
        ),
        false,
        None,
    ),
    (
        "trace_landing",
        Some("TTL toDateTime(least(intDiv(received_ms, 1000) + (5 * 3600), 4294967295))"),
        true,
        Some(3600),
    ),
    (
        "traces",
        Some(
            "TTL toDateTime(least(((intDiv(last_start_ns, 86400000000000) + 1) * 86400) + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
];

/// Issue #496: every day partition is a UTC day. A lookup row is due at
/// the end of the UTC day its latest instant falls in, plus the retention,
/// which outlives a part only when the part's day is a UTC day too. A
/// `Date` or `DateTime('UTC')` column is one already; a conversion from an
/// epoch integer names `'UTC'`, or it takes the server's time zone.
#[test]
fn every_day_partition_is_a_utc_day() {
    for base in [single(), clustered()] {
        let mut by_form: Vec<String> = Vec::new();
        for stmt in rendered_statements(&base) {
            for line in stmt.lines().filter(|l| l.starts_with("PARTITION BY ")) {
                by_form.push(line.to_string());
            }
        }
        by_form.sort();
        by_form.dedup();
        assert_eq!(
            by_form,
            vec![
                "PARTITION BY day",
                "PARTITION BY month",
                "PARTITION BY toDate(fromUnixTimestamp64Milli(t_end - 1), 'UTC')",
                "PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli), 'UTC')",
                "PARTITION BY toDate(fromUnixTimestamp64Nano(bucket * 300000000000), 'UTC')",
                "PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns), 'UTC')",
                "PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns), 'UTC')",
                "PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns), 'UTC')",
                "PARTITION BY toDate(minute)",
                "PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))",
            ],
            "{:?}",
            base.cluster
        );
    }
}

/// Issue #497: every table's TTL line, part-drop setting and TTL merge
/// interval are exactly [`EXPIRY`]'s, in both variants, and the database
/// comment states the same retention. The hours differ per signal and from
/// the retention, so a swapped token, a changed anchor or a lost setting
/// fails; so does a table added without a row.
#[test]
fn every_table_expires_exactly_as_documented() {
    for base in [single(), clustered()] {
        let variant = if base.cluster.is_some() {
            "clustered"
        } else {
            "single"
        };
        let ctx = RenderCtx {
            retention_days: 17,
            metrics_landing_retention_hours: 3,
            log_landing_retention_hours: 4,
            trace_landing_retention_hours: 5,
            ..base
        };
        let mut got = Vec::new();
        let mut comment = None;
        for stmt in rendered_statements(&ctx) {
            if stmt.starts_with("CREATE DATABASE") {
                comment = stmt
                    .split_once(" COMMENT ")
                    .map(|(_, c)| c.trim_end_matches(';').to_string());
                continue;
            }
            let Some(rest) = stmt.strip_prefix("CREATE TABLE IF NOT EXISTS pulsus.") else {
                continue;
            };
            if stmt.contains("ENGINE = Distributed(") {
                continue;
            }
            let name = rest
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .next()
                .unwrap();
            let ttl: Vec<&str> = stmt.lines().filter(|l| l.starts_with("TTL ")).collect();
            assert!(ttl.len() <= 1, "{variant} {name}: {ttl:?}");
            let settings = stmt
                .lines()
                .find(|l| l.starts_with("SETTINGS "))
                .unwrap_or_else(|| panic!("{variant} {name}: no SETTINGS line"));
            let timeout = settings
                .split(", ")
                .find_map(|s| {
                    s.trim_start_matches("SETTINGS ")
                        .strip_prefix("merge_with_ttl_timeout = ")
                })
                .map(|v| v.parse::<u32>().unwrap());
            got.push((
                name.to_string(),
                ttl.first().map(|l| l.to_string()),
                settings.contains("ttl_only_drop_parts = 1"),
                timeout,
            ));
        }
        got.sort();
        let mut want: Vec<(String, Option<String>, bool, Option<u32>)> = EXPIRY
            .iter()
            .map(|(n, t, p, m)| (n.to_string(), t.map(str::to_string), *p, *m))
            .collect();
        want.sort();
        assert_eq!(got, want, "{variant}: every table's expiry");
        assert_eq!(
            comment.as_deref(),
            Some("'retention_days=17'"),
            "{variant}: the database comment"
        );
    }
}

/// Issue #497: `schema/schema.sh` refuses to render without a retention, and
/// refuses one that is not a whole number of days from 1 to 4294967295, or a
/// landing value that is not a whole number of hours from 1 to 168, before it
/// contacts a server. Each refusal is exit 2 naming the variable.
#[test]
fn the_script_refuses_a_missing_or_malformed_retention() {
    let root = repo_root();
    let run = |vars: &[(&str, &str)]| {
        let mut cmd = Command::new("sh");
        cmd.arg(root.join("schema/schema.sh"))
            .args(["--mode", "single", "--print"])
            .current_dir(&root);
        for var in [
            "PULSUS_RETENTION_DAYS",
            "PULSUS_METRICS_LANDING_RETENTION_HOURS",
            "PULSUS_LOG_LANDING_RETENTION_HOURS",
            "PULSUS_TRACE_LANDING_RETENTION_HOURS",
            "PULSUS_CLUSTER",
        ] {
            cmd.env_remove(var);
        }
        cmd.env("CLICKHOUSE_SERVER", "127.0.0.1")
            .env("CLICKHOUSE_HTTP_PORT", "1");
        for (k, v) in vars {
            cmd.env(k, v);
        }
        let out = cmd.output().expect("run schema.sh");
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    let (code, err) = run(&[]);
    assert_eq!(code, Some(2), "unset: {err}");
    assert!(
        err.contains("PULSUS_RETENTION_DAYS is not set"),
        "unset: {err}"
    );
    for bad in [
        "",
        "0",
        "07",
        "7d",
        "-1",
        "1.5",
        "4294967296",
        "99999999999",
    ] {
        let (code, err) = run(&[("PULSUS_RETENTION_DAYS", bad)]);
        assert_eq!(code, Some(2), "{bad:?}: {err}");
        assert!(err.contains("PULSUS_RETENTION_DAYS"), "{bad:?}: {err}");
    }
    for good in ["1", "4294967295"] {
        let (code, err) = run(&[("PULSUS_RETENTION_DAYS", good)]);
        assert_eq!(code, Some(0), "{good:?}: {err}");
    }
    for var in [
        "PULSUS_METRICS_LANDING_RETENTION_HOURS",
        "PULSUS_LOG_LANDING_RETENTION_HOURS",
        "PULSUS_TRACE_LANDING_RETENTION_HOURS",
    ] {
        for bad in ["0", "06", "169", "1000", "abc"] {
            let (code, err) = run(&[("PULSUS_RETENTION_DAYS", "7"), (var, bad)]);
            assert_eq!(code, Some(2), "{var}={bad:?}: {err}");
            assert!(err.contains(var), "{var}={bad:?}: {err}");
        }
        for good in ["1", "168"] {
            let (code, err) = run(&[("PULSUS_RETENTION_DAYS", "7"), (var, good)]);
            assert_eq!(code, Some(0), "{var}={good:?}: {err}");
        }
    }
}

/// Issue #497: docs/configuration.md §7's expiry table has one row per
/// table the file creates, named in its first cell, and no other.
#[test]
fn the_documented_expiry_table_names_every_table() {
    let doc = std::fs::read_to_string(repo_root().join("docs/configuration.md")).unwrap();
    let section = doc
        .split_once("\n## 7. ")
        .expect("a §7 heading")
        .1
        .split_once("\n## ")
        .expect("a heading after §7")
        .0;
    let mut documented: Vec<String> = section
        .lines()
        .filter(|l| l.starts_with("| `") && !l.starts_with("| `PULSUS_"))
        .map(|l| {
            l.split('|')
                .nth(1)
                .unwrap()
                .trim()
                .trim_matches('`')
                .to_string()
        })
        .collect();
    documented.sort();
    // The rollup levels (issue #18) are one row, `metric_samples_<N>s`.
    let mut want: Vec<String> = EXPIRY
        .iter()
        .map(|(n, ..)| {
            let level = n
                .strip_prefix("metric_samples_")
                .and_then(|rest| rest.strip_suffix('s'))
                .is_some_and(|r| !r.is_empty() && r.bytes().all(|b| b.is_ascii_digit()));
            if level {
                "metric_samples_<N>s".to_string()
            } else {
                n.replace("log_metrics_5s", "log_metrics_<res>")
            }
        })
        .collect();
    want.sort();
    want.dedup();
    assert_eq!(documented, want, "docs/configuration.md §7's expiry rows");
}

// ---------------------------------------------------------------------------
// Issue #18 part A: the rollup levels.
// ---------------------------------------------------------------------------

/// The level statements of `stmts`: each `CREATE` whose object is
/// `metric_samples_<digits>s…`, by its first line.
fn level_heads(stmts: &[String]) -> Vec<String> {
    stmts
        .iter()
        .filter(|s| {
            [
                "CREATE TABLE IF NOT EXISTS pulsus.metric_samples_",
                "CREATE MATERIALIZED VIEW pulsus.metric_samples_",
            ]
            .iter()
            .any(|head| {
                s.strip_prefix(head)
                    .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
            })
        })
        .map(|s| {
            s.lines()
                .next()
                .expect("a statement has a line")
                .to_string()
        })
        .collect()
}

/// The one statement whose first line is `head`.
fn statement_headed(stmts: &[String], head: &str) -> String {
    let hits: Vec<&String> = stmts
        .iter()
        .filter(|s| s.lines().next() == Some(head))
        .collect();
    assert_eq!(hits.len(), 1, "one statement headed {head:?}");
    hits[0].clone()
}

/// **A1 (issue #18 part A): the rollup levels render from one template.**
/// For each level set and both variants, the level statements come in level
/// order, each level's table then (clustered) its wrapper then its view, and
/// each view's timer is the one plan Q4 derives from the sizes. Each table
/// names its level in its comment, expires by the UTC day of `t_end − 1 ms`,
/// is keyed by the series, and carries the projection the next level's run
/// reads exactly when that next level is shorter than a day. The first view reads `metric_landing` with
/// exact repeats dropped and the landing block named; every other view
/// reads the level below with its block named; every view writes its run
/// row in the same statement and runs on two threads. Nothing names the
/// removed `metric_rollup`, and `metric_landing` is keyed by the block.
#[test]
fn the_rollup_levels_render_from_one_template() {
    let sets: [(&[u32], &[u32], &[&str]); 3] = [
        (
            &[60, 720, 7200, 86400],
            &[86400, 604800, 7776000],
            &[
                "EVERY 1 MINUTE",
                "EVERY 12 MINUTE OFFSET 240 SECOND",
                "EVERY 2 HOUR OFFSET 300 SECOND",
                "EVERY 1 DAY OFFSET 360 SECOND",
            ],
        ),
        (
            &[5, 60, 720, 7200, 86400],
            &[3600, 86400, 604800, 7776000],
            &[
                "EVERY 1 MINUTE",
                "EVERY 1 MINUTE",
                "EVERY 12 MINUTE OFFSET 300 SECOND",
                "EVERY 2 HOUR OFFSET 360 SECOND",
                "EVERY 1 DAY OFFSET 420 SECOND",
            ],
        ),
        (
            &[720, 86400],
            &[7776000],
            &["EVERY 12 MINUTE", "EVERY 1 DAY OFFSET 780 SECOND"],
        ),
    ];
    for (levels, ranges, timers) in sets {
        assert_eq!(levels.len(), timers.len());
        for base in [single(), clustered()] {
            let ctx = RenderCtx {
                rollup_levels: levels.to_vec(),
                rollup_ranges: ranges.to_vec(),
                ..base
            };
            let what = format!("{levels:?}, cluster {:?}", ctx.cluster);
            let on = if ctx.cluster.is_some() {
                " ON CLUSTER 'prod'"
            } else {
                ""
            };
            let stmts = rendered_statements(&ctx);
            let text = rendered(&ctx);
            assert!(!text.contains("metric_rollup"), "{what}: metric_rollup");

            let mut want_heads = Vec::new();
            for (i, r) in levels.iter().enumerate() {
                want_heads.push(format!(
                    "CREATE TABLE IF NOT EXISTS pulsus.metric_samples_{r}s{on}"
                ));
                if ctx.cluster.is_some() {
                    want_heads.push(format!(
                        "CREATE TABLE IF NOT EXISTS pulsus.metric_samples_{r}s_dist{on} AS pulsus.metric_samples_{r}s"
                    ));
                }
                want_heads.push(format!(
                    "CREATE MATERIALIZED VIEW pulsus.metric_samples_{r}s_mv{on} REFRESH {} APPEND TO pulsus.metric_samples_{r}s",
                    timers[i]
                ));
            }
            assert_eq!(
                level_heads(&stmts),
                want_heads,
                "{what}: the level statements"
            );

            for (i, r) in levels.iter().enumerate() {
                let table = statement_headed(
                    &stmts,
                    &format!("CREATE TABLE IF NOT EXISTS pulsus.metric_samples_{r}s{on}"),
                );
                let lines: Vec<&str> = table.lines().collect();
                let from = match i {
                    0 => "0".to_string(),
                    _ => format!("{}000", ranges[i - 1]),
                };
                for line in [
                    format!(
                        "COMMENT 'rollup_res_ms={r}000 rollup_from_ms={from} counter_buckets=5';"
                    ),
                    "TTL toDateTime(least(intDiv(t_end - 1, 86400000) * 86400 + (7 * 86400), 4294967295))".to_string(),
                    "ORDER BY (org_id, fingerprint, t_end)".to_string(),
                    "PARTITION BY toDate(fromUnixTimestamp64Milli(t_end - 1), 'UTC')".to_string(),
                ] {
                    assert!(lines.contains(&line.as_str()), "{what}: {r}s lacks {line}: {table}");
                }
                match levels.get(i + 1).filter(|next| **next < 86_400) {
                    Some(next) => assert!(
                        lines.contains(&format!(
                            "    , PROJECTION by_parent (SELECT * ORDER BY (intDiv(t_end - 1, {next}000), org_id, fingerprint, t_end))"
                        ).as_str()),
                        "{what}: {r}s's projection is keyed by the next level: {table}"
                    ),
                    None => assert!(
                        !table.contains("PROJECTION"),
                        "{what}: a level whose next level is a day or none has no projection: {table}"
                    ),
                }

                let view = statement_headed(
                    &stmts,
                    &format!(
                        "CREATE MATERIALIZED VIEW pulsus.metric_samples_{r}s_mv{on} REFRESH {} APPEND TO pulsus.metric_samples_{r}s",
                        timers[i]
                    ),
                );
                if i == 0 {
                    let landing = if ctx.cluster.is_some() {
                        "FROM clusterAllReplicas('prod', pulsus.metric_landing)\n"
                    } else {
                        "FROM pulsus.metric_landing\n"
                    };
                    for needle in [
                        landing,
                        "SELECT DISTINCT org_id, fingerprint, unix_milli",
                        "AND intDiv(unix_milli, 300000) BETWEEN intDiv(lo - 120000, 300000) AND intDiv(hi, 300000)",
                    ] {
                        assert!(
                            view.contains(needle),
                            "{what}: {r}s's view lacks {needle:?}: {view}"
                        );
                    }
                    // Issue #18 part B: a series with no sample in the
                    // lookback takes its predecessor from the level's own
                    // newest row before `lo`, up to a day back, and no
                    // predecessor more than a day before a sample is used.
                    let own = format!("FROM pulsus.metric_samples_{r}s\n");
                    for needle in [
                        "anyLast(if(bits = 9218868437227405314, NULL, toNullable(reinterpretAsFloat64(bits)))) OVER (PARTITION BY org_id, fingerprint ORDER BY unix_milli, bits ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS q0,",
                        "anyLast(if(bits = 9218868437227405314, NULL, toNullable(unix_milli))) OVER (PARTITION BY org_id, fingerprint ORDER BY unix_milli, bits ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS t0",
                        "maxIf(unix_milli, bits != 9218868437227405314) AS t_last, argMaxIf(value, (unix_milli, bits), bits != 9218868437227405314) AS last,",
                        "sum(d) AS inc, toUInt8(if(argMax(bits, (unix_milli, bits)) = 9218868437227405314, 1, 0) + if(countIf(isNaN(value) AND bits != 9218868437227405314) > 0, 2, 0)) AS flags, any(now_ms) AS built_at",
                        "if(isNull(q0), if(unix_milli - tp0 <= 86400000, p0, NULL), if(unix_milli - t0 <= 86400000, q0, NULL)) AS q",
                        "toNullable(argMaxIf(last, t_last, t_last > 0)) AS p0,",
                        "toNullable(maxIf(t_last, t_last > 0)) AS tp0",
                        own.as_str(),
                        "WHERE kind = 0 AND t_end <= lo AND t_end > lo - 86400000",
                        "AND (org_id, fingerprint) IN (SELECT org_id, fingerprint",
                        "GROUP BY org_id, fingerprint HAVING minIf(unix_milli, reinterpretAsUInt64(value) != 9218868437227405314) > lo)",
                        "USING (org_id, fingerprint)",
                    ] {
                        assert!(
                            view.contains(needle),
                            "{what}: {r}s's view lacks {needle:?}: {view}"
                        );
                    }
                } else {
                    let source = levels[i - 1];
                    for needle in [
                        format!("FROM pulsus.metric_samples_{source}s\n"),
                        format!(
                            "AND intDiv(t_end - 1, {r}000) BETWEEN intDiv(lo, {r}000) AND intDiv(hi - 1, {r}000)"
                        ),
                        // Issue #18 part B: flag 1 from the newest row, flag
                        // 2 from any.
                        "sum(ic) AS inc, toUInt8(bitAnd(argMax(fl, te), 1) + bitAnd(groupBitOr(fl), 2)) AS flags, any(now_ms) AS built_at".to_string(),
                        "sum AS sm, count AS ct, inc AS ic, flags AS fl".to_string(),
                    ] {
                        assert!(
                            view.contains(&needle),
                            "{what}: {r}s's view lacks {needle:?}: {view}"
                        );
                    }
                    assert!(
                        !view.contains("metric_landing"),
                        "{what}: {r}s reads landing: {view}"
                    );
                }
                for needle in ["\nUNION ALL\n", "SETTINGS max_threads = 2,"] {
                    assert!(
                        view.contains(needle),
                        "{what}: {r}s's view lacks {needle:?}: {view}"
                    );
                }
            }

            let landing = create_of(&ctx, "metric_landing");
            assert_eq!(
                line_of(&landing, "ORDER BY "),
                "ORDER BY (kind, intDiv(unix_milli, 300000), org_id, metric_name, fingerprint, unix_milli)",
                "{what}: metric_landing is keyed by the 5-minute block"
            );
        }
    }
}

/// **A3 (issue #18 part A): the script and the crate refuse the same level
/// lists, with the same message, and accept the same.** The script exits 2
/// before it contacts a server; the crate returns the message.
#[test]
fn the_script_and_the_crate_refuse_the_same_level_lists() {
    let root = repo_root();
    let run = |levels: &str| {
        let mut cmd = Command::new("sh");
        cmd.arg(root.join("schema/schema.sh"))
            .args(["--mode", "single", "--print"])
            .current_dir(&root);
        for var in [
            "PULSUS_CLUSTER",
            "PULSUS_LOG_LANDING_RETENTION_HOURS",
            "PULSUS_TRACE_LANDING_RETENTION_HOURS",
            "PULSUS_TRACEQL_INDEXED_ATTRIBUTES",
        ] {
            cmd.env_remove(var);
        }
        cmd.env("CLICKHOUSE_SERVER", "127.0.0.1")
            .env("CLICKHOUSE_HTTP_PORT", "1")
            .env("PULSUS_RETENTION_DAYS", "7")
            .env("PULSUS_METRICS_LANDING_RETENTION_HOURS", "6")
            .env("PULSUS_ROLLUP_LEVELS", levels);
        let out = cmd.output().expect("run schema.sh");
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    for (levels, message) in [
        ("60,45", "45 is below 60 and does not divide 60"),
        ("60,90", "90 is not a multiple of 60"),
        ("7,60", "7 does not divide a day (86400)"),
        ("60,60", "60 then 60: the levels must increase"),
        (
            "060",
            "060 is not a whole number of seconds from 1 to 86400",
        ),
        ("60,,720", "an empty item in '60,,720'"),
        (",60", "an empty item in ',60'"),
        ("25,60", "25 is below 60 and does not divide 60"),
        ("1,2,6,12,60,120,240,720,1440", "more than 8 levels"),
        ("0", "0 is not a whole number of seconds from 1 to 86400"),
        (
            "90000",
            "90000 is not a whole number of seconds from 1 to 86400",
        ),
        (
            "14400,86400",
            "the first level 14400 outlives metric_landing: 2 × 14400 + 870 > 6 × 3600",
        ),
    ] {
        let (code, err) = run(levels);
        assert_eq!(code, Some(2), "{levels:?}: the script must refuse: {err}");
        assert_eq!(
            err.trim_end(),
            format!("schema.sh: PULSUS_ROLLUP_LEVELS: {message}"),
            "{levels:?}: the script's message"
        );
        assert_eq!(
            pulsus_schema::parse_rollup_levels(levels, 6),
            Err(message.to_string()),
            "{levels:?}: the crate's message"
        );
    }
    for (levels, want) in [
        ("60,720,7200,86400", vec![60, 720, 7200, 86400]),
        ("5,60", vec![5, 60]),
        ("720,86400", vec![720, 86400]),
        ("", vec![]),
    ] {
        let (code, err) = run(levels);
        assert_eq!(code, Some(0), "{levels:?}: the script must accept: {err}");
        assert_eq!(
            pulsus_schema::parse_rollup_levels(levels, 6),
            Ok(want),
            "{levels:?}: the crate must accept"
        );
    }
}

/// **T6 (issue #18 follow-up): the default levels carry their switch
/// points.** The default render, on one node and clustered, has the five
/// level tables of the owner's table (2026-10-26), each comment naming its
/// width, the query range from which it is the resolution, and the counter
/// bucket count; 7,776,000,000 is written as digits.
#[test]
fn the_default_levels_carry_their_switch_points() {
    let want = [
        (5, "rollup_res_ms=5000 rollup_from_ms=0 counter_buckets=5"),
        (
            60,
            "rollup_res_ms=60000 rollup_from_ms=3600000 counter_buckets=5",
        ),
        (
            720,
            "rollup_res_ms=720000 rollup_from_ms=86400000 counter_buckets=5",
        ),
        (
            7200,
            "rollup_res_ms=7200000 rollup_from_ms=604800000 counter_buckets=5",
        ),
        (
            86400,
            "rollup_res_ms=86400000 rollup_from_ms=7776000000 counter_buckets=5",
        ),
    ];
    for ctx in [single(), clustered()] {
        let on = if ctx.cluster.is_some() {
            " ON CLUSTER 'prod'"
        } else {
            ""
        };
        let stmts = rendered_statements(&ctx);
        let tables: Vec<String> = level_heads(&stmts)
            .into_iter()
            .filter(|h| h.starts_with("CREATE TABLE") && !h.contains("_dist"))
            .collect();
        let want_tables: Vec<String> = want
            .iter()
            .map(|(r, _)| format!("CREATE TABLE IF NOT EXISTS pulsus.metric_samples_{r}s{on}"))
            .collect();
        assert_eq!(tables, want_tables, "the default level tables");
        for (r, comment) in want {
            let table = statement_headed(
                &stmts,
                &format!("CREATE TABLE IF NOT EXISTS pulsus.metric_samples_{r}s{on}"),
            );
            let line = format!("COMMENT '{comment}';");
            assert!(
                table.lines().any(|l| l == line),
                "{r}s lacks {line}: {table}"
            );
        }
    }
}

/// `schema.sh --print` with the three level variables; `None` leaves one
/// unset. Its exit code and standard error.
fn script_levels(
    levels: Option<&str>,
    ranges: Option<&str>,
    buckets: Option<&str>,
) -> (Option<i32>, String, String) {
    let root = repo_root();
    let mut cmd = Command::new("sh");
    cmd.arg(root.join("schema/schema.sh"))
        .args(["--mode", "single", "--print"])
        .current_dir(&root);
    for var in [
        "PULSUS_CLUSTER",
        "PULSUS_LOG_LANDING_RETENTION_HOURS",
        "PULSUS_TRACE_LANDING_RETENTION_HOURS",
        "PULSUS_TRACEQL_INDEXED_ATTRIBUTES",
        "PULSUS_ROLLUP_LEVELS",
        "PULSUS_ROLLUP_RANGES",
        "PULSUS_ROLLUP_COUNTER_BUCKETS",
    ] {
        cmd.env_remove(var);
    }
    cmd.env("CLICKHOUSE_SERVER", "127.0.0.1")
        .env("CLICKHOUSE_HTTP_PORT", "1")
        .env("PULSUS_RETENTION_DAYS", "7")
        .env("PULSUS_METRICS_LANDING_RETENTION_HOURS", "6");
    for (var, value) in [
        ("PULSUS_ROLLUP_LEVELS", levels),
        ("PULSUS_ROLLUP_RANGES", ranges),
        ("PULSUS_ROLLUP_COUNTER_BUCKETS", buckets),
    ] {
        if let Some(v) = value {
            cmd.env(var, v);
        }
    }
    let out = cmd.output().expect("run schema.sh");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// **T5 (issue #18 follow-up): the script and the crate refuse the same
/// switch points and counter bucket counts, with the same message, and
/// accept the same.** The ranges are checked only when there are levels.
#[test]
fn the_script_and_the_crate_refuse_the_same_ranges_and_counter_buckets() {
    let default_levels = "5,60,720,7200,86400";
    for (levels, ranges, message) in [
        (
            default_levels,
            "3600,,604800,7776000",
            "an empty item in '3600,,604800,7776000'",
        ),
        (
            "5,60",
            "1000000000",
            "1000000000 is not a whole number of seconds from 1 to 999999999",
        ),
        (
            "5,60",
            "0",
            "0 is not a whole number of seconds from 1 to 999999999",
        ),
        (
            "5,60",
            "03600",
            "03600 is not a whole number of seconds from 1 to 999999999",
        ),
        (
            "5,60",
            "1h",
            "1h is not a whole number of seconds from 1 to 999999999",
        ),
        (
            "5,60,720",
            "3600",
            "1 ranges for 3 levels: give one for each level after the first",
        ),
        (
            "5,60",
            "",
            "0 ranges for 2 levels: give one for each level after the first",
        ),
        (
            "5,60,720",
            "86400,3600",
            "86400 then 3600: the ranges must increase",
        ),
        (
            "5,60,720",
            "3600,3600",
            "3600 then 3600: the ranges must increase",
        ),
        ("5,7200", "3600", "3600 is shorter than its level 7200"),
    ] {
        let (code, _, err) = script_levels(Some(levels), Some(ranges), None);
        assert_eq!(
            code,
            Some(2),
            "{levels:?} {ranges:?}: the script must refuse: {err}"
        );
        assert_eq!(
            err.trim_end(),
            format!("schema.sh: PULSUS_ROLLUP_RANGES: {message}"),
            "{levels:?} {ranges:?}: the script's message"
        );
        let parsed = pulsus_schema::parse_rollup_levels(levels, 6).expect("valid levels");
        assert_eq!(
            pulsus_schema::parse_rollup_ranges(ranges, &parsed),
            Err(message.to_string()),
            "{levels:?} {ranges:?}: the crate's message"
        );
    }
    // The levels `60` with the default ranges: four for one level.
    let (code, _, err) = script_levels(Some("60"), None, None);
    let message = "4 ranges for 1 levels: give one for each level after the first";
    assert_eq!(code, Some(2), "60 with the default ranges: {err}");
    assert_eq!(
        err.trim_end(),
        format!("schema.sh: PULSUS_ROLLUP_RANGES: {message}")
    );
    assert_eq!(
        pulsus_schema::parse_rollup_ranges("3600,86400,604800,7776000", &[60]),
        Err(message.to_string())
    );
    assert_eq!(
        pulsus_schema::DEFAULT_ROLLUP_RANGES,
        &[3600, 86400, 604800, 7776000],
        "the default ranges"
    );

    for (buckets, message) in [
        ("1", "1 is not a whole number from 2 to 1000"),
        ("1001", "1001 is not a whole number from 2 to 1000"),
        ("05", "05 is not a whole number from 2 to 1000"),
        ("x", "x is not a whole number from 2 to 1000"),
    ] {
        let (code, _, err) = script_levels(None, None, Some(buckets));
        assert_eq!(code, Some(2), "{buckets:?}: the script must refuse: {err}");
        assert_eq!(
            err.trim_end(),
            format!("schema.sh: PULSUS_ROLLUP_COUNTER_BUCKETS: {message}"),
            "{buckets:?}: the script's message"
        );
        assert_eq!(
            pulsus_schema::parse_rollup_counter_buckets(buckets),
            Err(message.to_string()),
            "{buckets:?}: the crate's message"
        );
    }

    // Accepted.
    let (code, out, err) = script_levels(Some("5,60"), Some("999999999"), None);
    assert_eq!(code, Some(0), "999999999: {err}");
    assert!(
        out.contains(
            "COMMENT 'rollup_res_ms=60000 rollup_from_ms=999999999000 counter_buckets=5';"
        ),
        "999999999 is stored"
    );
    assert_eq!(
        pulsus_schema::parse_rollup_ranges("999999999", &[5, 60]),
        Ok(vec![999_999_999])
    );
    for (buckets, want) in [("2", 2u32), ("1000", 1000)] {
        let (code, out, err) = script_levels(None, None, Some(buckets));
        assert_eq!(code, Some(0), "{buckets}: {err}");
        assert!(
            out.contains(&format!(
                "COMMENT 'rollup_res_ms=5000 rollup_from_ms=0 counter_buckets={want}';"
            )),
            "{buckets} is stored"
        );
        assert_eq!(
            pulsus_schema::parse_rollup_counter_buckets(buckets),
            Ok(want)
        );
    }
    // One level and an empty range list.
    let (code, out, err) = script_levels(Some("60"), Some(""), None);
    assert_eq!(code, Some(0), "one level, no ranges: {err}");
    assert!(
        out.contains("COMMENT 'rollup_res_ms=60000 rollup_from_ms=0 counter_buckets=5';"),
        "one level, no ranges: {out}"
    );
    assert_eq!(pulsus_schema::parse_rollup_ranges("", &[60]), Ok(vec![]));
    // No levels: the ranges are not read.
    let (code, out, err) = script_levels(Some(""), Some("1,2"), None);
    assert_eq!(code, Some(0), "no levels: {err}");
    assert!(!out.contains("rollup_res_ms="), "no level objects");
    assert_eq!(pulsus_schema::parse_rollup_ranges("1,2", &[]), Ok(vec![]));
}
