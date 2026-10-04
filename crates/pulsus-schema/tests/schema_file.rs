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
//! - a statement ends at a line ending in `;` and nowhere else, which is
//!   what lets the script send one statement per request;
//! - the script and this crate render the identical text, so the two
//!   renderers cannot drift.

use std::process::Command;
use std::time::Duration;

use pulsus_schema::{RenderCtx, SCHEMA_SQL, rendered, rendered_statements};

fn single() -> RenderCtx {
    RenderCtx::for_tests("pulsus")
}

fn clustered() -> RenderCtx {
    RenderCtx {
        cluster: Some("prod".to_string()),
        ..RenderCtx::for_tests("pulsus")
    }
}

/// Statements beginning with `what`, in the rendered output.
fn starting_with(stmts: &[String], what: &str) -> usize {
    stmts.iter().filter(|s| s.trim_start().starts_with(what)).count()
}

/// The repository root, from this test binary's own crate directory.
fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the crate directory resolves")
}

/// The inventory, single-node: one database, 23 tables, 18 views each
/// dropped before it is created. No `Replicated*` engine, no `Distributed`
/// wrapper, no `ON CLUSTER`.
#[test]
fn the_single_node_render_is_the_whole_inventory_with_every_token_resolved() {
    let stmts = rendered_statements(&single());
    assert_eq!(stmts.len(), 60, "statement count");
    assert_eq!(starting_with(&stmts, "CREATE DATABASE"), 1);
    assert_eq!(starting_with(&stmts, "CREATE TABLE"), 23);
    assert_eq!(starting_with(&stmts, "CREATE MATERIALIZED VIEW"), 18);
    assert_eq!(starting_with(&stmts, "DROP VIEW"), 18);

    let text = rendered(&single());
    assert!(!text.contains("{{"), "an unrendered token reached the output");
    assert!(!text.contains("Replicated"), "single-node renders no replicated engine");
    assert!(!text.contains("Distributed"), "single-node renders no routing wrapper");
    assert!(!text.contains("ON CLUSTER"), "single-node renders no ON CLUSTER");
    assert!(!text.contains("{shard}") && !text.contains("{replica}"));
    // The deduplication-window setting follows the engine: the replicated
    // name applies to a `Replicated*` table only.
    assert!(
        !text.contains(" replicated_deduplication_window"),
        "single-node must not carry the replicated engines' setting name"
    );
    assert!(text.contains("non_replicated_deduplication_window = 10000"));
}

/// The inventory, clustered: the same statements plus 15 `_dist` wrappers,
/// `ON CLUSTER` on every one, and the macros intact.
#[test]
fn the_clustered_render_adds_the_wrappers_and_keeps_the_server_macros() {
    let stmts = rendered_statements(&clustered());
    assert_eq!(stmts.len(), 75, "statement count");
    assert_eq!(starting_with(&stmts, "CREATE TABLE"), 38, "23 tables + 15 wrappers");
    assert_eq!(starting_with(&stmts, "CREATE MATERIALIZED VIEW"), 18);

    let text = rendered(&clustered());
    assert!(!text.contains("{{"), "an unrendered token reached the output");
    assert_eq!(
        text.matches("ON CLUSTER 'prod'").count(),
        75,
        "every statement carries ON CLUSTER"
    );

    // `{shard}` and `{replica}` are the server's own macros and must arrive
    // literally. 18 tables take a per-shard replica set and 5 take the
    // cluster-wide one, so 23 paths name `{replica}` and 18 name `{shard}`.
    assert_eq!(text.matches("{shard}").count(), 18, "per-shard replica sets");
    assert_eq!(text.matches("{replica}").count(), 23, "replicated tables");
    assert_eq!(
        text.matches("/clickhouse/tables/all/").count(),
        5,
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
    for line in SCHEMA_SQL.lines() {
        let bare = line.trim_start_matches("--@cluster ").trim_start();
        if let Some(rest) = bare.strip_prefix("CREATE TABLE IF NOT EXISTS {{db}}.") {
            table = rest
                .split(['{', ' ', '('])
                .next()
                .expect("a name follows the database")
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
    assert_eq!(checked, 23, "every replicated table's path was checked");
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

/// Every materialized view is dropped before it is created: `CREATE
/// MATERIALIZED VIEW` carries no `IF NOT EXISTS`, so a second run without
/// the drop fails on the first view.
#[test]
fn every_view_is_dropped_before_it_is_created() {
    for ctx in [single(), clustered()] {
        let stmts = rendered_statements(&ctx);
        let mut dropped = Vec::new();
        for s in &stmts {
            if let Some(rest) = s.trim().strip_prefix("DROP VIEW IF EXISTS ") {
                dropped.push(rest.split([' ', ';']).next().unwrap().to_string());
            }
            if let Some(rest) = s.trim().strip_prefix("CREATE MATERIALIZED VIEW ") {
                let name = rest.split([' ', '\n']).next().unwrap().to_string();
                assert!(
                    dropped.contains(&name),
                    "{name} is created without being dropped first"
                );
            }
        }
        assert_eq!(dropped.len(), 18, "every view is dropped");
    }
}

/// A family's routing wrappers all shard on one expression: a series'
/// rollups must land on the shard its samples do.
#[test]
fn every_wrapper_in_a_family_shards_on_the_same_expression() {
    let text = rendered(&clustered());
    let mut by_family: std::collections::BTreeMap<&str, std::collections::BTreeSet<String>> =
        Default::default();
    for s in rendered_statements(&clustered()) {
        let Some(at) = s.find("ENGINE = Distributed(") else {
            continue;
        };
        let args = &s[at..];
        let expr = args
            .rsplit_once(", ")
            .expect("a sharding expression ends the argument list")
            .1
            .trim_end_matches([')', ';', '\n'])
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
        let is_table = s.trim_start().starts_with("CREATE TABLE")
            && !s.contains("ENGINE = Distributed(");
        if is_table {
            assert!(has, "a table without the configured storage policy: {s}");
            on_tables += 1;
        } else {
            assert!(!has, "the storage policy reached a wrapper or a view: {s}");
        }
    }
    assert_eq!(on_tables, 23);

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
        dist_suffix: "_routed".to_string(),
        ..clustered()
    };
    let text = rendered(&ctx);
    assert!(text.contains("(31 * 86400)"), "retention days");
    assert!(text.contains("pulsus.log_metrics_2m"), "rollup suffix names a table");
    assert!(text.contains("120000000000"), "rollup resolution in nanoseconds");
    assert!(text.contains("(11 * 3600)"), "metrics landing retention");
    assert!(text.contains("(12 * 3600)"), "logs landing retention");
    assert!(text.contains("(13 * 3600)"), "traces landing retention");
    assert!(text.contains("replicated_deduplication_window = 21"));
    assert!(text.contains("replicated_deduplication_window = 22"));
    assert!(text.contains("replicated_deduplication_window = 23"));
    assert!(text.contains("pulsus.spans_routed"), "the configured suffix");
    assert!(!text.contains("_dist"), "the default suffix is not baked in");
}

/// **The script and this crate render the identical text.** Two renderers
/// read one file; this is what stops them diverging.
#[test]
fn the_script_renders_exactly_what_this_crate_renders() {
    let root = repo_root();
    let script = root.join("schema/schema.sh");

    let cases: Vec<(&str, RenderCtx)> = vec![
        ("default single-node", single()),
        ("default clustered", clustered()),
        (
            "every value moved",
            RenderCtx {
                db: "other_db".to_string(),
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
        cmd.env("PULSUS_METRICS_DEDUP_WINDOW", ctx.metrics_dedup_window.to_string());
        cmd.env("PULSUS_LOG_DEDUP_WINDOW", ctx.log_dedup_window.to_string());
        cmd.env("PULSUS_TRACE_DEDUP_WINDOW", ctx.trace_dedup_window.to_string());
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

/// The two parts of the file that are read at run time rather than sent:
/// a view's own projection, which the rebuild commands replay a landing
/// window through, and a table's declared column list, which the trace
/// fetch derives its projection from.
#[test]
fn the_file_still_answers_the_two_run_time_questions() {
    let ctx = single();

    for mv in [
        "spans_mv",
        "traces_mv",
        "resources_mv",
        "tag_names_mv",
        "tag_values_mv",
    ] {
        let projection =
            pulsus_schema::mv_projection(mv, &ctx).unwrap_or_else(|| panic!("{mv} is in the file"));
        assert!(projection.starts_with("SELECT"), "{mv}: {projection}");
        assert!(
            projection.contains("WHERE row_kind = "),
            "{mv} must scan the landing table under a row_kind predicate, \
             which is where a replay inserts its window: {projection}"
        );
        assert!(!projection.ends_with(';'));
    }
    for mv in [
        "metric_samples_mv",
        "metric_series_mv",
        "metric_metadata_mv",
        "metric_hist_samples_mv",
    ] {
        let projection =
            pulsus_schema::mv_projection(mv, &ctx).unwrap_or_else(|| panic!("{mv} is in the file"));
        assert!(
            projection.contains("WHERE kind = "),
            "a metrics replay appends its window to this predicate: {projection}"
        );
    }
    assert!(pulsus_schema::mv_projection("not_a_view", &ctx).is_none());

    let spans = pulsus_schema::table_column_names("spans").expect("spans is in the file");
    assert_eq!(spans.first(), Some(&"trace_id"));
    assert_eq!(spans.last(), Some(&"end_ns"));
    assert_eq!(spans.len(), 27);
    let resources = pulsus_schema::table_column_names("resources").expect("resources");
    assert_eq!(
        resources,
        vec![
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
    assert_eq!(traces.first(), Some(&"day"));
    assert!(traces.contains(&"buckets"));
    assert!(pulsus_schema::table_column_names("not_a_table").is_none());

    // An index, a constraint and a projection are not columns.
    let log_samples = pulsus_schema::table_column_names("log_samples").expect("log_samples");
    assert_eq!(
        log_samples,
        vec![
            "service",
            "fingerprint",
            "timestamp_ns",
            "severity",
            "body",
            "structured_metadata",
        ]
    );
    let trace_spans = pulsus_schema::table_column_names("trace_spans").expect("trace_spans");
    assert!(!trace_spans.iter().any(|c| c.starts_with(char::is_uppercase)));
}
