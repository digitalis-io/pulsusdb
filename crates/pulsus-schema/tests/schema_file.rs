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

use pulsus_schema::{
    RenderCtx, SCHEMA_SQL, SchemaMode, ctx_from_vars, rendered, rendered_statements,
};

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

/// The inventory, single-node: one database, 30 tables, 27 views, and
/// nothing else. No `Replicated*` engine, no `Distributed` wrapper, no `ON
/// CLUSTER`.
#[test]
fn the_single_node_render_is_the_whole_inventory_with_every_token_resolved() {
    let stmts = rendered_statements(&single());
    assert_eq!(stmts.len(), 58, "statement count");
    assert_eq!(starting_with(&stmts, "CREATE DATABASE"), 1);
    assert_eq!(starting_with(&stmts, "CREATE TABLE"), 30);
    assert_eq!(starting_with(&stmts, "CREATE MATERIALIZED VIEW"), 27);

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

/// The inventory, clustered: the same statements plus 19 `_dist` wrappers,
/// `ON CLUSTER` on every one, and the macros intact.
#[test]
fn the_clustered_render_adds_the_wrappers_and_keeps_the_server_macros() {
    let stmts = rendered_statements(&clustered());
    assert_eq!(stmts.len(), 77, "statement count");
    assert_eq!(
        starting_with(&stmts, "CREATE TABLE"),
        49,
        "30 tables + 19 wrappers"
    );
    assert_eq!(starting_with(&stmts, "CREATE MATERIALIZED VIEW"), 27);

    let text = rendered(&clustered());
    assert!(
        !text.contains("{{"),
        "an unrendered token reached the output"
    );
    assert_eq!(
        text.matches("ON CLUSTER 'prod'").count(),
        77,
        "every statement carries ON CLUSTER"
    );

    // `{shard}` and `{replica}` are the server's own macros and must arrive
    // literally. 24 tables take a per-shard replica set and 6 take the
    // cluster-wide one, so 30 paths name `{replica}` and 24 name `{shard}`.
    // The five default rollup levels add five per-shard replica sets.
    assert_eq!(
        text.matches("{shard}").count(),
        24,
        "per-shard replica sets"
    );
    assert_eq!(text.matches("{replica}").count(), 30, "replicated tables");
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
    assert_eq!(checked, 30, "every replicated table's path was checked");
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
        "metric_samples_5s",
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
    assert_eq!(on_tables, 30);

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
                // Issue #743 part A: the metrics and traces move, the logs
                // at the retention do not.
                cold_volume: Some("cold".to_string()),
                metrics_hot_days: Some(5),
                logs_hot_days: Some(31),
                traces_hot_days: Some(6),
                rollup_retention_days: vec![31, 40, 50],
                // The `60s` level's move age is its retention: it does not move.
                rollup_hot_days: vec![3, 40, 9],
            },
        ),
        (
            "tiering, no family moves",
            RenderCtx {
                storage_policy: Some("tiered".to_string()),
                cold_volume: Some("cold".to_string()),
                metrics_hot_days: Some(7),
                logs_hot_days: Some(7),
                traces_hot_days: Some(7),
                ..single()
            },
        ),
        (
            "level retention only",
            RenderCtx {
                rollup_retention_days: vec![7, 7, 8, 91, 400],
                ..single()
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
        for var in [
            "PULSUS_COLD_VOLUME",
            "PULSUS_METRICS_HOT_DAYS",
            "PULSUS_LOGS_HOT_DAYS",
            "PULSUS_TRACES_HOT_DAYS",
            "PULSUS_ROLLUP_RETENTION_DAYS",
            "PULSUS_ROLLUP_HOT_DAYS",
        ] {
            cmd.env_remove(var);
        }
        // Issue #743 part B: each level list, when the case has one.
        for (var, list) in [
            ("PULSUS_ROLLUP_RETENTION_DAYS", &ctx.rollup_retention_days),
            ("PULSUS_ROLLUP_HOT_DAYS", &ctx.rollup_hot_days),
        ] {
            if !list.is_empty() {
                let text: Vec<String> = list.iter().map(u32::to_string).collect();
                cmd.env(var, text.join(","));
            }
        }
        if let Some(v) = &ctx.cold_volume {
            cmd.env("PULSUS_COLD_VOLUME", v);
        }
        for (var, days) in [
            ("PULSUS_METRICS_HOT_DAYS", ctx.metrics_hot_days),
            ("PULSUS_LOGS_HOT_DAYS", ctx.logs_hot_days),
            ("PULSUS_TRACES_HOT_DAYS", ctx.traces_hot_days),
        ] {
            if let Some(days) = days {
                cmd.env(var, days.to_string());
            }
        }

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
        assert_eq!(targets.len(), 27, "{variant:?}: twenty-seven views");
        refreshable.sort();
        let strings = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            refreshable,
            vec![
                (
                    "metric_samples_5s_mv".to_string(),
                    "metric_samples_5s".to_string(),
                    strings(&["metric_landing", "metric_samples_5s"]),
                    Vec::<String>::new(),
                ),
                (
                    "metric_samples_60s_mv".to_string(),
                    "metric_samples_60s".to_string(),
                    strings(&["metric_samples_5s", "metric_samples_60s"]),
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
            "{variant:?}: the six refreshable views, their targets, their direct \
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
    // Issue #18: the rollup levels, one row each, due at the end of the
    // UTC day of `t_end − 1 ms` plus the retention.
    (
        "metric_samples_5s",
        Some(
            "TTL toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "metric_samples_60s",
        Some(
            "TTL toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "metric_samples_720s",
        Some(
            "TTL toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "metric_samples_7200s",
        Some(
            "TTL toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + (17 * 86400), 4294967295))",
        ),
        true,
        None,
    ),
    (
        "metric_samples_86400s",
        Some(
            "TTL toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + (17 * 86400), 4294967295))",
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
                    "TTL toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + (7 * 86400), 4294967295))".to_string(),
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
    // Each accepted list with one switch point for each level after the
    // first (issue #18 follow-up).
    for (levels, ranges, want) in [
        (
            "60,720,7200,86400",
            "86400,604800,7776000",
            vec![60, 720, 7200, 86400],
        ),
        ("5,60", "3600", vec![5, 60]),
        ("720,86400", "7776000", vec![720, 86400]),
        ("", "", vec![]),
    ] {
        let (code, _, err) = script_levels(Some(levels), Some(ranges), None);
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

/// `schema.sh --print` with an empty environment but for `PATH`, the given
/// variables and arguments: its exit code, standard output as the bytes it
/// wrote, and standard error.
fn script_clean(vars: &[(&str, &str)], args: &[&str]) -> (Option<i32>, Vec<u8>, String) {
    script_clean_in(vars, args, &repo_root())
}

/// [`script_clean`], run from the directory `cwd` (issue #746 part A: a
/// value the script left open to pathname expansion read the directory it
/// ran in).
fn script_clean_in(
    vars: &[(&str, &str)],
    args: &[&str],
    cwd: &std::path::Path,
) -> (Option<i32>, Vec<u8>, String) {
    let root = repo_root();
    let mut cmd = Command::new("sh");
    cmd.arg(root.join("schema/schema.sh"))
        .args(args)
        .arg("--print")
        .current_dir(cwd)
        .env_clear()
        .env("PATH", std::env::var("PATH").expect("PATH is set"));
    for (var, value) in vars {
        cmd.env(var, value);
    }
    let out = cmd.output().expect("run schema.sh");
    (
        out.status.code(),
        out.stdout,
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// **A0 (issue #743 part A): with no cold volume the schema renders as it
/// did before tiering.** The script's raw output is pinned by its digest,
/// and the crate's render is the same bytes. A later schema change
/// replaces the three digests and says so in its commit.
#[test]
fn with_no_cold_volume_the_schema_renders_as_it_did_before_743() {
    let tiered_ctx = RenderCtx {
        storage_policy: Some("tiered".to_string()),
        ..clustered()
    };
    let cases = [
        (
            "single-node",
            vec![],
            vec!["--mode", "single"],
            single(),
            "5295ea5f4f205e7e42838d845a27741566652cc270e9289d35e3941df9d2b4fc",
        ),
        (
            "clustered",
            vec![("PULSUS_CLUSTER", "prod")],
            vec![],
            clustered(),
            "1a3e9d34d55cc2af05de1acad35a805cf250ff3dbb9a5030c8f44efbd2280964",
        ),
        (
            "clustered with a storage policy",
            vec![
                ("PULSUS_CLUSTER", "prod"),
                ("PULSUS_STORAGE_POLICY", "tiered"),
            ],
            vec![],
            tiered_ctx,
            "aa31710015720cb50e242c7d9b11a2916955c11b3ec48589bd918f778b875cc6",
        ),
    ];
    for (what, extra, args, ctx, digest) in cases {
        let mut vars = vec![("CLICKHOUSE_DB", "pulsus"), ("PULSUS_RETENTION_DAYS", "7")];
        vars.extend(extra);
        let (code, out, err) = script_clean(&vars, &args);
        assert_eq!(code, Some(0), "{what}: the script refused: {err}");
        assert_eq!(sha256_hex(&out), digest, "{what}: the script's bytes");
        assert!(
            rendered(&ctx).as_bytes() == out.as_slice(),
            "{what}: the crate's bytes must equal the script's"
        );
    }
}

/// The variable a `TieringInput` text comes from, in the order the script
/// and the crate check them.
const HOT_DAYS_VARS: [&str; 3] = [
    "PULSUS_METRICS_HOT_DAYS",
    "PULSUS_LOGS_HOT_DAYS",
    "PULSUS_TRACES_HOT_DAYS",
];

/// One set of tiering inputs: the storage policy, the cold volume and the
/// metrics, logs and traces move ages; `None` leaves a variable unset.
type TieringCase<'a> = (Option<&'a str>, Option<&'a str>, [Option<&'a str>; 3]);

/// `schema.sh --mode single --print` with `PULSUS_RETENTION_DAYS=30` and
/// the case's variables, and `validate_tiering` over the same inputs.
fn both_tiering(
    case: TieringCase<'_>,
) -> (
    (Option<i32>, String),
    Result<pulsus_schema::Tiering, String>,
) {
    let (policy, volume, hot) = case;
    let mut vars = vec![("PULSUS_RETENTION_DAYS", "30")];
    if let Some(p) = policy {
        vars.push(("PULSUS_STORAGE_POLICY", p));
    }
    if let Some(v) = volume {
        vars.push(("PULSUS_COLD_VOLUME", v));
    }
    for (var, value) in HOT_DAYS_VARS.iter().zip(hot) {
        if let Some(value) = value {
            vars.push((var, value));
        }
    }
    let (code, _, err) = script_clean(&vars, &["--mode", "single"]);
    let from_crate = pulsus_schema::validate_tiering(&pulsus_schema::TieringInput {
        storage_policy: policy,
        cold_volume: volume,
        retention_days: 30,
        metrics_hot_days: hot[0],
        logs_hot_days: hot[1],
        traces_hot_days: hot[2],
        levels: pulsus_schema::DEFAULT_ROLLUP_LEVELS,
        ranges: pulsus_schema::DEFAULT_ROLLUP_RANGES,
        rollup_retention_days: None,
        rollup_hot_days: None,
    });
    ((code, err), from_crate)
}

/// **A1 (issue #743 part A): the script and the crate refuse the same
/// tiering settings, with the same message, and accept the same.** The
/// overlapping cases pin which rule is reported first.
#[test]
fn the_script_and_the_crate_refuse_the_same_tiering_settings() {
    const MISSING: &str = "is not set; with PULSUS_COLD_VOLUME set each of \
        PULSUS_METRICS_HOT_DAYS, PULSUS_LOGS_HOT_DAYS and PULSUS_TRACES_HOT_DAYS is required";
    let two = Some("2");
    let mut cases: Vec<(&str, TieringCase<'_>, String)> = vec![
        (
            "rule 1",
            (None, Some("cold"), [two, two, two]),
            "PULSUS_COLD_VOLUME is set and PULSUS_STORAGE_POLICY is not".to_string(),
        ),
        (
            "rule 2",
            (Some("p"), Some("co-ld"), [two, two, two]),
            "cold_volume must be letters, digits and underscores: co-ld".to_string(),
        ),
        (
            "3a",
            (Some("p"), Some("cold"), [None, two, two]),
            format!("PULSUS_METRICS_HOT_DAYS {MISSING}"),
        ),
        (
            "4a",
            (Some("p"), Some("cold"), [two, None, two]),
            format!("PULSUS_LOGS_HOT_DAYS {MISSING}"),
        ),
        (
            "5a",
            (Some("p"), Some("cold"), [two, two, None]),
            format!("PULSUS_TRACES_HOT_DAYS {MISSING}"),
        ),
        (
            "3b",
            (None, None, [two, None, None]),
            "PULSUS_METRICS_HOT_DAYS is set and PULSUS_COLD_VOLUME is not".to_string(),
        ),
        (
            "4b",
            (Some("p"), None, [None, two, None]),
            "PULSUS_LOGS_HOT_DAYS is set and PULSUS_COLD_VOLUME is not".to_string(),
        ),
        (
            "5b",
            (None, None, [None, None, two]),
            "PULSUS_TRACES_HOT_DAYS is set and PULSUS_COLD_VOLUME is not".to_string(),
        ),
        (
            "4c",
            (Some("p"), Some("cold"), [two, Some("0"), two]),
            "PULSUS_LOGS_HOT_DAYS must be a whole number of days from 1 to 30: 0".to_string(),
        ),
        (
            "5c",
            (Some("p"), Some("cold"), [two, two, Some("31")]),
            "PULSUS_TRACES_HOT_DAYS must be a whole number of days from 1 to 30: 31".to_string(),
        ),
        // The overlapping cases: each breaks two rules, and the message is
        // the first rule's.
        (
            "(i) rule 1 before rule 2",
            (None, Some("co-ld"), [None, None, None]),
            "PULSUS_COLD_VOLUME is set and PULSUS_STORAGE_POLICY is not".to_string(),
        ),
        (
            "(ii) rule 2 before 3a",
            (Some("p"), Some("co-ld"), [None, None, None]),
            "cold_volume must be letters, digits and underscores: co-ld".to_string(),
        ),
        (
            "(iii) 3a before 4c",
            (Some("p"), Some("cold"), [None, Some("0"), None]),
            format!("PULSUS_METRICS_HOT_DAYS {MISSING}"),
        ),
        (
            "(iv) 3c before 4a",
            (Some("p"), Some("cold"), [Some("31"), None, None]),
            "PULSUS_METRICS_HOT_DAYS must be a whole number of days from 1 to 30: 31".to_string(),
        ),
        (
            "(v) 3b before 3c",
            (None, None, [Some("0"), Some("0"), None]),
            "PULSUS_METRICS_HOT_DAYS is set and PULSUS_COLD_VOLUME is not".to_string(),
        ),
    ];
    for bad in ["0", "31", "030", "abc", "12345678901"] {
        cases.push((
            "3c",
            (Some("p"), Some("cold"), [Some(bad), two, two]),
            format!("PULSUS_METRICS_HOT_DAYS must be a whole number of days from 1 to 30: {bad}"),
        ));
    }
    for (what, case, message) in cases {
        let ((code, err), from_crate) = both_tiering(case);
        assert_eq!(
            (code, err.trim_end()),
            (Some(2), format!("schema.sh: {message}").as_str()),
            "{what} {case:?}: the script must refuse with the rule's message"
        );
        assert_eq!(
            from_crate,
            Err(message.clone()),
            "{what} {case:?}: the crate must refuse with the same message"
        );
    }

    let ((code, err), from_crate) =
        both_tiering((Some("p"), Some("cold"), [Some("1"), Some("30"), two]));
    assert_eq!(code, Some(0), "every age from 1 to the retention: {err}");
    assert_eq!(
        from_crate,
        Ok(pulsus_schema::Tiering {
            cold_volume: Some("cold".to_string()),
            metrics_hot_days: Some(1),
            logs_hot_days: Some(30),
            traces_hot_days: Some(2),
            rollup_retention_days: Vec::new(),
            rollup_hot_days: Vec::new(),
        })
    );
    let ((code, err), from_crate) = both_tiering((None, None, [None, None, None]));
    assert_eq!(code, Some(0), "no tiering variable: {err}");
    assert_eq!(
        from_crate,
        Ok(pulsus_schema::Tiering {
            cold_volume: None,
            metrics_hot_days: None,
            logs_hot_days: None,
            traces_hot_days: None,
            rollup_retention_days: Vec::new(),
            rollup_hot_days: Vec::new(),
        })
    );
}

/// The tables of the default render, by name, without the routing
/// wrappers: each `CREATE TABLE` statement that is not `Distributed`.
fn local_tables(ctx: &RenderCtx) -> std::collections::BTreeMap<String, String> {
    let head = format!("CREATE TABLE IF NOT EXISTS {}.", ctx.db);
    let mut out = std::collections::BTreeMap::new();
    for stmt in rendered_statements(ctx) {
        let Some(rest) = stmt.strip_prefix(&head) else {
            continue;
        };
        if stmt.contains("ENGINE = Distributed") {
            continue;
        }
        let name = rest
            .split(|c: char| c.is_whitespace() || c == '(')
            .next()
            .expect("a table name")
            .to_string();
        assert!(
            out.insert(name.clone(), stmt.clone()).is_none(),
            "{name} is created twice"
        );
    }
    out
}

const METRICS_FAMILY: [&str; 3] = ["metric_samples", "metric_hist_samples", "metric_exemplars"];
const LOGS_FAMILY: [&str; 3] = ["log_samples", "log_metrics_5s", "log_patterns"];
const TRACES_FAMILY: [&str; 5] = [
    "spans",
    "error_spans",
    "tag_bucket_values",
    "service_bucket_resources",
    "service_edges",
];
/// The fourteen tables that never move (docs/configuration.md §7).
const NEVER_MOVE: [&str; 14] = [
    "log_landing",
    "log_streams",
    "log_streams_idx",
    "metric_labels",
    "metric_label_index",
    "metric_label_values",
    "metric_landing",
    "metric_metadata",
    "metric_series",
    "resources",
    "tag_names",
    "tag_values",
    "trace_landing",
    "traces",
];

/// The move line issue #743 part A's plan (§3.4) gives `table`, at `days`.
fn move_line(table: &str, days: u32) -> String {
    let time = match table {
        "metric_samples" | "metric_hist_samples" | "metric_exemplars" => "intDiv(unix_milli, 1000)",
        "log_samples" => "intDiv(timestamp_ns, 1000000000)",
        "log_metrics_5s" | "log_patterns" => "intDiv(bucket_ns, 1000000000)",
        "spans" | "error_spans" => "intDiv(start_ns, 1000000000)",
        "tag_bucket_values" | "service_bucket_resources" => {
            "intDiv(bucket * 300000000000, 1000000000)"
        }
        "service_edges" => "toUInt32(minute) - if(conn_type = '', 240, 0)",
        other => panic!("{other} has no move line"),
    };
    format!(", toDateTime(least({time} + ({days} * 86400), 4294967295)) TO VOLUME 'cold'")
}

/// Per local table: its ` TO VOLUME 'cold'` count, and whether it carries
/// `storage_policy = 'tiered'`.
fn assert_tiering(what: &str, ctx: &RenderCtx, moving: &[(&str, u32)]) {
    let tables = local_tables(ctx);
    assert_eq!(tables.len(), 30, "{what}: thirty tables");
    for name in NEVER_MOVE {
        assert!(tables.contains_key(name), "{what}: {name} is rendered");
    }
    for (name, stmt) in &tables {
        let moves = stmt.matches(" TO VOLUME 'cold'").count();
        match moving.iter().find(|(t, _)| t == name) {
            Some((_, days)) => {
                assert_eq!(moves, 1, "{what}: {name} carries one move clause");
                let want = move_line(name, *days);
                assert!(
                    stmt.lines().any(|l| l == want),
                    "{what}: {name} carries `{want}`:\n{stmt}"
                );
            }
            None => assert_eq!(moves, 0, "{what}: {name} carries no move clause"),
        }
        let policy = stmt.contains("storage_policy = 'tiered'");
        let never = NEVER_MOVE.contains(&name.as_str());
        match (ctx.cold_volume.is_some(), never) {
            (true, true) => assert!(
                !stmt.contains("storage_policy"),
                "{what}: {name} never moves and is on the server's default policy:\n{stmt}"
            ),
            _ => assert!(policy, "{what}: {name} is on the policy:\n{stmt}"),
        }
    }
}

/// **A2 (issue #743 part A): the move clauses render only for a family
/// that moves**, in both modes; the tables that never move lose the
/// policy, and without a cold volume nothing changes.
#[test]
fn the_move_clauses_render_only_for_a_family_that_moves() {
    for (mode, base) in [("single-node", single()), ("clustered", clustered())] {
        let tiered = RenderCtx {
            retention_days: 30,
            storage_policy: Some("tiered".to_string()),
            cold_volume: Some("cold".to_string()),
            metrics_hot_days: Some(2),
            logs_hot_days: Some(3),
            traces_hot_days: Some(30),
            ..base.clone()
        };
        let mut moving: Vec<(&str, u32)> = METRICS_FAMILY.iter().map(|t| (*t, 2)).collect();
        moving.extend(LOGS_FAMILY.iter().map(|t| (*t, 3)));
        assert_tiering(&format!("{mode}, with the volume"), &tiered, &moving);

        let untiered = RenderCtx {
            cold_volume: None,
            metrics_hot_days: None,
            logs_hot_days: None,
            traces_hot_days: None,
            ..tiered
        };
        assert_tiering(&format!("{mode}, without the volume"), &untiered, &[]);
    }
}

/// **A2b (issue #743 part A): every trace table carries its move clause.**
#[test]
fn every_trace_table_carries_its_move_clause() {
    for (mode, base) in [("single-node", single()), ("clustered", clustered())] {
        let ctx = RenderCtx {
            retention_days: 30,
            storage_policy: Some("tiered".to_string()),
            cold_volume: Some("cold".to_string()),
            metrics_hot_days: Some(30),
            logs_hot_days: Some(30),
            traces_hot_days: Some(3),
            ..base
        };
        let moving: Vec<(&str, u32)> = TRACES_FAMILY.iter().map(|t| (*t, 3)).collect();
        assert_tiering(mode, &ctx, &moving);
    }
}

/// The tiering variables of one B1 case (issue #743 part B): none, part
/// A's set (`PULSUS_STORAGE_POLICY=p`, `PULSUS_COLD_VOLUME=cold`, the three
/// family move ages `2`), or that set without the metrics move age.
#[derive(Debug, Clone, Copy)]
enum LevelTiering {
    Unset,
    Set,
    SetWithoutMetrics,
}

/// `schema.sh --mode single --print` with `PULSUS_RETENTION_DAYS=2`,
/// `levels`, `ranges`, the tiering variables and each level list that is
/// `Some` (an empty text set as the empty string), and `validate_tiering`
/// over the same inputs, each list text passed unchanged.
fn both_levels(
    levels: &str,
    ranges: &str,
    tiering: LevelTiering,
    retention: Option<&str>,
    hot: Option<&str>,
) -> (
    (Option<i32>, String),
    Result<pulsus_schema::Tiering, String>,
) {
    let (policy, volume, ages) = match tiering {
        LevelTiering::Unset => (None, None, [None; 3]),
        LevelTiering::Set => (Some("p"), Some("cold"), [Some("2"); 3]),
        LevelTiering::SetWithoutMetrics => (Some("p"), Some("cold"), [None, Some("2"), Some("2")]),
    };
    let mut vars = vec![
        ("PULSUS_RETENTION_DAYS", "2"),
        ("PULSUS_ROLLUP_LEVELS", levels),
        ("PULSUS_ROLLUP_RANGES", ranges),
    ];
    if let Some(p) = policy {
        vars.push(("PULSUS_STORAGE_POLICY", p));
    }
    if let Some(v) = volume {
        vars.push(("PULSUS_COLD_VOLUME", v));
    }
    for (var, value) in HOT_DAYS_VARS.iter().zip(ages) {
        if let Some(value) = value {
            vars.push((var, value));
        }
    }
    if let Some(text) = retention {
        vars.push(("PULSUS_ROLLUP_RETENTION_DAYS", text));
    }
    if let Some(text) = hot {
        vars.push(("PULSUS_ROLLUP_HOT_DAYS", text));
    }
    let (code, _, err) = script_clean(&vars, &["--mode", "single"]);
    let level_list = pulsus_schema::parse_rollup_levels(levels, 6).expect("the levels");
    let range_list =
        pulsus_schema::parse_rollup_ranges(ranges, &level_list).expect("the switch points");
    let from_crate = pulsus_schema::validate_tiering(&pulsus_schema::TieringInput {
        storage_policy: policy,
        cold_volume: volume,
        retention_days: 2,
        metrics_hot_days: ages[0],
        logs_hot_days: ages[1],
        traces_hot_days: ages[2],
        levels: &level_list,
        ranges: &range_list,
        rollup_retention_days: retention,
        rollup_hot_days: hot,
    });
    ((code, err), from_crate)
}

/// **B1 (issue #743 part B): the script and the crate refuse the same level
/// day lists, with the same message, and accept the same.** Levels `60,
/// 720, 7200` with switch points `86400, 604800`, and `R` = 2. A list
/// variable that is set is checked, empty included; rule 4 is checked
/// before rule 5, 4 c for every level before 4 d, and 5 a before 5 b.
#[test]
fn the_script_and_the_crate_refuse_the_same_level_day_lists() {
    use LevelTiering::{Set, SetWithoutMetrics, Unset};
    const KEEP: &str = "PULSUS_ROLLUP_RETENTION_DAYS: ";
    const HOT: &str = "PULSUS_ROLLUP_HOT_DAYS: ";
    let not_kept = |item: &str, level: u32| {
        format!(
            "{KEEP}{item} for the {level}s level is not a whole number of days from 2 to 4294967295"
        )
    };
    let too_short = format!(
        "{KEEP}7 for the 720s level is too short: the next level's switch point, 604800 s, \
         requires 8 retention days"
    );
    let no_volume = "PULSUS_ROLLUP_HOT_DAYS is set and PULSUS_COLD_VOLUME is not".to_string();
    let missing = "PULSUS_METRICS_HOT_DAYS is not set; with PULSUS_COLD_VOLUME set each of \
        PULSUS_METRICS_HOT_DAYS, PULSUS_LOGS_HOT_DAYS and PULSUS_TRACES_HOT_DAYS is required"
        .to_string();
    let levels = "60,720,7200";
    let ranges = "86400,604800";
    type Case<'a> = (
        &'a str,
        &'a str,
        &'a str,
        LevelTiering,
        Option<&'a str>,
        Option<&'a str>,
        String,
    );
    let refused: Vec<Case<'_>> = vec![
        (
            "r1",
            levels,
            ranges,
            Unset,
            Some("2,,30"),
            None,
            format!("{KEEP}an empty item in '2,,30'"),
        ),
        (
            "r2",
            levels,
            ranges,
            Unset,
            Some("2,8,"),
            None,
            format!("{KEEP}an empty item in '2,8,'"),
        ),
        (
            "r3",
            levels,
            ranges,
            Unset,
            Some(","),
            None,
            format!("{KEEP}an empty item in ','"),
        ),
        (
            "r4",
            levels,
            ranges,
            Unset,
            Some("2,8"),
            None,
            format!("{KEEP}2 items for 3 levels"),
        ),
        (
            "r5",
            levels,
            ranges,
            Unset,
            Some("2,8,30,40"),
            None,
            format!("{KEEP}4 items for 3 levels"),
        ),
        (
            "r6",
            levels,
            ranges,
            Unset,
            Some("2,1,30"),
            None,
            not_kept("1", 720),
        ),
        (
            "r7",
            levels,
            ranges,
            Unset,
            Some("02,8,30"),
            None,
            not_kept("02", 60),
        ),
        (
            "r8",
            levels,
            ranges,
            Unset,
            Some("2,8,x"),
            None,
            not_kept("x", 7200),
        ),
        (
            "r9",
            levels,
            ranges,
            Unset,
            Some("2,8,4294967296"),
            None,
            not_kept("4294967296", 7200),
        ),
        (
            "r10",
            levels,
            ranges,
            Unset,
            Some("2,8,12345678901"),
            None,
            not_kept("12345678901", 7200),
        ),
        (
            "r11",
            levels,
            ranges,
            Unset,
            Some("2,7,30"),
            None,
            too_short.clone(),
        ),
        (
            "r12",
            levels,
            ranges,
            Unset,
            Some("2,7,1"),
            None,
            not_kept("1", 7200),
        ),
        (
            "r13",
            "",
            "",
            Unset,
            Some("30"),
            None,
            format!("{KEEP}1 items for 0 levels"),
        ),
        (
            "r14",
            levels,
            ranges,
            Unset,
            Some(""),
            None,
            format!("{KEEP}an empty item in ''"),
        ),
        (
            "h1",
            levels,
            ranges,
            Unset,
            None,
            Some("1,1,1"),
            no_volume.clone(),
        ),
        (
            "h2",
            levels,
            ranges,
            Unset,
            None,
            Some(",1"),
            no_volume.clone(),
        ),
        (
            "h8",
            levels,
            ranges,
            Unset,
            None,
            Some(""),
            no_volume.clone(),
        ),
        (
            "h9",
            levels,
            ranges,
            Set,
            None,
            Some(""),
            format!("{HOT}an empty item in ''"),
        ),
        (
            "h3",
            levels,
            ranges,
            Set,
            None,
            Some("1,,1"),
            format!("{HOT}an empty item in '1,,1'"),
        ),
        (
            "h4",
            levels,
            ranges,
            Set,
            None,
            Some("1,1"),
            format!("{HOT}2 items for 3 levels"),
        ),
        (
            "h5",
            levels,
            ranges,
            Set,
            None,
            Some("0,1,1"),
            format!("{HOT}0 for the 60s level is not a whole number of days from 1 to 2"),
        ),
        (
            "h6",
            levels,
            ranges,
            Set,
            None,
            Some("1,3,1"),
            format!("{HOT}3 for the 720s level is not a whole number of days from 1 to 2"),
        ),
        (
            "h7",
            levels,
            ranges,
            Set,
            Some("2,8,30"),
            Some("2,9,30"),
            format!("{HOT}9 for the 720s level is not a whole number of days from 1 to 8"),
        ),
        (
            "o1",
            levels,
            ranges,
            Set,
            Some("2,7,30"),
            Some("0,1,1"),
            too_short.clone(),
        ),
        (
            "o2",
            levels,
            ranges,
            SetWithoutMetrics,
            Some("2,7,30"),
            None,
            missing,
        ),
    ];
    for (id, levels, ranges, tiering, retention, hot, message) in refused {
        let ((code, err), from_crate) = both_levels(levels, ranges, tiering, retention, hot);
        assert_eq!(
            (code, err.trim_end()),
            (Some(2), format!("schema.sh: {message}").as_str()),
            "{id}: the script must refuse with the rule's message"
        );
        assert_eq!(
            from_crate,
            Err(message.clone()),
            "{id}: the crate must refuse with the same message"
        );
    }

    let unset = |keep: Vec<u32>| pulsus_schema::Tiering {
        cold_volume: None,
        metrics_hot_days: None,
        logs_hot_days: None,
        traces_hot_days: None,
        rollup_retention_days: keep,
        rollup_hot_days: Vec::new(),
    };
    let accepted = [
        ("a1", Unset, Some("2,8,30"), None, unset(vec![2, 8, 30])),
        (
            "a2",
            Unset,
            Some("2,8,4294967295"),
            None,
            unset(vec![2, 8, 4_294_967_295]),
        ),
        (
            "a3",
            Set,
            Some("2,8,30"),
            Some("1,8,29"),
            pulsus_schema::Tiering {
                cold_volume: Some("cold".to_string()),
                metrics_hot_days: Some(2),
                logs_hot_days: Some(2),
                traces_hot_days: Some(2),
                rollup_retention_days: vec![2, 8, 30],
                rollup_hot_days: vec![1, 8, 29],
            },
        ),
    ];
    for (id, tiering, retention, hot, want) in accepted {
        let ((code, err), from_crate) = both_levels(levels, ranges, tiering, retention, hot);
        assert_eq!(code, Some(0), "{id}: the script must accept: {err}");
        assert_eq!(from_crate, Ok(want), "{id}: the crate must accept");
    }
}

/// The level `TTL` line at `days`, and the level move line at `days`.
fn level_ttl(days: u32) -> String {
    format!(
        "TTL toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + ({days} * 86400), 4294967295))"
    )
}

fn level_move(days: u32) -> String {
    format!(
        ", toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + ({days} * 86400), 4294967295)) TO VOLUME 'cold'"
    )
}

/// The statement's line that starts with `head`.
fn line_starting<'a>(stmt: &'a str, head: &str) -> Option<&'a str> {
    stmt.lines().find(|l| l.starts_with(head))
}

/// **B2 (issue #743 part B): each level keeps its own retention and moves
/// at its own age, and the metric catalogue keeps the longest.** `R` = 7,
/// the default levels kept `30, 30, 90, 365, 730` days and moving at `7, 7,
/// 30, 30, 730`, in both modes; every table outside the levels and the
/// catalogue renders as it does without the two lists.
#[test]
fn each_level_keeps_its_own_retention_and_the_catalogue_the_longest() {
    for (mode, base) in [("single-node", single()), ("clustered", clustered())] {
        let ctx = RenderCtx {
            storage_policy: Some("tiered".to_string()),
            cold_volume: Some("cold".to_string()),
            metrics_hot_days: Some(7),
            logs_hot_days: Some(7),
            traces_hot_days: Some(7),
            rollup_retention_days: vec![30, 30, 90, 365, 730],
            rollup_hot_days: vec![7, 7, 30, 30, 730],
            ..base
        };
        let plain = RenderCtx {
            rollup_retention_days: Vec::new(),
            rollup_hot_days: Vec::new(),
            ..ctx.clone()
        };
        let tables = local_tables(&ctx);
        let plain_tables = local_tables(&plain);
        assert_eq!(
            tables.keys().collect::<Vec<_>>(),
            plain_tables.keys().collect::<Vec<_>>(),
            "{mode}: the same tables"
        );

        let levels = [
            ("metric_samples_5s", 30, Some(7)),
            ("metric_samples_60s", 30, Some(7)),
            ("metric_samples_720s", 90, Some(30)),
            ("metric_samples_7200s", 365, Some(30)),
            ("metric_samples_86400s", 730, None),
        ];
        for (name, keep, moves) in levels {
            let stmt = &tables[name];
            let ttl = level_ttl(keep);
            assert!(
                stmt.lines().any(|l| l == ttl),
                "{mode}: {name} carries `{ttl}`:\n{stmt}"
            );
            let n = stmt.matches(" TO VOLUME").count();
            match moves {
                Some(h) => {
                    assert_eq!(
                        stmt.matches(" TO VOLUME 'cold'").count(),
                        1,
                        "{mode}: {name} carries one move clause:\n{stmt}"
                    );
                    let want = level_move(h);
                    assert!(
                        stmt.lines().any(|l| l == want),
                        "{mode}: {name} carries `{want}`:\n{stmt}"
                    );
                }
                None => assert_eq!(n, 0, "{mode}: {name} carries no move clause:\n{stmt}"),
            }
            assert!(
                stmt.contains("storage_policy = 'tiered'"),
                "{mode}: {name} is on the policy:\n{stmt}"
            );
            assert_eq!(
                line_starting(stmt, "COMMENT"),
                line_starting(&plain_tables[name], "COMMENT"),
                "{mode}: {name}'s comment"
            );
        }

        let catalogue = [
            (
                "metric_labels",
                "TTL toDateTime(least(((intDiv(last_seen, 86400000) + 1) * 86400) + (730 * 86400), 4294967295))",
            ),
            (
                "metric_label_index",
                "TTL toDateTime(least(((intDiv(last_seen, 86400000) + 1) * 86400) + (730 * 86400), 4294967295))",
            ),
            (
                "metric_label_values",
                "TTL toDateTime(least(((intDiv(last_seen, 86400000) + 1) * 86400) + (730 * 86400), 4294967295))",
            ),
            (
                "metric_metadata",
                "TTL toDateTime(least(intDiv(updated_ns, 1000000000) + (730 * 86400), 4294967295))",
            ),
            (
                "metric_series",
                "TTL toDateTime(least((toUInt64(toUInt16(day)) + 1 + 730) * 86400, 4294967295))",
            ),
        ];
        for (name, ttl) in catalogue {
            let stmt = &tables[name];
            assert!(
                stmt.lines().any(|l| l == ttl),
                "{mode}: {name} carries `{ttl}`:\n{stmt}"
            );
            assert_eq!(
                stmt.matches(" TO VOLUME").count(),
                0,
                "{mode}: {name} carries no move clause:\n{stmt}"
            );
        }

        for (name, stmt) in &tables {
            if levels.iter().any(|(t, _, _)| t == name) || catalogue.iter().any(|(t, _)| t == name)
            {
                continue;
            }
            assert_eq!(
                stmt, &plain_tables[name],
                "{mode}: {name} renders as without the lists"
            );
        }
        let raw = &tables["metric_samples"];
        let raw_ttl = "TTL toDateTime(least(intDiv(unix_milli, 1000) + (7 * 86400), 4294967295))";
        assert!(
            raw.lines().any(|l| l == raw_ttl),
            "{mode}: metric_samples carries `{raw_ttl}`:\n{raw}"
        );
        assert_eq!(
            raw.matches(" TO VOLUME").count(),
            0,
            "{mode}: metric_samples does not move:\n{raw}"
        );

        let database = rendered_statements(&ctx)
            .into_iter()
            .find(|s| s.starts_with("CREATE DATABASE"))
            .expect("the database statement");
        assert!(
            database.contains("COMMENT 'retention_days=7'"),
            "{mode}: {database}"
        );
    }
}

// ---------------------------------------------------------------------------
// Issue #746 part A: one set of schema rules, `pulsus_schema::ctx_from_vars`,
// held against the script.

/// The script's arguments for `mode`.
fn mode_args(mode: Option<SchemaMode>) -> Vec<&'static str> {
    match mode {
        Some(SchemaMode::Single) => vec!["--mode", "single"],
        Some(SchemaMode::Cluster) => vec!["--mode", "cluster"],
        None => vec![],
    }
}

/// What the script printed after `schema.sh: `, when it printed one line
/// there and nothing else.
fn script_message(err: &str) -> Option<String> {
    let line = err.strip_suffix('\n')?;
    if line.contains('\n') {
        return None;
    }
    line.strip_prefix("schema.sh: ").map(str::to_string)
}

/// `schema.sh --print` and `ctx_from_vars` over the same variables. `None`
/// when the two agree: the same refusal with the same message, or both
/// accept and render the same bytes. Otherwise what differs.
fn script_and_crate_differ(vars: &[(&str, &str)], mode: Option<SchemaMode>) -> Option<String> {
    script_and_crate_differ_in(vars, mode, &repo_root())
}

/// [`script_and_crate_differ`], the script run from `cwd`.
fn script_and_crate_differ_in(
    vars: &[(&str, &str)],
    mode: Option<SchemaMode>,
    cwd: &std::path::Path,
) -> Option<String> {
    let (code, out, err) = script_clean_in(vars, &mode_args(mode), cwd);
    let map: std::collections::BTreeMap<&str, &str> = vars.iter().copied().collect();
    let from_crate = ctx_from_vars(&map, mode);
    match (code, from_crate) {
        (Some(0), Ok(ctx)) => {
            let from_crate = rendered(&ctx);
            (from_crate.as_bytes() != out.as_slice())
                .then(|| format!("{vars:?} {mode:?}: both accept; the renders differ"))
        }
        (Some(2), Err(msg)) => match script_message(&err) {
            Some(s) if s == msg => None,
            _ => Some(format!(
                "{vars:?} {mode:?}: the script printed {err:?}, the crate {msg:?}"
            )),
        },
        (code, from_crate) => Some(format!(
            "{vars:?} {mode:?}: the script exited {code:?} ({err:?}), the crate returned {:?}",
            from_crate.map(|_| "a context")
        )),
    }
}

/// The literal pieces of a `REFUSALS` template, split at each `<name>`.
fn template_pieces(template: &str) -> Vec<&str> {
    let mut pieces = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while let Some(open) = template[i..].find('<').map(|j| i + j) {
        i = open + 1;
        let Some(close) = template[open + 1..].find('>').map(|j| open + 1 + j) else {
            break;
        };
        let name = &template[open + 1..close];
        if !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            pieces.push(&template[start..open]);
            start = close + 1;
            i = close + 1;
        }
    }
    pieces.push(&template[start..]);
    pieces
}

/// `text` is `template` with each `<name>` standing for any text.
fn matches_template(text: &str, template: &str) -> bool {
    let pieces = template_pieces(template);
    if pieces.len() == 1 {
        return text == template;
    }
    let (first, last) = (pieces[0], pieces[pieces.len() - 1]);
    if text.len() < first.len() + last.len() || !text.starts_with(first) || !text.ends_with(last) {
        return false;
    }
    let mut rest = &text[first.len()..text.len() - last.len()];
    for mid in &pieces[1..pieces.len() - 1] {
        match rest.find(mid) {
            Some(at) => rest = &rest[at + mid.len()..],
            None => return false,
        }
    }
    true
}

/// `REFUSALS` row `n`, numbered from 1 as the documentation numbers it.
fn refusal_row(n: usize) -> &'static str {
    let row = pulsus_schema::REFUSALS.get(n - 1);
    assert!(
        row.is_some(),
        "REFUSALS has no row {n}: {:?}",
        pulsus_schema::REFUSALS
    );
    row.unwrap().0
}

#[test]
fn the_template_matcher_reads_each_placeholder_as_any_text() {
    assert!(matches_template("a 1 b 2 c", "a <x> b <y> c"));
    assert!(matches_template(
        "PULSUS_X: an empty item in ''",
        "<1>: an empty item in '<2>'"
    ));
    assert!(!matches_template("a 1 b 2", "a <x> b <y> c"));
    assert!(!matches_template("x 1 b 2 c", "a <x> b <y> c"));
    assert!(!matches_template("a 1 c", "a <x> b <y> c"));
    assert!(matches_template(
        "2 × 60 + 870 > 6 × 3600",
        "2 × <p> + 870 > <h> × 3600"
    ));
    assert!(!matches_template("abc", "abd"));
}

const MISSING_HOT: &str = "is not set; with PULSUS_COLD_VOLUME set each of \
    PULSUS_METRICS_HOT_DAYS, PULSUS_LOGS_HOT_DAYS and PULSUS_TRACES_HOT_DAYS is required";

const LANDING_VARS: [&str; 3] = [
    "PULSUS_METRICS_LANDING_RETENTION_HOURS",
    "PULSUS_LOG_LANDING_RETENTION_HOURS",
    "PULSUS_TRACE_LANDING_RETENTION_HOURS",
];

const DEDUP_VARS: [&str; 3] = [
    "PULSUS_METRICS_DEDUP_WINDOW",
    "PULSUS_LOG_DEDUP_WINDOW",
    "PULSUS_TRACE_DEDUP_WINDOW",
];

const RESOLUTION_REFUSED: &str = "PULSUS_LOG_ROLLUP_RESOLUTION must be a whole number with no \
    leading zero, then ms (at most 12 digits), s (9), m (8), h (6), d (5) or w (4): ";

/// One A1 case: the `REFUSALS` row it reaches, the variables over
/// `PULSUS_RETENTION_DAYS=30` (`None` unsets one), the mode, and the message.
type RefusalCase = (usize, Vec<(String, Option<String>)>, SchemaMode, String);

fn refusal_case(
    row: usize,
    vars: &[(&str, Option<&str>)],
    mode: SchemaMode,
    message: impl Into<String>,
) -> RefusalCase {
    (
        row,
        vars.iter()
            .map(|(k, v)| (k.to_string(), v.map(str::to_string)))
            .collect(),
        mode,
        message.into(),
    )
}

/// The variables of a case: `PULSUS_RETENTION_DAYS=30`, then each override.
fn case_vars(overrides: &[(String, Option<String>)]) -> Vec<(String, String)> {
    let mut map =
        std::collections::BTreeMap::from([("PULSUS_RETENTION_DAYS".to_string(), "30".to_string())]);
    for (k, v) in overrides {
        match v {
            Some(v) => map.insert(k.clone(), v.clone()),
            None => map.remove(k),
        };
    }
    map.into_iter().collect()
}

/// **A1 (issue #746 part A): the script and `ctx_from_vars` refuse every
/// setting the same way, with the same message.** One case per message the
/// settings can reach, and two-failure cases over every adjacent pair of
/// rule groups and within the landing and dedup groups, which pin the order.
/// Each message is also held to its `REFUSALS` row.
#[test]
fn the_script_and_the_crate_refuse_every_setting_the_same_way() {
    use SchemaMode::{Cluster, Single};
    let s = Some;
    let mut cases: Vec<RefusalCase> = Vec::new();
    let name_vars = [
        "CLICKHOUSE_DB",
        "PULSUS_CLUSTER",
        "PULSUS_DIST_SUFFIX",
        "PULSUS_STORAGE_POLICY",
    ];
    let name_whats = ["database", "cluster", "dist_suffix", "storage_policy"];
    for (var, what) in name_vars.into_iter().zip(name_whats) {
        cases.push(refusal_case(
            3,
            &[(var, s("a-b"))],
            Single,
            format!("{what} must be letters, digits and underscores: a-b"),
        ));
    }
    cases.push(refusal_case(
        4,
        &[("PULSUS_RETENTION_DAYS", None)],
        Single,
        "PULSUS_RETENTION_DAYS is not set; retention has no default (docs/configuration.md §7)",
    ));
    cases.push(refusal_case(
        5,
        &[("PULSUS_RETENTION_DAYS", s("0"))],
        Single,
        "PULSUS_RETENTION_DAYS must be a whole number of days from 1 to 4294967295: 0",
    ));
    for var in LANDING_VARS {
        cases.push(refusal_case(
            6,
            &[(var, s("0"))],
            Single,
            format!("{var} must be a whole number of hours from 1 to 168: 0"),
        ));
    }
    for var in DEDUP_VARS {
        cases.push(refusal_case(
            7,
            &[(var, s("0"))],
            Single,
            format!("{var} must be a whole number of blocks from 1 to 1000000: 0"),
        ));
    }
    let levels = "PULSUS_ROLLUP_LEVELS";
    for (row, text, message) in [
        (8, "60,,720", "an empty item in '60,,720'"),
        (
            9,
            "86401",
            "86401 is not a whole number of seconds from 1 to 86400",
        ),
        (10, "7", "7 does not divide a day (86400)"),
        (11, "45", "45 is below 60 and does not divide 60"),
        (12, "720,60", "720 then 60: the levels must increase"),
        (13, "60,90", "90 is not a multiple of 60"),
        (14, "1,2,4,12,60,120,240,720,86400", "more than 8 levels"),
        (
            15,
            "14400",
            "the first level 14400 outlives metric_landing: 2 × 14400 + 870 > 6 × 3600",
        ),
    ] {
        cases.push(refusal_case(
            row,
            &[(levels, s(text))],
            Single,
            format!("{levels}: {message}"),
        ));
    }
    let ranges = "PULSUS_ROLLUP_RANGES";
    for (row, text, message) in [
        (
            16,
            "3600,,604800,7776000",
            "an empty item in '3600,,604800,7776000'",
        ),
        (
            17,
            "3600,86400,604800,1000000000",
            "1000000000 is not a whole number of seconds from 1 to 999999999",
        ),
        (
            18,
            "3600",
            "1 ranges for 5 levels: give one for each level after the first",
        ),
        (
            19,
            "86400,3600,604800,7776000",
            "86400 then 3600: the ranges must increase",
        ),
        (
            20,
            "59,86400,604800,7776000",
            "59 is shorter than its level 60",
        ),
    ] {
        cases.push(refusal_case(
            row,
            &[(ranges, s(text))],
            Single,
            format!("{ranges}: {message}"),
        ));
    }
    cases.push(refusal_case(
        21,
        &[("PULSUS_ROLLUP_COUNTER_BUCKETS", s("1"))],
        Single,
        "PULSUS_ROLLUP_COUNTER_BUCKETS: 1 is not a whole number from 2 to 1000",
    ));
    cases.push(refusal_case(
        22,
        &[("PULSUS_COLD_VOLUME", s("cold"))],
        Single,
        "PULSUS_COLD_VOLUME is set and PULSUS_STORAGE_POLICY is not",
    ));
    cases.push(refusal_case(
        23,
        &[
            ("PULSUS_STORAGE_POLICY", s("p")),
            ("PULSUS_COLD_VOLUME", s("co-ld")),
        ],
        Single,
        "cold_volume must be letters, digits and underscores: co-ld",
    ));
    let tiered = [
        ("PULSUS_STORAGE_POLICY", s("p")),
        ("PULSUS_COLD_VOLUME", s("cold")),
    ];
    for var in HOT_DAYS_VARS {
        let mut missing: Vec<(&str, Option<&str>)> = tiered.to_vec();
        let mut over: Vec<(&str, Option<&str>)> = tiered.to_vec();
        for other in HOT_DAYS_VARS {
            over.push((other, s(if other == var { "31" } else { "2" })));
            if other != var {
                missing.push((other, s("2")));
            }
        }
        cases.push(refusal_case(
            24,
            &missing,
            Single,
            format!("{var} {MISSING_HOT}"),
        ));
        cases.push(refusal_case(
            25,
            &[(var, s("2"))],
            Single,
            format!("{var} is set and PULSUS_COLD_VOLUME is not"),
        ));
        cases.push(refusal_case(
            26,
            &over,
            Single,
            format!("{var} must be a whole number of days from 1 to 30: 31"),
        ));
    }
    let with_cold: Vec<(&str, Option<&str>)> = tiered
        .iter()
        .copied()
        .chain(HOT_DAYS_VARS.iter().map(|v| (*v, s("2"))))
        .collect();
    let keep = "PULSUS_ROLLUP_RETENTION_DAYS";
    let hot = "PULSUS_ROLLUP_HOT_DAYS";
    cases.push(refusal_case(
        27,
        &[(keep, s(""))],
        Single,
        format!("{keep}: an empty item in ''"),
    ));
    cases.push(refusal_case(
        28,
        &[(keep, s("30"))],
        Single,
        format!("{keep}: 1 items for 5 levels"),
    ));
    let mut hot_empty = with_cold.clone();
    hot_empty.push((hot, s("")));
    cases.push(refusal_case(
        27,
        &hot_empty,
        Single,
        format!("{hot}: an empty item in ''"),
    ));
    let mut hot_one = with_cold.clone();
    hot_one.push((hot, s("1")));
    cases.push(refusal_case(
        28,
        &hot_one,
        Single,
        format!("{hot}: 1 items for 5 levels"),
    ));
    cases.push(refusal_case(
        29,
        &[(keep, s("29,30,30,30,30"))],
        Single,
        format!("{keep}: 29 for the 5s level is not a whole number of days from 30 to 4294967295"),
    ));
    cases.push(refusal_case(
        30,
        &[("PULSUS_RETENTION_DAYS", s("1")), (keep, s("1,1,1,1,1"))],
        Single,
        format!(
            "{keep}: 1 for the 5s level is too short: the next level's switch point, 3600 s, \
             requires 2 retention days"
        ),
    ));
    cases.push(refusal_case(
        31,
        &[(hot, s("1,1,1,1,1"))],
        Single,
        format!("{hot} is set and PULSUS_COLD_VOLUME is not"),
    ));
    let mut hot_over = with_cold.clone();
    hot_over.push((hot, s("31,1,1,1,1")));
    cases.push(refusal_case(
        32,
        &hot_over,
        Single,
        format!("{hot}: 31 for the 5s level is not a whole number of days from 1 to 30"),
    ));
    cases.push(refusal_case(
        33,
        &[],
        Cluster,
        "--mode cluster needs PULSUS_CLUSTER set to the cluster name",
    ));
    cases.push(refusal_case(
        34,
        &[("PULSUS_LOG_ROLLUP_RESOLUTION", s("5ns"))],
        Single,
        format!("{RESOLUTION_REFUSED}5ns"),
    ));
    let attrs = "PULSUS_TRACEQL_INDEXED_ATTRIBUTES";
    let long_key = format!("span.{}", "a".repeat(129));
    let seventeen = (1..=17)
        .map(|i| format!("span.a{i}"))
        .collect::<Vec<_>>()
        .join(",");
    for (row, text, message) in [
        (
            35,
            "span.a,,span.b".to_string(),
            "an empty item in 'span.a,,span.b'".to_string(),
        ),
        (
            36,
            "resource.c".to_string(),
            "resource.c: a resource attribute is read through resource_id, which the span \
             table already indexes"
                .to_string(),
        ),
        (
            37,
            "x.y".to_string(),
            "x.y: the scope must be span. or event.".to_string(),
        ),
        (
            38,
            "span.a..b".to_string(),
            "span.a..b: the key must be dot-separated segments of letters, digits, '_' and '-'"
                .to_string(),
        ),
        (
            39,
            long_key.clone(),
            format!("{long_key}: the key is longer than 128 bytes"),
        ),
        (
            40,
            "span.a,span.a".to_string(),
            "span.a is listed twice".to_string(),
        ),
        (41, seventeen, "names more than 16 attributes".to_string()),
    ] {
        cases.push(refusal_case(
            row,
            &[(attrs, Some(text.as_str()))],
            Single,
            format!("{attrs}: {message}"),
        ));
    }
    // Two failures at once: the earlier rule is the message.
    let [ml, ll, tl] = LANDING_VARS;
    let [md, ld, td] = DEDUP_VARS;
    let two: Vec<RefusalCase> = vec![
        refusal_case(
            3,
            &[("CLICKHOUSE_DB", s("a-b")), ("PULSUS_RETENTION_DAYS", None)],
            Single,
            "database must be letters, digits and underscores: a-b",
        ),
        refusal_case(
            5,
            &[("PULSUS_RETENTION_DAYS", s("0")), (ml, s("0"))],
            Single,
            "PULSUS_RETENTION_DAYS must be a whole number of days from 1 to 4294967295: 0",
        ),
        refusal_case(
            6,
            &[(ml, s("0")), (ll, s("0"))],
            Single,
            format!("{ml} must be a whole number of hours from 1 to 168: 0"),
        ),
        refusal_case(
            6,
            &[(ll, s("0")), (tl, s("0"))],
            Single,
            format!("{ll} must be a whole number of hours from 1 to 168: 0"),
        ),
        refusal_case(
            6,
            &[(tl, s("0")), (md, s("0"))],
            Single,
            format!("{tl} must be a whole number of hours from 1 to 168: 0"),
        ),
        refusal_case(
            7,
            &[(md, s("0")), (ld, s("0"))],
            Single,
            format!("{md} must be a whole number of blocks from 1 to 1000000: 0"),
        ),
        refusal_case(
            7,
            &[(ld, s("0")), (td, s("0"))],
            Single,
            format!("{ld} must be a whole number of blocks from 1 to 1000000: 0"),
        ),
        refusal_case(
            7,
            &[(td, s("0")), (levels, s("7"))],
            Single,
            format!("{td} must be a whole number of blocks from 1 to 1000000: 0"),
        ),
        refusal_case(
            10,
            &[(levels, s("7")), (ranges, s("1000000000"))],
            Single,
            format!("{levels}: 7 does not divide a day (86400)"),
        ),
        refusal_case(
            18,
            &[
                (ranges, s("3600")),
                ("PULSUS_ROLLUP_COUNTER_BUCKETS", s("1")),
            ],
            Single,
            format!("{ranges}: 1 ranges for 5 levels: give one for each level after the first"),
        ),
        refusal_case(
            21,
            &[
                ("PULSUS_ROLLUP_COUNTER_BUCKETS", s("1")),
                ("PULSUS_COLD_VOLUME", s("cold")),
            ],
            Single,
            "PULSUS_ROLLUP_COUNTER_BUCKETS: 1 is not a whole number from 2 to 1000",
        ),
        refusal_case(
            22,
            &[("PULSUS_COLD_VOLUME", s("cold"))],
            Cluster,
            "PULSUS_COLD_VOLUME is set and PULSUS_STORAGE_POLICY is not",
        ),
        refusal_case(
            33,
            &[("PULSUS_LOG_ROLLUP_RESOLUTION", s("5ns"))],
            Cluster,
            "--mode cluster needs PULSUS_CLUSTER set to the cluster name",
        ),
        refusal_case(
            34,
            &[
                ("PULSUS_LOG_ROLLUP_RESOLUTION", s("5ns")),
                (attrs, s("x.y")),
            ],
            Single,
            format!("{RESOLUTION_REFUSED}5ns"),
        ),
    ];
    cases.extend(two);

    for (row, overrides, mode, want) in &cases {
        let vars = case_vars(overrides);
        let vars: Vec<(&str, &str)> = vars.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let args = mode_args(Some(*mode));
        let (code, _, err) = script_clean(&vars, &args);
        assert_eq!(
            code,
            Some(2),
            "{vars:?} {mode:?}: the script's exit ({err})"
        );
        assert_eq!(
            script_message(&err).as_deref(),
            Some(want.as_str()),
            "{vars:?} {mode:?}: the script's message"
        );
        let map: std::collections::BTreeMap<&str, &str> = vars.iter().copied().collect();
        assert_eq!(
            ctx_from_vars(&map, Some(*mode)).map(|_| ()),
            Err(want.clone()),
            "{vars:?} {mode:?}: ctx_from_vars"
        );
        let template = refusal_row(*row);
        assert!(
            matches_template(want, template),
            "{want:?} is not row {row}, {template:?}"
        );
    }
}

/// One A2 case: what it is, the variables, the mode.
type RenderCase = (&'static str, Vec<(&'static str, &'static str)>, SchemaMode);

/// The eight cases of `the_script_renders_exactly_what_this_crate_renders`,
/// as the variables the script reads, and the four A2 adds.
fn a2_cases() -> Vec<RenderCase> {
    use SchemaMode::{Cluster, Single};
    let base = [("CLICKHOUSE_DB", "pulsus"), ("PULSUS_RETENTION_DAYS", "7")];
    // Composed rather than written as a literal, as in
    // `the_script_renders_exactly_what_this_crate_renders`: `live_db_naming`
    // reads every literal given to a database variable.
    let other_db: &'static str = Box::leak(format!("{}_elsewhere", single().db).into_boxed_str());
    let with = |extra: &[(&'static str, &'static str)]| {
        let mut v = base.to_vec();
        v.extend_from_slice(extra);
        v
    };
    vec![
        ("default single-node", with(&[]), Single),
        (
            "default clustered",
            with(&[("PULSUS_CLUSTER", "prod")]),
            Cluster,
        ),
        (
            "every value moved",
            vec![
                ("CLICKHOUSE_DB", other_db),
                ("PULSUS_CLUSTER", "c1"),
                ("PULSUS_DIST_SUFFIX", "_routed"),
                ("PULSUS_STORAGE_POLICY", "hot_cold"),
                ("PULSUS_RETENTION_DAYS", "31"),
                ("PULSUS_LOG_ROLLUP_RESOLUTION", "500ms"),
                ("PULSUS_METRICS_LANDING_RETENTION_HOURS", "11"),
                ("PULSUS_LOG_LANDING_RETENTION_HOURS", "12"),
                ("PULSUS_TRACE_LANDING_RETENTION_HOURS", "13"),
                ("PULSUS_METRICS_DEDUP_WINDOW", "21"),
                ("PULSUS_LOG_DEDUP_WINDOW", "22"),
                ("PULSUS_TRACE_DEDUP_WINDOW", "23"),
                (
                    "PULSUS_TRACEQL_INDEXED_ATTRIBUTES",
                    "span.app.request.id,event.exception.type,span.http-x.a_b",
                ),
                ("PULSUS_ROLLUP_LEVELS", "15,60,720"),
                ("PULSUS_ROLLUP_RANGES", "3600,86400"),
                ("PULSUS_ROLLUP_COUNTER_BUCKETS", "7"),
                ("PULSUS_COLD_VOLUME", "cold"),
                ("PULSUS_METRICS_HOT_DAYS", "5"),
                ("PULSUS_LOGS_HOT_DAYS", "31"),
                ("PULSUS_TRACES_HOT_DAYS", "6"),
                ("PULSUS_ROLLUP_RETENTION_DAYS", "31,40,50"),
                ("PULSUS_ROLLUP_HOT_DAYS", "3,40,9"),
            ],
            Cluster,
        ),
        (
            "tiering, no family moves",
            with(&[
                ("PULSUS_STORAGE_POLICY", "tiered"),
                ("PULSUS_COLD_VOLUME", "cold"),
                ("PULSUS_METRICS_HOT_DAYS", "7"),
                ("PULSUS_LOGS_HOT_DAYS", "7"),
                ("PULSUS_TRACES_HOT_DAYS", "7"),
            ]),
            Single,
        ),
        (
            "level retention only",
            with(&[("PULSUS_ROLLUP_RETENTION_DAYS", "7,7,8,91,400")]),
            Single,
        ),
        ("no levels", with(&[("PULSUS_ROLLUP_LEVELS", "")]), Single),
        (
            "whole minutes",
            with(&[("PULSUS_LOG_ROLLUP_RESOLUTION", "120000ms")]),
            Single,
        ),
        (
            "whole seconds given in milliseconds",
            with(&[("PULSUS_LOG_ROLLUP_RESOLUTION", "2000ms")]),
            Single,
        ),
        (
            "one level",
            with(&[("PULSUS_ROLLUP_LEVELS", "60"), ("PULSUS_ROLLUP_RANGES", "")]),
            Single,
        ),
        (
            "eight levels",
            with(&[
                ("PULSUS_ROLLUP_LEVELS", "5,10,20,60,120,240,720,86400"),
                (
                    "PULSUS_ROLLUP_RANGES",
                    "3600,7200,14400,28800,86400,604800,7776000",
                ),
            ]),
            Single,
        ),
        (
            "no levels, tiering set",
            with(&[
                ("PULSUS_ROLLUP_LEVELS", ""),
                ("PULSUS_STORAGE_POLICY", "p"),
                ("PULSUS_COLD_VOLUME", "cold"),
                ("PULSUS_METRICS_HOT_DAYS", "2"),
                ("PULSUS_LOGS_HOT_DAYS", "2"),
                ("PULSUS_TRACES_HOT_DAYS", "2"),
            ]),
            Single,
        ),
        (
            "an empty resolution",
            with(&[("PULSUS_LOG_ROLLUP_RESOLUTION", "")]),
            Single,
        ),
    ]
}

/// **A2 (issue #746 part A): `ctx_from_vars` renders what the script
/// renders.** The script's `--print` output and `rendered` of the context
/// built from the same variables are the same bytes; an empty resolution is
/// the default; with no `--mode` the mode is the script's default, cluster
/// exactly when `PULSUS_CLUSTER` is non-empty.
#[test]
fn ctx_from_vars_renders_what_the_script_renders() {
    for (what, vars, mode) in a2_cases() {
        let (code, out, err) = script_clean(&vars, &mode_args(Some(mode)));
        assert_eq!(code, Some(0), "{what}: the script refused: {err}");
        let map: std::collections::BTreeMap<&str, &str> = vars.iter().copied().collect();
        let ctx = ctx_from_vars(&map, Some(mode));
        assert!(ctx.is_ok(), "{what}: ctx_from_vars refused: {ctx:?}");
        let from_crate = rendered(&ctx.unwrap());
        assert!(
            from_crate.as_bytes() == out.as_slice(),
            "{what}: the crate's bytes must equal the script's"
        );
    }

    // An empty resolution renders as an unset one, in both.
    let unset = [("CLICKHOUSE_DB", "pulsus"), ("PULSUS_RETENTION_DAYS", "7")];
    let empty = [
        ("CLICKHOUSE_DB", "pulsus"),
        ("PULSUS_RETENTION_DAYS", "7"),
        ("PULSUS_LOG_ROLLUP_RESOLUTION", ""),
    ];
    let single = mode_args(Some(SchemaMode::Single));
    let (_, from_unset, _) = script_clean(&unset, &single);
    let (_, from_empty, _) = script_clean(&empty, &single);
    assert!(from_unset == from_empty, "the script: empty and unset");
    let crate_of = |vars: &[(&str, &str)], mode| {
        let map: std::collections::BTreeMap<&str, &str> = vars.iter().copied().collect();
        ctx_from_vars(&map, mode)
    };
    assert_eq!(
        crate_of(&empty, Some(SchemaMode::Single)),
        crate_of(&unset, Some(SchemaMode::Single)),
        "the crate: empty and unset"
    );
    assert!(
        String::from_utf8_lossy(&from_unset).contains("log_metrics_5s"),
        "the default resolution is 5s"
    );

    // No `--mode`: the script's default.
    for (what, vars, same_as) in [
        (
            "PULSUS_CLUSTER set",
            vec![
                ("CLICKHOUSE_DB", "pulsus"),
                ("PULSUS_RETENTION_DAYS", "7"),
                ("PULSUS_CLUSTER", "prod"),
            ],
            SchemaMode::Cluster,
        ),
        ("PULSUS_CLUSTER unset", unset.to_vec(), SchemaMode::Single),
    ] {
        let (code, by_default, err) = script_clean(&vars, &[]);
        assert_eq!(code, Some(0), "{what}: the script refused: {err}");
        let (_, by_mode, _) = script_clean(&vars, &mode_args(Some(same_as)));
        assert!(by_default == by_mode, "{what}: the script's default mode");
        let ctx = crate_of(&vars, None);
        assert_eq!(
            ctx,
            crate_of(&vars, Some(same_as)),
            "{what}: the crate's default"
        );
        assert!(
            ctx.as_ref()
                .is_ok_and(|c| rendered(c).as_bytes() == by_default.as_slice()),
            "{what}: the crate's default renders the script's"
        );
    }
}

/// **A3 (issue #746 part A): the two variable lists are exactly the
/// variables the script reads.**
#[test]
fn the_variable_lists_are_every_variable_the_script_reads() {
    let schema_vars = [
        "CLICKHOUSE_DB",
        "PULSUS_CLUSTER",
        "PULSUS_DIST_SUFFIX",
        "PULSUS_STORAGE_POLICY",
        "PULSUS_RETENTION_DAYS",
        "PULSUS_LOG_ROLLUP_RESOLUTION",
        "PULSUS_METRICS_LANDING_RETENTION_HOURS",
        "PULSUS_LOG_LANDING_RETENTION_HOURS",
        "PULSUS_TRACE_LANDING_RETENTION_HOURS",
        "PULSUS_METRICS_DEDUP_WINDOW",
        "PULSUS_LOG_DEDUP_WINDOW",
        "PULSUS_TRACE_DEDUP_WINDOW",
        "PULSUS_ROLLUP_LEVELS",
        "PULSUS_ROLLUP_RANGES",
        "PULSUS_ROLLUP_COUNTER_BUCKETS",
        "PULSUS_COLD_VOLUME",
        "PULSUS_METRICS_HOT_DAYS",
        "PULSUS_LOGS_HOT_DAYS",
        "PULSUS_TRACES_HOT_DAYS",
        "PULSUS_ROLLUP_RETENTION_DAYS",
        "PULSUS_ROLLUP_HOT_DAYS",
        "PULSUS_TRACEQL_INDEXED_ATTRIBUTES",
    ];
    let connection_vars = [
        "CLICKHOUSE_SERVER",
        "CLICKHOUSE_HTTP_PORT",
        "CLICKHOUSE_AUTH",
        "PULSUS_SCHEMA_WAIT_SECONDS",
    ];
    assert_eq!(pulsus_schema::SCHEMA_VARS, schema_vars, "SCHEMA_VARS");
    assert_eq!(
        pulsus_schema::SCRIPT_CONNECTION_VARS,
        connection_vars,
        "SCRIPT_CONNECTION_VARS"
    );
    let mut union = std::collections::BTreeSet::new();
    for name in pulsus_schema::SCHEMA_VARS
        .iter()
        .chain(pulsus_schema::SCRIPT_CONNECTION_VARS)
    {
        assert!(union.insert(*name), "{name} is listed twice");
    }

    let script = std::fs::read_to_string(repo_root().join("schema/schema.sh")).expect("schema.sh");
    let mut scanned = std::collections::BTreeSet::new();
    for (at, _) in script.match_indices("${") {
        let name: String = script[at + 2..]
            .chars()
            .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
            .collect();
        if name.starts_with("PULSUS_") || name.starts_with("CLICKHOUSE_") {
            scanned.insert(name);
        }
    }
    assert_eq!(scanned.len(), 26, "the scan found {scanned:?}");
    let union: std::collections::BTreeSet<String> = union.into_iter().map(String::from).collect();
    assert_eq!(union, scanned, "the lists against the script");
}

/// **A4 (issue #746 part A): at most `MAX_LEVELS` levels, and it is 8.**
#[test]
fn a_schema_carries_at_most_eight_levels() {
    assert_eq!(pulsus_schema::MAX_LEVELS, 8, "MAX_LEVELS");
    assert_eq!(
        pulsus_schema::parse_rollup_levels("1,2,4,12,60,120,240,720", 6).map(|l| l.len()),
        Ok(8)
    );
    assert_eq!(
        pulsus_schema::parse_rollup_levels("1,2,4,12,60,120,240,720,86400", 6),
        Err("more than 8 levels".to_string())
    );
}

/// The text of a double-quoted shell or awk string starting at `s[0] == '"'`.
fn quoted(s: &str) -> Option<(&str, &str)> {
    let body = s.strip_prefix('"')?;
    let mut escaped = false;
    for (i, c) in body.char_indices() {
        match c {
            '\\' if !escaped => escaped = true,
            '"' if !escaped => return Some((&body[..i], &body[i + 1..])),
            _ => escaped = false,
        }
    }
    None
}

/// `$name`, `${name}`, `$1` written `<name>`, `<1>`.
fn templated(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        let name: String = match chars.peek() {
            Some('{') => {
                chars.next();
                let name: String = chars.by_ref().take_while(|c| *c != '}').collect();
                name
            }
            Some(d) if d.is_ascii_digit() => chars.next().into_iter().collect(),
            _ => {
                let mut name = String::new();
                while let Some(c) = chars.peek() {
                    if c.is_ascii_alphanumeric() || *c == '_' {
                        name.push(*c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                name
            }
        };
        if name.is_empty() {
            out.push('$');
        } else {
            out.push_str(&format!("<{name}>"));
        }
    }
    out
}

/// The exit a line of the script ends in: `exit <n>`, or `return …` with
/// `return "$ask_rc"` read as `curl`.
fn exit_on(line: &str) -> Option<String> {
    for word in ["exit ", "return "] {
        let mut from = 0;
        while let Some(at) = line[from..].find(word).map(|i| from + i) {
            let before = line[..at].chars().last();
            from = at + word.len();
            if before.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_') {
                continue;
            }
            let arg: String = line[from..]
                .chars()
                .take_while(|c| !c.is_whitespace() && *c != ';' && *c != '}')
                .collect();
            return Some(match arg.as_str() {
                "\"$ask_rc\"" => "curl".to_string(),
                _ => arg,
            });
        }
    }
    None
}

/// Every error message `schema/schema.sh` prints, in line order with a
/// repeat dropped, each with the exit it leads to.
fn script_refusals() -> Vec<(String, String)> {
    let script = std::fs::read_to_string(repo_root().join("schema/schema.sh")).expect("schema.sh");
    let joined = script.replace("\\\n", "");
    let lines: Vec<&str> = joined.lines().collect();
    let helpers = [
        ("bad_levels", "PULSUS_ROLLUP_LEVELS: "),
        ("bad_ranges", "PULSUS_ROLLUP_RANGES: "),
        ("bad_tiering", ""),
        ("bad_attr", "PULSUS_TRACEQL_INDEXED_ATTRIBUTES: "),
    ];
    // Each helper's body: its lines, and the exit it ends in.
    let mut in_body = vec![false; lines.len()];
    let mut helper_exit = std::collections::BTreeMap::new();
    for (name, _) in helpers {
        let head = format!("{name}() {{");
        let start = lines
            .iter()
            .position(|l| l.trim() == head)
            .unwrap_or_else(|| panic!("{name}'s body"));
        let end = start
            + lines[start..]
                .iter()
                .position(|l| l.trim() == "}")
                .expect("the body's end");
        for flag in &mut in_body[start..=end] {
            *flag = true;
        }
        let exit = lines[start..=end]
            .iter()
            .find_map(|l| exit_on(l))
            .expect("the body's exit");
        helper_exit.insert(name, exit);
    }
    let mut found: Vec<(String, String)> = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if in_body[i] {
            continue;
        }
        let mut message = None;
        if let Some(at) = line.find("echo \"schema.sh: ") {
            let (text, after) = quoted(&line[at + 5..]).expect("a closed string");
            if after.trim_start().starts_with(">&2") {
                let exit = lines[i..]
                    .iter()
                    .find_map(|l| exit_on(l))
                    .expect("an exit after the message");
                message = Some((text["schema.sh: ".len()..].to_string(), exit));
            }
        } else if let Some(at) = line.find("print \"schema.sh: ") {
            let (text, after) = quoted(&line[at + 6..]).expect("a closed string");
            if after.trim_start().starts_with("> \"/dev/stderr\"") {
                let exit = lines[i..]
                    .iter()
                    .find_map(|l| exit_on(l))
                    .expect("an exit after the message");
                message = Some((text["schema.sh: ".len()..].to_string(), exit));
            }
        } else {
            for (name, prefix) in helpers {
                let call = format!("{name} \"");
                let Some(at) = line.find(&call) else { continue };
                if line[..at]
                    .chars()
                    .last()
                    .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
                {
                    continue;
                }
                let (text, _) = quoted(&line[at + name.len() + 1..]).expect("a closed string");
                message = Some((format!("{prefix}{text}"), helper_exit[name].clone()));
            }
        }
        if let Some((text, exit)) = message {
            let template = templated(&text);
            if !found.iter().any(|(t, _)| *t == template) {
                found.push((template, exit));
            }
        }
    }
    found
}

/// The rows of `docs/configuration.md`'s `schema/schema.sh` errors table:
/// (template, exit), numbered from 1 in order.
fn documented_refusals() -> Vec<(String, String)> {
    let text = doc("configuration.md");
    let start = text
        .find("### `schema/schema.sh` errors")
        .map(|at| at + 1)
        .unwrap_or(text.len());
    let section = &text[start..];
    let end = section.find("\n#").unwrap_or(section.len());
    let mut rows = Vec::new();
    for line in section[..end].lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.len() < 5 || cells[1].is_empty() || !cells[1].bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        assert_eq!(
            cells[1].parse::<usize>().unwrap(),
            rows.len() + 1,
            "the rows are numbered in order: {line}"
        );
        // The message cell is one code span; its text may hold a `|` only
        // if the table did, which it does not.
        let message = cells[2..cells.len() - 2].join("|");
        let message = message
            .strip_prefix('`')
            .and_then(|m| m.strip_suffix('`'))
            .unwrap_or_else(|| panic!("the message is one code span: {line}"));
        rows.push((message.to_string(), cells[cells.len() - 2].to_string()));
    }
    rows
}

/// **A6 (issue #746 part A): every error the script prints is documented
/// and held in `REFUSALS`, in order, with its exit.** The script is
/// scanned, not trusted: each message is read out of its `echo`, `print` or
/// helper call, and each exit out of the script.
#[test]
fn every_error_the_script_prints_is_documented_and_in_refusals() {
    let constant: Vec<(String, String)> = pulsus_schema::REFUSALS
        .iter()
        .map(|(t, e)| (t.to_string(), e.to_string()))
        .collect();
    let mut seen = std::collections::BTreeSet::new();
    for (template, _) in &constant {
        assert!(seen.insert(template), "{template:?} is in REFUSALS twice");
    }
    let scanned = script_refusals();
    assert!(
        scanned.len() >= 50,
        "the scan found only {}: {scanned:#?}",
        scanned.len()
    );
    assert_eq!(constant, scanned, "REFUSALS against the script");
    assert_eq!(
        documented_refusals(),
        constant,
        "docs/configuration.md's table against REFUSALS"
    );
}

/// The scan reads each form the script prints a message in, and the exit
/// each reaches.
#[test]
fn the_refusal_scan_reads_every_form() {
    let scanned = script_refusals();
    let find = |template: &str| {
        scanned
            .iter()
            .find(|(t, _)| t == template)
            .map(|(_, e)| e.as_str())
    };
    assert_eq!(find("--mode takes single or cluster"), Some("2"));
    assert_eq!(find("unknown argument <1>"), Some("2"));
    assert_eq!(
        find("PULSUS_ROLLUP_LEVELS: an empty item in '<rollup_levels>'"),
        Some("2")
    );
    assert_eq!(find("<1>: <list_n> items for <level_n> levels"), Some("2"));
    assert_eq!(
        find(
            "PULSUS_ROLLUP_RETENTION_DAYS: <item> for the <level>s level is not a whole number \
             of days from <retention_days> to 4294967295"
        ),
        Some("2")
    );
    assert_eq!(find("the server refused a request; it said:"), Some("curl"));
    assert_eq!(find("the server refused this statement:"), Some("1"));
    assert_eq!(find("the server said:"), Some("1"));
    assert_eq!(
        find(
            "only <live> of <want_hosts> hosts in cluster '<cluster>' answered within \
             <wait_seconds>s; ON CLUSTER DDL would fail on the rest"
        ),
        Some("1")
    );
    assert_eq!(
        find("the file ends in an unterminated statement"),
        Some("1")
    );
    assert!(
        !scanned.iter().any(|(t, _)| t.starts_with("all ")),
        "a message on standard output is not an error"
    );
    assert_eq!(
        scanned
            .iter()
            .filter(|(t, _)| t.starts_with("cluster '<cluster>' is not defined"))
            .count(),
        1,
        "a repeat is dropped"
    );
}

/// A stub server on loopback: it answers its first request (the script's
/// wait loop) `200` and every later one `500` with the body `boom`. Its
/// address, the requests it has served, and each request's
/// `Authorization` header, empty when it had none.
fn refusing_stub() -> (
    std::net::SocketAddr,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
    std::sync::Arc<std::sync::Mutex<Vec<String>>>,
) {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a stub");
    let addr = listener.local_addr().expect("the stub's address");
    let served = std::sync::Arc::new(AtomicUsize::new(0));
    let auths = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let count = served.clone();
    let seen = auths.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().expect("clone the stream"));
            let mut length = 0usize;
            let mut auth = String::new();
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    break;
                }
                let lower = line.to_ascii_lowercase();
                if let Some(v) = lower.strip_prefix("content-length:") {
                    length = v.trim().parse().unwrap_or(0);
                }
                if lower.starts_with("authorization:") {
                    auth = line["authorization:".len()..].trim().to_string();
                }
                if line == "\r\n" {
                    break;
                }
            }
            let mut body = vec![0; length];
            let _ = reader.read_exact(&mut body);
            seen.lock().unwrap().push(auth);
            let n = count.fetch_add(1, Ordering::SeqCst);
            let (status, body) = if n == 0 {
                ("200 OK", "1\n")
            } else {
                ("500 Internal Server Error", "boom")
            };
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    (addr, served, auths)
}

/// **A6b (issue #746 part A): a request the server refuses ends the script
/// with curl's own exit code.** A stub answers the wait loop's first request
/// and refuses every later one.
#[test]
fn a_refused_request_exits_with_curls_code() {
    use std::sync::atomic::Ordering;
    let (addr, served, _) = refusing_stub();
    let root = repo_root();
    let out = Command::new("sh")
        .arg(root.join("schema/schema.sh"))
        .args(["--mode", "single"])
        .current_dir(&root)
        .env_clear()
        .env("PATH", std::env::var("PATH").expect("PATH is set"))
        .env("PULSUS_RETENTION_DAYS", "1")
        .env("CLICKHOUSE_SERVER", "127.0.0.1")
        .env("CLICKHOUSE_HTTP_PORT", addr.port().to_string())
        .env("PULSUS_SCHEMA_WAIT_SECONDS", "2")
        .output()
        .expect("run schema.sh");
    let err = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(22), "the exit: {err}");
    assert!(
        err.contains("schema.sh: the server refused a request; it said:\nboom\n"),
        "the message and the server's answer: {err}"
    );
    assert_eq!(
        served.load(Ordering::SeqCst),
        2,
        "the wait, then the version"
    );
}

/// Standard base64 of `bytes`, for the expected `Authorization` header.
fn base64(bytes: &[u8]) -> String {
    const ABC: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ABC[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// **Issue #746 part A: the credential reaches the server as written.**
/// `CLICKHOUSE_AUTH` holding a pattern character, run from a directory
/// where the pattern names a file: the request carries the credential, not
/// the file's name.
#[test]
fn the_credential_is_sent_as_written() {
    assert_eq!(base64(b"user:p*"), "dXNlcjpwKg==");
    let (addr, _, auths) = refusing_stub();
    let dir = std::env::temp_dir().join(format!("pulsus-schema-auth-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("a directory for the credential case");
    std::fs::write(dir.join("user:pX"), "").expect("a file the pattern names");
    let root = repo_root();
    let out = Command::new("sh")
        .arg(root.join("schema/schema.sh"))
        .args(["--mode", "single"])
        .current_dir(&dir)
        .env_clear()
        .env("PATH", std::env::var("PATH").expect("PATH is set"))
        .env("PULSUS_RETENTION_DAYS", "1")
        .env("CLICKHOUSE_SERVER", "127.0.0.1")
        .env("CLICKHOUSE_HTTP_PORT", addr.port().to_string())
        .env("CLICKHOUSE_AUTH", "user:p*")
        .env("PULSUS_SCHEMA_WAIT_SECONDS", "2")
        .output()
        .expect("run schema.sh");
    std::fs::remove_dir_all(&dir).expect("remove the credential directory");
    let err = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(22), "the exit: {err}");
    let auths = auths.lock().unwrap().clone();
    assert_eq!(
        auths.get(1).map(String::as_str),
        Some(format!("Basic {}", base64(b"user:p*")).as_str()),
        "the version request's credential, of {auths:?}"
    );
}

/// The numeric values of an A7 scalar between `lo` and `hi`; `None` unsets.
fn scalar_values(lo: u64, hi: u64) -> Vec<Option<String>> {
    let mut v: Vec<Option<String>> = vec![None];
    for s in [
        String::new(),
        "0".into(),
        "00".into(),
        "01".into(),
        (lo - 1).to_string(),
        lo.to_string(),
        (lo + 1).to_string(),
        (hi - 1).to_string(),
        hi.to_string(),
        (hi + 1).to_string(),
        format!("+{lo}"),
        "-1".into(),
        format!(" {lo}"),
        format!("{lo} "),
        "1.0".into(),
        "1234567890123456789012345".into(),
    ] {
        v.push(Some(s));
    }
    v
}

/// Every list of `0..=n` items from `items`, joined with `,`.
fn lists(items: &[&str], n: usize) -> Vec<String> {
    let mut all = vec![Vec::<&str>::new()];
    let mut last = vec![Vec::<&str>::new()];
    for _ in 0..n {
        let mut next = Vec::new();
        for l in &last {
            for item in items {
                let mut l = l.clone();
                l.push(item);
                next.push(l);
            }
        }
        all.extend(next.iter().cloned());
        last = next;
    }
    all.into_iter().map(|l| l.join(",")).collect()
}

/// A7's cases: the variables over `PULSUS_RETENTION_DAYS=30` (`None`
/// unsets one).
fn a7_cases() -> Vec<Vec<(String, Option<String>)>> {
    let mut cases: Vec<Vec<(String, Option<String>)>> = Vec::new();
    let one = |var: &str, v: Option<String>| vec![(var.to_string(), v)];
    let set = |pairs: &[(&str, &str)]| -> Vec<(String, Option<String>)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Some(v.to_string())))
            .collect()
    };
    for v in scalar_values(1, 4_294_967_295) {
        cases.push(one("PULSUS_RETENTION_DAYS", v));
    }
    for var in LANDING_VARS {
        for v in scalar_values(1, 168) {
            cases.push(one(var, v));
        }
    }
    for var in DEDUP_VARS {
        for v in scalar_values(1, 1_000_000) {
            cases.push(one(var, v));
        }
    }
    for v in scalar_values(2, 1000) {
        cases.push(one("PULSUS_ROLLUP_COUNTER_BUCKETS", v));
    }
    for var in HOT_DAYS_VARS {
        for v in scalar_values(1, 30) {
            let mut c = set(&[
                ("PULSUS_STORAGE_POLICY", "p"),
                ("PULSUS_COLD_VOLUME", "cold"),
            ]);
            for other in HOT_DAYS_VARS {
                if other != var {
                    c.push((other.to_string(), Some("2".to_string())));
                }
            }
            c.push((var.to_string(), v));
            cases.push(c);
        }
    }
    let names = [
        "a".to_string(),
        "_".to_string(),
        "1".to_string(),
        "a-b".to_string(),
        "a b".to_string(),
        "a.b".to_string(),
        "é".to_string(),
        "a".repeat(64),
        String::new(),
    ];
    for var in [
        "CLICKHOUSE_DB",
        "PULSUS_CLUSTER",
        "PULSUS_DIST_SUFFIX",
        "PULSUS_STORAGE_POLICY",
        "PULSUS_COLD_VOLUME",
    ] {
        for v in names.iter().map(|n| Some(n.clone())).chain([None]) {
            let mut c = if var == "PULSUS_COLD_VOLUME" {
                set(&[
                    ("PULSUS_STORAGE_POLICY", "p"),
                    ("PULSUS_METRICS_HOT_DAYS", "2"),
                    ("PULSUS_LOGS_HOT_DAYS", "2"),
                    ("PULSUS_TRACES_HOT_DAYS", "2"),
                ])
            } else {
                Vec::new()
            };
            c.push((var.to_string(), v));
            cases.push(c);
        }
    }
    let mut resolutions: Vec<Option<String>> = vec![None];
    for r in [
        "",
        "1ms",
        "5s",
        "500ms",
        "2m",
        "1h",
        "1d",
        "1w",
        "999999999999ms",
        "1000000000000ms",
        "999999999s",
        "1000000000s",
        "99999999m",
        "100000000m",
        "999999h",
        "1000000h",
        "99999d",
        "100000d",
        "9999w",
        "10000w",
        "0s",
        "05s",
        "5",
        "5ns",
        "-5s",
        "1.5s",
        "abcs",
        " 5s",
        "5s ",
    ] {
        resolutions.push(Some(r.to_string()));
    }
    for v in resolutions {
        cases.push(one("PULSUS_LOG_ROLLUP_RESOLUTION", v));
    }
    let widths = [
        "1", "2", "7", "60", "90", "120", "720", "1200", "1440", "86400", "86401", "172800",
    ];
    for list in lists(&widths, 3) {
        for hours in ["1", "6"] {
            cases.push(set(&[
                ("PULSUS_ROLLUP_LEVELS", list.as_str()),
                ("PULSUS_METRICS_LANDING_RETENTION_HOURS", hours),
            ]));
        }
    }
    for chain in [
        "1,2,4,12,60,120,240",
        "1,2,4,12,60,120,240,720",
        "1,2,4,12,60,120,240,720,86400",
    ] {
        cases.push(set(&[("PULSUS_ROLLUP_LEVELS", chain)]));
    }
    let switch = [
        "59",
        "719",
        "720",
        "3600",
        "86399",
        "86400",
        "999999999",
        "1000000000",
        "",
    ];
    for list in lists(&switch, 3) {
        cases.push(set(&[
            ("PULSUS_ROLLUP_LEVELS", "60,720,86400"),
            ("PULSUS_ROLLUP_RANGES", list.as_str()),
        ]));
    }
    let pairs = |items: &[&str]| -> Vec<String> {
        let mut out = Vec::new();
        for a in items {
            for b in items {
                out.push(format!("{a},{b}"));
            }
        }
        out
    };
    for var in ["PULSUS_ROLLUP_RETENTION_DAYS", "PULSUS_ROLLUP_HOT_DAYS"] {
        for (retention, items, hot) in [
            (
                "30",
                &["29", "30", "31", "4294967295", "4294967296", "0", "01", ""][..],
                "2",
            ),
            ("1", &["1", "2", "3"][..], "1"),
        ] {
            for list in pairs(items) {
                for tiered in [false, true] {
                    let mut c = set(&[
                        ("PULSUS_RETENTION_DAYS", retention),
                        ("PULSUS_ROLLUP_LEVELS", "60,720"),
                        ("PULSUS_ROLLUP_RANGES", "86400"),
                        (var, list.as_str()),
                    ]);
                    if tiered {
                        c.extend(set(&[
                            ("PULSUS_STORAGE_POLICY", "p"),
                            ("PULSUS_COLD_VOLUME", "cold"),
                            ("PULSUS_METRICS_HOT_DAYS", hot),
                            ("PULSUS_LOGS_HOT_DAYS", hot),
                            ("PULSUS_TRACES_HOT_DAYS", hot),
                        ]));
                    }
                    cases.push(c);
                }
            }
        }
    }
    let keys = [
        "span.a".to_string(),
        "event.b".to_string(),
        "resource.c".to_string(),
        "x.y".to_string(),
        "span.".to_string(),
        "span.a..b".to_string(),
        "span.-a".to_string(),
        format!("span.{}", "a".repeat(127)),
        format!("span.{}", "a".repeat(128)),
        format!("span.{}", "a".repeat(129)),
    ];
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let mut attr_lists = lists(&key_refs, 2);
    for n in [15, 16, 17] {
        attr_lists.push(
            (1..=n)
                .map(|i| format!("span.a{i}"))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    attr_lists.push("span.a,span.a".to_string());
    for list in attr_lists {
        cases.push(set(&[("PULSUS_TRACEQL_INDEXED_ATTRIBUTES", list.as_str())]));
    }
    cases
}

/// **A7 (issue #746 part A): the script and `ctx_from_vars` agree at every
/// boundary.** About 5,300 cases over every rule's limits, at and on each
/// side; for each, the same exit and message, and for an accepted case the
/// same render. Eight threads.
#[test]
fn the_script_and_the_crate_agree_at_every_boundary() {
    let cases = a7_cases();
    assert!(cases.len() > 5000, "only {} cases", cases.len());
    let next = std::sync::atomic::AtomicUsize::new(0);
    let failures = std::sync::Mutex::new(Vec::new());
    let accepted = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(case) = cases.get(i) else { break };
                    let vars = case_vars(case);
                    let vars: Vec<(&str, &str)> =
                        vars.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                    let map: std::collections::BTreeMap<&str, &str> =
                        vars.iter().copied().collect();
                    if ctx_from_vars(&map, Some(SchemaMode::Single)).is_ok() {
                        accepted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    if let Some(diff) = script_and_crate_differ(&vars, Some(SchemaMode::Single)) {
                        failures.lock().unwrap().push(diff);
                    }
                }
            });
        }
    });
    let failures = failures.into_inner().unwrap();
    assert!(
        failures.is_empty(),
        "{} of {} cases differ; the first: {:#?}",
        failures.len(),
        cases.len(),
        &failures[..failures.len().min(20)]
    );
    // Both sides accepting nothing would agree everywhere.
    assert!(
        accepted.load(std::sync::atomic::Ordering::Relaxed) > 100,
        "too few accepted cases"
    );

    // An attribute list holding a pattern character, from a directory
    // where the pattern names files: the script reads the text, as the
    // crate does, and never the directory.
    let dir = std::env::temp_dir().join(format!("pulsus-schema-glob-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("a directory for the pattern cases");
    for name in ["span.a", "event.b"] {
        std::fs::write(dir.join(name), "").expect("a file the pattern names");
    }
    let mut differ = Vec::new();
    for list in ["*", "span.*", "span.a,*", "?pan.a", "event.[b]"] {
        let vars = [
            ("PULSUS_RETENTION_DAYS", "30"),
            ("PULSUS_TRACEQL_INDEXED_ATTRIBUTES", list),
        ];
        differ.extend(script_and_crate_differ_in(
            &vars,
            Some(SchemaMode::Single),
            &dir,
        ));
    }
    std::fs::remove_dir_all(&dir).expect("remove the pattern directory");
    assert!(
        differ.is_empty(),
        "from a directory the patterns match: {differ:#?}"
    );
}

/// The rows of `docs/configuration.md`'s "Grammar boundaries" table:
/// (accepted, grammar, values), each value one code span taken exactly.
fn grammar_boundaries() -> Vec<(bool, String, Vec<String>)> {
    let text = doc("configuration.md");
    let start = text
        .find("### Grammar boundaries")
        .unwrap_or_else(|| panic!("no Grammar boundaries table"));
    let section = &text[start + 1..];
    let section = &section[..section.find("\n#").unwrap_or(section.len())];
    let mut rows = Vec::new();
    for line in section.lines() {
        let Some(rest) = line
            .strip_prefix("| accepted | ")
            .map(|r| (true, r))
            .or_else(|| line.strip_prefix("| refused | ").map(|r| (false, r)))
        else {
            continue;
        };
        let (accepted, rest) = rest;
        let (grammar, cell) = rest.split_once(" | ").expect("three cells");
        let cell = cell.strip_suffix(" |").expect("the row's end");
        let mut values = Vec::new();
        let mut cell = cell;
        loop {
            let body = cell
                .strip_prefix('`')
                .unwrap_or_else(|| panic!("a code span: {line}"));
            let end = body.find('`').expect("a closed code span");
            values.push(body[..end].to_string());
            cell = &body[end + 1..];
            if cell.is_empty() {
                break;
            }
            cell = cell
                .strip_prefix(", ")
                .unwrap_or_else(|| panic!("values are separated by ', ': {line}"));
        }
        rows.push((accepted, grammar.to_string(), values));
    }
    let kinds: Vec<(bool, &str)> = rows.iter().map(|(a, g, _)| (*a, g.as_str())).collect();
    assert_eq!(
        kinds,
        [
            (true, "dedup window"),
            (false, "dedup window"),
            (true, "log rollup resolution"),
            (false, "log rollup resolution"),
        ],
        "the table's rows"
    );
    rows
}

/// A resolution as a `Duration`, worked out here rather than by the parser.
fn resolution_of(v: &str) -> Duration {
    let digits: String = v.chars().take_while(char::is_ascii_digit).collect();
    let n: u64 = digits.parse().expect("digits");
    match &v[digits.len()..] {
        "ms" => Duration::from_millis(n),
        "s" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 3600),
        "d" => Duration::from_secs(n * 86_400),
        "w" => Duration::from_secs(n * 604_800),
        unit => panic!("unit {unit}"),
    }
}

/// **A8 (issue #746 part A): the script, `ctx_from_vars` and the binary's
/// parsers accept and refuse the documented boundary values.** Read from
/// `docs/configuration.md`'s "Grammar boundaries" table.
#[test]
fn the_script_the_crate_and_the_parsers_hold_the_documented_boundaries() {
    let rows = grammar_boundaries();
    for (accepted, grammar, values) in rows {
        for v in values {
            let checks: Vec<(&str, String)> = if grammar == "dedup window" {
                let parsed = pulsus_config::parse_dedup_window(&v);
                if accepted {
                    assert_eq!(parsed, Some(v.parse::<u64>().unwrap()), "{v:?}: the parser");
                } else {
                    assert_eq!(parsed, None, "{v:?}: the parser");
                }
                DEDUP_VARS
                    .iter()
                    .map(|var| {
                        (
                            *var,
                            format!(
                                "{var} must be a whole number of blocks from 1 to 1000000: {v}"
                            ),
                        )
                    })
                    .collect()
            } else {
                let parsed = pulsus_config::parse_log_rollup_resolution(&v);
                if accepted {
                    assert_eq!(parsed, Some(resolution_of(&v)), "{v:?}: the parser");
                } else {
                    assert_eq!(parsed, None, "{v:?}: the parser");
                }
                vec![(
                    "PULSUS_LOG_ROLLUP_RESOLUTION",
                    format!("{RESOLUTION_REFUSED}{v}"),
                )]
            };
            let row = if grammar == "dedup window" { 7 } else { 34 };
            for (var, refusal) in checks {
                let vars = [("PULSUS_RETENTION_DAYS", "30"), (var, v.as_str())];
                let (code, out, err) = script_clean(&vars, &["--mode", "single"]);
                let map: std::collections::BTreeMap<&str, &str> = vars.iter().copied().collect();
                let from_crate = ctx_from_vars(&map, Some(SchemaMode::Single));
                if accepted {
                    assert_eq!(code, Some(0), "{var}={v:?}: the script ({err})");
                    let ctx = from_crate
                        .unwrap_or_else(|e| panic!("{var}={v:?}: ctx_from_vars refused: {e}"));
                    assert!(
                        rendered(&ctx).as_bytes() == out.as_slice(),
                        "{var}={v:?}: the renders"
                    );
                    if grammar == "dedup window" {
                        let n: u64 = v.parse().unwrap();
                        let got = [
                            ctx.metrics_dedup_window,
                            ctx.log_dedup_window,
                            ctx.trace_dedup_window,
                        ];
                        let at = DEDUP_VARS.iter().position(|d| *d == var).unwrap();
                        assert_eq!(got[at], n, "{var}={v:?}: the context's window");
                    } else {
                        assert_eq!(ctx.log_rollup, resolution_of(&v), "{var}={v:?}");
                    }
                } else {
                    assert_eq!(code, Some(2), "{var}={v:?}: the script's exit ({err})");
                    assert_eq!(
                        script_message(&err),
                        Some(refusal.clone()),
                        "{var}={v:?}: the script's message"
                    );
                    assert_eq!(
                        from_crate.map(|_| ()),
                        Err(refusal.clone()),
                        "{var}={v:?}: ctx_from_vars"
                    );
                    assert!(
                        matches_template(&refusal, refusal_row(row)),
                        "{refusal:?} is row {row}"
                    );
                }
            }
        }
    }
}
