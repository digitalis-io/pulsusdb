//! The variables `schema/schema.sh` reads, and the one function from them to
//! a [`RenderCtx`] (issue #746 part A).
//!
//! The script and [`ctx_from_vars`] apply the same rules, in the same order,
//! with the same messages; `tests/schema_file.rs` holds the two together at
//! every boundary. Every error the script prints is a row of [`REFUSALS`]
//! and of docs/configuration.md §7's `schema/schema.sh` errors table.

use std::collections::BTreeMap;

use crate::{
    DEFAULT_ROLLUP_COUNTER_BUCKETS, DEFAULT_ROLLUP_LEVELS, DEFAULT_ROLLUP_RANGES,
    GRAPH_JOB_MAX_ROWS, RenderCtx, TieringInput, parse_rollup_counter_buckets, parse_rollup_levels,
    parse_rollup_ranges, validate_tiering,
};

/// Every variable `schema/schema.sh` reads to decide the schema.
pub const SCHEMA_VARS: &[&str] = &[
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

/// The variables `schema/schema.sh` reads only to reach the server.
pub const SCRIPT_CONNECTION_VARS: &[&str] = &[
    "CLICKHOUSE_SERVER",
    "CLICKHOUSE_HTTP_PORT",
    "CLICKHOUSE_AUTH",
    "PULSUS_SCHEMA_WAIT_SECONDS",
    "CLICKHOUSE_PROTO",
    "CLICKHOUSE_TLS_CA_FILE",
];

/// The most rollup levels a schema carries.
pub const MAX_LEVELS: usize = 8;

/// Every error `schema/schema.sh` prints, in script order: (message
/// template, exit). `<…>` is a value filled in; exit `"curl"` is curl's own
/// code. docs/configuration.md §7 numbers the same rows from 1.
pub const REFUSALS: &[(&str, &str)] = &[
    ("--mode takes single or cluster", "2"),
    ("unknown argument <1>", "2"),
    (
        "<what> must be letters, digits and underscores: <value>",
        "2",
    ),
    (
        "PULSUS_RETENTION_DAYS is not set; retention has no default (docs/configuration.md §7)",
        "2",
    ),
    (
        "PULSUS_RETENTION_DAYS must be a whole number of days from 1 to 4294967295: <retention_days>",
        "2",
    ),
    (
        "<what> must be a whole number of hours from 1 to 168: <value>",
        "2",
    ),
    (
        "<what> must be a whole number of blocks from 1 to 1000000: <value>",
        "2",
    ),
    (
        "PULSUS_ROLLUP_LEVELS: an empty item in '<rollup_levels>'",
        "2",
    ),
    (
        "PULSUS_ROLLUP_LEVELS: <item> is not a whole number of seconds from 1 to 86400",
        "2",
    ),
    (
        "PULSUS_ROLLUP_LEVELS: <item> does not divide a day (86400)",
        "2",
    ),
    (
        "PULSUS_ROLLUP_LEVELS: <item> is below 60 and does not divide 60",
        "2",
    ),
    (
        "PULSUS_ROLLUP_LEVELS: <prev> then <item>: the levels must increase",
        "2",
    ),
    (
        "PULSUS_ROLLUP_LEVELS: <item> is not a multiple of <prev>",
        "2",
    ),
    ("PULSUS_ROLLUP_LEVELS: more than 8 levels", "2"),
    (
        "PULSUS_ROLLUP_LEVELS: the first level <1> outlives metric_landing: 2 × <period> + 870 > <metrics_landing_hours> × 3600",
        "2",
    ),
    (
        "PULSUS_ROLLUP_RANGES: an empty item in '<rollup_ranges>'",
        "2",
    ),
    (
        "PULSUS_ROLLUP_RANGES: <item> is not a whole number of seconds from 1 to 999999999",
        "2",
    ),
    (
        "PULSUS_ROLLUP_RANGES: <n_ranges> ranges for <n_levels> levels: give one for each level after the first",
        "2",
    ),
    (
        "PULSUS_ROLLUP_RANGES: <prev> then <item>: the ranges must increase",
        "2",
    ),
    (
        "PULSUS_ROLLUP_RANGES: <item> is shorter than its level <level>",
        "2",
    ),
    (
        "PULSUS_ROLLUP_COUNTER_BUCKETS: <counter_buckets> is not a whole number from 2 to 1000",
        "2",
    ),
    (
        "PULSUS_COLD_VOLUME is set and PULSUS_STORAGE_POLICY is not",
        "2",
    ),
    (
        "cold_volume must be letters, digits and underscores: <cold_volume>",
        "2",
    ),
    (
        "<what> is not set; with PULSUS_COLD_VOLUME set each of PULSUS_METRICS_HOT_DAYS, PULSUS_LOGS_HOT_DAYS and PULSUS_TRACES_HOT_DAYS is required",
        "2",
    ),
    ("<what> is set and PULSUS_COLD_VOLUME is not", "2"),
    (
        "<what> must be a whole number of days from 1 to <retention_days>: <value>",
        "2",
    ),
    ("<1>: an empty item in '<2>'", "2"),
    ("<1>: <list_n> items for <level_n> levels", "2"),
    (
        "PULSUS_ROLLUP_RETENTION_DAYS: <item> for the <level>s level is not a whole number of days from <retention_days> to 4294967295",
        "2",
    ),
    (
        "PULSUS_ROLLUP_RETENTION_DAYS: <item> for the <level>s level is too short: the next level's switch point, <from> s, requires <need> retention days",
        "2",
    ),
    (
        "PULSUS_ROLLUP_HOT_DAYS is set and PULSUS_COLD_VOLUME is not",
        "2",
    ),
    (
        "PULSUS_ROLLUP_HOT_DAYS: <item> for the <level>s level is not a whole number of days from 1 to <keep>",
        "2",
    ),
    (
        "--mode cluster needs PULSUS_CLUSTER set to the cluster name",
        "2",
    ),
    (
        "PULSUS_LOG_ROLLUP_RESOLUTION must be a whole number with no leading zero, then ms (at most 12 digits), s (9), m (8), h (6), d (5) or w (4): <rollup>",
        "2",
    ),
    (
        "PULSUS_TRACEQL_INDEXED_ATTRIBUTES: an empty item in '<indexed>'",
        "2",
    ),
    (
        "PULSUS_TRACEQL_INDEXED_ATTRIBUTES: <item>: a resource attribute is read through resource_id, which the span table already indexes",
        "2",
    ),
    (
        "PULSUS_TRACEQL_INDEXED_ATTRIBUTES: <item>: the scope must be span. or event.",
        "2",
    ),
    (
        "PULSUS_TRACEQL_INDEXED_ATTRIBUTES: <item>: the key must be dot-separated segments of letters, digits, '_' and '-'",
        "2",
    ),
    (
        "PULSUS_TRACEQL_INDEXED_ATTRIBUTES: <item>: the key is longer than 128 bytes",
        "2",
    ),
    (
        "PULSUS_TRACEQL_INDEXED_ATTRIBUTES: <item> is listed twice",
        "2",
    ),
    (
        "PULSUS_TRACEQL_INDEXED_ATTRIBUTES: names more than 16 attributes",
        "2",
    ),
    (
        "the lines above carry a token this script does not render",
        "2",
    ),
    ("CLICKHOUSE_PROTO must be http or https: <proto>", "2"),
    ("the server refused a request; it said:", "curl"),
    ("the server refused this statement:", "1"),
    ("the server said:", "1"),
    (
        "clickhouse at <server>:<port> did not answer within <wait_seconds>s",
        "1",
    ),
    ("could not read a version from '<version>'", "1"),
    (
        "clickhouse <version> is below the minimum supported version 26.8",
        "1",
    ),
    ("cluster '<cluster>' is not defined on <server>:<port>", "1"),
    (
        "only <live> of <want_hosts> hosts in cluster '<cluster>' answered within <wait_seconds>s; ON CLUSTER DDL would fail on the rest",
        "1",
    ),
    (
        "<policy_what> is defined on <policy_have> of <policy_want> hosts; every server's configuration must carry it (docs/configuration.md §3)",
        "1",
    ),
    ("the file ends in an unterminated statement", "1"),
];

/// The script's `--mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaMode {
    Single,
    Cluster,
}

/// `text` as a whole number of at most `digits` digits with no leading zero.
fn whole(text: &str, digits: usize) -> Option<u64> {
    let ok = (1..=digits).contains(&text.len())
        && !text.starts_with('0')
        && text.bytes().all(|b| b.is_ascii_digit());
    if ok { text.parse().ok() } else { None }
}

fn joined(items: &[u32]) -> String {
    items
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// What `schema/schema.sh` decides from `vars` and `mode`, checked in the
/// script's order: the first rule that fails is the `Err`, the text the
/// script prints after `schema.sh: `.
///
/// A key absent from `vars` is unset and `""` is set and empty; keys outside
/// [`SCHEMA_VARS`] are not read. Each value is read as the script reads it:
/// `${V:-default}` (empty is the default) for every variable but the three
/// rollup lists, which keep an empty value (`${V-default}`), and the two
/// level day lists, which count as given whenever set. `mode` `None` is the
/// script's default: cluster when `PULSUS_CLUSTER` is non-empty.
pub fn ctx_from_vars(
    vars: &BTreeMap<&str, &str>,
    mode: Option<SchemaMode>,
) -> Result<RenderCtx, String> {
    let get = |name: &str| {
        debug_assert!(
            SCHEMA_VARS.contains(&name),
            "{name} is not a schema variable"
        );
        vars.get(name).copied()
    };
    // `${V:-default}`.
    let or_default = |name: &str, default: &'static str| match get(name) {
        Some(v) if !v.is_empty() => v,
        _ => default,
    };
    // `${V:-}`, as `None` when unset or empty.
    let non_empty = |name: &str| get(name).filter(|v| !v.is_empty());

    let db = or_default("CLICKHOUSE_DB", "pulsus");
    let cluster = or_default("PULSUS_CLUSTER", "");
    let dist_suffix = or_default("PULSUS_DIST_SUFFIX", "_dist");
    let storage_policy = or_default("PULSUS_STORAGE_POLICY", "");
    let retention = or_default("PULSUS_RETENTION_DAYS", "");
    let rollup = or_default("PULSUS_LOG_ROLLUP_RESOLUTION", "5s");
    let default_levels = joined(DEFAULT_ROLLUP_LEVELS);
    let default_ranges = joined(DEFAULT_ROLLUP_RANGES);
    let default_buckets = DEFAULT_ROLLUP_COUNTER_BUCKETS.to_string();
    let levels_text = get("PULSUS_ROLLUP_LEVELS").unwrap_or(&default_levels);
    let ranges_text = get("PULSUS_ROLLUP_RANGES").unwrap_or(&default_ranges);
    let buckets_text = get("PULSUS_ROLLUP_COUNTER_BUCKETS").unwrap_or(&default_buckets);
    let indexed = or_default("PULSUS_TRACEQL_INDEXED_ATTRIBUTES", "");
    let mode = mode.unwrap_or(if cluster.is_empty() {
        SchemaMode::Single
    } else {
        SchemaMode::Cluster
    });

    for (what, value) in [
        ("database", db),
        ("cluster", cluster),
        ("dist_suffix", dist_suffix),
        ("storage_policy", storage_policy),
    ] {
        if !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(format!(
                "{what} must be letters, digits and underscores: {value}"
            ));
        }
    }

    if retention.is_empty() {
        return Err(
            "PULSUS_RETENTION_DAYS is not set; retention has no default (docs/configuration.md §7)"
                .to_string(),
        );
    }
    let retention_days = whole(retention, 10)
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| {
            format!(
                "PULSUS_RETENTION_DAYS must be a whole number of days from 1 to 4294967295: {retention}"
            )
        })?;

    let mut landing = [0u32; 3];
    for (slot, name) in landing.iter_mut().zip([
        "PULSUS_METRICS_LANDING_RETENTION_HOURS",
        "PULSUS_LOG_LANDING_RETENTION_HOURS",
        "PULSUS_TRACE_LANDING_RETENTION_HOURS",
    ]) {
        let value = or_default(name, "6");
        *slot = whole(value, 3)
            .filter(|v| *v <= 168)
            .map(|v| v as u32)
            .ok_or_else(|| {
                format!("{name} must be a whole number of hours from 1 to 168: {value}")
            })?;
    }

    let mut dedup = [0u64; 3];
    for (slot, name) in dedup.iter_mut().zip([
        "PULSUS_METRICS_DEDUP_WINDOW",
        "PULSUS_LOG_DEDUP_WINDOW",
        "PULSUS_TRACE_DEDUP_WINDOW",
    ]) {
        let value = or_default(name, "10000");
        *slot = pulsus_config::parse_dedup_window(value).ok_or_else(|| {
            format!("{name} must be a whole number of blocks from 1 to 1000000: {value}")
        })?;
    }

    let levels = parse_rollup_levels(levels_text, landing[0])
        .map_err(|e| format!("PULSUS_ROLLUP_LEVELS: {e}"))?;
    let ranges = parse_rollup_ranges(ranges_text, &levels)
        .map_err(|e| format!("PULSUS_ROLLUP_RANGES: {e}"))?;
    let counter_buckets = parse_rollup_counter_buckets(buckets_text)
        .map_err(|e| format!("PULSUS_ROLLUP_COUNTER_BUCKETS: {e}"))?;

    let tiering = validate_tiering(&TieringInput {
        storage_policy: non_empty("PULSUS_STORAGE_POLICY"),
        cold_volume: non_empty("PULSUS_COLD_VOLUME"),
        retention_days,
        metrics_hot_days: non_empty("PULSUS_METRICS_HOT_DAYS"),
        logs_hot_days: non_empty("PULSUS_LOGS_HOT_DAYS"),
        traces_hot_days: non_empty("PULSUS_TRACES_HOT_DAYS"),
        levels: &levels,
        ranges: &ranges,
        rollup_retention_days: get("PULSUS_ROLLUP_RETENTION_DAYS"),
        rollup_hot_days: get("PULSUS_ROLLUP_HOT_DAYS"),
    })?;

    if mode == SchemaMode::Cluster && cluster.is_empty() {
        return Err("--mode cluster needs PULSUS_CLUSTER set to the cluster name".to_string());
    }

    let log_rollup = pulsus_config::parse_log_rollup_resolution(rollup).ok_or_else(|| {
        format!(
            "PULSUS_LOG_ROLLUP_RESOLUTION must be a whole number with no leading zero, then ms \
             (at most 12 digits), s (9), m (8), h (6), d (5) or w (4): {rollup}"
        )
    })?;

    const ATTRS: &str = "PULSUS_TRACEQL_INDEXED_ATTRIBUTES";
    let mut attributes: Vec<String> = Vec::new();
    if !indexed.is_empty() {
        if indexed.split(',').any(str::is_empty) {
            return Err(format!("{ATTRS}: an empty item in '{indexed}'"));
        }
        for item in indexed.split(',') {
            pulsus_config::parse_indexed_attribute(item)
                .map_err(|e| format!("{ATTRS}: {item}: {e}"))?;
            if attributes.iter().any(|a| a == item) {
                return Err(format!("{ATTRS}: {item} is listed twice"));
            }
            attributes.push(item.to_string());
            if attributes.len() > pulsus_config::MAX_INDEXED_ATTRIBUTES {
                return Err(format!(
                    "{ATTRS}: names more than {} attributes",
                    pulsus_config::MAX_INDEXED_ATTRIBUTES
                ));
            }
        }
    }

    Ok(RenderCtx {
        db: db.to_string(),
        cluster: (mode == SchemaMode::Cluster).then(|| cluster.to_string()),
        dist_suffix: dist_suffix.to_string(),
        storage_policy: non_empty("PULSUS_STORAGE_POLICY").map(str::to_string),
        retention_days,
        log_rollup,
        metrics_landing_retention_hours: landing[0],
        metrics_dedup_window: dedup[0],
        log_landing_retention_hours: landing[1],
        log_dedup_window: dedup[1],
        trace_landing_retention_hours: landing[2],
        trace_dedup_window: dedup[2],
        trace_indexed_attributes: attributes,
        graph_job_max_rows: GRAPH_JOB_MAX_ROWS,
        rollup_levels: levels,
        rollup_ranges: ranges,
        rollup_counter_buckets: counter_buckets,
        cold_volume: tiering.cold_volume,
        metrics_hot_days: tiering.metrics_hot_days,
        logs_hot_days: tiering.logs_hot_days,
        traces_hot_days: tiering.traces_hot_days,
        rollup_retention_days: tiering.rollup_retention_days,
        rollup_hot_days: tiering.rollup_hot_days,
    })
}
