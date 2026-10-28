//! Token substitution for `schema/schema.sql`: explicit `{{token}}` string
//! replacement, twenty-four names and nothing else. The level tokens of the
//! file's level block are resolved earlier, by [`expand_levels`].
//!
//! **Double braces, always.** ClickHouse's own `{shard}` and `{replica}`
//! macros are single-brace and must survive verbatim into `Replicated*`
//! engine arguments; a substitution expression that matched a single brace
//! pair would eat them, and the server accepts the result without complaint
//! — every shard then joins one replica set. Double-brace tokens cannot
//! match them.

use std::time::Duration;

/// Config-derived rendering context, and the parameter the test toolkit's
/// `run_init` takes. The same struct doubles as both so there is exactly one
/// config-shaped struct in this crate rather than two kept in sync by hand.
pub type SchemaParams = RenderCtx;

/// The config-derived context every DDL block renders against. Re-exported
/// from `pulsus-schema` as `SchemaParams` — the same struct doubles as the
/// parameter of `pulsus-schema-testkit`'s `run_init` and the internal
/// rendering context, so there is exactly
/// one config-shaped struct in this crate rather than two kept in sync by
/// hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderCtx {
    /// `CLICKHOUSE_DB` / `clickhouse.database` (docs/configuration.md §2).
    pub db: String,
    /// `PULSUS_CLUSTER` (docs/configuration.md §4). `None` = single-node:
    /// plain `MergeTree`-family engines, no `_dist` wrappers, no `ON
    /// CLUSTER`. `Some(name)` = clustered: `Replicated*` engines, `_dist`
    /// wrappers, `ON CLUSTER '<name>'` on every DDL statement.
    pub cluster: Option<String>,
    /// `PULSUS_DIST_SUFFIX` (docs/configuration.md §4, default `_dist`).
    pub dist_suffix: String,
    /// `PULSUS_STORAGE_POLICY` (docs/configuration.md §3). Injected as a
    /// `storage_policy` table SETTING when set.
    pub storage_policy: Option<String>,
    /// `PULSUS_RETENTION_DAYS` (docs/configuration.md §7): required, no
    /// default (issue #497). Renders every target table's TTL and the
    /// database comment the binary reads it back from.
    pub retention_days: u32,
    /// `PULSUS_LOG_ROLLUP_RESOLUTION` (docs/configuration.md §3, default
    /// 5s). Sets both the `log_metrics_<res>` table-name suffix and the
    /// bucket-floor expression in its materialized view.
    pub log_rollup: Duration,
    /// `PULSUS_METRICS_LANDING_RETENTION_HOURS` (issue #603): the metrics
    /// landing table's delete-TTL, in hours.
    pub metrics_landing_retention_hours: u32,
    /// `PULSUS_METRICS_DEDUP_WINDOW` (issue #603): the block-deduplication
    /// window the landing table and the four derived metric tables carry.
    pub metrics_dedup_window: u64,
    /// `PULSUS_LOG_LANDING_RETENTION_HOURS` (issue #603): the logs landing
    /// table's delete-TTL, in hours.
    pub log_landing_retention_hours: u32,
    /// `PULSUS_LOG_DEDUP_WINDOW` (issue #603): the block-deduplication window
    /// the logs landing table and the five derived logs tables carry.
    pub log_dedup_window: u64,
    /// `PULSUS_TRACE_LANDING_RETENTION_HOURS` (issues #584 to #586): the
    /// traces landing table's delete-TTL, in hours.
    pub trace_landing_retention_hours: u32,
    /// `PULSUS_TRACE_DEDUP_WINDOW` (issues #584 to #586): the
    /// block-deduplication window the traces landing table and the five
    /// derived trace tables carry.
    pub trace_dedup_window: u64,
    /// `PULSUS_TRACEQL_INDEXED_ATTRIBUTES` (issue #595 part 2): validated
    /// `span.<key>` / `event.<key>` items, each rendered into one index on
    /// `spans` by `{{trace_attr_indexes}}`.
    pub trace_indexed_attributes: Vec<String>,
    /// `{{graph_job_max_rows}}` (issue #599): the rows one run of the
    /// service-graph job may read. Not a setting: production renders
    /// [`GRAPH_JOB_MAX_ROWS`], and a test lowers it to make a run fail.
    pub graph_job_max_rows: u64,
    /// `PULSUS_ROLLUP_LEVELS` (issue #18): the validated rollup levels in
    /// seconds, smallest first; empty for none.
    pub rollup_levels: Vec<u32>,
    /// `PULSUS_ROLLUP_RANGES` (issue #18): the validated switch points in
    /// seconds, one per level after the first.
    pub rollup_ranges: Vec<u32>,
    /// `PULSUS_ROLLUP_COUNTER_BUCKETS` (issue #18): the buckets a counter's
    /// range covers at least.
    pub rollup_counter_buckets: u32,
    /// `PULSUS_COLD_VOLUME` (docs/configuration.md §3, issue #743 part A):
    /// the volume of the storage policy old parts move to; `None` moves
    /// nothing.
    pub cold_volume: Option<String>,
    /// `PULSUS_METRICS_HOT_DAYS` (docs/configuration.md §3, issue #743 part
    /// A): the days a metric sample stays on the local volume.
    pub metrics_hot_days: Option<u32>,
    /// `PULSUS_LOGS_HOT_DAYS` (docs/configuration.md §3, issue #743 part A):
    /// the days a log line and its rollups stay on the local volume.
    pub logs_hot_days: Option<u32>,
    /// `PULSUS_TRACES_HOT_DAYS` (docs/configuration.md §3, issue #743 part
    /// A): the days a span and the trace tables read by time stay on the
    /// local volume.
    pub traces_hot_days: Option<u32>,
    /// `PULSUS_ROLLUP_RETENTION_DAYS` (docs/configuration.md §7, issue #743
    /// part B): each level's retention in days, one per level; empty: every
    /// level keeps `retention_days`.
    pub rollup_retention_days: Vec<u32>,
    /// `PULSUS_ROLLUP_HOT_DAYS` (docs/configuration.md §7, issue #743 part
    /// B): each level's move age in days, one per level; empty: no level
    /// moves.
    pub rollup_hot_days: Vec<u32>,
}

/// The rows one run of the service-graph job may read (issue #599), the
/// value `schema/schema.sh` renders: three 5-minute buckets of spans at
/// about 67,000 spans a second on one shard.
pub const GRAPH_JOB_MAX_ROWS: u64 = 60_000_000;

/// The rollup levels `schema/schema.sh` renders when `PULSUS_ROLLUP_LEVELS`
/// is unset (issue #18; the owner's table, 2026-10-26).
pub const DEFAULT_ROLLUP_LEVELS: &[u32] = &[5, 60, 720, 7200, 86400];

/// The switch points `schema/schema.sh` renders when `PULSUS_ROLLUP_RANGES`
/// is unset: the query range, in seconds, from which each level after the
/// first is the resolution (issue #18 follow-up).
pub const DEFAULT_ROLLUP_RANGES: &[u32] = &[3600, 86400, 604800, 7776000];

/// `PULSUS_ROLLUP_COUNTER_BUCKETS` when unset: the buckets of a level a
/// `rate` or `increase` range covers at least (issue #18 follow-up).
pub const DEFAULT_ROLLUP_COUNTER_BUCKETS: u32 = 5;

impl RenderCtx {
    /// Whether a family with move age `hot_days` moves to the cold volume
    /// (issue #743 part A): a cold volume is set and the move age is under
    /// the retention.
    pub fn moves(&self, hot_days: Option<u32>) -> bool {
        self.cold_volume.is_some() && hot_days.is_some_and(|h| h < self.retention_days)
    }

    /// Each level's move age when it moves (issue #743 part B): `Some(h)`
    /// for a level whose move age is under its retention, with a cold volume
    /// set; empty when no level moves.
    pub fn level_moves(&self) -> Vec<Option<u32>> {
        if self.cold_volume.is_none() || self.rollup_hot_days.is_empty() {
            return Vec::new();
        }
        self.rollup_hot_days
            .iter()
            .enumerate()
            .map(|(i, &h)| {
                let keep = self
                    .rollup_retention_days
                    .get(i)
                    .copied()
                    .unwrap_or(self.retention_days);
                (h < keep).then_some(h)
            })
            .collect()
    }

    /// The metric catalogue's retention (issue #743 part B): the largest of
    /// `retention_days` and every level's.
    pub fn metrics_catalogue_retention_days(&self) -> u32 {
        self.rollup_retention_days
            .iter()
            .copied()
            .fold(self.retention_days, u32::max)
    }

    /// A context for a test: `db` and `retention_days` as given, every other
    /// field at the value `schema/schema.sh` defaults it to. Retention has no
    /// default (issue #497), so every caller states it.
    ///
    /// **Production does not build a `RenderCtx`**: `schema/schema.sh`
    /// renders the schema from the environment, and nothing in the binary
    /// renders it since the rebuild commands were removed. This exists
    /// because every new field otherwise edits one literal per test file —
    /// thirty-odd of them for the two fields above.
    pub fn for_tests(db: &str, retention_days: u32) -> Self {
        RenderCtx {
            db: db.to_string(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days,
            log_rollup: Duration::from_secs(5),
            metrics_landing_retention_hours: 6,
            metrics_dedup_window: 10_000,
            log_landing_retention_hours: 6,
            log_dedup_window: 10_000,
            trace_landing_retention_hours: 6,
            trace_dedup_window: 10_000,
            trace_indexed_attributes: Vec::new(),
            graph_job_max_rows: GRAPH_JOB_MAX_ROWS,
            rollup_levels: DEFAULT_ROLLUP_LEVELS.to_vec(),
            rollup_ranges: DEFAULT_ROLLUP_RANGES.to_vec(),
            rollup_counter_buckets: DEFAULT_ROLLUP_COUNTER_BUCKETS,
            cold_volume: None,
            metrics_hot_days: None,
            logs_hot_days: None,
            traces_hot_days: None,
            rollup_retention_days: Vec::new(),
            rollup_hot_days: Vec::new(),
        }
    }
}

/// Renders the human-readable rollup-resolution suffix used in
/// `log_metrics_<res>` (docs/schemas.md §3.1), e.g. `5s`, `500ms`, `2m`.
/// Whole units are preferred over sub-units so the default (`5s`) matches
/// the documented table name exactly; a duration that isn't a whole number
/// of any unit falls back to milliseconds.
pub fn rollup_suffix(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos == 0 {
        return "0ms".to_string();
    }
    let millis = d.as_millis();
    if millis > 0 && nanos.is_multiple_of(1_000_000) {
        if millis.is_multiple_of(60_000) {
            return format!("{}m", millis / 60_000);
        }
        if millis.is_multiple_of(1_000) {
            return format!("{}s", millis / 1_000);
        }
        return format!("{millis}ms");
    }
    format!("{nanos}ns")
}

/// Applies every `{{token}}` substitution the file uses. Never touches
/// ClickHouse's own single-brace `{shard}`/`{replica}` macros — they are
/// simply absent from double-brace matching.
///
/// `schema.sh` carries the same list as twenty-three `sed` expressions and two
/// `awk` steps;
/// `the_script_renders_exactly_what_this_crate_renders` holds the two
/// together.
pub(crate) fn substitute_tokens(tmpl: &str, ctx: &RenderCtx) -> String {
    let on_cluster = match &ctx.cluster {
        Some(name) => format!(" ON CLUSTER '{}'", escape_literal(name)),
        None => String::new(),
    };
    let cluster_name = ctx.cluster.clone().unwrap_or_default();
    let log_rollup_ns = ctx.log_rollup.as_nanos().to_string();
    let storage_policy = match &ctx.storage_policy {
        Some(policy) => format!(", storage_policy = '{}'", escape_literal(policy)),
        None => String::new(),
    };
    // Issue #743 part A: the tables that never move are on the server's
    // `default` policy once a cold volume is set, so nothing can move them.
    let storage_policy_local = match &ctx.cold_volume {
        Some(_) => String::new(),
        None => storage_policy.clone(),
    };
    let days = |d: Option<u32>| d.map(|d| d.to_string()).unwrap_or_default();

    tmpl.replace("{{db}}", &ctx.db)
        .replace("{{on_cluster}}", &on_cluster)
        .replace("{{cluster}}", &cluster_name)
        .replace("{{dist_suffix}}", &ctx.dist_suffix)
        .replace("{{route_suffix}}", route_suffix(ctx))
        .replace("{{retention_days}}", &ctx.retention_days.to_string())
        .replace("{{log_rollup_suffix}}", &rollup_suffix(ctx.log_rollup))
        .replace("{{log_rollup_ns}}", &log_rollup_ns)
        .replace(
            "{{metrics_landing_retention_hours}}",
            &ctx.metrics_landing_retention_hours.to_string(),
        )
        .replace(
            "{{log_landing_retention_hours}}",
            &ctx.log_landing_retention_hours.to_string(),
        )
        .replace(
            "{{trace_landing_retention_hours}}",
            &ctx.trace_landing_retention_hours.to_string(),
        )
        .replace(
            "{{metrics_dedup_window}}",
            &ctx.metrics_dedup_window.to_string(),
        )
        .replace("{{log_dedup_window}}", &ctx.log_dedup_window.to_string())
        .replace(
            "{{trace_dedup_window}}",
            &ctx.trace_dedup_window.to_string(),
        )
        .replace(
            "{{dedup_window_seconds}}",
            &crate::checks::DEDUP_WINDOW_SECONDS.to_string(),
        )
        .replace(
            "{{graph_job_max_rows}}",
            &ctx.graph_job_max_rows.to_string(),
        )
        .replace("{{storage_policy}}", &storage_policy)
        .replace("{{storage_policy_local}}", &storage_policy_local)
        .replace(
            "{{cold_volume}}",
            &escape_literal(ctx.cold_volume.as_deref().unwrap_or("")),
        )
        .replace("{{metrics_hot_days}}", &days(ctx.metrics_hot_days))
        .replace("{{logs_hot_days}}", &days(ctx.logs_hot_days))
        .replace("{{traces_hot_days}}", &days(ctx.traces_hot_days))
        .replace(
            "{{metrics_catalogue_retention_days}}",
            &ctx.metrics_catalogue_retention_days().to_string(),
        )
        .replace("{{trace_attr_indexes}}", &trace_attr_indexes(ctx))
}

/// `{{trace_attr_indexes}}` (issue #595 part 2): for the `n`th configured
/// attribute, `,` then `INDEX idx_attr_<n> <expression> TYPE
/// bloom_filter(0.01) GRANULARITY 1` on its own line; nothing for none.
fn trace_attr_indexes(ctx: &RenderCtx) -> String {
    let mut out = String::new();
    for (i, item) in ctx.trace_indexed_attributes.iter().enumerate() {
        let expr = match (item.strip_prefix("span."), item.strip_prefix("event.")) {
            (Some(key), _) => pulsus_clickhouse::json_column::span_attr_index_expr(key),
            (None, Some(key)) => pulsus_clickhouse::json_column::event_attr_index_expr(key),
            (None, None) => panic!("an unvalidated indexed attribute: {item:?}"),
        };
        out.push_str(&format!(
            ",\n    INDEX idx_attr_{} {expr} TYPE bloom_filter(0.01) GRANULARITY 1",
            i + 1
        ));
    }
    out
}

/// `PULSUS_ROLLUP_LEVELS` (issue #18): `text` validated against
/// `landing_hours` (`PULSUS_METRICS_LANDING_RETENTION_HOURS`), the levels in
/// seconds, smallest first. An empty `text` is no levels. Each rule is
/// checked in turn and the first that fails is the message, without the
/// `PULSUS_ROLLUP_LEVELS: ` prefix `schema/schema.sh` prints before the
/// same message.
pub fn parse_rollup_levels(text: &str, landing_hours: u32) -> Result<Vec<u32>, String> {
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let items: Vec<&str> = text.split(',').collect();
    if items.iter().any(|item| item.is_empty()) {
        return Err(format!("an empty item in '{text}'"));
    }
    let mut levels = Vec::with_capacity(items.len());
    for item in &items {
        let whole =
            item.len() <= 5 && !item.starts_with('0') && item.bytes().all(|b| b.is_ascii_digit());
        match item.parse::<u32>() {
            Ok(v) if whole && v <= 86_400 => levels.push(v),
            _ => {
                return Err(format!(
                    "{item} is not a whole number of seconds from 1 to 86400"
                ));
            }
        }
    }
    if let Some(v) = levels.iter().find(|v| 86_400 % **v != 0) {
        return Err(format!("{v} does not divide a day (86400)"));
    }
    if let Some(v) = levels.iter().find(|v| **v < 60 && 60 % **v != 0) {
        return Err(format!("{v} is below 60 and does not divide 60"));
    }
    if let Some(w) = levels.windows(2).find(|w| w[1] <= w[0]) {
        return Err(format!("{} then {}: the levels must increase", w[0], w[1]));
    }
    if let Some(w) = levels.windows(2).find(|w| w[1] % w[0] != 0) {
        return Err(format!("{} is not a multiple of {}", w[1], w[0]));
    }
    if levels.len() > 8 {
        return Err("more than 8 levels".to_string());
    }
    let p = levels[0].max(60);
    if 2 * u64::from(p) + 870 > u64::from(landing_hours) * 3600 {
        return Err(format!(
            "the first level {} outlives metric_landing: 2 × {p} + 870 > {landing_hours} × 3600",
            levels[0]
        ));
    }
    Ok(levels)
}

/// `PULSUS_ROLLUP_RANGES` (issue #18 follow-up): `text` validated against
/// the validated `levels`, the switch points in seconds, one per level
/// after the first. With no levels the text is not read. An empty `text`
/// is an empty list. Each rule is checked in turn and the first that fails
/// is the message, without the `PULSUS_ROLLUP_RANGES: ` prefix
/// `schema/schema.sh` prints before the same message.
pub fn parse_rollup_ranges(text: &str, levels: &[u32]) -> Result<Vec<u32>, String> {
    if levels.is_empty() {
        return Ok(Vec::new());
    }
    let items: Vec<&str> = if text.is_empty() {
        Vec::new()
    } else {
        text.split(',').collect()
    };
    if items.iter().any(|item| item.is_empty()) {
        return Err(format!("an empty item in '{text}'"));
    }
    let mut ranges = Vec::with_capacity(items.len());
    for item in &items {
        let whole =
            item.len() <= 9 && !item.starts_with('0') && item.bytes().all(|b| b.is_ascii_digit());
        match item.parse::<u32>() {
            Ok(v) if whole => ranges.push(v),
            _ => {
                return Err(format!(
                    "{item} is not a whole number of seconds from 1 to 999999999"
                ));
            }
        }
    }
    if ranges.len() + 1 != levels.len() {
        return Err(format!(
            "{} ranges for {} levels: give one for each level after the first",
            ranges.len(),
            levels.len()
        ));
    }
    if let Some(w) = ranges.windows(2).find(|w| w[1] <= w[0]) {
        return Err(format!("{} then {}: the ranges must increase", w[0], w[1]));
    }
    if let Some((range, level)) = ranges
        .iter()
        .zip(&levels[1..])
        .find(|(range, level)| range < level)
    {
        return Err(format!("{range} is shorter than its level {level}"));
    }
    Ok(ranges)
}

/// `PULSUS_ROLLUP_COUNTER_BUCKETS` (issue #18 follow-up): a whole number
/// from 2 to 1000, with no leading zero. The message is the one
/// `schema/schema.sh` prints after `PULSUS_ROLLUP_COUNTER_BUCKETS: `.
pub fn parse_rollup_counter_buckets(text: &str) -> Result<u32, String> {
    let whole =
        text.len() <= 4 && !text.starts_with('0') && text.bytes().all(|b| b.is_ascii_digit());
    match text.parse::<u32>() {
        Ok(v) if whole && (2..=1000).contains(&v) => Ok(v),
        _ => Err(format!("{text} is not a whole number from 2 to 1000")),
    }
}

/// Every input `schema/schema.sh`'s tiering block reads (issue #743 part A).
/// A text is `None` when its variable is unset or empty, as the script
/// treats `${VAR:-}`.
///
/// `levels` and `ranges` are what the two parsers returned; a list text is
/// `None` only when its variable is unset; set and empty is `Some("")`,
/// which `level_items` refuses as an empty item. (The three family texts
/// keep part A's rule, `None` when unset or empty.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TieringInput<'a> {
    pub storage_policy: Option<&'a str>,
    pub cold_volume: Option<&'a str>,
    pub retention_days: u32,
    pub metrics_hot_days: Option<&'a str>,
    pub logs_hot_days: Option<&'a str>,
    pub traces_hot_days: Option<&'a str>,
    /// What `parse_rollup_levels` returned (issue #743 part B).
    pub levels: &'a [u32],
    /// What `parse_rollup_ranges` returned.
    pub ranges: &'a [u32],
    pub rollup_retention_days: Option<&'a str>,
    pub rollup_hot_days: Option<&'a str>,
}

/// The `RenderCtx` fields the tiering block decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tiering {
    pub cold_volume: Option<String>,
    pub metrics_hot_days: Option<u32>,
    pub logs_hot_days: Option<u32>,
    pub traces_hot_days: Option<u32>,
    pub rollup_retention_days: Vec<u32>,
    pub rollup_hot_days: Vec<u32>,
}

/// The tiering settings `schema/schema.sh` validates (issue #743), checked
/// in the script's order; the first rule that fails is the `Err`, the text
/// the script prints after `schema.sh: `. With no cold volume the `Ok` has
/// no volume and no move age; only `rollup_retention_days` can be set.
pub fn validate_tiering(input: &TieringInput<'_>) -> Result<Tiering, String> {
    if let Some(volume) = input.cold_volume {
        if input.storage_policy.is_none() {
            return Err("PULSUS_COLD_VOLUME is set and PULSUS_STORAGE_POLICY is not".to_string());
        }
        if !volume
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(format!(
                "cold_volume must be letters, digits and underscores: {volume}"
            ));
        }
    }
    let r = input.retention_days;
    let mut days = [None; 3];
    for (slot, (name, text)) in days.iter_mut().zip([
        ("PULSUS_METRICS_HOT_DAYS", input.metrics_hot_days),
        ("PULSUS_LOGS_HOT_DAYS", input.logs_hot_days),
        ("PULSUS_TRACES_HOT_DAYS", input.traces_hot_days),
    ]) {
        let text = match (input.cold_volume, text) {
            (Some(_), None) => {
                return Err(format!(
                    "{name} is not set; with PULSUS_COLD_VOLUME set each of \
                     PULSUS_METRICS_HOT_DAYS, PULSUS_LOGS_HOT_DAYS and PULSUS_TRACES_HOT_DAYS \
                     is required"
                ));
            }
            (None, Some(_)) => return Err(format!("{name} is set and PULSUS_COLD_VOLUME is not")),
            (None, None) => continue,
            (Some(_), Some(text)) => text,
        };
        let whole =
            text.len() <= 10 && !text.starts_with('0') && text.bytes().all(|b| b.is_ascii_digit());
        match text.parse::<u64>() {
            Ok(v) if whole && v <= u64::from(r) => *slot = Some(v as u32),
            _ => {
                return Err(format!(
                    "{name} must be a whole number of days from 1 to {r}: {text}"
                ));
            }
        }
    }
    let keep = match input.rollup_retention_days {
        None => Vec::new(),
        Some(text) => level_retention(text, input)?,
    };
    let hot = match input.rollup_hot_days {
        None => Vec::new(),
        Some(_) if input.cold_volume.is_none() => {
            return Err("PULSUS_ROLLUP_HOT_DAYS is set and PULSUS_COLD_VOLUME is not".to_string());
        }
        Some(text) => level_hot(text, input, &keep)?,
    };
    Ok(Tiering {
        cold_volume: input.cold_volume.map(str::to_string),
        metrics_hot_days: days[0],
        logs_hot_days: days[1],
        traces_hot_days: days[2],
        rollup_retention_days: keep,
        rollup_hot_days: hot,
    })
}

/// A level list's items (issue #743 part B): refused when one is empty or
/// their count is not the number of levels.
fn level_items<'t>(name: &str, text: &'t str, levels: &[u32]) -> Result<Vec<&'t str>, String> {
    let items: Vec<&str> = text.split(',').collect();
    if items.iter().any(|item| item.is_empty()) {
        return Err(format!("{name}: an empty item in '{text}'"));
    }
    if items.len() != levels.len() {
        return Err(format!(
            "{name}: {} items for {} levels",
            items.len(),
            levels.len()
        ));
    }
    Ok(items)
}

/// `text` as a whole number of days from `lo` to `hi`, without a leading
/// zero; `hi` is at most `u32::MAX`.
fn days_in(text: &str, lo: u64, hi: u64) -> Option<u32> {
    let whole =
        text.len() <= 10 && !text.starts_with('0') && text.bytes().all(|b| b.is_ascii_digit());
    match text.parse::<u64>() {
        Ok(v) if whole && (lo..=hi).contains(&v) => u32::try_from(v).ok(),
        _ => None,
    }
}

/// `PULSUS_ROLLUP_RETENTION_DAYS` (issue #743 part B): rules 4 a–4 d.
fn level_retention(text: &str, input: &TieringInput<'_>) -> Result<Vec<u32>, String> {
    const NAME: &str = "PULSUS_ROLLUP_RETENTION_DAYS";
    let items = level_items(NAME, text, input.levels)?;
    let r = input.retention_days;
    let mut keep = Vec::with_capacity(items.len());
    for (item, level) in items.iter().zip(input.levels) {
        match days_in(item, u64::from(r), 4_294_967_295) {
            Some(v) => keep.push(v),
            None => {
                return Err(format!(
                    "{NAME}: {item} for the {level}s level is not a whole number of days from {r} to 4294967295"
                ));
            }
        }
    }
    for ((days, level), from) in keep.iter().zip(input.levels).zip(input.ranges) {
        let need = from.div_ceil(86_400) + 1;
        if *days < need {
            return Err(format!(
                "{NAME}: {days} for the {level}s level is too short: the next level's switch point, {from} s, requires {need} retention days"
            ));
        }
    }
    Ok(keep)
}

/// `PULSUS_ROLLUP_HOT_DAYS` (issue #743 part B): rules 5 b–5 d; 5 a is the
/// caller's.
fn level_hot(text: &str, input: &TieringInput<'_>, keep: &[u32]) -> Result<Vec<u32>, String> {
    const NAME: &str = "PULSUS_ROLLUP_HOT_DAYS";
    let items = level_items(NAME, text, input.levels)?;
    let mut hot = Vec::with_capacity(items.len());
    for (i, (item, level)) in items.iter().zip(input.levels).enumerate() {
        let most = keep.get(i).copied().unwrap_or(input.retention_days);
        match days_in(item, 1, u64::from(most)) {
            Some(v) => hot.push(v),
            None => {
                return Err(format!(
                    "{NAME}: {item} for the {level}s level is not a whole number of days from 1 to {most}"
                ));
            }
        }
    }
    Ok(hot)
}

/// Which levels a line of the level block is kept for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LevelLine {
    /// `--@level `: every level.
    All,
    /// `--@level-first `: the first level, built from `metric_landing`.
    First,
    /// `--@level-next `: every later level, built from the level below.
    Next,
    /// `--@level-feeds `: every level whose next level is shorter than a
    /// day; a day-long run reads its source's day partition.
    Feeds,
    /// `--@level-cold `: every level that moves to the cold volume (issue #743 part B).
    Cold,
}

/// `line`'s level marker and the line after the marker and its one space,
/// or `None` for a line outside the level block.
fn level_marked(line: &str) -> Option<(LevelLine, &str)> {
    [
        ("--@level-cold ", LevelLine::Cold),
        ("--@level-first ", LevelLine::First),
        ("--@level-next ", LevelLine::Next),
        ("--@level-feeds ", LevelLine::Feeds),
        ("--@level ", LevelLine::All),
    ]
    .into_iter()
    .find_map(|(marker, kind)| line.strip_prefix(marker).map(|rest| (kind, rest)))
}

/// `v` seconds in the largest of `DAY`, `HOUR` and `MINUTE` that divides
/// it, else in `SECOND`.
fn timer_unit(v: u32) -> String {
    match v {
        v if v % 86_400 == 0 => format!("{} DAY", v / 86_400),
        v if v % 3_600 == 0 => format!("{} HOUR", v / 3_600),
        v if v % 60 == 0 => format!("{} MINUTE", v / 60),
        v => format!("{v} SECOND"),
    }
}

/// The level block of `sql` (issue #18), emitted once per level of
/// `levels`, smallest first, in place; every other line passes through.
///
/// Lines marked `--@level `, `--@level-first `, `--@level-next `,
/// `--@level-feeds ` or `--@level-cold ` are collected; at the first unmarked line after them,
/// and at the end, they are emitted for each level that keeps them, with
/// the marker and its one space removed and the level tokens replaced:
/// `{{level}}` (`<R>s`), `{{level_ms}}` (`<R>000`), `{{source}}` (the level
/// below's table), `{{parent_ms}}` (the next level's milliseconds),
/// `{{period_ms}}` (the first level's period, `max(R_1, 60)`, in
/// milliseconds), `{{timer}}`, `{{level_retention_days}}` (the level's
/// retention, or `{{retention_days}}` when the list is empty, which
/// [`substitute_tokens`] then resolves) and `{{level_hot_days}}` (the
/// level's move age; its `--@level-cold ` line is kept only for a level
/// that moves). The first level's timer is `EVERY p`;
/// level `i > 1` runs `EVERY R_i`, offset by `d_i mod R_i` seconds when
/// that is not 0, where `d_1 = ⌈150 ÷ p⌉ × p` and `d_i = d_{i−1} + 60`.
/// Every number is written as text, never computed in milliseconds, so the
/// script's `awk` prints the same digits;
/// `the_script_renders_exactly_what_this_crate_renders` holds the two
/// together. Output lines end in `\n`.
pub(crate) fn expand_levels(sql: &str, set: LevelSet<'_>) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut block: Vec<(LevelLine, &str)> = Vec::new();
    for line in sql.lines() {
        if let Some(marked) = level_marked(line) {
            block.push(marked);
            continue;
        }
        emit_levels(&mut out, &block, set);
        block.clear();
        out.push_str(line);
        out.push('\n');
    }
    emit_levels(&mut out, &block, set);
    out
}

/// The levels, their switch points, the counter bucket count, the
/// retention list (empty or one per level) and [`RenderCtx::level_moves`],
/// as [`expand_levels`] renders them.
pub(crate) type LevelSet<'a> = (&'a [u32], &'a [u32], u32, &'a [u32], &'a [Option<u32>]);

/// `block`, once per level, into `out`: [`expand_levels`]' rule.
fn emit_levels(out: &mut String, block: &[(LevelLine, &str)], set: LevelSet<'_>) {
    let (levels, ranges, counter_buckets, keep, moves) = set;
    let Some(&first) = levels.first() else {
        return;
    };
    if block.is_empty() {
        return;
    }
    let p = first.max(60);
    let mut d = 150u32.div_ceil(p) * p;
    let last = levels.len() - 1;
    for (i, &r) in levels.iter().enumerate() {
        let timer = if i == 0 {
            format!("EVERY {}", timer_unit(p))
        } else {
            d += 60;
            match d % r {
                0 => format!("EVERY {}", timer_unit(r)),
                off => format!("EVERY {} OFFSET {off} SECOND", timer_unit(r)),
            }
        };
        let level = format!("{r}s");
        let level_ms = format!("{r}000");
        let source = match i {
            0 => String::new(),
            _ => format!("metric_samples_{}s", levels[i - 1]),
        };
        let parent_ms = levels
            .get(i + 1)
            .map(|next| format!("{next}000"))
            .unwrap_or_default();
        let period_ms = format!("{p}000");
        // Written as text, as `{{level_ms}}`: 7,776,000,000 exceeds 32 bits.
        let from_ms = match i {
            0 => "0".to_string(),
            _ => format!(
                "{}000",
                ranges.get(i - 1).map(u32::to_string).unwrap_or_default()
            ),
        };
        let counter_buckets = counter_buckets.to_string();
        // Issue #743 part B: the level's own retention, else the raw
        // retention's token, which `substitute_tokens` resolves; and its
        // move age when it moves.
        let level_retention_days = keep
            .get(i)
            .map(u32::to_string)
            .unwrap_or_else(|| "{{retention_days}}".to_string());
        let level_hot = moves.get(i).copied().flatten();
        let level_hot_days = level_hot.map(|h| h.to_string()).unwrap_or_default();
        for (kind, rest) in block {
            let kept = match kind {
                LevelLine::All => true,
                LevelLine::First => i == 0,
                LevelLine::Next => i > 0,
                LevelLine::Feeds => i < last && levels[i + 1] < 86_400,
                LevelLine::Cold => level_hot.is_some(),
            };
            if !kept {
                continue;
            }
            let line = rest
                .replace("{{level}}", &level)
                .replace("{{level_ms}}", &level_ms)
                .replace("{{source}}", &source)
                .replace("{{parent_ms}}", &parent_ms)
                .replace("{{period_ms}}", &period_ms)
                .replace("{{from_ms}}", &from_ms)
                .replace("{{counter_buckets}}", &counter_buckets)
                .replace("{{timer}}", &timer)
                .replace("{{level_retention_days}}", &level_retention_days)
                .replace("{{level_hot_days}}", &level_hot_days);
            out.push_str(&line);
            out.push('\n');
        }
    }
}

/// The suffix a materialized view's `TO` clause carries when its target is
/// the **routing** table on a cluster and the local table on a single node.
///
/// **Rendered from the same field that renders the engine**, which is the
/// rule `{{on_cluster}}` and `{{dedup_window_setting}}` already follow. It
/// cannot be `{{dist_suffix}}`: that token renders the configured suffix
/// unconditionally, and every template using it today is
/// [`crate::catalog::Ddl::StaticClusterOnly`] or [`crate::catalog::Ddl::Dist`]
/// and so is never rendered without a cluster. A view is rendered in both
/// modes, so it needs the conditional form.
///
/// Two of the five trace views carry it: a trace's spans arrive from as many
/// senders as there are services in it, and the whole trace read design rests
/// on a trace being whole on one shard.
pub(crate) fn route_suffix(ctx: &RenderCtx) -> &str {
    match ctx.cluster {
        Some(_) => &ctx.dist_suffix,
        None => "",
    }
}

/// Escapes a single-quoted SQL string literal. Config-derived, not
/// adversarial input (operator-supplied cluster/db names), but cheap
/// insurance against a stray `'` producing invalid DDL rather than a clear
/// syntax error.
fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// Renders a table/view *name* template (may contain
/// `{{log_rollup_suffix}}`, never `{{on_cluster}}`).
pub fn render_name(name_tmpl: &str, ctx: &RenderCtx) -> String {
    substitute_tokens(name_tmpl, ctx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> RenderCtx {
        RenderCtx {
            db: "pulsus".to_string(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: Duration::from_secs(5),
            metrics_landing_retention_hours: 6,
            metrics_dedup_window: 10_000,
            log_landing_retention_hours: 6,
            log_dedup_window: 10_000,
            trace_landing_retention_hours: 6,
            trace_dedup_window: 10_000,
            trace_indexed_attributes: Vec::new(),
            graph_job_max_rows: GRAPH_JOB_MAX_ROWS,
            rollup_levels: DEFAULT_ROLLUP_LEVELS.to_vec(),
            rollup_ranges: DEFAULT_ROLLUP_RANGES.to_vec(),
            rollup_counter_buckets: DEFAULT_ROLLUP_COUNTER_BUCKETS,
            cold_volume: None,
            metrics_hot_days: None,
            logs_hot_days: None,
            traces_hot_days: None,
            rollup_retention_days: Vec::new(),
            rollup_hot_days: Vec::new(),
        }
    }

    #[test]
    fn substitute_tokens_replaces_db_and_leaves_ch_macros_verbatim() {
        let tmpl = "CREATE TABLE {{db}}.x{{on_cluster}} ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.x', '{replica}')";
        let out = substitute_tokens(tmpl, &ctx());
        assert!(out.contains("pulsus.x"));
        assert!(out.contains("{shard}"));
        assert!(out.contains("{replica}"));
        assert!(!out.contains("{{"));
    }

    /// The seconds window renders the one constant that carries it, so the
    /// relation a case asserts and the statement text a deployment receives
    /// cannot disagree.
    #[test]
    fn the_seconds_window_token_renders_the_constant() {
        assert_eq!(
            substitute_tokens("SETTING x = {{dedup_window_seconds}};", &ctx()),
            format!("SETTING x = {};", crate::checks::DEDUP_WINDOW_SECONDS)
        );
    }

    /// The two logs landing tokens render the context's own values, so a
    /// statement hard-coding either passes at the default and fails here.
    #[test]
    fn the_log_landing_tokens_render_the_configured_values() {
        let ctx = RenderCtx {
            log_landing_retention_hours: 24,
            log_dedup_window: 5_000,
            ..ctx()
        };
        assert_eq!(
            substitute_tokens("{{log_landing_retention_hours}}/{{log_dedup_window}}", &ctx),
            "24/5000"
        );
    }

    /// The two trace landing tokens render the context's own values, so a
    /// statement hard-coding either passes at the default and fails here.
    #[test]
    fn the_trace_landing_tokens_render_the_configured_values() {
        let ctx = RenderCtx {
            trace_landing_retention_hours: 24,
            trace_dedup_window: 5_000,
            ..ctx()
        };
        assert_eq!(
            substitute_tokens(
                "{{trace_landing_retention_hours}}/{{trace_dedup_window}}",
                &ctx
            ),
            "24/5000"
        );
    }

    /// `{{route_suffix}}` follows the cluster, not the suffix: it renders
    /// the configured suffix on a cluster and the empty string without one,
    /// where `{{dist_suffix}}` renders the suffix in both modes.
    #[test]
    fn the_route_suffix_token_follows_the_cluster() {
        const TMPL: &str = "TO {{db}}.spans{{route_suffix}}";
        assert_eq!(substitute_tokens(TMPL, &ctx()), "TO pulsus.spans");
        let clustered = RenderCtx {
            cluster: Some("prod".to_string()),
            ..ctx()
        };
        assert_eq!(substitute_tokens(TMPL, &clustered), "TO pulsus.spans_dist");
        let renamed = RenderCtx {
            cluster: Some("prod".to_string()),
            dist_suffix: "_routed".to_string(),
            ..ctx()
        };
        assert_eq!(substitute_tokens(TMPL, &renamed), "TO pulsus.spans_routed");
    }

    #[test]
    fn on_cluster_is_empty_when_no_cluster_configured() {
        let out = substitute_tokens("t{{on_cluster}}", &ctx());
        assert_eq!(out, "t");
    }

    #[test]
    fn on_cluster_renders_quoted_cluster_name() {
        let mut c = ctx();
        c.cluster = Some("prod".to_string());
        let out = substitute_tokens("t{{on_cluster}}", &c);
        assert_eq!(out, "t ON CLUSTER 'prod'");
    }

    /// A level block of every marker, between two ordinary lines. The
    /// `--@level --@cluster` line keeps its mode marker for the later pass,
    /// and the three spaces after `--@level` keep two of them.
    const LEVEL_BLOCK: &str = "head\n\
        --@level A {{level}} {{level_ms}} {{timer}}\n\
        --@level --@cluster C {{level}}\n\
        --@level   indented\n\
        --@level-first F {{period_ms}}\n\
        --@level-next N {{source}}\n\
        --@level-feeds P {{parent_ms}}\n\
        tail\n";

    /// Issue #18: no levels, no block.
    #[test]
    fn no_levels_emit_nothing_for_the_block() {
        assert_eq!(
            expand_levels(LEVEL_BLOCK, (&[], &[], 5, &[], &[])),
            "head\ntail\n"
        );
    }

    /// Issue #18: one level is first and last, so it has no `--@level-next`
    /// and no `--@level-feeds` line.
    #[test]
    fn one_level_is_first_and_feeds_nothing() {
        assert_eq!(
            expand_levels(LEVEL_BLOCK, (&[60], &[], 5, &[], &[])),
            "head\n\
             A 60s 60000 EVERY 1 MINUTE\n\
             --@cluster C 60s\n  indented\n\
             F 60000\n\
             tail\n"
        );
    }

    /// Issue #18: the levels are emitted smallest first, each with its own
    /// tokens. With `[5, 60]` the period is 60 s, `d_1` 180 s and `d_2` 240
    /// s, and `240 mod 60 = 0`, so the 60 s level has no `OFFSET`.
    #[test]
    fn a_level_below_a_minute_runs_every_minute_and_the_next_has_no_offset() {
        assert_eq!(
            expand_levels(LEVEL_BLOCK, (&[5, 60], &[3600], 5, &[], &[])),
            "head\n\
             A 5s 5000 EVERY 1 MINUTE\n\
             --@cluster C 5s\n  indented\n\
             F 60000\n\
             P 60000\n\
             A 60s 60000 EVERY 1 MINUTE\n\
             --@cluster C 60s\n  indented\n\
             N metric_samples_5s\n\
             tail\n"
        );
    }

    /// Issue #18: `[720, 86400]` has period 720 s, `d_1` 720 s and `d_2`
    /// 780 s.
    #[test]
    fn the_timers_follow_the_level_sizes() {
        let out = expand_levels(LEVEL_BLOCK, (&[720, 86400], &[86400], 5, &[], &[]));
        assert!(out.contains("A 720s 720000 EVERY 12 MINUTE\n"), "{out}");
        assert!(
            out.contains("A 86400s 86400000 EVERY 1 DAY OFFSET 780 SECOND\n"),
            "{out}"
        );
        // The next level is a whole day, so the 720 s level feeds no
        // projection.
        assert!(out.contains("F 720000\nA 86400s"), "{out}");
        assert!(!out.contains("\nP "), "{out}");
        assert!(out.contains("N metric_samples_720s\ntail\n"), "{out}");
    }

    /// Issue #18: the lists `parse_rollup_levels` refuses, each with the
    /// message of the first rule it breaks, and the lists it accepts.
    #[test]
    fn parse_rollup_levels_refuses_and_accepts_as_documented() {
        for (text, hours, message) in [
            ("60,45", 6, "45 is below 60 and does not divide 60"),
            ("60,90", 6, "90 is not a multiple of 60"),
            ("7,60", 6, "7 does not divide a day (86400)"),
            ("60,60", 6, "60 then 60: the levels must increase"),
            (
                "060",
                6,
                "060 is not a whole number of seconds from 1 to 86400",
            ),
            ("60,,720", 6, "an empty item in '60,,720'"),
            (",60", 6, "an empty item in ',60'"),
            ("25,60", 6, "25 is below 60 and does not divide 60"),
            ("1,2,6,12,60,120,240,720,1440", 6, "more than 8 levels"),
            ("0", 6, "0 is not a whole number of seconds from 1 to 86400"),
            (
                "90000",
                6,
                "90000 is not a whole number of seconds from 1 to 86400",
            ),
            (
                "14400,86400",
                6,
                "the first level 14400 outlives metric_landing: 2 × 14400 + 870 > 6 × 3600",
            ),
        ] {
            assert_eq!(
                parse_rollup_levels(text, hours),
                Err(message.to_string()),
                "{text:?}"
            );
        }
        for (text, want) in [
            ("60,720,7200,86400", vec![60, 720, 7200, 86400]),
            ("5,60", vec![5, 60]),
            ("720,86400", vec![720, 86400]),
            ("", vec![]),
        ] {
            assert_eq!(parse_rollup_levels(text, 6), Ok(want), "{text:?}");
        }
    }

    #[test]
    fn rollup_suffix_prefers_whole_seconds() {
        assert_eq!(rollup_suffix(Duration::from_secs(5)), "5s");
    }

    #[test]
    fn rollup_suffix_prefers_whole_minutes_over_seconds() {
        assert_eq!(rollup_suffix(Duration::from_secs(120)), "2m");
    }

    #[test]
    fn rollup_suffix_falls_back_to_millis() {
        assert_eq!(rollup_suffix(Duration::from_millis(500)), "500ms");
    }

    #[test]
    fn rollup_suffix_falls_back_to_nanos_for_sub_millisecond() {
        assert_eq!(rollup_suffix(Duration::from_nanos(123)), "123ns");
    }
}
