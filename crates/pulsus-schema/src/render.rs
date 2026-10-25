//! Token substitution for `schema/schema.sql`: explicit `{{token}}` string
//! replacement, eighteen names and nothing else. The level tokens of the
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
}

/// The rows one run of the service-graph job may read (issue #599), the
/// value `schema/schema.sh` renders: three 5-minute buckets of spans at
/// about 67,000 spans a second on one shard.
pub const GRAPH_JOB_MAX_ROWS: u64 = 60_000_000;

/// The rollup levels `schema/schema.sh` renders when `PULSUS_ROLLUP_LEVELS`
/// is unset (issue #18).
pub const DEFAULT_ROLLUP_LEVELS: &[u32] = &[60, 720, 7200, 86400];

impl RenderCtx {
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
/// `schema.sh` carries the same list as seventeen `sed` expressions and two
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

/// Which levels a line of the level block is kept for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LevelLine {
    /// `--@level `: every level.
    All,
    /// `--@level-first `: the first level, built from `metric_landing`.
    First,
    /// `--@level-next `: every later level, built from the level below.
    Next,
    /// `--@level-feeds `: every level but the last.
    Feeds,
}

/// `line`'s level marker and the line after the marker and its one space,
/// or `None` for a line outside the level block.
fn level_marked(line: &str) -> Option<(LevelLine, &str)> {
    [
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
/// Lines marked `--@level `, `--@level-first `, `--@level-next ` or
/// `--@level-feeds ` are collected; at the first unmarked line after them,
/// and at the end, they are emitted for each level that keeps them, with
/// the marker and its one space removed and the level tokens replaced:
/// `{{level}}` (`<R>s`), `{{level_ms}}` (`<R>000`), `{{source}}` (the level
/// below's table), `{{parent_ms}}` (the next level's milliseconds),
/// `{{period_ms}}` (the first level's period, `max(R_1, 60)`, in
/// milliseconds) and `{{timer}}`. The first level's timer is `EVERY p`;
/// level `i > 1` runs `EVERY R_i`, offset by `d_i mod R_i` seconds when
/// that is not 0, where `d_1 = ⌈150 ÷ p⌉ × p` and `d_i = d_{i−1} + 60`.
/// Every number is written as text, never computed in milliseconds, so the
/// script's `awk` prints the same digits;
/// `the_script_renders_exactly_what_this_crate_renders` holds the two
/// together. Output lines end in `\n`.
pub(crate) fn expand_levels(sql: &str, levels: &[u32]) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut block: Vec<(LevelLine, &str)> = Vec::new();
    for line in sql.lines() {
        if let Some(marked) = level_marked(line) {
            block.push(marked);
            continue;
        }
        emit_levels(&mut out, &block, levels);
        block.clear();
        out.push_str(line);
        out.push('\n');
    }
    emit_levels(&mut out, &block, levels);
    out
}

/// `block`, once per level, into `out`: [`expand_levels`]' rule.
fn emit_levels(out: &mut String, block: &[(LevelLine, &str)], levels: &[u32]) {
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
        for (kind, rest) in block {
            let kept = match kind {
                LevelLine::All => true,
                LevelLine::First => i == 0,
                LevelLine::Next => i > 0,
                LevelLine::Feeds => i < last,
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
                .replace("{{timer}}", &timer);
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
        assert_eq!(expand_levels(LEVEL_BLOCK, &[]), "head\ntail\n");
    }

    /// Issue #18: one level is first and last, so it has no `--@level-next`
    /// and no `--@level-feeds` line.
    #[test]
    fn one_level_is_first_and_feeds_nothing() {
        assert_eq!(
            expand_levels(LEVEL_BLOCK, &[60]),
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
            expand_levels(LEVEL_BLOCK, &[5, 60]),
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
        let out = expand_levels(LEVEL_BLOCK, &[720, 86400]);
        assert!(out.contains("A 720s 720000 EVERY 12 MINUTE\n"), "{out}");
        assert!(
            out.contains("A 86400s 86400000 EVERY 1 DAY OFFSET 780 SECOND\n"),
            "{out}"
        );
        assert!(out.contains("F 720000\nP 86400000\n"), "{out}");
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
