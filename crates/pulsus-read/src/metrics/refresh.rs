//! The only ClickHouse-touching code in this module: the docs/architecture.md
//! §5.2 sweep (`SELECT fingerprint, metric_name, labels FROM metric_series
//! WHERE unix_milli >= floor(now - window) ORDER BY unix_milli DESC LIMIT 1
//! BY metric_name, fingerprint`), building a whole new
//! [`super::labels::CacheSnapshot`] and atomically swapping it into the
//! resident [`super::labels::LabelCache`]. [`spawn_refresh_loop`] runs this
//! on an interval in the self-healing shape of
//! `pulsus_server::serve::spawn_rotation_task` (not importable here — the
//! same *shape*, reimplemented against this crate's own types): a failed
//! sweep logs a warning and bumps `refresh_failures_total`, but **never**
//! clobbers the last good snapshot — a blanked cache would mass-false-empty
//! every in-window query.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChError, QuerySettings};
use pulsus_model::{Fingerprint, LabelSet, floor_to_activity_bucket};
use tokio::task::JoinHandle;

use super::labels::{CacheSnapshot, LabelCache};
use super::rows::SeriesRow;

/// Renders the §5.2 sweep SQL: `unix_milli >= floor(now - window)`, no
/// upper bound (the sweep always runs "as of now"). Pure so it is
/// snapshot-testable without a clock/DB.
fn sweep_sql(series_table: &str, lower_bound_ms: i64) -> String {
    format!(
        "SELECT fingerprint, metric_name, labels\nFROM {series_table}\nWHERE unix_milli >= {lower_bound_ms}\nORDER BY unix_milli DESC\nLIMIT 1 BY metric_name, fingerprint"
    )
}

/// Wall-clock now, milliseconds since the Unix epoch. `SystemTime::now()`
/// predating the Unix epoch is a broken-clock scenario, not one that
/// happens on any deployed system; it degrades to `0` rather than panicking
/// (mirrors `pulsus_write::ingest::http::now_unix_nanos`'s precedent).
/// `pub(crate)`: also used by [`super::labels::LabelCache::age_ms`] (the
/// `/metrics` age gauge, code-review round-2 fix), so the "what time is it"
/// primitive lives in exactly one place.
pub(crate) fn now_unix_ms() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

/// Parses PulsusDB's canonical flat label JSON (`{"key":"value", ...}`,
/// already sorted/normalized keys — docs/architecture.md §2.2) into a
/// [`LabelSet`], without pulling in a JSON crate dependency (mirrors
/// `logql::exec`'s private `parse_flat_labels` helper — duplicated here,
/// not shared, since that helper is module-private in a sibling module and
/// this crate's Cargo.toml deliberately adds no JSON crate for this single
/// use). Malformed input — which should never occur, this only ever reads
/// back what the writer produced — yields whatever pairs parsed so far
/// rather than panicking.
fn parse_canonical_labels(json: &str) -> LabelSet {
    LabelSet::from_verbatim(parse_flat_label_pairs(json))
}

fn parse_flat_label_pairs(json: &str) -> Vec<(String, String)> {
    let mut chars = json.chars().peekable();
    let mut out = Vec::new();
    while let Some(&c) = chars.peek() {
        chars.next();
        if c == '{' {
            break;
        }
    }
    loop {
        skip_ws(&mut chars);
        match chars.peek() {
            None | Some('}') => break,
            Some(',') => {
                chars.next();
                continue;
            }
            Some('"') => {}
            Some(_) => break,
        }
        let Some(key) = parse_json_string(&mut chars) else {
            break;
        };
        skip_ws(&mut chars);
        if chars.peek() == Some(&':') {
            chars.next();
        }
        skip_ws(&mut chars);
        let Some(value) = parse_json_string(&mut chars) else {
            break;
        };
        out.push((key, value));
    }
    out
}

fn skip_ws<I: Iterator<Item = char>>(chars: &mut std::iter::Peekable<I>) {
    while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
        chars.next();
    }
}

fn parse_json_string<I: Iterator<Item = char>>(
    chars: &mut std::iter::Peekable<I>,
) -> Option<String> {
    if chars.next() != Some('"') {
        return None;
    }
    let mut out = String::new();
    loop {
        match chars.next()? {
            '"' => return Some(out),
            '\\' => match chars.next()? {
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                '/' => out.push('/'),
                'n' => out.push('\n'),
                't' => out.push('\t'),
                'r' => out.push('\r'),
                'u' => {
                    let hex: String = (0..4).filter_map(|_| chars.next()).collect();
                    if let Ok(code) = u32::from_str_radix(&hex, 16)
                        && let Some(c) = char::from_u32(code)
                    {
                        out.push(c);
                    }
                }
                other => out.push(other),
            },
            c => out.push(c),
        }
    }
}

/// One sweep + swap: streams [`SeriesRow`]s, builds a whole new
/// [`CacheSnapshot`], swaps it into `cache.snapshot` under a brief write
/// lock (never held across an `.await`), and updates `cache.metrics`. On a
/// `ChError`, the last good snapshot is left untouched and
/// `refresh_failures_total` is bumped — this is the self-healing contract
/// [`LabelCache::refresh`] and [`spawn_refresh_loop`] both rely on.
pub(crate) async fn run_sweep(cache: &LabelCache) -> Result<(), ChError> {
    let now_ms = now_unix_ms();
    let lower_bound_ms =
        floor_to_activity_bucket(now_ms - cache.config.window_ms, cache.config.bucket_ms);
    let sql = sweep_sql(&cache.config.series_table, lower_bound_ms);

    let result = fetch_rows(cache, &sql).await;
    let rows = match result {
        Ok(rows) => rows,
        Err(err) => {
            cache.metrics.record_refresh_failure();
            return Err(err);
        }
    };

    let mut by_fingerprint: HashMap<Fingerprint, LabelSet> = HashMap::with_capacity(rows.len());
    let mut by_metric: HashMap<String, Vec<Fingerprint>> = HashMap::new();
    for row in rows {
        // A fingerprint is shared across metric names (`metric_fingerprint`
        // excludes `__name__`), so `by_metric` — not `by_fingerprint` — is
        // where identical-label-set series for different metrics stay
        // disjoint (architect plan edge case 7: never "dedup" across
        // metrics). `by_fingerprint` keying on the bare fingerprint is still
        // well-defined here: two rows sharing a fingerprint carry the exact
        // same label set (verbatim identity, not merely `==`-equal), so
        // whichever the sweep saw last simply overwrites with the same
        // content.
        by_fingerprint.insert(row.fingerprint, parse_canonical_labels(&row.labels));
        by_metric
            .entry(row.metric_name)
            .or_default()
            .push(row.fingerprint);
    }
    for fps in by_metric.values_mut() {
        fps.sort_unstable();
        fps.dedup();
    }

    let series_count = by_fingerprint.len() as u64;
    let generation = cache.current_snapshot().generation.saturating_add(1);
    let snapshot = CacheSnapshot {
        by_fingerprint,
        by_metric,
        // The sweep's own `now_ms` (code review round-2 fix: this — not a
        // wall-clock `Instant` captured at swap time — is the recency
        // anchor `resolve_over`'s upper-bound gate and `LabelCache::age_ms`
        // both measure against; see `CacheSnapshot::sweep_time_ms`'s doc
        // comment).
        sweep_time_ms: now_ms,
        covered_from_ms: lower_bound_ms,
        generation,
    };

    {
        let mut guard = match cache.snapshot.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = Arc::new(snapshot);
    }
    cache
        .metrics
        .record_refresh(series_count, cache.config.cache_max_series);
    Ok(())
}

async fn fetch_rows(cache: &LabelCache, sql: &str) -> Result<Vec<SeriesRow>, ChError> {
    let mut rows = Vec::new();
    let mut stream = cache
        .client
        .query_stream::<SeriesRow>(sql, &QuerySettings::new())
        .await?;
    while let Some(row) = stream.next().await {
        rows.push(row?);
    }
    Ok(rows)
}

/// Spawns the recurring refresh task: ticks every `ttl`, running one
/// [`run_sweep`] per tick. Mirrors `serve::spawn_rotation_task`'s
/// self-healing shape: a failed sweep only logs and bumps a failure
/// counter (already done inside `run_sweep`), it never aborts the loop or
/// panics — the next tick simply tries again. `tokio::time::interval`'s
/// first tick fires immediately, so the cache starts warming as soon as
/// this task is spawned, not after the first full `ttl` elapses.
pub fn spawn_refresh_loop(cache: Arc<LabelCache>, ttl: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(ttl);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(err) = run_sweep(&cache).await {
                tracing::warn!(
                    error = %err,
                    "label cache refresh sweep failed; serving the last good snapshot"
                );
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sweep_sql_renders_the_lower_bound_with_no_upper_bound() {
        let sql = sweep_sql("metric_series", 1_000);
        assert!(sql.contains("unix_milli >= 1000"));
        assert!(!sql.contains("unix_milli <="));
        assert!(sql.ends_with("LIMIT 1 BY metric_name, fingerprint"));
    }

    #[test]
    fn parse_flat_label_pairs_reads_simple_pairs() {
        let pairs = parse_flat_label_pairs(r#"{"env":"prod","team":"checkout"}"#);
        assert_eq!(
            pairs,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "checkout".to_string())
            ]
        );
    }

    #[test]
    fn parse_flat_label_pairs_handles_escaped_quotes_and_backslashes() {
        let pairs = parse_flat_label_pairs(r#"{"msg":"a\"b\\c"}"#);
        assert_eq!(pairs, vec![("msg".to_string(), "a\"b\\c".to_string())]);
    }

    #[test]
    fn parse_flat_label_pairs_of_empty_object_is_empty() {
        assert!(parse_flat_label_pairs("{}").is_empty());
    }

    #[test]
    fn parse_canonical_labels_round_trips_through_label_set() {
        let set = parse_canonical_labels(r#"{"job":"api","env":"prod"}"#);
        assert_eq!(set.get("job"), Some("api"));
        assert_eq!(set.get("env"), Some("prod"));
    }

    #[test]
    fn now_unix_ms_is_a_plausible_recent_timestamp() {
        // Sanity bound: some time after 2024-01-01 in milliseconds.
        assert!(now_unix_ms() > 1_700_000_000_000);
    }
}
