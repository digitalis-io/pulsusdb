//! Per-query (never per-connection) ClickHouse settings injection.
//!
//! Settings are applied via the `clickhouse` crate's per-request builder
//! (`Query::with_setting` / `Insert::with_setting`, sent as HTTP query
//! parameters) rather than concatenated into the SQL text: a `SETTINGS`
//! clause appended textually would collide with `CREATE TABLE ...
//! ENGINE = MergeTree ... SETTINGS index_granularity = ...`, which is
//! table-engine syntax, not query settings. Because settings travel with
//! the request rather than being `SET` on a session, a setting like
//! `optimize_skip_unused_shards = 1` chosen for one clustered read can
//! never leak into a later, unrelated query that reuses the same pooled
//! connection (edge case #2 — distributed-correctness risk of
//! session-scoped settings on a pooled connection).

use std::time::Duration;

/// Renders a [`Duration`] as the fractional-seconds string ClickHouse's
/// `max_execution_time` setting expects, shared by both the per-query
/// [`QuerySettings::with_max_execution_time`] and `insert_block`'s
/// server-side bound so the two render identically. Rounds up so a
/// sub-second remainder is not silently dropped to a stricter-than-requested
/// deadline.
pub(crate) fn max_execution_time_secs(d: Duration) -> String {
    let secs = d.as_secs_f64().max(0.001);
    format!("{secs:.3}")
}

/// An ordered list of `(key, value)` ClickHouse settings, applied to exactly
/// one statement.
#[derive(Clone, Default, Debug)]
pub struct QuerySettings(Vec<(String, String)>);

impl QuerySettings {
    /// Starts an empty settings set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets (or overrides, if already present) one ClickHouse setting.
    pub fn set(mut self, key: &str, val: impl ToString) -> Self {
        let val = val.to_string();
        if let Some(existing) = self.0.iter_mut().find(|(k, _)| k == key) {
            existing.1 = val;
        } else {
            self.0.push((key.to_string(), val));
        }
        self
    }

    /// The value of one setting, if present. Introspection only (tests /
    /// gate assertions); the client applies settings via
    /// [`Self::apply_to_query`], not through this accessor.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// docs/schemas.md §7 clustered-reader settings block, emitted exactly:
    /// `optimize_skip_unused_shards`, `optimize_distributed_group_by_sharding_key`,
    /// `distributed_aggregation_memory_efficient`, `prefer_localhost_replica`
    /// (all `1`), and `skip_unavailable_shards` per the caller-supplied flag
    /// (`PULSUS_SKIP_UNAVAILABLE_SHARDS`).
    pub fn clustered_reader(skip_unavailable_shards: bool) -> Self {
        Self::new()
            .set("optimize_skip_unused_shards", 1)
            .set("optimize_distributed_group_by_sharding_key", 1)
            .set("distributed_aggregation_memory_efficient", 1)
            .set("prefer_localhost_replica", 1)
            .set("skip_unavailable_shards", u8::from(skip_unavailable_shards))
    }

    /// Couples the client-side deadline to the server-side
    /// `max_execution_time` (edge case #4 — a query-timeout split-brain
    /// otherwise leaves the server running an abandoned query, or the
    /// client cancelling a query the server would have finished).
    pub fn with_max_execution_time(self, d: Duration) -> Self {
        self.set("max_execution_time", max_execution_time_secs(d))
    }

    /// Write-side quorum consistency (issue #114). Returns `self` unchanged
    /// when `quorum == 0` (quorum off — the default, byte-for-byte the
    /// pre-#114 insert): the `insert_quorum_parallel`/`insert_quorum_timeout`
    /// values are only meaningful alongside a non-zero quorum. When
    /// `quorum > 0` all three are emitted so behaviour is pinned regardless
    /// of the server default. `timeout` is rendered in **milliseconds**
    /// (`as_millis`) — ClickHouse's unit for `insert_quorum_timeout`.
    pub fn with_insert_quorum(self, quorum: u64, parallel: bool, timeout: Duration) -> Self {
        if quorum == 0 {
            return self;
        }
        self.set("insert_quorum", quorum)
            .set("insert_quorum_parallel", u8::from(parallel))
            .set("insert_quorum_timeout", timeout.as_millis())
    }

    /// Read-side sequential consistency (issue #114). Sets
    /// `select_sequential_consistency = 1` iff `enabled`; emits nothing when
    /// `false` (the default — byte-for-byte the pre-#114 select).
    pub fn with_select_sequential_consistency(self, enabled: bool) -> Self {
        if enabled {
            self.set("select_sequential_consistency", 1)
        } else {
            self
        }
    }

    /// Iterates the `(key, value)` pairs so a caller (e.g. `insert_block`)
    /// can apply them to an `Insert` builder, which has no typed settings
    /// helper of its own.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Public introspection twin of [`Self::iter`] (same posture as
    /// [`Self::get`]): lets a caller outside this crate compare its own
    /// settings' exact `(key, value)` entry set against another builder's
    /// — issue #35's `xtask` bench drift guard needs exactly this to prove
    /// its settings never diverge from production's without reaching into
    /// `pub(crate)` internals.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.iter()
    }

    /// Applies every `(key, value)` pair to a `clickhouse::query::Query`
    /// builder as per-request settings (sent as HTTP query parameters, not
    /// SQL text).
    pub(crate) fn apply_to_query(
        &self,
        mut q: clickhouse::query::Query,
    ) -> clickhouse::query::Query {
        for (k, v) in &self.0 {
            q = q.with_setting(k, v);
        }
        q
    }

    /// Renders the ` SETTINGS k=v, ...` SQL suffix this settings set would
    /// produce if it were textually inlined. Used only for introspection /
    /// tests — the client applies settings via [`Self::apply_to_query`],
    /// never by concatenating this into SQL text.
    #[cfg(test)]
    pub(crate) fn render_suffix(&self) -> String {
        if self.0.is_empty() {
            return String::new();
        }
        let body = self
            .0
            .iter()
            .map(|(k, v)| format!("{k} = {v}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" SETTINGS {body}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_settings_render_no_suffix() {
        assert_eq!(QuerySettings::new().render_suffix(), "");
    }

    #[test]
    fn set_renders_key_value_pairs() {
        let s = QuerySettings::new().set("max_threads", 4);
        assert_eq!(s.render_suffix(), " SETTINGS max_threads = 4");
    }

    /// Issue #35: `entries()` is the public introspection twin of the
    /// crate-private `iter()` — the bench drift guard's exact-entry-set
    /// equality check depends on this being complete and in insertion
    /// order.
    #[test]
    fn entries_exposes_every_key_value_pair_in_insertion_order() {
        let s = QuerySettings::new()
            .set("max_query_size", 8_388_608u64)
            .set("query_id", "abc");
        let got: Vec<(&str, &str)> = s.entries().collect();
        assert_eq!(
            got,
            vec![("max_query_size", "8388608"), ("query_id", "abc")]
        );
    }

    #[test]
    fn set_overrides_existing_key_rather_than_duplicating() {
        let s = QuerySettings::new()
            .set("max_threads", 4)
            .set("max_threads", 8);
        assert_eq!(s.render_suffix(), " SETTINGS max_threads = 8");
    }

    #[test]
    fn clustered_reader_emits_exactly_the_five_schemas_settings() {
        let s = QuerySettings::clustered_reader(true);
        assert_eq!(
            s.render_suffix(),
            " SETTINGS optimize_skip_unused_shards = 1, \
             optimize_distributed_group_by_sharding_key = 1, \
             distributed_aggregation_memory_efficient = 1, \
             prefer_localhost_replica = 1, skip_unavailable_shards = 1"
        );
    }

    #[test]
    fn clustered_reader_respects_skip_unavailable_shards_flag() {
        let s = QuerySettings::clustered_reader(false);
        assert!(s.render_suffix().ends_with("skip_unavailable_shards = 0"));
    }

    #[test]
    fn with_max_execution_time_renders_seconds() {
        let s = QuerySettings::new().with_max_execution_time(Duration::from_secs(30));
        assert_eq!(s.render_suffix(), " SETTINGS max_execution_time = 30.000");
    }

    /// AC1 (issue #114): an enabled quorum emits all three keys, with
    /// `insert_quorum_timeout` in milliseconds (`as_millis`); a zero quorum
    /// emits nothing (off = pre-#114 insert).
    #[test]
    fn with_insert_quorum_emits_the_trio_in_ms_and_nothing_when_off() {
        let s = QuerySettings::new().with_insert_quorum(2, false, Duration::from_secs(5));
        assert_eq!(
            s.render_suffix(),
            " SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 5000"
        );
        let off = QuerySettings::new().with_insert_quorum(0, true, Duration::from_secs(5));
        assert_eq!(off.render_suffix(), "");
    }

    /// AC2 (issue #114): sequential consistency emits `= 1` only when
    /// enabled; nothing when disabled (off = pre-#114 select).
    #[test]
    fn with_select_sequential_consistency_emits_one_only_when_enabled() {
        let on = QuerySettings::new().with_select_sequential_consistency(true);
        assert_eq!(
            on.render_suffix(),
            " SETTINGS select_sequential_consistency = 1"
        );
        let off = QuerySettings::new().with_select_sequential_consistency(false);
        assert_eq!(off.render_suffix(), "");
    }
}
