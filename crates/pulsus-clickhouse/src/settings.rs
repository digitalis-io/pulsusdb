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

    /// Issue #560: the two block-deduplication settings pinned on every
    /// insert into a source table whose derived tables are maintained by
    /// materialized view, so a repeated identical block is recognised by
    /// those tables' own deduplication windows whatever the server profile
    /// says.
    ///
    /// Two byte-identical blocks of samples or log entries can be two
    /// genuine pushes, so a signal whose repeat is a resend of one batch
    /// pairs these with a per-batch token rather than relying on the
    /// content: see [`Self::landing_insert`].
    pub fn deduplicate_through_views() -> Self {
        Self::new()
            .set("deduplicate_insert", "enable")
            .set("deduplicate_blocks_in_dependent_materialized_views", 1)
    }

    /// The settings every insert of one metrics landing block carries
    /// (issue #603): [`Self::deduplicate_through_views`], plus the token
    /// the writer minted for that block — repeated byte-identical on every
    /// resend, so a resend of a block the server already accepted stores
    /// nothing twice — plus **every limit that decides where one block
    /// ends**, so one push is never split into two.
    ///
    /// **Seven settings decide where a block ends or whether it is
    /// deduplicated, and only two of them count rows.** The server forms
    /// blocks as it parses the request body, and its own entry
    /// for `max_insert_block_size` states when one is emitted: "A block is
    /// emitted when either condition is met: Min thresholds (AND): Both
    /// min_insert_block_size_rows AND min_insert_block_size_bytes are
    /// reached — Max thresholds (OR): Either max_insert_block_size OR
    /// max_insert_block_size_bytes is reached". Three more names carry their
    /// own emit rule. Every default and quotation below is read from the
    /// server's own `system.settings` at 26.3.29.7 — the same catalogue the
    /// startup check reads, which is why each of the seven is in
    /// `pulsus_schema::REQUIRED_SERVER_NAMES` and none is sent on the
    /// strength of a name somebody remembered. The two lists are checked
    /// against each other, both ways, by
    /// `the_settings_read_back_at_startup_are_the_ones_the_landing_insert_sends`:
    ///
    /// | name | default | pinned to | what its own entry says |
    /// |---|---|---|---|
    /// | `max_insert_block_size` | 1048449 | `max_rows` | the maximum pair's row half; `max_insert_block_size_rows` carries `alias_for = max_insert_block_size` in the same catalogue, and `0` is not accepted (`NonZeroUInt64`) |
    /// | `max_insert_block_size_bytes` | 0 | `0` | "0 — setting does not participate in block formation" |
    /// | `min_insert_block_size_rows` | 1048449 | `max_rows` | the minimum pair's row half — its entry states the same emit rule, quoted above: the pair emits only when both halves are reached |
    /// | `min_insert_block_size_bytes` | 268402944 | `0` | "0 — setting does not participate in block formation" |
    /// | `input_format_max_block_size_bytes` | 0 | `0` | "Limits the size of the blocks formed during data parsing in input formats in bytes … 0 means no limit in bytes" |
    /// | `input_format_max_block_wait_ms` | 0 | `0` | "Limits the maximum time in milliseconds to wait before emitting a block during parsing in row-based input formats. 0 means no limit." |
    /// | `input_format_connection_handling` | 0 | `0` | "if the connection closes unexpectedly, any remaining data in the buffer will be parsed and processed instead of being treated as an error … Enabling this option disables parallel parsing and **makes deduplication impossible**" |
    ///
    /// So every byte limit and the wait are pinned to the value their own
    /// entry calls "no limit" or "does not participate", the connection
    /// handling to the value that leaves deduplication possible, and both
    /// row counts to the ceiling admission has already refused a larger push
    /// against. A row count is then the only thing left that can end a
    /// block, and no admitted push reaches it — under either reading of the
    /// minimum pair's AND, since a pair whose byte half does not participate
    /// either never fires or fires at the same row ceiling.
    ///
    /// **The last of the seven is not a splitting problem.** A setting that
    /// makes deduplication impossible defeats the token, which is the whole
    /// of the retry safety: a resend of a block the server already committed
    /// would store it twice. It is pinned for that reason and not because it
    /// divides a push.
    ///
    /// Pinned rather than inherited, exactly as the deduplication pair is:
    /// the defaults of four of them already sit where this pins them, but a
    /// server profile may set any, and then a push well inside the row
    /// ceiling becomes several blocks and a prefix of it can commit alone.
    ///
    /// **What pinning them off costs.** A deployment that set a byte limit
    /// to bound the memory one insert takes does not get it on this insert.
    /// What bounds this block instead is the per-push byte ceiling
    /// (`PULSUS_BATCH_BYTES`), which refuses the push whole before anything
    /// is queued. A deployment that enabled connection handling to salvage a
    /// broken upload's buffered rows does not get that on this insert
    /// either; a broken upload is instead a failed attempt the writer
    /// resends under the same token.
    ///
    /// **In the class, and closed somewhere else.** Each of these decides
    /// block formation or deduplication for some insert, and none needs a pin
    /// here:
    ///
    /// | name | why not pinned here |
    /// |---|---|
    /// | `async_insert` | every insert this client makes already pins it to `0` (issue #376, [`crate::ChClient::insert_settings_of`]); its default flipped to `1` at 26.2, and an asynchronous insert buffers one query's rows for a flush that combines queries |
    /// | `async_insert_deduplicate`, `async_insert_max_data_size`, `async_insert_poll_timeout_ms`, `wait_for_async_insert` | each takes effect only for an asynchronous insert, which the pin above rules out |
    /// | `insert_deduplicate` | `deduplicate_insert`'s own entry: "The setting overrides `insert_deduplicate` and `async_insert_deduplicate` settings", and this insert pins it to `enable` |
    /// | `deduplicate_insert_select` | its entry scopes it to `INSERT SELECT`; this is `INSERT … FORMAT RowBinary…` |
    /// | `input_format_parallel_parsing` | its entry: "Supported only for TabSeparated (TSV), TSKV, CSV and JSONEachRow formats" — not the `RowBinary` family this client writes |
    /// | `max_parsing_threads` | its entry scopes it to "input formats that support parallel parsing", which is the four above |
    /// | `min_insert_block_size_rows_for_materialized_views`, `min_insert_block_size_bytes_for_materialized_views`, `materialized_views_squash_parallel_inserts` | squashing combines blocks into bigger ones and never divides one, and a single-block insert gives each view one block to push. The part-per-thread case the third one's entry names needs `max_insert_threads`, whose own entry scopes it to `INSERT SELECT` |
    /// | `max_partitions_per_insert_block` | it refuses a block, it does not split one; every row of a push carries one `received_ms`, so the block lies in one partition |
    ///
    /// **How the set was derived, and what it does not close.** Two queries
    /// over the 1,550 rows of `system.settings` at 26.3.29.7: names or
    /// descriptions matching `block` or `dedup` (96 rows), and, of the rest,
    /// those matching `squash`, `flush`, `emit`, `buffer`, `batch`, `queue`,
    /// `parsing`, `parsed`, `split` or `duplicat` (a further 96). Every name
    /// in either answer whose own description concerns this statement is in
    /// one of the two tables above. What that cannot close is a setting that
    /// ends a block without using any of those words in its description;
    /// against that the only closure is the catalogue's own emit rule,
    /// quoted at the top, which enumerates the conditions for format
    /// parsing.
    ///
    /// **Nothing else divides one request into blocks.** The vendored client
    /// flushes its buffer to the socket every `MIN_CHUNK_SIZE` bytes
    /// (`vendor/clickhouse/src/insert.rs:18`), but those are transfer chunks
    /// of one `INSERT … FORMAT RowBinary…` request rather than blocks, and
    /// the condition that client states for an atomic insert is the row one
    /// alone (`vendor/clickhouse/README.md:157`).
    pub fn landing_insert(token: &str, max_rows: u64) -> Self {
        Self::deduplicate_through_views()
            .set("insert_deduplication_token", token)
            .set("max_insert_block_size", max_rows)
            .set("max_insert_block_size_bytes", 0)
            .set("input_format_max_block_size_bytes", 0)
            .set("min_insert_block_size_rows", max_rows)
            .set("min_insert_block_size_bytes", 0)
            .set("input_format_connection_handling", 0)
            .set("input_format_max_block_wait_ms", 0)
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

    /// The bytes this settings set's own allocations hold: the vector by
    /// **capacity**, and every key and value string by capacity.
    ///
    /// Introspection for one caller (same posture as [`Self::get`] and
    /// [`Self::entries`]): the metrics landing path charges its queue for
    /// everything a sealed block retains, one settings set included, and the
    /// case that prices a block walks it with this. A figure over the strings'
    /// lengths would understate what the allocator holds, which is what the
    /// charge has to cover.
    pub fn allocated_bytes(&self) -> u64 {
        self.0.capacity() as u64 * std::mem::size_of::<(String, String)>() as u64
            + self
                .0
                .iter()
                .map(|(k, v)| (k.capacity() + v.capacity()) as u64)
                .sum::<u64>()
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

    /// **Every setting that forms, ends or emits a block, and every setting
    /// that decides whether the insert is deduplicated, is pinned** (issue
    /// #603 code review rounds 7 and 8, finding 1). The set and the
    /// catalogue quote behind each value are in [`QuerySettings::
    /// landing_insert`]'s own table; this is that table as assertions, one
    /// per pin and exact, because a pin that is merely present can still
    /// carry the wrong value.
    ///
    /// Two of them decide a block by counting rows (`max_insert_block_size`
    /// and `min_insert_block_size_rows`, both at the ceiling admission has
    /// already refused a larger push against), three by counting bytes, one
    /// by elapsed time, and the last by whether a broken connection's
    /// buffered rows are processed at all — which is the one that disables
    /// deduplication, and so defeats the token rather than splitting a push.
    #[test]
    fn the_landing_insert_pins_every_limit_that_forms_a_block() {
        let s = QuerySettings::landing_insert("tok-1", 1_048_576);
        assert_eq!(
            s.get("max_insert_block_size"),
            Some("1048576"),
            "the row ceiling admission refuses against"
        );
        assert_eq!(
            s.get("max_insert_block_size_bytes"),
            Some("0"),
            "the byte limit on the blocks an insert forms must not participate"
        );
        assert_eq!(
            s.get("input_format_max_block_size_bytes"),
            Some("0"),
            "nor the byte limit on the blocks the input format forms"
        );
        assert_eq!(
            s.get("min_insert_block_size_rows"),
            Some("1048576"),
            "the minimum pair emits a block when BOTH are reached, so the row \
             half is pinned to the same ceiling as the maximum's"
        );
        assert_eq!(
            s.get("min_insert_block_size_bytes"),
            Some("0"),
            "and the byte half must not participate"
        );
        assert_eq!(
            s.get("input_format_connection_handling"),
            Some("0"),
            "the one that makes deduplication impossible when enabled, which \
             defeats the token rather than splitting the push"
        );
        assert_eq!(
            s.get("input_format_max_block_wait_ms"),
            Some("0"),
            "nor may a block be emitted because time passed"
        );
        assert_eq!(
            s.get("insert_deduplication_token"),
            Some("tok-1"),
            "the minted token is what makes a resend safe"
        );
        assert_eq!(
            s.get("deduplicate_insert"),
            Some("enable"),
            "it overrides insert_deduplicate and async_insert_deduplicate, so \
             one pin closes all three"
        );
        assert_eq!(
            s.get("deduplicate_blocks_in_dependent_materialized_views"),
            Some("1"),
            "and the views each check their own window"
        );
        assert_eq!(
            s.entries().count(),
            10,
            "the pinned set is closed: a setting added to it without a row in \
             landing_insert's table, and without its required-name entry, is \
             a name sent on somebody's memory"
        );
    }

    /// The row ceiling is a deployment's value, and **both** row limits
    /// follow it: a deployment at the floor of its range gets the same
    /// one-block guarantee as one at the default, because the pin that ends
    /// a block by counting rows is the same figure admission refuses
    /// against.
    #[test]
    fn both_row_limits_follow_the_deployments_own_ceiling() {
        for max_rows in [1_000u64, 1_048_576, 10_000_000] {
            let s = QuerySettings::landing_insert("tok-1", max_rows);
            let want = max_rows.to_string();
            assert_eq!(s.get("max_insert_block_size"), Some(want.as_str()));
            assert_eq!(s.get("min_insert_block_size_rows"), Some(want.as_str()));
        }
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
