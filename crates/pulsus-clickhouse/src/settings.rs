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

/// `format_binary_max_object_size`, the server's own default on ClickHouse
/// 26.3.29.7 and, in its words, "The maximum allowed number of paths in a
/// single Object for JSON type RowBinary format".
///
/// **The decode gate and the pin carry this one constant**, so neither can
/// admit what the other refuses: a span whose stored paths would exceed it is
/// refused at decode with a `400`, where without the gate the push would be
/// admitted and the insert would fail after the block was sent.
pub const MAX_JSON_PATHS_PER_VALUE: u64 = 100_000;

/// `max_partitions_per_insert_block`, the server's own default on ClickHouse
/// 26.3.29.7. `spans`, `traces` and `resources` are partitioned by UTC day,
/// so one view's insert produces one part per distinct day in the push and
/// the server throws above this many.
///
/// **The admission gate and the pin carry this one constant**, for the reason
/// [`MAX_JSON_PATHS_PER_VALUE`] gives: a push whose spans fall on more dates
/// than this is refused `413` at admission rather than answered `200` with
/// its spans and resources absent from the new tables.
pub const MAX_PARTITIONS_PER_INSERT_BLOCK: u64 = 100;

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
    /// | `max_parsing_threads`, `min_chunk_bytes_for_parallel_parsing` | both belong to parallel parsing, which `input_format_parallel_parsing`'s entry supports for the four formats above and not for this one: the first is its thread count, the second what one thread takes |
    /// | `min_insert_block_size_rows_for_materialized_views`, `min_insert_block_size_bytes_for_materialized_views`, `materialized_views_squash_parallel_inserts` | squashing combines blocks into bigger ones and never divides one, and a single-block insert gives each view one block to push. The part-per-thread case the third one's entry names needs `max_insert_threads`, whose own entry scopes it to `INSERT SELECT` |
    /// | `max_partitions_per_insert_block` | it refuses a block, it does not split one; every row of a push carries one `received_ms`, so the block lies in one partition |
    ///
    /// **How the set was derived.** Two searches over the 1,550 rows of
    /// `system.settings` at 26.3.29.7, both here literally so their counts can
    /// be re-run — swap `count()` for `name` to list what each returns:
    ///
    /// ```sql
    /// -- A, 96 rows: the class by its own words.
    /// WITH lower(concat(name, ' ', description)) AS x
    /// SELECT count() FROM system.settings
    /// WHERE position(x, 'block') > 0 OR position(x, 'dedup') > 0
    ///
    /// -- B, 104 rows: of the rest, the neighbouring words.
    /// WITH lower(concat(name, ' ', description)) AS x
    /// SELECT count() FROM system.settings
    /// WHERE NOT (position(x, 'block') > 0 OR position(x, 'dedup') > 0)
    ///   AND arrayExists(w -> position(x, w) > 0,
    ///       ['squash', 'flush', 'emit', 'buffer', 'batch', 'queue',
    ///        'parsing', 'parsed', 'split', 'duplicat'])
    /// ```
    ///
    /// Every name in A whose own description concerns this statement is in one
    /// of the two tables above. **Every name in B is accounted for here**, in
    /// nine groups that do not overlap and add to 104, so nothing rests on a
    /// figure a reader cannot re-derive:
    ///
    /// 1. **Already in the second table above (7).** `async_insert`,
    ///    `async_insert_max_data_size`, `async_insert_poll_timeout_ms`,
    ///    `input_format_parallel_parsing`,
    ///    `materialized_views_squash_parallel_inserts`, `max_parsing_threads`,
    ///    `min_chunk_bytes_for_parallel_parsing`.
    ///
    /// 2. **An input format's own rule, or a type- or schema-inference rule
    ///    (27).** Each decides how the request's bytes become values, never when
    ///    a block ends; most of them name a format this insert does not use.
    ///    `cast_string_to_date_time_mode`, `date_time_input_format`,
    ///    `enable_parsing_to_custom_serialization`, `format_schema`,
    ///    `input_format_json_defaults_for_missing_elements_in_named_tuple`,
    ///    `input_format_json_ignore_unnecessary_fields`,
    ///    `input_format_json_read_arrays_as_strings`,
    ///    `input_format_json_read_bools_as_numbers`,
    ///    `input_format_json_read_bools_as_strings`,
    ///    `input_format_json_read_numbers_as_strings`,
    ///    `input_format_json_read_objects_as_strings`,
    ///    `input_format_orc_row_batch_size`,
    ///    `input_format_parquet_enable_json_parsing`,
    ///    `input_format_parquet_enable_row_group_prefetch`,
    ///    `input_format_try_infer_dates`, `input_format_try_infer_datetimes`,
    ///    `input_format_values_accurate_types_of_literals`,
    ///    `input_format_values_deduce_templates_of_expressions`,
    ///    `input_format_values_interpret_expressions`,
    ///    `json_type_escape_dots_in_keys`,
    ///    `max_dynamic_subcolumns_in_json_type_parsing`, `precise_float_parsing`,
    ///    `schema_inference_make_columns_nullable`, `session_timezone`,
    ///    `type_json_allow_duplicated_key_with_literal_and_nested_object`,
    ///    `type_json_skip_duplicated_paths`,
    ///    `type_json_use_partial_match_to_skip_paths_by_regexp`.
    ///
    /// 3. **The SQL parser's own limits, or a function's own behaviour (8).**
    ///    `formatdatetime_parsedatetime_m_is_month_name`, `max_ast_depth`,
    ///    `max_ast_elements`, `max_parser_backtracks`, `max_query_size`,
    ///    `parsedatetime_e_requires_space_padding`,
    ///    `parsedatetime_parse_without_leading_zeros`,
    ///    `splitby_max_substrings_includes_remaining_string`.
    ///
    /// 4. **The read path, or a `SELECT` rewrite (17).**
    ///    `apply_prewhere_after_final`,
    ///    `cluster_table_function_buckets_batch_size`,
    ///    `correlated_subqueries_use_in_memory_buffer`, `enable_vertical_final`,
    ///    `external_storage_max_read_bytes`, `external_storage_max_read_rows`,
    ///    `merge_tree_compact_parts_min_granules_to_multibuffer_read`,
    ///    `merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability`,
    ///    `optimize_duplicate_order_by_and_distinct`,
    ///    `parallel_replicas_custom_key`,
    ///    `parallel_replicas_custom_key_range_lower`,
    ///    `parallel_replicas_custom_key_range_upper`, `query_plan_split_filter`,
    ///    `read_in_order_use_buffering`,
    ///    `split_intersecting_parts_ranges_into_layers_final`,
    ///    `split_parts_ranges_into_intersecting_and_non_intersecting_final`,
    ///    `union_default_mode`.
    ///
    /// 5. **A buffer or a limit on a file or network channel (18)** — a cache,
    ///    object storage, an archive, a temporary file, the HTTP transport. None
    ///    of them forms a block. `archive_adaptive_buffer_max_size_bytes`,
    ///    `azure_list_object_keys_size`,
    ///    `distributed_cache_prefer_bigger_buffer_size`,
    ///    `filesystem_cache_allow_background_download`,
    ///    `filesystem_cache_prefer_bigger_buffer_size`,
    ///    `filesystem_cache_segments_batch_size`, `http_headers_read_timeout`,
    ///    `http_max_multipart_form_data_size`, `http_response_buffer_size`,
    ///    `http_wait_end_of_query`, `max_download_buffer_size`,
    ///    `max_read_buffer_size`, `max_read_buffer_size_local_fs`,
    ///    `max_read_buffer_size_remote_fs`, `prefetch_buffer_size`,
    ///    `s3_list_object_keys_size`, `temporary_files_buffer_size`,
    ///    `write_through_distributed_cache_buffer_size`.
    ///
    /// 6. **An output format (3):** the response, nothing on the way in.
    ///    `output_format_parquet_batch_size`,
    ///    `output_format_parquet_bloom_filter_flush_threshold_bytes`,
    ///    `output_format_sql_insert_max_batch_size`.
    ///
    /// 7. **Another table engine's, another storage engine's, or a background
    ///    subsystem of the server's (18).**
    ///    `allow_experimental_object_storage_queue_hive_partitioning`,
    ///    `allow_experimental_s3queue`,
    ///    `background_buffer_flush_schedule_pool_size`,
    ///    `backup_restore_batch_size_for_keeper_multi`,
    ///    `backup_restore_batch_size_for_keeper_multiread`,
    ///    `database_replicated_initial_query_timeout_sec`,
    ///    `distributed_background_insert_batch`,
    ///    `distributed_background_insert_split_batch_on_failure`,
    ///    `distributed_directory_monitor_batch_inserts`,
    ///    `distributed_directory_monitor_split_batch_on_failure`,
    ///    `mysql_max_rows_to_insert`, `s3queue_allow_experimental_sharded_mode`,
    ///    `s3queue_default_zookeeper_path`,
    ///    `s3queue_enable_logging_to_s3queue_log`,
    ///    `s3queue_keeper_fault_injection_probability`,
    ///    `s3queue_migrate_old_metadata_to_buckets`,
    ///    `stream_like_engine_allow_direct_select`,
    ///    `stream_like_engine_insert_queue`.
    ///
    /// 8. **After the block rather than where it ends (1):** it delays the flush
    ///    of the part the block became.
    ///    `max_insert_delayed_streams_for_parallel_write`.
    ///
    /// 9. **Diagnostics, the client, or query admission (5).**
    ///    `apply_settings_from_server`, `jemalloc_enable_profiler`,
    ///    `log_comment`, `queue_max_wait_ms`, `trace_profile_events_list`.
    ///
    /// **What none of this closes** is a setting that ends a block without
    /// using any of those words in its description; against that the only
    /// closure is the catalogue's own emit rule, quoted at the top, which
    /// enumerates the conditions for format parsing.
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

    /// The settings every insert of one traces landing block carries, and
    /// every statement `pulsusdb rebuild-traces` issues (issues #584 to
    /// #586): [`Self::landing_insert`] whole, plus **nineteen further pins
    /// of seven classes the metrics set did not need**.
    ///
    /// The rule behind the set, stated once so it does not grow without
    /// bound: **pin what a profile could use to make an answer wrong or a
    /// loss silent; disclose what it could only use to make a push fail
    /// loudly.** Every value below is this server's own default, read out of
    /// `system.settings` on ClickHouse 26.3.29.7 with
    /// `SELECT name, value, default FROM system.settings WHERE name IN (…)`,
    /// so pinning changes nothing where a deployment keeps them. Each is in
    /// `pulsus_schema::REQUIRED_SERVER_NAMES` and none is sent on the
    /// strength of a name somebody remembered; the two lists are derived
    /// from each other, both ways, by
    /// `the_settings_read_back_at_startup_are_the_ones_the_trace_insert_sends`.
    ///
    /// | setting | pinned to | class, and why it is in the set |
    /// |---|---|---|
    /// | `input_format_binary_read_json_as_string` | `0` | **how the block's bytes are read.** At `1` the server reads a `JSON` column from RowBinary as a JSON *string*, and the writer sends the native binary form. A profile that set it would make every trace push fail, or store the encoder's bytes as text |
    /// | `format_binary_max_object_size` | `100000` | the same class: it bounds the paths one JSON value may carry in RowBinary. A profile that lowered it would refuse spans the decode gate admitted, after the block was sent. The decode gate and this pin carry the **same constant**, so neither can admit what the other refuses |
    /// | `max_partitions_per_insert_block` | `100` | **a limit that refuses a block rather than dividing it.** `spans`, `traces` and `resources` are partitioned by UTC day, so a view's insert touches one part per day the push's spans fall in. A profile that lowered this to 1 would fail every push that straddles midnight |
    /// | `throw_on_max_partitions_per_insert_block` | `1` | the same limit's other half. At `0` the server does not store part of the block: the throw in `MergeTreeDataWriter::buildScatterSelector` sits inside the row loop behind `&& throw_on_limit`, so with it false the loop completes the selector over every row and the whole block is accepted, one part per partition, with a warning. **What `0` costs is the ceiling, not the block** — the admission date gate would then be refusing pushes the server would have taken. Pinned so the gate and the engine cannot disagree about the same limit |
    /// | `materialized_views_ignore_errors` | `0` | **a third class: what an error does.** The server's own words: "Allows to ignore errors for MATERIALIZED VIEW, and deliver original block to the table regardless of MVs". At `1` a view's exception is ignored, the insert **succeeds**, and the target is short behind a `200` — the silent-loss shape the whole design exists to prevent. Every failure statement on this path rests on this value |
    /// | `ignore_materialized_views_with_dropped_target_table` | `0` | the same class. `InsertDependenciesBuilder::observePath`, on a view whose target table cannot be locked: `if (!ignore_materialized_views_with_dropped_target_table) throw Exception(UNKNOWN_TABLE, …)` then `LOG_INFO(…); return false;` — so at `1` the view is **skipped and the insert returns success**, with that target's rows silently absent |
    /// | `min_insert_block_size_rows_for_materialized_views` | `0` | **the class [`Self::landing_insert`] already owns — a setting that could divide the request into more than one block — reaching the part of the path its pins do not.** `createSelectInsertContext` *overrides* `min_insert_block_size_rows` with this value for a **view's** insert alone, so the pin on the non-view variant does not govern there; a profile could reshape one view's output into several blocks, each its own commit into the target |
    /// | `min_insert_block_size_bytes_for_materialized_views` | `0` | the same, for bytes |
    /// | `distributed_foreground_insert` | `1` | **a fourth class: what an acknowledgement means.** The server's default is `0`, "data is inserted in background mode", and the two per-trace views insert into a routing table. At `1` the insert "succeeds only after all the data is saved on all shards (at least one replica for each shard if `internal_replication` is true)". **It is pinned here and not on the table**: a `Distributed` table accepts the clause on a `CREATE` and the server keeps nothing — the setting is absent from `SHOW CREATE TABLE` and from `system.tables.create_table_query`, and `ALTER TABLE … MODIFY SETTING` answers `Code: 48`. The chain that makes the query pin reach the view's insert is `InsertDependenciesBuilder::createSelectInsertContext` copying the parent context and changing four named settings, and `StorageDistributed::write` computing `insert_sync` from the context it is given |
    /// | `insert_shard_id` | `0` | **a fifth class: where a row is placed.** `DistributedSink::writeSync`: `if (settings[Setting::insert_shard_id]) { start = insert_shard_id - 1; end = insert_shard_id; }` — the whole block goes to that one shard and the sharding expression is not consulted. A hash-pruned read then looks on the shard `cityHash64(trace_id)` selects while the rows are elsewhere: wrong answers on a cluster from a setting nobody pinned |
    /// | `json_type_escape_dots_in_keys` | `0` | **a sixth class: what path a value is stored under.** Its description says it escapes dots "during parsing", and this path sends paths in the binary form rather than parsing text — but nothing read here establishes that the binary form is unaffected, and if it were affected every stored path would differ from what the encoder renders. Pinned because the question is open, not because the effect is known |
    /// | `type_json_skip_duplicated_paths` | `0` | the block-forming class again. At `1` a repeated path in one value "will be ignored and only the first one will be inserted instead of an exception" — the same result the writer's own per-scope deduplication produces, which is why it must stay the **only** mechanism: at `1` a writer that emitted a duplicate would be silently absorbed instead of erroring |
    /// | the seven overflow modes — `read_overflow_mode`, `read_overflow_mode_leaf`, `timeout_overflow_mode`, `group_by_overflow_mode`, `distinct_overflow_mode`, `sort_overflow_mode`, `result_overflow_mode` | `throw`, each one's own declared default | **a seventh class: whether exceeding a limit is an error or a success that need not be complete.** At `break` — and at `any` for the group-by one — the engine keeps what it had and the statement **succeeds**: `SizeLimits::softCheck` returns `false` and `executeJob` then cancels the source; `ExecutionSpeedLimits::handleOverflowMode` returns `false` for the deadline; and `Aggregator::checkLimits` returns `false` or sets `no_more_keys` instead of raising. A repair run's success would then stop meaning a complete replay, and nothing in the outcome would tell a complete replay from a strict prefix. **The cost, stated because it is not zero**: a deployment that has set both a cap and `break` gets a loud failure here where it had a quiet success. A total landing exactly on the cap is not that deployment — `softCheck` breaks at `>=` while `check` raises only at `>` — so only an overshoot turns loud |
    ///
    /// **Four members are inert for an `INSERT … SELECT` and are named here
    /// so nobody prunes them and reintroduces the drift** the repair's
    /// statements would then carry: `input_format_binary_read_json_as_string`
    /// and `format_binary_max_object_size` govern a RowBinary input such a
    /// statement has none of, and the two view settings govern views, which a
    /// repair statement's target has none attached. Two are load-bearing
    /// there — `distributed_foreground_insert` and `insert_shard_id`, because
    /// the repair writes the routing table for `spans` and `traces` — and two
    /// are moot by the repair's own shape rather than by a pin, since it
    /// replays one target partition per statement and so reaches neither
    /// partition limit.
    ///
    /// **`async_insert` is deliberately not in the set, because it is already
    /// pinned one layer down.** [`crate::ChClient::insert_settings_of`]
    /// begins every insert's settings with `async_insert = 0` and then
    /// applies the call's own settings on top by key, so every insert this
    /// client makes carries the pin whether or not a signal's constructor
    /// names it. A second pin here would state one rule twice. What the query
    /// pin cannot reach is the `MergeTree` setting of the same name, which the
    /// landing table's own `CREATE` handles.
    pub fn trace_landing_insert(token: &str, max_rows: u64) -> Self {
        Self::landing_insert(token, max_rows)
            .set("input_format_binary_read_json_as_string", 0)
            .set("format_binary_max_object_size", MAX_JSON_PATHS_PER_VALUE)
            .set(
                "max_partitions_per_insert_block",
                MAX_PARTITIONS_PER_INSERT_BLOCK,
            )
            .set("throw_on_max_partitions_per_insert_block", 1)
            .set("materialized_views_ignore_errors", 0)
            .set("ignore_materialized_views_with_dropped_target_table", 0)
            .set("min_insert_block_size_rows_for_materialized_views", 0)
            .set("min_insert_block_size_bytes_for_materialized_views", 0)
            .set("distributed_foreground_insert", 1)
            .set("insert_shard_id", 0)
            .set("json_type_escape_dots_in_keys", 0)
            .set("type_json_skip_duplicated_paths", 0)
            .set("read_overflow_mode", "throw")
            .set("read_overflow_mode_leaf", "throw")
            .set("timeout_overflow_mode", "throw")
            .set("group_by_overflow_mode", "throw")
            .set("distinct_overflow_mode", "throw")
            .set("sort_overflow_mode", "throw")
            .set("result_overflow_mode", "throw")
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
    /// STUB (issue #623, tests first).
    pub fn text_capacity(&self) -> u64 {
        0
    }

    /// STUB (issue #623, tests first).
    pub fn len(&self) -> usize {
        usize::MAX
    }

    /// STUB (issue #623, tests first).
    pub fn is_empty(&self) -> bool {
        false
    }

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
            14,
            "the pinned set is closed: a setting added to it without a row in \
             landing_insert's table, and without its required-name entry, is \
             a name sent on somebody's memory"
        );
    }

    /// **W1 (issue #623): a landing insert fails when a view does not
    /// write.** The four view settings are pinned to the server's own
    /// defaults, so a profile cannot turn a failed view into a stored push
    /// with a target row missing: an error in a view, or a view whose target
    /// is gone, fails the push and the resend under the same token writes
    /// what is missing; and a view's own insert is not divided into blocks
    /// the source block was not.
    #[test]
    fn the_landing_insert_pins_the_four_view_settings() {
        let s = QuerySettings::landing_insert("t1", 1_048_576);
        for key in [
            "materialized_views_ignore_errors",
            "ignore_materialized_views_with_dropped_target_table",
            "min_insert_block_size_rows_for_materialized_views",
            "min_insert_block_size_bytes_for_materialized_views",
        ] {
            assert_eq!(s.get(key), Some("0"), "{key}");
        }
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

    /// **The trace landing insert's pin set is exactly the metrics one plus
    /// the fifteen the trace path needs**, and the two sets are compared as
    /// **sets**, with the difference taken against those fifteen — so
    /// neither an added pin nor a removed one passes.
    ///
    /// It is the only place in this change that writes a setting name as a
    /// literal. The classes behind the fifteen, and the catalogue
    /// quotation behind each value, are
    /// [`QuerySettings::trace_landing_insert`]'s own doc comment.
    ///
    /// **`async_insert` is not one of them, and its absence is asserted.**
    /// `ChClient::insert_settings_with` pins it on every insert this client
    /// makes, so a second pin here would state one rule twice; the `MergeTree`
    /// setting of the same name is not reachable from a query setting at all
    /// and is pinned on the landing table's own `CREATE`.
    #[test]
    fn the_trace_landing_insert_pins_every_setting_this_design_names() {
        use std::collections::BTreeMap;

        const WANT: &[(&str, &str)] = &[
            ("input_format_binary_read_json_as_string", "0"),
            ("format_binary_max_object_size", "100000"),
            ("max_partitions_per_insert_block", "100"),
            ("throw_on_max_partitions_per_insert_block", "1"),
            ("distributed_foreground_insert", "1"),
            ("insert_shard_id", "0"),
            ("json_type_escape_dots_in_keys", "0"),
            ("type_json_skip_duplicated_paths", "0"),
            ("read_overflow_mode", "throw"),
            ("read_overflow_mode_leaf", "throw"),
            ("timeout_overflow_mode", "throw"),
            ("group_by_overflow_mode", "throw"),
            ("distinct_overflow_mode", "throw"),
            ("sort_overflow_mode", "throw"),
            ("result_overflow_mode", "throw"),
        ];

        let base = QuerySettings::landing_insert("tok-1", 1_048_576);
        let traces = QuerySettings::trace_landing_insert("tok-1", 1_048_576);
        let base_entries: BTreeMap<&str, &str> = base.entries().collect();
        let trace_entries: BTreeMap<&str, &str> = traces.entries().collect();

        let added: BTreeMap<&str, &str> = trace_entries
            .iter()
            .filter(|(k, _)| !base_entries.contains_key(**k))
            .map(|(k, v)| (*k, *v))
            .collect();
        let want: BTreeMap<&str, &str> = WANT.iter().copied().collect();
        assert_eq!(
            added, want,
            "the difference against the metrics landing insert is exactly the \
             fifteen pins this design names"
        );

        for (key, value) in &base_entries {
            assert_eq!(
                trace_entries.get(key),
                Some(value),
                "{key} must keep the value the metrics landing insert gives it"
            );
        }

        assert_eq!(
            traces.get("async_insert"),
            None,
            "async_insert is pinned one layer down, for every insert this \
             client makes; a second pin here would state one rule twice"
        );
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
