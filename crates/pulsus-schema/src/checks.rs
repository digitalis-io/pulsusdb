//! The two things the binary asks ClickHouse before it serves: the server
//! version, and whether every setting and function name this build sends
//! exists on it.
//!
//! **This crate no longer creates schema.** `schema/schema.sql` carries the
//! DDL and `schema/schema.sh` applies it; what is left here is read at run
//! time, never sent as DDL.

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChError, QuerySettings, Row};

use crate::error::SchemaError;

/// Parses a ClickHouse `SELECT version()` string (e.g. `26.3.17.110`) and
/// refuses anything older than 26.3 (docs/schemas.md §8). Pure and
/// injectable (task-manager resolution #3 on issue #5) so refusal messages
/// are unit-tested without a live server; `run_init` supplies the real
/// server-reported string.
pub fn check_version(version: &str) -> Result<(), SchemaError> {
    let mut parts = version.trim().split('.');
    let major: u32 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| SchemaError::Version(version.to_string()))?;
    let minor: u32 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| SchemaError::Version(version.to_string()))?;
    if (major, minor) < (26, 3) {
        return Err(SchemaError::UnsupportedVersion {
            found: version.to_string(),
        });
    }
    Ok(())
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct VersionRow {
    v: String,
}

/// Fetches the connected server's `version()` string.
///
/// Public because the startup name check gates on the version before it asks
/// which names are present: a name this build sends that arrived in a later
/// server version is absent *because* the server is too old, and the version
/// is the one an operator can act on (issue #603 code review round 7).
pub async fn server_version(client: &ChClient) -> Result<String, SchemaError> {
    let mut stream = client
        .query_stream::<VersionRow>("SELECT version() AS v", &QuerySettings::new())
        .await?;
    match stream.next().await {
        Some(Ok(row)) => Ok(row.v),
        Some(Err(e)) => Err(e.into()),
        None => Err(SchemaError::Version(
            "empty result from SELECT version()".to_string(),
        )),
    }
}

/// One name read off a catalogue: a `system.tables` row here, and a
/// `system.settings`/`system.merge_tree_settings`/`system.functions` row for
/// [`absent_server_names`] (issue #603).
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct NameRow {
    name: String,
}

/// Whether `db` exists on the connected server.
///
/// **The binary does not create schema.** `schema/schema.sh` does, and
/// startup refuses when the database is absent rather than letting the
/// serving pool report `UNKNOWN_DATABASE` — the refusal can then name the
/// command that builds it.
pub async fn database_exists(client: &ChClient, db: &str) -> Result<bool, SchemaError> {
    let sql = format!(
        "SELECT name FROM system.databases WHERE name = '{}'",
        db.replace('\'', "''")
    );
    Ok(!read_names(client, &sql).await?.is_empty())
}

/// The seconds deduplication window every clustered write-path table is
/// pinned to (issue #603), in seconds.
///
/// **Why this number is enough.** The window guards one thing: the writer
/// resending its own block. That is bounded by the landing budget —
/// `WriterRuntime::landing_budget`, 120 s, measured from the push's admission
/// — so a block settles strictly before it. A client's re-push is a different
/// question, answered by the suppression index. The relation, not the number,
/// is what a case holds, in the one crate that can see both sides
/// (`pulsus-server`'s `chconfig`): `pulsus-schema` and `pulsus-write` do not
/// depend on each other.
///
/// It is the server's own default, so pinning changes nothing where the server
/// keeps it and raises it back where a server configuration lowered it.
pub const DEDUP_WINDOW_SECONDS: u64 = 3600;

/// Which `system` table answers whether one name exists (issue #603).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameCatalogue {
    Setting,
    MergeTreeSetting,
    Function,
}

impl NameCatalogue {
    /// The `system` table this catalogue is read from.
    pub(crate) const fn table(self) -> &'static str {
        match self {
            NameCatalogue::Setting => "system.settings",
            NameCatalogue::MergeTreeSetting => "system.merge_tree_settings",
            NameCatalogue::Function => "system.functions",
        }
    }
}

/// Every setting and function name this build sends that was not already
/// somewhere in `crates/`, read back from the server before anything sends
/// it (issue #603). A name that might not exist is never sent: absent,
/// startup refuses and says which.
pub const REQUIRED_SERVER_NAMES: &[(&str, NameCatalogue)] = &[
    ("insert_deduplication_token", NameCatalogue::Setting),
    // The seven settings one landing insert pins, so that one push is one
    // deduplicated block: `QuerySettings::landing_insert` names them
    // together, quotes what each one's own catalogue entry says, and states
    // how the set was derived. A pin added there without a row here is
    // caught by
    // `the_settings_read_back_at_startup_are_the_ones_the_landing_insert_sends`.
    ("max_insert_block_size", NameCatalogue::Setting),
    ("max_insert_block_size_bytes", NameCatalogue::Setting),
    ("input_format_max_block_size_bytes", NameCatalogue::Setting),
    ("min_insert_block_size_rows", NameCatalogue::Setting),
    ("min_insert_block_size_bytes", NameCatalogue::Setting),
    ("input_format_connection_handling", NameCatalogue::Setting),
    ("input_format_max_block_wait_ms", NameCatalogue::Setting),
    ("merge_with_ttl_timeout", NameCatalogue::MergeTreeSetting),
    // The three block/seconds deduplication-window names `apply_ttl` sends
    // (issue #603). Which of the first two a statement carries is rendered
    // from the same thing that renders the engine
    // (`render::dedup_window_setting`), and the third is sent on a clustered
    // deployment only — but startup does not know which tables a later
    // reconfiguration will render, so all three are read back. The
    // non-replicated name was absent from this list although this build
    // already sent it.
    (
        "non_replicated_deduplication_window",
        NameCatalogue::MergeTreeSetting,
    ),
    (
        "replicated_deduplication_window",
        NameCatalogue::MergeTreeSetting,
    ),
    (
        "replicated_deduplication_window_seconds",
        NameCatalogue::MergeTreeSetting,
    ),
    // The nineteen further settings one TRACE landing insert pins, of seven
    // classes the metrics set did not need: how the block's bytes are read,
    // a limit that refuses a block rather than dividing it, what an error
    // does, what an acknowledgement means, where a row is placed, what path
    // a value is stored under, and whether exceeding a limit is an error or
    // a success that need not be complete.
    // `QuerySettings::trace_landing_insert` names them together and quotes
    // what each one's own catalogue entry says. A pin added there without a
    // row here is caught by
    // `the_settings_read_back_at_startup_are_the_ones_the_trace_insert_sends`,
    // which derives both directions rather than carrying a list.
    (
        "input_format_binary_read_json_as_string",
        NameCatalogue::Setting,
    ),
    ("format_binary_max_object_size", NameCatalogue::Setting),
    ("max_partitions_per_insert_block", NameCatalogue::Setting),
    (
        "throw_on_max_partitions_per_insert_block",
        NameCatalogue::Setting,
    ),
    ("materialized_views_ignore_errors", NameCatalogue::Setting),
    (
        "ignore_materialized_views_with_dropped_target_table",
        NameCatalogue::Setting,
    ),
    (
        "min_insert_block_size_rows_for_materialized_views",
        NameCatalogue::Setting,
    ),
    (
        "min_insert_block_size_bytes_for_materialized_views",
        NameCatalogue::Setting,
    ),
    ("distributed_foreground_insert", NameCatalogue::Setting),
    ("insert_shard_id", NameCatalogue::Setting),
    ("json_type_escape_dots_in_keys", NameCatalogue::Setting),
    ("type_json_skip_duplicated_paths", NameCatalogue::Setting),
    // The seven overflow modes the repair's statements reach. Each is one
    // `DECLARE` line in the engine's own `src/Core/Settings.cpp` at
    // `v26.3.29.7-lts`, defaulting to `throw`, and each is present in
    // `system.settings` on that build with that default — so pinning
    // changes nothing where a deployment keeps them, and makes a
    // deployment that set `break` fail loudly where it had a success that
    // need not have been complete.
    ("read_overflow_mode", NameCatalogue::Setting),
    ("read_overflow_mode_leaf", NameCatalogue::Setting),
    ("timeout_overflow_mode", NameCatalogue::Setting),
    ("group_by_overflow_mode", NameCatalogue::Setting),
    ("distinct_overflow_mode", NameCatalogue::Setting),
    ("sort_overflow_mode", NameCatalogue::Setting),
    ("result_overflow_mode", NameCatalogue::Setting),
    ("generateUUIDv7", NameCatalogue::Function),
    ("toStartOfHour", NameCatalogue::Function),
    ("tupleElement", NameCatalogue::Function),
];

/// One `SELECT name FROM <catalogue> WHERE name IN (…)` statement per
/// catalogue that has at least one required name, in catalogue order. An
/// empty `required` renders no statement.
///
/// **One statement per catalogue, not one joined statement.** A deployment's
/// ClickHouse user may hold `SELECT` on some of these `system` tables and not
/// others — `system.settings` and `system.functions` are readable by any
/// user, while `system.merge_tree_settings` needs an explicit grant — and a
/// joined statement fails whole on the one it cannot read, so a readable
/// catalogue would go unchecked because of an unreadable one.
pub fn required_names_sql(
    required: &[(&'static str, NameCatalogue)],
) -> Vec<(NameCatalogue, String)> {
    let mut out: Vec<(NameCatalogue, String)> = Vec::new();
    for catalogue in [
        NameCatalogue::Setting,
        NameCatalogue::MergeTreeSetting,
        NameCatalogue::Function,
    ] {
        let names: Vec<String> = required
            .iter()
            .filter(|(_, c)| *c == catalogue)
            .map(|(name, _)| format!("'{name}'"))
            .collect();
        if names.is_empty() {
            continue;
        }
        out.push((
            catalogue,
            format!(
                "SELECT name FROM {} WHERE name IN ({})",
                catalogue.table(),
                names.join(", ")
            ),
        ));
    }
    out
}

/// The required names absent from `present`, in `required` order. It matches
/// on the name alone; the statement is what binds a name to its catalogue.
pub fn missing_server_names(
    required: &[(&'static str, NameCatalogue)],
    present: &[String],
) -> Vec<&'static str> {
    required
        .iter()
        .filter(|(name, _)| !present.iter().any(|p| p == name))
        .map(|(name, _)| *name)
        .collect()
}

/// Reads each of [`required_names_sql`]'s statements off the server and
/// reports what [`missing_server_names`] makes of the answers. An empty
/// `required` runs no statement and returns an empty list.
///
/// **A catalogue this user has no grant to read is a catalogue this build
/// cannot check, not a refusal** — and nothing wider than that: see
/// [`catalogue_read_is_unchecked`]. A name a readable catalogue reports ABSENT
/// still refuses.
pub async fn absent_server_names(
    client: &ChClient,
    required: &[(&'static str, NameCatalogue)],
) -> Result<Vec<&'static str>, SchemaError> {
    let mut reads = Vec::new();
    for (catalogue, sql) in required_names_sql(required) {
        reads.push((catalogue, read_names(client, &sql).await));
    }
    fold_catalogue_reads(required, reads)
}

/// ClickHouse's `ACCESS_DENIED` server error code, which a `SELECT` on a
/// `system` table the deployment's user holds no grant for is answered with.
/// The only failure a catalogue read is allowed to continue past.
const ACCESS_DENIED: i32 = 497;

/// Whether one catalogue read's failure leaves that catalogue **unchecked**
/// rather than refusing startup.
///
/// Access denial alone. A denied `SELECT` says nothing about whether the name
/// is there, and refusing on it would stop every least-privilege deployment
/// from starting — `system.merge_tree_settings` needs a grant a user granted
/// only its own database does not hold. Every other failure — a timeout, a
/// transport fault, a decode failure, any other server exception — means the
/// catalogue was not read for a reason that says nothing about grants, so
/// continuing would let startup send a name nothing checked (issue #603 code
/// review, finding 6).
fn catalogue_read_is_unchecked(err: &SchemaError) -> bool {
    matches!(
        err,
        SchemaError::Clickhouse(ChError::Server {
            code: ACCESS_DENIED,
            ..
        })
    )
}

/// Turns one catalogue read per catalogue into the list of required names the
/// server reported absent. Pure, so which failures are tolerated and which
/// names go unchecked is testable without a server.
///
/// A read that succeeded contributes its names and puts its catalogue's
/// required names into the checked set. A read denied by access control
/// contributes neither, with a warning naming the catalogue. Any other
/// failure is returned.
fn fold_catalogue_reads(
    required: &[(&'static str, NameCatalogue)],
    reads: Vec<(NameCatalogue, Result<Vec<String>, SchemaError>)>,
) -> Result<Vec<&'static str>, SchemaError> {
    let mut present: Vec<String> = Vec::new();
    let mut checked: Vec<(&'static str, NameCatalogue)> = Vec::new();
    for (catalogue, read) in reads {
        match read {
            Ok(names) => {
                present.extend(names);
                checked.extend(required.iter().filter(|(_, c)| *c == catalogue).copied());
            }
            Err(err) if catalogue_read_is_unchecked(&err) => tracing::warn!(
                catalogue = catalogue.table(),
                error = %err,
                "no grant to read a name catalogue; the names it holds are not checked"
            ),
            Err(err) => return Err(err),
        }
    }
    Ok(missing_server_names(&checked, &present))
}

/// Every `name` one catalogue statement returns.
async fn read_names(client: &ChClient, sql: &str) -> Result<Vec<String>, SchemaError> {
    let mut stream = client
        .query_stream::<NameRow>(sql, &QuerySettings::new())
        .await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?.name);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The decision is pure: over an empty server every required name is
    /// reported, over a complete one none is, and over each subset with one
    /// name left out exactly that name. A list that hard-codes one absent
    /// name cannot pass the subsets.
    ///
    /// **The production list is asserted non-empty first**, because every
    /// assertion below is over it: an empty list makes each one hold
    /// vacuously — the two `missing_server_names` calls both answer `[]`, the
    /// subset loop runs no iteration — and the case would pass over a build
    /// that requires nothing (issue #603 code review round 2). Two names is
    /// the least that exercises a subset with one left out.
    #[test]
    fn the_startup_name_decision_is_pure() {
        assert!(
            REQUIRED_SERVER_NAMES.len() >= 2,
            "the assertions below are over this list; it must name at least \
             two required names: {REQUIRED_SERVER_NAMES:?}"
        );
        let all: Vec<String> = REQUIRED_SERVER_NAMES
            .iter()
            .map(|(n, _)| (*n).to_string())
            .collect();
        assert_eq!(
            missing_server_names(REQUIRED_SERVER_NAMES, &[]),
            REQUIRED_SERVER_NAMES
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>(),
            "over an empty server every required name is missing, in order"
        );
        assert!(missing_server_names(REQUIRED_SERVER_NAMES, &all).is_empty());
        for (i, (left_out, _)) in REQUIRED_SERVER_NAMES.iter().enumerate() {
            let mut present = all.clone();
            present.remove(i);
            assert_eq!(
                missing_server_names(REQUIRED_SERVER_NAMES, &present),
                vec![*left_out],
                "the subset without {left_out} reports exactly it"
            );
        }
        assert!(missing_server_names(&[], &[]).is_empty());
    }

    /// The statements ask for exactly the required names, and each names its
    /// own catalogue alone — a merge-tree setting looked for in
    /// `system.settings` is absent there and would refuse every startup.
    ///
    /// **One statement per catalogue**, so a catalogue this user cannot read
    /// does not blind the ones it can.
    #[test]
    fn the_sql_asks_for_exactly_the_required_names() {
        let statements = required_names_sql(REQUIRED_SERVER_NAMES);
        assert_eq!(
            statements.len(),
            3,
            "one per catalogue with a required name"
        );
        let quoted: std::collections::BTreeSet<&str> = statements
            .iter()
            .flat_map(|(_, sql)| sql.split('\'').skip(1).step_by(2))
            .collect();
        let want: std::collections::BTreeSet<&str> =
            REQUIRED_SERVER_NAMES.iter().map(|(n, _)| *n).collect();
        assert_eq!(quoted, want, "the literals ARE the list: {statements:?}");
        for (catalogue, sql) in &statements {
            for table in [
                "system.settings",
                "system.merge_tree_settings",
                "system.functions",
            ] {
                let expected = usize::from(table == catalogue.table());
                assert_eq!(
                    sql.matches(table).count(),
                    expected,
                    "{table} in the {} statement: {sql}",
                    catalogue.table()
                );
            }
        }

        // One row of each catalogue names that catalogue alone.
        for (catalogue, table) in [
            (NameCatalogue::Setting, "system.settings"),
            (
                NameCatalogue::MergeTreeSetting,
                "system.merge_tree_settings",
            ),
            (NameCatalogue::Function, "system.functions"),
        ] {
            assert_eq!(
                required_names_sql(&[("only_one", catalogue)]),
                vec![(
                    catalogue,
                    format!("SELECT name FROM {table} WHERE name IN ('only_one')")
                )]
            );
        }
        assert!(
            required_names_sql(&[]).is_empty(),
            "an empty list runs no statement"
        );
    }

    /// **The setting names read back at startup are exactly the setting names
    /// the landing insert sends**, checked both ways against one written-out
    /// set, so neither side can move without the other (issue #603 code
    /// review round 8, finding 1: two rounds each found further settings that
    /// end a block, and each time the list was maintained beside the pins by
    /// hand).
    ///
    /// The two exemptions are the pair the **shipped** span insert already
    /// sends (`QuerySettings::deduplicate_through_views`, issue #560). They
    /// were in `crates/` before this work, which is the class the startup
    /// check draws: every name this work sends that was not already there.
    /// A server too old to carry them is refused by its version, which is a
    /// separate check.
    ///
    /// **The logs landing path widens neither side** (issue #603): its insert
    /// pins the same ten settings the metrics one does, and its read path
    /// sends no query setting at all — the discovery reads dispatch with the
    /// budget settings and nothing more. `the_discovery_reads_send_no_distributed_product_mode`
    /// (`crates/pulsus-read/src/logql/exec.rs`) asserts the other side of
    /// that.
    #[test]
    fn the_settings_read_back_at_startup_are_the_ones_the_landing_insert_sends() {
        use std::collections::BTreeSet;

        /// In `crates/` before this work, so outside the probed class.
        const ALREADY_SHIPPED: &[&str] = &[
            "deduplicate_insert",
            "deduplicate_blocks_in_dependent_materialized_views",
        ];

        let want: BTreeSet<&str> = BTreeSet::from([
            "insert_deduplication_token",
            "max_insert_block_size",
            "max_insert_block_size_bytes",
            "input_format_max_block_size_bytes",
            "min_insert_block_size_rows",
            "min_insert_block_size_bytes",
            "input_format_connection_handling",
            "input_format_max_block_wait_ms",
        ]);

        // The trace landing insert's own nineteen pins are rows too (issues
        // #584 to #586), and they are **derived from that constructor** here
        // rather than written out a second time: this case owns the eight
        // above as a literal set, and
        // `the_settings_read_back_at_startup_are_the_ones_the_trace_insert_sends`
        // owns the rest, both ways.
        let trace_only_set = QuerySettings::trace_landing_insert("tok-1", 1_048_576);
        let trace_only: BTreeSet<&str> = trace_only_set
            .entries()
            .map(|(k, _)| k)
            .filter(|k| !ALREADY_SHIPPED.contains(k) && !want.contains(k))
            .collect();
        assert!(
            !trace_only.is_empty(),
            "the trace landing insert adds no pin of its own, so the union \
             below is the metrics set and this case closes nothing new"
        );
        let want_all: BTreeSet<&str> = want.union(&trace_only).copied().collect();

        let read_back: BTreeSet<&str> = REQUIRED_SERVER_NAMES
            .iter()
            .filter(|(_, c)| *c == NameCatalogue::Setting)
            .map(|(n, _)| *n)
            .collect();
        assert_eq!(
            read_back, want_all,
            "the settings catalogue's required names are the two landing \
             inserts' pins and nothing else"
        );

        let settings = QuerySettings::landing_insert("tok-1", 1_048_576);
        let sent: BTreeSet<&str> = settings.entries().map(|(k, _)| k).collect();
        let sent_new: BTreeSet<&str> = sent
            .iter()
            .copied()
            .filter(|k| !ALREADY_SHIPPED.contains(k))
            .collect();
        assert_eq!(
            sent_new, want,
            "every setting one landing insert sends is either read back at \
             startup or one of the two the shipped span insert already sends"
        );
        for name in ALREADY_SHIPPED {
            assert!(
                sent.contains(name),
                "{name} is exempted from the check but is not sent at all"
            );
        }
    }

    /// **T30.** The three statements, written out, so the list and the
    /// statements cannot drift apart unnoticed.
    ///
    /// The `MergeTree` catalogue carries §4's **three** deduplication-window
    /// names and nothing else: which of the two block names a statement
    /// renders follows the engine, and the seconds name is sent on a clustered
    /// deployment only, but startup cannot know which shape a later
    /// reconfiguration renders, so all three are read back. **No query setting
    /// is added**: the discovery reads send none.
    #[test]
    fn the_required_names_statements_are_exactly_this_text() {
        assert_eq!(
            required_names_sql(REQUIRED_SERVER_NAMES),
            vec![
                (
                    NameCatalogue::Setting,
                    "SELECT name FROM system.settings WHERE name IN \
                     ('insert_deduplication_token', 'max_insert_block_size', \
                     'max_insert_block_size_bytes', 'input_format_max_block_size_bytes', \
                     'min_insert_block_size_rows', 'min_insert_block_size_bytes', \
                     'input_format_connection_handling', 'input_format_max_block_wait_ms', \
                     'input_format_binary_read_json_as_string', \
                     'format_binary_max_object_size', 'max_partitions_per_insert_block', \
                     'throw_on_max_partitions_per_insert_block', \
                     'materialized_views_ignore_errors', \
                     'ignore_materialized_views_with_dropped_target_table', \
                     'min_insert_block_size_rows_for_materialized_views', \
                     'min_insert_block_size_bytes_for_materialized_views', \
                     'distributed_foreground_insert', 'insert_shard_id', \
                     'json_type_escape_dots_in_keys', 'type_json_skip_duplicated_paths', \
                     'read_overflow_mode', 'read_overflow_mode_leaf', \
                     'timeout_overflow_mode', 'group_by_overflow_mode', \
                     'distinct_overflow_mode', 'sort_overflow_mode', \
                     'result_overflow_mode')"
                        .to_string()
                ),
                (
                    NameCatalogue::MergeTreeSetting,
                    "SELECT name FROM system.merge_tree_settings WHERE name IN \
                     ('merge_with_ttl_timeout', 'non_replicated_deduplication_window', \
                     'replicated_deduplication_window', \
                     'replicated_deduplication_window_seconds')"
                        .to_string()
                ),
                (
                    NameCatalogue::Function,
                    "SELECT name FROM system.functions WHERE name IN \
                     ('generateUUIDv7', 'toStartOfHour', 'tupleElement')"
                        .to_string()
                ),
            ]
        );
    }

    /// **The setting names read back at startup are exactly the setting names
    /// the trace landing insert sends**, derived both ways with no list on
    /// either side.
    ///
    /// **Forward**: every key `QuerySettings::trace_landing_insert` sends has
    /// a `REQUIRED_SERVER_NAMES` row of catalogue `Setting`, bar the pair the
    /// shipped span insert already sent — the same exemption
    /// `the_settings_read_back_at_startup_are_the_ones_the_landing_insert_sends`
    /// carries, and for the same reason: they were in `crates/` before this
    /// work, which is the class the startup check draws.
    ///
    /// **Reverse**: the rows this change added, taken as the set difference
    /// between the `Setting` rows and the keys the **metrics** landing insert
    /// sends — which is what those rows were at the base revision, pinned by
    /// the case named above — must every one be a key the trace constructor
    /// sends.
    ///
    /// Neither direction names a count or a name, so a pin added without a
    /// row, a row added without a pin, and a row added for one of the
    /// nineteen while the reverse check only walked some of them all fail.
    #[test]
    fn the_settings_read_back_at_startup_are_the_ones_the_trace_insert_sends() {
        use std::collections::BTreeSet;

        /// In `crates/` before this work, so outside the probed class.
        const ALREADY_SHIPPED: &[&str] = &[
            "deduplicate_insert",
            "deduplicate_blocks_in_dependent_materialized_views",
        ];

        let rows: BTreeSet<&str> = REQUIRED_SERVER_NAMES
            .iter()
            .filter(|(_, c)| *c == NameCatalogue::Setting)
            .map(|(n, _)| *n)
            .collect();
        let trace_sent_set = QuerySettings::trace_landing_insert("tok-1", 1_048_576);
        let trace_sent: BTreeSet<&str> = trace_sent_set.entries().map(|(k, _)| k).collect();
        let metrics_sent_set = QuerySettings::landing_insert("tok-1", 1_048_576);
        let metrics_sent: BTreeSet<&str> = metrics_sent_set.entries().map(|(k, _)| k).collect();

        // Forward.
        let unread: Vec<&str> = trace_sent
            .iter()
            .copied()
            .filter(|k| !ALREADY_SHIPPED.contains(k) && !rows.contains(k))
            .collect();
        assert!(
            unread.is_empty(),
            "the trace landing insert sends settings nothing reads back at \
             startup: {unread:?}"
        );

        // Reverse: the rows this change added.
        let added: Vec<&str> = rows
            .iter()
            .copied()
            .filter(|k| !metrics_sent.contains(k))
            .collect();
        assert!(
            !added.is_empty(),
            "this change adds no setting row at all, so the reverse direction \
             checks nothing"
        );
        let unsent: Vec<&str> = added
            .iter()
            .copied()
            .filter(|k| !trace_sent.contains(k))
            .collect();
        assert!(
            unsent.is_empty(),
            "a required-name row was added for a setting the trace landing \
             insert does not send: {unsent:?}"
        );
        for name in ALREADY_SHIPPED {
            assert!(
                trace_sent.contains(name),
                "{name} is exempted from the check but is not sent at all"
            );
        }
    }

    /// Issue #603 code review, finding 6: a catalogue read that failed for a
    /// reason other than a missing grant must refuse startup, not leave the
    /// names it holds silently unchecked. A timeout, a transport fault, a
    /// decode failure or any other server exception says nothing about
    /// grants — continuing past one lets the build send a name nothing ever
    /// looked for, which is the whole point of the check.
    #[test]
    fn only_a_denied_grant_leaves_a_catalogue_unchecked() {
        const NAMES: &[(&str, NameCatalogue)] = &[
            ("a_setting", NameCatalogue::Setting),
            ("a_function", NameCatalogue::Function),
        ];
        let denied = || {
            SchemaError::Clickhouse(ChError::Server {
                code: 497,
                message: "Not enough privileges on system.merge_tree_settings".to_string(),
            })
        };

        // Access denial on the settings catalogue: its name goes unchecked,
        // and the readable catalogue's absent name still refuses.
        let absent = fold_catalogue_reads(
            NAMES,
            vec![
                (NameCatalogue::Setting, Err(denied())),
                (NameCatalogue::Function, Ok(Vec::new())),
            ],
        )
        .expect("a denied grant is not a refusal");
        assert_eq!(
            absent,
            vec!["a_function"],
            "the unreadable catalogue's name is unchecked; the readable \
             catalogue's absent name still refuses"
        );

        // Every other failure is returned.
        for (label, err) in [
            (
                "a timeout",
                SchemaError::Clickhouse(ChError::Timeout("deadline".to_string())),
            ),
            (
                "a transport fault",
                SchemaError::Clickhouse(ChError::Io("reset by peer".to_string())),
            ),
            (
                "a decode failure",
                SchemaError::Clickhouse(ChError::Decode("not a NameRow".to_string())),
            ),
            (
                "another server exception",
                SchemaError::Clickhouse(ChError::Server {
                    code: 60,
                    message: "Table system.settings does not exist".to_string(),
                }),
            ),
            (
                "a non-ClickHouse schema error",
                SchemaError::Version("nonsense".to_string()),
            ),
        ] {
            let got = fold_catalogue_reads(
                NAMES,
                vec![
                    (NameCatalogue::Setting, Err(err)),
                    (NameCatalogue::Function, Ok(vec!["a_function".to_string()])),
                ],
            );
            assert!(
                got.is_err(),
                "{label} must refuse rather than leave a catalogue unchecked, got {got:?}"
            );
        }

        // Both readable: nothing absent.
        let absent = fold_catalogue_reads(
            NAMES,
            vec![
                (NameCatalogue::Setting, Ok(vec!["a_setting".to_string()])),
                (NameCatalogue::Function, Ok(vec!["a_function".to_string()])),
            ],
        )
        .expect("two good reads");
        assert!(absent.is_empty(), "{absent:?}");
    }

    #[test]
    fn check_version_accepts_the_minimum_supported_version() {
        assert!(check_version("26.3.0.1").is_ok());
    }

    #[test]
    fn check_version_accepts_newer_versions() {
        assert!(check_version("26.4.0.0").is_ok());
        assert!(check_version("27.1.0.0").is_ok());
    }

    #[test]
    fn check_version_refuses_older_minor_versions() {
        let err = check_version("26.2.9.1").unwrap_err();
        assert!(matches!(err, SchemaError::UnsupportedVersion { .. }));
        assert!(err.to_string().contains("26.2.9.1"));
    }

    #[test]
    fn check_version_refuses_older_major_versions() {
        // The version this floor replaced (issue #376). 24.8 is refused
        // for the reason `SchemaError::UnsupportedVersion` gives: an
        // HTTP-200 mid-stream exception carries no server-declared
        // length there, so it cannot be told from result text.
        let err = check_version("24.8.14.39").unwrap_err();
        assert!(matches!(err, SchemaError::UnsupportedVersion { .. }));
    }

    #[test]
    fn check_version_reports_unparseable_strings_distinctly() {
        let err = check_version("not-a-version").unwrap_err();
        assert!(matches!(err, SchemaError::Version(_)));
    }
}
