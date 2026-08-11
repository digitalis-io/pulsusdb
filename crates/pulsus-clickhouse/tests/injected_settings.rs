//! Every ClickHouse setting this workspace injects exists on the connected
//! server and is not obsolete (issue #376).
//!
//! A version bump is exactly when a setting quietly becomes obsolete or
//! disappears, and ClickHouse does not fail a query for a setting it has
//! retired — it warns in `system.settings.description`/`tier` and moves on.
//! So "we only inject live settings" is a claim nothing checked until this
//! suite.
//!
//! **The key list is derived, not hand-written.** It is checked in below so
//! the live test has something stable to read, and the hermetic test in
//! this file re-derives it by scanning the workspace's own sources for
//! `.set("…")` and fails if the two disagree. That is what stops the list
//! from being a snapshot of what somebody remembered.
//!
//! Run locally:
//!
//! ```text
//! podman run -d --rm --name pulsus-ch-test -p 19123:8123 -p 19000:9000 \
//!     clickhouse/clickhouse-server:26.3
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-clickhouse --test injected_settings
//! podman rm -f pulsus-ch-test
//! ```

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, QuerySettings, Row};

/// Every key the workspace passes to `QuerySettings::set`, sorted.
///
/// Regenerate with the same command the hermetic test re-implements:
///
/// ```text
/// git grep -ohE '\.set\("[a-z_0-9]+"' | sed -E 's/.*\.set\("([a-z_0-9]+)".*/\1/' | sort -u
/// ```
const INJECTED_SETTINGS: &[&str] = &[
    "async_insert",
    "distributed_aggregation_memory_efficient",
    "distributed_product_mode",
    "insert_quorum",
    "insert_quorum_parallel",
    "insert_quorum_timeout",
    "log_comment",
    "max_block_size",
    "max_bytes_before_external_group_by",
    "max_bytes_in_set",
    "max_bytes_to_read",
    "max_execution_time",
    "max_memory_usage",
    "max_query_size",
    "max_result_bytes",
    "max_rows_in_set",
    "max_rows_to_read",
    "max_threads",
    "optimize_distributed_group_by_sharding_key",
    "optimize_read_in_order",
    "optimize_skip_unused_shards",
    "prefer_localhost_replica",
    "query_id",
    "read_overflow_mode",
    "result_overflow_mode",
    "select_sequential_consistency",
    "set_overflow_mode",
    "skip_unavailable_shards",
    "use_query_condition_cache",
    "wait_end_of_query",
];

/// The two names in [`INJECTED_SETTINGS`] that are **HTTP interface
/// parameters, not server settings**, and so are absent from
/// `system.settings` by design.
///
/// This is a closed pair justified by a probe, never a pattern: both are
/// absent from `system.settings` on 24.8.14.39 **and** 26.3.17.110, and
/// `POST /?wait_end_of_query=1` answers `200` on both — so their absence is
/// what the interface specifies, not rot. `query_id` is likewise a
/// URL/interface parameter; `ChClient` renders both into the query string.
///
/// Anything else missing from `system.settings` is a real finding and
/// reddens this suite.
const HTTP_INTERFACE_PARAMS: &[&str] = &["query_id", "wait_end_of_query"];

fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 2,
        query_timeout: Duration::from_secs(20),
        ..ChConnConfig::default()
    }
}

/// The workspace root, from this crate's manifest directory.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/pulsus-clickhouse has a workspace root two levels up")
        .to_path_buf()
}

/// Every `.rs` file under the workspace's own source directories. Walks
/// `crates/` and `xtask/` only — the same ground `git grep` covers for
/// first-party code, without needing git at test time.
fn workspace_rust_files() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                // `target/` holds generated code and vendored builds.
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    let root = workspace_root();
    let mut out = Vec::new();
    for sub in ["crates", "xtask"] {
        walk(&root.join(sub), &mut out);
    }
    assert!(
        out.len() > 50,
        "the source walk found only {} files — it is looking in the wrong place",
        out.len()
    );
    out
}

/// The `.set("<key>"` occurrences in a source text, as keys.
fn set_keys_in(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut rest = text;
    while let Some(i) = rest.find(".set(\"") {
        let tail = &rest[i + ".set(\"".len()..];
        let key: String = tail
            .chars()
            .take_while(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_')
            .collect();
        if !key.is_empty() && tail[key.len()..].starts_with('"') {
            out.insert(key);
        }
        rest = tail;
    }
    out
}

/// Hermetic: the checked-in list is still exactly what the workspace
/// injects. Without this, [`INJECTED_SETTINGS`] would be a list somebody
/// wrote once, and the live test below would be checking that list rather
/// than the code.
#[test]
fn the_checked_in_setting_list_is_what_the_workspace_actually_injects() {
    let mut found = BTreeSet::new();
    for path in workspace_rust_files() {
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        found.extend(set_keys_in(&text));
    }
    let declared: BTreeSet<String> = INJECTED_SETTINGS.iter().map(|s| s.to_string()).collect();
    let missing: Vec<_> = found.difference(&declared).collect();
    let stale: Vec<_> = declared.difference(&found).collect();
    assert!(
        missing.is_empty() && stale.is_empty(),
        "INJECTED_SETTINGS has drifted from the workspace. Injected but not listed: \
         {missing:?}. Listed but no longer injected: {stale:?}. Regenerate with the command \
         in this file's INJECTED_SETTINGS doc comment."
    );
}

/// Hermetic: the exemptions are a closed pair drawn from the list, so a
/// future edit cannot widen them into a pattern.
#[test]
fn the_http_interface_exemptions_are_a_closed_pair_inside_the_list() {
    assert_eq!(HTTP_INTERFACE_PARAMS.len(), 2);
    for name in HTTP_INTERFACE_PARAMS {
        assert!(
            INJECTED_SETTINGS.contains(name),
            "{name:?} is exempted from the live check but is not injected at all"
        );
    }
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SettingRow {
    name: String,
    tier: String,
}

/// Live: every injected setting exists on the connected server and is not
/// obsolete.
///
/// `system.settings.tier` (26.x) carries `Production`/`Beta`/`Experimental`/
/// `Obsolete` as a first-class column, which is a better instrument than
/// the description-substring check an earlier probe used. Measured on
/// 26.3.17.110: all 27 non-interface keys are present and `Production`.
#[tokio::test]
async fn every_injected_setting_is_live_and_not_obsolete() {
    if !should_run() {
        eprintln!(
            "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
             (see crates/pulsus-clickhouse/tests/injected_settings.rs for setup)"
        );
        return;
    }
    let client = ChClient::new(test_config()).await.expect("connect");

    let expected: BTreeSet<&str> = INJECTED_SETTINGS
        .iter()
        .copied()
        .filter(|k| !HTTP_INTERFACE_PARAMS.contains(k))
        .collect();
    let quoted = expected
        .iter()
        .map(|k| format!("'{k}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT name, CAST(tier AS String) AS tier FROM system.settings \
         WHERE name IN ({quoted}) ORDER BY name"
    );
    let mut stream = client
        .query_stream::<SettingRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.settings");
    let mut seen = BTreeSet::new();
    let mut obsolete = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode system.settings row");
        if row.tier == "Obsolete" {
            obsolete.push(row.name.clone());
        }
        seen.insert(row.name);
    }

    let missing: Vec<_> = expected
        .iter()
        .filter(|k| !seen.contains(**k))
        .copied()
        .collect();
    assert!(
        missing.is_empty(),
        "these injected settings do not exist on the connected server: {missing:?}. If one is \
         an HTTP interface parameter rather than a server setting, prove it with a probe and \
         add it to HTTP_INTERFACE_PARAMS; otherwise it is silently doing nothing."
    );
    assert!(
        obsolete.is_empty(),
        "these injected settings are marked Obsolete on the connected server: {obsolete:?}"
    );

    // The exemptions are proved, not asserted: they must be absent from
    // `system.settings`, which is what makes them interface parameters
    // rather than settings we forgot to check.
    let quoted_exempt = HTTP_INTERFACE_PARAMS
        .iter()
        .map(|k| format!("'{k}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT name, CAST(tier AS String) AS tier FROM system.settings \
         WHERE name IN ({quoted_exempt})"
    );
    let mut stream = client
        .query_stream::<SettingRow>(&sql, &QuerySettings::new())
        .await
        .expect("query system.settings for the exemptions");
    let mut present = Vec::new();
    while let Some(row) = stream.next().await {
        present.push(row.expect("decode row").name);
    }
    assert!(
        present.is_empty(),
        "{present:?} ARE server settings on this version — the HTTP-interface-parameter \
         exemption no longer applies to them and they must be checked like the rest"
    );
}
