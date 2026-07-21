//! Issue #156: fetch-on-cache-miss for the upstream Prometheus
//! promqltest corpus. The `.test` files are no longer vendored in-tree —
//! they are fetched at test time from the pinned upstream commit
//! (addressed by SHA, never a movable ref), verified per file against
//! the committed `upstream-manifest.json` (SHA-256 + line count, the
//! trust anchor), and cached under a pin-keyed directory. A warm cache
//! never spawns a network process; `cargo build`/packaging never runs
//! tests and therefore never fetches.
//!
//! Transport is a system-`curl` subprocess (zero new dependencies, TLS
//! delegated to the OS — the KISS-testing convention; CI already shells
//! curl). Tests inject a `file://` base and a scratch cache dir instead
//! of the network (see `ensure_cached_in`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::{UpstreamFileEntry, UpstreamManifest, sha256_hex};

/// Real source root; tests inject a `file://` base instead.
pub const UPSTREAM_SOURCE_BASE: &str = "https://raw.githubusercontent.com/prometheus/prometheus";

/// Process-wide fetch sequence number — the third component of the
/// per-writer-unique temp-file name (plan v2 Δ1).
static FETCH_SEQ: AtomicU64 = AtomicU64::new(0);

/// `$PULSUSDB_PROMQLTEST_CACHE_DIR`, else `$XDG_CACHE_HOME/pulsusdb/
/// promqltest`, else `$HOME/.cache/pulsusdb/promqltest`. Panics with
/// remediation text if no root is resolvable (HOME unset and no
/// override).
pub fn cache_root() -> PathBuf {
    if let Some(dir) = std::env::var_os("PULSUSDB_PROMQLTEST_CACHE_DIR")
        && !dir.is_empty()
    {
        return PathBuf::from(dir);
    }
    if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME")
        && !xdg.is_empty()
    {
        return PathBuf::from(xdg).join("pulsusdb").join("promqltest");
    }
    if let Some(home) = std::env::var_os("HOME")
        && !home.is_empty()
    {
        return PathBuf::from(home)
            .join(".cache")
            .join("pulsusdb")
            .join("promqltest");
    }
    panic!(
        "cannot resolve the promqltest corpus cache root: HOME and XDG_CACHE_HOME are unset — \
         set PULSUSDB_PROMQLTEST_CACHE_DIR to a writable directory"
    );
}

/// `cache_root()/<manifest.prometheus_sha>` — pin-keyed, so a version
/// bump never collides with stale entries.
pub fn pin_dir(manifest: &UpstreamManifest) -> PathBuf {
    cache_root().join(&manifest.prometheus_sha)
}

/// For every manifest entry: use the cached file if sha256+lines verify;
/// otherwise fetch `{base}/{prometheus_sha}/promql/promqltest/testdata/
/// {name}` via system curl to a tmp sibling, verify sha256+lines against
/// the manifest, atomically rename into place. A cached mismatch
/// self-heals by refetching ONCE; a post-fetch mismatch panics with the
/// URL plus expected/actual sha256. A fetch failure panics with URL,
/// cache dir, and the pre-warm instruction. Returns contents keyed by
/// file name (exactly what `load_upstream_verified` returned before
/// #156).
pub fn ensure_cached(manifest: &UpstreamManifest, base: &str) -> BTreeMap<String, String> {
    ensure_cached_in(&pin_dir(manifest), manifest, base)
}

/// [`ensure_cached`] with an explicit pin directory. The hermetic guard
/// tests use this (scratch dir + `file://` base) instead of mutating
/// `PULSUSDB_PROMQLTEST_CACHE_DIR`: env vars are process-global and
/// `std::env::set_var` is unsafe under edition 2024, so an env-based
/// override would race the real-cache loads running on sibling test
/// threads.
pub fn ensure_cached_in(
    dir: &Path,
    manifest: &UpstreamManifest,
    base: &str,
) -> BTreeMap<String, String> {
    std::fs::create_dir_all(dir)
        .unwrap_or_else(|e| panic!("failed to create cache dir {}: {e}", dir.display()));
    let mut contents = BTreeMap::new();
    for entry in &manifest.files {
        let path = dir.join(&entry.name);
        let text = match cached_verified(&path, entry) {
            Some(text) => text,
            // Cache miss, or a corrupted entry self-healing exactly once:
            // a persistent (post-fetch) mismatch panics inside.
            None => fetch_verify_install(dir, entry, &manifest.prometheus_sha, base),
        };
        contents.insert(entry.name.clone(), text);
    }
    contents
}

/// Reads a cached file and verifies it against its manifest entry.
/// `None` on missing file or any mismatch (the caller refetches).
fn cached_verified(path: &Path, entry: &UpstreamFileEntry) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    (sha256_hex(text.as_bytes()) == entry.sha256 && text.lines().count() == entry.lines)
        .then_some(text)
}

/// Fetches one file to a per-writer-unique temp sibling, verifies it
/// against the manifest, and atomically renames it into place.
///
/// Temp path: `<dir>/.<name>.tmp-<pid>-<tid>-<seq>` — unique per WRITER
/// (pid + thread id + process-wide AtomicU64 counter), so plain
/// multi-thread `cargo test` in one process is as race-free as
/// nextest's process-per-test model (plan v2 Δ1). Rename-into-place is
/// the only commit step: racing writers install identical
/// manifest-verified bytes, last-writer-wins is benign, and each
/// writer's rename consumes its own temp file so none are left behind.
fn fetch_verify_install(
    dir: &Path,
    entry: &UpstreamFileEntry,
    prometheus_sha: &str,
    base: &str,
) -> String {
    let url = format!(
        "{base}/{prometheus_sha}/promql/promqltest/testdata/{}",
        entry.name
    );
    // ThreadId has no stable accessor; its Debug repr is `ThreadId(<n>)` —
    // keep the digits.
    let tid: String = format!("{:?}", std::thread::current().id())
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect();
    let tmp = dir.join(format!(
        ".{}.tmp-{}-{}-{}",
        entry.name,
        std::process::id(),
        tid,
        FETCH_SEQ.fetch_add(1, Ordering::Relaxed),
    ));

    let output = std::process::Command::new("curl")
        .arg("--silent")
        .arg("--show-error")
        .arg("--fail")
        .arg("--location")
        .arg("--retry")
        .arg("3")
        .arg("--connect-timeout")
        .arg("10")
        .arg("--max-time")
        .arg("120")
        .arg("--output")
        .arg(&tmp)
        .arg(&url)
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn curl for {url}: {e}"));
    if !output.status.success() {
        let _ = std::fs::remove_file(&tmp);
        panic!(
            "failed to fetch the pinned upstream corpus file {url} ({}; stderr: {}) into cache \
             dir {} — the corpus is fetched once and cached; to pre-warm (or when offline), run \
             `cargo test -p pulsus-promql --test promqltest_corpus \
             upstream_corpus_matches_its_integrity_manifest` on a networked machine, or point \
             PULSUSDB_PROMQLTEST_CACHE_DIR at a pre-populated cache",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
            dir.display(),
        );
    }

    let text = std::fs::read_to_string(&tmp)
        .unwrap_or_else(|e| panic!("failed to read fetched temp file {}: {e}", tmp.display()));
    let sha = sha256_hex(text.as_bytes());
    if sha != entry.sha256 {
        let _ = std::fs::remove_file(&tmp);
        panic!(
            "checksum mismatch for fetched upstream corpus file {url}: expected sha256 {} \
             (committed upstream-manifest.json, the trust anchor) but fetched bytes hash to \
             {sha} — refusing to install; this guards against truncation, tampering, and \
             upstream ref rewrites",
            entry.sha256,
        );
    }
    let lines = text.lines().count();
    if lines != entry.lines {
        let _ = std::fs::remove_file(&tmp);
        panic!(
            "line-count mismatch for fetched upstream corpus file {url}: expected {} lines \
             (upstream-manifest.json) but fetched {lines}",
            entry.lines,
        );
    }

    std::fs::rename(&tmp, dir.join(&entry.name)).unwrap_or_else(|e| {
        panic!(
            "failed to install verified corpus file {} into {}: {e}",
            entry.name,
            dir.display()
        )
    });
    text
}
