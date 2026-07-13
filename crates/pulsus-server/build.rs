use std::path::PathBuf;
use std::process::Command;

fn main() {
    // Short git SHA for --version; falls back to "unknown" outside a git checkout.
    let sha = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=PULSUS_GIT_SHA={sha}");

    // `/buildinfo` (docs/api.md §7) needs a build timestamp and the
    // compiler version alongside the git SHA above; both are recomputed
    // every build (no `rerun-if-changed` guard), which is correct — unlike
    // the SHA, these two are meant to change on every rebuild, not just
    // when HEAD moves.
    println!("cargo:rustc-env=PULSUS_BUILT_AT={}", built_at_rfc3339());
    println!("cargo:rustc-env=PULSUS_RUSTC={}", rustc_version());

    // Re-run when HEAD moves so the embedded SHA stays current. The workspace
    // `.git` directory does not live under this crate, so ask git for its real
    // location instead of assuming a fixed relative path; degrade to emitting
    // nothing if we are not inside a git checkout (e.g. a source tarball).
    if let Some(git_dir) = git_dir() {
        let head = git_dir.join("HEAD");
        let refs = git_dir.join("refs");
        if head.exists() {
            println!("cargo:rerun-if-changed={}", head.display());
        }
        if refs.exists() {
            println!("cargo:rerun-if-changed={}", refs.display());
        }
    }
}

/// The current build time as RFC 3339 UTC (e.g. `2026-07-10T12:34:56Z`),
/// computed from `SystemTime` with a hand-rolled civil-calendar conversion
/// so `build.rs` does not need a `chrono`/`time` dependency just for this
/// one timestamp.
fn built_at_rfc3339() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let (days, secs_of_day) = (secs / 86_400, secs % 86_400);
    let (hour, min, sec) = (
        secs_of_day / 3600,
        (secs_of_day / 60) % 60,
        secs_of_day % 60,
    );
    let (year, month, day) = civil_from_days(days as i64);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

/// Converts a day count since the Unix epoch (1970-01-01) to a
/// (year, month, day) civil date, using Howard Hinnant's well-known
/// `civil_from_days` algorithm (proleptic Gregorian calendar, correct for
/// every date this build script will ever run against).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, day)
}

/// The compiler's version string (e.g. `rustc 1.93.0 (... 2025-12-15)`),
/// via `rustc -V` against `$RUSTC` (the compiler cargo is actually
/// invoking, not necessarily `rustc` on `$PATH`).
fn rustc_version() -> String {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    Command::new(rustc)
        .arg("-V")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Resolves the git directory for the workspace this crate lives in, as an
/// absolute path. Returns `None` outside a git checkout.
fn git_dir() -> Option<PathBuf> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let output = Command::new("git")
        .args(["rev-parse", "--absolute-git-dir"])
        .current_dir(manifest_dir)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = String::from_utf8(output.stdout).ok()?;
    let path = path.trim();
    if path.is_empty() {
        return None;
    }
    Some(PathBuf::from(path))
}
