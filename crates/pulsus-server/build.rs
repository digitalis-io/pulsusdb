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
