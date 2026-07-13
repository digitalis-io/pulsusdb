//! Compose-runtime resolution and stack lifecycle (`up -d` / `down -v` /
//! `logs`). Runtime-neutral fixtures (root `docker-compose.yaml`,
//! `ci/clickhouse-cluster/compose.yaml`) work under either `docker compose`
//! or Podman's compose tooling (architect plan: avoid compose `include:`,
//! which is spotty under Podman; use repeated `-f` instead — see
//! `harness::compose_for`). CI always uses the runner's Docker; local
//! development defaults to Podman.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;

/// Which compose-capable runtime drives the stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum EngineKind {
    Docker,
    Podman,
}

impl EngineKind {
    /// `--engine` flag > `PULSUS_E2E_ENGINE` env var > probing `docker`
    /// then `podman`/`podman-compose` on `PATH`.
    pub fn resolve(flag: Option<EngineKind>) -> Result<EngineKind> {
        if let Some(kind) = flag {
            return Ok(kind);
        }
        if let Ok(v) = std::env::var("PULSUS_E2E_ENGINE") {
            return match v.as_str() {
                "docker" => Ok(EngineKind::Docker),
                "podman" => Ok(EngineKind::Podman),
                other => bail!("PULSUS_E2E_ENGINE={other:?} must be \"docker\" or \"podman\""),
            };
        }
        if binary_on_path("docker") {
            return Ok(EngineKind::Docker);
        }
        if binary_on_path("podman") || binary_on_path("podman-compose") {
            return Ok(EngineKind::Podman);
        }
        bail!("neither docker nor podman/podman-compose found on PATH")
    }

    /// The argv this engine's compose invocation starts with.
    fn compose_argv0(self) -> Vec<&'static str> {
        match self {
            EngineKind::Docker => vec!["docker", "compose"],
            EngineKind::Podman if podman_has_native_compose() => vec!["podman", "compose"],
            // Podman only grew a built-in `compose` subcommand in 4.5+;
            // older Podman (this sandbox ships 3.4) has no such subcommand,
            // so fall back to the standalone `podman-compose` binary that
            // the rest of the repo's fixtures already document using.
            // `--in-pod=false` is load-bearing on old Podman/CNI
            // combinations (this sandbox's 3.4.4): `podman-compose`'s
            // default pod-per-project mode creates each service's
            // container with `--pod ... --infra=false`, under which this
            // Podman version silently ignores the container's own
            // `--network=<name>:ip=<addr>` and attaches it to the default
            // bridge instead — breaking every fixture's static-IP
            // addressing. Running one container per network namespace
            // (no shared pod) makes per-container network attachment
            // honored again; scenarios never rely on containers sharing a
            // pod's namespace, so this changes nothing observable.
            EngineKind::Podman => vec!["podman-compose", "--in-pod=false"],
        }
    }
}

fn binary_on_path(name: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(name).is_file()))
        .unwrap_or(false)
}

fn podman_has_native_compose() -> bool {
    Command::new("podman")
        .args(["compose", "version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// One compose stack: a fixed list of `-f` files plus a project name that
/// isolates the single/cluster variants (and any concurrent local runs)
/// from each other's containers, networks, and volumes.
#[derive(Debug, Clone)]
pub struct Compose {
    engine: EngineKind,
    files: Vec<&'static str>,
    project: &'static str,
}

impl Compose {
    pub fn new(engine: EngineKind, files: Vec<&'static str>, project: &'static str) -> Self {
        Compose {
            engine,
            files,
            project,
        }
    }

    pub fn project(&self) -> &'static str {
        self.project
    }

    pub fn up(&self) -> Result<()> {
        self.run(&["up", "-d"]).map(|_| ())
    }

    /// `down -v` — dropping volumes too: stale ClickHouse/Keeper data (in
    /// particular Keeper znodes backing `Replicated*` tables) across runs
    /// would corrupt replicated DDL on the next `up` (architect plan edge
    /// case).
    pub fn down(&self) -> Result<()> {
        self.run(&["down", "-v"]).map(|_| ())
    }

    /// Combined stdout+stderr of `logs <service>`, for the
    /// failure-diagnosability dump — not machine-parsed, so a failed fetch
    /// is folded into the returned string rather than propagated.
    pub fn logs(&self, service: &str) -> String {
        match self.run(&["logs", service]) {
            Ok(output) => format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ),
            Err(err) => format!("(failed to fetch logs for {service}: {err:#})"),
        }
    }

    fn run(&self, args: &[&str]) -> Result<Output> {
        let mut argv = self.engine.compose_argv0();
        for f in &self.files {
            argv.push("-f");
            argv.push(f);
        }
        argv.push("-p");
        argv.push(self.project);
        argv.extend_from_slice(args);

        // Invariant: `compose_argv0` always returns at least one element.
        let (program, rest) = argv.split_first().expect("compose argv is never empty");
        let output = Command::new(program)
            .args(rest)
            .current_dir(workspace_root())
            // Resolved by the compose overlays (`build.context`, volume
            // mounts) so relative paths are unambiguous regardless of
            // which `-f` file a given compose implementation treats as the
            // base for path resolution.
            .env("PULSUS_E2E_ROOT", workspace_root())
            .output()
            .with_context(|| format!("failed to run `{program} {}`", rest.join(" ")))?;
        if !output.status.success() {
            bail!(
                "`{program} {}` failed (status {}):\n{}",
                rest.join(" "),
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(output)
    }
}

/// Tears the stack down on drop — including on panic or an early `?`
/// return from a scenario — unless `--keep` was requested (architect
/// plan: "a Drop guard tears the stack down (incl. on panic/failure)").
pub struct ComposeGuard {
    compose: Compose,
    keep: bool,
}

impl ComposeGuard {
    pub fn new(compose: Compose, keep: bool) -> Self {
        ComposeGuard { compose, keep }
    }
}

impl Drop for ComposeGuard {
    fn drop(&mut self) {
        if self.keep {
            eprintln!(
                "pulsus-e2e: --keep set, leaving project {:?} running",
                self.compose.project()
            );
            return;
        }
        if let Err(err) = self.compose.down() {
            eprintln!(
                "pulsus-e2e: teardown of {:?} failed: {err:#}",
                self.compose.project()
            );
        }
    }
}

/// The repository root, resolved at compile time from this crate's
/// manifest directory — robust regardless of the caller's current working
/// directory (unlike relying on process `cwd`).
pub(crate) fn workspace_root() -> PathBuf {
    // Infallible invariant: `e2e/` is always a direct child of the
    // workspace root in this repository's layout.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("e2e crate is nested one level under the workspace root")
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_honors_the_explicit_flag_over_everything_else() {
        assert_eq!(
            EngineKind::resolve(Some(EngineKind::Docker)).unwrap(),
            EngineKind::Docker
        );
        assert_eq!(
            EngineKind::resolve(Some(EngineKind::Podman)).unwrap(),
            EngineKind::Podman
        );
    }

    #[test]
    fn workspace_root_contains_the_root_cargo_toml() {
        assert!(workspace_root().join("Cargo.toml").is_file());
    }

    #[test]
    fn compose_new_keeps_the_project_name() {
        let compose = Compose::new(EngineKind::Docker, vec!["docker-compose.yaml"], "proj");
        assert_eq!(compose.project(), "proj");
    }
}
