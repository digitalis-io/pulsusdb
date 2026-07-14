//! Scenario registry (issue #7 architect plan). `test/fixtures/README.md`
//! documents the contract: adding a scenario is one
//! `test/fixtures/<area>/<name>.*` fixture file, one assertion fn here,
//! and one `SCENARIOS` entry. The M0 skeleton ships exactly two ops-only
//! scenarios, both green on a fresh compose stack with no ingest wired yet
//! (M1 adds ingest/query scenarios the same way).

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use anyhow::{Context, Result, bail};
use clap::ValueEnum;

/// Which compose variant a [`Scenario`] runs under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Variant {
    Single,
    Cluster,
}

/// Per-run context handed to every scenario: an HTTP client bound to the
/// stack's published `:3100`, which variant is running, and the fixtures
/// directory scenarios load expected data from.
pub struct Ctx {
    pub http: reqwest::Client,
    pub base_url: String,
    pub variant: Variant,
    pub fixtures_dir: PathBuf,
}

impl Ctx {
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

/// A scenario's entry point: a plain fn pointer returning a boxed future.
/// No trait, no `async-trait` — a bare fn pointer is enough for a
/// `&'static [Scenario]` registry. Named as a type alias purely to keep
/// `Scenario` readable (same type the architect plan's interface
/// specifies, `fn(&Ctx) -> Pin<Box<dyn Future<Output = Result<()>> + '_>>`).
pub type ScenarioFn = fn(&Ctx) -> Pin<Box<dyn Future<Output = Result<()>> + '_>>;

/// One scenario: a name (for logging/diagnostics), the variants it applies
/// to, and its [`ScenarioFn`].
pub struct Scenario {
    pub name: &'static str,
    pub variants: &'static [Variant],
    pub run: ScenarioFn,
}

pub const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "readiness",
        variants: &[Variant::Single, Variant::Cluster],
        run: |ctx| Box::pin(readiness(ctx)),
    },
    Scenario {
        name: "buildinfo_roundtrip",
        variants: &[Variant::Single, Variant::Cluster],
        run: |ctx| Box::pin(buildinfo_roundtrip(ctx)),
    },
    Scenario {
        name: "grafana_loki_compat",
        // Single-variant only (architect plan): the `grafana` service and
        // its Loki-datasource provisioning ship only in
        // `deploy/e2e/compose.single.yaml`.
        variants: &[Variant::Single],
        run: |ctx| Box::pin(grafana_loki_compat(ctx)),
    },
];

/// `GET /ready` is already gated on by the harness's own polling
/// (`harness::wait_ready`) before any scenario runs; this scenario
/// re-asserts 200 as the skeleton milestone's trivially-green per-variant
/// case (docs/api.md §7).
async fn readiness(ctx: &Ctx) -> Result<()> {
    println!("pulsus-e2e:   readiness check for {:?}", ctx.variant);
    let res = ctx
        .http
        .get(ctx.url("/ready"))
        .send()
        .await
        .context("GET /ready failed")?;
    if !res.status().is_success() {
        bail!("GET /ready returned {}", res.status());
    }
    Ok(())
}

/// `GET /buildinfo` (docs/api.md §7): 200, plus every field named in
/// `test/fixtures/ops/buildinfo.fields.json` present and non-empty —
/// exercises the fixture-file contract itself, not just the endpoint.
async fn buildinfo_roundtrip(ctx: &Ctx) -> Result<()> {
    let fields = load_fixture_fields(&ctx.fixtures_dir.join("ops/buildinfo.fields.json"))?;

    let res = ctx
        .http
        .get(ctx.url("/buildinfo"))
        .send()
        .await
        .context("GET /buildinfo failed")?;
    if !res.status().is_success() {
        bail!("GET /buildinfo returned {}", res.status());
    }
    let body: serde_json::Value = res
        .json()
        .await
        .context("GET /buildinfo body was not JSON")?;

    for field in &fields {
        let present = body
            .get(field)
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty());
        if !present {
            bail!("GET /buildinfo missing or empty field {field:?} in {body}");
        }
    }
    Ok(())
}

/// Grafana's own published base URL for this stack (deploy/e2e/
/// compose.single.yaml's `grafana` service, `ports: ["3000:3000"]`) —
/// distinct from `ctx.base_url` (pulsusdb's `:3100`), so this scenario
/// builds its own client/URL rather than using `Ctx::url`.
const GRAFANA_BASE_URL: &str = "http://127.0.0.1:3000";

/// M1 log-query compat alias check via a real Loki datasource (issue #14,
/// docs/api.md §8.1; task-manager-approved option A on the architect
/// plan's open question). Drives Grafana's datasource proxy
/// (`POST /api/ds/query`) with an M1 `query_range` against the
/// `pulsus-loki` datasource provisioned in
/// `deploy/e2e/grafana/provisioning/datasources/loki.yaml`, which points
/// at pulsusdb's `/loki/api/v1/*` compat surface
/// (`PULSUS_COMPAT_ENDPOINTS=true` in the single-variant compose overlay).
/// Asserts a well-formed Loki envelope with no query error — proving alias
/// routing and Loki-datasource wire compatibility end to end.
///
/// Empty results are permitted and expected here: M1 ships no ingest
/// receiver (`writer_router()` is still empty, docs/api.md §8.2 is M6), so
/// nothing seeds data through the running HTTP-only stack. Byte-identical
/// behaviour against *real* data is already proven at the `pulsus-server`
/// live layer (`crates/pulsus-server/tests/logs_api_live.rs`) via direct
/// `ChClient` seeding. **Issue #15's collector-to-query e2e upgrades this
/// same scenario to assert non-empty frames** once the ingest path is
/// wired through the running stack.
async fn grafana_loki_compat(_ctx: &Ctx) -> Result<()> {
    let http = reqwest::Client::new();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    let from_ms = now_ms.saturating_sub(3_600_000);

    let request_body = serde_json::json!({
        "queries": [{
            "refId": "A",
            "datasource": { "type": "loki", "uid": "pulsus-loki" },
            "expr": r#"{service_name="checkout"}"#,
            "queryType": "range",
            "maxLines": 100,
        }],
        "from": from_ms.to_string(),
        "to": now_ms.to_string(),
    });

    let res = http
        .post(format!("{GRAFANA_BASE_URL}/api/ds/query"))
        .json(&request_body)
        .send()
        .await
        .context("POST /api/ds/query failed")?;
    if !res.status().is_success() {
        bail!("POST /api/ds/query returned {}", res.status());
    }
    let payload: serde_json::Value = res
        .json()
        .await
        .context("POST /api/ds/query body was not JSON")?;

    let result_a = payload
        .get("results")
        .and_then(|results| results.get("A"))
        .with_context(|| format!("no results.A in ds/query response: {payload}"))?;

    if let Some(error) = result_a.get("error") {
        bail!("Loki query_range through the compat alias errored: {error}");
    }
    if !result_a.get("frames").is_some_and(|f| f.is_array()) {
        bail!("results.A missing a frames array in ds/query response: {result_a}");
    }
    Ok(())
}

fn load_fixture_fields(path: &Path) -> Result<Vec<String>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read fixture {}", path.display()))?;
    let fields: Vec<String> = serde_json::from_str(&raw)
        .with_context(|| format!("fixture {} was not a JSON array of strings", path.display()))?;
    if fields.is_empty() {
        bail!("fixture {} listed no fields", path.display());
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenarios_is_non_empty_per_variant() {
        for variant in [Variant::Single, Variant::Cluster] {
            assert!(
                SCENARIOS.iter().any(|s| s.variants.contains(&variant)),
                "no scenarios registered for {variant:?}"
            );
        }
    }

    #[test]
    fn load_fixture_fields_reads_the_shipped_buildinfo_fixture() {
        let root = crate::engine::workspace_root();
        let fields =
            load_fixture_fields(&root.join("test/fixtures/ops/buildinfo.fields.json")).unwrap();
        assert_eq!(fields, vec!["version", "revision", "builtAt", "rustc"]);
    }

    #[test]
    fn load_fixture_fields_rejects_an_empty_list() {
        let dir = std::env::temp_dir().join("pulsus-e2e-test-empty-fixture");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty.json");
        std::fs::write(&path, "[]").unwrap();
        assert!(load_fixture_fields(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
