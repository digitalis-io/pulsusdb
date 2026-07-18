//! Availability-zone auto-detection from cloud instance metadata (issue #43,
//! owner scope extension).
//!
//! This is the ONLY place that decides how [`Config::availability_zone`] is
//! populated at startup. The pool's connection-spreading / zone-preference /
//! failover mechanics never see this module — they consume the resolved
//! `local_zone: Option<String>` exactly as they would an operator-supplied
//! value. Precedence (highest first):
//!
//! 1. an explicitly-set `availability_zone` / `PULSUS_AVAILABILITY_ZONE` —
//!    the operator override, ALWAYS wins and skips detection entirely;
//! 2. `az_detect` (`PULSUS_AZ_DETECT`) naming a cloud (or `auto`) — one-time
//!    detection at startup from the provider's instance-metadata service;
//! 3. otherwise (`off`, the default) — unset, i.e. spread evenly across all
//!    endpoints (backward-compatible).
//!
//! Detection is **fail-soft**: any error/timeout/miss leaves the zone unset
//! (even spreading) rather than blocking startup — the metadata service is a
//! link-local address that simply will not answer off-cloud. Each probe is
//! individually time-bounded (see [`PROBE_TIMEOUT`]) so a blocked IMDS can
//! never wedge startup.
//!
//! The HTTP calls sit behind the [`MetadataClient`] seam so every parse rule
//! and precedence branch is covered by hermetic tests that inject canned
//! per-cloud responses — no real network. Live IMDS is not reachable in CI
//! and is therefore not exercised there.

use std::time::Duration;

use pulsus_config::{AzDetect, Config};

/// Per-probe timeout (connect + read). IMDS is a link-local endpoint; a
/// couple of seconds is generous on-cloud and short enough that an off-cloud
/// or firewalled node falls back to unzoned promptly.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// HTTP verb for a metadata probe (AWS IMDSv2 needs a `PUT` for its token).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MetaMethod {
    Get,
    Put,
}

/// A metadata-fetch failure. Detection treats every variant identically
/// (fail-soft → unzoned); the cause is surfaced in a trace via [`Display`]
/// (see [`fetch_logged`]) and tests use the variants to force the failure
/// path.
#[derive(Debug)]
pub(crate) enum MetaError {
    Transport(String),
    Status(u16),
}

impl std::fmt::Display for MetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaError::Transport(msg) => write!(f, "transport error: {msg}"),
            MetaError::Status(code) => write!(f, "http status {code}"),
        }
    }
}

/// The single HTTP seam metadata detection depends on. Real deployments use
/// [`ReqwestMetadataClient`]; tests inject a fake returning canned bodies.
pub(crate) trait MetadataClient: Send + Sync {
    fn fetch(
        &self,
        method: MetaMethod,
        url: String,
        headers: Vec<(&'static str, String)>,
    ) -> impl std::future::Future<Output = Result<String, MetaError>> + Send;
}

/// The three providers' metadata base URLs. Injectable so tests can point a
/// fake client at arbitrary bases; production uses [`ProviderUrls::real`].
#[derive(Clone, Debug)]
pub(crate) struct ProviderUrls {
    pub aws: String,
    pub gcp: String,
    pub azure: String,
}

impl ProviderUrls {
    fn real() -> Self {
        Self {
            aws: "http://169.254.169.254".to_string(),
            gcp: "http://metadata.google.internal".to_string(),
            azure: "http://169.254.169.254".to_string(),
        }
    }
}

/// Issues one probe, logging (and swallowing) any failure so detection stays
/// fail-soft. Logging via `Display` also surfaces the failure cause.
async fn fetch_logged<C: MetadataClient>(
    c: &C,
    cloud: &'static str,
    method: MetaMethod,
    url: String,
    headers: Vec<(&'static str, String)>,
) -> Option<String> {
    match c.fetch(method, url, headers).await {
        Ok(body) => Some(body),
        Err(e) => {
            tracing::debug!(cloud, error = %e, "cloud instance-metadata probe failed");
            None
        }
    }
}

/// AWS **IMDSv2** (token-required; IMDSv1 is deliberately not used — SSRF
/// hardening): `PUT /latest/api/token`, then `GET
/// /latest/meta-data/placement/availability-zone` with the token header.
async fn detect_aws<C: MetadataClient>(c: &C, base: &str) -> Option<String> {
    let token = fetch_logged(
        c,
        "aws",
        MetaMethod::Put,
        format!("{base}/latest/api/token"),
        vec![("X-aws-ec2-metadata-token-ttl-seconds", "21600".to_string())],
    )
    .await?;
    let token = token.trim().to_string();
    if token.is_empty() {
        return None;
    }
    let az = fetch_logged(
        c,
        "aws",
        MetaMethod::Get,
        format!("{base}/latest/meta-data/placement/availability-zone"),
        vec![("X-aws-ec2-metadata-token", token)],
    )
    .await?;
    non_empty(az.trim())
}

/// GCP: `GET /computeMetadata/v1/instance/zone` with `Metadata-Flavor:
/// Google`; the body is `projects/<num>/zones/<zone>` — take the trailing
/// path segment.
async fn detect_gcp<C: MetadataClient>(c: &C, base: &str) -> Option<String> {
    let body = fetch_logged(
        c,
        "gcp",
        MetaMethod::Get,
        format!("{base}/computeMetadata/v1/instance/zone"),
        vec![("Metadata-Flavor", "Google".to_string())],
    )
    .await?;
    parse_gcp_zone(&body)
}

/// Azure: `GET /metadata/instance/compute/zone?...&format=text` with
/// `Metadata: true`; the body is the raw zone string (empty on a
/// non-zonal VM).
async fn detect_azure<C: MetadataClient>(c: &C, base: &str) -> Option<String> {
    let body = fetch_logged(
        c,
        "azure",
        MetaMethod::Get,
        format!("{base}/metadata/instance/compute/zone?api-version=2021-02-01&format=text"),
        vec![("Metadata", "true".to_string())],
    )
    .await?;
    non_empty(body.trim())
}

fn parse_gcp_zone(body: &str) -> Option<String> {
    non_empty(body.trim().rsplit('/').next().unwrap_or("").trim())
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Runs detection for `provider` against `urls`, returning the first zone a
/// probe yields (`auto` tries AWS, then GCP, then Azure). `Off` never probes.
pub(crate) async fn detect_zone<C: MetadataClient>(
    c: &C,
    provider: AzDetect,
    urls: &ProviderUrls,
) -> Option<String> {
    match provider {
        AzDetect::Off => None,
        AzDetect::Aws => detect_aws(c, &urls.aws).await,
        AzDetect::Gcp => detect_gcp(c, &urls.gcp).await,
        AzDetect::Azure => detect_azure(c, &urls.azure).await,
        AzDetect::Auto => {
            if let Some(z) = detect_aws(c, &urls.aws).await {
                return Some(z);
            }
            if let Some(z) = detect_gcp(c, &urls.gcp).await {
                return Some(z);
            }
            detect_azure(c, &urls.azure).await
        }
    }
}

/// The production [`MetadataClient`], backed by `reqwest` with a short
/// per-request timeout and **proxies disabled** (a configured HTTP proxy
/// must never intercept a link-local metadata call — SSRF hardening).
struct ReqwestMetadataClient {
    client: reqwest::Client,
}

impl ReqwestMetadataClient {
    fn new() -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(PROBE_TIMEOUT)
            .connect_timeout(PROBE_TIMEOUT)
            .no_proxy()
            .build()?;
        Ok(Self { client })
    }
}

impl MetadataClient for ReqwestMetadataClient {
    async fn fetch(
        &self,
        method: MetaMethod,
        url: String,
        headers: Vec<(&'static str, String)>,
    ) -> Result<String, MetaError> {
        let mut req = match method {
            MetaMethod::Get => self.client.get(&url),
            MetaMethod::Put => self.client.put(&url),
        };
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| MetaError::Transport(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(MetaError::Status(status.as_u16()));
        }
        resp.text()
            .await
            .map_err(|e| MetaError::Transport(e.to_string()))
    }
}

/// Resolves `config.availability_zone` at startup (issue #43). No-op when the
/// operator set the zone explicitly (override wins) or `az_detect` is `off`.
/// Fail-soft: on any detection failure the zone stays unset (even spreading).
pub(crate) async fn resolve_local_zone(config: &mut Config) {
    if config.availability_zone.is_some() || config.az_detect == AzDetect::Off {
        return;
    }
    let client = match ReqwestMetadataClient::new() {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "could not build the metadata HTTP client; leaving the availability zone unset");
            return;
        }
    };
    match detect_zone(&client, config.az_detect, &ProviderUrls::real()).await {
        Some(zone) => {
            tracing::info!(zone = %zone, provider = ?config.az_detect, "detected availability zone from cloud instance metadata");
            config.availability_zone = Some(zone);
        }
        None => {
            tracing::warn!(provider = ?config.az_detect, "availability-zone auto-detection found no zone; spreading connections evenly across all ClickHouse endpoints");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// One recorded probe: verb, URL, and its headers.
    type SeenCall = (MetaMethod, String, Vec<(&'static str, String)>);

    /// A fake metadata client: canned responses keyed by exact URL, so tests
    /// assert the exact path/verb each provider probe uses. A missing URL is
    /// a transport error (models an unreachable endpoint / non-cloud host).
    struct FakeClient {
        responses: HashMap<String, Result<String, MetaError>>,
        /// URLs seen, to assert e.g. AWS never issues a v1 (tokenless) GET.
        seen: std::sync::Mutex<Vec<SeenCall>>,
    }

    impl FakeClient {
        fn new(pairs: Vec<(&str, Result<String, MetaError>)>) -> Self {
            Self {
                responses: pairs.into_iter().map(|(u, r)| (u.to_string(), r)).collect(),
                seen: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl MetadataClient for FakeClient {
        async fn fetch(
            &self,
            method: MetaMethod,
            url: String,
            headers: Vec<(&'static str, String)>,
        ) -> Result<String, MetaError> {
            self.seen
                .lock()
                .unwrap()
                .push((method, url.clone(), headers));
            match self.responses.get(&url) {
                Some(Ok(body)) => Ok(body.clone()),
                Some(Err(MetaError::Status(c))) => Err(MetaError::Status(*c)),
                Some(Err(MetaError::Transport(m))) => Err(MetaError::Transport(m.clone())),
                None => Err(MetaError::Transport("unreachable (no route)".to_string())),
            }
        }
    }

    fn urls() -> ProviderUrls {
        ProviderUrls {
            aws: "http://aws".to_string(),
            gcp: "http://gcp".to_string(),
            azure: "http://azure".to_string(),
        }
    }

    #[tokio::test]
    async fn aws_imdsv2_token_then_az() {
        let c = FakeClient::new(vec![
            ("http://aws/latest/api/token", Ok("tok-123".to_string())),
            (
                "http://aws/latest/meta-data/placement/availability-zone",
                Ok("us-east-1a".to_string()),
            ),
        ]);
        assert_eq!(
            detect_zone(&c, AzDetect::Aws, &urls()).await.as_deref(),
            Some("us-east-1a")
        );
        // IMDSv2 only: the token call is a PUT with the TTL header, and the
        // AZ call carries the token header — never a tokenless v1 GET.
        let seen = c.seen.lock().unwrap();
        assert_eq!(seen[0].0, MetaMethod::Put);
        assert!(
            seen[0]
                .2
                .iter()
                .any(|(k, _)| *k == "X-aws-ec2-metadata-token-ttl-seconds")
        );
        assert_eq!(seen[1].0, MetaMethod::Get);
        assert!(
            seen[1]
                .2
                .iter()
                .any(|(k, v)| *k == "X-aws-ec2-metadata-token" && v == "tok-123")
        );
    }

    #[tokio::test]
    async fn aws_without_token_yields_no_zone_and_never_falls_back_to_v1() {
        // Token PUT fails (IMDSv2 disabled/blocked). Detection must NOT try a
        // tokenless v1 GET; it simply returns None.
        let c = FakeClient::new(vec![(
            "http://aws/latest/api/token",
            Err(MetaError::Status(403)),
        )]);
        assert_eq!(detect_zone(&c, AzDetect::Aws, &urls()).await, None);
        let seen = c.seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "no second (v1) request after a failed token");
    }

    #[tokio::test]
    async fn gcp_parses_trailing_zone_from_path() {
        let c = FakeClient::new(vec![(
            "http://gcp/computeMetadata/v1/instance/zone",
            Ok("projects/123456789/zones/us-central1-a".to_string()),
        )]);
        assert_eq!(
            detect_zone(&c, AzDetect::Gcp, &urls()).await.as_deref(),
            Some("us-central1-a")
        );
        let seen = c.seen.lock().unwrap();
        assert!(
            seen[0]
                .2
                .iter()
                .any(|(k, v)| *k == "Metadata-Flavor" && v == "Google")
        );
    }

    #[tokio::test]
    async fn azure_returns_raw_zone_text() {
        let c = FakeClient::new(vec![(
            "http://azure/metadata/instance/compute/zone?api-version=2021-02-01&format=text",
            Ok("2".to_string()),
        )]);
        assert_eq!(
            detect_zone(&c, AzDetect::Azure, &urls()).await.as_deref(),
            Some("2")
        );
        let seen = c.seen.lock().unwrap();
        assert!(
            seen[0]
                .2
                .iter()
                .any(|(k, v)| *k == "Metadata" && v == "true")
        );
    }

    #[tokio::test]
    async fn azure_empty_zone_is_unzoned() {
        let c = FakeClient::new(vec![(
            "http://azure/metadata/instance/compute/zone?api-version=2021-02-01&format=text",
            Ok("   ".to_string()),
        )]);
        assert_eq!(detect_zone(&c, AzDetect::Azure, &urls()).await, None);
    }

    #[tokio::test]
    async fn auto_tries_each_provider_and_first_success_wins() {
        // AWS token unreachable, GCP answers -> GCP zone; Azure never tried.
        let c = FakeClient::new(vec![
            (
                "http://gcp/computeMetadata/v1/instance/zone",
                Ok("projects/1/zones/europe-west1-b".to_string()),
            ),
            (
                "http://azure/metadata/instance/compute/zone?api-version=2021-02-01&format=text",
                Ok("3".to_string()),
            ),
        ]);
        assert_eq!(
            detect_zone(&c, AzDetect::Auto, &urls()).await.as_deref(),
            Some("europe-west1-b")
        );
        let seen = c.seen.lock().unwrap();
        assert!(
            !seen.iter().any(|(_, u, _)| u.contains("azure")),
            "Azure must not be probed once GCP succeeds"
        );
    }

    #[tokio::test]
    async fn detection_failure_falls_back_to_unzoned() {
        // Every probe unreachable (off-cloud / IMDS blocked) -> None.
        let c = FakeClient::new(vec![]);
        assert_eq!(detect_zone(&c, AzDetect::Auto, &urls()).await, None);
    }

    #[tokio::test]
    async fn off_never_probes() {
        let c = FakeClient::new(vec![]);
        assert_eq!(detect_zone(&c, AzDetect::Off, &urls()).await, None);
        assert!(c.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn explicit_zone_wins_over_detection() {
        let mut config = Config {
            availability_zone: Some("operator-set".to_string()),
            az_detect: AzDetect::Auto,
            ..Config::default()
        };
        // resolve_local_zone would only probe the (unreachable) real IMDS;
        // with the zone already set it must short-circuit and keep it.
        resolve_local_zone(&mut config).await;
        assert_eq!(config.availability_zone.as_deref(), Some("operator-set"));
    }

    #[tokio::test]
    async fn off_leaves_zone_unset_without_probing() {
        let mut config = Config::default();
        assert_eq!(config.az_detect, AzDetect::Off);
        resolve_local_zone(&mut config).await;
        assert_eq!(config.availability_zone, None);
    }

    #[test]
    fn parse_gcp_zone_handles_bare_and_pathed_and_empty() {
        assert_eq!(
            parse_gcp_zone("projects/9/zones/us-central1-a").as_deref(),
            Some("us-central1-a")
        );
        assert_eq!(
            parse_gcp_zone("us-central1-a").as_deref(),
            Some("us-central1-a")
        );
        assert_eq!(parse_gcp_zone("  ").as_deref(), None);
    }
}
