# pulsusdb

A Helm chart deploying PulsusDB — a lightweight, all-in-one observability database for logs, metrics, traces, and profiles, backed by ClickHouse. See the [repo README](../../../README.md) and [docs/](../../../docs/) for the product itself; this document covers the chart only.

## Quick start

```sh
helm install my-pulsusdb oci://ghcr.io/digitalis-io/charts/pulsusdb --version <chart-version>
```

The default install (`topology: single`, `clickhouse.enabled: true`) is a self-contained single-node stack — bundled ClickHouse, one all-mode `pulsusdb` Deployment, and an OTel Collector — the Kubernetes equivalent of [`deploy/e2e/compose.single.yaml`](../../e2e/compose.single.yaml).

`image.repository`/`image.tag` default to a placeholder (`ghcr.io/digitalis-io/pulsusdb`, tag unset) until the M7 release job publishes the application image — override with `--set image.tag=<released-tag>` for a real install.

## Two orthogonal topology axes

This chart models two independent scaling decisions as separate values, deliberately not conflated into one `topology` flag (issue #38 architect plan):

| Axis | Values key | What it controls |
|------|-----------|-------------------|
| **ClickHouse topology** | `topology: single \| cluster` | Whether the bundled ClickHouse is one StatefulSet or a sharded set (`clickhouse.shards`, `clickhouse.shards >= 2` required for `cluster`) + a Keeper StatefulSet |
| **PulsusDB process split** | `pulsusdb.split.enabled` | `false` (default): one `--mode all` Deployment (writer + reader in one process, [docs/architecture.md §1](../../../docs/architecture.md)). `true`: independently-scaled `--mode writer` and `--mode reader` Deployments |

Every combination of the two axes is valid (e.g. `topology: cluster` + `pulsusdb.split.enabled: false` is a single all-mode process talking to a sharded ClickHouse).

## Deployment, not StatefulSet

PulsusDB processes are stateless — "All durable state lives in ClickHouse … processes are stateless" ([docs/architecture.md §1](../../../docs/architecture.md)). The only writable path is the writer's insert-failure spool (`/var/lib/pulsusdb/spool`, [docs/configuration.md §5](../../../docs/configuration.md)), backed by a dedicated volume (`emptyDir` by default, optional PVC via `pulsusdb.spool.persistence`) — not identity-addressed storage. Every `pulsusdb` workload this chart renders is therefore a `Deployment`, never a `StatefulSet`. The bundled ClickHouse *is* a `StatefulSet` — it is genuinely stateful.

## Configuration model

`pulsusdb.config` carries a subset of [docs/configuration.md §9](../../../docs/configuration.md)'s YAML document schema (secrets excluded — see below), each key at the binary's default except `pulsusdb.config.clickhouse.server`, which is empty; `values.schema.json` rejects any other key. The ConfigMap template renders it into `config.yaml` with three rewrites: `pulsusdb.config.clickhouse.server` becomes the bundled ClickHouse's Service (shard 0's under `topology: cluster`) when `clickhouse.enabled: true`, and is passed as given otherwise (`values.schema.json` refuses surrounding whitespace); `cluster` becomes `clickhouse.clusterName` when `clickhouse.enabled: true` and `topology: cluster`; and `auth_user` is added from `pulsusdb.auth.user` when that is set. The file is mounted at `/etc/pulsusdb/config.yaml` and passed via `--config` (plus a per-Deployment `--mode` flag — `mode` is not a key in the rendered file). No chart value invents a config key `pulsus-config` doesn't understand.

Secrets are never rendered into the ConfigMap:

- `pulsusdb.auth.password` → `PULSUS_AUTH_PASSWORD` env (HTTP Basic auth; both `pulsusdb.auth.user`/`password` unset means no auth).
- The ClickHouse password → `CLICKHOUSE_AUTH=<user>:<password>` env, combining a plaintext username with a Secret-sourced password via Kubernetes' ["dependent environment variables"](https://kubernetes.io/docs/tasks/inject-data-application/define-interdependent-environment-variables/) `$(VAR)` substitution — there is no separate password-only env var upstream, so this is the only way to inject a Secret-sourced ClickHouse password without ever writing it to a ConfigMap.

Environment variables always win over the YAML file ([docs/configuration.md](../../../docs/configuration.md) precedence: CLI flags > env > YAML > defaults), so the Secret-sourced `CLICKHOUSE_AUTH`/`PULSUS_AUTH_PASSWORD` correctly override the ConfigMap's own (unset/default) values.

## ClickHouse: bundled vs. external

`clickhouse.enabled: true` (default) renders a bundled ClickHouse — a CI/compose-fixture-parity deployment (mirrors [`deploy/e2e/compose.single.yaml`](../../e2e/compose.single.yaml) and [`ci/clickhouse-cluster/`](../../../ci/clickhouse-cluster/)), **not a hardened, HA production ClickHouse**. It is never passwordless: a random password is generated on install (stable across upgrades — a `lookup`-guarded Secret) and wired into both the bundled server and `pulsusdb`'s `CLICKHOUSE_AUTH`. `clickhouse.image`/`clickhouse.keeperImage` are pinned to a full patch tag (`26.8.21.10`), not a floating `26.8` minor — re-verify the pin is still a published, non-EOL patch before bumping the chart's own default.

**Recommended production path: `clickhouse.enabled: false`** — point `pulsusdb.config.clickhouse.server` (+ `port`/`http_port`/`database`) at your own ClickHouse and supply credentials via `clickhouse.auth.existingSecret` (a Secret you manage, holding the password under `clickhouse.auth.existingSecretPasswordKey`, default `password`).

### AC #3 / cluster connection model

`topology: cluster` + `clickhouse.shards: N` renders `N` distinct ClickHouse StatefulSets (`<release>-clickhouse-shard-0..N-1`) plus a Keeper StatefulSet — but `pulsusdb.config.clickhouse.server` remains a **single** entry point (shard 0's Service), with `cluster: <clickhouse.clusterName>` set. This matches the product's real connection model: `CLICKHOUSE_SERVER` is one host ([docs/configuration.md §2](../../../docs/configuration.md)); ClickHouse's own `Distributed` engine (`remote_servers.xml`, rendered by this chart's ClickHouse ConfigMap and enumerating **all** shards) fans queries out from there. PulsusDB does not, and cannot today, load-balance connections across shards itself — a documented product gap tracked as a follow-up, not something this chart can work around.

`clickhouse.replicasPerShard` is restricted to `1` (schema `maximum: 1` + a template-time `fail` guard) — true multi-replica-per-shard support needs per-replica `macros.xml` identity and Keeper paths tied to StatefulSet ordinals, a documented follow-up.

### Digest-pinning images

All four images this chart can render support an immutable digest pin, `values.schema.json`-validated:

- `image.digest` (`sha256:<64 hex>`) is preferred over `image.tag` when set — `_helpers.tpl`'s `pulsusdb.image` renders `repository@sha256:...` and silently drops `tag` rather than producing an invalid combined ref.
- `clickhouse.image`, `clickhouse.keeperImage`, and `otelCollector.image` are single-string refs rendered verbatim; an optional `@sha256:<64 hex>` suffix on any of them is schema-validated (e.g. `--set clickhouse.image=docker.io/clickhouse/clickhouse-server:26.8.21.10@sha256:d3fd302416d9523d5d64239d5713d1c16c20ecea2045ac70d7e941b376cb266e`).

### Inter-node authentication (cluster mode)

Sharded ClickHouse needs two more credentials beyond the client password above, both Secret-sourced via `from_env` — **never a literal** in the rendered `remote_servers.xml`/`interserver-credentials.xml` ConfigMap (code review round-1 finding #1):

- `remote_servers.xml`'s `<secret>` element — authenticates distributed-query traffic between cluster nodes.
- `interserver_http_credentials` — authenticates the interserver-HTTP replication protocol (part-fetch traffic between shards; real traffic even at `replicasPerShard: 1`, since docs/schemas.md §7's shard-less bookkeeping/catalog replica set makes shard-to-shard replication happen regardless).

Both pull the same generated Secret value (`clickhouse.auth.existingSecretInterserverKey`, default key `interserver-secret`) — an `existingSecret` for cluster mode must carry this key alongside the password key.

## Probe contract — do not deviate

| Probe | Handler | Effect on failure |
|-------|---------|---------------------|
| **liveness** | TCP `pulsusdb.config.port` | **restarts the pod** — the only probe that ever does |
| **readiness** | `GET /ready` on `pulsusdb.config.port` | removes the pod from Service endpoints only — **never restarts** |
| **startupProbe** | TCP `pulsusdb.config.port` | gates liveness until the port is bound |

`/ready` returns `200` only when ClickHouse is reachable and (in reader-enabled modes) the label cache is warm ([docs/api.md §7](../../../docs/api.md)) — it is *supposed to* fail during a ClickHouse outage or cold start. Wiring liveness or startupProbe to `/ready` would restart-loop every pod during any ClickHouse outage or slow reader warmup. The string `/ready` appears in `readinessProbe` only, in every rendered template — this is load-bearing, not a style choice.

**Single source of truth for the port** (code review round-1 finding #8): `containerPort` and every probe's port are all derived from `pulsusdb.config.port` (`_helpers.tpl`'s `pulsusdb.httpPort` helper) — never the independently-defaulted `service.port`. `service.port` remains distinct and independently settable: it is the *Service's* own external port, and the Service's `targetPort` is name-based (`http`), so it always resolves to whatever the container actually listens on regardless of `service.port`'s value.

ClickHouse's own bundled pods (single-node, sharded, and Keeper) deliberately keep HTTP `/ping` (ClickHouse) / exec `ruok` (Keeper) liveness+readiness instead of TCP — this is not an oversight. The TCP-only contract above exists specifically to stop an *external* ClickHouse outage from restart-storming *pulsusdb*; ClickHouse itself has no such external dependency, and a genuinely wedged ClickHouse process *should* be restarted (see the inline comment on `templates/clickhouse-statefulset-single.yaml`).

## The schema Job

`initJob.enabled` (default `true`) renders a plain (non-hook) Job that runs `schema/schema.sh` from the image. It is the only thing that creates the schema: the binary creates none, and a serving pod stays unready (`/ready` returns `503`) until the schema exists, retrying on its own. The script drops the database and builds it fresh. The Job's name carries a digest of the Job's whole `spec`, so an upgrade that changes anything the Job carries (a schema parameter, the image, `initJob.resources`, `initJob.backoffLimit`, the pod security contexts, `pulsusdb.extraEnv`) renders a new Job, which rebuilds the database and discards what it holds; an upgrade that changes nothing the Job carries leaves it as it is. With `initJob.enabled: false`, run `schema/schema.sh` against the database yourself; until then nothing serves.

## Rollouts on config/secret change

Every Deployment and StatefulSet pod template carries `checksum/config` and/or `checksum/secret` annotations (code review round-1 finding #3), computed from the relevant ConfigMap/Secret's rendered content — Deployments and StatefulSets never watch their mounted ConfigMap/Secret for in-place updates on their own, so without this a `helm upgrade` that only changes `pulsusdb.config.*` would silently leave already-running pods on the old config. The schema Job's pod template carries none: a Job is never rolled, and its name follows its `spec` instead (see "The schema Job"). The pulsusdb config volume is mounted as the whole ConfigMap directory (not a single `subPath`-mounted file) for the same reason — `subPath` mounts never receive live kubelet ConfigMap updates, which would defeat the point of the checksum-triggered rollout the moment a pod happened to survive on stale content for an unrelated reason.

**The bundled ClickHouse credentials Secret is a documented exception** (code review round-2 finding #2): its `checksum/secret` is a hash of the `clickhouse.auth` *values* subtree (which `existingSecret`/keys are wired in) plus the resolved Secret name — deterministic, but it cannot see the Secret's live *content*, only the chart's own configuration of which Secret to use. This is a genuine Helm limitation, not an oversight: `lookup()` results (which is how the chart reads an already-existing generated password so upgrades never rotate it) are excluded from Helm's diffing and are unreliable/absent under `--dry-run`, so no chart can deterministically fingerprint live Secret content this way. **If you rotate the generated ClickHouse password out-of-band** (edit the Secret directly, rather than through `helm upgrade`), existing pods will not automatically roll — run `kubectl rollout restart` on every pulsusdb and ClickHouse workload in the release afterward. `templates/NOTES.txt` repeats this at install time.

## Long release names

Every name this chart generates stays within its kind's limit up to Helm's own 53-character release-name cap (`_helpers.tpl`'s `pulsusdb.componentFullname`, code review round-1 finding #7): each component's suffix (`-writer`, `-reader`, `-clickhouse-shard-N`, `-clickhouse-keeper`, `-otel-collector`, ...) is reserved *before* truncating the base name to 63 characters, so two different components can never collide onto the same truncated prefix regardless of release-name length. Services, Deployments and the schema Job, its digest suffix included, stay within 63 characters; StatefulSets stay within 52, because their pods carry a `controller-revision-hash` label of `<name>-<hash>`. `tests/render_test.sh` check D renders every name with a 53-character release name and holds each kind to its limit.

## Grafana Loki-compat datasource

`grafana.datasourceProvisioning.enabled` (default `false`) renders a ConfigMap labelled `grafana_datasource: "1"` (consumable by the Grafana community chart's sidecar) provisioning a Loki-compatible datasource against the query Service. It **only works when `pulsusdb.config.compat_endpoints: true`** ([docs/configuration.md §1](../../../docs/configuration.md), [docs/api.md §8](../../../docs/api.md)) — enabling the datasource without `compat_endpoints` produces a datasource that 404s on every query; `NOTES.txt` warns about this at install time.

## Values reference

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `nameOverride` / `fullnameOverride` | string | `""` / `""` | Standard Helm name overrides |
| `image.repository` | string | `ghcr.io/digitalis-io/pulsusdb` | Placeholder until M7 publishes the image |
| `image.tag` | string | `""` | Falls back to `.Chart.AppVersion`; dropped when `image.digest` is set |
| `image.digest` | string | `""` | `sha256:<64 hex>` — preferred over `image.tag` when set |
| `image.pullPolicy` | string | `IfNotPresent` | |
| `imagePullSecrets` | list | `[]` | |
| `topology` | enum | `single` | `single` \| `cluster` — ClickHouse topology axis |
| `pulsusdb.split.enabled` | bool | `false` | `false`: one all-mode Deployment. `true`: writer+reader Deployments |
| `pulsusdb.replicaCount` | int | `1` | All-mode replica count |
| `pulsusdb.resources` | object | `{requests: {cpu: 250m, memory: 512Mi}, limits: {cpu: "2", memory: 2Gi}}` | All-mode resources |
| `pulsusdb.writer.replicaCount` | int | `2` | Split-mode writer tier |
| `pulsusdb.writer.resources` | object | `{requests: {cpu: 250m, memory: 256Mi}, limits: {cpu: "1", memory: 1Gi}}` | Split-mode writer tier |
| `pulsusdb.reader.replicaCount` | int | `2` | Split-mode reader tier |
| `pulsusdb.reader.resources` | object | `{requests: {cpu: 250m, memory: 512Mi}, limits: {cpu: "2", memory: 2Gi}}` | Split-mode reader tier |
| `pulsusdb.config.*` | object | see `values.yaml` | A subset of docs/configuration.md §9, at the binary defaults except `pulsusdb.config.clickhouse.server` |
| `pulsusdb.auth.user` / `.password` / `.existingSecret` | string | `""` | HTTP Basic auth (off when all unset); `user` and a password source (`password` or `existingSecret`) must be set together, and `password`/`existingSecret` are mutually exclusive — enforced by a render-time `fail` guard |
| `pulsusdb.extraEnv` | list | `[]` | Escape hatch, `[{name, value\|valueFrom}, ...]` |
| `pulsusdb.spool.persistence.enabled` / `.size` / `.storageClass` | bool / string / string | `false` / `5Gi` / `""` | PVC for the insert-failure spool instead of `emptyDir` |
| `initJob.enabled` | bool | `true` | The schema Job — see above |
| `initJob.retentionDays` | int | none: required while `initJob.enabled` | The schema's retention in days, `PULSUS_RETENTION_DAYS` (docs/configuration.md §7). Without it the chart does not render; the serving pods read it back from the database |
| `initJob.resources` | object | `{requests: {cpu: 100m, memory: 256Mi}, limits: {cpu: "1", memory: 1Gi}}` | The schema Job's resources |
| `initJob.backoffLimit` | int | `10` | The schema Job's retry limit: how many times a failed run is retried before the Job is marked failed |
| `clickhouse.enabled` | bool | `true` | `false` => external/production ClickHouse |
| `clickhouse.image` | string | `docker.io/clickhouse/clickhouse-server:26.8.21.10` | Pinned to a full patch tag; an `@sha256:` suffix is schema-validated |
| `clickhouse.keeperImage` | string | `docker.io/clickhouse/clickhouse-keeper:26.8.21.10` | As `clickhouse.image`, for Keeper |
| `clickhouse.shards` | int | `1` | `>= 2` required when `topology: cluster` |
| `clickhouse.replicasPerShard` | int | `1` | Restricted to `1` — see above |
| `clickhouse.clusterName` | string | `pulsus_cluster` | |
| `clickhouse.auth.username` / `.existingSecret` / `.existingSecretPasswordKey` / `.existingSecretInterserverKey` | string | `default` / `""` / `password` / `interserver-secret` | Bundled or external ClickHouse credentials |
| `clickhouse.resources` | object | `{requests: {cpu: 500m, memory: 1Gi}, limits: {cpu: "2", memory: 4Gi}}` | Bundled ClickHouse pods |
| `clickhouse.keeperResources` | object | `{requests: {cpu: 100m, memory: 256Mi}, limits: {cpu: "1", memory: 1Gi}}` | Bundled Keeper pods |
| `clickhouse.podSecurityContext` / `.keeperPodSecurityContext` | object | `{runAsNonRoot: true, runAsUser: 101, runAsGroup: 101, fsGroup: 101, seccompProfile: {type: RuntimeDefault}}` | Bundled ClickHouse and Keeper pods, separately overridable |
| `clickhouse.securityContext` / `.keeperSecurityContext` | object | `{allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}` | Bundled ClickHouse and Keeper containers, separately overridable |
| `persistence.enabled` / `.size` / `.storageClass` / `.keeperSize` | bool / string / string / string | `true` / `20Gi` / `""` / `5Gi` | ClickHouse data volumes |
| `otelCollector.enabled` | bool | `true` | |
| `otelCollector.image` | string | `docker.io/otel/opentelemetry-collector-contrib:0.110.0` | Pinned tag |
| `otelCollector.resources` | object | `{requests: {cpu: 100m, memory: 256Mi}, limits: {cpu: "1", memory: 512Mi}}` | |
| `otelCollector.config` | object | `{}` | Passthrough escape hatch — non-empty replaces the chart's own release-aware collector config entirely (not deep-merged) |
| `otelCollector.podSecurityContext` | object | `{runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001, seccompProfile: {type: RuntimeDefault}}` | |
| `otelCollector.securityContext` | object | `{allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}` | |
| `grafana.datasourceProvisioning.enabled` | bool | `false` | See above |
| `serviceAccount.create` / `.name` / `.annotations` | bool / string / object | `true` / `""` / `{}` | |
| `service.type` / `.port` | string / int | `ClusterIP` / `3100` | The Service's own external port — distinct from `pulsusdb.config.port` (see probe contract) |
| `podSecurityContext` | object | `{runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001, fsGroup: 10001, seccompProfile: {type: RuntimeDefault}}` | |
| `securityContext` | object | `{allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}` | Safe only because `spool`/`tmp` are dedicated writable volumes |
| `livenessProbe` | object | `{tcpSocket: {port: 3100}, initialDelaySeconds: 5, periodSeconds: 10, failureThreshold: 3}` | Port fields are always overridden from `pulsusdb.config.port`, regardless of what's set here |
| `readinessProbe` | object | `{httpGet: {path: /ready, port: 3100}, periodSeconds: 5, failureThreshold: 3}` | As `livenessProbe` |
| `startupProbe` | object | `{tcpSocket: {port: 3100}, failureThreshold: 60, periodSeconds: 5}` | As `livenessProbe` |
| `ingress.enabled` | bool | `false` | Targets the query Service |
| `ingress.className` / `.annotations` / `.tls` | string / object / list | `""` / `{}` / `[]` | |
| `ingress.hosts` | list | `[{host: pulsusdb.example.com, paths: [{path: /, pathType: Prefix}]}]` | |
| `hpa.enabled` | bool | `false` | Disabled skeleton only — no functional autoscaling |
| `hpa.minReplicas` / `.maxReplicas` / `.targetCPUUtilizationPercentage` | int | `1` / `5` / `80` | |
| `podDisruptionBudget.enabled` / `.minAvailable` | bool / int | `false` / `1` | |
| `podAnnotations` / `podLabels` / `nodeSelector` / `affinity` | object | `{}` / `{}` / `{}` / `{}` | The pulsusdb Deployments only |
| `tolerations` | list | `[]` | The pulsusdb Deployments only |

Every key in `values.yaml` is in this table; `helm show values deploy/charts/pulsusdb` prints them with their comments.

## Testing

- `tests/unit/*.yaml` — [helm-unittest](https://github.com/helm-unittest/helm-unittest) render/schema specs (`helm unittest deploy/charts/pulsusdb`).
- `tests/golden/*.yaml` — `helm template` snapshots for manifest-drift review.
- `tests/bdd/` — `pytest-bdd` Gherkin `.feature` scenarios driving a live Kind install lifecycle (see `tests/bdd/conftest.py`'s module docstring / `.github/workflows/helm-chart.yml`'s `chart-test-kind` job for how CI runs them).

## Known limitations

- `values.schema.json` cannot express `requests <= limits` quantity ordering — Kubernetes' own API-server admission already rejects `requests > limits`, proven behaviourally by a `pytest-bdd` scenario instead of re-implemented here.
- The bundled ClickHouse is CI/compose-fixture parity, not hardened HA — see "ClickHouse: bundled vs. external" above.
- `clickhouse.replicasPerShard > 1` is not implemented (documented follow-up).
- PulsusDB-side multi-shard endpoint load-balancing does not exist yet (documented product-level follow-up, not a chart limitation).
- HPA/VPA autoscaling ships as a disabled skeleton only.
