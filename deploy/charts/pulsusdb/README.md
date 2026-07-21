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
| **PulsusDB process split** | `pulsusdb.split.enabled` | `false` (default): one `--mode all` Deployment (schema controller + writer + reader in one process, [docs/architecture.md §1](../../../docs/architecture.md)). `true`: independently-scaled `--mode writer` and `--mode reader` Deployments |

Every combination of the two axes is valid (e.g. `topology: cluster` + `pulsusdb.split.enabled: false` is a single all-mode process talking to a sharded ClickHouse).

## Deployment, not StatefulSet

PulsusDB processes are stateless — "All durable state lives in ClickHouse … processes are stateless" ([docs/architecture.md §1](../../../docs/architecture.md)). The only writable path is the writer's insert-failure spool (`/var/lib/pulsusdb/spool`, [docs/configuration.md §5](../../../docs/configuration.md)), backed by a dedicated volume (`emptyDir` by default, optional PVC via `pulsusdb.spool.persistence`) — not identity-addressed storage. Every `pulsusdb` workload this chart renders is therefore a `Deployment`, never a `StatefulSet`. The bundled ClickHouse *is* a `StatefulSet` — it is genuinely stateful.

## Configuration model

`pulsusdb.config` maps 1:1 onto [docs/configuration.md §9](../../../docs/configuration.md)'s complete YAML document schema (secrets excluded — see below) and is rendered verbatim into a ConfigMap, mounted at `/etc/pulsusdb/config.yaml`, passed via `--config` (plus a per-Deployment `--mode` flag — `mode` is not a key in the rendered file). No chart value invents a config key `pulsus-config` doesn't understand.

Secrets are never rendered into the ConfigMap:

- `pulsusdb.auth.password` → `PULSUS_AUTH_PASSWORD` env (HTTP Basic auth; both `pulsusdb.auth.user`/`password` unset means no auth).
- The ClickHouse password → `CLICKHOUSE_AUTH=<user>:<password>` env, combining a plaintext username with a Secret-sourced password via Kubernetes' ["dependent environment variables"](https://kubernetes.io/docs/tasks/inject-data-application/define-interdependent-environment-variables/) `$(VAR)` substitution — there is no separate password-only env var upstream, so this is the only way to inject a Secret-sourced ClickHouse password without ever writing it to a ConfigMap.

Environment variables always win over the YAML file ([docs/configuration.md](../../../docs/configuration.md) precedence: CLI flags > env > YAML > defaults), so the Secret-sourced `CLICKHOUSE_AUTH`/`PULSUS_AUTH_PASSWORD` correctly override the ConfigMap's own (unset/default) values.

## ClickHouse: bundled vs. external

`clickhouse.enabled: true` (default) renders a bundled ClickHouse — a CI/compose-fixture-parity deployment (mirrors [`deploy/e2e/compose.single.yaml`](../../e2e/compose.single.yaml) and [`ci/clickhouse-cluster/`](../../../ci/clickhouse-cluster/)), **not a hardened, HA production ClickHouse**. It is never passwordless: a random password is generated on install (stable across upgrades — a `lookup`-guarded Secret) and wired into both the bundled server and `pulsusdb`'s `CLICKHOUSE_AUTH`. `clickhouse.image`/`clickhouse.keeperImage` are pinned to a full patch tag (`24.8.14`), not a floating `24.8` minor — re-verify the pin is still a published, non-EOL patch before bumping the chart's own default.

**Recommended production path: `clickhouse.enabled: false`** — point `pulsusdb.config.clickhouse.server` (+ `port`/`http_port`/`database`) at your own ClickHouse and supply credentials via `clickhouse.auth.existingSecret` (a Secret you manage, holding the password under `clickhouse.auth.existingSecretPasswordKey`, default `password`).

### AC #3 / cluster connection model

`topology: cluster` + `clickhouse.shards: N` renders `N` distinct ClickHouse StatefulSets (`<release>-clickhouse-shard-0..N-1`) plus a Keeper StatefulSet — but `pulsusdb.config.clickhouse.server` remains a **single** entry point (shard 0's Service), with `cluster: <clickhouse.clusterName>` set. This matches the product's real connection model: `CLICKHOUSE_SERVER` is one host ([docs/configuration.md §2](../../../docs/configuration.md)); ClickHouse's own `Distributed` engine (`remote_servers.xml`, rendered by this chart's ClickHouse ConfigMap and enumerating **all** shards) fans queries out from there. PulsusDB does not, and cannot today, load-balance connections across shards itself — a documented product gap tracked as a follow-up, not something this chart can work around.

`clickhouse.replicasPerShard` is restricted to `1` (schema `maximum: 1` + a template-time `fail` guard) — true multi-replica-per-shard support needs per-replica `macros.xml` identity and Keeper paths tied to StatefulSet ordinals, a documented follow-up.

### Digest-pinning images

All four images this chart can render support an immutable digest pin, `values.schema.json`-validated:

- `image.digest` (`sha256:<64 hex>`) is preferred over `image.tag` when set — `_helpers.tpl`'s `pulsusdb.image` renders `repository@sha256:...` and silently drops `tag` rather than producing an invalid combined ref.
- `clickhouse.image`, `clickhouse.keeperImage`, and `otelCollector.image` are single-string refs rendered verbatim; an optional `@sha256:<64 hex>` suffix on any of them is schema-validated (e.g. `--set clickhouse.image=docker.io/clickhouse/clickhouse-server:24.8.14@sha256:<digest>`).

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

## No install-path init Job

Every serving pod (`all`/`writer`/`reader`) self-reconciles its own ClickHouse schema before it can serve: `crates/pulsus-server/src/serve.rs`'s `ensure_schema_then_connect` runs strictly before the connection pool is published, and `/ready` is `503` until that succeeds. Reconciliation is idempotent, so N replicas each self-reconciling on first boot is safe by contract. This chart therefore does **not** wire a schema-migration Job into `helm install`/`upgrade` — there is nothing for it to gate.

`initJob.enabled` (default `false`) ships a disabled-by-default, plain (non-hook) `--mode init` Job purely as an operator convenience for very large split/cluster fleets that want first-boot DDL serialized ahead of the first rollout. Normally unnecessary.

## Rollouts on config/secret change

Every pod template carries `checksum/config`/`checksum/secret` annotations (code review round-1 finding #3), computed from the relevant ConfigMap/Secret's rendered content — Deployments and StatefulSets never watch their mounted ConfigMap/Secret for in-place updates on their own, so without this a `helm upgrade` that only changes `pulsusdb.config.*` would silently leave already-running pods on the old config. The pulsusdb config volume is mounted as the whole ConfigMap directory (not a single `subPath`-mounted file) for the same reason — `subPath` mounts never receive live kubelet ConfigMap updates, which would defeat the point of the checksum-triggered rollout the moment a pod happened to survive on stale content for an unrelated reason.

**The bundled ClickHouse credentials Secret is a documented exception** (code review round-2 finding #2): its `checksum/secret` is a hash of the `clickhouse.auth` *values* subtree (which `existingSecret`/keys are wired in) plus the resolved Secret name — deterministic, but it cannot see the Secret's live *content*, only the chart's own configuration of which Secret to use. This is a genuine Helm limitation, not an oversight: `lookup()` results (which is how the chart reads an already-existing generated password so upgrades never rotate it) are excluded from Helm's diffing and are unreliable/absent under `--dry-run`, so no chart can deterministically fingerprint live Secret content this way. **If you rotate the generated ClickHouse password out-of-band** (edit the Secret directly, rather than through `helm upgrade`), existing pods will not automatically roll — run `kubectl rollout restart` on every pulsusdb and ClickHouse workload in the release afterward. `templates/NOTES.txt` repeats this at install time.

## Long release names

Every Service/StatefulSet name this chart generates is safe up to Helm's own 53-character release-name cap (`_helpers.tpl`'s `pulsusdb.componentFullname`, code review round-1 finding #7): each component's suffix (`-writer`, `-reader`, `-clickhouse-shard-N`, `-clickhouse-keeper`, `-otel-collector`, ...) is reserved *before* truncating the base name to 63 characters, so two different components can never collide onto the same truncated prefix regardless of release-name length.

## Grafana Loki-compat datasource

`grafana.datasourceProvisioning.enabled` (default `false`) renders a ConfigMap labelled `grafana_datasource: "1"` (consumable by the Grafana community chart's sidecar) provisioning a Loki-compatible datasource against the query Service. It **only works when `pulsusdb.config.compat_endpoints: true`** ([docs/configuration.md §1](../../../docs/configuration.md), [docs/api.md §8](../../../docs/api.md)) — enabling the datasource without `compat_endpoints` produces a datasource that 404s on every query; `NOTES.txt` warns about this at install time.

## Values reference

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `image.repository` | string | `ghcr.io/digitalis-io/pulsusdb` | Placeholder until M7 publishes the image |
| `image.tag` | string | `""` | Falls back to `.Chart.AppVersion`; dropped when `image.digest` is set |
| `image.digest` | string | `""` | `sha256:<64 hex>` — preferred over `image.tag` when set |
| `image.pullPolicy` | string | `IfNotPresent` | |
| `imagePullSecrets` | list | `[]` | |
| `topology` | enum | `single` | `single` \| `cluster` — ClickHouse topology axis |
| `pulsusdb.split.enabled` | bool | `false` | `false`: one all-mode Deployment. `true`: writer+reader Deployments |
| `pulsusdb.replicaCount` | int | `1` | All-mode replica count |
| `pulsusdb.writer.replicaCount` / `.resources` | int / object | `2` / non-zero | Split-mode writer tier |
| `pulsusdb.reader.replicaCount` / `.resources` | int / object | `2` / non-zero | Split-mode reader tier |
| `pulsusdb.resources` | object | non-zero | All-mode resources |
| `pulsusdb.config.*` | object | see `values.yaml` | 1:1 with docs/configuration.md §9 |
| `pulsusdb.auth.user` / `.password` / `.existingSecret` | string | `""` | HTTP Basic auth (off when all unset); `user` and a password source (`password` or `existingSecret`) must be set together, and `password`/`existingSecret` are mutually exclusive — enforced by a render-time `fail` guard |
| `pulsusdb.extraEnv` | list | `[]` | Escape hatch, `[{name, value\|valueFrom}, ...]` |
| `pulsusdb.spool.persistence.enabled` / `.size` / `.storageClass` | bool / string / string | `false` / `5Gi` / `""` | PVC for the insert-failure spool instead of `emptyDir` |
| `initJob.enabled` | bool | `false` | Disabled-by-default operator-convenience `--mode init` Job |
| `clickhouse.enabled` | bool | `true` | `false` => external/production ClickHouse |
| `clickhouse.shards` | int | `1` | `>= 2` required when `topology: cluster` |
| `clickhouse.replicasPerShard` | int | `1` | Restricted to `1` — see above |
| `clickhouse.clusterName` | string | `pulsus_cluster` | |
| `clickhouse.auth.username` / `.existingSecret` / `.existingSecretPasswordKey` / `.existingSecretInterserverKey` | string | `default` / `""` / `password` / `interserver-secret` | Bundled or external ClickHouse credentials |
| `clickhouse.podSecurityContext` / `.securityContext` | object | non-root uid/gid 101, `readOnlyRootFilesystem: true` | Bundled ClickHouse pods |
| `clickhouse.keeperPodSecurityContext` / `.keeperSecurityContext` | object | same shape, separately overridable | Bundled Keeper pods |
| `persistence.enabled` / `.size` / `.storageClass` / `.keeperSize` | bool / string / string / string | `true` / `20Gi` / `""` / `5Gi` | ClickHouse data volumes |
| `otelCollector.enabled` / `.image` / `.resources` | bool / string / object | `true` / pinned tag / non-zero | |
| `otelCollector.config` | object | `{}` | Passthrough escape hatch — non-empty replaces the chart's own release-aware collector config entirely (not deep-merged) |
| `otelCollector.podSecurityContext` / `.securityContext` | object | non-root uid/gid 10001, `readOnlyRootFilesystem: true` | |
| `grafana.datasourceProvisioning.enabled` | bool | `false` | See above |
| `serviceAccount.create` / `.name` / `.annotations` | bool / string / object | `true` / `""` / `{}` | |
| `service.type` / `.port` | string / int | `ClusterIP` / `3100` | The Service's own external port — distinct from `pulsusdb.config.port` (see probe contract) |
| `podSecurityContext` | object | non-root uid/gid 10001 | |
| `securityContext` | object | `readOnlyRootFilesystem: true` | Safe only because `spool`/`tmp` are dedicated writable volumes |
| `livenessProbe` / `readinessProbe` / `startupProbe` | object | see probe contract above | Port fields are always overridden from `pulsusdb.config.port`, regardless of what's set here |
| `ingress.enabled` | bool | `false` | Targets the query Service |
| `hpa.enabled` | bool | `false` | Disabled skeleton only — no functional autoscaling |
| `podDisruptionBudget.enabled` / `.minAvailable` | bool / int | `false` / `1` | |

Run `helm show values deploy/charts/pulsusdb` for the complete, commented set of defaults.

## Testing

- `tests/unit/*.yaml` — [helm-unittest](https://github.com/helm-unittest/helm-unittest) render/schema specs (`helm unittest deploy/charts/pulsusdb`).
- `tests/golden/*.yaml` — `helm template` snapshots for manifest-drift review.
- `tests/bdd/` — `pytest-bdd` Gherkin `.feature` scenarios driving a live Kind install lifecycle (see `tests/bdd/README` inline docs / `.github/workflows/helm-chart.yml`'s `chart-test-kind` job for how CI runs them).

## Known limitations

- `values.schema.json` cannot express `requests <= limits` quantity ordering — Kubernetes' own API-server admission already rejects `requests > limits`, proven behaviourally by a `pytest-bdd` scenario instead of re-implemented here.
- The bundled ClickHouse is CI/compose-fixture parity, not hardened HA — see "ClickHouse: bundled vs. external" above.
- `clickhouse.replicasPerShard > 1` is not implemented (documented follow-up).
- PulsusDB-side multi-shard endpoint load-balancing does not exist yet (documented product-level follow-up, not a chart limitation).
- HPA/VPA autoscaling ships as a disabled skeleton only.
