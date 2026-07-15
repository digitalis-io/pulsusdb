{{/*
Standard name/label/selector helpers (issue #38 architect plan) — every
template below sources its labels from here so label logic is never
duplicated. `pulsusdb.componentSelectorLabels` takes a component name
(`all`/`writer`/`reader`/`clickhouse`/`clickhouse-shard-N`/
`clickhouse-keeper`/`otel-collector`) so each workload gets its own stable
selector without hand-rolling it per template.
*/}}

{{- define "pulsusdb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "pulsusdb.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Suffix-after-truncate component naming (code review round-1 finding
[medium] #7): `pulsusdb.fullname` alone truncates to 63 chars with no
suffix-space reservation, so two different component suffixes appended
*after* truncation (the old approach) could collapse onto the identical
63-char prefix for a long release name — e.g. two shards' names silently
colliding. This helper truncates the **base** name to `63 - len(suffix)`
*before* appending the suffix, so the full result is always <= 63 chars
and distinct suffixes can never collide with each other regardless of
release-name length. `(dict "root" $ "suffix" "-clickhouse-shard-0")`.
*/}}
{{- define "pulsusdb.componentFullname" -}}
{{- $suffix := .suffix -}}
{{- $base := include "pulsusdb.fullname" .root -}}
{{- $maxBase := sub 63 (len $suffix) | int -}}
{{- if gt (len $base) $maxBase -}}
{{- $base = trunc $maxBase $base -}}
{{- end -}}
{{- printf "%s%s" $base $suffix -}}
{{- end -}}

{{- define "pulsusdb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "pulsusdb.labels" -}}
helm.sh/chart: {{ include "pulsusdb.chart" . }}
{{ include "pulsusdb.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "pulsusdb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pulsusdb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Per-component selector labels: `(dict "root" $ "component" "writer")`.
Distinct `app.kubernetes.io/component` per workload keeps Deployments'
`.spec.selector` (immutable after creation) from ever colliding across
components sharing one release.
*/}}
{{- define "pulsusdb.componentSelectorLabels" -}}
{{ include "pulsusdb.selectorLabels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "pulsusdb.componentLabels" -}}
{{ include "pulsusdb.labels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "pulsusdb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "pulsusdb.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
=== Component name helpers (all suffix-after-truncate-safe, see
pulsusdb.componentFullname above) ===
*/}}

{{- define "pulsusdb.writerFullname" -}}
{{- include "pulsusdb.componentFullname" (dict "root" . "suffix" "-writer") -}}
{{- end -}}

{{- define "pulsusdb.readerFullname" -}}
{{- include "pulsusdb.componentFullname" (dict "root" . "suffix" "-reader") -}}
{{- end -}}

{{- define "pulsusdb.otelCollectorFullname" -}}
{{- include "pulsusdb.componentFullname" (dict "root" . "suffix" "-otel-collector") -}}
{{- end -}}

{{/*
The Service the OTel Collector / any other ingest client should target:
the shared all-mode Service, or the `-writer` Service in split mode.
*/}}
{{- define "pulsusdb.ingestServiceName" -}}
{{- if .Values.pulsusdb.split.enabled -}}
{{- include "pulsusdb.writerFullname" . -}}
{{- else -}}
{{- include "pulsusdb.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
The Service query clients (Grafana, `ingress.yaml`) should target: the
shared all-mode Service, or the `-reader` Service in split mode.
*/}}
{{- define "pulsusdb.queryServiceName" -}}
{{- if .Values.pulsusdb.split.enabled -}}
{{- include "pulsusdb.readerFullname" . -}}
{{- else -}}
{{- include "pulsusdb.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
Full pulsusdb image reference. Empty `image.tag` falls back to
`Chart.AppVersion` (issue #38 plan: "appVersion == the released image
tag", not the pulsus-server crate's 0.0.0 dev placeholder).
*/}}
{{- define "pulsusdb.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Single source of truth for the HTTP port pulsusdb actually listens on
(code review round-1 finding [medium] #8): `pulsusdb.config.port`
(docs/configuration.md §9) is what the *process* binds — Service
targetPort, every probe's port, and the container's own `containerPort`
must all derive from this one helper so changing `pulsusdb.config.port`
can never silently desync from the Service/probes (which is what
independently-defaulted-to-3100 `service.port`/`livenessProbe.tcpSocket.port`
etc. allowed before this fix). `service.port` remains a distinct,
independently-settable value — it is the Service's own external port,
which callers use to reach the Service; Kubernetes' Service abstraction
lets that differ from the container's actual listening port by design,
so it is *not* merged into this helper.
*/}}
{{- define "pulsusdb.httpPort" -}}
{{- .Values.pulsusdb.config.port | default 3100 -}}
{{- end -}}

{{/*
=== ClickHouse naming/resolution helpers ===
AC #3 resolution (round-1 amendment §3): pulsusdb's config carries exactly
one `clickhouse.server` entry point + `cluster: <name>`; ClickHouse's own
Distributed engine (`remote_servers.xml`, `clickhouse-configmap.yaml`) is
where all N shard endpoints actually live. pulsusdb does not, and cannot
today, load-balance across shards itself (documented product gap, linked
follow-up filed at close).
*/}}

{{- define "pulsusdb.clickhouseFullname" -}}
{{- include "pulsusdb.componentFullname" (dict "root" . "suffix" "-clickhouse") -}}
{{- end -}}

{{- define "pulsusdb.clickhouseKeeperFullname" -}}
{{- include "pulsusdb.componentFullname" (dict "root" . "suffix" "-clickhouse-keeper") -}}
{{- end -}}

{{/* `(dict "root" $ "shard" 0)` -> "<fullname>-clickhouse-shard-0" (truncated-safe) */}}
{{- define "pulsusdb.clickhouseShardFullname" -}}
{{- include "pulsusdb.componentFullname" (dict "root" .root "suffix" (printf "-clickhouse-shard-%d" (int .shard))) -}}
{{- end -}}

{{/*
The single entry point pulsusdb's `clickhouse.server` config value
resolves to when ClickHouse is bundled: the single StatefulSet's Service
in `topology=single`, or shard 0's Service in `topology=cluster` (the
"cluster-entry Service" from the round-1 amendment) — ClickHouse's own
Distributed engine fans queries out to every shard from there. Required
non-empty from `pulsusdb.config.clickhouse.server` when
`clickhouse.enabled=false` (external ClickHouse; `values.schema.json`
enforces this).
*/}}
{{- define "pulsusdb.clickhouseServer" -}}
{{- if .Values.clickhouse.enabled -}}
{{- if eq .Values.topology "cluster" -}}
{{- include "pulsusdb.clickhouseShardFullname" (dict "root" . "shard" 0) -}}
{{- else -}}
{{- include "pulsusdb.clickhouseFullname" . -}}
{{- end -}}
{{- else -}}
{{- .Values.pulsusdb.config.clickhouse.server -}}
{{- end -}}
{{- end -}}

{{/*
Resolved `cluster:` config value: auto-set to `clickhouse.clusterName`
only when this chart owns the ClickHouse cluster (bundled +
`topology=cluster`); otherwise the user's own
`pulsusdb.config.cluster` passes through verbatim (covers both
single-node, unset, and an externally pre-clustered ClickHouse).
*/}}
{{- define "pulsusdb.clusterName" -}}
{{- if and .Values.clickhouse.enabled (eq .Values.topology "cluster") -}}
{{- .Values.clickhouse.clusterName -}}
{{- else -}}
{{- .Values.pulsusdb.config.cluster | default "" -}}
{{- end -}}
{{- end -}}

{{/*
ClickHouse username for CLICKHOUSE_AUTH — applies whether ClickHouse is
bundled or external; the password half is always Secret-sourced
(`templates/secret.yaml`, never the ConfigMap).
*/}}
{{- define "pulsusdb.clickhouseAuthUser" -}}
{{- .Values.clickhouse.auth.username -}}
{{- end -}}

{{/*
Secret name backing the bundled ClickHouse's password: an operator-
supplied `existingSecret`, or this chart's own generated Secret.
*/}}
{{- define "pulsusdb.clickhouseSecretName" -}}
{{- if .Values.clickhouse.auth.existingSecret -}}
{{- .Values.clickhouse.auth.existingSecret -}}
{{- else -}}
{{- include "pulsusdb.clickhouseFullname" . -}}
{{- end -}}
{{- end -}}

{{- define "pulsusdb.clickhouseSecretKey" -}}
{{- if .Values.clickhouse.auth.existingSecret -}}
{{- .Values.clickhouse.auth.existingSecretPasswordKey -}}
{{- else -}}
password
{{- end -}}
{{- end -}}

{{/*
Code review round-1 [high] finding #1: sharded ClickHouse's inter-node
distributed-query authentication (`<secret>` in `remote_servers.xml`) and
interserver-HTTP replication authentication
(`interserver_http_credentials`) must both be Secret-sourced, never a
literal in the rendered XML ConfigMap — this key lives on the same Secret
object `pulsusdb.clickhouseSecretName` resolves (the chart-generated one,
or the operator's `existingSecret`, which must then also carry this key).
One shared secret *value* per release, injected via `from_env` into every
shard (never the Keeper — Keeper's own Raft protocol doesn't use it).
*/}}
{{- define "pulsusdb.clickhouseInterserverSecretKey" -}}
{{- if .Values.clickhouse.auth.existingSecret -}}
{{- .Values.clickhouse.auth.existingSecretInterserverKey -}}
{{- else -}}
interserver-secret
{{- end -}}
{{- end -}}

{{/*
Fail-closed guard for the one cross-field rule values.schema.json cannot
express cleanly alongside the rest of its structural validation
(documented in values.schema.json's own description + here — issue #38
plan §4): `clickhouse.replicasPerShard` must be exactly 1 until per-replica
macros/Keeper-path identity is implemented (tracked follow-up).
*/}}
{{- define "pulsusdb.validateReplicasPerShard" -}}
{{- if gt (int .Values.clickhouse.replicasPerShard) 1 -}}
{{- fail "clickhouse.replicasPerShard > 1 is not yet supported: true multi-replica-per-shard identity (per-replica macros.xml, Keeper paths tied to StatefulSet ordinals) is a documented follow-up, not implemented in this chart. Set clickhouse.replicasPerShard: 1." -}}
{{- end -}}
{{- end -}}

{{- define "pulsusdb.validateClickhouseServer" -}}
{{- if and (not .Values.clickhouse.enabled) (not .Values.pulsusdb.config.clickhouse.server) -}}
{{- fail "pulsusdb.config.clickhouse.server must be set when clickhouse.enabled=false (external ClickHouse)." -}}
{{- end -}}
{{- end -}}

{{- define "pulsusdb.validateClickhouseAuth" -}}
{{- if and (not .Values.clickhouse.enabled) (not .Values.clickhouse.auth.existingSecret) -}}
{{- fail "clickhouse.auth.existingSecret must be set when clickhouse.enabled=false (external ClickHouse) — the chart never invents a password for a database it doesn't manage." -}}
{{- end -}}
{{- end -}}

{{/*
Env vars every pulsusdb-serving container (`all`/`writer`/`reader`/the
optional init Job) shares: ClickHouse credentials (always — every mode
connects to ClickHouse) via the Kubernetes "dependent environment
variables" `$(VAR)` substitution (combines the plaintext username with a
Secret-sourced password into the single `CLICKHOUSE_AUTH=user:password`
string `pulsus-config` expects — there is no separate password-only env
var upstream), optional `PULSUS_AUTH_PASSWORD`, then `extraEnv` last so it
can override any of the above.
*/}}
{{- define "pulsusdb.commonEnv" -}}
- name: CLICKHOUSE_AUTH_PASSWORD_ONLY
  valueFrom:
    secretKeyRef:
      name: {{ include "pulsusdb.clickhouseSecretName" . }}
      key: {{ include "pulsusdb.clickhouseSecretKey" . }}
- name: CLICKHOUSE_AUTH
  value: {{ printf "%s:$(CLICKHOUSE_AUTH_PASSWORD_ONLY)" (include "pulsusdb.clickhouseAuthUser" .) | quote }}
{{- if or .Values.pulsusdb.auth.password .Values.pulsusdb.auth.existingSecret }}
- name: PULSUS_AUTH_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ if .Values.pulsusdb.auth.existingSecret }}{{ .Values.pulsusdb.auth.existingSecret }}{{ else }}{{ include "pulsusdb.fullname" . }}-auth{{ end }}
      key: PULSUS_AUTH_PASSWORD
{{- end }}
{{- range .Values.pulsusdb.extraEnv }}
{{ toYaml (list .) }}
{{- end }}
{{- end -}}

{{/*
Code review round-1 [high] finding #3: `checksum/config`/`checksum/secret`
pod annotations so a ConfigMap/Secret content change triggers a rollout
(Deployments/StatefulSets never watch their mounted ConfigMap/Secret for
in-place updates on their own). `$.Template.BasePath` is the chart's
`templates/` directory regardless of which template includes this define,
so `print $.Template.BasePath "/configmap.yaml"` always resolves to the
same file. Call as `{{ include "pulsusdb.checksums" $ }}` (pass the root
context, not a dict, so `.Template`/`.Files` resolve correctly).
*/}}
{{- define "pulsusdb.checksums" -}}
checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") $ | sha256sum }}
checksum/secret: {{ include (print $.Template.BasePath "/secret.yaml") $ | sha256sum }}
{{- end -}}

{{/*
Bundled-ClickHouse pods: only the credentials Secret can change.

NOT `include (print $.Template.BasePath "/clickhouse-secret.yaml") $` —
unlike every other checksum helper, that template calls `randAlphaNum`
whenever `lookup` finds no existing Secret (every `helm template`
invocation, since `lookup` is always stubbed empty there, and a real
cluster's very first install before the Secret exists) — re-rendering it
here would independently re-roll a *different* random value each time
this helper is included, producing a non-deterministic checksum on every
render (verified: two successive `helm template` runs of the same values
produced two different checksums before this fix) instead of a stable
fingerprint.

**Documented trade (code review round-2 [medium] finding #2):** the
checksum is `sha256(toYaml .Values.clickhouse.auth + resolved secret
name)` — the deterministic *values* that decide *which* secret/keys get
wired in (`existingSecret`, both key names, `username`), plus the
resolved Secret object name itself, so switching between the
chart-generated Secret and an `existingSecret` (or renaming either) is
still caught. This checksum can **not**, and is not intended to, detect
an operator rotating the generated Secret's password **out-of-band**
(e.g. `kubectl edit secret` / re-running `helm install` after manually
deleting the Secret) — `lookup()` results are excluded from Helm's own
diffing and are unreliable/absent under `--dry-run`, so there is no
supported way for a chart to fingerprint *live* Secret content
deterministically. This is a documented, known Helm limitation, not an
oversight — see README.md's "Rollouts on config/secret change" section
and `templates/NOTES.txt` for the operator-facing callout: **rotating the
generated ClickHouse password out-of-band requires a manual `kubectl
rollout restart`** on every pulsusdb/ClickHouse workload afterward.
*/}}
{{- define "pulsusdb.clickhouseChecksums" -}}
checksum/secret: {{ printf "%s%s" (toYaml $.Values.clickhouse.auth) (include "pulsusdb.clickhouseSecretName" $) | sha256sum }}
{{- end -}}

{{/*
Cluster-mode shard/keeper pods: the cluster XML ConfigMap + credentials
Secret. See pulsusdb.clickhouseChecksums' doc comment above for why
checksum/secret is a values-subtree hash, not a re-render of
clickhouse-secret.yaml, and for the documented out-of-band-rotation trade.
*/}}
{{- define "pulsusdb.clickhouseClusterChecksums" -}}
checksum/config: {{ include (print $.Template.BasePath "/clickhouse-cluster-configmap.yaml") $ | sha256sum }}
checksum/secret: {{ printf "%s%s" (toYaml $.Values.clickhouse.auth) (include "pulsusdb.clickhouseSecretName" $) | sha256sum }}
{{- end -}}

{{- define "pulsusdb.otelChecksums" -}}
checksum/config: {{ include (print $.Template.BasePath "/otel-collector-configmap.yaml") $ | sha256sum }}
{{- end -}}

{{/*
Keeper: its ConfigMap lives in its own file (clickhouse-keeper-configmap.yaml)
precisely so this can reference it without self-inclusion. No
checksum/secret here — Keeper's container does reference the ClickHouse
Secret (`CLICKHOUSE_INTERSERVER_SECRET`, wired for parity with the shards,
round-2 code-review [low] finding #4), but that reference is inert
(Keeper's own Raft protocol has no notion of the interserver secret or
CLICKHOUSE_PASSWORD) and, like the shards' checksum/secret, would only
ever change when `.Values.clickhouse.auth` itself changes — the same
documented out-of-band-rotation trade applies (see
`pulsusdb.clickhouseChecksums`'s doc comment): omitted here deliberately,
not stale, since the operator-facing `kubectl rollout restart` guidance
(NOTES.txt/README) already covers rotating this alongside the shards.
*/}}
{{- define "pulsusdb.clickhouseKeeperChecksums" -}}
checksum/config: {{ include (print $.Template.BasePath "/clickhouse-keeper-configmap.yaml") $ | sha256sum }}
{{- end -}}

{{/*
The pulsusdb container spec shared by `deployment-all.yaml`,
`deployment-writer.yaml`, `deployment-reader.yaml`, and the optional
`init-job.yaml` — `(dict "root" $ "mode" "all" "resources" .Values.pulsusdb.resources)`.
Centralizes the probe matrix (issue #38 plan amendment §1: readiness is
the only probe that ever depends on `/ready`; liveness and startupProbe
are always plain TCP so no ClickHouse outage or cold cache can restart a
pod) and the `readOnlyRootFilesystem` + writable-spool-volume pairing the
review flagged as a silent-failure risk if ever split apart. Probe/
container ports are always derived from `pulsusdb.httpPort` (code review
round-1 finding #8) — user-supplied `livenessProbe`/`readinessProbe`/
`startupProbe` values may override every field *except* the port, which
this helper always forces to the single source of truth.
*/}}
{{- define "pulsusdb.container" -}}
{{- $root := .root -}}
{{- $port := include "pulsusdb.httpPort" $root | int -}}
name: pulsusdb
image: {{ include "pulsusdb.image" $root }}
imagePullPolicy: {{ $root.Values.image.pullPolicy }}
args:
  - --mode
  - {{ .mode }}
  - --config
  - /etc/pulsusdb/config.yaml
{{- if ne .mode "init" }}
ports:
  - name: http
    containerPort: {{ $port }}
    protocol: TCP
{{- end }}
env:
  {{- include "pulsusdb.commonEnv" $root | nindent 2 }}
{{- if ne .mode "init" }}
{{- $lp := deepCopy $root.Values.livenessProbe }}
{{- $_ := set $lp.tcpSocket "port" $port }}
livenessProbe:
  {{- toYaml $lp | nindent 2 }}
{{- $rp := deepCopy $root.Values.readinessProbe }}
{{- $_ := set $rp.httpGet "port" $port }}
readinessProbe:
  {{- toYaml $rp | nindent 2 }}
{{- $sp := deepCopy $root.Values.startupProbe }}
{{- $_ := set $sp.tcpSocket "port" $port }}
startupProbe:
  {{- toYaml $sp | nindent 2 }}
{{- end }}
resources:
  {{- toYaml .resources | nindent 2 }}
securityContext:
  {{- toYaml $root.Values.securityContext | nindent 2 }}
volumeMounts:
  - name: config
    mountPath: /etc/pulsusdb
    readOnly: true
  - name: spool
    mountPath: /var/lib/pulsusdb
  - name: tmp
    mountPath: /tmp
{{- end -}}

{{/*
Volumes paired with `pulsusdb.container`'s mounts. `spool` backs the
writer's insert-failure spool (`/var/lib/pulsusdb/spool`, docs/
configuration.md §5) — the one writable path `readOnlyRootFilesystem: true`
requires; `emptyDir` by default, a PVC when `pulsusdb.spool.persistence`
is enabled (issue #38 review fix — the mount+flag must never be split).
`config` is mounted as the whole ConfigMap directory (code review round-1
finding #3), not a single `subPath`-mounted file — `subPath` mounts never
receive live ConfigMap updates from the kubelet, which would silently
defeat the `checksum/config`-triggered-rollout mechanism the moment a pod
survived long enough for a *second* independent (non-rollout) reason to
still be running the stale file.
*/}}
{{- define "pulsusdb.volumes" -}}
- name: config
  configMap:
    name: {{ include "pulsusdb.fullname" . }}-config
{{- if .Values.pulsusdb.spool.persistence.enabled }}
- name: spool
  persistentVolumeClaim:
    claimName: {{ include "pulsusdb.fullname" . }}-spool
{{- else }}
- name: spool
  emptyDir: {}
{{- end }}
- name: tmp
  emptyDir: {}
{{- end -}}
