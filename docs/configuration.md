# PulsusDB Configuration

PulsusDB is configured by environment variables, optionally layered over a YAML file (`--config <path>`). Precedence: CLI flags (`--config`, `--mode` — the only two flags) > environment variables > YAML > defaults; an environment variable set to the empty string counts as unset. Every option has a sane single-node default: `pulsusdb` with only `CLICKHOUSE_SERVER` set is a working deployment.

## 1. Core

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_MODE` | `all` | `all` \| `writer` \| `reader` \| `init` (create/migrate schema and exit) |
| `PULSUS_HOST` | `0.0.0.0` | HTTP bind address |
| `PULSUS_PORT` | `3100` | HTTP port |
| `PULSUS_LOG_LEVEL` | `info` | `error` \| `warn` \| `info` \| `debug` \| `trace` |
| `PULSUS_AUTH_USER` / `PULSUS_AUTH_PASSWORD` | unset | enable HTTP Basic auth when both set |
| `PULSUS_COMPAT_ENDPOINTS` | `false` | mount third-party-compatible query aliases and ingest receivers ([api.md §8](api.md)); the PulsusDB API and OTLP ingestion are always on |
| `PULSUS_CORS_ORIGIN` | `*` | `Access-Control-Allow-Origin` value |
| `PULSUS_QUERY_TIMEOUT` | `2m` | hard per-query timeout |

## 2. ClickHouse connection

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_SERVER` | `localhost` | host (single-endpoint deployments; superseded by `CLICKHOUSE_SERVERS` when that is set) |
| `CLICKHOUSE_SERVERS` | unset | comma-separated multi-endpoint list `host[:port][=zone]` for connection spreading — e.g. `ch1:8123=az-a,ch2:8123=az-a,ch3:8123=az-b`. Omitted port ⇒ `CLICKHOUSE_HTTP_PORT`; omitted zone ⇒ unzoned. See [Connection spreading & AZ affinity](#connection-spreading--az-affinity). IPv6 literals (which contain `:`) must use the YAML `clickhouse.servers:` objects, not this flat form |
| `CLICKHOUSE_HTTP_PORT` | `8123` | HTTP-interface port — the port the chosen transport uses ([ADR 0001](decisions/0001-clickhouse-client.md)); also the per-endpoint port fallback for `CLICKHOUSE_SERVERS` entries that omit one |
| `CLICKHOUSE_PORT` | `9000` | native-protocol port; reserved for the documented fallback client, unused by the current transport |
| `CLICKHOUSE_DB` | `pulsus` | database (created by `init`/startup unless `PULSUS_SKIP_DDL=1`) |
| `CLICKHOUSE_PROTO` | `http` | `http` \| `https` (TLS to ClickHouse). `native` is reserved for the fallback client and rejected at startup with an error citing ADR 0001 |
| `CLICKHOUSE_AUTH` | `default:` | `user:password` |
| `CLICKHOUSE_TLS_SKIP_VERIFY` | `false` | accept self-signed certificates |
| `PULSUS_CH_POOL_SIZE` | `8` | connections per process |
| `CLICKHOUSE_INSERT_QUORUM` | `0` | replicas that must confirm a block before an insert is acknowledged (`0` = off, `1` = rejected at startup — a silent no-op in ClickHouse — `>= 2` = active quorum). Integer-only — `auto`/majority is unsupported. Only meaningful on `Replicated*` engines; see [Consistency](#consistency) |
| `CLICKHOUSE_INSERT_QUORUM_PARALLEL` | `true` | allow parallel quorum inserts; only applied when `CLICKHOUSE_INSERT_QUORUM > 0` |
| `CLICKHOUSE_INSERT_QUORUM_TIMEOUT` | `120s` | quorum wait bound; must be `0 < timeout <=` `PULSUS_QUERY_TIMEOUT` when quorum is enabled (the insert deadline otherwise preempts the wait). Reconciled to the default `PULSUS_QUERY_TIMEOUT`; ClickHouse's own default is 600s |
| `CLICKHOUSE_SELECT_SEQUENTIAL_CONSISTENCY` | `false` | reads see all prior quorum-committed writes (read-your-writes); off by default |

### Consistency

Defaults are all-off: writes are not quorum-committed and reads may hit a lagging replica. This preserves the lowest-latency behaviour on single-node and non-replicated deployments (quorum inserts *throw* on a plain `MergeTree`). On a `Replicated*` multi-replica cluster you can opt in to strong consistency — `CLICKHOUSE_INSERT_QUORUM > 0` makes an insert wait for that many replicas to confirm (so an ack survives a replica loss), and `CLICKHOUSE_SELECT_SEQUENTIAL_CONSISTENCY=true` gives read-your-writes. Both add latency (a quorum write waits for replication; a sequential read waits for the replica to catch up), which is why they are opt-in — enable them only where the durability/read-your-writes guarantee is worth the round-trip cost. The quorum timeout is bounded by `PULSUS_QUERY_TIMEOUT` (the insert deadline that bounds the whole insert), so it is rejected at startup if it exceeds — or is zero while quorum is enabled.

Two further constraints are enforced fail-fast at startup so a config never promises a guarantee ClickHouse will not honour:

- **`CLICKHOUSE_INSERT_QUORUM = 1` is rejected.** ClickHouse disables quorum writes below 2, so `1` is a silent no-op. Use `0` to disable quorum, or `>= 2` for an active quorum.
- **`CLICKHOUSE_SELECT_SEQUENTIAL_CONSISTENCY = true` requires non-parallel quorum inserts.** Read-your-writes only holds when quorum inserts are enabled *and* non-parallel, so seq-consistency is rejected unless `CLICKHOUSE_INSERT_QUORUM >= 2` **and** `CLICKHOUSE_INSERT_QUORUM_PARALLEL = false`. (The startup error names the exact keys and required values rather than silently forcing parallel off.)

The hard requirements are columnar bulk-insert/fetch performance and reliable DDL + `INSERT ... SELECT` maintenance statements; which transport serves which statement class is an implementation detail settled by the M0 client benchmark (docs/decisions/0001). `CLICKHOUSE_PORT`/`CLICKHOUSE_HTTP_PORT` both remain configurable so either split works.

## 3. Schema & retention

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_SKIP_DDL` | `false` | never issue DDL (schema managed externally / read-only credentials) |
| `PULSUS_RETENTION_DAYS` | `7` | TTL for raw log/metric/trace/profile tables |
| `PULSUS_STORAGE_POLICY` | unset | ClickHouse storage policy for all created tables |
| `PULSUS_ROTATION_INTERVAL` | `1h` | how often the schema controller re-applies TTL/rotation |
| `PULSUS_LOG_ROLLUP_RESOLUTION` | `5s` | bucket size of the derived log count/bytes rollup (table named for it, e.g. `log_metrics_5s`); raw log/metric samples always store source timestamps verbatim — no resolution is assumed or imposed anywhere |

## 4. Clustering

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_CLUSTER` | unset | ClickHouse cluster name; enables `ON CLUSTER` DDL, `Replicated*` engines, and `_dist` tables |
| `PULSUS_DIST_SUFFIX` | `_dist` | suffix of Distributed tables targeted by readers (set differently for cross-cluster read topologies) |
| `PULSUS_SKIP_UNAVAILABLE_SHARDS` | `false` | serve degraded reads when a shard is down |
| `PULSUS_AVAILABILITY_ZONE` | unset | this node's own availability zone; when set, the connection pool prefers `CLICKHOUSE_SERVERS` endpoints whose zone matches (see below). An explicit value always wins over `PULSUS_AZ_DETECT` |
| `PULSUS_AZ_DETECT` | `off` | when `PULSUS_AVAILABILITY_ZONE` is unset, auto-detect this node's zone from cloud instance metadata at startup: `off` \| `aws` \| `gcp` \| `azure` \| `auto` (tries each). Detection is fail-soft (any failure/timeout ⇒ unzoned) |

### Connection spreading & AZ affinity

By default PulsusDB dials one ClickHouse endpoint (`CLICKHOUSE_SERVER`/`CLICKHOUSE_HTTP_PORT`) and relies on the `_dist` tables for cross-shard fan-out. Setting `CLICKHOUSE_SERVERS` instead makes the connection pool hold one client **per endpoint** and spread separate queries across them (a single streaming cursor stays pinned to one endpoint for its whole life). The concurrency bound (`PULSUS_CH_POOL_SIZE`) is unchanged — it is a total, not per-endpoint, limit.

Selection is **zone-preferring round-robin**: endpoints whose `zone` matches this node's `PULSUS_AVAILABILITY_ZONE` are used first (round-robin among them), saving cross-AZ network cost; when none is configured or reachable the pool spreads evenly across all endpoints. On a transport failure (connection/timeout/IO or a retryable server code — never a bad-SQL/logic error) the failing endpoint is demoted for a short cooldown and the next request fails over to another zone. There is no health ping on the healthy hot path.

A demoted endpoint recovers via a **background re-probe**, not by relying on request traffic to rediscover it: every 5s a pass re-pings only demoted endpoints whose cooldown has expired, promoting one back to healthy after 2 consecutive successful pings (hysteresis, so a flapping endpoint can't thrash in and out). Each probe borrows one permit from the pool's own concurrency budget via a non-queuing attempt — it never waits behind real requests, so it can only use genuinely idle capacity, and a saturated pool simply defers recovery to the next 5s tick. Once promoted, the endpoint leads again on the very next `get()`, returning to local endpoints once one recovers.

`PULSUS_AZ_DETECT` populates this node's zone from the provider metadata service when you have not set `PULSUS_AVAILABILITY_ZONE` explicitly: AWS uses IMDSv2 (token-required), GCP `metadata.google.internal`, Azure the instance-metadata endpoint. It is a one-time startup probe, individually time-bounded, and fail-soft — off-cloud or when IMDS is blocked it simply leaves the zone unset. The zone strings it returns (e.g. `us-east-1a`, `us-central1-a`) must match the `=zone` labels you give `CLICKHOUSE_SERVERS` for affinity to take effect.

**Topology requirement:** every endpoint listed in `CLICKHOUSE_SERVERS` must be a cluster member that hosts the `_dist` wrapper tables, because a `_dist` query/insert is correct from any node (docs/schemas.md §7) — spreading only changes which node coordinates the identical query, never the result.

See [docs/connection-spreading.md](connection-spreading.md) for the pool architecture, the failover model, and the AZ auto-detection flow (with a diagram).

Deployment topologies:

1. **Single node** — one `pulsusdb` (mode `all`), one ClickHouse. No cluster vars.
2. **Split tiers** — N × `PULSUS_MODE=writer` behind an ingest LB, M × `PULSUS_MODE=reader` behind a query LB, same ClickHouse.
3. **Sharded ClickHouse** — set `PULSUS_CLUSTER`; run `pulsusdb --mode init` once (or an init container) to create replicated + distributed tables; writers/readers as in (2).
4. **Cross-cluster reads** — a reader pointed at a query-only ClickHouse cluster whose `_dist`-suffixed tables front the storage cluster; set `PULSUS_DIST_SUFFIX` accordingly.

## 5. Writer

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_BATCH_BYTES` | `16MiB` | flush a table buffer at this size |
| `PULSUS_BATCH_MS` | `200` | flush a table buffer at this age |
| `PULSUS_INSERT_MODE` | `sync` | `sync` \| `async` default when `X-Pulsus-Async` absent |
| `PULSUS_INGEST_QUEUE_BYTES` | `256MiB` | total buffered bytes before `429` backpressure |
| `PULSUS_METRICS_EXP_HISTOGRAM_MODE` | `classic` | `classic` \| `native` \| `dual` — how OTLP exponential histograms are stored (see below) |

`PULSUS_METRICS_EXP_HISTOGRAM_MODE` selects how OTLP **exponential-histogram** data points are ingested (M7-A4). `classic` (default) keeps the existing behavior byte-for-byte: each data point is flattened to cumulative `<name>_bucket{le}`/`<name>_sum`/`<name>_count` float series. `native` instead stores the sparse native histogram (schema, spans, delta-encoded buckets) in `metric_hist_samples` under the base metric name, and stamps `metric_series.value_type = 1` for that series. `dual` emits **both** — the classic float series (suffixed names) and one base-name native row; their fingerprints are disjoint so they never collide. Flipping the mode mid-stream leaves a series' pre-flip classic history and post-flip native history as disjoint series (a visible gap), which is expected, not a defect.

A batch that exhausts its insert retry budget is spooled to `./spool/{poison,uncertain}/<table>/` (relative to the process's working directory — a documented constant, not yet a `PULSUS_*` variable). In the published container image (§10), the working directory is `/var/lib/pulsusdb`, owned by the non-root `pulsus` user, so this resolves to `/var/lib/pulsusdb/spool/`; mount a volume over that path if spooled batches need to survive a container restart.

## 6. Reader

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_CACHE_TTL` | `60s` | label cache refresh interval |
| `PULSUS_CACHE_MAX_SERIES` | `50000` | **per-selector** match cap before falling back to a SQL JOIN (not a cache size limit) |
| `PULSUS_SERIES_ACTIVITY_BUCKET` | `1h` | granularity of `metric_series` activity rows; set `1d` at multi-million-series cardinality (~24× less metadata; coarser buckets over-include but never miss series) |
| `PULSUS_CACHE_WINDOW` | `24h` | active-series window loaded into the cache; the cache only answers queries whose data window lies inside it — older ranges always resolve from `metric_series` in ClickHouse. Reader RAM budget ≈ active series × (labels JSON + maps overhead, ~300–600 B/series): plan ~2–3 GiB per reader at 5M active series |
| `PULSUS_PROMQL_MAX_SAMPLES` | `50000000` | evaluation sample budget per query |
| `PULSUS_PROMQL_LOOKBACK` | `5m` | staleness/lookback delta |
| `PULSUS_PROMQL_EXPERIMENTAL_FUNCTIONS` | `false` | permit the experimental slice of the pinned Prometheus function registry (mirrors upstream `--enable-feature=promql-experimental-functions`); inert until the first experimental function lands (M6) — the machine-checked inventory of what it will gate is `crates/pulsus-promql/tests/promqltest/coverage/function-coverage.json` |
| `PULSUS_PROMQL_MAX_METRIC_FANOUT` | `1000` | cap on how many metric names one name-less/regex-`__name__` PromQL selector (e.g. `{job="api"}`, `{__name__=~"http_.*"}`) may fan out to; resolved from the warm label cache into a single `metric_name IN (...)` fetch — exceeding the cap returns "query too broad", never an unbounded scan. Accepted range `1..=1000000`: values above the ceiling are rejected at config load, since they would make the fan-out guard unreachable |
| `PULSUS_PROMQL_MAX_CACHE_SCAN` | `200000` | independent cap on how many resident label-cache entries (metric names plus candidate fingerprints) one name-less/regex-`__name__` selector's resolution may *examine* before it is rejected as too broad — distinct from `PULSUS_PROMQL_MAX_METRIC_FANOUT` (which bounds only the matched result): a selector whose matchers yield few or no matches can still examine the whole resident cache. Above the default, which sits above `PULSUS_CACHE_MAX_SERIES`, no legitimate warm resolution false-rejects |
| `PULSUS_PROMQL_MAX_INFO_SERIES` | `100000` | pathological-cardinality backstop on a PromQL `info()` node's synthetic `*_info` metadata-family selector — how many series that family may resolve to before the query is rejected `422 query_too_broad`, enforced BEFORE any sample fetch is issued. Distinct from `PULSUS_PROMQL_MAX_METRIC_FANOUT` (bounds distinct metric *names*, not series) and `PULSUS_PROMQL_MAX_CACHE_SCAN` (bounds examined, not matched, cache entries); a backstop above realistic scrape-target-fleet size, not identifying-label narrowing |
| `PULSUS_LOGQL_SCAN_BUDGET_BYTES` | `50GiB` | approximate best-effort per-query scan guard, **not** a hard byte ceiling: a first page alone over budget fails `QueryTooBroad`, but once at least one page has returned a spent budget (or a later page tripping its cap) returns the survivors so far with `data.stats.pulsus_partial` set; actual bytes scanned can exceed it under query parallelism / shard count (see `PULSUS_LOGQL_PIPELINE_SCAN_FACTOR`) |
| `PULSUS_LOGQL_PIPELINE_SCAN_FACTOR` | `10` | LogQL pipeline first-page fetch-size hint (must be >= 1): when a query pipeline contains an in-engine dropping stage that cannot push down to SQL (a label filter, or a line filter placed after `line_format`), the engine keyset-pages `limit × factor` rows at a time through the pipeline until the true `limit` fills, the window is exhausted, or the byte scan budget is spent — responses fill exactly to `limit` (no under-return) and never over-return. This is no longer an oversample-and-truncate ceiling; a larger factor only sizes the first page (fewer round-trips). `PULSUS_LOGQL_SCAN_BUDGET_BYTES` is an approximate best-effort scan guard, not a hard byte ceiling: if the first page alone exceeds the budget the query fails `QueryTooBroad`, but once at least one page has returned a spent budget (or a later page tripping its positive cap) returns the survivors so far with `data.stats.pulsus_partial` set — never a zero/unlimited cap. Because ClickHouse enforces the cap per read block per concurrent reader (per thread, per shard), actual bytes can exceed the budget, growing with parallelism and shard count |
| `PULSUS_TRACEQL_MAX_CANDIDATES` | `100000` | trace-search candidate depth: per-generator top-K and the merged consumption ceiling; engaging it marks the response `metrics.partial` (docs/api.md §4.2) |
| `PULSUS_TRACEQL_SCAN_BUDGET_ROWS` | `50000000` | per-query row scan cap on every trace-search read (`max_rows_to_read`, throw); exceeding returns `422 query_too_broad` — non-indexable searches are budget-limited, never silently slow |
| `PULSUS_QUERY_EVAL_CONCURRENCY` | `256` | process-wide bound on concurrent CPU-bound PromQL evaluations offloaded onto the blocking pool (the read path's one `spawn_blocking(evaluate)` site); a query past the limit waits (bounded by `PULSUS_QUERY_TIMEOUT`, `408`), never a hard rejection. Default sits below tokio's 512 blocking-pool ceiling yet above realistic heavy-query fan-in, so the uncontended fast path is the norm (must be >= 1) |
| `PULSUS_TAIL_POLL_INTERVAL` | `1s` | how often an idle (caught-up) live-tail connection re-polls for new rows (must be > 0) |
| `PULSUS_TAIL_MAX_DELAY` | `5s` | ceiling on a tail client's `delay_for` param (docs/api.md §2.4); larger requests are clamped |
| `PULSUS_TAIL_MAX_CONNECTIONS` | `100` | process-wide cap on concurrent tail WebSocket connections; the next one is rejected `429` before the upgrade (must be >= 1) |
| `PULSUS_TAIL_MAX_ENTRIES_PER_FRAME` | `1000` | bound on the per-frame `dropped_entries` representative sample sent to a slow consumer (the exact count always arrives as `dropped_total`) |
| `PULSUS_TAIL_CHANNEL_DEPTH` | `4` | undelivered tail frames buffered ahead of a slow WebSocket writer before the oldest frame is evicted into `dropped_entries`/`dropped_total` (must be >= 1) |
| `PULSUS_TAIL_SEND_TIMEOUT` | `30s` | per-send deadline on a tail WebSocket write; a client that stops reading past it is disconnected |
| `PULSUS_TAIL_MAX_FETCH_LIMIT` | `5000` | hard cap on one tail poll's fetched-row `LIMIT`; a client `limit` above it is silently clamped before the query is built (must be >= 1) |
| `PULSUS_TAIL_CATCHUP_SLICE` | `60s` | maximum time window one tail poll may scan/sort; backlog catch-up proceeds one slice per query, re-polling immediately until caught up (must be > 0) |

## 7. Downsampling (M3)

Tier layout is YAML-only (the shape doesn't flatten well into env vars):

```yaml
downsampling:
  enabled: true
  raw_retention: 7d           # overrides PULSUS_RETENTION_DAYS for metric_samples
  tiers:
    - name: 5m
      resolution: 5m
      table: metric_samples_5m
      retention: 90d
      min_step: 5m            # tier eligible when query step >= this; must be >= resolution
    - name: 1h
      resolution: 1h
      table: metric_samples_1h
      retention: 730d
      min_step: 1h
```

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_TIER_POLICY` | `exact` | `exact`: raw samples serve every range where raw still exists (tiers only beyond raw retention; a step is Prometheus-exact when its full evaluation window is raw-covered — boundary-straddling steps are flagged approximate). `fast`: any tier-eligible range is served from tiers (bucket-aligned approximation, flagged via `X-Pulsus-Explain`) |

Tier eligibility always requires `tier.resolution <= query step` **and** `tier.resolution <= the range-vector window` — a 5m-window `rate` is never answered from 1h buckets regardless of policy.

Tiers are populated **in real time by insert-triggered materialized views inside ClickHouse** — there is no downsampling process or schedule. Validation enforced at startup: `resolution`, `min_step`, and `retention` strictly increasing across tiers; `min_step >= resolution` per tier. The schema controller owns the DDL only: tier tables, MVs (recreated when the config checksum changes), TTLs, and a one-shot chunked backfill offered when a tier is first enabled over pre-existing data.

## 8. Ruler (M7)

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_RULER_ENABLED` | `false` | mount rule APIs + evaluation loop (mode `all` only) |
| `PULSUS_RULER_POLL_INTERVAL` | `30s` | rule-group reload cadence |
| `PULSUS_RULER_MAX_RESULT_BYTES` | `10MiB` | per-rule evaluation result cap |

## 9. YAML file

Everything above maps 1:1 into YAML (env var wins on conflict). This is the **complete** document schema — unknown keys are rejected; every key shown with its default:

```yaml
# root scalars
mode: all                        # all | writer | reader | init
host: 0.0.0.0
port: 3100
log_level: info                  # error | warn | info | debug | trace
auth_user: null                  # both set => basic auth; one-sided => startup error
auth_password: null              # secret: never appears in redacted output
compat_endpoints: false
cors_origin: "*"
query_timeout: 2m
skip_ddl: false
retention_days: 7
storage_policy: null
rotation_interval: 1h
log_rollup_resolution: 5s
cluster: null                    # ClickHouse cluster name; enables distributed DDL
dist_suffix: _dist
skip_unavailable_shards: false
availability_zone: null          # this node's AZ; pool prefers clickhouse.servers whose zone matches (§4)
az_detect: off                   # off | aws | gcp | azure | auto — detect zone from cloud instance
                                 # metadata at startup when availability_zone is unset (fail-soft)

clickhouse:
  server: localhost
  port: 9000
  http_port: 8123
  database: pulsus
  auth: "default:"               # user:password, split on first colon; password is secret
  proto: http                    # http | https  (native rejected at startup — ADR 0001)
  tls_skip_verify: false
  pool_size: 8
  insert_quorum: 0               # replicas that must confirm a block (0 = off; Replicated* only). §Consistency
  insert_quorum_parallel: true   # only applied when insert_quorum > 0
  insert_quorum_timeout: 120s    # 0 < timeout <= query_timeout when insert_quorum > 0 (deadline preempts otherwise)
  select_sequential_consistency: false  # read-your-writes; adds latency, off by default
  servers: []                    # multi-endpoint spreading (overrides `server` when non-empty).
                                 # Each entry: {host, http_port?, zone?}; http_port falls back to
                                 # clickhouse.http_port. IPv6 literal hosts must use this object form,
                                 # not CLICKHOUSE_SERVERS. e.g.:
                                 #   servers:
                                 #     - {host: ch1, zone: az-a}
                                 #     - {host: ch2, http_port: 8123, zone: az-b}

writer:
  batch_bytes: 16MiB
  batch_ms: 200
  insert_mode: sync              # sync | async
  ingest_queue_bytes: 256MiB

reader:
  cache_ttl: 60s
  cache_max_series: 50000
  series_activity_bucket: 1h
  cache_window: 24h
  promql_max_samples: 50000000
  promql_lookback: 5m
  promql_experimental_functions: false
  promql_max_metric_fanout: 1000
  promql_max_cache_scan: 200000
  promql_max_info_series: 100000
  logql_scan_budget_bytes: 50GiB
  logql_pipeline_scan_factor: 10
  traceql_max_candidates: 100000
  traceql_scan_budget_rows: 50000000
  query_eval_concurrency: 256
  tail_poll_interval: 1s
  tail_max_delay: 5s
  tail_max_connections: 100
  tail_max_entries_per_frame: 1000
  tail_channel_depth: 4
  tail_send_timeout: 30s
  tail_max_fetch_limit: 5000
  tail_catchup_slice: 60s

downsampling:
  enabled: false
  raw_retention: null            # overrides retention_days for metric_samples when set
  tier_policy: exact             # exact | fast
  tiers: []                      # see §7; name/table unique, resolution/min_step/retention
                                 # strictly increasing, min_step >= resolution per tier

ruler:
  enabled: false
  poll_interval: 30s
  max_result_bytes: 10MiB
```

Durations accept `ms|s|m|h|d|w`; byte sizes accept binary units (`KiB/MiB/GiB/TiB`), decimal units (`KB/MB/GB/TB`), or a bare integer of bytes.

## 10. Quickstart (podman compose / docker compose)

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:24.8
    volumes: [ clickhouse-data:/var/lib/clickhouse ]
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 5s

  pulsusdb:
    image: ghcr.io/digitalis-io/pulsusdb:latest
    environment:
      CLICKHOUSE_SERVER: clickhouse
      PULSUS_RETENTION_DAYS: "7"
    ports: [ "3100:3100" ]
    depends_on:
      clickhouse: { condition: service_healthy }

  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    volumes: [ ./otel-config.yaml:/etc/otelcol-contrib/config.yaml ]
    ports: [ "4317:4317", "4318:4318" ]
    depends_on: [ pulsusdb ]

volumes:
  clickhouse-data:
```

`otel-config.yaml` — the collector receives OTLP from your applications and pushes everything to PulsusDB:

```yaml
receivers:
  otlp:
    protocols: { grpc: { endpoint: 0.0.0.0:4317 }, http: { endpoint: 0.0.0.0:4318 } }

exporters:
  otlphttp:
    endpoint: http://pulsusdb:3100

service:
  pipelines:
    logs:    { receivers: [otlp], exporters: [otlphttp] }
    metrics: { receivers: [otlp], exporters: [otlphttp] }
    traces:  { receivers: [otlp], exporters: [otlphttp] }
    # profiles pipeline requires a collector build with the experimental profiles signal
```

Query via the PulsusDB API (`/api/logs/v1`, `/api/v1`, `/api/traces/v1`, `/api/profiles/v1`). To use existing dashboards or datasources that speak third-party observability APIs, set `PULSUS_COMPAT_ENDPOINTS=true` and point them at `http://pulsusdb:3100`.

**Minimum supported ClickHouse: 24.8 LTS** (projections, modern TTL and aggregate-state behavior). The schema controller verifies the server version at startup and refuses to run DDL against older servers.
