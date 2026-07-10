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
| `CLICKHOUSE_SERVER` | `localhost` | host |
| `CLICKHOUSE_PORT` | `9000` | native protocol port (HTTP port derived or set via `CLICKHOUSE_HTTP_PORT`, default `8123`) |
| `CLICKHOUSE_DB` | `pulsus` | database (created by `init`/startup unless `PULSUS_SKIP_DDL=1`) |
| `CLICKHOUSE_AUTH` | `default:` | `user:password` |
| `CLICKHOUSE_PROTO` | `native` | `native` \| `http` \| `https` (TLS to ClickHouse) |
| `CLICKHOUSE_TLS_SKIP_VERIFY` | `false` | accept self-signed certificates |
| `PULSUS_CH_POOL_SIZE` | `8` | connections per process |

The native protocol is used for bulk inserts and sample fetches; HTTP is used for DDL and `INSERT ... SELECT` maintenance statements.

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

## 6. Reader

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_CACHE_TTL` | `60s` | label cache refresh interval |
| `PULSUS_CACHE_MAX_SERIES` | `50000` | **per-selector** match cap before falling back to a SQL JOIN (not a cache size limit) |
| `PULSUS_SERIES_ACTIVITY_BUCKET` | `1h` | granularity of `metric_series` activity rows; set `1d` at multi-million-series cardinality (~24× less metadata; coarser buckets over-include but never miss series) |
| `PULSUS_CACHE_WINDOW` | `24h` | active-series window loaded into the cache; the cache only answers queries whose data window lies inside it — older ranges always resolve from `metric_series` in ClickHouse. Reader RAM budget ≈ active series × (labels JSON + maps overhead, ~300–600 B/series): plan ~2–3 GiB per reader at 5M active series |
| `PULSUS_PROMQL_MAX_SAMPLES` | `50000000` | evaluation sample budget per query |
| `PULSUS_PROMQL_LOOKBACK` | `5m` | staleness/lookback delta |
| `PULSUS_LOGQL_SCAN_BUDGET_BYTES` | `50GiB` | per-query scan cap; exceeding returns "query too broad" |
| `PULSUS_TRACEQL_MAX_CANDIDATES` | `100000` | candidate trace cap before top-K-by-recency truncation |

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

clickhouse:
  server: localhost
  port: 9000
  http_port: 8123
  database: pulsus
  auth: "default:"               # user:password, split on first colon; password is secret
  proto: native                  # native | http | https
  tls_skip_verify: false
  pool_size: 8

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
  logql_scan_budget_bytes: 50GiB
  traceql_max_candidates: 100000

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

## 10. Quickstart (docker compose)

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:24.8
    volumes: [ clickhouse-data:/var/lib/clickhouse ]
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 5s

  pulsusdb:
    image: ghcr.io/pulsusdb/pulsusdb:latest
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
    protocols: { grpc: {}, http: {} }

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
