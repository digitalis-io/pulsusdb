# PulsusDB Configuration

PulsusDB is configured by environment variables, optionally layered over a YAML file (`--config <path>`). Precedence: CLI flags (`--config`, `--mode` — the only two flags) > environment variables > YAML > defaults; an environment variable set to the empty string counts as unset. Every option of the binary has a sane single-node default: `pulsusdb` with only `CLICKHOUSE_SERVER` set is a working deployment, once `schema/schema.sh` has built the database with the retention it requires (§7).

## 1. Core

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_MODE` | `all` | `all` \| `writer` \| `reader` |
| `PULSUS_HOST` | `0.0.0.0` | HTTP bind address |
| `PULSUS_PORT` | `3100` | HTTP port |
| `PULSUS_LOG_LEVEL` | `info` | `error` \| `warn` \| `info` \| `debug` \| `trace` |
| `PULSUS_AUTH_USER` / `PULSUS_AUTH_PASSWORD` | unset | enable HTTP Basic auth when both set |
| `PULSUS_COMPAT_ENDPOINTS` | `false` | mount third-party-compatible query aliases and ingest receivers ([api.md §8](api.md)); the PulsusDB API and OTLP ingestion are always on |
| `PULSUS_CORS_ORIGIN` | `*` | `Access-Control-Allow-Origin` value |
| `PULSUS_QUERY_TIMEOUT` | `2m` | hard per-query timeout |
| `PULSUS_TLS_CERT` | unset | path to a PEM certificate chain (leaf first) for the HTTP listener. Both `PULSUS_TLS_CERT` and `PULSUS_TLS_KEY` set ⇒ the single `PULSUS_HOST:PULSUS_PORT` listener terminates TLS and is **TLS-only** (health probes and every client must use `https`); both unset ⇒ plaintext; exactly one set ⇒ startup error (fail closed). Policy is rustls's safe defaults — TLS 1.2 minimum, safe ciphers, ALPN `http/1.1` — with no tuning knobs; certificate rotation requires a restart. An empty-string value counts as unset, like every variable |
| `PULSUS_TLS_KEY` | unset | path to the matching PEM private key (PKCS#8, PKCS#1, or SEC1). Missing/unreadable/malformed cert or key — or a key that doesn't match the certificate — fails startup before the listener binds. See `PULSUS_TLS_CERT` for the pairing rule |

## 2. ClickHouse connection

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_SERVER` | `localhost` | host (single-endpoint deployments; superseded by `CLICKHOUSE_SERVERS` when that is set) |
| `CLICKHOUSE_SERVERS` | unset | comma-separated multi-endpoint list `host[:port][=zone]` for connection spreading — e.g. `ch1:8123=az-a,ch2:8123=az-a,ch3:8123=az-b`. Omitted port ⇒ `CLICKHOUSE_HTTP_PORT`; omitted zone ⇒ unzoned. See [Connection spreading & AZ affinity](#connection-spreading--az-affinity). IPv6 literals (which contain `:`) must use the YAML `clickhouse.servers:` objects, not this flat form |
| `CLICKHOUSE_HTTP_PORT` | `8123` | HTTP-interface port — the port the chosen transport uses ([ADR 0001](decisions/0001-clickhouse-client.md)); also the per-endpoint port fallback for `CLICKHOUSE_SERVERS` entries that omit one |
| `CLICKHOUSE_PORT` | `9000` | native-protocol port; reserved for the documented fallback client, unused by the current transport |
| `CLICKHOUSE_DB` | `pulsus` | database; created by `schema/schema.sh`, never by the binary — startup refuses when it does not exist |
| `CLICKHOUSE_PROTO` | `http` | `http` \| `https` (TLS to ClickHouse). `native` is reserved for the fallback client and rejected at startup with an error citing ADR 0001 |
| `CLICKHOUSE_AUTH` | `default:` | `user:password` |
| `CLICKHOUSE_TLS_SKIP_VERIFY` | `false` | accept self-signed certificates |
| `PULSUS_CH_POOL_SIZE` | `8` | connections per process. Accepted range `1..=8000`: values above the ceiling are rejected at config load — an unbounded pool disables the connection cap and can exceed the 32-bit tokio semaphore permit bound at startup. Scale tuning routes to #25 |
| `CLICKHOUSE_INSERT_QUORUM` | `0` | replicas that must confirm a block before an insert is acknowledged (`0` = off, `1` = rejected at startup — a silent no-op in ClickHouse — `>= 2` = active quorum). Integer-only — `auto`/majority is unsupported. Only meaningful on `Replicated*` engines; see [Consistency](#consistency) |
| `CLICKHOUSE_INSERT_QUORUM_PARALLEL` | `true` | allow parallel quorum inserts; only applied when `CLICKHOUSE_INSERT_QUORUM > 0` |
| `CLICKHOUSE_INSERT_QUORUM_TIMEOUT` | `120s` | quorum wait bound; must be `0 < timeout <=` `PULSUS_QUERY_TIMEOUT` when quorum is enabled (the insert deadline otherwise preempts the wait). Reconciled to the default `PULSUS_QUERY_TIMEOUT`; ClickHouse's own default is 600s |
| `CLICKHOUSE_SELECT_SEQUENTIAL_CONSISTENCY` | `false` | reads see all prior quorum-committed writes (read-your-writes); off by default |

### Consistency

Defaults are all-off: writes are not quorum-committed and reads may hit a lagging replica. This preserves the lowest-latency behaviour on single-node and non-replicated deployments (quorum inserts *throw* on a plain `MergeTree`). On a `Replicated*` multi-replica cluster you can opt in to strong consistency — `CLICKHOUSE_INSERT_QUORUM > 0` makes an insert wait for that many replicas to confirm (so an ack survives a replica loss), and `CLICKHOUSE_SELECT_SEQUENTIAL_CONSISTENCY=true` gives read-your-writes. Both cost, and the two costs are different in kind: a quorum write waits for replication, and **a sequential read does not wait — it fails**, because `select_sequential_consistency`'s own catalogue entry says the server "will generate an exception" when the client reaches a partial replica. That is why they are opt-in — enable them only where the durability/read-your-writes guarantee is worth the round-trip cost. The quorum timeout is bounded by `PULSUS_QUERY_TIMEOUT` (the insert deadline that bounds the whole insert), so it is rejected at startup if it exceeds — or is zero while quorum is enabled.

Two further constraints are enforced fail-fast at startup so a config never promises a guarantee ClickHouse will not honour:

- **`CLICKHOUSE_INSERT_QUORUM = 1` is rejected.** ClickHouse disables quorum writes below 2, so `1` is a silent no-op. Use `0` to disable quorum, or `>= 2` for an active quorum.
- **`CLICKHOUSE_SELECT_SEQUENTIAL_CONSISTENCY = true` requires non-parallel quorum inserts.** Read-your-writes only holds when quorum inserts are enabled *and* non-parallel, so seq-consistency is rejected unless `CLICKHOUSE_INSERT_QUORUM >= 2` **and** `CLICKHOUSE_INSERT_QUORUM_PARALLEL = false`. (The startup error names the exact keys and required values rather than silently forcing parallel off.)

The hard requirements are columnar bulk-insert/fetch performance and reliable DDL + `INSERT ... SELECT` maintenance statements; which transport serves which statement class is an implementation detail settled by the M0 client benchmark (docs/decisions/0001). `CLICKHOUSE_PORT`/`CLICKHOUSE_HTTP_PORT` both remain configurable so either split works.

## 3. Schema

Retention is not set here: `schema/schema.sh` takes it (§7), and the binary reads it back from the database.

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_STORAGE_POLICY` | unset | ClickHouse storage policy for the created tables. With `PULSUS_COLD_VOLUME` set (§7), the tables that never move are created without it. `schema/schema.sh` refuses, before it drops anything, a policy any server of the cluster lacks |
| `PULSUS_TRACEQL_INDEXED_ATTRIBUTES` | empty | comma-separated trace attributes `spans` carries a bloom-filter index for, each `span.<key>` or `event.<key>`, the key dot-separated segments of letters, digits, `_` and `-`; at most 16, none twice, each key at most 128 bytes. `schema/schema.sh` checks the list before it contacts the server and builds one index per item and the reader hints a search whose filter requires `key = "<string>"` on one. Changing it is a schema rebuild; a reader and a schema that disagree answer the same, only slower. Each index adds about 3–5 MB to a `spans` insert or merge at the default `PULSUS_BATCH_BYTES`, and 41–53 B more per span in a push larger than that |
| `PULSUS_LOG_ROLLUP_RESOLUTION` | `5s` | bucket size of the derived log count/bytes rollup (table named for it, e.g. `log_metrics_5s`); raw log/metric samples always store source timestamps verbatim — no resolution is assumed or imposed anywhere |
| `PULSUS_METRICS_DEDUP_WINDOW` | `10000` | how many recent blocks `metric_landing` and each of the eight tables the views maintain remember for deduplication, so a resend of a block the server already accepted is dropped before it is stored again. A window of `W` blocks covers a steady rate of `W / 120` pushes a second, the landing insert's budget being 120 s: **83** at the default and **8333** at the maximum, rounding down. Above that a block's token can be evicted before its resend arrives. **Which engine setting carries it follows the deployment**: a clustered deployment renders these tables `Replicated*`, where the window in force is the replicated engines' own. On a single node there is no companion **time** window, the engine having `non_replicated_deduplication_window` and no seconds-based counterpart there — a block is remembered until that many newer blocks have arrived and is never forgotten on a timer, which is the safe direction for a resend. On a cluster there is one, and it is pinned at the server's own default of one hour rather than configured: a deployment that lowered it below the landing budget would forget a block's token while the writer is still entitled to resend under it. Accepted range `1..=1000000`; values outside this range are rejected at config load |
| `PULSUS_LOG_DEDUP_WINDOW` | `10000` | how many recent blocks `log_landing` and each of the five tables the views maintain remember for deduplication, so a resend of a block the server already accepted is dropped before it is stored again. A window of `W` blocks covers a steady rate of `W / 120` pushes a second, the landing insert's budget being 120 s: **83** at the default and **8333** at the maximum, rounding down. Above that a block's token can be evicted before its resend arrives. **Which engine setting carries it follows the deployment**: a clustered deployment renders these tables `Replicated*`, where the window in force is the replicated engines' own. On a cluster there is a companion **time** window, and it is pinned at the server's own default of one hour rather than configured — a deployment that lowered it below the landing budget would forget a block's token while the writer is still entitled to resend under it. Accepted range `1..=1000000`; values outside this range are rejected at config load |
| `PULSUS_TRACE_DEDUP_WINDOW` | `10000` | how many recent blocks `trace_landing` and each of the eight tables the views maintain remember for deduplication, so a resend of a block the server already accepted is dropped before it is stored again. The shape is `PULSUS_LOG_DEDUP_WINDOW`'s, whole, including which engine setting carries it and the pinned companion time window on a cluster. Accepted range `1..=1000000`; values outside this range are rejected at config load |

## 4. Clustering

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_CLUSTER` | unset | ClickHouse cluster name; enables `ON CLUSTER` DDL, `Replicated*` engines, and `_dist` tables |
| `PULSUS_DIST_SUFFIX` | `_dist` | suffix of Distributed tables targeted by readers (set differently for cross-cluster read topologies); the service graph reads the cluster and database that `spans` plus this suffix names |
| `PULSUS_SKIP_UNAVAILABLE_SHARDS` | `false` | on a cluster, what a logs, metrics or trace read does when a shard cannot be reached or does not have the table. `false`: the read fails. `true`: it answers from the other shards, leaving that shard's data out, with no error. |
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
3. **Sharded ClickHouse** — set `PULSUS_CLUSTER`; run `PULSUS_CLUSTER=<name> schema/schema.sh` once (or the chart's init Job) to create the replicated + distributed tables; writers/readers as in (2). Every replica of a shard must run with the same ClickHouse server time zone (the server `timezone` setting), and that zone must not change while the cluster is serving; UTC is recommended. The service-graph job reads a time another replica of its shard wrote in that replica's zone.
4. **Cross-cluster reads** — a reader pointed at a query-only ClickHouse cluster whose `_dist`-suffixed tables front the storage cluster; set `PULSUS_DIST_SUFFIX` accordingly.

## 5. Writer

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_BATCH_BYTES` | `16MiB` | the per-push byte ceiling, for metrics, logs and traces alike (issues #603 and #602): a push whose estimated landing bytes exceed it is refused `413` whole, because a push is one insert of one block and is never split. A landing row's estimate is the row the writer holds — the union of that signal's target tables' columns — plus the text and arrays that row owns, so the figure is what the push occupies in memory rather than what it eventually stores. Accepted range `1..=17179869184` bytes (16 GiB): values above the ceiling are rejected at config load, since they would defeat the per-push byte ceiling. Scale tuning routes to #25 |
| `PULSUS_BATCH_MS` | `200` | the cadence at which the push-suppression index ages an unsettled claim. **It triggers no flush** (issues #603 and #602): every push is sealed and queued at admission, so nothing waits for an age trigger. Accepted range `1..=200000` ms: values above the ceiling are rejected at config load, since they would keep an unsettled claim open. Scale tuning routes to #25 |
| `PULSUS_INSERT_MODE` | `sync` | `sync` \| `async` default when `X-Pulsus-Async` absent |
| `PULSUS_INGEST_QUEUE_BYTES` | `256MiB` | total buffered bytes before `429` backpressure. A push of any signal is charged the same estimate `PULSUS_BATCH_BYTES` bounds — one whole landing block, its rows, its settings and its claim — and holds it until its block has committed or has been spooled (issues #603 and #602). The settings are charged at what the widest set any signal sends allocates, measured from the sets themselves, so a trace push's larger set is covered. Accepted range `1..=274877906944` bytes (256 GiB): values above the ceiling are rejected at config load, since they would defeat the memory backpressure guard. Scale tuning routes to #25 |
| `PULSUS_LOG_PATTERNS` | `true` | ingest-time log-pattern extraction (M7-C3): `true` extracts a deterministic template per log line and pre-aggregates it into `log_patterns`; `false` disables all extraction and every `log_patterns` append (zero write-path work) — the `GET /api/logs/v1/patterns` endpoint stays mounted and returns empty data |
| `PULSUS_DISCOVER_LOG_LEVELS` | `true` | ingest-time log-level detection (issue #483): `true` attaches a `detected_level` structured-metadata pair to **every** log entry on every log-ingest path — the Loki push receiver (both encodings) and the OTLP logs receiver — resolved from a pre-existing `detected_level` pair, then a stream label, then the entry's other structured metadata, then the entry itself (the OTLP severity number, or the line parsed as JSON/logfmt, or a word-bounded scan of the whole line), falling through to `unknown`; `false` attaches nothing at all and stores a client-supplied `detected_level` exactly as sent. The value is what makes level colouring in a log view and the breakdown in a log-volume histogram work for a stream whose level is in the line body rather than in a label. It never enters `log_streams_idx`, so `/labels` and `/detected_labels` do not list it |
| `PULSUS_INGEST_DEDUP` | `true` | retried-push suppression (issue #494, extended to the trace push by issue #586 — so it covers a log push, a metric push and a trace push). A client that retries a push after a network timeout would otherwise store its entries, samples or spans twice; for logs and metrics that means so `count_over_time`/`sum_over_time` return doubled values while `rate` looks right. `true` means a content-identical push that reaches **the same writer process** inside `PULSUS_INGEST_DEDUP_WINDOW` is admitted or not, and answered, as [api.md §1.0](api.md)'s retry table sets out; `false` restores the doubling |
| `PULSUS_INGEST_DEDUP_WINDOW` | `5m` | how long one writer remembers an accepted push, on all three signals. A byte-identical push arriving after this has elapsed is a new push and is stored. Accepted range `1s..=1h`; values outside this range are rejected at config load |
| `PULSUS_INGEST_DEDUP_MAX_BYTES` | `16MiB` | **per signal, and there are three of them from issue #586 on**: the bound on the whole suppression index — the reserved claim table plus every live registration of a blocked sync caller. The index reserves its collections once at construction and never grows them, so this is a hard bound: at it, a push is refused `429` with `pulsus_ingest_dedup_shed_total` (claim table) or `pulsus_ingest_dedup_wait_shed_total` (waiters) and nothing is stored. Accepted range `1MiB..=1GiB`; values outside this range are rejected at config load. Scale tuning routes to #25 |
| `PULSUS_METRICS_LANDING_RETRIES` | `3` | resends of a failed metrics landing insert (issue #603). Every resend carries the same minted `insert_deduplication_token` and the identical block, so a resend of one the server already accepted stores nothing twice while `PULSUS_METRICS_DEDUP_WINDOW` still holds it. The **wall-clock** bound on the whole loop — the queue wait, every attempt and every sleep, from the push's admission — is 120 s, not this count; whichever binds first ends it. Accepted range `0..=10` |
| `PULSUS_METRICS_LANDING_INSERTERS` | `4` | insert workers on the metrics landing queue, so how many landing inserts can be in flight at once. The queue itself is bounded by bytes, not by length: `PULSUS_INGEST_QUEUE_BYTES` is the only gate. Accepted range `1..=64` |
| `PULSUS_METRICS_LANDING_MAX_ROWS` | `1048576` | per-push landing row ceiling, counted over samples, histogram samples, exemplars, series registrations and descriptors together. A push at or above it is refused `413` naming its own size and both limits, never split — two blocks can commit a prefix, and a prefix is not all-or-nothing. `PULSUS_BATCH_BYTES` is the matching per-push byte ceiling. The same figure is pinned as each landing insert's `max_insert_block_size`, so an admitted push is strictly under the size the server would split a block at, and the three byte limits that also form blocks (`max_insert_block_size_bytes`, `min_insert_block_size_bytes`, `input_format_max_block_size_bytes`) are pinned to take no part in it whatever a server profile sets them to — the row count is then the only thing that ends a block, and `PULSUS_BATCH_BYTES` is what bounds the block's size. Accepted range `1000..=10000000` |
| `PULSUS_LOG_LANDING_RETRIES` | `3` | resends of a failed logs landing insert (issue #603). Every resend carries the same minted `insert_deduplication_token` and the identical block, so a resend of one the server already accepted stores nothing twice while `PULSUS_LOG_DEDUP_WINDOW` still holds it. The **wall-clock** bound on the whole loop — the queue wait, every attempt and every sleep, from the push's admission — is 120 s, not this count; whichever binds first ends it. Accepted range `0..=10` |
| `PULSUS_LOG_LANDING_INSERTERS` | `4` | insert workers on the logs landing queue, so how many landing inserts can be in flight at once. The queue itself is bounded by bytes, not by length: `PULSUS_INGEST_QUEUE_BYTES` is the only gate. Accepted range `1..=64` |
| `PULSUS_LOG_LANDING_MAX_ROWS` | `1048576` | per-push landing row ceiling, counted over log lines, stream registrations and pattern aggregates together. A push at or above it is refused `413` naming its own size and both limits, never split — two blocks can commit a prefix, and a prefix is not all-or-nothing. `PULSUS_BATCH_BYTES` is the matching per-push byte ceiling. The same figure is pinned as each landing insert's `max_insert_block_size`, exactly as the metrics key's is. Accepted range `1000..=10000000` |
| `PULSUS_TRACE_LANDING_RETRIES` | `3` | resends of a failed traces landing insert. Every resend carries the same minted `insert_deduplication_token` and the identical block, so a resend of one the server already accepted stores nothing twice while `PULSUS_TRACE_DEDUP_WINDOW` still holds it. The **wall-clock** bound on the whole loop — the queue wait, every attempt and every sleep, from the push's admission — is 120 s, not this count; whichever binds first ends it. Accepted range `0..=10` |
| `PULSUS_TRACE_LANDING_INSERTERS` | `4` | insert workers on the traces landing queue, so how many landing inserts can be in flight at once. The queue itself is bounded by bytes, not by length: `PULSUS_INGEST_QUEUE_BYTES` is the only gate. Accepted range `1..=64` |
| `PULSUS_TRACE_LANDING_MAX_ROWS` | `1048576` | per-push landing row ceiling, counted over spans, resources, tag names and tag values together. A push at or above it is refused `413` naming its own size and both limits, never split — two blocks can commit a prefix, and a prefix is not all-or-nothing. `PULSUS_BATCH_BYTES` is the matching per-push byte ceiling, and a push whose spans fall on more than 100 distinct UTC dates is refused `413` as well. The same figure is pinned as each landing insert's `max_insert_block_size`, exactly as the other two signals' keys are. Accepted range `1000..=10000000` |
| `PULSUS_METRICS_EXP_HISTOGRAM_MODE` | `classic` | `classic` \| `native` \| `dual` — how OTLP exponential histograms are stored (see below) |
| `PULSUS_OTLP_TRANSLATION_STRATEGY` | `UnderscoreEscapingWithSuffixes` | `NoUTF8EscapingWithSuffixes` \| `UnderscoreEscapingWithSuffixes` \| `UnderscoreEscapingWithoutSuffixes` \| `NoTranslation` — how OTLP metric and label names are rewritten on `POST /v1/metrics` (see below) |
| `PULSUS_OTLP_PROMOTE_SCOPE_METADATA` | `false` | promote the instrumentation scope's name, version, schema URL and attributes onto every metric series as `otel_scope_*` labels |

`PULSUS_METRICS_EXP_HISTOGRAM_MODE` selects how OTLP **exponential-histogram** data points are ingested (M7-A4). `classic` (default) keeps the existing behavior byte-for-byte: each data point is flattened to cumulative `<name>_bucket{le}`/`<name>_sum`/`<name>_count` float series. `native` instead stores the sparse native histogram (schema, spans, delta-encoded buckets) in `metric_hist_samples` under the base metric name, and stamps `value_type = 1` on that series' kind-2 landing row. `dual` emits **both** — the classic float series (suffixed names) and one base-name native row; their fingerprints are disjoint so they never collide. Flipping the mode mid-stream leaves a series' pre-flip classic history and post-flip native history as disjoint series (a visible gap), which is expected, not a defect.

`PULSUS_OTLP_TRANSLATION_STRATEGY` and `PULSUS_OTLP_PROMOTE_SCOPE_METADATA` mirror Prometheus v3.13.0's `otlp.translation_strategy` and `otlp.promote_scope_metadata`, **value for value and default for default** (issue #461), so a value copied out of a Prometheus `otlp:` block means the same thing here. The four spellings are the reference's own and are case-sensitive.

At the default, `UnderscoreEscapingWithSuffixes`, an OTLP metric name is escaped to `[a-zA-Z0-9:_]` and gains its unit and type suffixes (`app.checkout.request.count` with unit `1`, a monotonic Sum, is stored as `app_checkout_request_count_total`; `http.server.duration` with unit `s` becomes `http_server_duration_seconds`), and an attribute key is escaped to `[a-zA-Z0-9_]` with a `key`/`key_` prefix where it would otherwise start with `_` or a digit. `NoUTF8EscapingWithSuffixes` keeps the OTLP name and adds suffixes; `UnderscoreEscapingWithoutSuffixes` escapes and adds none; `NoTranslation` stores both verbatim — which makes a dotted name unreachable from PromQL except as `{__name__="…"}`, so it is experimental upstream too.

**Three rewrites are not gated by the strategy and happen under every value**: `job` is derived from `service.namespace`/`service.name`, `instance` from `service.instance.id`, and the resource's remaining attributes are published as a `target_info` series rather than copied onto every metric series. The `_bucket`/`_sum`/`_count` suffixes are likewise constants, not part of the naming strategy.

Every deliberate difference from the reference on this path is recorded in [benchmarks/metrics-differential-ledger.md](benchmarks/metrics-differential-ledger.md).

A batch that exhausts its insert retry budget is spooled to `./spool/{poison,uncertain}/<table>/` (relative to the process's working directory — a documented constant, not yet a `PULSUS_*` variable). In the published container image (§10), the working directory is `/var/lib/pulsusdb`, owned by the non-root `pulsus` user, so this resolves to `/var/lib/pulsusdb/spool/`; mount a volume over that path if spooled batches need to survive a container restart.

A trace request is also bounded **before** any of that, by a fixed ceiling on the bytes its decode materializes: 256 MiB, on `POST /v1/traces` and on the Zipkin receiver alike. It is not a knob — there is no `PULSUS_*` variable for it — and it is **not** `PULSUS_INGEST_QUEUE_BYTES` above, which is the separate queue bound that still applies after admission. The estimate is taken from wire lengths and counts what the decode stores: every attribute once, where it is stored (an array or key-value list at six times its wire bytes, bytes at twice), and every span with what it copies — the span row itself, the service name twice, the scope's attributes, name, version and schema url, and a resource row with the resource's attributes, schema url and entity references. Each piece is charged before it is built and the decode stops at the first charge that crosses the ceiling, so a refused request has built at most what fits under it, and nothing of it is admitted or stored; the Zipkin receiver charges the same measure block by block while it converts the request. The refusal is the whole-request `400` each receiver already returns for it ([api.md §1.1](api.md) for the native route, §8.2 for the other — they do not share an error envelope).

## 6. Reader

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_CACHE_TTL` | `60s` | label cache refresh interval |
| `PULSUS_CACHE_MAX_SERIES` | `50000` | **per-selector** match cap before falling back to a SQL JOIN (not a cache size limit), and the most series a PromQL selector with no metric name and no `__name__` matcher may resolve to through the label index (issue #635; more is `422`). Accepted range `1..=50000000`: values above the ceiling are rejected at config load, since they would make the series-cardinality guards unreachable. Scale tuning routes to #25 |
| `PULSUS_CACHE_WINDOW` | `24h` | active-series window loaded into the cache; the cache only answers queries whose data window lies inside it — older ranges always resolve from `metric_series` in ClickHouse. Reader RAM budget ≈ active series × (labels JSON + maps overhead, ~300–600 B/series): plan ~2–3 GiB per reader at 5M active series |
| `PULSUS_PROMQL_MAX_SAMPLES` | `50000000` | evaluation sample budget per query, enforced on the metrics read path (issue #138): counts **the rows this process materialises**, charged per row inside the drain loop, and aborts on the first over-budget row, returning `422 query_too_broad` naming this knob. Exactly-at-budget succeeds. Series/label hydration, cardinality probes, and discovery endpoints never charge it (they carry their own caps). **What a row IS depends on the route, and issue #549 is where the two stopped coinciding.** On the ordinary route a row is one fetched `metric_samples` or `metric_hist_samples` row (histogram rows count 1 each, pre-dedup and pre-merge); on the grouped instant read (`PULSUS_PROMQL_GROUPED_PUSH`) a row is one **run** row — a stretch of consecutive grid points over which a group's answer does not change — counted per statement, before the cross-chunk fold. Both are the same quantity, which is what the cap exists to bound: the rows this process holds. The number of rows ClickHouse read off storage to produce them is deliberately not counted — a database of this kind is built to read billions of rows and reduce them, so that number says nothing about this process's memory (owner ruling, 2026-09-17). **The set of requests the cap admits therefore moves in both directions on the queries the push takes**: a query whose sample rows exceed the cap but whose runs do not is served where it was refused (400 series over an hour at a 15 s step: 96,400 sample rows, 964 runs), and a short query whose runs exceed its sample rows is refused where it was served (three one-sample series in one group, counted: 3 sample rows, 5 runs). **Semantics diverge from Prometheus's `--query.max-samples`** (per-step in-eval peak accounting): PulsusDB drains a read fully before evaluation, so what it drains IS the memory event being bounded. Accepted range `1..=50000000000`: values above the ceiling are rejected at config load, since they would make the sample budget unreachable. Scale tuning routes to #25 |
| `PULSUS_PROMQL_LOOKBACK` | `5m` | staleness/lookback delta |
| `PULSUS_PROMQL_EXPERIMENTAL_FUNCTIONS` | `false` | permit the experimental slice of the pinned Prometheus function registry (mirrors upstream `--enable-feature=promql-experimental-functions`); the machine-checked inventory of what it gates is `crates/pulsus-promql/tests/promqltest/coverage/function-coverage.json` |
| `PULSUS_PROMQL_MAX_METRIC_FANOUT` | `1000` | cap on how many metric names one PromQL selector with a regex/negated `__name__` matcher and no concrete name (e.g. `{__name__=~"http_.*"}`) may fan out to (a selector with no metric name and no `__name__` matcher, e.g. `{job="api"}`, resolves through the label index instead, issue #635); resolved from the warm label cache into a single `metric_name IN (...)` fetch — exceeding the cap returns "query too broad", never an unbounded scan. Accepted range `1..=1000000`: values above the ceiling are rejected at config load, since they would make the fan-out guard unreachable. **Query-text interaction (issue #35):** a fanout configured near the 1,000,000 ceiling can render a `metric_name IN (...)` list past the 8 MiB read-path query-text budget before the fanout guard itself would trip — such a query fails a deterministic `422 query_too_broad` rather than an opaque ClickHouse parse error. No config-load cross-check between the fanout ceiling and the query-text budget is possible: rendered width depends on stored metric-name lengths, unknowable at config load. Scale tuning routes to #25 |
| `PULSUS_PROMQL_MAX_CACHE_SCAN` | `200000` | independent cap on how many resident label-cache entries (metric names plus candidate fingerprints) one regex/negated-`__name__` selector's resolution may *examine* before it is rejected as too broad — distinct from `PULSUS_PROMQL_MAX_METRIC_FANOUT` (which bounds only the matched result): a selector whose matchers yield few or no matches can still examine the whole resident cache. Above the default, which sits above `PULSUS_CACHE_MAX_SERIES`, no legitimate warm resolution false-rejects. Accepted range `1..=200000000`: values above the ceiling are rejected at config load, since they would make the scan-budget guard unreachable (an O(resident cache) walk per query). Scale tuning routes to #25 |
| `PULSUS_PROMQL_MAX_INFO_SERIES` | `100000` | pathological-cardinality backstop on a PromQL `info()` node's synthetic `*_info` metadata-family selector — how many series that family may resolve to before the query is rejected `422 query_too_broad`, enforced BEFORE any sample fetch is issued. Distinct from `PULSUS_PROMQL_MAX_METRIC_FANOUT` (bounds distinct metric *names*, not series) and `PULSUS_PROMQL_MAX_CACHE_SCAN` (bounds examined, not matched, cache entries); a backstop above realistic scrape-target-fleet size, not identifying-label narrowing. Accepted range `1..=100000000`: values above the ceiling are rejected at config load, since they would make the cardinality backstop unreachable. Scale tuning routes to #25 |
| `PULSUS_PROMQL_GROUPED_PUSH` | `true` | compile `min`/`max`/`count`/`group` over a **plain** instant selector — no range, no `offset`, no `@`, no subquery context, one concrete metric name with no other `__name__` matcher, or none at all (a `__name__` regex or label matchers only, issue #579) — into ONE ClickHouse statement per fingerprint chunk, returning the answer already reduced as a step function, instead of fetching every sample and reducing in this process (issue #549). Where the push applies it is a large reduction in bytes on the metered hop — measured on 400 series in 4 groups over an hour at a 15 s step, 691,011 bytes to 9,389, `ProfileEvents['NetworkSendBytes']` plus statement text, median of three — at the cost of more work in the database. **The threshold is a heuristic, and the row count it stands in for is a transition count.** A run ends when the group's answer CHANGES, so what the statement returns depends on how often that happens — not on how many series feed the group. Measured on two corpora with the same 100 series in the same 1 group: 39 rows where the count is flat through the middle, 199 rows where it changes at nearly every grid point. The transition count is not knowable before the statement runs, so the compiler uses what it can compute from the resolver's answer alone and declines below `series >= 2 * groups` — as many groups as series is the shape with no grouping work to do. **That is a proxy for the shape and not a bound on rows on either side of it**: measured on a declined corpus of three constant series in one group each, the push returns 3 rows against the raw read's 723 under every one of the four operations, so the threshold turns away queries it would have helped. It also declines a cold or degraded label cache, and every construct outside the four. **It changes what `PULSUS_PROMQL_MAX_SAMPLES` counts on the queries it takes** — see that row. **The same flag gates issue #579**: `sum`/`avg`/`count`/`min`/`max` over `rate`/`irate`/`increase` of a plain range selector compile into one statement per node that returns one row per group per step, both shapes are pushed at any node of the query rather than only at its root, and a window holding histogram samples takes the sample fetch instead; on those queries a row `PULSUS_PROMQL_MAX_SAMPLES` charges is one group's answer at one step. Set it `false` to take the pre-#549 route for every query; the response bodies are identical either way |
| `PULSUS_LOGQL_SCAN_BUDGET_BYTES` | `50GiB` | approximate best-effort per-query scan guard, **not** a hard byte ceiling: a first page alone over budget fails `QueryTooBroad`, but once at least one page has returned a spent budget (or a later page tripping its cap) returns the survivors so far with `data.stats.pulsus_partial` set; actual bytes scanned can exceed it under query parallelism / shard count (see `PULSUS_LOGQL_PIPELINE_SCAN_FACTOR`). Accepted range `1..=54975581388800` bytes (50 TiB): values above the ceiling are rejected at config load, since they would defeat the per-query scan guard. Scale tuning routes to #25 |
| `PULSUS_LOGQL_PIPELINE_SCAN_FACTOR` | `10` | LogQL pipeline first-page fetch-size hint (must be >= 1): when a query pipeline contains an in-engine dropping stage that cannot push down to SQL (a label filter, or a line filter placed after `line_format`), the engine keyset-pages `limit × factor` rows at a time through the pipeline until the true `limit` fills, the window is exhausted, or the byte scan budget is spent — responses fill exactly to `limit` (no under-return) and never over-return. This is no longer an oversample-and-truncate ceiling; a larger factor only sizes the first page (fewer round-trips). `PULSUS_LOGQL_SCAN_BUDGET_BYTES` is an approximate best-effort scan guard, not a hard byte ceiling: if the first page alone exceeds the budget the query fails `QueryTooBroad`, but once at least one page has returned a spent budget (or a later page tripping its positive cap) returns the survivors so far with `data.stats.pulsus_partial` set — never a zero/unlimited cap. Because ClickHouse enforces the cap per read block per concurrent reader (per thread, per shard), actual bytes can exceed the budget, growing with parallelism and shard count **One label filter is no longer such a stage (issue #544):** an equality or inequality over a **structured-metadata** name compiles into the statement and the request `LIMIT` goes with it, so no paging happens and this factor does not apply. The factor still governs every other dropping stage, and it governs this one again on either fallback: rendered fragments over `MAX_METADATA_FRAGMENT_BYTES`, or a selected stream carrying both the filter's name and that name without its `_extracted` suffix as labels. |
| `PULSUS_TEMPLATE_TIMEZONE` | `UTC` | IANA timezone the LogQL `line_format`/`label_format` template time functions (`now`, `date`, `toDate`, `__timestamp__`, …) render local times in. **Server configuration, never host state:** the process `$TZ` and `/etc/localtime` are not consulted, so every node carrying this configuration renders the same text for the same query — the reference resolves the zone per host, which makes one query return different text depending on which server answered it. Set it to the zone your fleet runs in to get exactly the reference's behaviour; it simply has to be stated rather than inherited (see docs/benchmarks/logs-differential-ledger.md, `template-timezone-configured`). An unknown zone name is rejected at config load, never silently replaced |
| `PULSUS_TRACEQL_SCAN_BUDGET_ROWS` | `50000000` | per-query row scan cap on every trace-search read (`max_rows_to_read`, throw); exceeding returns `422 query_too_broad`. The span-name and `q`-narrowed tag-value reads test the newest whole 5-minute buckets holding `R` spans and answer, `R` = max(1, min(10,000,000, this cap / (2 × the statement's `spans` and `traces` reads))) (`docs/api.md` §4.3), except a service-narrowed span-key or resource-key read, which answers from the bucket catalogue there; they reach this cap when one shard's 5-minute bucket at the cut holds more than about `R` spans, or when the cap is set below what one read of every part in the window costs — non-indexable searches are budget-limited, never silently slow. Must be >= 1 (`0` is ClickHouse's *unlimited* sentinel — it would silently disable the budget). Accepted range `1..=50000000000`: values above the ceiling are rejected at config load. Scale tuning routes to #25 |
| `PULSUS_TRACEQL_MAX_DEPTH` | `64` | the most parent links one climb of the `>>` and `<<` structural operators follows (issue #593). A climb that still has a parent to follow past it — a chain deeper than the bound, in a trace holding spans of both sides, not decided below it — answers `422 query_too_broad` rather than an answer the climb could not finish; a cycle ends the climb. `1`–`10000`; `0` is rejected at startup. YAML `reader.traceql_max_depth` |
| `PULSUS_TRACEQL_MAX_SERIES` | `1000` | safety cap on distinct output series a TraceQL metrics `by(...)` query may resolve, any key and up to five of them (issues #182, #596); the metrics statement counts its series inside itself, over the route's own window, and a breach is `422 query_too_broad`, never a silent subset. A `compare()` counts the values it keeps after `topN` on both sides plus four per key (#597). Other ungrouped metrics queries are not capped. Scale tuning routes to #25 |
| `PULSUS_TRACEQL_TAG_LOOKBACK` | `20m` | the window a `/api/traces/v1/tag/{tag}/values` read (and its two aliases) covers when the client sends no usable `start`/`end` (issue #478). Applies only to the reads that touch the span tables — the span-`name` dropdown and any value dropdown narrowed by `q`; an unnarrowed attribute-value read is still the catalog read, bounded by retention alone, and ignores this. The window is `[start, end)` at nanosecond precision, and the reads test at most its newest whole buckets holding `R` spans, `R` = max(1, min(10,000,000, `PULSUS_TRACEQL_SCAN_BUDGET_ROWS` / (2 × the statement's `spans` and `traces` reads))) (`docs/api.md` §4.3), so the knob decides both what a dropdown shows and what it reads. 20 minutes is near what the reference covers for a request without a range: its live store's blocks, kept for 20 minutes after they end. Must be > 0 |
| `PULSUS_LOGQL_READ_MAX_MEMORY_BYTES` | `8589934592` | per-query ClickHouse memory ceiling on **every** LogQL read (`max_memory_usage` + `max_bytes_before_external_group_by=0`, throw-not-spill); a breach returns `422 query_too_broad` naming this knob, never a `500` carrying the raw server exception (issue #398). **One exception, which never turns an answer into a refusal:** when the read that breaches is a lowered metric statement — the bucketed, sliding or reduced read (docs/query-to-sql.md) — the query is not refused; it is run again on the raw-scan read, which carries this same ceiling and returns the `422` if it breaches too. The failed attempt costs at most this ceiling in memory, released before the second read starts, and the time it ran. A timeout of a lowered statement is returned as a timeout, as for any read. Applied at the single settings origin every LogQL dispatch site goes through, including the stage-1 stream resolution `/query_range`, `/series`, `/detected_labels`, `/detected_fields` and `/stats` all share. **Two behaviour notes.** (1) `max_bytes_before_external_group_by=0` overrides an operator profile that enabled spill-to-disk: a wide aggregation that would previously have finished slowly now returns a loud `422` — deliberate, and consistent with every TraceQL read's `PULSUS_TRACEQL_READ_MAX_MEMORY_BYTES`. (2) `max_memory_usage` is per query **per server**: each shard bounds its own execution and the coordinator bounds its own; it is not a cluster-wide total. Must be >= 1 (`0` is ClickHouse's *unlimited* sentinel — it would silently disable the guard). Accepted range `1..=549755813888` bytes (512 GiB): values above the ceiling are rejected at config load. Scale tuning routes to #25 |
| `PULSUS_PROMQL_READ_MAX_MEMORY_BYTES` | `8589934592` | the same per-query memory ceiling on every PromQL read (issue #398), including the background label-cache refresh sweep. A breach on a request returns `422` with `errorType: "execution"` — the envelope Prometheus itself returns for a memory refusal — never a `500`. A breach on the **sweep** is unchanged in behaviour: it is logged and the last good snapshot keeps being served, so a degraded cache still answers `200` with results that may no longer reflect the store. Same per-server, throw-not-spill and `>= 1` rules as `PULSUS_LOGQL_READ_MAX_MEMORY_BYTES`; accepted range `1..=549755813888` bytes. Scale tuning routes to #25 |
| `PULSUS_TRACEQL_READ_MAX_MEMORY_BYTES` | `8589934592` | the same per-query memory ceiling on every TraceQL read (issue #398) — search, tag/tag-value catalog discovery, trace metrics, the service graph, and the trace-by-id point read (which previously carried no settings at all). A breach returns `422 query_too_broad` naming this knob. Same per-server, throw-not-spill and `>= 1` rules; accepted range `1..=549755813888` bytes. Scale tuning routes to #25 |
| `PULSUS_QUERY_EVAL_CONCURRENCY` | `256` | process-wide bound on concurrent CPU-bound PromQL evaluations offloaded onto the blocking pool (the read path's one `spawn_blocking(evaluate)` site); a query past the limit waits (bounded by `PULSUS_QUERY_TIMEOUT`; a breach on the metrics query surface is `503` `timeout` since issue #471, not the `408` this line used to name), never a hard rejection. Default sits below tokio's 512 blocking-pool ceiling yet above realistic heavy-query fan-in, so the uncontended fast path is the norm. Accepted range `1..=256000`: values above the ceiling are rejected at config load — an unbounded value disables the eval bound and can exceed the tokio semaphore permit bound at startup. Scale tuning routes to #25 |
| `PULSUS_TAIL_POLL_INTERVAL` | `1s` | how often an idle (caught-up) live-tail connection re-polls for new rows (must be > 0) |
| `PULSUS_TAIL_MAX_DELAY` | `5s` | ceiling on a tail client's `delay_for` param (docs/api.md §2.4); larger requests are clamped |
| `PULSUS_TAIL_MAX_CONNECTIONS` | `100` | process-wide cap on concurrent tail WebSocket connections; the next one is rejected `429` before the upgrade. Accepted range `1..=100000`: values above the ceiling are rejected at config load — an unbounded cap disables the 429 guard and can exceed the tokio semaphore permit bound at startup. Scale tuning routes to #25 |
| `PULSUS_TAIL_MAX_ENTRIES_PER_FRAME` | `1000` | bound on the per-frame `dropped_entries` representative sample sent to a slow consumer (the exact count always arrives as `dropped_total`). Accepted range `0..=1000000` (`0` = totals-only mode: exact `dropped_total`, empty sample): values above the ceiling are rejected at config load, since they would unbound the retained sample. Scale tuning routes to #25 |
| `PULSUS_TAIL_CHANNEL_DEPTH` | `4` | undelivered tail frames buffered ahead of a slow WebSocket writer before the oldest frame is evicted into `dropped_entries`/`dropped_total`. Accepted range `1..=4000`: values above the ceiling are rejected at config load, since they would make the eviction bound unreachable (an unbounded frame queue). Scale tuning routes to #25 |
| `PULSUS_TAIL_SEND_TIMEOUT` | `30s` | per-send deadline on a tail WebSocket write; a client that stops reading past it is disconnected |
| `PULSUS_TAIL_MAX_FETCH_LIMIT` | `5000` | hard cap on one tail poll's fetched-row `LIMIT`; a client `limit` above it is silently clamped before the query is built. Accepted range `1..=5000000`: values above the ceiling are rejected at config load, since they would make the pre-query clamp inert. Scale tuning routes to #25 |
| `PULSUS_TAIL_CATCHUP_SLICE` | `60s` | maximum time window one tail poll may scan/sort; backlog catch-up proceeds one slice per query, re-polling immediately until caught up (must be > 0) |

## 7. Retention (`schema/schema.sh`)

These variables are read by `schema/schema.sh` alone. They are not configuration of the binary and have no YAML key: a YAML file naming `retention_days` or a `*_landing_retention_hours` key is refused at load as an unknown key.

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_RETENTION_DAYS` | none: required | `R` in the table below, in days: the raw retention, each rollup level's when `PULSUS_ROLLUP_RETENTION_DAYS` is unset, and the least the metric catalogue keeps (`C`). Without it the script exits `2` before it contacts the server; it accepts a whole number from `1` to `4294967295` with no leading zero, and refuses anything else the same way. The script writes it into every TTL below but a rollup level's (`L`) and the five metric catalogue tables' (`C`), and into the database comment `retention_days=<N>` |
| `PULSUS_METRICS_LANDING_RETENTION_HOURS` | `6` | `H` for `metric_landing`, in hours. A whole number from `1` to `168`; anything else is exit `2` |
| `PULSUS_LOG_LANDING_RETENTION_HOURS` | `6` | `H` for `log_landing`, with the same rule |
| `PULSUS_TRACE_LANDING_RETENTION_HOURS` | `6` | `H` for `trace_landing`, with the same rule |
| `PULSUS_ROLLUP_LEVELS` | `5,60,720,7200,86400` | the rollup levels in seconds, smallest first; empty for none. Refused with exit `2` unless: whole numbers, no leading zero, no empty item; each divides 86400; one below 60 divides 60; each greater than and a multiple of the one before; at most 8; and `2 × max(first, 60) + 870 ≤ 3600 × PULSUS_METRICS_LANDING_RETENTION_HOURS`. Choose levels above the scrape interval: a level at or below it holds about one sample per series and bucket or fewer, so it stores more than raw and saves no rows |
| `PULSUS_ROLLUP_RANGES` | `3600,86400,604800,7776000` | the query range in seconds from which each level after the first is the resolution of a PromQL range query ([api.md](api.md) §3.6), one per level after the first, in level order; read only when there are levels, and empty for none. Stored in each level table's comment. Refused with exit `2`, the first failing rule the message: an empty item (`an empty item in '<text>'`); an item that is not a whole number from `1` to `999999999` with no leading zero (`<item> is not a whole number of seconds from 1 to 999999999`); not one for each level after the first (`<n> ranges for <m> levels: give one for each level after the first`); not increasing (`<a> then <b>: the ranges must increase`); shorter than its level (`<range> is shorter than its level <w>`) |
| `PULSUS_ROLLUP_COUNTER_BUCKETS` | `5` | the buckets of a level a `rate` or `increase` range covers at least for that level to answer it. Stored in each level table's comment. A whole number from `2` to `1000` with no leading zero; anything else is exit `2`, `<v> is not a whole number from 2 to 1000`. One bucket would give a `rate` window one point at the levels under 720 s. The distances in [api.md](api.md) §3.6 hold for `5` |
| `PULSUS_COLD_VOLUME` | unset: nothing moves | the volume of `PULSUS_STORAGE_POLICY` (§3) that old parts move to; letters, digits and `_`. Requires the policy; the volume must not be the policy's first, and the policy's `move_factor` must be `0` (`schema/schema.sh` checks every server before it drops anything, exit `1`) |
| `PULSUS_METRICS_HOT_DAYS` | none: required when `PULSUS_COLD_VOLUME` is set, refused when it is not | days after which a metric sample's part moves to the cold volume, from `1` to `PULSUS_RETENTION_DAYS`; the retention itself renders no move rule. Either way the tables stay on the policy, so a new part can still be written to the cold volume when the local volume is full (Tiering, below) |
| `PULSUS_LOGS_HOT_DAYS` | as above | the same for log lines and the log rollups |
| `PULSUS_TRACES_HOT_DAYS` | as above | the same for spans and the trace tables read by time |
| `PULSUS_ROLLUP_RETENTION_DAYS` | unset: every level keeps `PULSUS_RETENTION_DAYS` | each level's retention in days, comma-separated, one per level of `PULSUS_ROLLUP_LEVELS` in its order. Each a whole number from `PULSUS_RETENTION_DAYS` to `4294967295` with no leading zero (set and empty is refused as an empty item), and each but the last at least ⌈the next level's switch point (`PULSUS_ROLLUP_RANGES`) ÷ 86400⌉ + 1, so a range that selects a level finds every day it touches: with the default switch points the first four levels need at least `2`, `2`, `8`, `91`. The metric catalogue keeps the largest of `PULSUS_RETENTION_DAYS` and these (`C` below) |
| `PULSUS_ROLLUP_HOT_DAYS` | unset: no level moves | each level's move age in days, one per level; refused when `PULSUS_COLD_VOLUME` is unset. Each a whole number from `1` to that level's retention (set and empty is refused as an empty item); the retention itself renders no move rule |

**The binary reads the retention from the database.** At startup it reads the database comment and is not ready until the comment is exactly `retention_days=<N>`; its retry warning names the script. The trace read floor (a read that selects spans by a window reads no span that started `N` days or more before the request, [api.md](api.md) §4), the live-tail lower bound (`N` days and one more before the request) and `/api/v1/status/runtimeinfo`'s `storageRetention` all use that `N`. Changing retention is a rebuild with the script, which drops the database.

**When a row goes.** `R` is `PULSUS_RETENTION_DAYS` and `H` the table's landing hours. `L` is a level's `PULSUS_ROLLUP_RETENTION_DAYS` item, `R` when that is unset, and `C`, the metric catalogue's retention, the largest of `R` and every `L`. A row is due at the time below, computed in whole seconds and capped at `4294967295` (2106-02-07). The server deletes due rows only during a TTL merge, which it runs on a table at most once per `merge_with_ttl_timeout`: `3600` s on the three landing tables, the server's default `14400` s on every other. "Whole part" keeps a part until every row in it is due, and a part never spans two partitions. "Row by row" deletes each due row.

| Table | Due at | Removed | Partition |
|-------|--------|---------|-----------|
| `log_landing` | `received_ms` + `H` hours | whole part | hour of `received_ms` |
| `log_metrics_<res>` | `bucket_ns` + `R` days | whole part | UTC day of `bucket_ns` |
| `log_patterns` | `bucket_ns` + `R` days | whole part | UTC day of `bucket_ns` |
| `log_samples` | `timestamp_ns` + `R` days | whole part | UTC day of `timestamp_ns` |
| `log_streams` | the end of the UTC month `month` + `R` days | whole part | `month`, a UTC month |
| `log_streams_idx` | the end of the UTC month `month` + `R` days | whole part | `month`, a UTC month |
| `metric_exemplars` | `unix_milli` + `R` days | whole part | UTC day of `unix_milli` |
| `metric_hist_samples` | `unix_milli` + `R` days | whole part | UTC day of `unix_milli` |
| `metric_label_index` | the end of the UTC day of `last_seen` + `C` days | row by row | none |
| `metric_label_values` | the end of the UTC day of `last_seen` + `C` days | row by row | none |
| `metric_labels` | the end of the UTC day of `last_seen` + `C` days | row by row | none |
| `metric_landing` | `received_ms` + `H` hours | whole part | hour of `received_ms` |
| `metric_metadata` | `updated_ns` + `C` days | row by row | none |
| `metric_samples` | `unix_milli` + `R` days | whole part | UTC day of `unix_milli` |
| `metric_samples_<N>s` | the start of the UTC day of `t_end − 1 ms` + `L` days | whole part | UTC day of `t_end − 1 ms` |
| `metric_series` | the end of the UTC day `day` + `C` days | whole part | `day`, a UTC day |
| `resources` | the end of the UTC day `day` + `R` days | whole part | `day`, a UTC day |
| `service_bucket_resources` | the start of the 5-minute `bucket` + `R` days | whole part | UTC day of `bucket` |
| `service_edges` | `minute` + `R` days; `minute` − 240 s + `R` days on a row whose `conn_type` is empty | whole part | UTC day of `minute` |
| `spans` | `start_ns` + `R` days | whole part | UTC day of `start_ns` |
| `error_spans` | `start_ns` + `R` days | whole part | UTC day of `start_ns` |
| `tag_bucket_values` | the start of the 5-minute `bucket` + `R` days | whole part | UTC day of `bucket` |
| `tag_names` | the end of the UTC day of `last_start_ns` + `R` days | row by row | none |
| `tag_values` | the end of the UTC day of `last_start_ns` + `R` days | row by row | none |
| `trace_landing` | `received_ms` + `H` hours | whole part | hour of `received_ms` |
| `traces` | the end of the UTC day of `last_start_ns` + `R` days | whole part | `day`, the UTC day of the earliest of the trace's spans in one push |

With `PULSUS_COLD_VOLUME` set, a table of a Tiering family (below) also moves to the cold volume at its time column plus its family's `…_HOT_DAYS`; it is still removed at the time above.

An hour is in the server's time zone. `month` is the UTC month of the log lines a stream registration covers; `last_seen` is the latest sample instant a series registration carried; `last_start_ns` is, in `tag_names` and `tag_values`, the latest start of a span carrying the tag and, in `traces`, the latest start of the trace's spans. A row due at the end of a UTC day plus `R` outlives every part of a table partitioned by UTC day that holds the samples or spans it describes, so a series, stream or trace is never found without its labels or its index while its data is stored. The landing tables' rows are read by nothing once their views have run, so their expiry deletes nothing a query reads. The metric catalogue keeps `C` days, so the same holds for a rollup level kept longer than raw.

### Tiering

With `PULSUS_STORAGE_POLICY` and `PULSUS_COLD_VOLUME` set, `schema/schema.sh` gives each table of a family a second TTL rule that moves its parts to the cold volume. A family whose move age equals `PULSUS_RETENTION_DAYS` gets no move rule.

| Family | Tables | Moves after |
|--------|--------|-------------|
| metrics | `metric_samples`, `metric_hist_samples`, `metric_exemplars` | `PULSUS_METRICS_HOT_DAYS` |
| logs | `log_samples`, `log_metrics_<res>`, `log_patterns` | `PULSUS_LOGS_HOT_DAYS` |
| traces | `spans`, `error_spans`, `tag_bucket_values`, `service_bucket_resources`, `service_edges` | `PULSUS_TRACES_HOT_DAYS` |
| levels | `metric_samples_<N>s` | its `PULSUS_ROLLUP_HOT_DAYS` item; no move rule when that is unset or equals the level's retention |
| never moves | `log_landing`, `log_streams`, `log_streams_idx`, `metric_labels`, `metric_label_index`, `metric_label_values`, `metric_landing`, `metric_metadata`, `metric_series`, `resources`, `tag_names`, `tag_values`, `trace_landing`, `traces` | created without `storage_policy`, on the server's `default` policy |

A part moves when its newest row is older than the move age, so a UTC day `D` of raw data with move age `H` is local until `D + 1 + H` days; rows written for a day already moved go straight to the cold volume. A level's UTC day `D` with move age `H` is local until `D + H` days.

**A table on the policy can reach the cold volume without a move rule.** When the policy's first volume has no space for a new part — an insert, a merge or a replica's fetch — the server writes that part to the next volume that has space, the cold volume here. `move_factor` `0` stops the server moving existing parts to free space; it does not stop this. So a table on the policy — a family's table before its move age, a family whose move age equals `PULSUS_RETENTION_DAYS`, or a rollup level before its move age or without one — is local only while the local volume has room. The fourteen tables in the last row are not on the policy and cannot reach the cold volume.

Every server carries the policy, the same on every replica of every shard. For example, with `PULSUS_STORAGE_POLICY=tiered` and `PULSUS_COLD_VOLUME=cold`:

```xml
<clickhouse>
  <storage_configuration>
    <disks>
      <cold_store>
        <type>s3</type>
        <endpoint>https://BUCKET-ENDPOINT/BUCKET/pulsus/{shard}/{replica}/</endpoint>
        <use_environment_credentials>true</use_environment_credentials>
      </cold_store>
      <cold_cache>
        <type>cache</type>
        <disk>cold_store</disk>
        <path>CACHE-DIRECTORY</path>
        <max_size>CACHE-BYTES</max_size>
      </cold_cache>
    </disks>
    <policies>
      <tiered>
        <volumes>
          <hot><disk>default</disk></hot>
          <cold><disk>cold_cache</disk></cold>
        </volumes>
        <move_factor>0</move_factor>
      </tiered>
    </policies>
  </storage_configuration>
</clickhouse>
```

The script's refusals, each printed as `schema.sh: <message>`. Rules 1 to 5 are checked in this order before the server is contacted; the last, before anything is dropped:

| Rule | Message | Exit |
|------|---------|------|
| 1 | `PULSUS_COLD_VOLUME is set and PULSUS_STORAGE_POLICY is not` | `2` |
| 2 | `cold_volume must be letters, digits and underscores: <v>` | `2` |
| 3 a | `<VAR> is not set; with PULSUS_COLD_VOLUME set each of PULSUS_METRICS_HOT_DAYS, PULSUS_LOGS_HOT_DAYS and PULSUS_TRACES_HOT_DAYS is required` | `2` |
| 3 b | `<VAR> is set and PULSUS_COLD_VOLUME is not` | `2` |
| 3 c | `<VAR> must be a whole number of days from 1 to <R>: <value>` | `2` |
| 4 a | `PULSUS_ROLLUP_RETENTION_DAYS: an empty item in '<text>'` | `2` |
| 4 b | `PULSUS_ROLLUP_RETENTION_DAYS: <k> items for <m> levels` | `2` |
| 4 c | `PULSUS_ROLLUP_RETENTION_DAYS: <item> for the <s>s level is not a whole number of days from <R> to 4294967295` | `2` |
| 4 d | `PULSUS_ROLLUP_RETENTION_DAYS: <item> for the <s>s level is too short: the next level's switch point, <from> s, requires <d> retention days` | `2` |
| 5 a | `PULSUS_ROLLUP_HOT_DAYS is set and PULSUS_COLD_VOLUME is not` | `2` |
| 5 b | `PULSUS_ROLLUP_HOT_DAYS: an empty item in '<text>'` | `2` |
| 5 c | `PULSUS_ROLLUP_HOT_DAYS: <k> items for <m> levels` | `2` |
| 5 d | `PULSUS_ROLLUP_HOT_DAYS: <item> for the <s>s level is not a whole number of days from 1 to <keep>` | `2` |
| before the drop | `storage policy '<p>' with a volume '<v>' after its first and move_factor 0 is defined on <n> of <m> hosts; every server's configuration must carry it (docs/configuration.md §3)`, without `with a volume … move_factor 0` when no cold volume is set | `1` |

Rule 3 is checked for `PULSUS_METRICS_HOT_DAYS`, then `PULSUS_LOGS_HOT_DAYS`, then `PULSUS_TRACES_HOT_DAYS`, all of a, b and c for one before the next. `<VAR>` is the variable's name and `<R>` is `PULSUS_RETENTION_DAYS`. Rule 4 is checked when `PULSUS_ROLLUP_RETENTION_DAYS` is set, rule 5 when `PULSUS_ROLLUP_HOT_DAYS` is, each item in level order and 4 c for every level before 4 d. `<s>` is the level in seconds; `<from>` the switch point of the level after it; `<d>` = ⌈`<from>` ÷ 86400⌉ + 1; `<keep>` the level's `PULSUS_ROLLUP_RETENTION_DAYS` item, else `<R>`; `<k>` the number of items and `<m>` of levels.

## 8. Ruler (M7)

| Variable | Default | Description |
|----------|---------|-------------|
| `PULSUS_RULER_ENABLED` | `false` | mount rule APIs + evaluation loop (mode `all` only) |
| `PULSUS_RULER_POLL_INTERVAL` | `30s` | rule-group reload cadence |
| `PULSUS_RULER_MAX_RESULT_BYTES` | `10MiB` | per-rule evaluation result cap |

## 9. YAML file

Every variable in §§1–6 and §8 maps 1:1 into YAML (env var wins on conflict); §7's are the script's alone. This is the **complete** document schema — unknown keys are rejected; every key shown with its default:

```yaml
# root scalars
mode: all                        # all | writer | reader
host: 0.0.0.0
port: 3100
log_level: info                  # error | warn | info | debug | trace
auth_user: null                  # both set => basic auth; one-sided => startup error
auth_password: null              # secret: never appears in redacted output
compat_endpoints: false
cors_origin: "*"
query_timeout: 2m
tls_cert: null                   # PEM cert chain path; with tls_key set, the one listener is TLS-only
tls_key: null                    # PEM private key path; one-sided => startup error
storage_policy: null
traceql_indexed_attributes: []   # PULSUS_TRACEQL_INDEXED_ATTRIBUTES
log_rollup_resolution: 5s
metrics_dedup_window: 10000           # blocks each metrics table remembers for deduplication
log_dedup_window: 10000               # blocks each logs table remembers for deduplication
trace_dedup_window: 10000             # blocks each traces table remembers for deduplication
cluster: null                    # ClickHouse cluster name; enables distributed DDL
dist_suffix: _dist
skip_unavailable_shards: false
availability_zone: null          # this node's AZ; pool prefers clickhouse.servers whose zone matches (§4)
az_detect: off                   # off | aws | gcp | azure | auto — detect zone from cloud instance
                                 # metadata at startup when availability_zone is unset (fail-soft)
exp_histogram_mode: classic      # classic | native | dual
otlp_translation_strategy: UnderscoreEscapingWithSuffixes   # UnderscoreEscapingWithSuffixes | UnderscoreEscapingWithoutSuffixes
                                 # | NoUTF8EscapingWithSuffixes | NoTranslation
otlp_promote_scope_metadata: false

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
  log_patterns: true
  discover_log_levels: true
  ingest_dedup: true
  ingest_dedup_window: 5m
  ingest_dedup_max_bytes: 16MiB
  metrics_landing_retries: 3     # resends of a failed metrics landing insert
  metrics_landing_inserters: 4   # insert workers on the metrics landing queue
  metrics_landing_max_rows: 1048576   # per-push landing row ceiling
  log_landing_retries: 3         # resends of a failed logs landing insert
  log_landing_inserters: 4       # insert workers on the logs landing queue
  log_landing_max_rows: 1048576       # per-push landing row ceiling
  trace_landing_retries: 3       # resends of a failed traces landing insert
  trace_landing_inserters: 4     # insert workers on the traces landing queue
  trace_landing_max_rows: 1048576     # per-push landing row ceiling

reader:
  cache_ttl: 60s
  cache_max_series: 50000
  cache_window: 24h
  promql_max_samples: 50000000
  promql_lookback: 5m
  promql_experimental_functions: false
  promql_max_metric_fanout: 1000
  promql_max_cache_scan: 200000
  promql_max_info_series: 100000
  promql_grouped_push: true
  logql_scan_budget_bytes: 50GiB
  logql_pipeline_scan_factor: 10
  template_timezone: UTC          # IANA zone for LogQL template time functions; never the host's
  traceql_scan_budget_rows: 50000000
  traceql_max_depth: 64
  traceql_max_series: 1000
  traceql_tag_lookback: 20m
  logql_read_max_memory_bytes: 8589934592
  promql_read_max_memory_bytes: 8589934592
  traceql_read_max_memory_bytes: 8589934592
  query_eval_concurrency: 256
  tail_poll_interval: 1s
  tail_max_delay: 5s
  tail_max_connections: 100
  tail_max_entries_per_frame: 1000
  tail_channel_depth: 4
  tail_send_timeout: 30s
  tail_max_fetch_limit: 5000
  tail_catchup_slice: 60s

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
    image: clickhouse/clickhouse-server:26.8.21.10
    volumes: [ clickhouse-data:/var/lib/clickhouse ]
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 5s

  # The schema. The binary creates none, so this one-shot service builds it
  # before the server starts: it drops the database and applies
  # `schema/schema.sql` fresh. Set `PULSUS_CLUSTER` here too for a sharded
  # ClickHouse.
  schema:
    image: ghcr.io/digitalis-io/pulsusdb:latest
    entrypoint: [ "/usr/local/share/pulsusdb/schema/schema.sh" ]
    environment:
      CLICKHOUSE_SERVER: clickhouse
      PULSUS_RETENTION_DAYS: "7"
    depends_on:
      clickhouse: { condition: service_healthy }

  pulsusdb:
    image: ghcr.io/digitalis-io/pulsusdb:latest
    environment:
      CLICKHOUSE_SERVER: clickhouse
    ports: [ "3100:3100" ]
    depends_on:
      clickhouse: { condition: service_healthy }
      schema: { condition: service_completed_successfully }

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

**Minimum supported ClickHouse: 26.8 LTS** (the supported LTS line; servers before 26.3 do not tag an HTTP-200 mid-stream exception, so it cannot be told apart from result text — issue #412). `schema/schema.sh` verifies the server version before it sends anything and refuses an older server; the serving process verifies it again at startup.
