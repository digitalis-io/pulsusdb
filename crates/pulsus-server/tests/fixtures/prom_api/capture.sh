#!/usr/bin/env bash
# Golden-capture script for `/api/v1/*` (issue #32 architect plan
# amendment §3, pinned mechanism: "provenance-scripted, run-once, not
# CI" — mirrors the #29 corpus precedent,
# `crates/pulsus-promql/tests/corpus/extract-upstream-cases.py`).
#
# Runs a real `prom/prometheus:v3.13.0` (pinned by digest, see
# PROVENANCE.md), seeds it with a small, fixed sample set via
# `promtool tsdb create-blocks-from openmetrics` (deterministic, no
# wall-clock-relative timestamps — every seeded sample lands at a fixed
# reference time so captures never go stale) plus its own default
# self-scrape (job "prometheus", which supplies real `/api/v1/metadata`
# content with genuine HELP/TYPE text), then curls every `/api/v1/*`
# endpoint (GET/POST, success/error, plus the float/special-value corpus)
# and writes each response body **verbatim** to
# `fixtures/prom_api/<endpoint>.<case>.json`.
#
# Run-once by a human whenever the fixture set needs to change; **never**
# invoked from CI (mirrors the #29 precedent). Requires `podman` and
# network access to pull the pinned image. Re-run `sha256sum` over the
# output and update PROVENANCE.md's table after any change here.
#
# Usage:
#   cd crates/pulsus-server/tests/fixtures/prom_api
#   ./capture.sh

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Pinned by digest (see PROVENANCE.md) — always resolves to the exact same
# image regardless of what "v3.13.0" is retagged to later.
IMAGE="prom/prometheus@sha256:0e698e35e50d1ddc2d11a4a55b089fe62eb71358a5c204dfafd21bdf8ffe04b8"
CONTAINER=prom-api-golden-capture
HTTP_PORT=19090

# The fixed reference instant every seeded sample lands on — Prometheus's
# own documented API example timestamp (docs.prometheus.io's `/query`
# example uses exactly `1435781451.781`), chosen so captured goldens read
# naturally against that well-known reference rather than an arbitrary one.
REF_TS=1435781451

WORKDIR="$(mktemp -d)"
# The container runs as its image-default unprivileged user (`nobody`,
# uid 65534), which is never the host user that created `$WORKDIR` — world
# access is required for it to read the seed file / write the imported
# block, regardless of rootless podman's UID mapping.
chmod 777 "$WORKDIR"
trap 'podman rm -f "$CONTAINER" >/dev/null 2>&1 || true; rm -rf "$WORKDIR"' EXIT

echo "==> pulling $IMAGE"
podman pull "$IMAGE" >/dev/null

echo "==> writing the seed OpenMetrics fixture (fixed timestamp $REF_TS)"
cat >"$WORKDIR/seed.openmetrics" <<EOF
# TYPE up gauge
# HELP up 1 if the target is healthy
up{job="node",instance="localhost:9100"} 1 ${REF_TS}.000
up{job="node",instance="localhost:9101"} 0 ${REF_TS}.000
# TYPE http_requests_total counter
# HELP http_requests_total total HTTP requests
http_requests_total{job="api",method="get",code="200"} 1027 ${REF_TS}.000
http_requests_total{job="api",method="post",code="500"} 3 ${REF_TS}.000
# EOF
EOF

mkdir -p "$WORKDIR/data"
chmod 777 "$WORKDIR/data"
echo "==> importing the seed block via promtool"
podman run --rm -v "$WORKDIR":/work:Z --entrypoint promtool "$IMAGE" \
  tsdb create-blocks-from --experimental openmetrics /work/seed.openmetrics /work/data

cat >"$WORKDIR/prometheus.yml" <<'EOF'
global:
  scrape_interval: 2s
scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ["localhost:9090"]
EOF
chmod 666 "$WORKDIR/prometheus.yml"

echo "==> starting prometheus (bridge networking, static port — #15 lesson)"
podman rm -f "$CONTAINER" >/dev/null 2>&1 || true
podman run -d --rm --name "$CONTAINER" -p "${HTTP_PORT}:9090" \
  -v "$WORKDIR":/work:Z \
  "$IMAGE" \
  --config.file=/work/prometheus.yml \
  --storage.tsdb.path=/work/data \
  --web.enable-remote-write-receiver >/dev/null

echo "==> waiting for readiness"
for _ in $(seq 1 30); do
  if curl -fs "http://localhost:${HTTP_PORT}/-/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fs "http://localhost:${HTTP_PORT}/-/ready" >/dev/null

# Gives the self-scrape a few ticks (2s interval) so `/api/v1/metadata`
# has real, non-empty scrape-derived content.
sleep 8

BASE="http://localhost:${HTTP_PORT}"

capture_get() {
  local case="$1" path="$2"
  echo "  GET  $path -> $case"
  curl -fs -g "${BASE}${path}" -o "${case}.json"
}

capture_get_allow_error() {
  local case="$1" path="$2"
  echo "  GET  $path -> $case (error expected)"
  curl -s -g "${BASE}${path}" -o "${case}.json"
}

capture_post() {
  local case="$1" path="$2" body="$3"
  echo "  POST $path ($body) -> $case"
  curl -fs -X POST "${BASE}${path}" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data "$body" -o "${case}.json"
}

echo "==> capturing /api/v1/query"
capture_get "query.vector_get" "/api/v1/query?query=up&time=${REF_TS}"
capture_post "query.vector_post" "/api/v1/query" "query=up&time=${REF_TS}"
capture_get_allow_error "query.error_bad_data_get" "/api/v1/query?query=up%7B"

# Float/special-value corpus: bare PromQL number literals, evaluated as a
# scalar — exercises `prom_float` at every documented boundary without
# depending on any seeded series.
capture_get "query.scalar_zero" "/api/v1/query?query=0"
capture_get "query.scalar_neg_zero" "/api/v1/query?query=-0"
capture_get "query.scalar_one" "/api/v1/query?query=1"
capture_get "query.scalar_100000" "/api/v1/query?query=100000"
capture_get "query.scalar_1e20" "/api/v1/query?query=1e20"
capture_get "query.scalar_1e21" "/api/v1/query?query=1e21"
capture_get "query.scalar_1e_minus_4" "/api/v1/query?query=1e-4"
capture_get "query.scalar_1e_minus_5" "/api/v1/query?query=1e-5"
capture_get "query.scalar_5e_minus_324" "/api/v1/query?query=5e-324"
capture_get "query.scalar_f64_max" "/api/v1/query?query=1.7976931348623157e308"
capture_get "query.scalar_nan" "/api/v1/query?query=0%2F0"
capture_get "query.scalar_pos_inf" "/api/v1/query?query=1%2F0"
capture_get "query.scalar_neg_inf" "/api/v1/query?query=-1%2F0"

echo "==> capturing /api/v1/query_range"
capture_get "query_range.matrix_get" "/api/v1/query_range?query=up&start=$((REF_TS - 60))&end=${REF_TS}&step=30"
capture_post "query_range.matrix_post" "/api/v1/query_range" "query=up&start=$((REF_TS - 60))&end=${REF_TS}&step=30"
capture_get_allow_error "query_range.error_bad_data_get" "/api/v1/query_range?query=up%7B&start=0&end=1&step=1"

echo "==> capturing /api/v1/labels"
capture_get "labels.with_match_get" "/api/v1/labels?match[]=up&start=$((REF_TS - 60))&end=$((REF_TS + 60))"
capture_post "labels.with_match_post" "/api/v1/labels" "match%5B%5D=up&start=$((REF_TS - 60))&end=$((REF_TS + 60))"
capture_get "labels.no_match_get" "/api/v1/labels"
# Code-review round-1 fix: a matcher-only selector (no concrete metric
# name) is a valid Prometheus `match[]` — `{job="node"}` matches only the
# two seeded `up` series, never `http_requests_total` (job="api").
capture_get "labels.matcher_only_get" "/api/v1/labels?match[]=%7Bjob%3D%22node%22%7D&start=$((REF_TS - 60))&end=$((REF_TS + 60))"

echo "==> capturing /api/v1/label/{name}/values"
capture_get "label_values.job_get" "/api/v1/label/job/values?start=$((REF_TS - 60))&end=$((REF_TS + 60))"
capture_get "label_values.matcher_only_get" "/api/v1/label/instance/values?match[]=%7Bjob%3D%22node%22%7D&start=$((REF_TS - 60))&end=$((REF_TS + 60))"

echo "==> capturing /api/v1/series"
capture_get "series.with_match_get" "/api/v1/series?match[]=up&start=$((REF_TS - 60))&end=$((REF_TS + 60))"
capture_post "series.with_match_post" "/api/v1/series" "match%5B%5D=up&start=$((REF_TS - 60))&end=$((REF_TS + 60))"
capture_get_allow_error "series.no_match_error_get" "/api/v1/series"
capture_get "series.matcher_only_get" "/api/v1/series?match[]=%7Bjob%3D%22node%22%7D&start=$((REF_TS - 60))&end=$((REF_TS + 60))"

echo "==> capturing /api/v1/metadata"
capture_get "metadata.scrape_derived_get" "/api/v1/metadata?metric=go_goroutines"
# Code-review round-1 fix #3: `/metadata` never strips a derived-series
# suffix — `metric_metadata`/Prometheus's own metadata cache is keyed by
# the base family name only, so a `_bucket`/`_sum`/`_count`-suffixed query
# name (which is never itself a registered family) returns an empty `data`
# object, not the base family's descriptor.
capture_get "metadata.derived_suffix_name_get" "/api/v1/metadata?metric=go_goroutines_bucket"

echo "==> capturing /api/v1/query_exemplars"
capture_get "query_exemplars.empty_get" "/api/v1/query_exemplars?query=up&start=${REF_TS}&end=${REF_TS}"

echo "==> capturing /api/v1/status/*"
capture_get "status.buildinfo_get" "/api/v1/status/buildinfo"
capture_get "status.config_get" "/api/v1/status/config"
capture_get "status.flags_get" "/api/v1/status/flags"
capture_get "status.runtimeinfo_get" "/api/v1/status/runtimeinfo"
capture_get "status.tsdb_get" "/api/v1/status/tsdb"

echo "==> writing sha256 manifest"
sha256sum ./*.json | sort -k2 >SHA256SUMS

echo "==> done. Review the diff, then update PROVENANCE.md's capture date/image digest if changed."
