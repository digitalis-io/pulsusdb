#!/usr/bin/env bash
# One-shot self-signed cert generation for the `tls` benchmark scenario.
# Not run in CI. Produces xtask/docker/certs/{ca.crt,server.crt,server.key}.
#
# Usage: ./gen-certs.sh [output-dir]
set -euo pipefail

OUT_DIR="${1:-$(dirname "$0")/certs}"
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

CN="${CLICKHOUSE_TLS_CN:-localhost}"

openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
    -keyout ca.key -out ca.crt \
    -subj "/CN=pulsusdb-bench-ca"

openssl req -newkey rsa:2048 -nodes \
    -keyout server.key -out server.csr \
    -subj "/CN=${CN}"

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt -days 365 \
    -extfile <(printf "subjectAltName=DNS:%s,IP:127.0.0.1" "${CN}")

rm -f server.csr ca.srl
echo "certs written to ${OUT_DIR}: ca.crt server.crt server.key"
