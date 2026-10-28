#!/bin/sh
# Issue #746 part E: a cluster built from the setup form's own files, set up
# by following its own `setup.md` (tests E1-E6).
#
# `cargo xtask setup-bundle` writes the files for 4 ClickHouse hosts in 2
# shards of 2 replicas, 3 Keeper hosts, TLS with a CA made here, and the
# object store. Each container mounts its host's files, as setup.md's step 1
# copies them, and every check runs setup.md's own statement against its own
# expected value.
#
# Every container is on one network with a fixed address, and the hosts are
# given to the form by address, so nothing needs a name lookup. The checks
# reach the addresses directly: on a CI runner from the host, and with
# rootless podman from inside its network namespace:
#
#   cargo build -p pulsus-server -p xtask
#   RT=docker sh ci/setup-cluster/run.sh
#   RT=podman podman unshare --rootless-netns sh ci/setup-cluster/run.sh
#
# PREFIX names every container and the network; SUBNET is its first three
# octets; PULSUS_SETUP_PORT is the port PulsusDB listens on, on 127.0.0.1.
set -eu

RT=${RT:-docker}
P=${PREFIX:-pulsus-setup-}
SUBNET=${SUBNET:-172.31.46}
PORT=${PULSUS_SETUP_PORT:-33746}
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
target=${CARGO_TARGET_DIR:-$root/target}
work=$(mktemp -d)

SERVER_IMAGE=docker.io/clickhouse/clickhouse-server@sha256:d3fd302416d9523d5d64239d5713d1c16c20ecea2045ac70d7e941b376cb266e
KEEPER_IMAGE=docker.io/clickhouse/clickhouse-keeper@sha256:112ce72cf0ad4c48b5884159d40c4821914c06e3df3e73da61a5b69398960c63
STORE_IMAGE=docker.io/rustfs/rustfs@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2

# Test-only secrets, set in each server's environment as setup.md's step 2 says.
export PULSUS_CLICKHOUSE_PASSWORD=pulsus-setup-password
INTERSERVER=pulsus-setup-interserver
S3_KEY=pulsus-setup
S3_SECRET=pulsus-setup-secret

hosts="$SUBNET.11 $SUBNET.12 $SUBNET.13 $SUBNET.14"
keepers="$SUBNET.21 $SUBNET.22 $SUBNET.23"
store=$SUBNET.30
h1=$SUBNET.11

pulsus_pid=""
cleanup() {
    [ -z "$pulsus_pid" ] || kill "$pulsus_pid" 2>/dev/null || true
    for name in ch1 ch2 ch3 ch4 kp1 kp2 kp3 store; do
        $RT rm -f -v "${P}$name" >/dev/null 2>&1 || true
    done
    $RT network rm "${P}net" >/dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT INT TERM

fail() {
    echo "setup-cluster: $*" >&2
    exit 1
}

# Retries `$@` once a second for up to `$1` seconds.
retry() {
    tries=$1
    shift
    i=0
    until "$@"; do
        i=$((i + 1))
        [ "$i" -lt "$tries" ] || return 1
        sleep 1
    done
}

# --- The CA and one server certificate for every address. -------------------
tls=$work/tls
mkdir -p "$tls"
san="IP:127.0.0.1"
for ip in $hosts $keepers; do san="$san,IP:$ip"; done
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=pulsus setup CI CA" \
    -keyout "$tls/ca.key" -out "$tls/ca.crt" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=pulsus setup CI" \
    -keyout "$tls/server.key" -out "$tls/server.csr" 2>/dev/null
printf 'subjectAltName=%s\n' "$san" > "$tls/ext"
openssl x509 -req -in "$tls/server.csr" -CA "$tls/ca.crt" -CAkey "$tls/ca.key" \
    -CAcreateserial -days 2 -extfile "$tls/ext" -out "$tls/server.crt" 2>/dev/null
chmod 644 "$tls/server.key"
ca=$tls/ca.crt

# --- The form's files. --------------------------------------------------------
inputs() {
    for ip in $1; do echo "hosts=$ip"; done
    for ip in $keepers; do echo "keepers=$ip"; done
    cat <<EOF
shards=$2
replicas=2
cluster=pulsus_cluster
database=pulsus
tls_cert=/etc/ch/server.crt
tls_key=/etc/ch/server.key
tls_ca=/etc/ch/ca.crt
local_path=/var/lib/clickhouse/
remote_url=http://$store:9000/pulsus-setup/
cores=2
memory_gib=4
disk_gib=20
raw_local_days=7
raw_remote_days=30
l1_width=60
l1_from=3600
l1_local=30
l1_remote=335
l2_width=720
l2_from=86400
l2_local=400
EOF
}
inputs "$hosts" 2 > "$work/inputs"
bundle=$work/bundle
cargo run -q -p xtask -- setup-bundle "$work/inputs" "$bundle"
# E2's bundle: 3 shards of 2.
inputs "$hosts $SUBNET.15 $SUBNET.16" 3 > "$work/inputs-6"
cargo run -q -p xtask -- setup-bundle "$work/inputs-6" "$work/bundle-6"
md=$bundle/setup.md

# The body of the first fence of kind `$3` after the heading `### $2` in
# the setup.md at `$1`.
fence() {
    awk -v id="### $2" -v kind="$3" '
        $0 == id { inside = 1; next }
        inside && /^### / { exit }
        inside && !open && $0 == "```" kind { open = 1; next }
        open && $0 == "```" { exit }
        open { print }
    ' "$1"
}
sql() { fence "$md" "$1" sql; }
expected() { fence "$md" "$1" text; }

# A statement against host 1 over TLS, as the pulsus user.
ask() {
    curl -sS --fail-with-body --cacert "$ca" -u "pulsus:$PULSUS_CLICKHOUSE_PASSWORD" \
        "https://$h1:8443/" --data-binary "$1"
}

check() {
    got=$(ask "$(sql "$1")")
    want=$(expected "$1")
    [ "$got" = "$want" ] || fail "$1: got '$got', setup.md expects '$want'"
    echo "setup-cluster: $1 as setup.md expects"
}

# --- The containers. ------------------------------------------------------------
$RT network create --subnet "$SUBNET.0/24" "${P}net" >/dev/null

$RT run -d --name "${P}store" --network "${P}net" --ip "$store" \
    -e RUSTFS_ACCESS_KEY=$S3_KEY -e RUSTFS_SECRET_KEY=$S3_SECRET "$STORE_IMAGE" >/dev/null
make_bucket() {
    curl -sS --fail-with-body --aws-sigv4 aws:amz:us-east-1:s3 --user "$S3_KEY:$S3_SECRET" \
        -X PUT "http://$store:9000/pulsus-setup" -o /dev/null 2>/dev/null
}
retry 60 make_bucket || fail "the object store did not take the bucket"

# K1.
j=0
for ip in $keepers; do
    j=$((j + 1))
    $RT run -d --name "${P}kp$j" --network "${P}net" --ip "$ip" \
        -v "$bundle/keepers/$j/keeper_config.xml:/etc/clickhouse-keeper/keeper_config.xml:ro" \
        -v "$tls:/etc/ch:ro" "$KEEPER_IMAGE" >/dev/null
done

# Steps 1-3: each host's files where step 1 puts them, step 2's variables,
# and a start in place of the restart.
start_server() {
    i=$1
    ip=$2
    mounts=""
    for f in "$bundle/nodes/$i/config.d/"*; do
        mounts="$mounts -v $f:/etc/clickhouse-server/config.d/$(basename "$f"):ro"
    done
    for f in "$bundle/nodes/$i/users.d/"*; do
        mounts="$mounts -v $f:/etc/clickhouse-server/users.d/$(basename "$f"):ro"
    done
    # shellcheck disable=SC2086
    $RT run -d --name "${P}ch$i" --network "${P}net" --ip "$ip" $mounts \
        -v "$tls:/etc/ch:ro" \
        -e CLICKHOUSE_INTERSERVER_SECRET=$INTERSERVER \
        -e PULSUS_CLICKHOUSE_PASSWORD="$PULSUS_CLICKHOUSE_PASSWORD" \
        -e PULSUS_S3_ACCESS_KEY_ID=$S3_KEY -e PULSUS_S3_SECRET_ACCESS_KEY=$S3_SECRET \
        "$SERVER_IMAGE" >/dev/null
}
i=0
for ip in $hosts; do
    i=$((i + 1))
    start_server "$i" "$ip"
done

server_up() {
    curl -sS --fail-with-body --cacert "$ca" "https://$1:8443/ping" >/dev/null 2>&1
}
for ip in $hosts; do
    retry 120 server_up "$ip" || {
        $RT logs "${P}ch1" 2>&1 | tail -40 >&2
        fail "$ip did not answer over TLS"
    }
done
echo "setup-cluster: every server answers over TLS"

# `ruok` over TLS, with the CA; prints the answer.
ruok_tls() {
    printf ruok | timeout 5 openssl s_client -quiet -verify_return_error \
        -CAfile "$ca" -connect "$1:9181" 2>/dev/null || true
}
keeper_up() { [ "$(ruok_tls "$1")" = imok ]; }
for ip in $keepers; do
    retry 60 keeper_up "$ip" || fail "Keeper $ip did not answer imok over TLS"
done
echo "setup-cluster: every Keeper answers over TLS"

# --- E1: V1, V2, V0 as setup.md gives them. -------------------------------------
check V1
check V2
check V0

# --- E2: a bundle for 3 shards of 2 does not match this cluster. ------------------
got=$(ask "$(fence "$work/bundle-6/setup.md" V1 sql)")
want=$(fence "$work/bundle-6/setup.md" V1 text)
[ "$got" = 4 ] && [ "$want" = 6 ] || fail "E2: got '$got' against '$want'"
echo "setup-cluster: E2 the 3-shard bundle's V1 expects 6 and finds 4"

# --- Step 4, as setup.md gives it, with the CA where this machine keeps it. -------
# Step 4's command from setup.md, with the CA where this machine keeps it.
declare_step4() {
    printf 'cd %s\n' "$bundle"
    fence "$md" 4 sh | sed "s|/etc/ch/ca.crt|$ca|g"
}
step4() { sh -c "$(declare_step4)"; }
out=$(step4) || fail "step 4: $out"
printf '%s\n' "$out"
expected 4 | while IFS= read -r line; do
    printf '%s\n' "$out" | grep -qxF "$line" || fail "step 4 did not print: $line"
done
check V3
check V4

# --- E5: V0 after step 4 finds the database, so the steps say stop. ---------------
got=$(ask "$(sql V0)")
[ "$got" = 4 ] || fail "E5: V0 found '$got'"
echo "setup-cluster: E5 V0 after step 4 finds 4"

# Whether the server at `$1` reaches Keeper: a session lost with a stopped
# Keeper is re-made on another, and until then a request fails.
reaches_keeper() {
    n=$(curl -sS --fail-with-body --cacert "$ca" -u "pulsus:$PULSUS_CLICKHOUSE_PASSWORD" \
        "https://$1:8443/" --data-binary "SELECT count() FROM system.zookeeper WHERE path = '/'" \
        2>/dev/null) && [ "$n" -gt 0 ]
}
every_server_reaches_keeper() {
    for ip in $hosts; do
        retry 60 reaches_keeper "$ip" || fail "$ip does not reach Keeper"
    done
}

# --- E4: one Keeper down, step 4 succeeds; two down, it fails before dropping. ----
# A Keeper is taken down by pausing its container: the process stops
# answering, as a stopped one does, and comes back without a new network
# set-up, which rootless podman cannot always make from inside its namespace.
$RT pause "${P}kp3" >/dev/null
every_server_reaches_keeper
out=$(step4) || fail "E4: step 4 with one Keeper stopped: $out"
echo "setup-cluster: E4 step 4 succeeds with one Keeper down"
$RT pause "${P}kp2" >/dev/null
# Without a Keeper majority the drop, step 4's first statement, does not
# return: it waits on Keeper. The run is stopped after 120 s; nothing past
# the drop may have run, and the database must still be on every host.
# Output goes to a file: `timeout` stops the shell, and the request it was
# waiting on may outlive it.
(export PULSUS_SCHEMA_WAIT_SECONDS=30; timeout 120 sh -c "$(declare_step4)") \
    > "$work/e4.log" 2>&1 && fail "E4: step 4 with two Keepers stopped succeeded"
out=$(cat "$work/e4.log")
printf '%s\n' "$out" | grep -q "statements applied" &&
    fail "E4: step 4 with two Keepers stopped got past the drop: $out"
got=$(ask "$(sql V0)")
[ "$got" = 4 ] || fail "E4: the database was dropped: V0 found '$got'"
echo "setup-cluster: E4 step 4 stops at the drop with two Keepers down; the database stays"
$RT unpause "${P}kp2" >/dev/null
$RT unpause "${P}kp3" >/dev/null
for ip in $keepers; do
    retry 60 keeper_up "$ip" || fail "Keeper $ip did not come back"
done
every_server_reaches_keeper
# The stopped run's drop may still be waiting on host 1; it is let finish
# before the rebuild, so it cannot drop the rebuilt database.
no_drop() {
    [ "$(ask "SELECT count() FROM system.processes WHERE query LIKE 'DROP DATABASE%' AND query NOT LIKE '%system.processes%'")" = 0 ]
}
retry 300 no_drop || fail "the stopped run's drop did not finish"
# Step 4 again, as §8 says after a run that stopped part way.
out=$(step4) || fail "step 4 after Keeper came back: $out"
check V3
check V4

# --- Step 5: PulsusDB with pulsusdb.env, the CA where this machine keeps it. -------
set -a
# shellcheck disable=SC1091
. "$bundle/pulsusdb.env"
set +a
export CLICKHOUSE_TLS_CA_FILE="$ca"
export CLICKHOUSE_AUTH="pulsus:$PULSUS_CLICKHOUSE_PASSWORD"
export PULSUS_ADMIN_USER=admin PULSUS_ADMIN_PASSWORD=admin-password
export PULSUS_HOST=127.0.0.1 PULSUS_PORT=$PORT
"$target/debug/pulsusdb" > "$work/pulsusdb.log" 2>&1 &
pulsus_pid=$!
url=http://127.0.0.1:$PORT
ready() { curl -sS --fail-with-body "$url/ready" >/dev/null 2>&1; }
retry 120 ready || {
    cat "$work/pulsusdb.log" >&2
    fail "step 5: /ready did not answer 200"
}
echo "setup-cluster: step 5 /ready is 200"

# --- V5, the block between its markers. -------------------------------------------
awk '/^# V5 begin$/ { on = 1 } on { print } /^# V5 end$/ { exit }' "$md" > "$work/v5.sh"
PULSUS_URL=$url sh "$work/v5.sh" || fail "V5"

# --- E3: one sample through remote write, read back. -------------------------------
python3 - "$work/sample.bin" <<'EOF'
import struct, sys, time

def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)

def field(num, wire, payload):
    return varint(num << 3 | wire) + payload

def length(num, data):
    return field(num, 2, varint(len(data)) + data)

def label(name, value):
    return length(1, name.encode()) + length(2, value.encode())

sample = field(1, 1, struct.pack("<d", 42.0)) + field(2, 0, varint(int(time.time() * 1000)))
series = length(1, label("__name__", "setup_e3_up")) + length(1, label("job", "setup")) + length(2, sample)
body = length(1, series)
# Snappy: the length, then one literal of the whole body.
n = len(body) - 1
literal = bytes([n << 2]) if n < 60 else bytes([61 << 2]) + struct.pack("<H", n)
open(sys.argv[1], "wb").write(varint(len(body)) + literal + body)
EOF
curl -sS --fail-with-body -H 'Content-Type: application/x-protobuf' \
    -H 'Content-Encoding: snappy' -H 'X-Prometheus-Remote-Write-Version: 0.1.0' \
    --data-binary "@$work/sample.bin" "$url/api/v1/write" -o /dev/null
read_back() {
    curl -sS --fail-with-body "$url/api/v1/query" --data-urlencode 'query=setup_e3_up' |
        grep -q '"42"'
}
# Raw samples reach the table a query reads through a view refreshed every
# minute, so the read waits for it.
retry 240 read_back || {
    curl -sS "$url/api/v1/query" --data-urlencode 'query=setup_e3_up' >&2 || true
    tail -20 "$work/pulsusdb.log" >&2
    fail "E3: the sample did not read back"
}
echo "setup-cluster: E3 the sample written reads back"

# --- E6: no plaintext listener; Keeper answers ruok only over TLS. ------------------
refused() {
    python3 - "$1" "$2" <<'EOF'
import socket, sys
try:
    socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=5).close()
except ConnectionRefusedError:
    sys.exit(0)
sys.exit(1)
EOF
}
for ip in $hosts; do
    for port in 8123 9000 9009 9004 9005; do
        refused "$ip" "$port" || fail "E6: $ip:$port accepts a connection"
    done
done
ruok_plain() {
    python3 - "$1" <<'EOF'
import socket, sys
s = socket.create_connection((sys.argv[1], 9181), timeout=5)
s.sendall(b"ruok")
s.settimeout(5)
try:
    print(s.recv(16).decode(errors="replace"))
except OSError:
    print("")
EOF
}
for ip in $keepers; do
    [ "$(ruok_plain "$ip")" != imok ] || fail "E6: Keeper $ip answers ruok in plaintext"
    [ "$(ruok_tls "$ip")" = imok ] || fail "E6: Keeper $ip does not answer ruok over TLS"
done
echo "setup-cluster: E6 every plaintext port is closed; Keeper answers only over TLS"

echo "setup-cluster: every step and check passed"
