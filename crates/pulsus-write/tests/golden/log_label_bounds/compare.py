#!/usr/bin/env python3
"""Side-by-side accept-surface comparison of PulsusDB and grafana/loki 3.7.4
for issue #374's per-stream label rules.

Sends byte-identical bodies to both and records (status, body) for each.
Loki: 127.0.0.1:13174.  PulsusDB: 127.0.0.1:18374.
See oracle_probe.txt in this directory for the recorded run and the recipe.
"""
import http.client
import json
import sys
import time

LOKI = ("127.0.0.1", 13174)
PULSUS = ("127.0.0.1", 18374)
TS = str(int(time.time() * 1e9))


# --- snappy block format, literal-only (valid, no back-references) ---------
def snappy_raw(data: bytes) -> bytes:
    out = bytearray()
    v = len(data)
    while True:
        b = v & 0x7F
        v >>= 7
        out.append(b | (0x80 if v else 0))
        if not v:
            break
    ln = len(data) - 1
    if len(data) <= 60:
        out.append(ln << 2)
    elif ln < 256:
        out.append(60 << 2)
        out.append(ln)
    elif ln < 65536:
        out.append(61 << 2)
        out += ln.to_bytes(2, "little")
    elif ln < (1 << 24):
        out.append(62 << 2)
        out += ln.to_bytes(3, "little")
    else:
        out.append(63 << 2)
        out += ln.to_bytes(4, "little")
    out += data
    return bytes(out)


# --- minimal protobuf writer ----------------------------------------------
def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            break
    return bytes(out)


def tag(field, wire):
    return varint((field << 3) | wire)


def bytes_field(field, data):
    return tag(field, 2) + varint(len(data)) + data


def push_request(streams):
    """streams: list of (labels_literal, [(ts_ns, line)])"""
    body = b""
    for labels, entries in streams:
        s = bytes_field(1, labels.encode())
        for ts_ns, line in entries:
            ts = tag(1, 0) + varint(ts_ns // 10**9) + tag(2, 0) + varint(ts_ns % 10**9)
            e = bytes_field(1, ts) + bytes_field(2, line.encode())
            s += bytes_field(2, e)
        body += bytes_field(1, s)
    return snappy_raw(body)


def post(target, path, body, ctype):
    conn = http.client.HTTPConnection(*target, timeout=30)
    headers = {"Content-Type": ctype}
    conn.request("POST", path, body=body, headers=headers)
    r = conn.getresponse()
    data = r.read()
    conn.close()
    return r.status, data.decode("utf-8", "replace")


def loki_json(streams):
    return json.dumps({"streams": streams}, separators=(",", ":")).encode()


def stream(labels, line="hi"):
    return {"stream": labels, "values": [[TS, line]]}


# --- OTLP/JSON ------------------------------------------------------------
def otlp_json(resources):
    """resources: list of dict-of-attrs, or of lists of (k, value-object)
    pairs when a case needs duplicate keys or a non-string attribute value."""
    def kvs(attrs):
        pairs = attrs.items() if isinstance(attrs, dict) else attrs
        return [
            {"key": k, "value": v if isinstance(v, dict) else {"stringValue": v}}
            for k, v in pairs
        ]

    return json.dumps(
        {
            "resourceLogs": [
                {
                    "resource": {"attributes": kvs(attrs)},
                    "scopeLogs": [
                        {
                            "logRecords": [
                                {
                                    "timeUnixNano": TS,
                                    "body": {"stringValue": "hi"},
                                }
                            ]
                        }
                    ],
                }
                for attrs in resources
            ]
        },
        separators=(",", ":"),
    ).encode()


CASES = []


def case(name, transport, body, ctype, expect="SAME"):
    """expect="SAME": statuses must agree.  expect="DIFF": a KNOWN divergence,
    recorded so that it stays the one we recorded rather than silently
    changing.  Either way the run fails only on an UNEXPECTED verdict."""
    CASES.append((name, transport, body, ctype, expect))


A = "a" * 1024
A1 = "a" * 1025
B = "b" * 2048
B1 = "b" * 2049
Z = "z" * 2000
C = "c" * 3000

n15 = {f"l{i}": "v" for i in range(15)}
n16 = {f"l{i}": "v" for i in range(16)}

# ---- Loki push, JSON -----------------------------------------------------
case("json/15-labels", "json", loki_json([stream(dict(n15))]), "application/json")
case("json/16-labels", "json", loki_json([stream(dict(n16))]), "application/json")
case("json/15+service_name", "json",
     loki_json([stream({**n15, "service_name": "checkout"})]), "application/json")
case("json/16+service_name", "json",
     loki_json([stream({**n16, "service_name": "checkout"})]), "application/json")
case("json/15+one-empty", "json",
     loki_json([stream({**n15, "extra": ""})]), "application/json")
case("json/name-1024", "json", loki_json([stream({A: "v"})]), "application/json")
case("json/name-1025", "json", loki_json([stream({A1: "v"})]), "application/json")
case("json/value-2048", "json", loki_json([stream({"app": B})]), "application/json")
case("json/value-2049", "json", loki_json([stream({"app": B1})]), "application/json")
case("json/empty-valued-2000B-name", "json",
     loki_json([stream({Z: ""})]), "application/json")
case("json/count-outranks-length", "json",
     loki_json([stream({**n16, Z: C})]), "application/json")
case("json/name-outranks-value-same-label", "json",
     loki_json([stream({Z: C})]), "application/json")
case("json/sorted-order-picks-the-label", "json",
     loki_json([stream({"aaa": C, Z: "v"})]), "application/json")
case("json/duplicate-key-collapses", "json",
     b'{"streams":[{"stream":{"foo":"bar","foo":"barf"},"values":[["' + TS.encode() + b'","hi"]]}]}',
     "application/json")
# review case: 16 non-empty + 241 empty = 257 raw pairs
case("json/16-nonempty+241-empty", "json",
     loki_json([stream({**n16, **{f"e{i}": "" for i in range(241)}})]), "application/json")
case("json/15-nonempty+250-empty", "json",
     loki_json([stream({**n15, **{f"e{i}": "" for i in range(250)}})]), "application/json")
# The two PulsusDB-only structural caps the reference does not have, both
# directions (ledger residual 3).  257 non-empty labels: 400 on both, but our
# wording is the decode cap's, not `has N label names; limit 15`.  65,537 raw
# JSON keys of which none are non-empty: accepted upstream (WithoutEmpty drops
# them all), refused here by MAX_RAW_LABEL_PAIRS_PER_STREAM.
case("json/257-nonempty", "json",
     loki_json([stream({f"n{i}": "v" for i in range(257)})]), "application/json")
case("json/65537-raw-empty-keys", "json",
     loki_json([stream({f"e{i}": "" for i in range(65537)})]), "application/json",
     expect="DIFF")

case("json/internal-aggregated-metric-16-labels", "json",
     loki_json([stream({**n16, "__aggregated_metric__": "x"})]), "application/json")
case("json/internal-pattern-over-long-value", "json",
     loki_json([stream({"__pattern__": "x", "app": C})]), "application/json")
case("json/mixed-good-and-bad", "json",
     loki_json([stream({"app": "good"}, "good-line"), stream({"app": B1}, "bad-line")]),
     "application/json")
case("json/two-distinct-failures", "json",
     loki_json([stream({"app": B1}, "l1"), stream({A1: "v"}, "l2")]), "application/json")
case("json/control-bytes-in-sibling-label", "json",
     loki_json([stream({"app": B1, "ctl": "xyz"})]), "application/json")
case("json/entry-less-stream-over-wide", "json",
     json.dumps({"streams": [{"stream": {"app": C}, "values": []}]},
                separators=(",", ":")).encode(), "application/json")

# ---- Loki push, protobuf -------------------------------------------------
def pb(labels, line="hi"):
    return push_request([(labels, [(int(TS), line)])])


case("pb/duplicate-distinct", "pb", pb('{foo="bar", foo="barf"}'), "application/x-protobuf")
case("pb/257-nonempty", "pb",
     pb("{" + ", ".join(f'n{i}="v"' for i in range(257)) + "}"),
     "application/x-protobuf")
case("pb/duplicate-identical", "pb", pb('{foo="bar", foo="bar"}'), "application/x-protobuf")
case("pb/value-2049", "pb", pb('{app="%s"}' % B1), "application/x-protobuf")
case("pb/16-labels", "pb",
     pb("{" + ", ".join(f'l{i}="v"' for i in range(16)) + "}"), "application/x-protobuf")
case("pb/count-outranks-duplicate", "pb",
     pb("{" + ", ".join(f'l{i}="v"' for i in range(16)) + ', l0="again"}'),
     "application/x-protobuf")
case("pb/value-outranks-later-duplicate", "pb",
     pb('{aaa="%s", zzz="1", zzz="2"}' % C), "application/x-protobuf")
case("pb/earlier-duplicate-outranks-value", "pb",
     pb('{aaa="1", aaa="2", zzz="%s"}' % C), "application/x-protobuf")
case("pb/repeat-with-empty-copy-is-not-duplicate", "pb",
     pb('{foo="bar", foo=""}'), "application/x-protobuf")
# ...and in the other order.  The rule that applies to STREAM labels drops the
# empty pair (ls.WithoutEmpty(), parser.go:279-296 @ v3.7.4) and keeps the
# surviving twin; the delete-by-name rule a labels.Builder applies to
# STRUCTURED METADATA (distributor.go:698-722, issue #259) would lose the twin
# too, so this input is exactly where the two rules disagree.
case("pb/empty-copy-first-is-not-duplicate", "pb",
     pb('{d="", d="keep"}'), "application/x-protobuf")
case("pb/distinct-duplicate-is-still-a-duplicate", "pb",
     pb('{d="one", d="two"}'), "application/x-protobuf")
case("pb/16-nonempty+241-empty", "pb",
     pb("{" + ", ".join(f'l{i}="v"' for i in range(16)) + ", "
        + ", ".join(f'e{i}=""' for i in range(241)) + "}"), "application/x-protobuf")
case("pb/internal-pattern-16-labels", "pb",
     pb("{" + ", ".join(f'l{i}="v"' for i in range(16)) + ', __pattern__="x"}'),
     "application/x-protobuf")

# ---- OTLP logs -----------------------------------------------------------
# Only the 18 names in `default_resource_attributes_as_index_labels` become
# stream labels upstream (otlp_config.go:56-73 @ v3.7.4); everything else is
# structured metadata and never reaches ValidateLabels. Both halves are here.
# The selection is made on the RAW wire key: otlp.go:193 calls
# ActionForResourceAttribute(k) and only then attributeToLabels(k, ...)
# canonicalizes, and the match inside actionForAttribute is `cfgAttr ==
# attribute`, exact string equality (otlp_config.go:88-99 @ v3.7.4).  So the
# two directions below are generated from the reference's OWN list rather than
# sampled: the dotted spelling is an index label and is bounded, the same name
# already spelled with underscores is structured metadata and is not.  The
# earlier 44-case set sampled only (a) exact dotted names and (b) obviously
# arbitrary names, so it could not distinguish the raw rule from a
# canonicalized one -- which is how a canonicalized selection passed 44/44.
ALL_IDX = ["service.name"] + [
       "service.namespace", "service.instance.id", "deployment.environment",
       "deployment.environment.name", "cloud.region", "cloud.availability_zone",
       "k8s.cluster.name", "k8s.namespace.name", "k8s.pod.name",
       "k8s.container.name", "container.name", "k8s.replicaset.name",
       "k8s.deployment.name", "k8s.statefulset.name", "k8s.daemonset.name",
       "k8s.cronjob.name", "k8s.job.name"]
IDX = ALL_IDX[1:]
idx = lambda n: {k: "v" for k in IDX[:n]}

case("otlp/indexed-value-2049", "otlp", otlp_json([{"k8s.pod.name": B1}]), "application/json")
case("otlp/indexed-value-2048", "otlp", otlp_json([{"k8s.pod.name": B}]), "application/json")
case("otlp/16-indexed", "otlp", otlp_json([idx(16)]), "application/json")
case("otlp/15-indexed", "otlp", otlp_json([idx(15)]), "application/json")
case("otlp/15-indexed+service.name", "otlp",
     otlp_json([{**idx(15), "service.name": "checkout"}]), "application/json")
case("otlp/15-indexed+40-non-indexed", "otlp",
     otlp_json([{**idx(15), **{f"extra{i}": "v" for i in range(40)}}]), "application/json")
case("otlp/non-indexed-value-2049", "otlp", otlp_json([{"app": B1}]), "application/json")
case("otlp/non-indexed-name-1025", "otlp", otlp_json([{A1: "v"}]), "application/json")
case("otlp/16-non-indexed", "otlp", otlp_json([dict(n16)]), "application/json")
case("otlp/15-indexed+one-empty-indexed", "otlp",
     otlp_json([{**idx(15), "k8s.job.name": ""}]), "application/json")
case("otlp/16-indexed+__pattern__", "otlp",
     otlp_json([{**idx(16), "__pattern__": "x"}]), "application/json")
case("otlp/mixed-good-and-bad-indexed", "otlp",
     otlp_json([{"k8s.pod.name": "good"}, {"k8s.pod.name": B1}]), "application/json")

# -- raw vs canonical spelling, over the reference's whole list -------------
for _k in ALL_IDX:
    case(f"otlp/raw/{_k}/value-2049", "otlp", otlp_json([{_k: B1}]), "application/json")
    case(f"otlp/canonical/{_k.replace('.', '_')}/value-2049", "otlp",
         otlp_json([{_k.replace(".", "_"): B1}]), "application/json")

# Other separators that canonicalize onto an index label here and are
# structured metadata upstream.
for _k in ("service-name", "service name", "service/name", "cloud-region"):
    case(f"otlp/near-miss/{_k}/value-2049", "otlp", otlp_json([{_k: B1}]),
         "application/json")

# 17 underscored look-alikes: not index labels, so not counted.
case("otlp/17-canonical-spellings", "otlp",
     otlp_json([{k.replace(".", "_"): "v" for k in ALL_IDX}]), "application/json")

# The cross-transport twin of the defect: the SAME label name is bounded on
# /loki/api/v1/push (where it is a literal label) and unbounded on the OTLP
# receiver (where it is a raw attribute name that is not one of the 18).
# Neither transport's cases alone can see the two rules being swapped.
case("json/service_name-value-2049", "json",
     loki_json([stream({"service_name": B1})]), "application/json")

# A REPEATED wire key on an index attribute.  Upstream's streamLabels is a
# map, so the last write wins (otlp.go:191-193 @ v3.7.4) and the bound is
# charged on whichever value came last; ours collapses through
# LabelSet::from_normalized, whose resolution is issue #4's frozen
# greatest-(key, value) rule, so the bound is charged on the value we would
# actually store.  Recorded rather than matched: matching upstream's choice
# here means validating a value we do not store, which is the defect round 1
# of this issue was about.  Pre-existing -- the previous implementation
# validated the same collapsed LabelSet.
case("otlp/duplicate-index-key/bad-last", "otlp",
     otlp_json([[("k8s.pod.name", "ok"), ("k8s.pod.name", B1)]]), "application/json",
     expect="DIFF")
case("otlp/duplicate-index-key/bad-first", "otlp",
     otlp_json([[("k8s.pod.name", B1), ("k8s.pod.name", "ok")]]), "application/json")
# ...whereas an index name colliding with its own near-miss spelling is only a
# collision HERE (upstream indexes the dotted one and makes the other
# structured metadata), and agrees in both orders.
case("otlp/index-key-vs-near-miss/bad-on-near-miss", "otlp",
     otlp_json([[("k8s.pod.name", "ok"), ("k8s_pod_name", B1)]]), "application/json")
case("otlp/index-key-vs-near-miss/bad-on-index", "otlp",
     otlp_json([[("k8s.pod.name", B1), ("k8s_pod_name", "ok")]]), "application/json")

# Attribute value types other than string.  A MAP value under an index key
# fans out into several labels upstream, prefixed with the parent's
# canonicalized name (otlp.go:602-640 @ v3.7.4), so a long nested key becomes a
# long LABEL NAME there; we render the map to one JSON-valued label (#109).
case("otlp/index-key-map-value-long-nested-key", "otlp",
     otlp_json([[("service.name", {"kvlistValue": {"values": [
         {"key": A1, "value": {"stringValue": "v"}}]}})]]),
     "application/json", expect="DIFF")
case("otlp/index-key-map-value-long-nested-value", "otlp",
     otlp_json([[("service.name", {"kvlistValue": {"values": [
         {"key": "inner", "value": {"stringValue": B1}}]}})]]),
     "application/json")
case("otlp/index-key-int-value", "otlp",
     otlp_json([[("k8s.pod.name", {"intValue": "42"})]]), "application/json")
case("otlp/index-key-array-value-long", "otlp",
     otlp_json([[("k8s.pod.name", {"arrayValue": {"values": [
         {"stringValue": B1}]}})]]), "application/json")

PATHS = {
    "json": ("/loki/api/v1/push", "/loki/api/v1/push"),
    "pb": ("/loki/api/v1/push", "/loki/api/v1/push"),
    "otlp": ("/otlp/v1/logs", "/v1/logs"),
}


def trim(s, n=100000):
    s = s.replace("\n", "\\n")
    return s if len(s) <= n else s[:n] + f"…[{len(s)} bytes]"


unexpected = []
rows = []
for name, transport, body, ctype, expect in CASES:
    lpath, ppath = PATHS[transport]
    ls, lb = post(LOKI, lpath, body, ctype)
    ps, pb_ = post(PULSUS, ppath, body, ctype)
    # 200 and 204 are both "accepted": Loki answers 204 on /otlp/v1/logs,
    # PulsusDB answers the OTLP spec's 200 on /v1/logs.
    same = (ls in (200, 204)) == (ps in (200, 204)) and (ls == ps or {ls, ps} <= {200, 204})
    verdict = "SAME" if same else "DIFF"
    if verdict != expect:
        unexpected.append(name)
    rows.append((name, ls, ps, verdict, expect, trim(lb), trim(pb_)))

w = max(len(r[0]) for r in rows)
print(f"{'case'.ljust(w)}  loki  pulsus  verdict  expected")
for name, ls, ps, v, expect, lb, pbo in rows:
    flag = "" if v == expect else "   <-- UNEXPECTED"
    print(f"{name.ljust(w)}  {ls:<4}  {ps:<6}  {v:<7}  {expect}{flag}")
print()
agree = sum(1 for r in rows if r[3] == "SAME")
known = [r[0] for r in rows if r[4] == "DIFF"]
print(f"status agreement: {agree}/{len(rows)} "
      f"({len(known)} recorded divergence(s): {known})")
print(f"unexpected verdicts: {unexpected}")
print()
print("=" * 100)
print("bodies")
print("=" * 100)
for name, ls, ps, v, expect, lb, pbo in rows:
    print(f"--- {name} [{v}]")
    print(f"    loki   {ls}: {lb}")
    print(f"    pulsus {ps}: {pbo}")
sys.exit(1 if unexpected else 0)
