#!/usr/bin/env python3
"""Side-by-side accept-surface comparison of PulsusDB and grafana/loki 3.7.4
for issue #374's per-stream label rules.

Sends byte-identical bodies to both and records (status, body) for each.
Loki: 127.0.0.1:13174.  PulsusDB: 127.0.0.1:18374 (override with LOKI_PORT /
PULSUS_PORT when another agent holds those ports).
See oracle_probe.txt in this directory for the recorded run and the recipe.

Stdout is the case table, the summary line and the body dump, terminated by
GENERATED_END -- that span of oracle_probe.txt and no more.  The preamble and
the recorded-divergences notes in that file are hand-maintained prose; this
script neither writes nor checks them, so regenerate by replacing the span
rather than redirecting over the whole file.

Needs a Loki source checkout containing the pinned revision, because the
eighteen index-label names are read out of it rather than transcribed here
(LOKI_SRC, default /home/hayato/git/loki). Exits non-zero if it cannot.
"""
import http.client
import json
import os
import re
import subprocess
import sys
import time

LOKI = ("127.0.0.1", int(os.environ.get("LOKI_PORT", "13174")))
PULSUS = ("127.0.0.1", int(os.environ.get("PULSUS_PORT", "18374")))
TS = str(int(time.time() * 1e9))

# The oracle container's revision, from /loki/api/v1/status/buildinfo.
LOKI_SRC = os.environ.get("LOKI_SRC", "/home/hayato/git/loki")
LOKI_PIN = "b318f2829f0ae2094ab3a1e90780450e9e4b03be"  # v3.7.4


def reference_index_labels():
    """The `default_resource_attributes_as_index_labels` defaults, READ from
    the pinned reference (`pkg/loghttp/push/otlp_config.go:56-75 @ v3.7.4`),
    not transcribed.

    `git show <pin>:<path>` reads the blob at that revision, so the answer does
    not depend on what the checkout happens to have checked out. Failure to
    read it is fatal: a silent fallback to a copied list is exactly the defect
    the generated cases exist to rule out — a case set drawn from our own model
    of the rule cannot distinguish our model from the reference's.

    Drift against PulsusDB's own `OTLP_INDEX_ATTRIBUTES` is caught
    behaviourally rather than textually: every name here gets a raw case
    expecting `400` on both sides and a canonicalized case expecting `204`, so
    a name we index and the reference does not (or the reverse) lands as an
    UNEXPECTED verdict and exits non-zero.
    """
    path = "pkg/loghttp/push/otlp_config.go"
    proc = subprocess.run(
        ["git", "-C", LOKI_SRC, "show", f"{LOKI_PIN}:{path}"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        sys.exit(
            f"cannot read {path} at {LOKI_PIN[:8]} from {LOKI_SRC}: "
            f"{proc.stderr.strip()}\n"
            "Set LOKI_SRC to a Loki checkout containing that revision "
            "(v3.7.4). The list is deliberately not transcribed here."
        )
    m = re.search(
        r"cfg\.DefaultOTLPResourceAttributesAsIndexLabels\s*=\s*\[\]string\{(.*?)\n\t\}",
        proc.stdout,
        re.S,
    )
    if not m:
        sys.exit(
            f"{path}@{LOKI_PIN[:8]}: could not locate the "
            "DefaultOTLPResourceAttributesAsIndexLabels literal — the "
            "reference moved it, so the generated cases would be silently wrong"
        )
    names = re.findall(r'"([^"]+)"', m.group(1))
    if not names:
        sys.exit(f"{path}@{LOKI_PIN[:8]}: index-label literal parsed empty")
    return names


# --- snappy block format, literal-only (valid, no back-references) ---------
def snappy_raw(data: bytes) -> bytes:
    if not data:
        # A zero-length block is just the varint length, with no literal to
        # tag — the encoding the empty-`PushRequest` case below needs.
        return b"\x00"
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

# A push carrying NO streams is refused before any validation runs:
# `PushWithResolver` returns 422 MissingStreamsErrorMsg for len(req.Streams)
# == 0 (distributor.go:579-581 @ v3.7.4).  Both JSON spellings, plus the
# empty protobuf message below.  Their discriminating neighbour is
# json/entry-less-stream-over-wide above: a stream with labels and no entries
# IS a stream and stays accepted.
case("json/no-streams", "json", b'{"streams":[]}', "application/json")
case("json/no-streams-key", "json", b"{}", "application/json")
# A JSON object carrying an UNRELATED key -- #259 measured this body and read
# it as a third case.  It is not: both sides ignore the unknown key, find no
# streams and answer the same 422.  Pinned because a stricter JSON decode
# (400 on an unknown field) would silently break that agreement.
case("json/no-streams-unrelated-key", "json", b'{"nope":1}', "application/json")

# The envelope's `streams` key is matched with ASCII case folding, and a
# repeat of it is last-wins.  `loghttp.PushRequest` is a one-field struct
# decoded by jsoniter reflection (`pkg/loghttp/query.go:91-93 @ v3.7.4`) and
# `jsoniter.NewDecoder` runs `ConfigDefault`, whose `CaseSensitive` is false:
# the wire key is folded over 'A'..'Z' before it is hashed, the field decoder
# re-runs on every match and the slice decoder re-grows from index zero
# (`iter_object.go:85-87`, `reflect_struct_decoder.go:36-41,574-590`,
# `reflect_slice.go:66-99 @ jsoniter v1.1.12`, vendored in the Loki tree).
#
# These cases measure the WIRE only.  Status agreement is exactly what missed
# this before: both sides answered 204 while PulsusDB silently dropped the
# lines.  The storage half is asserted by
# `loki_push_live::a_case_variant_streams_key_is_accepted_and_its_lines_are_stored`
# and by `loki_push::tests::parse_json_matches_the_streams_key_case_insensitively`.
def envelope(key, label="a", line="hi"):
    return ('{"' + key + '":[{"stream":{"app":"' + label + '"},'
            '"values":[["' + TS + '","' + line + '"]]}]}').encode()


for _spelling in ("Streams", "STREAMS", "StReAmS", "streamS"):
    case(f"json/streams-key-{_spelling}", "json", envelope(_spelling), "application/json")
# The key on the wire is not the bytes `Streams`; both decoders unescape
# before folding.
case("json/streams-key-escaped-capital", "json", envelope(r"\u0053treams"),
     "application/json")
# Anchored and ASCII-only: a key that merely contains or extends the field
# name is unknown, so the request is stream-less on both sides.
for _name, _spelling in (("trailing-space", "streams "),
                         ("leading-space", " streams"),
                         ("extra-s", "streamss"),
                         ("prefixed", "xstreams")):
    case(f"json/streams-key-near-miss-{_name}", "json",
         envelope(_spelling), "application/json")
# Last-wins across spellings, and the last occurrence decides whether the
# request is stream-less at all.  `null` overwrites with nil
# (`reflect_slice.go:69-72`), so it empties the request like `[]` does.
case("json/streams-key-repeated", "json",
     b'{"streams":[{"stream":{"app":"a"},"values":[["' + TS.encode() + b'","first"]]}],'
     b'"Streams":[{"stream":{"app":"b"},"values":[["' + TS.encode() + b'","second"]]}]}',
     "application/json")
case("json/streams-key-repeated-trailing-empty", "json",
     b'{"Streams":[{"stream":{"app":"a"},"values":[["' + TS.encode() + b'","first"]]}],'
     b'"streams":[]}', "application/json")
case("json/streams-key-null", "json", b'{"streams":null}', "application/json")
case("json/streams-key-null-then-populated", "json",
     b'{"streams":null,"Streams":[{"stream":{"app":"a"},"values":[["'
     + TS.encode() + b'","second"]]}]}', "application/json")
# The fold is one field's, not the decoder's: a stream object is decoded by a
# hand-written `LogProtoStream.UnmarshalJSON` switching on `string(key)`
# (`query.go:99-121`), so `Stream`/`Values` are unknown keys on both sides --
# 204 with nothing stored, which is why the live test reads the rows back.
case("json/stream-object-keys-are-case-sensitive", "json",
     b'{"streams":[{"Stream":{"app":"a"},"Values":[["' + TS.encode() + b'","hi"]]}]}',
     "application/json")
# ...but a repeat of those keys is last-wins there too, except that a `null`
# `values` returns before assigning and leaves the entries alone
# (`query.go:110-112`).
case("json/stream-object-keys-repeated", "json",
     b'{"streams":[{"stream":{"app":"first"},"stream":{"app":"second"},"values":[["'
     + TS.encode() + b'","hi"]]}]}', "application/json")
case("json/values-null-after-entries", "json",
     b'{"streams":[{"stream":{"app":"a"},"values":[["' + TS.encode()
     + b'","kept"]],"values":null}]}', "application/json")
case("json/values-null-alone", "json",
     b'{"streams":[{"stream":{"app":"a"},"values":null}]}', "application/json")
# The fold does not skip the per-stream bounds: the over-wide value under a
# case variant is the same 400 it is under `streams`.
case("json/case-variant-value-2049", "json",
     ('{"Streams":[{"stream":{"app":"' + B1 + '"},"values":[["' + TS
      + '","x"]]}]}').encode(), "application/json")

# ---- last-wins resolves BEFORE any structural cap is charged --------------
# Round 11.  The reference never inspects a superseded value, so a cap-breaking
# first occurrence followed by a valid last one is 204 there and the last one's
# line is stored.  Charging our decode-time caps where they trip answered 400.
#
# This is the ENUMERATION, not a sample: three resolution points (the envelope
# `streams` key, a stream object's `stream` key, its `values` key) crossed with
# every per-occurrence cap reachable inside each.
#
# `-survives` is the discriminating neighbour -- the same over-cap value placed
# LAST, which must still be refused, so that deleting a cap outright cannot pass
# the superseded half.  It is only here for the caps whose surviving answer
# upstream is about labels: with 100,001 entries or 1,000,001 streams surviving,
# Loki answers 500 from its ingester's 4 MiB gRPC message limit and PulsusDB
# answers 400, which says nothing about the cap.  Those two neighbours are
# asserted precisely, on the error itself, by
# `loki_push::tests::parse_json_a_superseded_over_cap_value_does_not_decide_the_request`
# (every row of this enumeration, both directions) and, on stored state, by
# `loki_push_live::a_superseded_over_cap_value_is_accepted_and_the_final_one_is_stored`.
#
# The two SHARED counters are deliberately absent: MAX_TOTAL_ENTRIES_PER_REQUEST
# and MAX_DECODED_BYTES measure what the whole request has already materialized
# and stay immediately fatal, so a superseded occurrence past them IS a
# divergence -- `json/superseded-shared-budget` below, expect="DIFF".
_final = '[["' + TS + '","FINAL"]]'
_win = '{"stream":{"app":"w"},"values":' + _final + '}'
_over_values = "[" + ",".join('["' + TS + '","x"]' for _ in range(100_001)) + "]"
_over_labels = "{" + ",".join('"k%d":"v"' % i for i in range(257)) + "}"
_over_raw = "{" + ",".join('"dup":"v"' for _ in range(65_537)) + "}"
_over_sm = '[["' + TS + '","x",{' + ",".join('"m%d":"v"' % i for i in range(257)) + "}]]"
_over_streams = "[" + ",".join("{}" for _ in range(1_000_001)) + "]"


def superseded(name, body, survives=None):
    case("json/superseded-" + name, "json", body.encode(), "application/json")
    if survives:
        case("json/superseded-" + name + "-survives", "json", survives.encode(),
             "application/json")


superseded("streams-entries",
           '{"streams":[{"stream":{"app":"s"},"values":%s}],"StReAmS":[%s]}'
           % (_over_values, _win))
superseded("streams-label-count",
           '{"streams":[{"stream":%s,"values":%s}],"Streams":[%s]}'
           % (_over_labels, _final, _win),
           '{"Streams":[%s],"streams":[{"stream":%s,"values":%s}]}'
           % (_win, _over_labels, _final))
# No `-survives` twin: 65,537 raw pairs LAST is the ledgered raw-pair
# divergence, measured on its own as json/duplicate-key-65537-raw below.
superseded("streams-raw-pairs",
           '{"streams":[{"stream":%s,"values":%s}],"Streams":[%s]}'
           % (_over_raw, _final, _win))
superseded("streams-structured-metadata",
           '{"streams":[{"stream":{"app":"s"},"values":%s}],"Streams":[%s]}' % (_over_sm, _win),
           '{"Streams":[%s],"streams":[{"stream":{"app":"s"},"values":%s}]}' % (_win, _over_sm))
superseded("streams-stream-count",
           '{"streams":%s,"Streams":[%s]}' % (_over_streams, _win))
superseded("stream-label-count",
           '{"streams":[{"stream":%s,"stream":{"app":"w"},"values":%s}]}'
           % (_over_labels, _final),
           '{"streams":[{"stream":{"app":"w"},"stream":%s,"values":%s}]}'
           % (_over_labels, _final))
superseded("stream-raw-pairs",
           '{"streams":[{"stream":%s,"stream":{"app":"w"},"values":%s}]}'
           % (_over_raw, _final))
superseded("values-entries",
           '{"streams":[{"stream":{"app":"w"},"values":%s,"values":%s}]}'
           % (_over_values, _final))
superseded("values-structured-metadata",
           '{"streams":[{"stream":{"app":"w"},"values":%s,"values":%s}]}' % (_over_sm, _final),
           '{"streams":[{"stream":{"app":"w"},"values":%s,"values":%s}]}' % (_final, _over_sm))

# The fourth resolution point: duplicate keys collapse INSIDE one `stream` map
# (`LabelSet.UnmarshalJSON` assigns into a map[string]string,
# `pkg/loghttp/labels.go:25-40 @ v3.7.4`, and has no count bound of its own), so
# 257 repetitions of one key are ONE label upstream.
case("json/duplicate-key-257-collapses", "json",
     ('{"streams":[{"stream":{'
      + ",".join('"dup":"v%d"' % i for i in range(257))
      + ',"app":"w"},"values":' + _final + "}]}").encode(), "application/json")
# ...but the RAW-pair guard is not evaded by repeating a key, and that guard is
# the ledgered divergence (65,537 raw pairs, one label: 204 there, 400 here) --
# the same row as json/65537-raw-empty-keys, reached by the other spelling.
case("json/duplicate-key-65537-raw", "json",
     ('{"streams":[{"stream":{'
      + ",".join('"dup":"v"' for _ in range(65_537))
      + ',"app":"w"},"values":' + _final + "}]}").encode(), "application/json",
     expect="DIFF")
# The one residual in this family, measured rather than reasoned: 38 superseded
# streams of 100k minimal entries (34 MB, inside BOTH sides' body limits) puts
# the SHARED byte budget past 256 MiB before the surviving occurrence is even
# read.  Upstream's only bound on this path is the 100 MiB compressed body
# (`distributor.max-recv-msg-size`, distributor.go:124, applied by
# io.LimitReader in parsePushRequestBody, push.go:322-325 @ v3.7.4), so it
# answers 204 and stores FINAL.  A control at 30 streams agrees 204/204.
_bulk = ('{"stream":{"a":"b"},"values":['
         + ",".join('["1",""]' for _ in range(100_000)) + "]}")
case("json/superseded-shared-budget", "json",
     ('{"streams":[' + ",".join(_bulk for _ in range(38)) + '],"Streams":[' + _win + "]}").encode(),
     "application/json", expect="DIFF")
case("json/superseded-shared-budget-under", "json",
     ('{"streams":[' + ",".join(_bulk for _ in range(30)) + '],"Streams":[' + _win + "]}").encode(),
     "application/json")

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
case("pb/no-streams", "pb", push_request([]), "application/x-protobuf")
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
#
# The LIST itself is read out of the reference too (reference_index_labels()
# above), not copied: a generated set of pairs over a transcribed list is still
# a set drawn from our model of the rule.  ALL_IDX[0] is `service.name`, which
# the count bound discounts, so IDX is the rest.
ALL_IDX = reference_index_labels()
IDX = [k for k in ALL_IDX if k != "service.name"]
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

# No log records at all -> an EMPTY converted push request
# (`if ld.LogRecordCount() == 0`, otlp.go:144-146 @ v3.7.4) -> the same 422 the
# stream-less Loki-push cases get.  Every shape of "no records" is one case
# upstream because LogRecordCount sums over all (resource, scope) pairs.  On
# OUR side one bound outranks it -- the AnyValue depth cap, `*-over-deep-attr`
# below.
def otlp_records(resources):
    """resources: list of (attrs dict, record count)."""
    return json.dumps({"resourceLogs": [
        {"resource": {"attributes": [
            {"key": k, "value": {"stringValue": v}} for k, v in attrs.items()]},
         "scopeLogs": [{"logRecords": [
             {"timeUnixNano": TS, "body": {"stringValue": "hi"}}
             for _ in range(n)]}]}
        for attrs, n in resources]}, separators=(",", ":")).encode()


case("otlp/no-resource-logs", "otlp", b'{"resourceLogs":[]}', "application/json")
case("otlp/empty-request", "otlp", b"{}", "application/json")
case("otlp/no-log-records", "otlp",
     otlp_records([({"service.name": "probe"}, 0)]), "application/json")
case("otlp/no-scope-logs", "otlp",
     json.dumps({"resourceLogs": [{"resource": {"attributes": [
         {"key": "service.name", "value": {"stringValue": "probe"}}]},
         "scopeLogs": []}]}, separators=(",", ":")).encode(), "application/json")
# ...and the neighbour: one record anywhere in the request is enough, and the
# record-less resource beside it is not validated even though its label is far
# over the bound (distributor.go:639-641 skips an entry-less stream).
case("otlp/record-less-over-wide-resource+good", "otlp",
     otlp_records([({"k8s.pod.name": C}, 0), ({"service.name": "good"}, 1)]),
     "application/json")

# Ledger residual 6: PulsusDB caps AnyValue nesting at 32 (finding #54, a
# stack-safety guard) and charges it during DECODE on both transports, so it
# outranks the stream-less 422 above.  The reference has no depth cap -- the
# record-BEARING control is 204 there and 400 here -- so nothing upstream fixes
# the order between the two.  The at-cap neighbours are the discriminators:
# the same record-less shape one AnyValue node shallower -- exactly at the cap,
# the deepest tree still accepted -- 422 on both sides.  Measured edge, both
# transports: depth 32 -> 422/422, depth 33 -> 422/400.
MAX_ANYVALUE_DEPTH = 32  # crate::protocols::otlp_depth::MAX_ANYVALUE_DEPTH


def deep_json_value(depth):
    """An OTLP/JSON AnyValue tree `depth` AnyValue nodes deep, the leaf
    included -- the SAME unit as MAX_ANYVALUE_DEPTH and as the reject
    message's `depth count N`, so `depth == MAX_ANYVALUE_DEPTH` is the last
    accepted tree and `+ 1` is the first refused one.  (Counting the wrappers
    instead put the over-deep cases two past the edge, so their at-cap
    neighbour was not adjacent to them.)  ArrayValue (3 JSON levels per
    AnyValue node) rather than KvlistValue (4), so serde_json's own 128-deep
    recursion limit is not what answers first."""
    v = {"stringValue": "leaf"}
    for _ in range(1, depth):
        v = {"arrayValue": {"values": [v]}}
    return v


def otlp_deep_json(depth, records=0):
    """A record-less (by default) OTLP/JSON request whose sole resource
    attribute nests `depth` AnyValue nodes deep."""
    return json.dumps({"resourceLogs": [{
        "resource": {"attributes": [{"key": "deep", "value": deep_json_value(depth)}]},
        "scopeLogs": [{"logRecords": [
            {"timeUnixNano": TS, "body": {"stringValue": "hi"}} for _ in range(records)]}],
    }]}, separators=(",", ":")).encode()


def otlp_deep_pb(depth, records=0):
    """The same shape as an OTLP protobuf body, hand-encoded with the writer
    above -- the protobuf transport reaches the cap by a different route (the
    wire pre-scan, not the JSON deserializer), so it is its own case.  `depth`
    counts AnyValue nodes, as above.
    ExportLogsServiceRequest{1: ResourceLogs{1: Resource{1: KeyValue}, 2:
    ScopeLogs{2: LogRecord}}}; AnyValue{1: string, 5: ArrayValue{1: values}};
    LogRecord{1: fixed64 time, 5: AnyValue body}."""
    value = bytes_field(1, b"leaf")  # AnyValue.string_value
    for _ in range(1, depth):
        value = bytes_field(5, bytes_field(1, value))  # array_value -> values
    kv_pair = bytes_field(1, b"deep") + bytes_field(2, value)
    record = (tag(1, 1) + int(TS).to_bytes(8, "little")
              + bytes_field(5, bytes_field(1, b"hi")))
    scope = bytes_field(2, b"".join(bytes_field(2, record) for _ in range(records)))
    return bytes_field(1, bytes_field(1, bytes_field(1, kv_pair)) + scope)


case("otlp/record-less-over-deep-attr", "otlp",
     otlp_deep_json(MAX_ANYVALUE_DEPTH + 1), "application/json", expect="DIFF")
case("otlp/record-less-at-cap-attr", "otlp",
     otlp_deep_json(MAX_ANYVALUE_DEPTH), "application/json")
case("otlp/record-bearing-over-deep-attr", "otlp",
     otlp_deep_json(MAX_ANYVALUE_DEPTH + 1, records=1), "application/json", expect="DIFF")
case("otlppb/record-less-over-deep-attr", "otlppb",
     otlp_deep_pb(MAX_ANYVALUE_DEPTH + 1), "application/x-protobuf", expect="DIFF")
case("otlppb/record-less-at-cap-attr", "otlppb",
     otlp_deep_pb(MAX_ANYVALUE_DEPTH), "application/x-protobuf")
case("otlppb/record-bearing-over-deep-attr", "otlppb",
     otlp_deep_pb(MAX_ANYVALUE_DEPTH + 1, records=1), "application/x-protobuf",
     expect="DIFF")

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
#
# THE WIRE AGREEING IS NOT STORAGE AGREEING, and these four cases only measure
# the wire.  Both sides accept; upstream then stores the near-miss as
# structured metadata while we store it as an index label, and our collision
# rule (#4: greatest ORIGINAL key, and `_` 0x5F sorts after `.` 0x2E) hands the
# label to the UNVALIDATED near-miss -- so a 2049-byte value is stored under a
# name the bound passed at two bytes.  Statuses cannot see that; the stored
# labels and the fingerprint are asserted in
# otlp_logs.rs::an_index_attribute_and_its_near_miss_collide_on_the_unvalidated_value
# and, out of live ClickHouse, in loki_push_live.rs::
# an_otlp_near_miss_spelling_stores_an_over_wide_indexed_label.  The cause is
# that we index every OTLP attribute where the reference indexes eighteen
# (#109); this issue matches the rejection surface, and it does.
case("otlp/index-key-vs-near-miss/bad-on-near-miss", "otlp",
     otlp_json([[("k8s.pod.name", "ok"), ("k8s_pod_name", B1)]]), "application/json")
case("otlp/index-key-vs-near-miss/bad-on-index", "otlp",
     otlp_json([[("k8s.pod.name", B1), ("k8s_pod_name", "ok")]]), "application/json")
case("otlp/index-key-vs-near-miss/service_name-bad-on-near-miss", "otlp",
     otlp_json([[("service.name", "ok"), ("service_name", B1)]]), "application/json")
case("otlp/index-key-vs-near-miss/service_name-bad-on-index", "otlp",
     otlp_json([[("service.name", B1), ("service_name", "ok")]]), "application/json")

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
    "otlppb": ("/otlp/v1/logs", "/v1/logs"),
}


def trim(s, n=100000):
    """Renders one response body for the transcript.

    A run of 100 or more of the same alphanumeric character becomes
    `<cxN>` -- the bodies quote label names and values that are 1025, 2049
    or 65537 bytes of one letter, and an unsquashed transcript is 370 KB of
    `bbbb...`.  The squash lives here, in the harness, so that a fresh run
    reproduces the checked-in transcript's bodies rather than a
    differently-post-processed copy of them.
    """
    s = s.replace("\n", "\\n")
    s = re.sub(r"([A-Za-z0-9])\1{99,}", lambda m: f"<{m.group(1)}x{len(m.group(0))}>", s)
    return s if len(s) <= n else s[:n] + f"…[{len(s)} bytes]"


# Closes the span this script generates.  Chosen so it cannot collide with a
# generated line: body blocks start `--- `, rules are `=` * 100.
GENERATED_END = "#### end of generated section ####"

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
# Everything this script writes lies between the `case ... expected` header
# above and this marker; oracle_probe.txt's preamble and its recorded-
# divergences notes are hand-maintained around it.  The marker is what makes
# "the script regenerates that span" checkable with one `sed`+`diff` -- the
# recipe is in the transcript's preamble.
print(GENERATED_END)
sys.exit(1 if unexpected else 0)
