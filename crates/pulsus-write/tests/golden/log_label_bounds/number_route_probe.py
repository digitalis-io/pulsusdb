#!/usr/bin/env python3
"""Ignored-number route probe: the cell-level measurement behind the
504-cell / 216-cell figures quoted for issue #374.

`compare.py` beside this file sends ONE body per case through
`http.client`, so it pins one cell per shape: one ignored position, one wire
framing, one byte offset.  The claims made about those shapes are stronger
than that -- that each shape's verdict is a function of the REQUEST and not
of where the token sits in the body or of how the client chunked its writes.
This script is the instrument that measures the claim it is quoted for, and
the cell counts in the artifacts are its output rather than a report of it.

    20 shapes x 3 ignored positions x 4 wire framings x 3 byte offsets

  * exponent group -- the 14 shapes `compare.py`'s number cases and
    `loki_push::tests::parse_json_a_discarded_number_follows_the_references_overflow_rule`
    pin between them:  14 x 36 = 504 cells.
  * f32 group -- the 6 `-lead0-*` / control shapes:  6 x 36 = 216 cells.
  * three CONTROLS on the digits-only axis, below:  60 cells.

Every cell's expected pair of statuses is written down here; a cell whose
observed pair differs is reported MOVED and the run exits 1.  Nothing about
a run is inferred from another run: a shape claimed single-valued that is
not comes back as a table of the cells that disagreed.

Why the controls are load-bearing.  The 20 shapes are claimed INVARIANT
across framings and offsets, so if the framing loop never reached the
decoder -- if the kernel coalesced the writes, or if the server buffered the
whole body before parsing -- all four framings would return the same status
and the run would still be green.  A green run would then be evidence of
nothing.  So the probe also sends a 400-digit run at the two offsets either
side of upstream's read-buffer boundary:

    400 nines at offset 111 -- one write and 512-byte chunks are 204
                              upstream, 256- and 128-byte chunks are 400
    400 nines at offset 112 -- 400 upstream in all four framings

The third control is the registered divergence's headline row and its claim
is the opposite one:

    1,000 nines, all 3 offsets -- 400 upstream in every cell

1,000 digits is longer than the whole 512-byte window, so no offset and no
framing can make it fit, and no 1,000-digit value is representable in f64.
That is why `json/ignored-number-int-1000` is the row `compare.py` pins
expect="DIFF" and the fragile 400-nine one is not -- and this control is the
measurement behind that "framing-independent", over 36 cells rather than the
20 first reported in round 17.

`trySkipNumber` walks the CURRENT READ BUFFER only (`for i := iter.head; i <
iter.tail`, `iter_skip_strict.go:26 @ jsoniter v1.1.12`) over the 512 bytes
`jsoniter.NewDecoder` hands it (`config.go:366`, reached from
`unmarshal.DecodePushRequest`, `pkg/util/unmarshal/unmarshal.go:17 @ loki
v3.7.4`), and reports "skipped" only on reaching a terminating byte inside
that window (`:46-53`).  offset + length + 1 = 512 is therefore the last
accepted position in one write, which is why 111 passes and 112 does not;
and a 256-byte first read cuts the same token short, which is why the
chunked framings differ from the unchunked ones at 111.  A control cell that
does not answer what that predicts exits 1 with INSTRUMENT FAILURE -- a
message saying the instrument, not the subject, is what failed.  Measured
with the pause removed (`--delay 0`): the 20 shapes still report 720 agreeing
cells, and the only cells that move are chunked cells of the at-111 control.
HOW MANY of its six is a property of the host -- whether the kernel coalesced
that particular connection's writes -- and runs here have moved between 1 and
6 of them, so the count is not the claim.  What does not vary is: the run
exits 1, both shape groups stay at moved=0, at-112 and ctl-1000-nines stay
put, and every moved cell is a chunked at-111 cell.  The controls are the
only thing standing between a coalesced run and a green one.

All three controls are the registered length divergence (ledger residual 10),
so PulsusDB answers 204 to all 60 of their cells -- that is the divergence,
recorded cell by cell, not a surprise.

Timestamps.  Every body carries `now`.  `--stale-timestamps` re-runs the
whole matrix with a fixed 2023 timestamp instead and asserts the OPPOSITE
result: upstream's default one-week `reject_old_samples` window turns every
cell it would have accepted into a 400, so exactly the cells expected 204
from Loki must move and no others.  That is the probe's own mutant -- it
demonstrates that a green run is a measurement and not a rubber stamp, and
it is why the live-timestamp run uses `now`.

Usage (both servers must already be up, and must be two different servers;
the probe refuses to run otherwise -- see `preflight`):

    LOKI_PORT=13375 PULSUS_PORT=18375 python3 number_route_probe.py
    LOKI_PORT=13375 PULSUS_PORT=18375 python3 number_route_probe.py \
        --stale-timestamps

A serial run of the 780 cells takes MINUTES -- long enough to trip a
per-command timeout in some harnesses.  (How many is the host's, not the
probe's: measured here 3:35.9 for the live-timestamp run and 3:50.9 for the
`--stale-timestamps` one; the recording host measured the first three times
at 3:35.8-3:36.0.)  `--jobs N` puts N requests in flight at once and does
nothing else: stdout is assembled from the cell dictionary after every cell
is in, so it is byte-identical to the serial run -- measured, `--jobs 16`
finishes in ~18s and diffs clean against it.

Stdout is the whole measurement and nothing else, terminated by
GENERATED_END; the ports, the chunk pause and the job count go to stderr so
that a fresh run's stdout can be diffed against the recorded one with no
post-processing whatever ports it used.  See number_route_probe.txt in this directory for a
recorded run of each mode and the diff recipe, and oracle_probe.txt for the
servers and the container recipe.  A manual capture -- NOT run in CI.
"""
import argparse
import errno
from concurrent import futures
import os
import socket
import sys
import time

LOKI = ("127.0.0.1", int(os.environ.get("LOKI_PORT", "13174")))
PULSUS = ("127.0.0.1", int(os.environ.get("PULSUS_PORT", "18374")))
PATH = "/loki/api/v1/push"
CTYPE = "application/json"

# 2023-11-14, more than a week old under any plausible run date: the same
# constant `compare.py` uses for bodies both sides refuse at decode.
STALE_TS = "1700000000000000000"

# Wire framings.  `None` writes the body in one `sendall`; an integer writes
# it in chunks of that many bytes, with a pause between them so that the
# server's read returns the chunk rather than the whole body.
FRAMINGS = (None, 512, 256, 128)
# Byte offset of the value's FIRST byte within the request body.  120 and 300
# sit inside upstream's first 512-byte read; at 508 even the shortest shape
# here (`-0.0`, 4 bytes) has its terminating byte at index 512 or beyond, so
# the token runs to the end of that window.
OFFSETS = (120, 300, 508)
POSITIONS = ("envelope-key", "stream-key", "entry-4th-element")

# (name, literal, route, loki, pulsus) -- the expected pair for all 36 cells.
#
# `route` is which reader `Skip` dispatches to on the value's FIRST BYTE:
# `case '0'` unreads it and calls `ReadFloat32`, `case '-','1'..'9'` calls
# `skipNumber` (`iter_skip.go:72-96,83-87 @ jsoniter v1.1.12`).  It is
# recorded per shape because it is the rule being measured, not a label: the
# two routes range-check against different widths.
EXPONENT = (
    ("overflow", "1e999", "skipNumber", 400, 400),
    ("exp-309", "1e309", "skipNumber", 400, 400),
    ("exp-309-neg", "-1e309", "skipNumber", 400, 400),
    ("past-f64-max", "1.7976931348623159e308", "skipNumber", 400, 400),
    ("exp-1e22-digits", "1e1000000000000000000000", "skipNumber", 400, 400),
    ("exp-308", "1e308", "skipNumber", 204, 204),
    ("at-f64-max", "1.7976931348623157e308", "skipNumber", 204, 204),
    ("underflow", "1e-999", "skipNumber", 204, 204),
    ("denormal-min", "5e-324", "skipNumber", 204, 204),
    # Zero is finite in BOTH widths, which is why these two are 204 whichever
    # reader takes them -- and why no probe over this group could have shown
    # the dispatch in a status (issue #374 round 19).
    ("zero-exp", "0e999", "ReadFloat32", 204, 204),
    ("zero-exp-neg", "-0e999", "skipNumber", 204, 204),
    # No exponent: a short digits-only run, skipped unevaluated where it fits
    # the window and parsed to a finite value where it does not, so 204 either
    # way.  The LENGTH of such a run is the divergence -- see the controls.
    ("digits-20", "12345678901234567890", "skipNumber", 204, 204),
    ("small", "1.5e3", "skipNumber", 204, 204),
    ("neg-zero", "-0.0", "skipNumber", 204, 204),
)

# The dispatch itself: same magnitude, different first byte, different width.
F32 = (
    ("lead0-over", "0.35e39", "ReadFloat32", 400, 400),
    ("lead0-past-f32-max", "0.34028236e39", "ReadFloat32", 400, 400),
    ("lead0-under", "0.34e39", "ReadFloat32", 204, 204),
    ("lead0-at-f32-max", "0.34028235e39", "ReadFloat32", 204, 204),
    ("signed-lead0", "-0.35e39", "skipNumber", 204, 204),
    ("no-lead0", "3.5e38", "skipNumber", 204, 204),
)

# (name, literal, offsets, {framing: loki}, pulsus).  The first two exist to
# prove the framing and offset knobs reach upstream's decoder -- see the module
# docstring.  The third is the registered divergence's headline row, whose
# claim is the OPPOSITE one: 1,000 digits is longer than the whole 512-byte
# window, so it cannot fit at any offset under any framing, and no 1,000-digit
# value is representable in f64.  It is single-valued at 400 upstream for that
# reason and not by luck, which is why `json/ignored-number-int-1000` is the
# row pinned expect="DIFF" in compare.py and the fragile 400-nine one is not.
CONTROLS = (
    ("ctl-400-nines-at-111", "9" * 400, (111,), {None: 204, 512: 204, 256: 400, 128: 400}, 204),
    ("ctl-400-nines-at-112", "9" * 400, (112,), {None: 400, 512: 400, 256: 400, 128: 400}, 204),
    ("ctl-1000-nines", "9" * 1000, OFFSETS, {None: 400, 512: 400, 256: 400, 128: 400}, 204),
)


def body_parts(position, ts):
    """(before-pad, after-pad, tail) for one ignored position.

    The three positions are the ones the reference counts separately and the
    ones the hermetic test carries: an unknown key on the envelope, an unknown
    key inside a stream object, and an entry's fourth element.  Padding goes
    in an unknown envelope key so that the same knob works in all three.
    """
    win = '{"stream":{"app":"w"},"values":[["%s","FINAL"]]}' % ts
    if position == "envelope-key":
        return '{"pad":"', '","streams":[%s],"junk":' % win, "}"
    if position == "stream-key":
        return (
            '{"pad":"',
            '","streams":[{"stream":{"app":"a"},"values":[["%s","FINAL"]],"junk":' % ts,
            "}]}",
        )
    if position == "entry-4th-element":
        return (
            '{"pad":"',
            '","streams":[{"stream":{"app":"a"},"values":[["%s","x",{},' % ts,
            "]]}]}",
        )
    sys.exit("unknown position %r" % position)


def build(position, ts, value, offset):
    """A body whose `value` starts at exactly `offset` bytes in.

    The offset is structural, not searched for: the pad is sized so that the
    prefix is `offset` bytes long, and a target below the position's own
    minimum is fatal rather than silently rounded up to it.
    """
    head, mid, tail = body_parts(position, ts)
    pad = offset - (len(head) + len(mid))
    if pad < 0:
        sys.exit(
            "offset %d is unreachable in position %s: its shortest prefix is "
            "%d bytes" % (offset, position, len(head) + len(mid))
        )
    body = head + "a" * pad + mid + value + tail
    assert len(head) + pad + len(mid) == offset
    return body.encode()


def raw_post(target, body, framing, delay):
    """One push over a RAW SOCKET, with the body written in `framing`-byte
    chunks (or in one write when `framing` is None).

    `http.client` writes the body in one `sendall` and offers no way to say
    otherwise, which is why this is here.  The head is written on its own and
    followed by the same pause, so that the server's request-line reader does
    not pull the first body bytes into its own buffer along with the head --
    if it did, the first body read would return whatever it had left over
    rather than a chunk, and the framing would not be the one asked for.
    """
    head = (
        "POST %s HTTP/1.1\r\nHost: %s:%d\r\nContent-Type: %s\r\n"
        "Content-Length: %d\r\nConnection: close\r\n\r\n"
        % (PATH, target[0], target[1], CTYPE, len(body))
    ).encode()
    sock = socket.create_connection(target, timeout=30)
    try:
        try:
            sock.sendall(head)
            if framing is None:
                sock.sendall(body)
            else:
                time.sleep(delay)
                for i in range(0, len(body), framing):
                    sock.sendall(body[i : i + framing])
                    if i + framing < len(body):
                        time.sleep(delay)
        except (BrokenPipeError, ConnectionResetError):
            # A server that has already decided (a decode error part way
            # through the body) may close before the last chunk lands.  Its
            # answer is still on the socket, and that answer is the cell.
            pass
        buf = b""
        while b"\r\n" not in buf:
            chunk = sock.recv(65536)
            if not chunk:
                sys.exit("no response from %s:%d" % target)
            buf += chunk
        # Drain, so the peer sees an orderly close rather than a reset.
        try:
            while sock.recv(65536):
                pass
        except OSError:
            pass
    finally:
        sock.close()
    # Exactly an HTTP/1.1 status line, field by field.  A peer that answers
    # something else is not a cell of this matrix and must not be read as one,
    # and `status-line = HTTP-version SP status-code SP [ reason-phrase ]`
    # (RFC 9112 s4) has three fields plus the CRLF that delimits it, each of
    # which can be malformed on its own.  All of them, and what catches each,
    # measured against a stub responder answering a fixed line:
    #
    #   nothing, or no CRLF at all  -> `no response from` above (recv b"")
    #   fewer than two tokens       -> len(parts) < 2       ("", "HTTP/1.1")
    #   the version                 -> == b"HTTP/1.1"       ("HTTP/1.0 204 x",
    #                                                        "204 No Content")
    #   the separator               -> split on ONE b" "    ("HTTP/1.1\t204 x")
    #   the status, not a number    -> isdigit              ("HTTP/1.1 abc x")
    #   the status, wrong width     -> len(...) == 3        ("0204", "204000",
    #                                                        "20", "2040")
    #   the status, wrong value     -> the CALLER's one     ("200", "100")
    #                                  expected value: preflight demands 204
    #                                  and a matrix cell its own pair, so an
    #                                  interim 1xx or a 200 is refused there
    #                                  rather than here
    #   the reason phrase           -> NOT checked: RFC 9112 s4 makes it
    #                                  arbitrary, so "HTTP/1.1 204" with none
    #                                  is a valid answer and stays accepted
    #
    # There is no ninth row.  Four of the eight are the grammar's own parts
    # -- the version, the separator, the status code (three rows, one per way
    # a code can be wrong) and the reason phrase -- and the other two are the
    # two ways the line fails to arrive at all: nothing on the socket, and a
    # line too short to hold a code.  Descent stops at `bytes.isdigit`,
    # which is ASCII-only -- unlike `str.isdigit`, which accepts superscripts
    # and other Unicode digits; `buf` is bytes and is never decoded.  The
    # headers and body after the line are not parsed at all, deliberately: a
    # cell is the status and nothing else.
    #
    # Width plus digits is what makes `int()` faithful.  `.isdigit()` alone
    # accepted `0204` and `00000000204` as 204 (measured: both passed
    # `preflight`), so the returned int was not the status token.  With
    # exactly three ASCII digits, `int(parts[1]) == 204` iff
    # `parts[1] == b"204"`.
    parts = buf.split(b"\r\n", 1)[0].split(b" ")
    if (
        len(parts) < 2
        or parts[0] != b"HTTP/1.1"
        or len(parts[1]) != 3
        or not parts[1].isdigit()
    ):
        sys.exit("not an HTTP/1.1 status line from %s:%d: %r" % (target + (parts,)))
    return int(parts[1])


# The ONE status a Loki-compatible push receiver answers to a valid, synchronous
# push.  Not a range: the point of the preflight is that a responder which is
# not the server you think it is cannot validate the matrix, and "anything but
# an error" is exactly what such a responder is most likely to answer.  Both
# servers answer this and only this -- see the `preflight` docstring.
PREFLIGHT_STATUS = 204


def preflight(delay):
    """Refuse to run without both servers, and without them being two.

    A push nothing objects to has to come back `204` from each of them first
    -- `204` exactly, not "not an error".  A connection refused, a server that
    answers `400` to a trivially valid body, and a server that answers `200`
    are all the same failure: the matrix would be measuring the environment
    rather than the two decoders.  Upstream's `/loki/api/v1/push` writes
    `w.WriteHeader(http.StatusNoContent)` on the success path
    (`pkg/distributor/http.go:107,157 @ loki v3.7.4`) and PulsusDB's
    `ingest_loki_push` answers the same `204`, so a `200` here means the port
    is answering something that is not this endpoint.  `raw_post` returns a
    number only from a three-digit status token, so `204` here means the peer
    wrote exactly `204` -- see the field-by-field note there.

    The two targets must also be DIFFERENT endpoints.  Every shape in the
    matrix expects the same status from both sides, so one server answering
    on both ports would agree with itself 720 times; only the controls, whose
    two columns differ, would notice.
    """
    if LOKI == PULSUS:
        sys.exit(
            "loki and pulsus are the same endpoint %s:%d; the matrix would be "
            "comparing one server with itself" % LOKI
        )
    ts = str(int(time.time() * 1e9))
    body = ('{"streams":[{"stream":{"app":"preflight"},"values":[["%s","ok"]]}]}' % ts).encode()
    for label, target in (("loki", LOKI), ("pulsus", PULSUS)):
        try:
            status = raw_post(target, body, None, delay)
        except OSError as exc:
            hint = "" if exc.errno != errno.ECONNREFUSED else (
                "  Start it first; LOKI_PORT / PULSUS_PORT override the ports."
            )
            sys.exit("%s at %s:%d is not reachable: %s%s" % (label, target[0], target[1], exc, hint))
        if status != PREFLIGHT_STATUS:
            sys.exit(
                "%s at %s:%d answered %d to a trivially valid push, not %d; the "
                "matrix would be measuring the environment"
                % (label, target[0], target[1], status, PREFLIGHT_STATUS)
            )


def measure(value, ts, offsets, delay, jobs):
    """Every cell of one value: (position, framing, offset) -> (loki, pulsus).

    Each cell is two independent requests on two fresh sockets, so they can be
    made concurrently when `jobs > 1`.  The RESULT is a dict keyed by cell, and
    every caller reads it by key, so `jobs` changes how long a run takes and
    nothing about what it prints -- see `--jobs` in `main`.
    """
    keys, calls = [], []
    for position in POSITIONS:
        for offset in offsets:
            body = build(position, ts, value, offset)
            for framing in FRAMINGS:
                keys.append((position, framing, offset))
                calls.append((LOKI, body, framing))
                calls.append((PULSUS, body, framing))
    if jobs == 1:
        statuses = [raw_post(*call, delay) for call in calls]
    else:
        with futures.ThreadPoolExecutor(max_workers=jobs) as pool:
            statuses = list(pool.map(lambda call: raw_post(*call, delay), calls))
    return {key: (statuses[2 * i], statuses[2 * i + 1]) for i, key in enumerate(keys)}


def framing_name(framing):
    return "one-write" if framing is None else "%dB-chunks" % framing


# Last stdout line of every run, whatever its verdict.
GENERATED_END = "#### end of generated section ####"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--stale-timestamps",
        action="store_true",
        help="re-run with a fixed 2023 timestamp and require that exactly the "
        "cells Loki accepts move to 400 (the probe's own mutant)",
    )
    ap.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="requests in flight at once (default 1, i.e. strictly serial -- "
        "which is what the recorded transcript was made with; the 780 cells "
        "take minutes that way, which is long enough to hit a per-command "
        "timeout). Raising it changes only how long a run takes: stdout is "
        "assembled from the cell dictionary afterwards, so it is "
        "byte-identical either way -- measured, --jobs 16 finishes the same "
        "matrix in ~18s and its stdout diffs clean against the serial run. "
        "Every cell is still its own connection with its own chunk pauses, "
        "and the controls still have to bite: if a parallel run reports "
        "INSTRUMENT FAILURE, lower --jobs before believing the shapes.",
    )
    ap.add_argument(
        "--delay",
        type=float,
        default=0.005,
        help="seconds to pause between wire chunks (default 0.005); raise it "
        "if the controls report that the framing did not reach the decoder",
    )
    args = ap.parse_args()
    if args.jobs < 1:
        sys.exit("--jobs must be at least 1")

    preflight(args.delay)
    ts = STALE_TS if args.stale_timestamps else str(int(time.time() * 1e9))

    groups = (("exponent", EXPONENT), ("f32", F32))
    print(
        "number_route_probe: %d shapes x %d positions x %d framings x %d offsets"
        % (len(EXPONENT) + len(F32), len(POSITIONS), len(FRAMINGS), len(OFFSETS))
    )
    print(
        "timestamps: %s"
        % ("FIXED 2023 (--stale-timestamps)" if args.stale_timestamps else "now")
    )
    # The ports, the pause and the job count are run parameters, not
    # measurements: they go to STDERR so that stdout is byte-reproducible
    # across runs and can be diffed against the recorded transcript with no
    # post-processing.
    print(
        "loki %s:%d   pulsus %s:%d   chunk pause %g s   jobs %d"
        % (LOKI[0], LOKI[1], PULSUS[0], PULSUS[1], args.delay, args.jobs),
        file=sys.stderr,
    )
    print()

    moved = []
    rows = []
    for group, shapes in groups:
        for shape in shapes:
            name, value, route, loki_ok, pulsus_ok = shape
            if args.stale_timestamps:
                # Every accepted cell must be refused instead; a refused one
                # never reaches the timestamp check and must not move.
                loki_ok = 400
            observed = measure(value, ts, OFFSETS, args.delay, args.jobs)
            bad = {k: v for k, v in observed.items() if v != (loki_ok, pulsus_ok)}
            statuses = sorted({v for v in observed.values()})
            rows.append(
                (group, name, value, route, len(observed), statuses, bad, (loki_ok, pulsus_ok))
            )
            if bad:
                moved.append((group, name, bad, (loki_ok, pulsus_ok)))

    control_rows = []
    for control in CONTROLS:
        name, value, offsets, loki_by_framing, pulsus_ok = control
        observed = measure(value, ts, offsets, args.delay, args.jobs)
        expected = {
            k: ((400 if args.stale_timestamps else loki_by_framing[k[1]]), pulsus_ok)
            for k in observed
        }
        bad = {k: v for k, v in observed.items() if v != expected[k]}
        control_rows.append((name, value, offsets, observed, expected, bad))
        if bad:
            moved.append(("control", name, bad, None))

    width = max(len(r[1]) for r in rows)
    print("%s  %-10s  %-11s  cells  observed (loki,pulsus)" % ("shape".ljust(width), "group", "route"))
    for group, name, value, route, count, statuses, bad, expect in rows:
        rendered = " ".join("%d/%d" % s for s in statuses)
        flag = "   <-- MOVED, expected %d/%d" % expect if bad else ""
        print("%s  %-10s  %-11s  %-5d  %s%s" % (name.ljust(width), group, route, count, rendered, flag))
    print()

    # Not "the first two are NOT single-valued", which is what this line used
    # to say and what the runs refute: at-111 has two status pairs, at-112 has
    # one (400/204 in all four framings). The pair of them is the OFFSET knob
    # -- same 400 digits, one byte apart, opposite verdicts -- and at-111
    # alone is the framing knob.
    print(
        "controls (ctl-400-nines-at-111 is NOT single-valued; at-112 is, one "
        "byte later -- see the docstring)"
    )
    for name, value, offsets, observed, expected, bad in control_rows:
        print(
            "  %s  (%d digits at offset%s %s)"
            % (
                name,
                len(value),
                "" if len(offsets) == 1 else "s",
                ", ".join(str(o) for o in offsets),
            )
        )
        for position in POSITIONS:
            for offset in offsets:
                for framing in FRAMINGS:
                    key = (position, framing, offset)
                    got, want = observed[key], expected[key]
                    flag = "   <-- MOVED, expected %d/%d" % want if got != want else ""
                    print(
                        "    %-18s off %-3d %-11s  loki %d  pulsus %d%s"
                        % (position, offset, framing_name(framing), got[0], got[1], flag)
                    )
    print()

    # Keyed on the group tag the control loop appends, not on the shape's name:
    # a shape in EXPONENT or F32 that happened to be called `ctl-…` would
    # otherwise claim the instrument failed, and a control renamed out of that
    # prefix would stop claiming it.
    if any(group == "control" for group, _n, _b, _e in moved):
        print(
            "INSTRUMENT FAILURE: a control moved. The framing or the offset knob "
            "did not reach upstream's decoder, so the invariance the shapes above "
            "report is not evidence. Raise --delay and re-run."
        )

    for group, shapes in groups:
        cells = len(shapes) * len(POSITIONS) * len(FRAMINGS) * len(OFFSETS)
        group_moved = sum(len(b) for g, _n, b, _e in moved if g == group)
        print(
            "%s group: %d shapes x %d cells = %d cells, moved=%d"
            % (group, len(shapes), len(POSITIONS) * len(FRAMINGS) * len(OFFSETS), cells, group_moved)
        )
    for name, _v, offsets, observed, _e, bad in control_rows:
        print(
            "control %s: %d positions x %d framings x %d offset(s) = %d cells, "
            "moved=%d"
            % (name, len(POSITIONS), len(FRAMINGS), len(offsets), len(observed), len(bad))
        )
    ctl_cells = sum(len(r[3]) for r in control_rows)
    total = sum(len(s) for _g, s in groups) * len(POSITIONS) * len(FRAMINGS) * len(OFFSETS) + ctl_cells
    print("total %d cells, moved=%d" % (total, sum(len(b) for _g, _n, b, _e in moved)))

    mutant_failed = False
    if args.stale_timestamps:
        # The mutant's own assertion: the shapes that MUST move are exactly
        # the ones the live-timestamp table records Loki accepting, and the
        # comparison above already expects them at 400, so a correct mutant
        # run reports moved=0 while its `observed` column differs from the
        # live one.  What is checked here is the change itself.
        should = [n for _g, s in groups for (n, _v, _r, l, _p) in s if l == 204]
        should_ctl = [
            n for (n, _v, _o, by_framing, _p) in CONTROLS if 204 in by_framing.values()
        ]
        not_refused = [r[1] for r in rows if any(o[0] != 400 for o in r[5])]
        mutant_failed = bool(not_refused)
        print()
        print(
            "stale-timestamp mutant: every cell must be 400 upstream, which for "
            "the %d shape(s) Loki accepts with `now` (%s) and the %d control(s) "
            "with an accepted cell (%s) is a MOVE"
            % (len(should), ", ".join(should), len(should_ctl), ", ".join(should_ctl))
        )
        if mutant_failed:
            print("still accepted upstream under the stale timestamp: %s" % ", ".join(not_refused))
            print(
                "The mutant did not bite, so a green run of this probe is not "
                "evidence that its comparator can see a change."
            )
        else:
            print(
                "every one of them moved; the probe's comparator sees a change "
                "when there is one to see. PulsusDB has no reject_old_samples "
                "equivalent, so its column is unchanged -- which is why the "
                "live-timestamp run sends `now`."
            )

    # Closes the span this script generates, the way `compare.py`'s
    # GENERATED_END closes its own: everything from the first line to here is
    # stdout verbatim, so the recorded transcript can be checked with one
    # `sed`+`diff`.  The recipe is in number_route_probe.txt's preamble.
    print(GENERATED_END)
    return 1 if (moved or mutant_failed) else 0


if __name__ == "__main__":
    sys.exit(main())
