#!/bin/sh
# Create the dedicated pulsusdb system user/group before files land. Idempotent
# (guarded by getent) and cross-format: deb and rpm both invoke this before
# unpacking. No fixed uid — a bare-metal install has no cross-host uid-matching
# requirement, so the system range is fine.
set -e

if ! getent group pulsusdb >/dev/null 2>&1; then
    groupadd --system pulsusdb
fi
if ! getent passwd pulsusdb >/dev/null 2>&1; then
    useradd --system --gid pulsusdb \
        --home-dir /var/lib/pulsusdb --no-create-home \
        --shell /usr/sbin/nologin pulsusdb
fi

exit 0
