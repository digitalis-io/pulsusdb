#!/bin/sh
# Post-removal — reload systemd so a removed unit drops out of its view. Leaves
# the pulsusdb user/group and /var/lib/pulsusdb data in place (standard: purge of
# data/user is an operator decision, not done automatically).
set -e

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
fi

exit 0
