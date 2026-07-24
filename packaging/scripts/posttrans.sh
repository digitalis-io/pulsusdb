#!/bin/sh
# rpm %posttrans (via rpm.scripts.posttrans). Runs once at the very end of the
# transaction — the only safe place to restart on an rpm upgrade, since %post
# runs before the old package's %postun. try-restart is a no-op when the unit is
# inactive, so a fresh install (never started) is untouched; an already-running
# unit is restarted onto the new binary.
set -e

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
    systemctl try-restart pulsusdb.service || true
fi

exit 0
