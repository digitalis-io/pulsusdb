#!/bin/sh
# Shared postinstall — used as the Debian postinst (deb `scripts.postinstall`).
# The rpm %post uses postinstall.rpm.sh (no restart branch); rpm's upgrade
# restart lives in %posttrans (posttrans.sh), because rpm runs %post BEFORE the
# old package's %postun on upgrade — restarting there is unsafe.
set -e

chown pulsusdb:pulsusdb /var/lib/pulsusdb 2>/dev/null || true

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
    # deb postinst args: $1=configure, $2=previously-configured version.
    # $2 is EMPTY on a fresh install and SET on an upgrade, so this restarts
    # only an already-configured (upgrade) install — a fresh install, which we
    # never start, is left untouched. try-restart is itself a no-op when the
    # unit is inactive, so an operator who has not enabled it stays stopped.
    if [ "$1" = "configure" ] && [ -n "${2:-}" ]; then
        systemctl try-restart pulsusdb.service || true
    fi
fi

echo "pulsusdb installed. It is NOT started automatically."
echo "Edit /etc/pulsusdb/config.yaml (and /etc/pulsusdb/pulsusdb.env for secrets), then:"
echo "  systemctl enable --now pulsusdb"

exit 0
