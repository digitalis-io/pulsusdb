#!/bin/sh
# Pre-removal — stop+disable the unit ONLY on full removal, not on the remove
# half of an upgrade. deb passes "remove" (and "upgrade" on the upgrade path);
# rpm passes the remaining-install count ("0" on final removal, "1" on upgrade).
set -e

if { [ "$1" = "remove" ] || [ "$1" = "0" ]; } && [ -d /run/systemd/system ]; then
    systemctl --no-reload disable --now pulsusdb.service >/dev/null 2>&1 || true
fi

exit 0
