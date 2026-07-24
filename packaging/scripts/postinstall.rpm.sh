#!/bin/sh
# rpm %post (via overrides.rpm.scripts.postinstall). Chown + daemon-reload only;
# NO try-restart — rpm runs %post before the old package's %postun on upgrade, so
# the upgrade restart lives exclusively in %posttrans (posttrans.sh). Keeping the
# deb-gated restart branch out of %post makes the "%post carries no restart"
# invariant literally true.
set -e

chown pulsusdb:pulsusdb /var/lib/pulsusdb 2>/dev/null || true

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
fi

echo "pulsusdb installed. It is NOT started automatically."
echo "Edit /etc/pulsusdb/config.yaml (and /etc/pulsusdb/pulsusdb.env for secrets), then:"
echo "  systemctl enable --now pulsusdb"

exit 0
