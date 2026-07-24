#!/usr/bin/env bash
# Package smoke test (issue #213). Installs the arch-native .deb (ubuntu:22.04,
# glibc 2.35) and .rpm (rockylinux:9, glibc 2.34) in throwaway containers and
# asserts: install succeeds; the systemd unit carries the exact ExecStart +
# hardening directives; the pulsusdb user/group and /var/lib/pulsusdb ownership
# land; a lower-version base package is upgraded to the real one with the on-disk
# binary REPLACED (sha256 match) and conffile edits PRESERVED (noreplace); the
# upgrade-restart scriptlet is present and correctly gated/placed per format;
# clean uninstall. No PID-1 systemd in the containers — every systemctl call in
# the scriptlets is `[ -d /run/systemd/system ]`-guarded, so install/upgrade/
# uninstall never error; unit assertions are file-based.
#
# Both distro glibc floors exceed the 2.31 build floor, so a too-high build base
# would fail the Rocky leg — this test IS the floor's proof.
#
# Usage: packaging/smoke.sh <arch> <pkg_version>
#   <arch>         amd64 | arm64   (matches the runner; picks the container arch)
#   <pkg_version>  the rpm-legal PKG_VERSION the real packages were built with
#                  (e.g. 1.2.3 or 1.2.3~rc.1)
#
# Expects, relative to the repo root (built by the caller / release.yml):
#   dist/pulsusdb                                  the REAL staged binary
#   out/pulsusdb_<ver>_<arch>.deb                  real deb
#   out/pulsusdb-<ver>-1.<rpm_arch>.rpm            real rpm
#   smoke-base/pulsusdb_0.0.0~smoketest_<arch>.deb base deb (distinct payload)
#   smoke-base/pulsusdb-0.0.0~smoketest-1.<rpm_arch>.rpm base rpm
set -euo pipefail

ARCH="${1:?usage: smoke.sh <arch> <pkg_version>}"
PKG_VERSION="${2:?usage: smoke.sh <arch> <pkg_version>}"
BASE_VERSION="0.0.0~smoketest"

case "$ARCH" in
    amd64) RPM_ARCH=x86_64 ;;
    arm64) RPM_ARCH=aarch64 ;;
    *) echo "unsupported arch: $ARCH" >&2; exit 1 ;;
esac

# Container runtime (podman locally, docker in CI).
DOCKER="${DOCKER:-docker}"
command -v "$DOCKER" >/dev/null 2>&1 || DOCKER=podman

# The REAL package's on-disk binary must match dist/pulsusdb byte-for-byte after
# the upgrade; the base package ships a distinct payload so a missed replacement
# is detectable.
REAL_SHA="$(sha256sum dist/pulsusdb | cut -d' ' -f1)"

echo "== smoke: arch=$ARCH rpm_arch=$RPM_ARCH version=$PKG_VERSION real_sha=$REAL_SHA =="

REAL_DEB="out/pulsusdb_${PKG_VERSION}_${ARCH}.deb"
BASE_DEB="smoke-base/pulsusdb_${BASE_VERSION}_${ARCH}.deb"
REAL_RPM="out/pulsusdb-${PKG_VERSION}-1.${RPM_ARCH}.rpm"
BASE_RPM="smoke-base/pulsusdb-${BASE_VERSION}-1.${RPM_ARCH}.rpm"

for f in "$REAL_DEB" "$BASE_DEB" "$REAL_RPM" "$BASE_RPM"; do
    [ -f "$f" ] || { echo "missing package: $f" >&2; exit 1; }
done

# ---------------------------------------------------------------------------
# DEB leg (ubuntu:22.04)
# ---------------------------------------------------------------------------
echo "== DEB leg (ubuntu:22.04) =="
"$DOCKER" run --rm -e REAL_SHA="$REAL_SHA" -e PKG_VERSION="$PKG_VERSION" \
    -e BASE_VERSION="$BASE_VERSION" -e ARCH="$ARCH" \
    -v "$PWD":/w:z -w /w docker.io/library/ubuntu:22.04 bash -euo pipefail -c '
    UNIT=/usr/lib/systemd/system/pulsusdb.service
    REAL_DEB="out/pulsusdb_${PKG_VERSION}_${ARCH}.deb"
    BASE_DEB="smoke-base/pulsusdb_${BASE_VERSION}_${ARCH}.deb"

    # 1) install the base (lower version, distinct payload)
    dpkg -i "$BASE_DEB"
    sha_base="$(sha256sum /usr/bin/pulsusdb | cut -d" " -f1)"
    [ "$sha_base" != "$REAL_SHA" ] || { echo "FAIL: base payload equals real payload"; exit 1; }

    # 2) operator edits a conffile, then upgrade to the real package
    printf "\n# operator edit\n" >> /etc/pulsusdb/config.yaml
    dpkg -i "$REAL_DEB"

    # (a) metadata upgrade proof: version transitioned off the base
    iv="$(dpkg-query -W -f="\${Version}" pulsusdb)"
    [ "$iv" != "$BASE_VERSION" ] || { echo "FAIL: still on base version"; exit 1; }
    case "$iv" in "$PKG_VERSION"|"$PKG_VERSION"-*) ;; *) echo "FAIL: unexpected version $iv"; exit 1 ;; esac
    # (b) file-replacement proof: on-disk binary is the real package payload
    [ "$(sha256sum /usr/bin/pulsusdb | cut -d" " -f1)" = "$REAL_SHA" ] || { echo "FAIL: binary not replaced"; exit 1; }
    # (c) conffile noreplace held
    grep -q "# operator edit" /etc/pulsusdb/config.yaml || { echo "FAIL: conffile edit lost"; exit 1; }

    # 3) unit hardening directives (AC 6)
    grep -q "^ExecStart=/usr/bin/pulsusdb --config /etc/pulsusdb/config.yaml$" "$UNIT"
    grep -q "^User=pulsusdb$"                     "$UNIT"
    grep -q "^WorkingDirectory=/var/lib/pulsusdb$" "$UNIT"
    grep -q "^ReadWritePaths=/var/lib/pulsusdb$"   "$UNIT"
    grep -q "^NoNewPrivileges=true$"               "$UNIT"
    grep -q "^ProtectSystem=strict$"               "$UNIT"

    # 4) user/group/dirs
    getent passwd pulsusdb >/dev/null || { echo "FAIL: no pulsusdb user"; exit 1; }
    getent group  pulsusdb >/dev/null || { echo "FAIL: no pulsusdb group"; exit 1; }
    [ "$(stat -c %U /var/lib/pulsusdb)" = "pulsusdb" ] || { echo "FAIL: /var/lib/pulsusdb not owned by pulsusdb"; exit 1; }

    # 5) scriptlet gate: deb try-restart present AND gated by configure + upgrade ($2 nonempty)
    dpkg-deb -e "$REAL_DEB" DEBIAN
    grep -q "systemctl try-restart pulsusdb.service" DEBIAN/postinst || { echo "FAIL: deb postinst has no try-restart"; exit 1; }
    grep -Fq "[ \"\$1\" = \"configure\" ] && [ -n \"\${2:-}\" ]" DEBIAN/postinst || { echo "FAIL: deb gate literal missing"; exit 1; }
    grep -Pzoq "\[ \"\\\$1\" = \"configure\" \] && \[ -n \"\\\$\{2:-\}\" \]; then\n[[:space:]]*systemctl try-restart pulsusdb\.service" DEBIAN/postinst \
        || { echo "FAIL: deb try-restart not gated by configure+upgrade"; exit 1; }

    # sanity: the real binary actually runs (proves the payload executes on the
    # target glibc). A failure here MUST fail the smoke test (no masking).
    /usr/bin/pulsusdb --version

    # 6) clean uninstall removes binary + unit
    dpkg -r pulsusdb
    [ ! -f /usr/bin/pulsusdb ] || { echo "FAIL: binary survived removal"; exit 1; }
    [ ! -f "$UNIT" ] || { echo "FAIL: unit survived removal"; exit 1; }
    echo "DEB leg OK"
'

# ---------------------------------------------------------------------------
# RPM leg (rockylinux:9)
# ---------------------------------------------------------------------------
echo "== RPM leg (rockylinux:9) =="
"$DOCKER" run --rm -e REAL_SHA="$REAL_SHA" -e PKG_VERSION="$PKG_VERSION" \
    -e BASE_VERSION="$BASE_VERSION" -e RPM_ARCH="$RPM_ARCH" \
    -v "$PWD":/w:z -w /w docker.io/rockylinux:9 bash -euo pipefail -c '
    UNIT=/usr/lib/systemd/system/pulsusdb.service
    REAL_RPM="out/pulsusdb-${PKG_VERSION}-1.${RPM_ARCH}.rpm"
    BASE_RPM="smoke-base/pulsusdb-${BASE_VERSION}-1.${RPM_ARCH}.rpm"

    # 1) install base
    rpm -i "$BASE_RPM"
    sha_base="$(sha256sum /usr/bin/pulsusdb | cut -d" " -f1)"
    [ "$sha_base" != "$REAL_SHA" ] || { echo "FAIL: base payload equals real payload"; exit 1; }

    # 2) operator edit + upgrade
    printf "\n# operator edit\n" >> /etc/pulsusdb/config.yaml
    rpm -U "$REAL_RPM"

    iv="$(rpm -q --qf "%{VERSION}" pulsusdb)"
    [ "$iv" = "$PKG_VERSION" ] && [ "$iv" != "$BASE_VERSION" ] || { echo "FAIL: version did not transition ($iv)"; exit 1; }
    [ "$(sha256sum /usr/bin/pulsusdb | cut -d" " -f1)" = "$REAL_SHA" ] || { echo "FAIL: binary not replaced"; exit 1; }
    grep -q "# operator edit" /etc/pulsusdb/config.yaml || { echo "FAIL: config(noreplace) edit lost"; exit 1; }

    # 3) unit hardening directives (AC 6)
    grep -q "^ExecStart=/usr/bin/pulsusdb --config /etc/pulsusdb/config.yaml$" "$UNIT"
    grep -q "^User=pulsusdb$"                     "$UNIT"
    grep -q "^WorkingDirectory=/var/lib/pulsusdb$" "$UNIT"
    grep -q "^ReadWritePaths=/var/lib/pulsusdb$"   "$UNIT"
    grep -q "^NoNewPrivileges=true$"               "$UNIT"
    grep -q "^ProtectSystem=strict$"               "$UNIT"

    # 4) user/group/dirs
    getent passwd pulsusdb >/dev/null || { echo "FAIL: no pulsusdb user"; exit 1; }
    getent group  pulsusdb >/dev/null || { echo "FAIL: no pulsusdb group"; exit 1; }
    [ "$(stat -c %U /var/lib/pulsusdb)" = "pulsusdb" ] || { echo "FAIL: /var/lib/pulsusdb not owned by pulsusdb"; exit 1; }

    # 5) scriptlet hook: try-restart in %posttrans, ABSENT from %post
    scripts="$(rpm -qp --scripts "$REAL_RPM")"
    printf "%s\n" "$scripts" | awk "/scriptlet \(using/{f=0} /^posttrans scriptlet/{f=1} f" \
        | grep -q "systemctl try-restart pulsusdb.service" \
        || { echo "FAIL: rpm %posttrans missing try-restart"; exit 1; }
    if printf "%s\n" "$scripts" | awk "/scriptlet \(using/{f=0} /^postinstall scriptlet/{f=1} f" \
         | grep -q "systemctl try-restart pulsusdb.service"; then
        echo "FAIL: try-restart leaked into rpm %post"; exit 1
    fi

    # sanity: the real binary actually runs (proves the payload executes on the
    # target glibc). A failure here MUST fail the smoke test (no masking).
    /usr/bin/pulsusdb --version

    # 6) clean uninstall
    rpm -e pulsusdb
    [ ! -f /usr/bin/pulsusdb ] || { echo "FAIL: binary survived removal"; exit 1; }
    [ ! -f "$UNIT" ] || { echo "FAIL: unit survived removal"; exit 1; }
    echo "RPM leg OK"
'

echo "== smoke OK =="
