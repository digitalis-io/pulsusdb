#!/usr/bin/env bash
# Release names for one architecture (issue #213), derived from GITHUB_REF_NAME.
# Prints three KEY=VALUE lines for "$GITHUB_ENV":
#   PKG_VERSION   the ref without its leading `v`, every `-` rewritten to `~`
#                 (legal in an rpm Version; sorts below the release in deb and rpm)
#   PKG_ARCH      amd64 | arm64 (nfpm maps it to x86_64 / aarch64 for rpm)
#   ARCHIVE_NAME  pulsusdb-<the ref without its leading v>-linux-<arch>
# In both names a `/` in the ref becomes `-`.
# Usage: packaging/release-env.sh <amd64|arm64>
set -euo pipefail

ARCH="${1:?usage: release-env.sh <amd64|arm64>}"
case "$ARCH" in
    amd64|arm64) ;;
    *) echo "release-env: unsupported arch: $ARCH" >&2; exit 1 ;;
esac
REF="${GITHUB_REF_NAME:?release-env: GITHUB_REF_NAME is unset}"
[ -n "${REF#v}" ] || { echo "release-env: ref name '$REF' cannot name a release file" >&2; exit 1; }
# A branch name may hold `/` (workflow_dispatch); a file name may not.
V="$(printf '%s' "${REF#v}" | sed 's#/#-#g')"

# sed, not ${V//-/~}: bash tilde-expands a `~` in that replacement to $HOME.
printf 'PKG_VERSION=%s\n' "$(printf '%s' "$V" | sed 's/-/~/g')"
printf 'PKG_ARCH=%s\n' "$ARCH"
printf 'ARCHIVE_NAME=pulsusdb-%s-linux-%s\n' "$V" "$ARCH"
