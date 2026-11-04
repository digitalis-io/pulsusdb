#!/usr/bin/env bash
# Release assets (issue #213): fail unless <dir> holds exactly one .deb, one .rpm
# and one binary archive per architecture and nothing else, then write
# <dir>/SHA256SUMS over all six.
# Usage: packaging/release-assets.sh <dir>
set -euo pipefail

cd "${1:?usage: release-assets.sh <dir>}"
for s in _amd64.deb _arm64.deb .x86_64.rpm .aarch64.rpm -linux-amd64.tar.gz -linux-arm64.tar.gz; do
    set -- ./*"$s"
    if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then
        echo "release-assets: FAIL: want one asset ending in $s, found: $*" >&2; exit 1
    fi
done
n="$(find . -mindepth 1 -maxdepth 1 | wc -l)"
[ "$n" -eq 6 ] || { echo "release-assets: FAIL: want 6 entries, found $n" >&2; exit 1; }
sha256sum ./*.deb ./*.rpm ./*.tar.gz > SHA256SUMS
