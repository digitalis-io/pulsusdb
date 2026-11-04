#!/usr/bin/env bash
# Binary archive (issue #213). Run from the repo root after the packages are built.
#   build  stage dist/pulsusdb, LICENSE and packaging/config/config.yaml under
#          <ARCHIVE_NAME>/ and write out/<ARCHIVE_NAME>.tar.gz
#   check  unpack that archive into a fresh directory and fail unless every
#          assertion below holds
# Env: ARCHIVE_NAME, PKG_VERSION, PKG_ARCH (packaging/release-env.sh),
#      GITHUB_REF_NAME, GITHUB_SHA. SOURCE_DATE_EPOCH overrides the member mtime.
# Usage: packaging/archive.sh <build|check>
set -euo pipefail

MODE="${1:?usage: archive.sh <build|check>}"
: "${ARCHIVE_NAME:?}" "${PKG_VERSION:?}" "${PKG_ARCH:?}" "${GITHUB_REF_NAME:?}" "${GITHUB_SHA:?}"
ARCHIVE="out/${ARCHIVE_NAME}.tar.gz"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
fail() { echo "archive: FAIL: $*" >&2; exit 1; }
sha() { sha256sum "$1" | cut -d' ' -f1; }

case "$MODE" in
build)
    S="$WORK/$ARCHIVE_NAME"
    mkdir -p "$S" out
    install -m 0755 dist/pulsusdb                "$S/pulsusdb"
    install -m 0644 LICENSE                      "$S/LICENSE"
    install -m 0644 packaging/config/config.yaml "$S/config.yaml"
    tar -C "$WORK" --sort=name --owner=0 --group=0 --numeric-owner \
        --mtime="@${SOURCE_DATE_EPOCH:-$(git show -s --format=%ct HEAD)}" \
        -cf - "$ARCHIVE_NAME" | gzip -n -9 > "$ARCHIVE"
    echo "archive: wrote $ARCHIVE"
    ;;
check)
    case "$PKG_ARCH" in
        amd64) MACHINE=62 ;;   # ELF e_machine EM_X86_64
        arm64) MACHINE=183 ;;  # ELF e_machine EM_AARCH64
        *) fail "unsupported arch: $PKG_ARCH" ;;
    esac
    printf '%s\n' "$ARCHIVE_NAME/" "$ARCHIVE_NAME/LICENSE" \
        "$ARCHIVE_NAME/config.yaml" "$ARCHIVE_NAME/pulsusdb" | LC_ALL=C sort > "$WORK/want"
    tar -tzf "$ARCHIVE" | LC_ALL=C sort > "$WORK/got"
    diff -u "$WORK/want" "$WORK/got" >&2 || fail "member list differs"
    mkdir "$WORK/x" "$WORK/deb"
    tar -xzf "$ARCHIVE" -C "$WORK/x"
    D="$WORK/x/$ARCHIVE_NAME"
    [ "$(stat -c %a "$D/pulsusdb")" = 755 ] || fail "binary mode is not 0755"
    [ "$(od -An -tu2 -j18 -N2 "$D/pulsusdb" | tr -d ' ')" = "$MACHINE" ] || fail "binary is not built for $PKG_ARCH"
    WANT="$(sha dist/pulsusdb)"
    [ "$(sha "$D/pulsusdb")" = "$WANT" ] || fail "archive binary differs from dist/pulsusdb"
    dpkg-deb -x "out/pulsusdb_${PKG_VERSION}_${PKG_ARCH}.deb" "$WORK/deb"
    [ "$(sha "$WORK/deb/usr/bin/pulsusdb")" = "$WANT" ] || fail ".deb binary differs from dist/pulsusdb"
    cmp -s LICENSE "$D/LICENSE" || fail "LICENSE differs"
    cmp -s packaging/config/config.yaml "$D/config.yaml" || fail "config.yaml differs"
    OUT="$(cd "$D" && ./pulsusdb --version)"
    EXPECT="pulsusdb ${GITHUB_REF_NAME#v} (${GITHUB_SHA})"
    [ "$OUT" = "$EXPECT" ] || fail "--version printed '$OUT', want '$EXPECT'"
    echo "archive: $ARCHIVE OK: $OUT"
    ;;
*) echo "usage: archive.sh <build|check>" >&2; exit 1 ;;
esac
