#!/usr/bin/env bash
# Issue #213: self-test for packaging/release-env.sh, packaging/archive.sh and
# packaging/release-assets.sh. Each negative case breaks one thing and must fail
# with that thing's own message. Needs rustc and dpkg-deb. Run from the repo root.
set -euo pipefail

ROOT="$PWD"
T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
pass=0
ok()  { pass=$((pass + 1)); echo "ok   $1"; }
bad() { echo "FAIL $1" >&2; exit 1; }
# expect_fail <id> <message> <command...>: the command must exit non-zero AND print <message>.
expect_fail() {
    local id="$1" msg="$2"; shift 2
    if "$@" > "$T/out" 2>&1; then bad "$id: exited 0"; fi
    grep -qF -- "$msg" "$T/out" || { cat "$T/out" >&2; bad "$id: no '$msg'"; }
    ok "$id"
}
# expect_ok <id> <command...>: the command must exit 0.
expect_ok() {
    local id="$1"; shift
    if "$@"; then ok "$id"; else bad "$id"; fi
}

case "$(uname -m)" in
    x86_64) ARCH=amd64; OTHER=arm64 ;;
    aarch64) ARCH=arm64; OTHER=amd64 ;;
    *) bad "unsupported host $(uname -m)" ;;
esac

# --- release-env.sh -------------------------------------------------------
env_of() { GITHUB_REF_NAME="$1" HOME=/HOME-MARKER "$ROOT/packaging/release-env.sh" "$2"; }
expect_ok E1 test "$(env_of v1.2.3 amd64)" = \
    "$(printf 'PKG_VERSION=1.2.3\nPKG_ARCH=amd64\nARCHIVE_NAME=pulsusdb-1.2.3-linux-amd64')"
expect_ok E2 test "$(env_of v1.2.3-rc.1 arm64)" = \
    "$(printf 'PKG_VERSION=1.2.3~rc.1\nPKG_ARCH=arm64\nARCHIVE_NAME=pulsusdb-1.2.3-rc.1-linux-arm64')"
expect_ok E3 test "$(env_of feat/x-y amd64)" = \
    "$(printf 'PKG_VERSION=feat~x~y\nPKG_ARCH=amd64\nARCHIVE_NAME=pulsusdb-feat-x-y-linux-amd64')"
expect_fail E5 "cannot name a release file" env_of v amd64
expect_fail E4 "unsupported arch" env_of v1.2.3 s390x

# --- archive.sh -----------------------------------------------------------
cat > "$T/stub.rs" <<'RS'
fn main() {
    if std::env::args().nth(1).as_deref() == Some("--version") {
        println!("{}", std::env::var("STUB_VERSION_LINE").unwrap_or_default());
    }
}
RS
rustc -O -o "$T/stub" "$T/stub.rs"

export GITHUB_REF_NAME=v1.2.3-rc.1 GITHUB_SHA=0123456789abcdef0123456789abcdef01234567 SOURCE_DATE_EPOCH=0
export PKG_VERSION='1.2.3~rc.1' PKG_ARCH="$ARCH" ARCHIVE_NAME="pulsusdb-1.2.3-rc.1-linux-$ARCH"
GOOD="pulsusdb 1.2.3-rc.1 ($GITHUB_SHA)"

# fresh workspace W: dist/pulsusdb = stub, out/ holds a .deb whose /usr/bin/pulsusdb is <payload>
workspace() {
    local payload="$1" deb_arch="$2"
    W="$T/w"; rm -rf "$W"; mkdir -p "$W/dist" "$W/out" "$W/packaging/config" "$T/root/DEBIAN" "$T/root/usr/bin"
    cp "$ROOT/LICENSE" "$W/"; cp "$ROOT/packaging/config/config.yaml" "$W/packaging/config/"
    install -m 0755 "$T/stub" "$W/dist/pulsusdb"
    install -m 0755 "$payload" "$T/root/usr/bin/pulsusdb"
    printf 'Package: pulsusdb\nVersion: %s\nArchitecture: %s\nMaintainer: t\nDescription: t\n' \
        "$PKG_VERSION" "$deb_arch" > "$T/root/DEBIAN/control"
    dpkg-deb -b --root-owner-group "$T/root" "$W/out/pulsusdb_${PKG_VERSION}_${deb_arch}.deb" > /dev/null
    cd "$W"
}
run() { STUB_VERSION_LINE="$1" "$ROOT/packaging/archive.sh" check; }

workspace "$T/stub" "$ARCH"; "$ROOT/packaging/archive.sh" build > /dev/null
owners() { [ "$(tar -tvzf "out/$ARCHIVE_NAME.tar.gz" | awk '{print $2}' | sort -u)" = 0/0 ]; }
expect_ok A0 owners
expect_ok A1 run "$GOOD"
expect_fail A2 "--version printed" run "$GOOD x"
expect_fail A3 "--version printed" run "pulsusdb 1.2.3 ($GITHUB_SHA)"
printf x >> dist/pulsusdb
expect_fail A4 "archive binary differs from dist/pulsusdb" run "$GOOD"

cp "$T/stub" "$T/other"; printf x >> "$T/other"
workspace "$T/other" "$ARCH"; "$ROOT/packaging/archive.sh" build > /dev/null
expect_fail A5 ".deb binary differs from dist/pulsusdb" run "$GOOD"

workspace "$T/stub" "$ARCH"; "$ROOT/packaging/archive.sh" build > /dev/null
mkdir "$T/re"; tar -xzf "out/$ARCHIVE_NAME.tar.gz" -C "$T/re"; touch "$T/re/$ARCHIVE_NAME/README.md"
tar -C "$T/re" -czf "out/$ARCHIVE_NAME.tar.gz" "$ARCHIVE_NAME"
expect_fail A6 "member list differs" run "$GOOD"

workspace "$T/stub" "$ARCH"; "$ROOT/packaging/archive.sh" build > /dev/null
printf '# edited\n' >> packaging/config/config.yaml
expect_fail A7 "config.yaml differs" run "$GOOD"

workspace "$T/stub" "$ARCH"; "$ROOT/packaging/archive.sh" build > /dev/null
printf '# edited\n' >> LICENSE
expect_fail A9 "LICENSE differs" run "$GOOD"

workspace "$T/stub" "$OTHER"
PKG_ARCH="$OTHER" ARCHIVE_NAME="pulsusdb-1.2.3-rc.1-linux-$OTHER" "$ROOT/packaging/archive.sh" build > /dev/null
expect_fail A8 "binary is not built for $OTHER" \
    env PKG_ARCH="$OTHER" ARCHIVE_NAME="pulsusdb-1.2.3-rc.1-linux-$OTHER" STUB_VERSION_LINE="$GOOD" "$ROOT/packaging/archive.sh" check
cd "$ROOT"

# --- release-assets.sh ----------------------------------------------------
assets() {
    A="$T/assets"; rm -rf "$A"; mkdir -p "$A"
    for f in pulsusdb_1.2.3_amd64.deb pulsusdb_1.2.3_arm64.deb pulsusdb-1.2.3-1.x86_64.rpm \
             pulsusdb-1.2.3-1.aarch64.rpm pulsusdb-1.2.3-linux-amd64.tar.gz pulsusdb-1.2.3-linux-arm64.tar.gz; do
        echo "$f" > "$A/$f"
    done
}
assets; "$ROOT/packaging/release-assets.sh" "$A"
sums() { [ "$(wc -l < "$A/SHA256SUMS")" -eq 6 ] && (cd "$A" && sha256sum -c --quiet SHA256SUMS); }
expect_ok R1 sums
assets; rm "$A/pulsusdb-1.2.3-linux-arm64.tar.gz"
expect_fail R2 "ending in -linux-arm64.tar.gz" "$ROOT/packaging/release-assets.sh" "$A"
assets; echo x > "$A/pulsusdb_0.0.0~smoketest_amd64.deb"
expect_fail R3 "ending in _amd64.deb" "$ROOT/packaging/release-assets.sh" "$A"
assets; echo x > "$A/notes.txt"
expect_fail R4 "want 6 entries, found 7" "$ROOT/packaging/release-assets.sh" "$A"

echo "release packaging self-test: $pass cases passed"
