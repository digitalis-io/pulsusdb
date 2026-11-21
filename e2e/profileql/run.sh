#!/bin/sh
# Issue #832: runs pages.mjs beside the library installed in the image.
# Usage: run.sh <front end base URL> <specs JSON>
set -eu
cp /src/pages.mjs /opt/profileql/
exec node /opt/profileql/pages.mjs "$@"
