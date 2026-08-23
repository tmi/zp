#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p build
if [ ! -f build/Bench.class ] || [ src/Bench.java -nt build/Bench.class ]; then
  javac -d build src/Bench.java 1>&2
fi
exec java -Xms1g -Xmx1g -cp build Bench
