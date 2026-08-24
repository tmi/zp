#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release --quiet 1>&2
exec ./target/release/bench
