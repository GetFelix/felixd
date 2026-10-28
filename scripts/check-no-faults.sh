#!/usr/bin/env bash
# Builds felixd the way a release is built and fails if the binary still reads
# FELIXD_FAULTS. Only test builds may carry the crash hooks.
set -euo pipefail
profile="${1:-dev}"
cargo build --locked -p felixd --bin felixd --profile "$profile"
dir="${CARGO_TARGET_DIR:-target}/$([ "$profile" = dev ] && echo debug || echo "$profile")"
if LC_ALL=C grep -a -q FELIXD_FAULTS "$dir/felixd"; then
  echo "$dir/felixd reads FELIXD_FAULTS; the faults feature leaked into a normal build" >&2
  exit 1
fi
echo "$dir/felixd has no fault hooks"
