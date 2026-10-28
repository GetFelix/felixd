#!/usr/bin/env bash
# Copies the development credentials out of the running test stack into DIR.
set -euo pipefail
dir=${1:?usage: credentials.sh DIR}
here=$(cd "$(dirname "$0")" && pwd)
compose=${COMPOSE:-docker compose}
mkdir -p "$dir"
for f in client.jwt controlplane.jwt broker-cert.pem; do
  $compose -f "$here/compose.yaml" exec -T felix-broker cat "/var/lib/felix/$f" > "$dir/$f"
done
