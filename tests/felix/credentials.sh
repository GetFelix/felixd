#!/usr/bin/env bash
# Copies the development credentials out of the running test stack into DIR.
# With `cluster`, from the three-broker stack.
set -euo pipefail
dir=${1:?usage: credentials.sh DIR [cluster]}
here=$(cd "$(dirname "$0")" && pwd)
compose=${COMPOSE:-docker compose}
mkdir -p "$dir"
if [ "${2:-}" = cluster ]; then
  for f in client.jwt controlplane.jwt operator.jwt; do
    $compose -f "$here/cluster.yaml" exec -T broker-1 cat "/var/lib/felix/$f" > "$dir/$f"
  done
  $compose -f "$here/cluster.yaml" exec -T broker-1 cat /var/lib/felix/ca.pem > "$dir/broker-cert.pem"
else
  for f in client.jwt controlplane.jwt broker-cert.pem; do
    $compose -f "$here/compose.yaml" exec -T felix-broker cat "/var/lib/felix/$f" > "$dir/$f"
  done
fi
