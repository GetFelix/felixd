#!/usr/bin/env bash
# Brings up deploy/compose.yaml, runs the counter workload under a steady
# producer, kills the node holding it and checks that another takes over and
# the output stream has no gaps or repeats (`felixd check --complete`).
#   scripts/compose-failover.sh
# ENGINE (docker) is the container CLI and COMPOSE ("$ENGINE compose") the
# compose command. Leaves the stack running; tear it down with `down -v`.
set -euo pipefail

cd "$(dirname "$0")/.."
engine=${ENGINE:-docker}
compose=(${COMPOSE:-$engine compose} -f deploy/compose.yaml)
c() { "${compose[@]}" "$@"; }
wasm=/usr/local/share/felixd/counter.wasm

# Retries a command until it succeeds or the deadline, in seconds, passes.
within() {
  local deadline=$((SECONDS + $1)); shift
  until "$@"; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 1
  done
}

holder() { # node number that has claimed the workload, if any
  local n
  for n in 1 2 3; do
    if [ "$(claims "$n")" -gt 0 ]; then echo "$n"; return; fi
  done
}
claims() { c logs --no-color "felixd-$1" 2>&1 | grep -a 'workload=counter' | grep -ac claimed || true; }
records() { # records on the counter's output stream
  c run --rm -T felixd check --id counter 2>&1 | sed -n 's/.*: \([0-9]*\) records, ok.*/\1/p' | head -n 1
}

c up -d --wait broker-1 broker-2 broker-3
c run --rm -T felix-fleet
c build felixd-1
c up -d felixd-1 felixd-2 felixd-3
c run --rm -T felixd run --id counter --input in --replication-factor 3 --snapshot-every 25 "$wasm"
c --profile producer up -d producer

within 60 test -n "$(holder)" || { echo "no node claimed the counter"; exit 1; }
victim=$(holder)
before=$(records)
echo "node-$victim holds the counter with ${before:-0} outputs; killing it"
"$engine" kill "felixd-node-$victim"

survivor_claimed() {
  for n in 1 2 3; do
    [ "$n" = "$victim" ] && continue
    [ "$(claims "$n")" -gt 0 ] && return 0
  done
  return 1
}
within 60 survivor_claimed || { echo "no other node took the counter over"; c logs --no-color; exit 1; }

grew() { [ "$(records || echo 0)" -ge $((${before:-0} + 30)) ]; }
within 90 grew || { echo "the output did not keep growing after the kill"; c logs --no-color; exit 1; }

c stop producer
caught_up() { c run --rm -T felixd check --complete >/tmp/felixd-check.out 2>&1; }
within 90 caught_up || { cat /tmp/felixd-check.out; echo "felixd check found problems"; c logs --no-color; exit 1; }
cat /tmp/felixd-check.out
c run --rm -T felixd ps
echo "failover ok"
