#!/usr/bin/env bash
# Model-check a module in specs/ under each configuration registered for it
# below, and hold each to its outcome. `TLA_MODULE` picks the module:
# Felixd (the default), FelixdSpecs or FelixdMerge.
#
# A configuration expected to pass must pass. One expected to find a
# violation must find exactly that invariant violated: it turns one mechanism
# off to show the invariant depends on it, or pins a known gap. A violation
# that quietly became a pass would be a model that stopped saying anything.
#
# `scripts/check_tla.sh Felixd FelixdNoMarker` checks only those; a name not
# registered below must pass. `TLA_MODULE=FelixdSpecs scripts/check_tla.sh`
# checks the spec history model, and `TLA_MODULE=FelixdMerge` the merge log
# model. `TLC_WORKERS=4` caps TLC's worker threads.
#
# `TLC_SIMULATE=N` walks N random behaviours of up to `TLC_DEPTH` (default
# 100) steps instead of checking every state. The nightly job runs
# FelixdFull, FelixdSpecsFull and FelixdMergeFull this way, as they are too
# large to check exhaustively.
#
# Needs Java 11+ on PATH. The TLA+ tools are fetched once, pinned by release
# and checksum, into target/tla/.
set -euo pipefail

cd "$(dirname "$0")/.."

TLA_VERSION="v1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/${TLA_VERSION}/tla2tools.jar"
JAR="target/tla/tla2tools-${TLA_VERSION}.jar"
SPEC_DIR="specs"

felixd_expectations=(
  "Felixd pass"
  "FelixdGen pass"
  "FelixdThree pass"
  "FelixdIdle pass"
  "FelixdRecreate pass"
  "FelixdLive pass"
  "FelixdNoMarker violates NoInterleave"
  "FelixdNoFloor violates EpochMonotone"
  "FelixdNoConfirm violates NoStaleFailure"
  "FelixdFailWindow pass"
  "FelixdNoFailRecord violates NoStaleFailure"
  "FelixdNoReplayGuard violates OutputsExact"
  "FelixdNoReplayGuardSplit violates SnapshotsSplit"
  "FelixdRecreateAnytime violates UniqueEpoch"
  "FelixdTailFold violates NoPoison"
  "FelixdNoLaterEpochWins violates EpochMonotone"
  "FelixdPartial pass"
  "FelixdPartialNoIndex violates NoPoison"
)

felixd_specs_expectations=(
  "FelixdSpecs pass"
  "FelixdSpecsStale pass"
  "FelixdSpecsNoFloorCarry violates RanMonotone"
  "FelixdSpecsNoRetryFloor violates FloorMonotone"
  "FelixdSpecsNoNopFloor violates NoRunBelowFloor"
  "FelixdSpecsNopFirst violates OutputLineage"
  "FelixdSpecsNoHistory violates OutputLineage"
  "FelixdSpecsNoPostCheck violates RanMonotone"
  "FelixdSpecsCheckNopFirst violates CheckMatchesRun"
  "FelixdSpecsCheckNoNopFloor violates CheckMatchesRun"
  "FelixdSpecsCheckNoRefusal violates CheckMatchesRun"
  "FelixdSpecsCheckNoLineage violates CheckMatchesRun"
)

felixd_merge_expectations=(
  "FelixdMergeNoClaim violates EntriesClaimed"
  "FelixdMergeNoClaimFence violates MergeEpochMonotone"
  "FelixdMergeNoClaimCheck violates MergeEpochMonotone"
  "FelixdMergeNoCatchUp violates OutputsFollowMerge"
  "FelixdMergeBeforeOut violates OneWriter"
  "FelixdMergeNoSameBytes violates StepsFollowMerge"
  "FelixdMergeNoClaimExact pass"
  "FelixdMergeNoSkip pass"
  "FelixdMergeRecreate pass"
  "FelixdMergeLost pass"
  "FelixdMerge pass"
)

MODULE="${TLA_MODULE:-Felixd}"
case "$MODULE" in
  Felixd) expectations=("${felixd_expectations[@]}") ;;
  FelixdSpecs) expectations=("${felixd_specs_expectations[@]}") ;;
  FelixdMerge) expectations=("${felixd_merge_expectations[@]}") ;;
  *) echo "unknown TLA_MODULE: $MODULE" >&2; exit 1 ;;
esac

if [ ! -f "$JAR" ]; then
  mkdir -p "$(dirname "$JAR")"
  echo "fetching TLA+ tools ${TLA_VERSION}"
  curl -sSL -o "$JAR.tmp" "$TLA_URL"
  actual="$(shasum -a 256 "$JAR.tmp" | awk '{print $1}')"
  if [ "$actual" != "$TLA_SHA256" ]; then
    echo "tla2tools.jar checksum mismatch: $actual" >&2
    rm -f "$JAR.tmp"
    exit 1
  fi
  mv "$JAR.tmp" "$JAR"
fi

selected=("${expectations[@]}")
if [ "$#" -gt 0 ]; then
  selected=()
  for name in "$@"; do
    entry="$name pass"
    for known in "${expectations[@]}"; do
      if [ "${known%% *}" = "$name" ]; then entry="$known"; fi
    done
    selected+=("$entry")
  done
fi

failed=0
for entry in "${selected[@]}"; do
  cfg="${entry%% *}"
  expect="${entry#* }"
  echo "== $cfg (expected: $expect)"
  # TLC's state files go outside the tree, and are removed after each run.
  scratch="$(mktemp -d "${TMPDIR:-/tmp}/tlc.XXXXXX")"
  mode=()
  if [ -n "${TLC_SIMULATE:-}" ]; then
    mode=(-simulate "num=$TLC_SIMULATE" -depth "${TLC_DEPTH:-100}")
  fi
  status=0
  output="$(cd "$SPEC_DIR" && java -XX:+UseParallelGC -jar "../$JAR" -metadir "$scratch" \
    ${mode[@]+"${mode[@]}"} -deadlock -workers "${TLC_WORKERS:-auto}" -checkpoint 0 \
    -config "$cfg.cfg" "$MODULE.tla" 2>&1)" || status=$?
  rm -rf "$scratch"
  echo "$output" | grep -E "states generated|distinct states|depth of the complete|^Error:|is violated|Finished in" | tail -5 || true
  case "$expect" in
    pass)
      if [ "$status" -ne 0 ] || grep -qE "is violated" <<<"$output"; then
        echo "   FAIL: expected no violation"
        echo "$output" | tail -120
        failed=1
      fi
      ;;
    "violates "*)
      invariant="${expect#violates }"
      if ! grep -q "Invariant $invariant is violated" <<<"$output"; then
        echo "   FAIL: expected TLC to violate $invariant"
        echo "$output" | tail -60
        failed=1
      else
        # The trace, for the log.
        echo "$output" | sed -n '/is violated/,/^[0-9]* states generated/p' | head -150 || true
      fi
      ;;
  esac
done

if [ "$failed" -ne 0 ]; then
  echo "TLA+ checks failed"
  exit 1
fi
echo "TLA+ checks passed"
