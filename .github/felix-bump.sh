#!/usr/bin/env bash
# The pin patterns are literal text, not shell expansions.
# shellcheck disable=SC2016
# Moves this repo's Felix pins from one version to another:
#   .github/felix-bump.sh <old> <new>
# The felix-release workflow runs it, then updates .felix-version and the
# lockfile. Every pin listed here must be found, so a moved or renamed pin
# fails the bump instead of being skipped.
set -euo pipefail
OLD=${1:?usage: felix-bump.sh <old> <new>}
NEW=${2:?usage: felix-bump.sh <old> <new>}
cd "$(dirname "$0")/.."

die() { echo "felix-bump: $*" >&2; exit 1; }

# pin FILE PATTERN: replace PATTERN with {V}=OLD by PATTERN with {V}=NEW.
pin() {
  local file=$1 pattern=$2
  [ -f "$file" ] || die "$file does not exist"
  FROM=${pattern//'{V}'/$OLD} TO=${pattern//'{V}'/$NEW} \
    perl -0777 -pi -e '$n += s/\Q$ENV{FROM}\E/$ENV{TO}/g; END { exit($n ? 0 : 3) }' "$file" \
    || die "$file: expected '${pattern//'{V}'/$OLD}'"
}

pin Cargo.toml 'felix-client = "={V}"'
for f in deploy/compose.yaml tests/felix/compose.yaml tests/felix/cluster.yaml; do
  pin "$f" '${FELIX_VERSION:-{V}}'
done
