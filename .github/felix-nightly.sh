#!/usr/bin/env bash
# The nightly run against Felix main: starts each test stack from the Felix
# images tagged $FELIX_IMAGE_TAG (default: nightly) and runs the integration
# tests that CI runs against it. The felix-nightly workflow calls this after
# pointing Cargo.toml at the same Felix commit.
set -euo pipefail
cd "$(dirname "$0")/.."

export FELIX_VERSION=${FELIX_IMAGE_TAG:-nightly}
single=tests/felix/compose.yaml
cluster=tests/felix/cluster.yaml
deploy=deploy/compose.yaml

# Every Felix image in the stacks takes its tag from FELIX_VERSION. A hardcoded
# tag would run the pinned release and pass.
for f in "$single" "$cluster" "$deploy"; do
  if docker compose --profile '*' -f "$f" config --images | grep 'ghcr\.io/getfelix/felix-' | grep -qv ":${FELIX_VERSION}\$"; then
    echo "$f has a Felix image that is not on the $FELIX_VERSION tag" >&2
    exit 1
  fi
done

logs() {
  [ $? -eq 0 ] || for f in "$single" "$cluster" "$deploy"; do
    docker compose --profile '*' -f "$f" logs --no-color || true
  done
}
trap logs EXIT

export FELIXD_REQUIRE_FELIX=1 FELIX_AUTH_TENANT=dev
export FELIX_CA_FILE=/tmp/felix/broker-cert.pem FELIX_CONTROLPLANE_URL=http://127.0.0.1:8443
export FELIX_AUTH_TOKEN_FILE=/tmp/felix/client.jwt FELIX_CONTROLPLANE_TOKEN_FILE=/tmp/felix/controlplane.jwt

echo "::group::One broker"
docker compose -f "$single" up -d --wait
tests/felix/credentials.sh /tmp/felix
FELIX_BROKERS=127.0.0.1:5000 \
FELIXD_RESTRICTED_TOKEN_FILE=/tmp/felix/restricted-node.jwt \
FELIXD_GRANTS_DIR=/tmp/felix \
FELIXD_SECRETS_ADMISSION_TOKEN_FILE=/tmp/felix/admission-client.jwt \
  cargo test --workspace --test '*' -- --test-threads=1
docker compose -f "$single" down -v
echo "::endgroup::"

echo "::group::Three brokers"
docker compose -f "$cluster" up -d --wait broker-1 broker-2 broker-3
docker compose -f "$cluster" run --rm felix-fleet
tests/felix/credentials.sh /tmp/felix cluster
FELIX_BROKERS=127.0.0.1:5100,127.0.0.1:5200,127.0.0.1:5300 \
FELIX_OPERATOR_TOKEN_FILE=/tmp/felix/operator.jwt \
FELIXD_CLUSTER="$PWD/$cluster" \
  cargo test -p felixd --test cluster --test fence --test batches --test answers \
    --test pipeline --test merge --test drain --test admission -- --test-threads=1
docker compose -f "$cluster" down -v
echo "::endgroup::"

echo "::group::Failover in the deploy stack"
scripts/compose-failover.sh
docker compose -f "$deploy" down -v
echo "::endgroup::"
