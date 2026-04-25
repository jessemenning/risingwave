#!/usr/bin/env bash
# run-solace-integration-tests.sh
#
# Runs the RisingWave Solace connector integration tests against a live broker.
#
# Usage:
#   ./run-solace-integration-tests.sh                  # run tests only
#   ./run-solace-integration-tests.sh --setup          # provision queues, then run
#   ./run-solace-integration-tests.sh --setup --teardown  # provision, run, tear down
#
# The default broker target is the local docker-compose from solace-rs/:
#   docker compose -f solace-rs/docker-compose.yaml up -d
#
# Override with environment variables if pointing at a different broker:
#   BROKER_URL=tcp://... SEMP_URL=http://... ./run-solace-integration-tests.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# ---------------------------------------------------------------------------
# Parse flags
# ---------------------------------------------------------------------------
DO_SETUP=false
DO_TEARDOWN=false
for arg in "$@"; do
  case "${arg}" in
    --setup)    DO_SETUP=true ;;
    --teardown) DO_TEARDOWN=true ;;
    *) echo "Unknown argument: ${arg}"; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Broker connection defaults (local docker-compose)
# ---------------------------------------------------------------------------
export BROKER_URL="${BROKER_URL:-tcp://localhost:55554}"
export SEMP_URL="${SEMP_URL:-http://localhost:8080}"
export SEMP_VPN="${SEMP_VPN:-default}"
export SEMP_USERNAME="${SEMP_USERNAME:-admin}"
export SEMP_PASSWORD="${SEMP_PASSWORD:-admin}"

echo "==> Broker: ${BROKER_URL}"
echo "==> SEMP:   ${SEMP_URL}"
echo

# ---------------------------------------------------------------------------
# Optional: provision queues before running tests
# ---------------------------------------------------------------------------
if [[ "${DO_SETUP}" == true ]]; then
  echo "==> Provisioning test queues ..."
  "${SCRIPT_DIR}/scripts/setup-solace-queues.sh"
  echo
fi

# ---------------------------------------------------------------------------
# Run tests
# ---------------------------------------------------------------------------
echo "==> Running Solace connector integration tests ..."
cd "${REPO_ROOT}"
cargo test \
  -p risingwave_connector \
  --features source-solace,sink-solace \
  --test solace_integration \
  -- --include-ignored --test-threads=1

echo
echo "==> All tests complete."

# ---------------------------------------------------------------------------
# Optional: tear down after tests
# ---------------------------------------------------------------------------
if [[ "${DO_TEARDOWN}" == true ]]; then
  echo
  echo "==> Tearing down test queues ..."
  # Use SEMP DELETE to remove each test queue.
  for queue in "rw-integration-test-queue"; do
    echo -n "    Deleting queue '${queue}' ... "
    http_code=$(curl --silent --output /dev/null --write-out "%{http_code}" \
      -u "${SEMP_USERNAME}:${SEMP_PASSWORD}" \
      -X DELETE \
      "${SEMP_URL}/SEMP/v2/config/msgVpns/${SEMP_VPN}/queues/${queue}")
    echo "HTTP ${http_code}"
  done
fi
