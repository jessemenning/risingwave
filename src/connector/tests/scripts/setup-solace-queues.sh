#!/usr/bin/env bash
# setup-solace-queues.sh
#
# Creates the durable queue(s) required by the RisingWave connector
# integration tests via the Solace SEMP v2 REST API.
#
# Defaults target the local docker-compose broker from solace-rs/:
#   docker compose -f solace-rs/docker-compose.yaml up -d
#
# Usage:
#   ./setup-solace-queues.sh                         # local docker defaults
#   SEMP_URL=http://localhost:8080 ./setup-solace-queues.sh
#   SEMP_VPN=my-vpn SEMP_USERNAME=admin ./setup-solace-queues.sh

set -euo pipefail

SEMP_URL="${SEMP_URL:-http://localhost:8080}"
SEMP_VPN="${SEMP_VPN:-default}"
SEMP_USERNAME="${SEMP_USERNAME:-admin}"
SEMP_PASSWORD="${SEMP_PASSWORD:-admin}"

QUEUES=(
  "rw-integration-test-queue"
)

echo "==> SEMP endpoint: ${SEMP_URL}/SEMP/v2/config/msgVpns/${SEMP_VPN}/queues"
echo

create_queue() {
  local queue="$1"
  echo -n "    Creating queue '${queue}' ... "

  local http_code
  http_code=$(curl --silent --output /dev/null --write-out "%{http_code}" \
    -u "${SEMP_USERNAME}:${SEMP_PASSWORD}" \
    -H "Content-Type: application/json" \
    -X POST \
    "${SEMP_URL}/SEMP/v2/config/msgVpns/${SEMP_VPN}/queues" \
    -d "{
      \"queueName\": \"${queue}\",
      \"accessType\": \"exclusive\",
      \"permission\": \"consume\",
      \"ingressEnabled\": true,
      \"egressEnabled\": true
    }")

  case "${http_code}" in
    200|201)
      echo "created (HTTP ${http_code})"
      ;;
    4*)
      # Check if it already exists (SEMP error code 89 = ALREADY_EXISTS).
      local body
      body=$(curl --silent \
        -u "${SEMP_USERNAME}:${SEMP_PASSWORD}" \
        -H "Content-Type: application/json" \
        -X POST \
        "${SEMP_URL}/SEMP/v2/config/msgVpns/${SEMP_VPN}/queues" \
        -d "{
          \"queueName\": \"${queue}\",
          \"accessType\": \"exclusive\",
          \"permission\": \"consume\",
          \"ingressEnabled\": true,
          \"egressEnabled\": true
        }" 2>/dev/null || true)

      if echo "${body}" | grep -q '"code":89'; then
        echo "already exists (skipped)"
      else
        echo "FAILED (HTTP ${http_code})"
        echo "${body}" | head -5
        exit 1
      fi
      ;;
    *)
      echo "FAILED (HTTP ${http_code})"
      exit 1
      ;;
  esac
}

echo "==> Provisioning queues ..."
for q in "${QUEUES[@]}"; do
  create_queue "${q}"
done

echo
echo "==> Queue setup complete."
