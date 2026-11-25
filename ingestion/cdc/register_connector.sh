#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${DEBEZIUM_CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/debezium_outbox_connector.json"

echo "Registering Debezium Outbox Connector at ${CONNECT_URL}..."
curl -fsS -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  --data @"${CONFIG_FILE}"
echo ""
echo "Connector registration complete."
