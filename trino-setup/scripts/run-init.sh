#!/bin/bash
# Runs init.sql against the local Trino instance.
# The stack must be fully up before running this.

set -e

TRINO_HOST=${TRINO_HOST:-localhost}
TRINO_PORT=${TRINO_PORT:-8080}

echo "Waiting for Trino to be ready..."
until curl -sf "http://${TRINO_HOST}:${TRINO_PORT}/v1/info" | grep -q '"starting":false'; do
  echo "  Trino not ready yet, retrying in 5s..."
  sleep 5
done
echo "Trino is ready."

echo "Running init.sql..."
docker exec -i trino trino \
  --server "http://${TRINO_HOST}:${TRINO_PORT}" \
  --user admin \
  < "$(dirname "$0")/init.sql"

echo "Done. Sample Iceberg data loaded into iceberg.sales.orders"
