#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WAYANG_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TRINO_SETUP="$SCRIPT_DIR"
TRINO_CONTAINER="trino"
MAVEN_FLAGS="-Pskip-prerequisite-check -Drat.skip=true -Dmaven.javadoc.skip=true"

banner() {
  echo
  echo "============================================================"
  printf "  %s\n" "$*"
  echo "============================================================"
  echo
}

step() {
  echo
  echo "-- $*"
  echo
}

pause() {
  if [[ "${WAYANG_DEMO_AUTO:-false}" != "true" ]]; then
    echo
    read -rp "Press ENTER to continue..." _ || true
    echo
  fi
}

run_wayang_demo() {
  "$WAYANG_ROOT/mvnw" exec:java -pl wayang-platforms/wayang-trino \
    -Dexec.mainClass="org.apache.wayang.trino.TrinoDemo" \
    ${MAVEN_FLAGS}
}

banner "ACT 1: Start Trino + Iceberg via Docker"

step "1a. Starting the stack"
cd "$TRINO_SETUP"
docker compose up -d

step "1b. Containers running"
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}" \
  | grep -E "NAMES|trino|minio|metastore|postgres"

step "1c. Waiting for Trino to be ready"
MAX_WAIT=90
ELAPSED=0
until docker exec "$TRINO_CONTAINER" \
  trino --execute "SELECT 1" --output-format ALIGNED >/dev/null 2>&1; do
  if [[ "$ELAPSED" -ge "$MAX_WAIT" ]]; then
    echo "Timed out waiting for Trino after ${MAX_WAIT}s"
    exit 1
  fi
  printf ". waiting (%ds elapsed)\r" "$ELAPSED"
  sleep 3
  ELAPSED=$((ELAPSED + 3))
done
echo "Trino is ready at http://localhost:8080"

step "1d. Initialising Iceberg tables"
docker exec -i "$TRINO_CONTAINER" trino < "$TRINO_SETUP/scripts/init.sql" 2>&1 \
  | grep -v "^WARNING\|jline\|org.jline" || true
echo "iceberg.sales.orders seeded"

step "1e. Table schema"
docker exec "$TRINO_CONTAINER" \
  trino --execute "DESCRIBE iceberg.sales.orders" \
  --output-format ALIGNED 2>/dev/null

pause

banner "ACT 2: Query Iceberg directly via Trino CLI"

step "2a. Full table scan"
echo "SQL: SELECT * FROM iceberg.sales.orders"
docker exec "$TRINO_CONTAINER" \
  trino --execute "SELECT * FROM iceberg.sales.orders ORDER BY order_id" \
  --output-format ALIGNED 2>/dev/null

step "2b. Filter: region = 'AMER'"
echo "SQL: SELECT * FROM iceberg.sales.orders WHERE region = 'AMER'"
docker exec "$TRINO_CONTAINER" \
  trino --execute "SELECT * FROM iceberg.sales.orders WHERE region = 'AMER' ORDER BY order_id" \
  --output-format ALIGNED 2>/dev/null

step "2c. Projection with filter"
echo "SQL: SELECT region, product, amount FROM iceberg.sales.orders WHERE region = 'AMER'"
docker exec "$TRINO_CONTAINER" \
  trino --execute \
    "SELECT region, product, amount
     FROM iceberg.sales.orders
     WHERE region = 'AMER'
     ORDER BY order_id" \
  --output-format ALIGNED 2>/dev/null

pause

banner "ACT 3: Wayang API filter + projection pushdown"
cd "$WAYANG_ROOT"
run_wayang_demo

banner "Demo complete"
echo "Trino UI: http://localhost:8080"
echo "MinIO UI: http://localhost:9001 (minioadmin / minioadmin)"
echo
echo "To stop the stack:"
echo "  cd trino-setup && docker compose down"
