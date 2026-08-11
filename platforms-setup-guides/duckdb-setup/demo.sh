#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WAYANG_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DB_FILE="$SCRIPT_DIR/data/wayang.duckdb"
MAVEN_FLAGS="-Pskip-prerequisite-check -Drat.skip=true -Dlicense.skip=true"

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

banner "ACT 1: Create a local DuckDB file with Docker"

step "1a. Running the DuckDB CLI container"
mkdir -p "$SCRIPT_DIR/data"
docker compose -f "$SCRIPT_DIR/docker-compose.yml" run --rm duckdb

step "1b. Inspecting the local database file"
docker run --rm -i \
  -v "$SCRIPT_DIR:/workspace" \
  duckdb/duckdb:1.5.5 \
  duckdb /workspace/data/wayang.duckdb < "$SCRIPT_DIR/scripts/check.sql"

pause

banner "ACT 2: Run Wayang DuckDB tests against that file"

cd "$WAYANG_ROOT"
./mvnw ${MAVEN_FLAGS} -pl wayang-platforms/wayang-duckdb -am \
  -Dtest=DuckDBOperatorsIT \
  -Dduckdb.url="jdbc:duckdb:$DB_FILE" \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false \
  test

pause

banner "ACT 3: Run DuckDB Parquet and GCS tests"

./mvnw ${MAVEN_FLAGS} -pl wayang-platforms/wayang-duckdb -am \
  -Dtest=DuckDBParquetSourceIT \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false \
  test

pause

banner "ACT 4: Run a DuckDB cost-profiling smoke"

./mvnw ${MAVEN_FLAGS} -pl wayang-platforms/wayang-duckdb -am \
  -Dtest=DuckDBCostPilotIT#runPilot \
  -Dduckdb.profile.rowCounts=100,1000 \
  -Dduckdb.profile.plans=S01,S02 \
  -Dduckdb.profile.repetitions=2 \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false \
  test

pause

banner "ACT 5: Run the standalone DuckDB setup integration tests"

./mvnw -f platforms-setup-guides/duckdb-setup/pom.xml \
  -Pintegration \
  -Dtest=DuckDBIntegrationTest \
  -Dduckdb.url="jdbc:duckdb:$DB_FILE" \
  test

pause

banner "ACT 6: Run the Wayang DuckDB demo"

./mvnw ${MAVEN_FLAGS} -pl wayang-platforms/wayang-duckdb \
  -DskipTests \
  exec:java \
  -Dexec.mainClass=org.apache.wayang.duckdb.DuckDBDemo \
  -Dduckdb.url="jdbc:duckdb:$DB_FILE"

banner "Demo complete"
echo "DuckDB file: $DB_FILE"
