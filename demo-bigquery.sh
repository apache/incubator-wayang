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

WAYANG_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIVE_MODE=false
[[ "${1:-}" == "--live" ]] && LIVE_MODE=true

BQ_PROJECT="${BQ_PROJECT:-my-project}"
BQ_URL="${BQ_URL:-}"
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

run_demo_class() {
  local main_class="$1"
  shift
  cd "$WAYANG_ROOT"
  "$WAYANG_ROOT/mvnw" exec:java -pl wayang-platforms/wayang-bigquery \
    -Dexec.mainClass="$main_class" \
    "$@" \
    ${MAVEN_FLAGS} -q 2>/dev/null || true
}

banner "ACT 1: BigQuery cost model"
step "Read cost settings from wayang-bigquery-defaults.properties"
run_demo_class "org.apache.wayang.bigquery.BigQueryDemo" \
  "-Dbigquery.mode=cost" \
  "-Dbigquery.project=${BQ_PROJECT}"

pause

banner "ACT 2: BigQuery filter operator"
if [[ "$LIVE_MODE" == true && -n "$BQ_URL" ]]; then
  run_demo_class "org.apache.wayang.bigquery.BigQueryDemo" \
    "-Dbigquery.mode=filter" \
    "-Dbigquery.url=${BQ_URL}" \
    "-Dbigquery.project=${BQ_PROJECT}"
else
  run_demo_class "org.apache.wayang.bigquery.BigQueryDemo" \
    "-Dbigquery.mode=filter" \
    "-Dbigquery.project=${BQ_PROJECT}"
fi

pause

banner "ACT 3: BigQuery projection operator"
if [[ "$LIVE_MODE" == true && -n "$BQ_URL" ]]; then
  run_demo_class "org.apache.wayang.bigquery.BigQueryDemo" \
    "-Dbigquery.mode=projection" \
    "-Dbigquery.url=${BQ_URL}" \
    "-Dbigquery.project=${BQ_PROJECT}"
else
  run_demo_class "org.apache.wayang.bigquery.BigQueryDemo" \
    "-Dbigquery.mode=projection" \
    "-Dbigquery.project=${BQ_PROJECT}"
fi

banner "Demo complete"
