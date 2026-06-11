#!/usr/bin/env bash

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
