#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# wayang-service: Process Watchdog Script
#
# Usage: wayang-service {start|stop|restart|status}
#

# Get the absolute path of the script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="$SCRIPT_DIR/pid"
PID_FILE="$PID_DIR/wayang-service.pid"
LOG_DIR="$SCRIPT_DIR/logs"
EXECUTABLE="$SCRIPT_DIR/wayang-submit"
ARGS="org.apache.wayang.api.json.Main"
SLEEP_INTERVAL=5

# Ensure necessary directories exist
mkdir -p "$PID_DIR" "$LOG_DIR"

start_service() {
  if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" >/dev/null 2>&1; then
      echo "Wayang-service is already running with PID $PID."
      exit 1
    else
      echo "Removing stale PID file."
      rm -f "$PID_FILE"
    fi
  fi

  echo "Starting Wayang-service..."
  nohup "$SCRIPT_DIR/$(basename "$0")" run >> "$LOG_DIR/wayang-$(date +'%Y-%m-%d_%H-%M-%S').log" 2>&1 &
  PID=$!
  echo $PID > "$PID_FILE"
  echo "Wayang-service started with PID $PID."
}

run_service() {
  while true; do
    # Define a new log file for each process restart
    SUB_LOG_FILE="$LOG_DIR/wayang-$(date +'%Y-%m-%d_%H-%M-%S').log"
    echo "Starting subprocess; logging to $SUB_LOG_FILE"
    "$EXECUTABLE" $ARGS >> "$SUB_LOG_FILE" 2>&1 &
    CHILD_PID=$!
    echo "Subprocess started with PID $CHILD_PID."

    # Wait for the process to exit
    wait $CHILD_PID
    EXIT_CODE=$?
    echo "Subprocess (PID $CHILD_PID) terminated with exit code $EXIT_CODE."
    echo "Restarting subprocess in $SLEEP_INTERVAL seconds..."
    sleep $SLEEP_INTERVAL
  done
}

stop_service() {
  if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" >/dev/null 2>&1; then
      echo "Stopping Wayang-service (PID $PID)..."
      kill "$PID"
      sleep 1
      rm -f "$PID_FILE"
      echo "Wayang-service stopped."
    else
      echo "No process found with PID $PID. Removing stale PID file."
      rm -f "$PID_FILE"
    fi
  else
    echo "Wayang-service is not running."
  fi
}

status_service() {
  if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" >/dev/null 2>&1; then
      echo "Wayang-service is running with PID $PID."
    else
      echo "PID file exists, but no process with PID $PID is running."
    fi
  else
    echo "Wayang-service is not running."
  fi
}

case "$1" in
  start)
    start_service
    ;;
  run)
    run_service
    ;;
  stop)
    stop_service
    ;;
  restart)
    stop_service
    sleep 1
    start_service
    ;;
  status)
    status_service
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
    ;;
esac
