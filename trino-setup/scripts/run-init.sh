#!/bin/bash
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
