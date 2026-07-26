#!/usr/bin/env sh
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

python - <<'PY'
import os
import socket
import sys
import time
import urllib.parse

base_url = os.environ.get("WAYANG_API_URL")
if base_url is not None:
    parsed_url = urllib.parse.urlparse(base_url)
    host = parsed_url.hostname
    port = parsed_url.port or 80
else:
    host = os.environ.get("WAYANG_API_HOST", "localhost")
    port = int(os.environ.get("WAYANG_API_PORT", "8080"))

deadline = time.time() + int(os.environ.get("WAYANG_API_WAIT_TIMEOUT", "60"))
while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=2):
            pass
        break
    except Exception:
        time.sleep(1)
else:
    print(f"Timed out waiting for Wayang REST API at {host}:{port}", file=sys.stderr)
    sys.exit(1)
PY

exec "$@"
