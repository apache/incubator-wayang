#!/usr/bin/env bash

set -euo pipefail

WAYANG_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$WAYANG_ROOT/trino-setup/demo.sh" "$@"
