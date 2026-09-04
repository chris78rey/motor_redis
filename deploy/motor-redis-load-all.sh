#!/bin/bash
set -euo pipefail

exec 9>/run/user/1000/motor-redis-load-all.lock
flock -n 9 || exit 0

/usr/bin/curl --fail --silent --show-error --max-time 60 \
  --request POST \
  http://127.0.0.1:8091/api/cache/load-all
echo
