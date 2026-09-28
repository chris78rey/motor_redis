#!/bin/bash
set -euo pipefail

month=$(TZ=America/Guayaquil date +%Y%m)
exec /usr/bin/curl --fail --silent --show-error --max-time 60 \
  --request POST \
  "http://127.0.0.1:8091/api/reportes/hojas/recalcular?desde=${month}&hasta=${month}"
