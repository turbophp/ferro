#!/usr/bin/env bash
# testkit/wait-for-pg.sh — block until the pg service reports healthy (or time out).
set -euo pipefail
compose="docker compose -f $(dirname "$0")/docker-compose.yml"
for _ in $(seq 1 60); do
  status=$($compose ps --format '{{.Health}}' pg 2>/dev/null || true)
  [ "$status" = "healthy" ] && { echo "pg healthy"; exit 0; }
  sleep 1
done
echo "pg did not become healthy in time" >&2
$compose logs pg >&2 || true
exit 1
