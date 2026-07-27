#!/usr/bin/env bash
# testkit/smoke.sh — up -> wait -> assert seed -> down. Proves the Dockerized backend works.
set -euo pipefail
dir="$(dirname "$0")"
compose="docker compose -f $dir/docker-compose.yml"
cleanup() { $compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT
$compose up -d
"$dir/wait-for-pg.sh"
count=$($compose exec -T pg psql -U ferro -d ferro -tAc "select count(*) from ferro_smoke;")
echo "ferro_smoke rows: $count"
[ "$count" = "2" ] || { echo "FAIL: expected 2 seed rows, got '$count'" >&2; exit 1; }
echo "testkit smoke OK"
