#!/usr/bin/env bash
# ci/local-gate.sh — run every charter Definition-of-Done gate locally.
# Offline by default (integration tests skip without FERRO_TEST_PG_URL).
# --with-pg brings up the Dockerized backend and exports FERRO_TEST_PG_URL for the run.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
with_pg=0; [ "${1:-}" = "--with-pg" ] && with_pg=1

if [ "$with_pg" = 1 ]; then
  docker compose -f testkit/docker-compose.yml up -d
  ./testkit/wait-for-pg.sh
  export FERRO_TEST_PG_URL="postgres://ferro:ferro@localhost:55432/ferro"
  trap 'docker compose -f testkit/docker-compose.yml down -v >/dev/null 2>&1 || true' EXIT
fi

echo "== rust: fmt =="   ; cargo fmt --check
echo "== rust: clippy ==" ; cargo clippy --workspace -- -D warnings
echo "== rust: test =="   ; cargo test --workspace
echo "== php: install ==" ; (cd php/client && composer install --no-interaction --quiet)
echo "== php: phpunit ==" ; (cd php/client && ./vendor/bin/phpunit)
echo "== php: phpstan ==" ; (cd php/client && ./vendor/bin/phpstan analyse src --level 9)
echo "ALL GATES GREEN"
