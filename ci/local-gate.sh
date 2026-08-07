#!/usr/bin/env bash
# ci/local-gate.sh — run every charter Definition-of-Done gate locally.
#
# It must never be WEAKER than .github/workflows/ci.yml, or "ALL GATES GREEN" is a lie a developer
# then pushes. Through M1-S7 it was: it exported only FERRO_TEST_PG_URL (so the entire
# MySQL/MariaDB tier silently skipped), ran `cargo clippy --workspace` WITHOUT `--all-targets` (so
# the live acceptance gates in tests/ were never linted), never ran the PHP live tier, and had no
# skip detector at all.
#
# Offline by default (every live suite skips without its FERRO_TEST_*_URL).
# `--live` (alias: the legacy `--with-pg`) brings up ALL THREE backends, waits on their compose
# healthchecks, exports all three DSNs, builds `ferrod` for the PHP live tier, and then REFUSES to
# report green if any suite skipped.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
live=0
case "${1:-}" in
  --live|--with-pg) live=1 ;;
  "") ;;
  *) echo "usage: $0 [--live]" >&2; exit 2 ;;
esac

if [ "$live" = 1 ]; then
  # Same compose file, same digest-pinned images, same healthchecks as CI — and the same explicit
  # service list, so the `ferrod` sidecar image is not rebuilt for a test run.
  docker compose -f testkit/docker-compose.yml up -d --wait pg mysql mariadb
  # Register teardown BEFORE anything that can fail, so a backend that never becomes healthy under
  # `set -e` still tears the compose stack down instead of leaking it.
  trap 'docker compose -f testkit/docker-compose.yml down -v >/dev/null 2>&1 || true' EXIT
  export FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro"
  export FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro"
  export FERRO_TEST_MARIADB_URL="mysql://ferro:ferro@127.0.0.1:33061/ferro"
fi

echo "== rust: fmt =="   ; cargo fmt --check
# `--all-targets` is load-bearing, not cosmetic: without it clippy never lints tests, benches or
# examples — which is exactly where the live acceptance gates live. CI has it; so must this.
echo "== rust: clippy ==" ; cargo clippy --workspace --all-targets -- -D warnings
echo "== rust: test =="
if [ "$live" = 1 ]; then
  set -o pipefail
  cargo test --workspace -- --nocapture 2>&1 | tee "$root/live.log"
  # THE no-op detector, shared verbatim with the CI integration lane.
  echo "== rust: no-skip gate ==" ; ./ci/assert-no-skips.sh "$root/live.log"
  rm -f "$root/live.log"
else
  cargo test --workspace
fi
echo "== php: install ==" ; (cd php/client && composer install --no-interaction --quiet)
if [ "$live" = 1 ]; then
  # LiveTestCase spawns a real ferrod per test and looks for target/debug/ferrod.
  echo "== php: build ferrod ==" ; cargo build -p ferrod
fi
echo "== php: phpunit ==" ; (cd php/client && ./vendor/bin/phpunit)
if [ "$live" = 1 ]; then
  echo "== php: phpunit (live, skips fatal) =="
  (cd php/client && ./vendor/bin/phpunit tests/Live --fail-on-skipped)
fi
echo "== php: phpstan ==" ; (cd php/client && ./vendor/bin/phpstan analyse src --level 9)
echo "== d12 gate ==" ; ./ci/check-d12-recorded.sh
if [ "$live" = 1 ]; then echo "ALL GATES GREEN (live: pg + mysql + mariadb + php live tier)"
else echo "ALL GATES GREEN (OFFLINE — live suites skipped; re-run with --live before pushing)"; fi
