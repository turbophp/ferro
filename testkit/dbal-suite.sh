#!/usr/bin/env bash
# M1-S8b: run a CURATED subset of doctrine/dbal's own functional suite against Ferro.
#
# NO `docker compose down` TRAP OF ANY KIND. testkit/smoke.sh and testkit/e2e-demo.sh both tear the
# stack down on EXIT; copying that here would destroy the databases every other suite is using.
# The only EXIT trap below kills the ferrod THIS script started and removes its socket.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${FERRO_DBAL_TAG:-4.4.4}"
pool="${FERRO_DBAL_POOL:-default}"
# The suite gets its OWN database on every family. NEVER the shared `ferro` one: this suite creates
# and abandons ~40 tables, 8+ sequences, several schemas, a domain type and views, and nothing would
# ever clean them out of a database every other live suite in this repo uses.
dsn="${FERRO_DBAL_DSN:-postgres://ferro:ferro@127.0.0.1:55432/doctrine_tests}"
# Which container to reset, and how. `--no-reset` exists for fast iteration; a RECORDED run must not
# use it (see the results file's environment manifest).
svc="${FERRO_DBAL_SVC:-pg}"
work="${FERRO_DBAL_WORK:-$root/.dbal-suite}"
src="$work/dbal-$tag"
reset=1
args=()
for a in "$@"; do
  case "$a" in
    --no-reset) reset=0 ;;
    *) args+=("$a") ;;
  esac
done

mkdir -p "$work"

# 1. The PINNED source. The packagist DIST ships `src/` only — no tests, no phpunit.xml.dist — so a
#    git clone is the only way to get the suite, and the tag must be pinned or the bar drifts.
#
#    The clone supplies `tests/` AND NOTHING ELSE. `src/`, PHPUnit and every dependency come from
#    `php/doctrine-dbal/vendor`, so the clone's own `composer install` is never run. MEASURED reason:
#    registering two Composer autoloaders in one process makes the driver package's PHPUnit 11.5.56
#    answer for classes the clone's 11.5.50 binary is executing —
#    `Call to undefined method PHPUnit\TextUI\Configuration\Source::identifyIssueTrigger()` at
#    `Runner/ErrorHandler.php:74`, before the first test. Using ONE vendor tree removes that class of
#    failure entirely, and step 1b makes the src-vs-tests version match a hard, checked precondition
#    rather than an assumption.
if [ ! -d "$src" ]; then
  git clone --depth 1 --branch "$tag" https://github.com/doctrine/dbal.git "$src"
fi

# 3. The driver package must be installed (its vendor/ is its own, and is the ONLY one this run uses).
(cd "$root/php/doctrine-dbal" && composer install --no-interaction --no-progress --quiet)

# 1b. …which means the tests come from the clone at $tag and the code under test comes from the
#     driver package's vendor. If those two versions ever diverge the suite silently tests the wrong
#     source, so assert they are equal.
installed="$(cd "$root/php/doctrine-dbal" && composer show doctrine/dbal 2>/dev/null | awk '$1=="versions" {print $NF}')"
if [ "$installed" != "$tag" ]; then
  echo "::error:: doctrine/dbal in php/doctrine-dbal/vendor is '$installed' but the test tree is pinned at '$tag'."
  echo "          The suite would run $tag's tests against $installed's source. Pin one to the other."
  exit 1
fi

# 2. The patched TestUtil, copied over the upstream one, and VERIFIED — a silently-failed patch is
#    exactly how this suite goes green against SQLite.
cp "$root/testkit/dbal/TestUtil.ferro.php" "$src/tests/TestUtil.php"
grep -q 'db_driverClass is not set' "$src/tests/TestUtil.php" \
  || { echo "::error:: TestUtil patch did not apply"; exit 1; }

# 4. ONE ferrod for the whole run — not one per test. The suite shares a single Connection across
#    every test (FunctionalTestCase::$sharedConnection), so this is the right granularity.
cargo build -p ferrod --manifest-path "$root/Cargo.toml"
sock="$(mktemp -u /tmp/ferro-dbal-XXXXXX.sock)"
env FERRO_SOCK="$sock" FERRO_POOLS="$pool" \
    "FERRO_POOL_$(echo "$pool" | tr '[:lower:]-' '[:upper:]_')_DSN=$dsn" \
    "$root/target/debug/ferrod" >"$work/ferrod.log" 2>&1 &
ferrod_pid=$!
trap 'kill "$ferrod_pid" 2>/dev/null || true; rm -f "$sock"' EXIT   # ONLY our own daemon.
for _ in $(seq 1 100); do [ -S "$sock" ] && break; sleep 0.1; done
[ -S "$sock" ] || { echo "::error:: ferrod did not create $sock"; cat "$work/ferrod.log"; exit 1; }

# 5. The phpunit config, with the allowlist expanded into <file>/<directory> entries. Generated
#    rather than committed expanded, so allowlist.txt stays the single source of truth.
cfg="$work/phpunit.generated.xml"
{
  echo '<?xml version="1.0" encoding="UTF-8"?>'
  echo '<phpunit bootstrap="'"$root"'/testkit/dbal/bootstrap.php" colors="true" cacheDirectory="'"$work"'/.phpunit.cache">'
  echo '  <testsuites><testsuite name="ferro-dbal-subset">'
  while IFS= read -r line; do
    case "$line" in ''|'#'*) continue ;; esac
    if [ -d "$src/$line" ]; then echo "    <directory>$src/$line</directory>"
    else echo "    <file>$src/$line</file>"; fi
  done < "$root/testkit/dbal/allowlist.txt"
  echo '  </testsuite></testsuites>'
  echo '  <php>'
  echo '    <var name="db_driverClass" value="Ferro\DBAL\Driver"/>'
  echo '    <var name="db_unix_socket" value="'"$sock"'"/>'
  echo '    <var name="db_driver_options" value="{&quot;pool&quot;:&quot;'"$pool"'&quot;}"/>'
  echo '  </php>'
  echo '</phpunit>'
} > "$cfg"

# 6. THE RESET — the suite's only source of idempotence, and a hard precondition of recording a
#    number. Upstream gets it from TestUtil::initializeDatabase()'s dropDatabase/createDatabase,
#    which Ferro structurally cannot do (PHP holds no credentials, SPEC §12/D8), so it happens
#    container-side with no PHP credentials at all — the same shape as the MySQL grant.
#
#    MEASURED, against a KNOWN-GOOD driver, with no reset: the same command gave `Errors 23,
#    Failures 3` and then `Errors 33, Failures 1`; with upstream's TestUtil it gave 0/0 before and
#    after. A number that degrades on every run is worse than no number, because the triage table
#    then blames the driver for leftover state.
if [ "$reset" = 1 ]; then
  case "$svc" in
    pg)
      docker compose -f "$root/testkit/docker-compose.yml" exec -T pg \
        psql -v ON_ERROR_STOP=1 -U ferro -d doctrine_tests -q < "$root/testkit/dbal/reset-pg.sql"
      echo "[ferro] reset: pg/doctrine_tests from testkit/dbal/reset-pg.sql"
      ;;
    mysql)
      docker compose -f "$root/testkit/docker-compose.yml" exec -T mysql \
        mysql -uroot -pferro < "$root/testkit/dbal/reset-mysql.sql" 2>&1 | grep -v 'Using a password' || true
      echo "[ferro] reset: mysql/doctrine_tests from testkit/dbal/reset-mysql.sql"
      ;;
    mariadb)
      # The MariaDB image ships `mariadb`, not `mysql`, as the client binary.
      docker compose -f "$root/testkit/docker-compose.yml" exec -T mariadb \
        mariadb -uroot -pferro < "$root/testkit/dbal/reset-mysql.sql" 2>&1 | grep -v 'Using a password' || true
      echo "[ferro] reset: mariadb/doctrine_tests from testkit/dbal/reset-mysql.sql"
      ;;
    *) echo "::error:: unknown FERRO_DBAL_SVC=$svc"; exit 1 ;;
  esac
else
  echo "[ferro] reset: SKIPPED (--no-reset) — this run's numbers MUST NOT be recorded"
fi

# 7. Run it, with the DRIVER package's phpunit (see step 1 — one vendor tree, no version collision).
#    The bootstrap's contact assertion runs first and exits non-zero if the connection is not a
#    Ferro one.
FERRO_DBAL_SRC="$src" "$root/php/doctrine-dbal/vendor/bin/phpunit" -c "$cfg" "${args[@]+"${args[@]}"}"
