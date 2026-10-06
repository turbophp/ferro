#!/usr/bin/env bash
# M1-S8b: run a CURATED subset of doctrine/dbal's own functional suite against Ferro.
#
# NO `docker compose down` TRAP OF ANY KIND. testkit/smoke.sh and testkit/e2e-demo.sh both tear the
# stack down on EXIT; copying that here would destroy the databases every other suite is using.
# The only EXIT trap below kills the ferrod THIS script started and removes its socket.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${FERRO_DBAL_TAG:-4.4.4}"
# THE MAJOR (M2-C5b) is derived from the pinned tag, never configured beside it, so the tests and
# the code under test cannot name different majors. Everything major-specific hangs off it: the
# driver package's VENDOR TREE (DBAL 3 lives in vendor-dbal3/, installed from composer.dbal3.json —
# the package's own DBAL 3 lane), the replacement TestUtil (3.10.6's public surface differs from
# 4.4.4's), and the driverClass.
major="${tag%%.*}"
pkg="$root/php/doctrine-dbal"
case "$major" in
  4) vendor="$pkg/vendor";       composer_file=composer.json;       testutil=TestUtil.ferro.php;       driver_class='Ferro\DBAL\Driver' ;;
  3) vendor="$pkg/vendor-dbal3"; composer_file=composer.dbal3.json; testutil=TestUtil.ferro.dbal3.php; driver_class='Ferro\DBAL\Dbal3\Driver' ;;
  *) echo "::error:: FERRO_DBAL_TAG=$tag names DBAL major $major; the driver serves 3 and 4"; exit 1 ;;
esac
pool="${FERRO_DBAL_POOL:-default}"
# Which container to reset, and how. `--no-reset` exists for fast iteration; a RECORDED run must not
# use it (see the results file's environment manifest).
#
# `sqlite` is the odd one out in every direction, and the differences are listed once here rather
# than rediscovered at each branch below: there is NO container (so the reset is a file delete, not
# a `docker compose exec`), the reset must happen BEFORE ferrod opens the file rather than after it
# is running, and the whole column therefore runs on a box with no database server at all — the
# first of the three that does.
svc="${FERRO_DBAL_SVC:-pg}"
# THE CONTROL COLUMN (`FERRO_DBAL_CONTROL=1`): the identical tests against the identical database,
# run through upstream's OWN `pdo_sqlite` / `pdo_pgsql` / `pdo_mysql` with no Ferro anywhere — no
# daemon is started and no socket is passed. It is what turns "28 non-passes" into an attribution
# instead of a guess, and `bootstrap.php`'s contact assertion INVERTS for it (it refuses to run if
# the connection turns out to be a Ferro one). It is the C2e shape (SPEC §22.2 (ar)).
#
# The server families' control takes its credentials from `FERRO_DBAL_DSN` and writes them into the
# GENERATED phpunit config (M2-C5b). That is not the §12/D8 boundary being crossed: D8 keeps
# credentials out of the PRODUCT's PHP, and the control column is a harness that exists precisely to
# measure the product against the stock driver, which cannot connect without them — the Laravel
# runner's `stock-pgsql` column has done the same since C2e. It was added so the rule SPEC §22.2 (bz)
# records — compare a column's SKIP set against a control, not only its failures — can be followed
# from the repo for every family rather than with an ad-hoc config.
control="${FERRO_DBAL_CONTROL:-0}"
work="${FERRO_DBAL_WORK:-$root/.dbal-suite}"
# The SQLite column's database file. Inside $work so it is discarded with the rest of the scratch
# tree, and named for the suite so no other lane can be pointed at it by accident.
sqlite_db="${FERRO_DBAL_SQLITE_DB:-$work/doctrine_tests.sqlite}"
# The suite gets its OWN database on every family. NEVER the shared `ferro` one: this suite creates
# and abandons ~40 tables, 8+ sequences, several schemas, a domain type and views, and nothing would
# ever clean them out of a database every other live suite in this repo uses. On SQLite "its own
# database" is free — a file nothing else opens.
if [ "$svc" = sqlite ]; then
  dsn="${FERRO_DBAL_DSN:-sqlite://$sqlite_db}"
else
  dsn="${FERRO_DBAL_DSN:-postgres://ferro:ferro@127.0.0.1:55432/doctrine_tests}"
fi
# The control's stock driver, and — for a server family — the DSN taken apart into the five
# parameters `TestUtil` reads. Refused rather than guessed when the DSN does not parse: a control
# that quietly connected somewhere else would measure the wrong database.
ctl_driver=""
if [ "$control" = 1 ]; then
  case "$svc" in
    sqlite) ctl_driver=pdo_sqlite ;;
    pg|psql) ctl_driver=pdo_pgsql ;;
    mysql|mariadb|mysql-local) ctl_driver=pdo_mysql ;;
    *) echo "::error:: FERRO_DBAL_CONTROL=1 has no stock driver for FERRO_DBAL_SVC=$svc"; exit 1 ;;
  esac
  if [ "$svc" != sqlite ]; then
    re='^[a-z]+://([^:@/]+):([^@/]*)@([^:/]+):([0-9]+)/([^/?]+)$'
    if [[ ! "$dsn" =~ $re ]]; then
      echo "::error:: the control needs FERRO_DBAL_DSN as scheme://user:password@host:port/dbname"
      exit 1
    fi
    ctl_user="${BASH_REMATCH[1]}" ctl_pass="${BASH_REMATCH[2]}" ctl_host="${BASH_REMATCH[3]}"
    ctl_port="${BASH_REMATCH[4]}" ctl_db="${BASH_REMATCH[5]}"
  fi
fi
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

# 3. The driver package must be installed (its vendor tree for THIS major is the ONLY one this run
#    uses — vendor/ for DBAL 4, vendor-dbal3/ for DBAL 3).
(cd "$pkg" && COMPOSER="$composer_file" composer install --no-interaction --no-progress --quiet)

# 1b. …which means the tests come from the clone at $tag and the code under test comes from the
#     driver package's vendor. If those two versions ever diverge the suite silently tests the wrong
#     source, so assert they are equal.
installed="$(cd "$pkg" && COMPOSER="$composer_file" composer show doctrine/dbal 2>/dev/null | awk '$1=="versions" {print $NF}')"
if [ "$installed" != "$tag" ]; then
  echo "::error:: doctrine/dbal in ${vendor#$root/} is '$installed' but the test tree is pinned at '$tag'."
  echo "          The suite would run $tag's tests against $installed's source. Pin one to the other."
  exit 1
fi

# 2. The patched TestUtil, copied over the upstream one, and VERIFIED — a silently-failed patch is
#    exactly how this suite goes green against SQLite.
cp "$root/testkit/dbal/$testutil" "$src/tests/TestUtil.php"
grep -q 'neither db_driverClass nor db_driver is set' "$src/tests/TestUtil.php" \
  || { echo "::error:: TestUtil patch did not apply"; exit 1; }

# THE RESET — defined here, CALLED at step 4 (SQLite) or step 6 (the server families).
#    It is the suite's only source of idempotence, and a hard precondition of recording a
#    number. Upstream gets it from TestUtil::initializeDatabase()'s dropDatabase/createDatabase,
#    which Ferro structurally cannot do (PHP holds no credentials, SPEC §12/D8), so it happens
#    container-side with no PHP credentials at all — the same shape as the MySQL grant.
#
#    MEASURED, against a KNOWN-GOOD driver, with no reset: the same command gave `Errors 23,
#    Failures 3` and then `Errors 33, Failures 1`; with upstream's TestUtil it gave 0/0 before and
#    after. A number that degrades on every run is worse than no number, because the triage table
#    then blames the driver for leftover state.
#
#    It is a FUNCTION rather than a step because WHEN it runs is family-dependent. On the server
#    families it runs after ferrod is up — the reset talks to the container, not to the daemon, and
#    dropping schemas under a pool's idle connections is harmless there. On SQLite the "database"
#    is a file that ferrod's pool OPENS, and deleting an open SQLite database out from under a live
#    connection is exactly the corruption SQLite's own docs warn about, so that column resets first
#    and starts the daemon afterwards. Calling it at the wrong moment would not fail loudly, which
#    is why the call sites are explicit rather than one line at the bottom.
do_reset() {
  if [ "$reset" != 1 ]; then
    echo "[ferro] reset: SKIPPED (--no-reset) — this run's numbers MUST NOT be recorded"
    return 0
  fi
  case "$svc" in
    pg)
      docker compose -f "$root/testkit/docker-compose.yml" exec -T pg \
        psql -v ON_ERROR_STOP=1 -U ferro -d doctrine_tests -q < "$root/testkit/dbal/reset-pg.sql"
      echo "[ferro] reset: pg/doctrine_tests from testkit/dbal/reset-pg.sql"
      ;;
    # MYSQL_PWD rather than `-p`, so there is no password warning to filter. The previous
    # `2>&1 | grep -v 'Using a password' || true` swallowed the CLIENT's failure along with grep's
    # exit status, so a reset that never ran still printed the success line below (found by the
    # M2-C1f review on the Laravel runner, which had copied it from here).
    mysql)
      docker compose -f "$root/testkit/docker-compose.yml" exec -T -e MYSQL_PWD=ferro mysql \
        mysql -uroot < "$root/testkit/dbal/reset-mysql.sql"
      echo "[ferro] reset: mysql/doctrine_tests from testkit/dbal/reset-mysql.sql"
      ;;
    mariadb)
      # The MariaDB image ships `mariadb`, not `mysql`, as the client binary.
      docker compose -f "$root/testkit/docker-compose.yml" exec -T -e MYSQL_PWD=ferro mariadb \
        mariadb -uroot < "$root/testkit/dbal/reset-mysql.sql"
      echo "[ferro] reset: mariadb/doctrine_tests from testkit/dbal/reset-mysql.sql"
      ;;
    # LOCAL clients (E9), for a dev box with the servers but no Docker daemon: the Laravel runner's
    # `psql`/`mysql-local` arms — the same SQL against the same database, only the client binary's
    # location differs. They exist so a column can be measured locally WITH its reset rather than
    # under `--no-reset`, which is exactly how a number stops being reproducible. The credentials
    # are the DSN the shell already holds for ferrod's own config, never PHP's (SPEC §12/D8).
    psql)
      psql -v ON_ERROR_STOP=1 -q -d "$dsn" -f "$root/testkit/dbal/reset-pg.sql"
      echo "[ferro] reset: local psql against \$FERRO_DBAL_DSN from testkit/dbal/reset-pg.sql"
      ;;
    # The container reset runs as root because it re-GRANTs. The suite's own user cannot GRANT and
    # does not need to: MySQL and MariaDB keep a database-level grant by NAME (`mysql.db`), so the
    # grant testkit/mysql-init.sql made survives the DROP (testkit/laravel/reset-mysql.sql relies on
    # the same fact). So the local arm runs the identical file minus its GRANT/FLUSH lines.
    mysql-local)
      re='^[a-z]+://([^:@/]+):([^@/]*)@([^:/]+):([0-9]+)/'
      [[ "$dsn" =~ $re ]] || { echo "::error:: cannot parse FERRO_DBAL_DSN for the local reset"; exit 1; }
      grep -vE '^(GRANT|FLUSH) ' "$root/testkit/dbal/reset-mysql.sql" \
        | MYSQL_PWD="${BASH_REMATCH[2]}" mysql -u"${BASH_REMATCH[1]}" -h"${BASH_REMATCH[3]}" -P"${BASH_REMATCH[4]}"
      echo "[ferro] reset: local mysql against \$FERRO_DBAL_DSN from testkit/dbal/reset-mysql.sql"
      ;;
    sqlite)
      # Deleting the file IS the drop-and-create, and it is strictly more thorough than either SQL
      # reset: no schema, sequence, view or leftover row can survive it. The `-wal` and `-shm`
      # sidecars must go WITH it — the pool opens every connection in WAL mode (C3-3a verifies the
      # pragma rather than requesting it), and a stale WAL left beside a deleted database is how a
      # "reset" silently restores the rows it was meant to remove.
      rm -f "$sqlite_db" "$sqlite_db-wal" "$sqlite_db-shm"
      mkdir -p "$(dirname "$sqlite_db")"
      echo "[ferro] reset: sqlite $sqlite_db removed (with -wal/-shm)"
      ;;
    *) echo "::error:: unknown FERRO_DBAL_SVC=$svc"; exit 1 ;;
  esac
}

# 4. ONE ferrod for the whole run — not one per test. The suite shares a single Connection across
#    every test (FunctionalTestCase::$sharedConnection), so this is the right granularity.
#    SQLite resets FIRST: the daemon below opens the database file, and it must open a fresh one.
if [ "$svc" = sqlite ]; then
  do_reset
fi
sock=""
if [ "$control" = 1 ]; then
  # No daemon, no socket, no cargo build. The control must not merely AVOID using Ferro — it must
  # have no Ferro to use, so a mis-set variable cannot quietly route through one.
  if [ "$svc" = sqlite ]; then
    echo "[control] no ferrod started; pdo_sqlite will open $sqlite_db directly"
  else
    echo "[control] no ferrod started; $ctl_driver will connect to $ctl_host:$ctl_port/$ctl_db directly"
  fi
else
  cargo build -p ferrod --manifest-path "$root/Cargo.toml"
  sock="$(mktemp -u /tmp/ferro-dbal-XXXXXX.sock)"
  # `CARGO_TARGET_DIR` is honoured (E9): the build above writes there when it is set, and starting
  # `$root/target/debug/ferrod` instead would run whatever STALE binary an earlier build left — a
  # column measuring code other than the tree under test, with nothing to say so.
  env FERRO_SOCK="$sock" FERRO_POOLS="$pool" \
      "FERRO_POOL_$(echo "$pool" | tr '[:lower:]-' '[:upper:]_')_DSN=$dsn" \
      "${CARGO_TARGET_DIR:-$root/target}/debug/ferrod" >"$work/ferrod.log" 2>&1 &
  ferrod_pid=$!
  trap 'kill "$ferrod_pid" 2>/dev/null || true; rm -f "$sock"' EXIT   # ONLY our own daemon.
  for _ in $(seq 1 100); do [ -S "$sock" ] && break; sleep 0.1; done
  [ -S "$sock" ] || { echo "::error:: ferrod did not create $sock"; cat "$work/ferrod.log"; exit 1; }
fi

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
  if [ "$control" = 1 ]; then
    # `driver`/`path` and NOTHING else: TestUtil refuses a run that sets both `driver` and
    # `driverClass`, so the two columns cannot be blended by accident.
    echo '    <var name="db_driver" value="'"$ctl_driver"'"/>'
    if [ "$svc" = sqlite ]; then
      echo '    <var name="db_path" value="'"$sqlite_db"'"/>'
    else
      echo '    <var name="db_host" value="'"$ctl_host"'"/>'
      echo '    <var name="db_port" value="'"$ctl_port"'"/>'
      echo '    <var name="db_user" value="'"$ctl_user"'"/>'
      echo '    <var name="db_password" value="'"$ctl_pass"'"/>'
      echo '    <var name="db_dbname" value="'"$ctl_db"'"/>'
    fi
  else
    echo '    <var name="db_driverClass" value="'"$driver_class"'"/>'
    echo '    <var name="db_unix_socket" value="'"$sock"'"/>'
    echo '    <var name="db_driver_options" value="{&quot;pool&quot;:&quot;'"$pool"'&quot;}"/>'
  fi
  echo '  </php>'
  echo '</phpunit>'
} > "$cfg"

# 6. THE RESET, for the families whose reset talks to a container. SQLite's already ran, above the
#    daemon launch — see `do_reset`.
if [ "$svc" != sqlite ]; then
  do_reset
fi

# 7. Run it, with the DRIVER package's phpunit (see step 1 — one vendor tree, no version collision).
#    The bootstrap's contact assertion runs first and exits non-zero if the connection is not a
#    Ferro one.
FERRO_DBAL_SRC="$src" FERRO_DBAL_CONTROL="$control" FERRO_DBAL_VENDOR="$vendor" \
  "$vendor/bin/phpunit" -c "$cfg" "${args[@]+"${args[@]}"}"
