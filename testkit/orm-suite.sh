#!/usr/bin/env bash
# M2: run upstream doctrine/orm's FUNCTIONAL suite through Ferro's DBAL driver, against a stock-PDO
# control (SPEC §14 acceptance; §22.2 (ci)).
#
#   FERRO_ORM_SVC       pg | psql (local PostgreSQL) | mysql | mariadb | mysql-local
#   FERRO_ORM_CONTROL   1 = upstream's own pdo_pgsql/pdo_mysql, no Ferro anywhere (the contact
#                       assertion INVERTS and refuses a Ferro connection)
#   FERRO_ORM_SEQUENCE  1 = the old D-S8b-5 remedy as a labelled harness change: PostgreSQL identity
#                       generation prefers SEQUENCE. Under DBAL 4 the ORM maps AUTO to IDENTITY on
#                       PostgreSQL, which needs lastInsertId() — answered inside a transaction since
#                       §22.2 (ci). The STOCK-config column is the one that measures config-only
#                       adoption; this one is kept to show the two configurations agree.
#   FERRO_ORM_SRC       an existing prepared clone to reuse (skips clone + composer)
#
# NO `docker compose down` TRAP OF ANY KIND — the only EXIT trap kills the ferrod THIS script
# started (the testkit/dbal-suite.sh rule).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${FERRO_ORM_TAG:-3.7.3}"
svc="${FERRO_ORM_SVC:-pg}"
control="${FERRO_ORM_CONTROL:-0}"
sequence="${FERRO_ORM_SEQUENCE:-0}"
db="${FERRO_ORM_DB:-orm_tests}"
work="${FERRO_ORM_WORK:-$root/.orm-suite}"
src="${FERRO_ORM_SRC:-$work/orm-$tag}"
mkdir -p "$work"

case "$svc" in
  pg|psql) dsn="postgres://ferro:ferro@127.0.0.1:55432/$db"; ctl_driver=pdo_pgsql; port=55432 ;;
  mysql|mysql-local) dsn="mysql://ferro:ferro@127.0.0.1:33060/$db"; ctl_driver=pdo_mysql; port=33060 ;;
  mariadb) dsn="mysql://ferro:ferro@127.0.0.1:33061/$db"; ctl_driver=pdo_mysql; port=33061 ;;
  *) echo "::error:: unknown FERRO_ORM_SVC=$svc (pg | psql | mysql | mariadb | mysql-local)"; exit 1 ;;
esac

# 1. The PINNED source and its own vendor tree (unless a prepared clone is supplied).
if [ -z "${FERRO_ORM_SRC:-}" ]; then
  if [ ! -d "$src" ]; then
    git clone --depth 1 --branch "$tag" https://github.com/doctrine/orm.git "$src"
  fi
  # doctrine/dbal at the version the driver's own lock carries.
  dbal="$(php -r '$l=json_decode(file_get_contents($argv[1]),true); foreach($l["packages"] as $p){ if($p["name"]==="doctrine/dbal"){ echo ltrim($p["version"],"v"); } }' "$root/php/doctrine-dbal/composer.lock")"
  [ -n "$dbal" ] || { echo "::error:: cannot read doctrine/dbal's version from php/doctrine-dbal/composer.lock"; exit 1; }
  (cd "$src" && git checkout -q -- composer.json)
  php "$root/testkit/orm/prepare-composer.php" "$src" "$root" "$dbal"
  # Resolve once per prepared composer.json, not once per run (a lane runs the suite 8 times).
  stamp="$(sha256sum "$src/composer.json" | cut -d' ' -f1)"
  if [ ! -f "$src/vendor/.ferro-stamp" ] || [ "$(cat "$src/vendor/.ferro-stamp")" != "$stamp" ]; then
    (cd "$src" && rm -f composer.lock && composer update --no-interaction --no-progress ${FERRO_ORM_COMPOSER_FLAGS:-})
    echo "$stamp" > "$src/vendor/.ferro-stamp"
  fi
fi

# 1b. The vendor tree must carry THIS checkout's packages — a symlinked path repository resolved
#     against another checkout measures that checkout's code, and every number would be wrong.
for pkg in client:client doctrine-dbal-driver:doctrine-dbal; do
  link="$src/vendor/ferro/${pkg%%:*}"; want="$root/php/${pkg#*:}"
  [ "$(readlink -f "$link")" = "$(readlink -f "$want")" ] \
    || { echo "::error:: $link resolves to $(readlink -f "$link"), not $want"; exit 1; }
done

# 2. The replacement TestUtil and the SEQUENCE hook, each VERIFIED after applying.
cp "$root/testkit/orm/TestUtil.ferro.php" "$src/tests/Tests/TestUtil.php"
grep -q 'ferro orm-suite TestUtil' "$src/tests/Tests/TestUtil.php" || { echo "::error:: TestUtil patch did not apply"; exit 1; }
base="$src/tests/Tests/OrmFunctionalTestCase.php"
if ! grep -q "FERRO_ORM_SEQUENCE" "$base"; then
  perl -0pi -e 's/(        \$config->setMetadataDriverImpl\(\$mappingDriver\);\n)/$1\n        if (getenv(\x27FERRO_ORM_SEQUENCE\x27) === \x271\x27) { \/\/ testkit\/orm-suite.sh: the D-S8b-5 remedy, labelled\n            \$config->setIdentityGenerationPreferences([\\Doctrine\\DBAL\\Platforms\\PostgreSQLPlatform::class => \\Doctrine\\ORM\\Mapping\\ClassMetadata::GENERATOR_TYPE_SEQUENCE]);\n        }\n/' "$base"
fi
grep -q "getenv('FERRO_ORM_SEQUENCE')" "$base" || { echo "::error:: SEQUENCE hook patch did not apply"; exit 1; }

# 3. THE RESET: the whole database, dropped and recreated before ferrod opens it. `DROP SCHEMA …
#    CASCADE` over the suite's leftovers exhausts max_locks_per_transaction on PostgreSQL (measured).
case "$svc" in
  pg|psql)
    admin="postgres://ferro:ferro@127.0.0.1:55432/postgres"
    if [ "$svc" = pg ]; then
      docker compose -f "$root/testkit/docker-compose.yml" exec -T pg psql -v ON_ERROR_STOP=1 -q -U ferro -d postgres \
        -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" -c "CREATE DATABASE $db OWNER ferro"
    else
      psql -v ON_ERROR_STOP=1 -q -d "$admin" -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" -c "CREATE DATABASE $db OWNER ferro"
    fi ;;
  mysql|mariadb)
    bin=mysql; [ "$svc" = mariadb ] && bin=mariadb
    sed "s/orm_tests/$db/g" "$root/testkit/orm/reset-mysql.sql" \
      | docker compose -f "$root/testkit/docker-compose.yml" exec -T -e MYSQL_PWD=ferro "$svc" "$bin" -uroot ;;
  mysql-local)
    sed "s/orm_tests/$db/g" "$root/testkit/orm/reset-mysql.sql" | mysql -uroot -S "${FERRO_ORM_MYSQL_SOCKET:-/var/run/mysqld/mysqld.sock}" ;;
esac
echo "[ferro] reset: $svc/$db dropped and recreated"

# 4. ONE ferrod for the run (Ferro columns only; the control starts none).
sock=""
if [ "$control" != 1 ]; then
  bin="${FERRO_ORM_FERROD:-}"
  if [ -z "$bin" ]; then cargo build -q -p ferrod --manifest-path "$root/Cargo.toml"; bin="${CARGO_TARGET_DIR:-$root/target}/debug/ferrod"; fi
  sock="$(mktemp -u /tmp/ferro-orm-XXXXXX.sock)"
  env FERRO_SOCK="$sock" FERRO_POOLS=orm FERRO_POOL_ORM_DSN="$dsn" "$bin" >"$work/ferrod.log" 2>&1 &
  ferrod_pid=$!
  trap 'kill "$ferrod_pid" 2>/dev/null || true; rm -f "$sock"' EXIT
  for _ in $(seq 1 100); do [ -S "$sock" ] && break; sleep 0.1; done
  [ -S "$sock" ] || { echo "::error:: ferrod did not create $sock"; cat "$work/ferrod.log"; exit 1; }
fi

# 5. The phpunit config: the whole Functional directory, minus the two groups upstream's own CI
#    excludes (performance, locking_functional).
cfg="$work/phpunit.orm.xml"
{
  echo '<?xml version="1.0" encoding="utf-8"?>'
  echo "<phpunit bootstrap=\"$root/testkit/orm/bootstrap.php\" colors=\"false\" cacheDirectory=\"$work/.phpunit.cache\""
  echo '         beStrictAboutOutputDuringTests="true" failOnNotice="true" failOnWarning="true" failOnRisky="true">'
  echo '  <php>'
  echo '    <ini name="error_reporting" value="-1"/>'
  echo '    <env name="DOCTRINE_DEPRECATIONS" value="trigger"/>'
  if [ "$control" = 1 ]; then
    echo "    <var name=\"db_driver\" value=\"$ctl_driver\"/>"
    echo "    <var name=\"db_host\" value=\"127.0.0.1\"/><var name=\"db_port\" value=\"$port\"/>"
    echo "    <var name=\"db_user\" value=\"ferro\"/><var name=\"db_password\" value=\"ferro\"/><var name=\"db_dbname\" value=\"$db\"/>"
  else
    echo '    <var name="db_driverClass" value="Ferro\DBAL\Driver"/>'
    echo "    <var name=\"db_unix_socket\" value=\"$sock\"/>"
    echo '    <var name="db_driver_options" value="{&quot;pool&quot;:&quot;orm&quot;}"/>'
  fi
  echo '  </php>'
  echo "  <testsuites><testsuite name=\"orm-functional\"><directory>$src/tests/Tests/ORM/Functional</directory></testsuite></testsuites>"
  echo '  <groups><exclude><group>performance</group><group>locking_functional</group></exclude></groups>'
  echo '</phpunit>'
} > "$cfg"

# 6. Run. The bootstrap's contact assertion runs first.
FERRO_ORM_SRC="$src" FERRO_ORM_CONTROL="$control" FERRO_ORM_SEQUENCE="$sequence" FERRO_ORM_DB="$db" \
  php -d memory_limit=-1 "$src/vendor/bin/phpunit" -c "$cfg" "$@"
