#!/usr/bin/env bash
# M1-S9: run doctrine/orm's own functional suite against Ferro (mode=ferro) or the stock PDO
# comparator (mode=stock). Modeled on testkit/dbal-suite.sh — read that file first; every guard
# here was paid for there or by the M1-S9 ORM probe
# (.superpowers/sdd/2026-08-13-ferro-m1-s9-exit-gate/research-orm.md).
#
# NO `docker compose down` TRAP OF ANY KIND. The only EXIT trap kills the ferrod THIS script
# started and removes its socket.
#
# Env contract (Tasks 4 and 7 call it exactly this way):
#   FERRO_ORM_SVC={pg|mysql|mariadb}   FERRO_ORM_MODE={ferro|stock}
#   FERRO_ORM_TAG (default 3.6.8)      FERRO_ORM_DBAL_PIN (default 4.4.4)
#   FERRO_ORM_PHPUNIT_PIN (default: the driver package's own)   FERRO_ORM_BASELINE=update
#   --no-reset
# exit 0 <=> a recordable run whose non-passing set byte-matches
# docs/orm-suite/baseline/{stock-}?<svc>.txt
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${FERRO_ORM_TAG:-3.6.8}"
pool="${FERRO_ORM_POOL:-default}"
svc="${FERRO_ORM_SVC:-pg}"
mode="${FERRO_ORM_MODE:-ferro}"

case "$svc" in
  pg)      want_scheme=postgres; want_port=55432; pdo_driver=pdo_pgsql ;;
  mysql)   want_scheme=mysql;    want_port=33060; pdo_driver=pdo_mysql ;;
  mariadb) want_scheme=mysql;    want_port=33061; pdo_driver=pdo_mysql ;;
  *) echo "::error:: unknown FERRO_ORM_SVC=$svc (expected: pg | mysql | mariadb)"; exit 1 ;;
esac
case "$mode" in ferro|stock) ;; *) echo "::error:: FERRO_ORM_MODE must be ferro|stock"; exit 1 ;; esac

# THE VERSION PINS, DERIVED FROM THE DRIVER PACKAGE, NOT FROM A LITERAL. The probe's ORM clone
# happened to resolve doctrine/dbal to exactly the tag Ferro is gated on and PHPUnit to exactly the
# driver package's build — "lucky and load-bearing" in its own words (research-orm step 1). Luck is
# not a pin, so both are pinned by explicit `composer require` below and both are asserted after
# resolution. The DEFAULTS are read out of php/doctrine-dbal/composer.lock (committed, so no vendor
# install is needed): that makes the ORM harness track the driver package automatically, and makes a
# future DBAL bump that forgets this runner fail LOUDLY instead of silently testing ORM against a
# DBAL the driver was never gated on.
lockfile="$root/php/doctrine-dbal/composer.lock"
lock_version() { # <package>
  php -r '
    $lock = json_decode(file_get_contents($argv[1]), true);
    if (! is_array($lock)) { fwrite(STDERR, "unreadable composer.lock\n"); exit(1); }
    foreach (array_merge($lock["packages"] ?? [], $lock["packages-dev"] ?? []) as $p) {
        if ($p["name"] === $argv[2]) { echo ltrim($p["version"], "v"); exit(0); }
    }
    fwrite(STDERR, "package {$argv[2]} not found in composer.lock\n");
    exit(1);
  ' "$lockfile" "$1"
}
driver_dbal_pin="$(lock_version doctrine/dbal)"
driver_phpunit_pin="$(lock_version phpunit/phpunit)"
dbal_pin="${FERRO_ORM_DBAL_PIN:-$driver_dbal_pin}"
phpunit_pin="${FERRO_ORM_PHPUNIT_PIN:-$driver_phpunit_pin}"
if [ "$dbal_pin" != "$driver_dbal_pin" ]; then
  echo "::error:: FERRO_ORM_DBAL_PIN=$dbal_pin but php/doctrine-dbal is locked at $driver_dbal_pin."
  echo "          The ORM suite would exercise the driver against a DBAL its own gates never ran."
  exit 1
fi

# The svc/DSN agreement guard, verbatim shape from dbal-suite.sh step 0 (the measured failure it
# closes: reset one backend, test another, publish a third number under the wrong name).
dsn="${FERRO_ORM_DSN:-$want_scheme://ferro:ferro@127.0.0.1:$want_port/doctrine_orm_tests}"
dsn_scheme="${dsn%%://*}"
dsn_authority="${dsn#*://}"; dsn_authority="${dsn_authority%%/*}"
dsn_hostport="${dsn_authority##*@}"
dsn_port="${dsn_hostport##*:}"
if [ "$dsn_port" = "$dsn_hostport" ]; then dsn_port=""; fi
if [ "$dsn_scheme" != "$want_scheme" ] || [ "$dsn_port" != "$want_port" ]; then
  echo "::error:: FERRO_ORM_SVC and FERRO_ORM_DSN disagree about which backend this is."
  echo "          FERRO_ORM_SVC=$svc  ->  resets the '$svc' container, expects ${want_scheme}://…:${want_port}/…"
  echo "          FERRO_ORM_DSN   ->  scheme='${dsn_scheme}' port='${dsn_port:-<none>}'"
  exit 1
fi
# Credentials stripped ($dsn_hostport is everything after the last '@') — SPEC §12.
echo "[ferro-orm] backend: $svc via ${dsn_scheme}://${dsn_hostport}/doctrine_orm_tests · mode: $mode"

work="${FERRO_ORM_WORK:-$root/.orm-suite}"
src="$work/orm-$tag"
reset=1
args=()
for a in "$@"; do
  case "$a" in
    --no-reset) reset=0 ;;
    *) args+=("$a") ;;
  esac
done

# THE RECORDING BANNER, printed FIRST (dbal-suite.sh's: a --filter run keeps every checklist line
# true while executing 1/3485 of the suite — the log must say which kind of run this was). Anything
# not on the OUTPUT-ONLY safe-list counts as narrowing: fail closed, because the argument that
# matters (--filter, --group, a bare path) is the one nobody thinks to declare.
narrowing=()
for a in "${args[@]+"${args[@]}"}"; do
  case "$a" in
    --display-*|--colors|--colors=*|--testdox|-v|--verbose|--debug|--log-*|--no-progress) ;;
    *) narrowing+=("$a") ;;
  esac
done
if [ ${#narrowing[@]} -eq 0 ] && [ "$reset" = 1 ]; then
  echo "[ferro-orm] recordable: yes (whole suite, reset applied, mode=$mode)"
else
  why=""
  if [ ${#narrowing[@]} -gt 0 ]; then why="run narrowed by: ${narrowing[*]}"; fi
  if [ "$reset" != 1 ]; then why="${why:+$why; }--no-reset"; fi
  echo "[ferro-orm] recordable: NO — $why"
fi

mkdir -p "$work"

# 1. The PINNED clone; tests/ restored to the tag on every invocation, refuse residual drift (an
#    ADDED test file survives checkout and silently changes the acceptance number — measured in the
#    S8b whole-branch review).
if [ ! -d "$src" ]; then
  git clone --depth 1 --branch "$tag" https://github.com/doctrine/orm.git "$src"
fi
git -C "$src" checkout -f "$tag" -- tests
dirty="$(git -C "$src" status --porcelain -- tests)"
if [ -n "$dirty" ]; then
  echo "::error:: the pinned ORM clone's tests/ differs from tag $tag after a hard restore:"
  echo "$dirty"; echo "          Delete $src and let the runner re-clone."; exit 1
fi
src_sha="$(git -C "$src" rev-parse HEAD)"

# 2. ONE vendor tree — the CLONE's (the inverse of dbal-suite.sh, whose one tree is the driver
#    package's; the driver package's vendor has no ORM). Ferro packages enter via path
#    repositories, and BOTH must be required explicitly at @dev — a dependency's path repository
#    does not propagate through minimum-stability (measured, research-orm step 3).
(cd "$src" \
  && composer config repositories.ferro-client path "$root/php/client" \
  && composer config repositories.ferro-dbal path "$root/php/doctrine-dbal" \
  && composer require --no-interaction --no-progress --quiet --with-all-dependencies \
       "doctrine/dbal:$dbal_pin" "ferro/client:@dev" "ferro/doctrine-dbal-driver:@dev" \
  && composer require --dev --no-interaction --no-progress --quiet \
       "phpunit/phpunit:$phpunit_pin")
resolved_dbal="$(cd "$src" && composer show doctrine/dbal 2>/dev/null | awk '$1=="versions" {print $NF}')"
resolved_phpunit="$(cd "$src" && composer show phpunit/phpunit 2>/dev/null | awk '$1=="versions" {print $NF}')"
if [ "$resolved_dbal" != "$dbal_pin" ]; then
  echo "::error:: doctrine/dbal resolved to '$resolved_dbal' in the ORM clone but the pin is '$dbal_pin'."
  echo "          The suite would test ORM-$tag against a DBAL the driver was never gated on."
  exit 1
fi
if [ "$resolved_phpunit" != "$phpunit_pin" ]; then
  echo "::error:: phpunit/phpunit resolved to '$resolved_phpunit' in the ORM clone but the pin is '$phpunit_pin'."
  echo "          A re-resolved PHPUnit changes which tests report warnings/deprecations and moves"
  echo "          the recorded non-passing set for reasons that have nothing to do with Ferro."
  exit 1
fi
echo "[ferro-orm] pins: doctrine/dbal $resolved_dbal · phpunit/phpunit $resolved_phpunit (both = php/doctrine-dbal's lock)"

# 3. Mode patches. tests/ was just restored, so a stock run is UPSTREAM-CLEAN except TestUtil (ours
#    serves both modes — the stock branch keeps everything stock-shaped), and a ferro run
#    additionally re-parents the suite's QueryLog wrapper onto FerroConnection (§22.2 (ah); ONE
#    line; FerroConnection declares no constructor so the QueryLog wiring is inherited). Each patch
#    is grep-VERIFIED: a silently-failed patch is exactly how an acceptance suite goes green against
#    the wrong engine.
cp "$root/testkit/orm/TestUtil.ferro.php" "$src/tests/Tests/TestUtil.php"
grep -q 'FERRO ORM HARNESS TESTUTIL' "$src/tests/Tests/TestUtil.php" \
  || { echo "::error:: TestUtil patch did not apply"; exit 1; }
if [ "$mode" = ferro ]; then
  perl -0pi -e 's/use Doctrine\\DBAL\\Connection as BaseConnection;/use Ferro\\DBAL\\Wrapper\\FerroConnection as BaseConnection;/' \
    "$src/tests/Tests/DbalExtensions/Connection.php"
  grep -q 'FerroConnection as BaseConnection' "$src/tests/Tests/DbalExtensions/Connection.php" \
    || { echo "::error:: the DbalExtensions parent patch did not apply (did upstream rename the import?)"; exit 1; }
fi

# 4. THE RESET — fail-closed, BEFORE ferrod, dedicated database (never the shared `ferro` one).
#    Measured cost of skipping / half-running it: 48 non-passing -> 227, with a triage that blames
#    the driver (research-orm step 6b).
run_reset() { # <service> <client-binary> <sql-file>   (verbatim shape from dbal-suite.sh)
  local service="$1" client="$2" sql="$3" out status
  set +e
  out="$(docker compose -f "$root/testkit/docker-compose.yml" exec -T "$service" \
         "$client" -uroot -pferro < "$sql" 2>&1)"
  status=$?
  set -e
  printf '%s\n' "$out" | grep -v 'Using a password' | grep -v '^$' || true
  if [ "$status" -ne 0 ]; then
    echo "::error:: the $service reset FAILED (exit $status). Not recordable; refusing to continue."
    exit 1
  fi
}
if [ "$reset" = 1 ]; then
  case "$svc" in
    pg)
      docker compose -f "$root/testkit/docker-compose.yml" exec -T pg \
        psql -v ON_ERROR_STOP=1 -U ferro -d postgres -q < "$root/testkit/orm/reset-pg.sql"
      ;;
    mysql)   run_reset mysql   mysql   "$root/testkit/orm/reset-mysql.sql" ;;
    mariadb) run_reset mariadb mariadb "$root/testkit/orm/reset-mysql.sql" ;;  # the image ships `mariadb`
  esac
  echo "[ferro-orm] reset: $svc/doctrine_orm_tests"
else
  echo "[ferro-orm] reset: SKIPPED (--no-reset) — this run's numbers MUST NOT be recorded"
fi

# 5. ONE ferrod (ferro mode only), on a FRESH random socket. The freshness is the guard: the
#    probe's worst run connected to a SURVIVING daemon over a REUSED socket path against an unreset
#    database, and every log line looked right. A path that did not exist until `mktemp -u` chose it
#    cannot be owned by a daemon that started earlier. `kill -0` then proves the socket belongs to a
#    process that is still alive — ours.
#    Sockets live in /tmp: ferrod refuses long paths (sun_path is 108 bytes), which is why the
#    mktemp location is load-bearing rather than style (research-orm step 3).
sock=""
if [ "$mode" = ferro ]; then
  cargo build -p ferrod --manifest-path "$root/Cargo.toml"
  sock="$(mktemp -u /tmp/ferro-orm-XXXXXX.sock)"
  env FERRO_SOCK="$sock" FERRO_POOLS="$pool" \
      "FERRO_POOL_$(echo "$pool" | tr '[:lower:]-' '[:upper:]_')_DSN=$dsn" \
      "$root/target/debug/ferrod" >"$work/ferrod.log" 2>&1 &
  ferrod_pid=$!
  trap 'kill "$ferrod_pid" 2>/dev/null || true; rm -f "$sock"' EXIT   # ONLY our own daemon.
  for _ in $(seq 1 100); do [ -S "$sock" ] && break; sleep 0.1; done
  [ -S "$sock" ] || { echo "::error:: ferrod did not create $sock"; cat "$work/ferrod.log"; exit 1; }
  kill -0 "$ferrod_pid" 2>/dev/null \
    || { echo "::error:: the ferrod this runner started (pid $ferrod_pid) is already gone"; cat "$work/ferrod.log"; exit 1; }
  echo "[ferro-orm] ferrod: pid $ferrod_pid on $sock (fresh path, this run's own daemon)"
fi

# 6. The generated phpunit config. ORM-DIFF: upstream CI's own group exclusions (performance,
#    locking_functional) — the probe's 3485-test denominator is defined by them.
#
#    The harness's own switches are passed as REAL environment variables at the phpunit invocation
#    (step 7), not as <env> elements: testkit/orm/bootstrap.php reads them with getenv() before the
#    first test, and whether PHPUnit has applied its <php><env> block by bootstrap time is an
#    implementation detail no acceptance harness should depend on. dbal-suite.sh passes
#    FERRO_DBAL_SRC the same way.
cfg="$work/phpunit.generated-$mode-$svc.xml"
{
  echo '<?xml version="1.0" encoding="UTF-8"?>'
  echo '<phpunit bootstrap="'"$root"'/testkit/orm/bootstrap.php" colors="true" cacheDirectory="'"$work"'/.phpunit.cache">'
  echo '  <testsuites><testsuite name="ferro-orm-functional">'
  echo "    <directory>$src/tests/Tests/ORM</directory>"
  echo '  </testsuite></testsuites>'
  echo '  <groups><exclude><group>performance</group><group>locking_functional</group></exclude></groups>'
  echo '  <php>'
  if [ "$mode" = ferro ]; then
    echo '    <var name="db_driverClass" value="Ferro\DBAL\Driver"/>'
    echo '    <var name="db_unix_socket" value="'"$sock"'"/>'
    echo '    <var name="db_dbname" value="doctrine_orm_tests"/>'
    echo '    <var name="db_driver_options" value="{&quot;pool&quot;:&quot;'"$pool"'&quot;}"/>'
  else
    echo '    <var name="db_driver" value="'"$pdo_driver"'"/>'
    echo '    <var name="db_host" value="127.0.0.1"/>'
    echo '    <var name="db_port" value="'"$want_port"'"/>'
    echo '    <var name="db_user" value="ferro"/>'
    echo '    <var name="db_password" value="ferro"/>'
    echo '    <var name="db_dbname" value="doctrine_orm_tests"/>'
  fi
  echo '  </php>'
  echo '</phpunit>'
} > "$cfg"

# D-S8b-5, the documented adoption path, PG only — WITHIN-BAR (SPEC §14; measured: 1229 errors
# without it). It is applied by testkit/orm/TestUtil.ferro.php::configureProxies() and PROVEN in
# effect by testkit/orm/bootstrap.php's preference assertion before any test runs.
pg_sequence=0
if [ "$mode" = ferro ] && [ "$svc" = pg ]; then pg_sequence=1; fi

repo_sha="$(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo unknown)"
repo_dirty=""
if [ -n "$(git -C "$root" status --porcelain 2>/dev/null)" ]; then repo_dirty=" +local-changes"; fi
echo "[ferro-orm] tree: $repo_sha$repo_dirty · orm tests: $tag @ ${src_sha:0:12} · dbal: $resolved_dbal · phpunit: $resolved_phpunit · backend: $svc · mode: $mode · COLUMNS=120 (pinned, see step 7)"

# 7. Run with the CLONE's phpunit (one vendor tree). --log-junit always on: the junit is what the
#    baseline diff reads.
#
#    COLUMNS IS PINNED, and that is a measured requirement rather than tidiness. Nine tests under
#    tests/Tests/ORM/Tools/Console/Command assert Symfony Console output verbatim, and Symfony sizes
#    that output to the terminal: with no COLUMNS and no tty (`stty` fails when the runner is piped
#    or run from CI) the width falls back to 80 and the CAUTION/INFO banners WRAP. Measured on this
#    tree, same code, same backend, one filter:
#      COLUMNS unset -> 9 failures · COLUMNS=100 -> 4 · COLUMNS=120 -> 0 · COLUMNS=200 -> 0
#    Unpinned, the committed baseline would encode the terminal the recording agent happened to use,
#    and a later re-run from a narrower window would report 9 "new" non-passing tests that have
#    nothing to do with Ferro. It applies identically to both modes, so the stock comparator stays a
#    fair comparator.
#    The junit path is RECORDABILITY-AWARE, and that is a measured requirement (M1-S9 Task 4). The
#    junit is the evidence the triage is derived from — the half of a recorded run that a reader
#    cannot re-derive from the committed baseline. A narrowed DEBUG run used to write it to the same
#    fixed path, so `--filter Foo` silently replaced a 3485-test recorded run's evidence with a
#    6-test one while the baseline gate correctly reported "not compared". The baseline (the
#    contract) was protected; the evidence was not. A non-recordable run now writes `.debug.xml`.
junit="$work/junit-$mode-$svc.xml"
if [ ${#narrowing[@]} -ne 0 ] || [ "$reset" != 1 ]; then
  junit="$work/junit-$mode-$svc.debug.xml"
fi
set +e
env COLUMNS=120 FERRO_ORM_SRC="$src" FERRO_ORM_MODE="$mode" \
    FERRO_ORM_PG_SEQUENCE="$( [ "$pg_sequence" = 1 ] && echo 1 || echo 0 )" \
    "$src/vendor/bin/phpunit" -c "$cfg" --log-junit "$junit" "${args[@]+"${args[@]}"}"
phpunit_status=$?
set -e

# 8. THE BASELINE DIFF (verbatim mechanism from dbal-suite.sh step 8, including the extractor): the
#    exit status becomes "does this match what we recorded", drift in EITHER direction is reported,
#    and only a recordable run may compare or update. ORM-DIFF: stock mode gates against its own
#    committed comparator baseline (stock-<svc>.txt) — "stock is not green on MySQL" becomes a
#    recorded, checkable artifact instead of a sentence.
baseline_dir="$root/docs/orm-suite/baseline"
prefix=""; [ "$mode" = stock ] && prefix="stock-"
baseline="$baseline_dir/$prefix$svc.txt"
observed="$work/nonpassing-$mode-$svc.txt"

cat > "$work/nonpassing.php" <<'EXTRACTOR'
<?php
// Emit "Class::method" for every JUnit <testcase> carrying a <failure> or <error>, sorted, unique.
// JUnit XML is parsed instead of PHPUnit's human output because that output's "N) Class::method"
// blocks are also produced for skipped/incomplete tests in other verbosity modes — a text scan is
// exactly the species of guard this repository keeps having to delete.
$xml = simplexml_load_file($argv[1]);
if ($xml === false) { fwrite(STDERR, "unreadable junit xml: {$argv[1]}\n"); exit(1); }
$out = [];
foreach ($xml->xpath('//testcase[failure or error]') as $tc) {
    $cls = (string) $tc['class'];
    $name = (string) $tc['name'];
    $out[] = $cls !== '' ? "$cls::$name" : $name;
}
$out = array_values(array_unique($out));
sort($out, SORT_STRING);
echo implode("\n", $out), $out === [] ? '' : "\n";
EXTRACTOR

if [ ${#narrowing[@]} -eq 0 ] && [ "$reset" = 1 ] && [ -f "$junit" ]; then
  php "$work/nonpassing.php" "$junit" > "$observed"
  observed_n=$(grep -c . "$observed" || true)
  if [ "${FERRO_ORM_BASELINE:-}" = "update" ]; then
    mkdir -p "$baseline_dir"
    cp "$observed" "$baseline"
    echo "[ferro-orm] baseline: UPDATED $prefix$svc.txt ($observed_n non-passing) — commit it with the results file"
    phpunit_status=0
  elif [ ! -f "$baseline" ]; then
    echo "::error:: no baseline at docs/orm-suite/baseline/$prefix$svc.txt ($observed_n non-passing observed)."
    echo "          Record one with: FERRO_ORM_BASELINE=update FERRO_ORM_SVC=$svc FERRO_ORM_MODE=$mode $0"
    exit 1
  elif diff -u "$baseline" "$observed" > "$work/baseline-$mode-$svc.diff" 2>&1; then
    echo "[ferro-orm] baseline: MATCHES docs/orm-suite/baseline/$prefix$svc.txt ($observed_n non-passing, exactly as recorded)"
    phpunit_status=0
  else
    echo "::error:: the non-passing set DRIFTED from docs/orm-suite/baseline/$prefix$svc.txt"
    echo "          '-' lines now PASS (or no longer run); '+' lines are newly non-passing."
    sed -n '3,$p' "$work/baseline-$mode-$svc.diff"
    echo "          If intended: FERRO_ORM_BASELINE=update FERRO_ORM_SVC=$svc FERRO_ORM_MODE=$mode $0"
    phpunit_status=1
  fi
else
  echo "[ferro-orm] baseline: not compared (this run is not recordable)"
fi

exit "$phpunit_status"
