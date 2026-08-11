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
# Which container to reset, and how. `--no-reset` exists for fast iteration; a RECORDED run must not
# use it (see the results file's environment manifest).
svc="${FERRO_DBAL_SVC:-pg}"

# 0. THE BACKEND IDENTITY, DERIVED ONCE. `FERRO_DBAL_SVC` decides which container is RESET and
#    `FERRO_DBAL_DSN` decides which backend is TESTED; when they were independent variables with
#    independent defaults, dropping one from a copied command — the exact slip a retyped command or
#    a templated CI job makes — reset MariaDB, tested an unreset PostgreSQL, printed both
#    load-bearing log lines, and published a third number (74 errors) under the wrong backend's
#    name. Measured, in the whole-branch review. So the DSN now DEFAULTS from the svc, and an
#    explicit DSN must AGREE with it.
#
#    The suite gets its OWN database on every family. NEVER the shared `ferro` one: this suite
#    creates and abandons ~40 tables, 8+ sequences, several schemas, a domain type and views, and
#    nothing would ever clean them out of a database every other live suite in this repo uses.
case "$svc" in
  pg)      want_scheme=postgres; want_port=55432 ;;
  mysql)   want_scheme=mysql;    want_port=33060 ;;
  mariadb) want_scheme=mysql;    want_port=33061 ;;
  *) echo "::error:: unknown FERRO_DBAL_SVC=$svc (expected: pg | mysql | mariadb)"; exit 1 ;;
esac
dsn="${FERRO_DBAL_DSN:-$want_scheme://ferro:ferro@127.0.0.1:$want_port/doctrine_tests}"

# The cross-check, for the case where a DSN IS given. Ports are the compose mappings in
# testkit/docker-compose.yml (55432 / 33060 / 33061) — the runner resets through
# `docker compose exec <svc>`, so a DSN that does not reach that same container is not a
# configuration this runner can produce a meaningful number from.
dsn_scheme="${dsn%%://*}"
dsn_authority="${dsn#*://}"; dsn_authority="${dsn_authority%%/*}"
dsn_hostport="${dsn_authority##*@}"
dsn_port="${dsn_hostport##*:}"
if [ "$dsn_port" = "$dsn_hostport" ]; then dsn_port=""; fi   # no explicit port in the DSN
if [ "$dsn_scheme" != "$want_scheme" ] || [ "$dsn_port" != "$want_port" ]; then
  echo "::error:: FERRO_DBAL_SVC and FERRO_DBAL_DSN disagree about which backend this is."
  echo "          FERRO_DBAL_SVC=$svc  ->  resets the '$svc' container, expects ${want_scheme}://…:${want_port}/…"
  echo "          FERRO_DBAL_DSN   ->  scheme='${dsn_scheme}' port='${dsn_port:-<none>}'"
  echo "          A run like that resets one database and tests another: both log lines would be"
  echo "          true and the pair a lie. Set FERRO_DBAL_SVC alone (the DSN defaults from it), or"
  echo "          set both to the same backend."
  exit 1
fi

# Print the pair, so the log can be AUDITED instead of trusted: the failure this guard closes was a
# log in which `[ferro] reset: mariadb/…` and `platform=PostgreSQL120Platform` were both true.
# Credentials are stripped — `$dsn_hostport` is everything after the last `@` — because SPEC §12
# says a credential-bearing DSN must never reach a log, and this repository shipped exactly that bug
# once already (`infer_pool_kind`, M1-S6).
dsn_db="${dsn#*://}"
if [ "${dsn_db#*/}" = "$dsn_db" ]; then dsn_db="<none>"; else dsn_db="${dsn_db#*/}"; fi
echo "[ferro] backend: $svc via ${dsn_scheme}://${dsn_hostport}/${dsn_db}"

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

# THE RECORDING BANNER, printed FIRST — before the clone, before the reset, before a single row is
# read. A run with extra PHPUnit arguments is a DEBUG run: `--filter` narrows it to one test of 730
# while every other line of the documented checklist stays present and correct (measured:
# `--filter 'testFetchAllAssociative$'` prints the reset line, the contact line and `OK (1 test)`,
# and exits 0). The log has to say which kind of run it was, at the top, where a reader starts.
#
# Anything not on the OUTPUT-ONLY safe-list counts as narrowing: fail closed, because the argument
# that matters (`--filter`, `--group`, a bare path) is the one nobody thinks to declare.
narrowing=()
for a in "${args[@]+"${args[@]}"}"; do
  case "$a" in
    --display-*|--colors|--colors=*|--testdox|-v|--verbose|--debug|--log-*|--no-progress) ;;
    *) narrowing+=("$a") ;;
  esac
done
if [ ${#narrowing[@]} -eq 0 ] && [ "$reset" = 1 ]; then
  echo "[ferro] recordable: yes (whole subset, reset applied)"
else
  why=""
  if [ ${#narrowing[@]} -gt 0 ]; then why="run narrowed by: ${narrowing[*]}"; fi
  if [ "$reset" != 1 ]; then why="${why:+$why; }--no-reset"; fi
  echo "[ferro] recordable: NO — $why"
fi

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

# 1a. The clone is a CACHE, not a pin, until this runs. It is created once and never re-verified,
#     and the runner itself writes into it (`tests/TestUtil.php`), so its own `git status` is dirty
#     by design and cannot serve as a signal. Demonstrated in the whole-branch review: appending one
#     failing test method to the pinned `tests/Functional/ResultTest.php` changed the acceptance
#     number with no warning at all. So restore `tests/` to the tag on every invocation, and then
#     refuse anything still differing (an ADDED file survives `checkout`).
git -C "$src" checkout -f "$tag" -- tests
dirty="$(git -C "$src" status --porcelain -- tests)"
if [ -n "$dirty" ]; then
  echo "::error:: the pinned DBAL clone's tests/ tree differs from tag $tag after a hard restore:"
  echo "$dirty"
  echo "          Delete $src and let the runner re-clone it."
  exit 1
fi
src_sha="$(git -C "$src" rev-parse HEAD)"

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

# 6. THE RESET — the suite's only source of idempotence, and a hard precondition of recording a
#    number. Upstream gets it from TestUtil::initializeDatabase()'s dropDatabase/createDatabase,
#    which Ferro structurally cannot do (PHP holds no credentials, SPEC §12/D8), so it happens
#    container-side with no PHP credentials at all — the same shape as the MySQL grant.
#
#    MEASURED, against a KNOWN-GOOD driver, with no reset: the same command gave `Errors 23,
#    Failures 3` and then `Errors 33, Failures 1`; with upstream's TestUtil it gave 0/0 before and
#    after. A number that degrades on every run is worse than no number, because the triage table
#    then blames the driver for leftover state.
#
#    It runs BEFORE ferrod is launched. The old order (launch, then drop the schema underneath the
#    daemon) happened to work only because the pool is lazy and nothing had run yet.
#
#    EVERY ARM IS FAIL-CLOSED. The MySQL-family arms used to end in
#    `2>&1 | grep -v 'Using a password' || true`, which swallowed every failure — a wrong password,
#    a stopped container, a renamed client binary (the MariaDB image ships `mariadb`, not `mysql`,
#    so that one is not hypothetical) — and the runner then printed `[ferro] reset: …` anyway.
#    MEASURED cost of that: MySQL publishes `Errors 4` reset and `Errors 16` unreset, and the
#    unreset number came out under a log that claimed the reset had run.
run_reset() { # <service> <client-binary> <sql-file>
  local service="$1" client="$2" sql="$3" out status
  set +e
  out="$(docker compose -f "$root/testkit/docker-compose.yml" exec -T "$service" \
         "$client" -uroot -pferro < "$sql" 2>&1)"
  status=$?
  set -e
  # The client writes "mysql: [Warning] Using a password on the command line…" to stderr on EVERY
  # invocation; it is noise, and it is the ONLY thing the old `grep -v` was there for.
  printf '%s\n' "$out" | grep -v 'Using a password' | grep -v '^$' || true
  if [ "$status" -ne 0 ]; then
    echo "::error:: the $service reset FAILED (exit $status). This run is not recordable; refusing to continue."
    exit 1
  fi
}

if [ "$reset" = 1 ]; then
  case "$svc" in
    pg)
      docker compose -f "$root/testkit/docker-compose.yml" exec -T pg \
        psql -v ON_ERROR_STOP=1 -U ferro -d doctrine_tests -q < "$root/testkit/dbal/reset-pg.sql"
      ;;
    mysql)   run_reset mysql   mysql   "$root/testkit/dbal/reset-mysql.sql" ;;
    mariadb) run_reset mariadb mariadb "$root/testkit/dbal/reset-mysql.sql" ;;  # the image ships `mariadb`
  esac
  echo "[ferro] reset: $svc/doctrine_tests from testkit/dbal/reset-$( [ "$svc" = pg ] && echo pg || echo mysql ).sql"
else
  echo "[ferro] reset: SKIPPED (--no-reset) — this run's numbers MUST NOT be recorded"
fi

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
  # M1-S8c Task 4: the suite must measure the configuration the driver's own docs call REQUIRED.
  # `FerroConnection` is not a convenience wrapper — without it an indeterminate write inside
  # `Doctrine\DBAL\Connection::transactional()` reaches the application as `NoActiveTransaction`
  # instead of `IndeterminateWriteException`, i.e. the spec's defining safety property thrown away
  # by DBAL's own cleanup.
  #
  # MEASURED, both ways, twice each, before this line was added: the recorded COUNTS are identical
  # with and without it on all three backends (PG stays 3/7, MySQL and MariaDB 2/9), so it does not
  # move any number in this file's comparison with S8b. What it moves is what
  # `TransactionTest::testTransactionalFailureDuringCommit` reports — "There is no active
  # transaction." without it, the driver's real `IndeterminateWriteException` with it. Equal counts,
  # opposite meanings, which is exactly why the bootstrap ASSERTS the wrapper rather than trusting
  # this line to stay here.
  echo '    <var name="db_wrapperClass" value="Ferro\DBAL\Wrapper\FerroConnection"/>'
  echo '    <var name="db_unix_socket" value="'"$sock"'"/>'
  echo '    <var name="db_driver_options" value="{&quot;pool&quot;:&quot;'"$pool"'&quot;}"/>'
  echo '  </php>'
  echo '</phpunit>'
} > "$cfg"

# The provenance the results file's environment manifest needs, printed by the run itself so it
# cannot be transcribed wrongly: the revision of the code under test (driver + client + engine all
# live in this one repository), and the pinned tests' commit — `--branch <tag>` resolves a MUTABLE
# ref, so the tag alone does not identify the tree.
repo_sha="$(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo unknown)"
repo_dirty=""
if [ -n "$(git -C "$root" status --porcelain 2>/dev/null)" ]; then repo_dirty=" +local-changes"; fi
echo "[ferro] tree: $repo_sha$repo_dirty · dbal tests: $tag @ ${src_sha:0:12} · backend: $svc"

# A run with extra PHPUnit arguments is a DEBUG run: `--filter` narrows it to one test while every
# other line of the recording checklist stays present and correct (measured: `--filter
# 'testFetchAllAssociative$'` prints the reset line, the contact line and `OK (1 test)`, exit 0).
# Say so in the log, so the checklist cannot be satisfied by a run that executed 1/730 of the subset.
# 7. Run it, with the DRIVER package's phpunit (see step 1 — one vendor tree, no version collision).
#    The bootstrap's contact assertion runs first and exits non-zero if the connection is not a
#    Ferro one.
FERRO_DBAL_SRC="$src" "$root/php/doctrine-dbal/vendor/bin/phpunit" -c "$cfg" "${args[@]+"${args[@]}"}"
