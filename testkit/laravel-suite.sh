#!/usr/bin/env bash
# M2 / C2: run a CURATED subset of laravel/framework's own database integration suite against Ferro.
#
# NO `docker compose down` TRAP OF ANY KIND — same rule as testkit/dbal-suite.sh. testkit/smoke.sh
# and testkit/e2e-demo.sh both tear the stack down on EXIT; copying that here would destroy the
# databases every other suite is using. The only EXIT trap below kills the ferrod THIS script
# started and removes its socket.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${FERRO_LARAVEL_TAG:-v11.51.0}"
pool="${FERRO_LARAVEL_POOL:-default}"
# Its OWN database, never the shared `ferro` one — the suite migrates a schema from scratch before
# every test (`migrate:fresh`) and would otherwise wipe what every other live suite depends on.
dsn="${FERRO_LARAVEL_DSN:-postgres://ferro:ferro@127.0.0.1:55432/laravel_tests}"
svc="${FERRO_LARAVEL_SVC:-pg}"
work="${FERRO_LARAVEL_WORK:-$root/.laravel-suite}"
# Which Illuminate `driver` NAME the application registers Ferro under. Both values are real
# product configurations (see testkit/laravel/DatabaseTestCase.ferro.php): `ferro-pgsql` is §15's
# one-word config change, `pgsql` is the opt-in alias `FerroConnections::register()` accepts. The
# recorded numbers differ between them because upstream's own tests branch on the name.
driver="${FERRO_LARAVEL_DRIVER:-ferro-pgsql}"
src="$work/laravel-$tag"
reset=1
args=()
for a in "$@"; do
  case "$a" in
    --no-reset) reset=0 ;;
    *) args+=("$a") ;;
  esac
done

mkdir -p "$work"

# 1. The PINNED source. packagist's dist ships `src/` only — no `tests/` — so a git clone is the
#    only way to get the suite, and the tag must be pinned or the bar drifts under us.
if [ ! -d "$src" ]; then
  git clone --depth 1 --branch "$tag" https://github.com/laravel/framework.git "$src"
fi

# 2. The harness vendor tree — the ONLY one this run uses (see bootstrap.php for why two autoloaders
#    cannot coexist). It carries laravel/framework at the pinned version, testbench-core, PHPUnit,
#    and ferro/laravel + ferro/client through path repositories.
(cd "$root/testkit/laravel" && composer install --no-interaction --no-progress --quiet)

# 2b. …so the tests come from the clone at $tag and the CODE UNDER TEST comes from vendor. If those
#     versions ever diverge the suite silently tests the wrong source. Assert they are equal.
installed="$(cd "$root/testkit/laravel" && composer show laravel/framework 2>/dev/null | awk '$1=="versions" {print $NF}')"
want="${tag#v}"
if [ "${installed#v}" != "$want" ]; then
  echo "::error:: laravel/framework in testkit/laravel/vendor is '$installed' but the test tree is pinned at '$tag'."
  echo "          The suite would run $tag's tests against $installed's source. Pin one to the other."
  exit 1
fi

# 3. The patched base class, copied over the upstream one, and VERIFIED. A silently-failed patch is
#    exactly how a suite goes green against SQLite — the sibling proved that deliberately.
cp "$root/testkit/laravel/DatabaseTestCase.ferro.php" "$src/tests/Integration/Database/DatabaseTestCase.php"
grep -q 'FerroConnections::register' "$src/tests/Integration/Database/DatabaseTestCase.php" \
  || { echo "::error:: DatabaseTestCase patch did not apply"; exit 1; }

# 4. ONE ferrod for the whole run, not one per test.
cargo build -p ferrod --manifest-path "$root/Cargo.toml"
sock="$(mktemp -u /tmp/ferro-laravel-XXXXXX.sock)"
env FERRO_SOCK="$sock" FERRO_POOLS="$pool" \
    "FERRO_POOL_$(echo "$pool" | tr '[:lower:]-' '[:upper:]_')_DSN=$dsn" \
    "$root/target/debug/ferrod" >"$work/ferrod.log" 2>&1 &
ferrod_pid=$!
trap 'kill "$ferrod_pid" 2>/dev/null || true; rm -f "$sock"' EXIT   # ONLY our own daemon.
for _ in $(seq 1 100); do [ -S "$sock" ] && break; sleep 0.1; done
[ -S "$sock" ] || { echo "::error:: ferrod did not create $sock"; cat "$work/ferrod.log"; exit 1; }

# 5. The phpunit config, generated from the allowlist so that file stays the single source of truth.
cfg="$work/phpunit.generated.xml"
{
  echo '<?xml version="1.0" encoding="UTF-8"?>'
  echo '<phpunit bootstrap="'"$root"'/testkit/laravel/bootstrap.php" colors="true" cacheDirectory="'"$work"'/.phpunit.cache">'
  echo '  <testsuites><testsuite name="ferro-laravel-subset">'
  while IFS= read -r line; do
    case "$line" in ''|'#'*) continue ;; esac
    if [ -d "$src/$line" ]; then echo "    <directory>$src/$line</directory>"
    else echo "    <file>$src/$line</file>"; fi
  done < "$root/testkit/laravel/allowlist.txt"
  echo '  </testsuite></testsuites>'
  echo '</phpunit>'
} > "$cfg"

# 6. THE RESET — the suite's only source of idempotence, and a hard precondition of recording a
#    number. It runs container-side with no PHP credentials at all (SPEC §12/D8). The sibling
#    MEASURED what skipping it costs: consecutive runs of the same command drifted 23 -> 33 errors.
if [ "$reset" = 1 ]; then
  case "$svc" in
    pg)
      docker compose -f "$root/testkit/docker-compose.yml" exec -T pg \
        psql -v ON_ERROR_STOP=1 -U ferro -d laravel_tests -q < "$root/testkit/laravel/reset-pg.sql"
      echo "[ferro] reset: pg/laravel_tests from testkit/laravel/reset-pg.sql"
      ;;
    # A LOCAL psql instead of a container one. Same SQL, same database, same guarantee — the only
    # difference is where the client binary lives, which is why this is a supported mode rather than
    # a reason to run --no-reset. It exists because a dev box (and this project's own agent
    # container) can have PostgreSQL without a Docker daemon, and "skip the reset" is exactly how a
    # number stops being reproducible. Credentials stay out of PHP either way (SPEC §12/D8): psql
    # reads the DSN the shell already holds for ferrod's own config.
    psql)
      psql -v ON_ERROR_STOP=1 -q -d "$dsn" -f "$root/testkit/laravel/reset-pg.sql"
      echo "[ferro] reset: local psql against \$FERRO_LARAVEL_DSN from testkit/laravel/reset-pg.sql"
      ;;
    *) echo "::error:: unknown FERRO_LARAVEL_SVC=$svc (wired: pg | psql — the tier registers ferro-pgsql only)"; exit 1 ;;
  esac
else
  echo "[ferro] reset: SKIPPED (--no-reset) — this run's numbers MUST NOT be recorded"
fi

# 7. Run it. The bootstrap's contact assertion runs first and exits non-zero if the connection is
#    not a Ferro one reaching a real PostgreSQL.
FERRO_LARAVEL_SRC="$src" FERRO_LARAVEL_SOCK="$sock" FERRO_LARAVEL_POOL="$pool" \
  FERRO_LARAVEL_DRIVER="$driver" \
  "$root/testkit/laravel/vendor/bin/phpunit" -c "$cfg" "${args[@]+"${args[@]}"}"
