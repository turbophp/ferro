#!/usr/bin/env bash
# M1-S8c Task 4 — THE PROMISE, not a proxy for it: `doctrine/migrations` really running on
# PostgreSQL through Ferro. Generate a migration from a schema diff, execute it, diff again to prove
# the introspection round-trips, then roll it back.
#
# D-S8b-6's stated payoff is "the stock PG schema manager and therefore `doctrine/migrations` and
# schema diffing". A schema-manager unit test is a PROXY for that; this is the thing itself, with
# the real `vendor/bin/doctrine-migrations` CLI, unpatched.
#
# NO `docker compose down` TRAP OF ANY KIND (the shared containers serve every other suite in this
# repo). The only EXIT trap kills the ferrod THIS script started.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
here="$root/testkit/migrations"
db="${FERRO_MIG_DB:-ferro_migrations}"
svc="${FERRO_MIG_SVC:-pg}"
compose="docker compose -f $root/testkit/docker-compose.yml"

pg() { $compose exec -T "$svc" psql -v ON_ERROR_STOP=1 -U ferro -d "$1" -Atc "$2"; }

fail() { echo "::error:: $*"; exit 1; }

# --- 0. A FRESH database every run. Same principle as testkit/dbal/reset-pg.sql: a number (or a
#        pass) that depends on what the previous run left behind is not a measurement. It is its own
#        database, never the shared `ferro` one and never `doctrine_tests`, so nothing else in this
#        repo can be disturbed by it or disturb it.
$compose exec -T "$svc" psql -v ON_ERROR_STOP=1 -U ferro -d postgres -q \
  -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" -c "CREATE DATABASE $db OWNER ferro"
echo "[ferro] migrations: fresh database $db"

# --- 1. The engine, built from THIS tree.
cargo build -p ferrod --manifest-path "$root/Cargo.toml"
sock="$(mktemp -u /tmp/ferro-mig-XXXXXX.sock)"
env FERRO_SOCK="$sock" FERRO_POOLS=default \
    "FERRO_POOL_DEFAULT_DSN=postgres://ferro:ferro@127.0.0.1:55432/$db" \
    "$root/target/debug/ferrod" >"$here/ferrod.log" 2>&1 &
ferrod_pid=$!
trap 'kill "$ferrod_pid" 2>/dev/null || true; rm -f "$sock"' EXIT   # ONLY our own daemon.
for _ in $(seq 1 100); do [ -S "$sock" ] && break; sleep 0.1; done
[ -S "$sock" ] || { cat "$here/ferrod.log"; fail "ferrod did not create $sock"; }

# --- 2. The harness's own vendor tree (doctrine/migrations 3.x + DBAL pinned to the suite's 4.4.4,
#        with ferro/doctrine-dbal-driver and ferro/client symlinked out of THIS tree).
(cd "$here" && composer install --no-interaction --no-progress --quiet)
installed="$(cd "$here" && composer show doctrine/dbal 2>/dev/null | awk '$1=="versions" {print $NF}')"
[ "$installed" = "4.4.4" ] || fail "doctrine/dbal is '$installed', expected the pinned 4.4.4"
mig_ver="$(cd "$here" && composer show doctrine/migrations 2>/dev/null | awk '$1=="versions" {print $NF}')"
echo "[ferro] migrations: doctrine/migrations $mig_ver, doctrine/dbal $installed"

rm -rf "$here/generated"; mkdir -p "$here/generated"
cli=(env FERRO_SOCK="$sock" "$here/vendor/bin/doctrine-migrations" --no-interaction)
# `filter-expression` keeps BOTH sides of the diff scoped to the fixture tables, so the migrations
# metadata table (which the diff would otherwise propose to DROP, since no target schema declares
# it) is invisible to the comparison. This is doctrine/migrations' own documented option.
filter='/^s8c_/'

step() { echo; echo "----- $* -----"; }

# --- 3. DIFF #1 against the empty database.
step "diff #1 (empty database -> target schema)"
(cd "$here" && "${cli[@]}" migrations:diff --filter-expression="$filter") \
  || fail "migrations:diff failed"
gen="$(find "$here/generated" -name 'Version*.php' | head -1)"
[ -n "$gen" ] || fail "diff generated no migration file"
echo "[ferro] generated: $(basename "$gen")"
grep -q 'CREATE TABLE s8c_author' "$gen" || fail "the generated up() does not create s8c_author"
grep -q 'CREATE TABLE s8c_book' "$gen" || fail "the generated up() does not create s8c_book"
grep -q 's8c_book_author_title_idx' "$gen" || fail "the generated up() does not create the index"
grep -q 's8c_book_author_fk' "$gen" || fail "the generated up() does not create the foreign key"
grep -q 'DROP TABLE s8c_book' "$gen" || fail "the generated down() does not drop s8c_book"

# --- 4. MIGRATE.
step "migrate (execute the generated migration)"
(cd "$here" && "${cli[@]}" migrations:migrate) || fail "migrations:migrate failed"

# --- 5. THE INDEPENDENT ORACLE. Every assertion below is made by psql INSIDE the container, i.e.
#        without Ferro in the path at all. A driver that reported success while doing nothing —
#        or a migration executed against some other database — cannot survive this.
step "verify with psql (independent of Ferro)"
[ "$(pg "$db" "SELECT count(*) FROM pg_class WHERE relname IN ('s8c_author','s8c_book') AND relkind='r'")" = 2 ] \
  || fail "the two tables were not created"
[ "$(pg "$db" "SELECT count(*) FROM pg_indexes WHERE indexname='s8c_book_author_title_idx'")" = 1 ] \
  || fail "the secondary index was not created"
[ "$(pg "$db" "SELECT count(*) FROM pg_constraint WHERE conname='s8c_book_author_fk' AND contype='f'")" = 1 ] \
  || fail "the foreign key was not created"
[ "$(pg "$db" "SELECT count(*) FROM ferro_migration_versions WHERE executed_at IS NOT NULL")" = 1 ] \
  || fail "the migration was not recorded in the metadata table"
echo "[ferro] psql: 2 tables + index + FK + 1 recorded version — all present"

# --- 6. DIFF #2. THE STRONG CLAIM. This one introspects the tables that now EXIST — their columns,
#        their primary keys, the two-column index (whose column ORDER is decoded from the
#        `int2vector` `pg_index.indkey`) and the foreign key — and must find NOTHING to change.
#        Diff #1 alone would be a weak proof: an empty database has no index to read, so it can
#        succeed without the read path ever meeting `indkey`.
step "diff #2 (must detect NO changes — the introspection round-trip)"
set +e
out="$(cd "$here" && "${cli[@]}" migrations:diff --filter-expression="$filter" 2>&1)"
rc=$?
set -e
echo "$out"
echo "$out" | grep -qi 'No changes detected' \
  || fail "diff #2 detected changes (or failed, rc=$rc) — introspection does not round-trip"
[ "$(find "$here/generated" -name 'Version*.php' | wc -l)" = 1 ] \
  || fail "diff #2 wrote a second migration file"

# --- 7. ROLL BACK.
step "migrate prev (roll the migration back)"
(cd "$here" && "${cli[@]}" migrations:migrate prev) || fail "migrations:migrate prev failed"
[ "$(pg "$db" "SELECT count(*) FROM pg_class WHERE relname IN ('s8c_author','s8c_book') AND relkind='r'")" = 0 ] \
  || fail "rollback did not drop the tables"
[ "$(pg "$db" "SELECT count(*) FROM ferro_migration_versions")" = 0 ] \
  || fail "rollback did not remove the recorded version"
echo "[ferro] psql: both tables gone, metadata table empty — rollback verified"

echo
echo "[ferro] MIGRATIONS E2E: PASS (diff -> migrate -> empty diff -> rollback, all verified by psql)"
