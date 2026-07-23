# testkit

Dockerized Postgres backend used by Ferro's integration tests (SPEC §16). This is a
real, digest-pinned Postgres 17 instance seeded with a small deterministic table —
no mocks, no in-memory substitutes.

## Start it

```
docker compose -f testkit/docker-compose.yml up -d
```

This starts a single `pg` service (Postgres 17, pinned by digest) on host port
**55432** (mapped to the container's 5432), so it won't clash with a Postgres
already running on the host's default 5432. Data lives on tmpfs — it is ephemeral
and reset on every `down -v`.

Connection URL:

```
postgres://ferro:ferro@localhost:55432/ferro
```

It is seeded (via `postgres/init.sql`, mounted into
`/docker-entrypoint-initdb.d/`) with:

```sql
CREATE TABLE ferro_smoke (
    id   integer PRIMARY KEY,
    note text NOT NULL
);
INSERT INTO ferro_smoke (id, note) VALUES (1, 'hello'), (2, 'world');
```

## `FERRO_TEST_PG_URL` convention

Integration tests (Rust and PHP, in later slices) read the connection URL from the
`FERRO_TEST_PG_URL` environment variable, e.g.:

```
export FERRO_TEST_PG_URL="postgres://ferro:ferro@localhost:55432/ferro"
```

**When `FERRO_TEST_PG_URL` is unset, integration tests that need a real Postgres
backend must skip (not fail).** This keeps unit-test runs and CI jobs without the
testkit green while still exercising the real backend wherever it's available.

## Wait for health

```
testkit/wait-for-pg.sh
```

Polls `docker compose ps --format '{{.Health}}' pg` (once per second, up to 60s)
until the healthcheck (`pg_isready`) reports `healthy`, then exits 0. On timeout it
dumps `docker compose logs pg` and exits 1.

## Validate end-to-end

```
testkit/smoke.sh
```

Brings the stack up, waits for health, asserts `ferro_smoke` has exactly 2 rows,
prints `testkit smoke OK`, and tears everything down (`down -v`) on exit —
success or failure. Exit code 0 means the testkit works.

## Stop / tear down

```
docker compose -f testkit/docker-compose.yml down -v
```

(`smoke.sh` does this automatically via a trap.)

## Refreshing the pinned digest

The compose file pins `postgres:17` by digest for reproducibility. To pick up a new
build of the `17` tag:

```
docker pull postgres:17
docker inspect --format '{{index .RepoDigests 0}}' postgres:17
```

Copy the resulting `sha256:...` value into `testkit/docker-compose.yml`'s
`image: postgres:17@sha256:...` line, then re-run `testkit/smoke.sh` to confirm
the new image still passes.
