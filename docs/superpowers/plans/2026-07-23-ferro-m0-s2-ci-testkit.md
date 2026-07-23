# Ferro M0 · Slice S2 — CI + testkit (Dockerized Postgres, gates, cargo-deny) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Stand up the real, Dockerized Postgres backend that later slices' integration tests run against, plus the charter Definition-of-Done gates (local + CI) and the `cargo-deny` guard that enforces the hand-rolled-pool decision (D9). Everything is validated by *actually running it* (compose up, gate scripts).

**Architecture:** `testkit/docker-compose.yml` runs a digest-pinned Postgres 17 as the upstream `ferrod` will pool against; `ci/local-gate.sh` mirrors the CI pipeline for local runs; `.github/workflows/ci.yml` runs fmt/clippy/test, PHPUnit+PHPStan (with ext-msgpack provisioned), the nightly cargo-fuzz smoke, cargo-deny, and the Docker-PG-backed integration lane. Integration tests read `FERRO_TEST_PG_URL` and **skip** when unset, so the offline gate stays green. The `ferrod` container image + sidecar compose are deferred to S3 (the daemon binary doesn't exist yet).

**Tech Stack:** Docker Compose v5, Postgres 17, GitHub Actions, `cargo-deny`, bash. No new Rust/PHP code.

## Global Constraints

- **Everything in Docker (maintainer decision):** the Postgres backend runs as a digest-pinned container; integration + bench run against it. The `ferrod` sidecar container joins in S3.
- **Charter DoD gates** (must all pass, and the scripts must enforce them): `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace`, PHPUnit, PHPStan level 9 on `php/client`.
- **Offline-green:** with `FERRO_TEST_PG_URL` unset, `cargo test --workspace` and the local gate must pass without Docker (integration tests skip, not fail).
- **cargo-deny bans `deadpool` and `bb8`** — the pool is hand-rolled (SPEC §21 D9); a generic-pool dependency is a defect.
- **Digest-pin the Postgres image** — reproducible reference environment (SPEC §16); a floating tag is a defect.
- **`bench/results/` is committed** (SPEC §16); the D12 gate script lives here for S8.

## File Structure

```
/testkit/
  docker-compose.yml        postgres:17 (digest-pinned), healthcheck, tmpfs, init.sql, port
  postgres/init.sql         seed schema + rows for integration/smoke tests
  wait-for-pg.sh            block until the PG container is healthy
  smoke.sh                  up -> wait -> psql seed assertion -> down (validates the testkit)
  README.md                 how to run backends, the FERRO_TEST_PG_URL convention
/ci/
  local-gate.sh             runs all charter gates locally; --with-pg adds the integration lane
  check-d12-recorded.sh     M0 exit-gate script: >=1 committed bench/results/*.json (used by S8)
/.github/workflows/ci.yml   rust + php + fuzz-smoke + cargo-deny + docker-pg integration lanes
/deny.toml                  cargo-deny: ban deadpool/bb8, advisories, licenses
/bench/results/.gitkeep     committed dir for D12 runs
```

---

### Task 1: testkit — Dockerized Postgres + seed + wait/smoke scripts

**Files:**
- Create: `testkit/docker-compose.yml`, `testkit/postgres/init.sql`, `testkit/wait-for-pg.sh`, `testkit/smoke.sh`, `testkit/README.md`

**Interfaces:**
- Produces: a reachable Postgres at `postgres://ferro:ferro@localhost:55432/ferro` (non-default host port to avoid clashing a host PG), seeded with a `ferro_smoke(id int primary key, note text)` table. `FERRO_TEST_PG_URL` convention documented for later slices.

- [ ] **Step 1: Write the compose file (digest-pinned)**

```yaml
# testkit/docker-compose.yml
# Postgres 17 pinned by digest for a reproducible reference backend (SPEC §16).
# To refresh the digest: docker pull postgres:17 && docker inspect --format '{{index .RepoDigests 0}}' postgres:17
services:
  pg:
    image: postgres:17@sha256:REPLACE_WITH_REAL_DIGEST
    environment:
      POSTGRES_USER: ferro
      POSTGRES_PASSWORD: ferro
      POSTGRES_DB: ferro
    ports:
      - "55432:5432"
    tmpfs:
      - /var/lib/postgresql/data      # ephemeral, fast — this is a test backend
    volumes:
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ferro -d ferro"]
      interval: 1s
      timeout: 3s
      retries: 30
```

> **Digest:** the implementer MUST replace `REPLACE_WITH_REAL_DIGEST` with the actual digest from `docker pull postgres:17 && docker inspect --format '{{index .RepoDigests 0}}' postgres:17` (record the resolved `postgres:17@sha256:...`).

- [ ] **Step 2: Write the seed**

```sql
-- testkit/postgres/init.sql
-- Minimal seed so integration/smoke tests have deterministic rows to read.
CREATE TABLE ferro_smoke (
    id   integer PRIMARY KEY,
    note text NOT NULL
);
INSERT INTO ferro_smoke (id, note) VALUES (1, 'hello'), (2, 'world');
```

- [ ] **Step 3: Write the wait script**

```bash
#!/usr/bin/env bash
# testkit/wait-for-pg.sh — block until the pg service reports healthy (or time out).
set -euo pipefail
compose="docker compose -f $(dirname "$0")/docker-compose.yml"
for _ in $(seq 1 60); do
  status=$($compose ps --format '{{.Health}}' pg 2>/dev/null || true)
  [ "$status" = "healthy" ] && { echo "pg healthy"; exit 0; }
  sleep 1
done
echo "pg did not become healthy in time" >&2
$compose logs pg >&2 || true
exit 1
```

- [ ] **Step 4: Write the smoke script (validates the whole testkit end-to-end)**

```bash
#!/usr/bin/env bash
# testkit/smoke.sh — up -> wait -> assert seed -> down. Proves the Dockerized backend works.
set -euo pipefail
dir="$(dirname "$0")"
compose="docker compose -f $dir/docker-compose.yml"
cleanup() { $compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT
$compose up -d
"$dir/wait-for-pg.sh"
count=$($compose exec -T pg psql -U ferro -d ferro -tAc "select count(*) from ferro_smoke;")
echo "ferro_smoke rows: $count"
[ "$count" = "2" ] || { echo "FAIL: expected 2 seed rows, got '$count'" >&2; exit 1; }
echo "testkit smoke OK"
```

- [ ] **Step 5: Write `testkit/README.md`** documenting: `docker compose -f testkit/docker-compose.yml up -d` to start; the connection URL `postgres://ferro:ferro@localhost:55432/ferro`; that integration tests read `FERRO_TEST_PG_URL` and **skip** when unset; `testkit/smoke.sh` to validate; how to refresh the pinned digest.

- [ ] **Step 6: Make scripts executable and VALIDATE by running**

Run: `chmod +x testkit/wait-for-pg.sh testkit/smoke.sh && ./testkit/smoke.sh`
Expected: `pg healthy`, `ferro_smoke rows: 2`, `testkit smoke OK`, exit 0. (This actually pulls the digest-pinned image, starts PG, asserts the seed, tears down.)

- [ ] **Step 7: Commit**

```bash
git add testkit && git commit -m "feat(s2): dockerized postgres testkit + seed + wait/smoke scripts"
```

---

### Task 2: cargo-deny — ban generic pools (D9) + advisories

**Files:**
- Create: `deny.toml`

**Interfaces:**
- Produces: `cargo deny check bans` failing if `deadpool` or `bb8` ever enter the tree.

- [ ] **Step 1: Write `deny.toml`**

```toml
# deny.toml — enforces SPEC §21 D9 (the pool is hand-rolled) and basic supply-chain hygiene.
[bans]
multiple-versions = "allow"
deny = [
    { name = "deadpool" },
    { name = "bb8" },
]

[advisories]
version = 2
ignore = []

[licenses]
version = 2
allow = ["Apache-2.0", "MIT", "BSD-3-Clause", "BSD-2-Clause", "ISC", "Unicode-3.0", "Zlib"]
```

- [ ] **Step 2: Validate (install cargo-deny if missing, else defer to CI)**

Run: `cargo deny --version || cargo install cargo-deny --locked`
Then: `cargo deny check bans`
Expected: `bans ok` (no deadpool/bb8 in the tree). If `cargo install cargo-deny` is infeasible (network/time), run it in the nightly-agnostic Docker path `docker run --rm -v "$PWD":/w -w /w rust:1.95 bash -c 'cargo install cargo-deny --locked && cargo deny check bans'`, or document that the CI job (Task 4) is the enforcing run and note it in the report.

> `cargo deny check advisories`/`licenses` may report findings that need triage; for S2 the binding requirement is `check bans` (deadpool/bb8). Record any advisory/license findings for follow-up but do not let them block S2 unless trivially fixable.

- [ ] **Step 3: Commit**

```bash
git add deny.toml && git commit -m "feat(s2): cargo-deny bans (deadpool/bb8 per D9) + advisories/licenses"
```

---

### Task 3: local gate script + D12 gate script

**Files:**
- Create: `ci/local-gate.sh`, `ci/check-d12-recorded.sh`, `bench/results/.gitkeep`

**Interfaces:**
- Produces: `ci/local-gate.sh` (exit 0 iff all charter gates pass); `ci/check-d12-recorded.sh` (exit 0 iff ≥1 committed `bench/results/*.json`).

- [ ] **Step 1: Write `ci/local-gate.sh`**

```bash
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
```

- [ ] **Step 2: Write `ci/check-d12-recorded.sh`**

```bash
#!/usr/bin/env bash
# ci/check-d12-recorded.sh — the M0 EXIT GATE (used by S8): a D12 bench run must be committed.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
n=$(find "$root/bench/results" -maxdepth 1 -name '*.json' -type f 2>/dev/null | wc -l | tr -d ' ')
if [ "$n" -lt 1 ]; then
  echo "M0 EXIT GATE NOT MET: no bench/results/*.json recorded (see SPEC §16.1 / D12)" >&2
  exit 1
fi
echo "D12 gate: $n recorded bench result(s)"
```

- [ ] **Step 3: `bench/results/.gitkeep`** — empty committed file so the dir exists.

- [ ] **Step 4: Make executable and VALIDATE**

Run: `chmod +x ci/local-gate.sh ci/check-d12-recorded.sh && ./ci/local-gate.sh`
Expected: every gate section prints, ends `ALL GATES GREEN`, exit 0 (offline — integration skips).
Run: `./ci/check-d12-recorded.sh; echo "exit=$?"`
Expected: exit 1 with the "NOT MET" message (no bench results yet — correct; S8 makes it pass).

- [ ] **Step 5: Verify the gate actually FAILS on a broken gate (don't ship a vacuous gate)**

Temporarily introduce a fmt error (e.g. add a badly-indented line to a Rust file), run `./ci/local-gate.sh`, confirm it exits nonzero at the fmt step, then revert.

- [ ] **Step 6: Commit**

```bash
git add ci bench/results/.gitkeep && git commit -m "feat(s2): local-gate + D12-recorded gate scripts"
```

---

### Task 4: GitHub Actions CI

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Produces: CI lanes: `rust` (fmt/clippy/test), `php` (PHPUnit+PHPStan with ext-msgpack), `deny` (cargo-deny bans), `fuzz-smoke` (nightly, short), `integration` (docker-compose PG + `FERRO_TEST_PG_URL`).

- [ ] **Step 1: Write `.github/workflows/ci.yml`**

```yaml
# .github/workflows/ci.yml
name: ci
on:
  push: { branches: [main, m0-build] }
  pull_request:
jobs:
  rust:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { components: rustfmt, clippy }
      - run: cargo fmt --check
      - run: cargo clippy --workspace -- -D warnings
      - run: cargo test --workspace          # offline: integration tests skip (no FERRO_TEST_PG_URL)

  integration:
    runs-on: ubuntu-latest
    services:
      pg:
        image: postgres:17
        env: { POSTGRES_USER: ferro, POSTGRES_PASSWORD: ferro, POSTGRES_DB: ferro }
        ports: ["5432:5432"]
        options: >-
          --health-cmd "pg_isready -U ferro -d ferro" --health-interval 1s
          --health-timeout 3s --health-retries 30
    env:
      FERRO_TEST_PG_URL: postgres://ferro:ferro@localhost:5432/ferro
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo test --workspace          # integration lane: FERRO_TEST_PG_URL is set

  php:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: shivammathur/setup-php@v2
        with: { php-version: "8.4", extensions: msgpack, tools: composer }
      - run: (cd php/client && composer install --no-interaction)
      - run: (cd php/client && ./vendor/bin/phpunit)
      - run: (cd php/client && ./vendor/bin/phpstan analyse src --level 9)

  deny:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: EmbarkStudios/cargo-deny-action@v2
        with: { command: check bans }

  fuzz-smoke:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@nightly
      - run: cargo install cargo-fuzz --locked
      - run: cd engine/crates/ferro-proto && cargo +nightly fuzz run decode_frame -- -runs=20000 -max_total_time=60
      - run: cd engine/crates/ferro-proto && cargo +nightly fuzz run roundtrip_frame -- -runs=20000 -max_total_time=60
```

- [ ] **Step 2: Validate the YAML is well-formed**

Run (if available): `docker run --rm -v "$PWD":/repo rhysd/actionlint:latest -color .github/workflows/ci.yml` OR `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml OK')"`.
Expected: parses without error (actionlint may warn on minor style; a clean parse is the bar for S2 since there is no runner here).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml && git commit -m "feat(s2): CI — rust/php/deny/fuzz-smoke/integration lanes"
```

---

## Self-Review

- **Spec coverage (design S2 gate):** `docker compose up` → PG healthy → Task 1 (`smoke.sh`). `local-gate.sh` runs all gates + fails on break → Task 3 (Steps 4–5). Offline-green → Task 3 Step 4. cargo-deny bans deadpool/bb8 → Task 2. CI lanes incl. fuzz-smoke + docker-PG + both-codec (ext-msgpack) → Task 4. `check-d12-recorded.sh` → Task 3. Digest-pinned PG → Task 1.
- **Deferred to S3 (noted):** the `ferrod` container image + sidecar compose (no daemon binary until S3). The first Rust integration test using `FERRO_TEST_PG_URL` lands in S4 (needs the PG backend crate); S2 proves PG reachability via `smoke.sh`/psql instead.
- **Placeholder scan:** the one intentional placeholder is `REPLACE_WITH_REAL_DIGEST` — Task 1 Step 1 explicitly requires resolving it to a real digest before commit.
- **Execution-time confirmations:** `cargo-deny`/`actionlint` availability (fallback to Docker/CI documented); GitHub Actions YAML can't be run here (validated by parse only).

## Execution Handoff

Subagent-driven: dispatch a fresh implementer per task, review between, then a brief whole-slice check. The infra tasks are validated by *running* them (compose up, gate scripts), which is stronger than reviewing config text.
