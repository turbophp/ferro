#!/usr/bin/env bash
# testkit/e2e-demo.sh — ONE command to watch the whole Ferro wire path work: bring up the testkit
# Postgres, run the `ferro-e2e` narrated in-process-ferrod demo against it, then tear the Postgres
# down again (kept data, no `-v`). See engine/crates/ferro-e2e for what the demo does.
set -euo pipefail

# Resolve the repo root from this script's location so it runs from any cwd (cargo finds the
# workspace by walking up from here).
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"
cd "$root"

compose() { docker compose -f testkit/docker-compose.yml "$@"; }

# Always tear the Postgres down on exit — success OR failure. No `-v`: the tmpfs data is ephemeral
# anyway, and keeping the volume def avoids a spurious "removing volume" line.
trap 'compose down' EXIT

compose up -d pg

# Wait for the healthcheck before pointing the demo at it (reuse the shared helper).
if [ -x testkit/wait-for-pg.sh ]; then
  testkit/wait-for-pg.sh
fi

FERRO_TEST_PG_URL="postgres://ferro:ferro@localhost:55432/ferro" cargo run -p ferro-e2e
