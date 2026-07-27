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
