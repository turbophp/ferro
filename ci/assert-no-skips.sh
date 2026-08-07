#!/usr/bin/env bash
# ci/assert-no-skips.sh — THE live-lane no-op detector, shared by `.github/workflows/ci.yml`
# (the `integration` job) and `ci/local-gate.sh` so the two can never drift apart.
#
# Every live suite in this repo is skip-if-unconfigured BY DESIGN: a missing `FERRO_TEST_*_URL`
# makes it print one line and return green. That is correct offline and catastrophic in the live
# lane, where it turns a whole tier into "N passed" with ZERO database contact. In a live run a
# skip is a FAILURE.
#
# The match is deliberately CASE-INSENSITIVE and word-anchored. The previous `grep -n 'skip:'`
# was one capitalization away from blind: nine suites in ferro-backend-mysql printed
# `SKIP <testname>: …` (uppercase, with the colon after the NAME), which that pattern never
# matched — the whole MySQL/MariaDB tier could skip silently and the lane still passed. Those nine
# are now lowercase `skip: …`, and this pattern would catch them either way.
#
# Anchoring rules, and why they are what they are:
#   - `(^|[^[:alnum:]_])` — "skip" must start a word. Without it, Rust test NAMES containing the
#     substring (`guard_skips_leading_comments`, `parse_pools_skips_pool_with_no_dsn…`,
#     `clean_reset_profile_none_skips_hygiene…`, `numeric_reemits_skipped_leading_zero_groups`)
#     match every run and the gate cries wolf until someone deletes it.
#   - `(:|[[:space:]]|$)` — "skip" must END there too, so `skipping`/`skipped`/`skip_leading_noise`
#     in ordinary log prose stay quiet while BOTH house styles (`skip: …` and `SKIP name: …`) hit.
set -euo pipefail
log="${1:?usage: assert-no-skips.sh <test-log>}"

# A log with no test results at all is the same silent no-op one layer up: grep finds nothing and
# the gate reports success for a run that never happened.
if ! grep -q 'test result:' "$log"; then
  echo "::error::${log} contains no 'test result:' line — the test run produced no suites at all" >&2
  exit 1
fi

if grep -niE '(^|[^[:alnum:]_])skip(:|[[:space:]]|$)' "$log"; then
  echo "::error::a live suite skipped itself — a FERRO_TEST_*_URL is missing, a backend is unreachable, or an in-flight guard could not prove execution" >&2
  exit 1
fi
echo "no-skip gate: every live suite made database contact"
