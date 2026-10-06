#!/usr/bin/env bash
# ci/check-suite-triage.sh — the gate for the D18 triage files (SPEC D18; E9, SPEC §22.2 (da)).
#
# The sibling of ci/check-incompatibilities-doc.sh, and it exists for the same reason. A triage entry
# is what lets a Ferro-only non-pass stand in a suite column that SPEC D18 still calls green, so an
# entry whose explanation has gone stale is worse than no entry: the gate keeps passing on a reason
# that is no longer true. Prose cannot be checked; citations can, and the format makes every entry
# carry one. This script fails the build when an entry is malformed or a citation stops resolving:
#
#   1. every entry has the five ` | `-separated fields compare-columns.php reads, a known kind, a
#      Class::method test glob and a non-empty reason
#   2. every `§22.2 (xx)` exists in the spec's changelog (spelled `**(xx) <title>**`)
#   3. every `docs/…` path exists
#   4. every `known:"…"` text appears verbatim in docs/known-incompatibilities.md — D18 admits a
#      Ferro-only non-pass as an INCOMPATIBILITY only when that page documents it, so an
#      `incompatibility` entry must point into the page, and editing the page out from under it fails
#   5. every `upstream:<path>:<line>:"…"` citation is well-formed, and a `driver-name` entry has one
#
# What it does NOT check, stated: whether an `upstream:` line really contains the quoted text. That
# needs the pinned upstream clone, which this lane does not have; testkit/dbal/compare-columns.php
# checks it on every suite run, against the clone the run just measured.
#
# It needs no toolchain and no backend — a file check, like ci/check-d12-recorded.sh — and it is
# written to the same conventions as ci/check-incompatibilities-doc.sh.
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
spec="$root/ferro-spec-v0.2.md"
known="$root/docs/known-incompatibilities.md"
fail=0
n_entries=0

bad() { printf 'FAIL  %s\n' "$*" >&2; fail=1; }

files=("$@")
if [ ${#files[@]} -eq 0 ]; then
  files=("$root/testkit/dbal/triage.txt" "$root/testkit/laravel/triage.txt")
fi

for file in "${files[@]}"; do
  rel="${file#$root/}"
  [ -f "$file" ] || { bad "$rel is missing"; continue; }
  ln=0
  entries=0
  while IFS= read -r raw || [ -n "$raw" ]; do
    ln=$((ln + 1))
    line="${raw#"${raw%%[![:space:]]*}"}"      # trim leading whitespace
    case "$line" in ''|'#'*) continue ;; esac
    entries=$((entries + 1))
    where="$rel:$ln"
    # Split on the exact separator compare-columns.php splits on.
    IFS=$'\x1f' read -r -a f <<< "${line// | /$'\x1f'}"
    if [ "${#f[@]}" -ne 5 ]; then
      bad "$where: expected 5 fields separated by ' | ', got ${#f[@]}"
      continue
    fi
    cols="${f[0]}" test="${f[1]}" kind="${f[2]}" cites="${f[3]}" reason="${f[4]}"
    for c in ${cols//,/ }; do
      [[ "$c" =~ ^[a-z0-9*.-]+$ ]] || bad "$where: bad column glob '$c'"
    done
    [[ "$test" == *::* ]] || bad "$where: the test glob must be Class::method"
    [ -n "${reason// /}" ] || bad "$where: a reason is required"
    case "$kind" in
      driver-name|incompatibility) ;;
      *) bad "$where: kind must be driver-name or incompatibility, got '$kind'" ;;
    esac
    has_known=0 has_upstream=0
    IFS=';' read -r -a cl <<< "$cites"
    for c in "${cl[@]}"; do
      c="${c#"${c%%[![:space:]]*}"}"; c="${c%"${c##*[![:space:]]}"}"
      if [[ "$c" =~ ^§22\.2\ (\([a-z]+\))$ ]]; then
        grep -qF "**${BASH_REMATCH[1]} " "$spec" || bad "$where: §22.2 ${BASH_REMATCH[1]} is cited but not present in ferro-spec-v0.2.md"
      elif [[ "$c" =~ ^docs/[^[:space:]]+$ ]]; then
        [ -e "$root/$c" ] || bad "$where: cited path does not exist: $c"
      elif [[ "$c" =~ ^known:\"([^\"]+)\"$ ]]; then
        has_known=1
        grep -qF -- "${BASH_REMATCH[1]}" "$known" \
          || bad "$where: docs/known-incompatibilities.md does not contain: ${BASH_REMATCH[1]}"
      elif [[ "$c" =~ ^upstream:[^:[:space:]]+:[0-9]+:\"[^\"]+\"$ ]]; then
        has_upstream=1
      else
        bad "$where: unrecognised citation '$c'"
      fi
    done
    [ "$kind" = driver-name ] && [ "$has_upstream" = 0 ] \
      && bad "$where: a driver-name entry must cite the upstream line that branches on the name (upstream:…)"
    [ "$kind" = incompatibility ] && [ "$has_known" = 0 ] \
      && bad "$where: an incompatibility entry must cite docs/known-incompatibilities.md (known:\"…\") — D18"
  done < "$file"
  [ "$entries" -gt 0 ] || bad "$rel has no entries"
  n_entries=$((n_entries + entries))
done

if [ "$fail" -ne 0 ]; then
  echo "" >&2
  echo "a D18 triage file has a malformed entry or a dead citation (see above)." >&2
  exit 1
fi
echo "suite triage gate: ${#files[@]} files, $n_entries entries, every citation resolves (upstream lines are checked by the suite runs)"
