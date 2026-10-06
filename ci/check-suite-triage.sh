#!/usr/bin/env bash
# ci/check-suite-triage.sh — the gate for the D18 triage files (SPEC D18; E9, SPEC §22.2 (da)).
#
# The sibling of ci/check-incompatibilities-doc.sh, and it exists for the same reason. A triage entry
# is what lets a difference from the stock-driver control stand in a suite column that SPEC D18 still
# calls green, so an entry whose explanation has gone stale is worse than no entry: the gate keeps
# passing on a reason that is no longer true. Prose cannot be checked; citations can, and the format
# makes every entry carry one. This script fails the build when an entry is malformed or a citation
# stops resolving. It reads the format testkit/dbal/compare-columns.php documents at its top:
#
#   <column globs> | <test> | <kind> | <expect> | <citations> | <reason>
#
#   1. six fields; a known kind (driver-name, incompatibility, defect); a non-empty reason
#   2. the test is Class::method, with `*` ONLY as a trailing ` with data set *` or as the whole
#      method (`Class::*`), and `Class::*` only with an `upstream-class:` citation — a method glob
#      once excused four ungated SQL-injection tests along with the six gated ones it meant
#   3. the expectation is `skip`, `pass` or `fail <Type> /^<regex>/`, and `skip`/`pass` (skip-SET
#      differences) only on a driver-name entry — D18 admits no other explanation for one. A `fail`
#      regex is REQUIRED, `^`-anchored, flags s/i/u only, and matches none of the gate's neutral
#      probes (E9 review R2-1: `/.*/`, `/e/` or no regex at all excused a different message). That
#      rule is compare-columns.php's own expect_problem(), called through `--check-expects`, so the
#      two cannot disagree — which makes PHP the one tool this check needs.
#   4. every `§22.2 (xx)` exists in the spec's changelog (spelled `**(xx) <title>**`)
#   5. every `docs/…` path exists; a `defect` entry cites a follow-up under docs/followups/ whose
#      status is OPEN (a defect whose follow-up is resolved is not a defect any more)
#   6. every `known:"…"` text appears verbatim in docs/known-incompatibilities.md — D18 admits an
#      incompatibility only when that page documents it — and an incompatibility entry has one
#   7. every `upstream:`/`upstream-class:`/`upstream-via:` citation is well-formed, a driver-name
#      entry has an `upstream:` or `upstream-class:` one, and an `upstream-via:` (a line in a helper
#      the test names) comes with the `upstream:` line on the test's own method that names it
#
# What it does NOT check, stated: whether an upstream line really contains the quoted text, is code,
# and is a gate on the entry's OWN class or method (E9 review R2-2). That needs the pinned upstream
# clone, which this lane does not have; compare-columns.php checks all of it on every suite run,
# against the clone the run just measured.
#
# It needs PHP (for the regex rule above) and no backend — a file check, like
# ci/check-d12-recorded.sh — and it is written to the same conventions as
# ci/check-incompatibilities-doc.sh.
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
spec="$root/ferro-spec-v0.2.md"
known="$root/docs/known-incompatibilities.md"
fail=0
n_entries=0
expects=""   # `<where>\t<expect>` lines, judged by compare-columns.php --check-expects below

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
    if [ "${#f[@]}" -ne 6 ]; then
      bad "$where: expected 6 fields separated by ' | ', got ${#f[@]}"
      continue
    fi
    cols="${f[0]}" test="${f[1]}" kind="${f[2]}" expect="${f[3]}" cites="${f[4]}" reason="${f[5]}"
    for c in ${cols//,/ }; do
      [[ "$c" =~ ^[a-z0-9*.-]+$ ]] || bad "$where: bad column glob '$c'"
    done
    t="${test% with data set \*}"
    [[ "$t" =~ ^[^:*[:space:]][^:*]*::([^*]+|\*)$ ]] \
      || bad "$where: the test must be Class::method — '*' only as a trailing ' with data set *' or as the whole method: '$test'"
    [ -n "${reason// /}" ] || bad "$where: a reason is required"
    case "$kind" in
      driver-name|incompatibility|defect) ;;
      *) bad "$where: kind must be driver-name, incompatibility or defect, got '$kind'" ;;
    esac
    if [[ "$expect" == skip || "$expect" == pass ]]; then
      [ "$kind" = driver-name ] || bad "$where: '$expect' is a skip-set difference, and D18 admits only a driver-name gate for one"
    else
      expects+="$where"$'\t'"$expect"$'\n'
    fi
    has_known=0 has_upstream=0 has_class=0 has_method=0 has_via=0 has_followup=0
    IFS=';' read -r -a cl <<< "$cites"
    for c in "${cl[@]}"; do
      c="${c#"${c%%[![:space:]]*}"}"; c="${c%"${c##*[![:space:]]}"}"
      if [[ "$c" =~ ^§22\.2\ (\([a-z]+\))$ ]]; then
        grep -qF "**${BASH_REMATCH[1]} " "$spec" || bad "$where: §22.2 ${BASH_REMATCH[1]} is cited but not present in ferro-spec-v0.2.md"
      elif [[ "$c" =~ ^docs/[^[:space:]]+$ ]]; then
        if [ ! -e "$root/$c" ]; then
          bad "$where: cited path does not exist: $c"
        elif [[ "$c" == docs/followups/* ]]; then
          has_followup=1
          if [ "$kind" = defect ] && ! grep -qE '\*\*STATUS: OPEN\b' "$root/$c"; then
            bad "$where: a defect entry cites a follow-up that is not OPEN: $c"
          fi
        fi
      elif [[ "$c" =~ ^known:\"([^\"]+)\"$ ]]; then
        has_known=1
        grep -qF -- "${BASH_REMATCH[1]}" "$known" \
          || bad "$where: docs/known-incompatibilities.md does not contain: ${BASH_REMATCH[1]}"
      elif [[ "$c" =~ ^upstream-class:[^:[:space:]]+:[0-9]+:\"[^\"]+\"$ ]]; then
        has_upstream=1 has_class=1
      elif [[ "$c" =~ ^upstream:[^:[:space:]]+:[0-9]+:\"[^\"]+\"$ ]]; then
        has_upstream=1 has_method=1
      elif [[ "$c" =~ ^upstream-via:[^:[:space:]]+:[0-9]+:\"[^\"]+\"$ ]]; then
        has_via=1
      else
        bad "$where: unrecognised citation '$c'"
      fi
    done
    [ "$kind" = driver-name ] && [ "$has_upstream" = 0 ] \
      && bad "$where: a driver-name entry must cite the upstream line that branches on the name (upstream:…)"
    [ "$kind" = incompatibility ] && [ "$has_known" = 0 ] \
      && bad "$where: an incompatibility entry must cite docs/known-incompatibilities.md (known:\"…\") — D18"
    [ "$kind" = defect ] && [ "$has_followup" = 0 ] \
      && bad "$where: a defect entry must cite its OPEN follow-up under docs/followups/"
    [ "$has_via" = 1 ] && [ "$has_method" = 0 ] \
      && bad "$where: an upstream-via: citation needs the upstream: line on the entry's own method that names its helper"
    [[ "$test" == *::\* ]] && [ "$has_class" = 0 ] \
      && bad "$where: a whole-class entry (Class::*) needs an upstream-class: citation"
  done < "$file"
  [ "$entries" -gt 0 ] || bad "$rel has no entries"
  n_entries=$((n_entries + entries))
done

command -v php >/dev/null || { bad "php is required: the expect regex rule is compare-columns.php's own"; expects=""; }
if [ -n "$expects" ]; then
  while IFS= read -r why; do
    bad "$why"
  done < <(printf '%s' "$expects" | php "$root/testkit/dbal/compare-columns.php" --check-expects)
fi

if [ "$fail" -ne 0 ]; then
  echo "" >&2
  echo "a D18 triage file has a malformed entry or a dead citation (see above)." >&2
  exit 1
fi
echo "suite triage gate: ${#files[@]} files, $n_entries entries, every citation resolves (upstream lines are checked by the suite runs)"
