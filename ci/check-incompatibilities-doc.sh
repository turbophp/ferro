#!/usr/bin/env bash
# ci/check-incompatibilities-doc.sh — the gate for docs/known-incompatibilities.md (SPEC §14–15).
#
# A published incompatibilities page rots in a way that is WORSE than an out-of-date README: it
# tells a reader that a working feature is broken, and they believe it and work around it. That is
# not hypothetical here — at C6 this page still said a `bigint` at or above 2^32 "currently cannot
# be READ", a HIGH-severity defect that M1-S9 had fixed, with a live test proving the whole int64
# range. Nothing was watching, so nothing said so.
#
# Prose cannot be checked by a script. CITATIONS can, and this page's convention is that every
# entry carries one, so the gate checks that every citation still RESOLVES:
#
#   1. every relative Markdown link, and every backticked `docs/…` path, points at a file that exists
#   2. every `§22.2 (xx)` reference exists in the spec's changelog
#   3. every backticked fully-qualified `Ferro\…` name resolves to a real PHP declaration
#   4. every cited `SomeTest` / `SomeTest::testFoo` is one of OUR tests and still has that method
#   5. every follow-up under docs/followups/ declares a STATUS, and the page cites only OPEN ones
#
# Rule 5 is the one that catches the rot above. A follow-up that has been fixed is not deleted (the
# measurement it records is worth keeping) but it stops being citable as a live defect: the page
# points at the SPEC §22.2 entry that closed it instead. So fixing a defect and forgetting the page
# turns this gate RED rather than leaving a false warning in front of users.
#
# It needs no toolchain and no backend — it is a file check, like ci/check-d12-recorded.sh.
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
doc="$root/docs/known-incompatibilities.md"
spec="$root/ferro-spec-v0.2.md"
followups="$root/docs/followups"
fail=0

bad() { printf 'FAIL  %s\n' "$*" >&2; fail=1; }
note() { printf '      %s\n' "$*" >&2; }

[ -f "$doc" ] || { echo "FAIL  docs/known-incompatibilities.md is missing" >&2; exit 1; }

# Names the page mentions ON PURPOSE while they do not exist. Each needs a reason, because an
# unexplained entry here is how a gate stops gating.
allow_missing_name() {
  case "$1" in
    # SPEC §14 names it as the planned first-class replacement for pdo_pgsql's COPY hacks, and the
    # page says in so many words that it does not exist yet. Deferred, not missing.
    'Ferro\Pg\Copy') return 0 ;;
    *) return 1 ;;
  esac
}

# ---- 1. paths -----------------------------------------------------------------------------------
links=$(grep -oE '\]\(([^)h][^)]*)\)' "$doc" | sed -E 's/^\]\((.*)\)$/\1/' | sed 's/#.*//' | sort -u)
for l in $links; do
  [ -e "$root/docs/$l" ] || bad "broken Markdown link: docs/$l"
done
paths=$(grep -oE '`docs/[^`]+\.md`' "$doc" | tr -d '`' | sort -u)
for p in $paths; do
  [ -e "$root/$p" ] || bad "broken path citation: $p"
done

# ---- 2. spec changelog refs ---------------------------------------------------------------------
# A citation may list several letters: `SPEC §22.2 (am), (ar)`. Checking only the first is how a
# check passes while doing nothing, so the whole comma-list is expanded.
refs=$(grep -oE '§22\.2 \([a-z]+\)(, \([a-z]+\))*' "$doc" | grep -oE '\([a-z]+\)' | sort -u)
for r in $refs; do
  # The changelog spells an entry `**(ae) <title>**`, so the letter is followed by a space.
  grep -qF "**$r " "$spec" || bad "§22.2 $r is cited but not present in ferro-spec-v0.2.md"
done

# ---- 3. PHP names -------------------------------------------------------------------------------
# Only fully-qualified names with at least one separator are checked; a bare `Ferro\Client` in prose
# is a namespace, not a class.
#
# TWO things this check has to get right, and the first draft got both wrong:
#   * `php/*/vendor` is gitignored, so it exists on a developer's machine and NOT in the `rust` CI
#     lane, which never runs `composer install`. Grepping it means the gate can pass locally and
#     fail in CI — or, worse, pass in BOTH for the wrong reason.
#   * matching the LEAF name alone is far too loose. `Connection` is declared a dozen times inside
#     doctrine/dbal's own tree, so a cited `Ferro\DBAL\Nonexistent\Connection` would have passed.
# So: only this repository's own sources, and the NAMESPACE has to match as well as the leaf.
php_own() { find "$root/php" -name vendor -prune -o -name '*.php' -print; }
# NOTE: the trailing word boundary is passed as an ARGUMENT, not inside the format string —
# printf expands `\b` in a FORMAT to a backspace, which silently produced a regex that matched
# nothing and reported every cited name as missing.
decl_re() {
  printf '%s%s%s' '^ *(final +)?(abstract +)?(readonly +)?(class|interface|enum|trait) +' "$1" '\b'
}

for c in $(grep -oE '`Ferro(\\[A-Za-z]+)+`' "$doc" | tr -d '`' | sort -u); do
  allow_missing_name "$c" && continue
  leaf="${c##*\\}"
  ns="${c%\\*}"
  ns_re=$(printf '%s' "$ns" | sed 's/\\/\\\\/g')
  found=""
  while IFS= read -r f; do
    grep -qE "^namespace +$ns_re *;" "$f" || continue
    grep -qE "$(decl_re "$leaf")" "$f" && { found=1; break; }
  done < <(php_own)
  # A name with no declaration may still be a NAMESPACE the page refers to in prose.
  if [ -z "$found" ]; then
    ns_self=$(printf '%s' "$c" | sed 's/\\/\\\\/g')
    while IFS= read -r f; do
      grep -qE "^namespace +$ns_self( *;|\\\\)" "$f" && { found=1; break; }
    done < <(php_own)
  fi
  [ -n "$found" ] || bad "cited PHP name does not exist: $c"
done

# ---- 4. cited tests -----------------------------------------------------------------------------
# A bare backticked `SomeTest` / `SomeTest::testFoo` means one of OUR tests. An UPSTREAM test is
# written fully qualified (`Doctrine\DBAL\Tests\…`) and is deliberately not matched here: it lives
# under `php/*/vendor`, which is gitignored and absent from this lane, so resolving it would be a
# check that passes only on a developer's machine.
for t in $(grep -oE '`[A-Za-z_][A-Za-z0-9_]*Test(::[A-Za-z0-9_]+)?`' "$doc" | tr -d '`' | sort -u); do
  cls="${t%%::*}"
  meth=""
  [ "$t" != "$cls" ] && meth="${t#*::}"
  # Our own tests only — `php/*/vendor` carries upstream's suites and is absent in the CI lane.
  file=$(php_own | xargs grep -lE "$(decl_re "$cls")" 2>/dev/null | head -1)
  if [ -z "$file" ]; then
    bad "cited test class does not exist: $cls"
  elif [ -n "$meth" ] && ! grep -qE "function $meth *\(" "$file"; then
    bad "cited test method does not exist on $cls: $t"
  fi
done

# ---- 5. follow-up status ------------------------------------------------------------------------
# Every follow-up declares one of these on its own first status line.
for f in "$followups"/*.md; do
  [ -e "$f" ] || continue
  st=$(grep -m1 -oE '\*\*STATUS: (OPEN|RESOLVED)\b' "$f" | grep -oE '(OPEN|RESOLVED)')
  if [ -z "$st" ]; then
    bad "follow-up declares no status: ${f#$root/} (needs a '> **STATUS: OPEN**' or '> **STATUS: RESOLVED' line)"
    continue
  fi
  rel="docs/followups/$(basename "$f")"
  if grep -qF "$rel" "$doc" && [ "$st" = "RESOLVED" ]; then
    bad "known-incompatibilities.md cites a RESOLVED follow-up as a live defect: $rel"
    note "cite the SPEC §22.2 entry that closed it instead, and rewrite the entry to say it is fixed"
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "" >&2
  echo "docs/known-incompatibilities.md has stale citations (see above)." >&2
  exit 1
fi

echo "incompatibilities doc gate: citations resolve; $(ls "$followups"/*.md 2>/dev/null | wc -l | tr -d ' ') follow-ups all declare a status"
