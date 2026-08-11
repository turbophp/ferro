#!/usr/bin/env bash
# The acceptance runner's OWN guards, exercised. `testkit/dbal-suite.sh` is the only thing standing
# between "a number" and "a recordable number", and every guard in it was added because the
# whole-branch review MEASURED the runner publishing a wrong number under a log that looked right.
# A guard nothing exercises is a comment.
#
# Each case below fails the run it is checking on purpose. Nothing here records a number, and — with
# the single exception noted in case 3 — nothing here touches a database.
#
#   FERRO_DBAL_SELFTEST_SKIP_DOCKER=1  drops case 3 (the only one that needs the containers up).
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runner="$root/testkit/dbal-suite.sh"
work="${FERRO_DBAL_WORK:-$root/.dbal-suite}"
mkdir -p "$work"
fails=0

ok()   { echo "  PASS  $1"; }
bad()  { echo "  FAIL  $1"; fails=$((fails + 1)); }

# 1. FERRO_DBAL_SVC vs FERRO_DBAL_DSN — the pair that used to be independent. MEASURED before the
#    fix: `FERRO_DBAL_SVC=mariadb ./testkit/dbal-suite.sh` (DSN forgotten) reset MariaDB, tested an
#    unreset PostgreSQL, printed BOTH load-bearing log lines, and published `Errors: 74` — a number
#    matching neither backend — under a log that reads as a MariaDB run.
echo "1) a DSN that disagrees with the svc is refused, BEFORE anything destructive"
out="$(FERRO_DBAL_SVC=mariadb FERRO_DBAL_DSN=postgres://ferro:ferro@127.0.0.1:55432/doctrine_tests \
       "$runner" 2>&1)"; status=$?
if [ "$status" -eq 0 ]; then bad "the mismatch was accepted (exit 0)"; else ok "refused, exit $status"; fi
case "$out" in *"FERRO_DBAL_SVC and FERRO_DBAL_DSN disagree"*) ok "the message names both variables" ;;
               *) bad "the message does not name the two variables: $out" ;; esac
case "$out" in *"[ferro] reset:"*) bad "IT RESET A DATABASE ON ITS WAY TO FAILING" ;;
               *) ok "no reset line — the refusal precedes the destructive step" ;; esac

# 2. …and the same command with the DSN simply OMITTED must now be self-consistent rather than
#    silently PostgreSQL. This is the shape a retyped or templated command actually takes.
#    A throwaway work dir + a tag that does not exist stops the run at the clone, so this case
#    cannot reach a database whatever it proves.
sandbox="$(mktemp -d)"
trap 'rm -rf "$sandbox"' EXIT
#    Asserted on the ENDPOINT the runner resolved, not merely on the absence of a complaint: a
#    default that ignored the svc (the shape this replaced — a hard-wired PostgreSQL DSN) also emits
#    no complaint, so "no error" is not evidence.
echo "2) the DSN defaults from the svc (the forgotten-variable case)"
for row in "pg postgres 55432" "mysql mysql 33060" "mariadb mysql 33061"; do
  set -- $row
  s="$1"; want="$2://127.0.0.1:$3/doctrine_tests"
  out="$(FERRO_DBAL_SVC=$s FERRO_DBAL_TAG=0.0.0-does-not-exist FERRO_DBAL_WORK="$sandbox" "$runner" 2>&1)"
  line="$(printf '%s' "$out" | grep -F '[ferro] backend:' | head -1)"
  case "$line" in *"$want"*) ok "svc=$s resolves to $want" ;;
                  *) bad "svc=$s resolved to '${line:-<no backend line>}', expected $want" ;; esac
  case "$out" in *"ferro:ferro"*|*"@127.0.0.1"*) bad "svc=$s: the log leaked DSN credentials" ;;
                 *) ok "svc=$s: no credentials in the log" ;; esac
  case "$out" in *"[ferro] reset:"*) bad "svc=$s: reached the reset with an unresolvable tag" ;; esac
done

# 3. The MySQL-family reset is FAIL-CLOSED. Proven by mutation, because the credential is not a knob
#    (a test-only escape hatch in the runner would be a second thing to trust): a copy of the runner
#    with the root password broken must exit non-zero and must NOT print the reset line. Before the
#    fix this arm ended in `2>&1 | grep -v 'Using a password' || true` and a failed reset published
#    `Errors: 16` under `[ferro] reset: …`.
if [ "${FERRO_DBAL_SELFTEST_SKIP_DOCKER:-0}" = 1 ]; then
  echo "3) SKIPPED (FERRO_DBAL_SELFTEST_SKIP_DOCKER=1)"
else
  echo "3) a FAILING MySQL-family reset stops the run and prints no reset line"
  mutant="$work/selftest-broken-credential.sh"
  sed 's/-uroot -pferro/-uroot -pWRONGPASS/' "$runner" > "$mutant"
  chmod +x "$mutant"
  grep -q 'WRONGPASS' "$mutant" || { bad "the mutation did not apply — this case would be vacuous"; }
  out="$(FERRO_DBAL_SVC=mysql "$mutant" 2>&1)"; status=$?
  rm -f "$mutant"
  if [ "$status" -eq 0 ]; then bad "a failed reset exited 0"; else ok "refused, exit $status"; fi
  case "$out" in *"[ferro] reset:"*) bad "it printed the reset line for a reset that FAILED" ;;
                 *) ok "no reset line" ;; esac
  case "$out" in *"the mysql reset FAILED"*) ok "the failure is named" ;;
                 *) bad "no diagnostic naming the failed reset" ;; esac
fi

# 4. The recording banner, which is printed before anything happens and therefore costs nothing to
#    exercise. It must tell a NARROWED run apart from a merely noisier one — `--filter` runs 1 test
#    of 730 with every documented log line still present and correct, `--display-skipped` runs all
#    730 — and it must call `--no-reset` out on its own.
echo "4) the recordable banner tells narrowing args apart from output-only ones"
banner() { # <args...> -> the banner line
  FERRO_DBAL_SVC=pg FERRO_DBAL_TAG=0.0.0-does-not-exist FERRO_DBAL_WORK="$sandbox" \
    "$runner" "$@" 2>&1 | grep -F '[ferro] recordable:' | head -1
}
case "$(banner)" in
  *"recordable: yes"*) ok "a plain run is recordable" ;;
  *) bad "a plain run was not marked recordable: $(banner)" ;;
esac
case "$(banner --display-skipped)" in
  *"recordable: yes"*) ok "--display-skipped does not narrow a run" ;;
  *) bad "an output-only flag was misclassified as narrowing" ;;
esac
case "$(banner --filter nope)" in
  *"recordable: NO"*"--filter"*) ok "--filter is called out by name" ;;
  *) bad "--filter did not make the run non-recordable: $(banner --filter nope)" ;;
esac
case "$(banner tests/Functional/WriteTest.php)" in
  *"recordable: NO"*) ok "a bare path argument is treated as narrowing (fail closed)" ;;
  *) bad "an unknown argument was assumed harmless" ;;
esac
case "$(banner --no-reset)" in
  *"recordable: NO"*"--no-reset"*) ok "--no-reset is called out by name" ;;
  *) bad "--no-reset did not make the run non-recordable" ;;
esac

echo
if [ "$fails" -eq 0 ]; then echo "dbal-suite selftest: OK"; else echo "dbal-suite selftest: $fails FAILED"; fi
exit "$fails"
