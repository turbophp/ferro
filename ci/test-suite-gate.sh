#!/usr/bin/env bash
# ci/test-suite-gate.sh — the tests of the D18 gate itself (E9 review F7, SPEC §22.2 (da)).
#
# testkit/dbal/compare-columns.php decides whether a suite column is green, and ci/check-suite-triage.sh
# decides whether a triage entry may stand. Before this file neither had a test: the review found the
# gate GREEN on an injected failure in an ungated sibling test (F1), on a §19.3 fate change and a
# TypeError in triaged tests (F2), and every one of its own mutations of the gate — fail-vs-skip,
# not-collected, different-type, the empty-JUnit guard, a column glob that matched everything, a
# counts-only --repro — survived the evidence the slice had recorded.
#
# Each case below is a small synthetic column in testkit/dbal/gate-fixtures/ (a stand-in upstream tree
# included), the exit status it must produce, and text its output must contain — the REASON, because
# a status can be right for the wrong reason. Run per push in ci.yml's `rust` lane and by
# ci/local-gate.sh; needs PHP and nothing else — no backend, no daemon, no Composer. The fixtures were
# generated once and are committed as files (read them; each name says what it breaks).
set -uo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
fx="$root/testkit/dbal/gate-fixtures"
gate="$root/testkit/dbal/compare-columns.php"
fail=0
n=0

# case <expected exit> <label> <text the output must contain, or ""> <command…>
#
# The text is checked as well as the exit status, because a status alone can be right for the wrong
# reason — the review found exactly that (F4: a "driver-name without upstream" refusal that really
# refused a six-field line for having six fields).
case_() {
  local want="$1" label="$2" needle="$3"
  shift 3
  n=$((n + 1))
  local out
  out=$("$@" 2>&1)
  local got=$?
  if [ "$got" != "$want" ]; then
    printf 'FAIL  %-58s want exit %s, got %s\n' "$label" "$want" "$got" >&2
    fail=1
  elif [ -n "$needle" ] && [[ "$out" != *"$needle"* ]]; then
    printf 'FAIL  %-58s exit %s, but for another reason (no "%s")\n' "$label" "$got" "$needle" >&2
    fail=1
  else
    printf 'ok    %-58s exit %s\n' "$label" "$got"
  fi
}
# d18 <expected exit> <label> <needle> <ferro column file> [triage name] [column key]
d18() {
  local want="$1" label="$2" needle="$3" ferro="$4" triage="${5:-good}" col="${6:-fx-col}"
  case_ "$want" "$label" "$needle" php "$gate" "$fx/$ferro" "$fx/control.xml" \
    --triage "$fx/triage-$triage.txt" --column "$col" --upstream "$fx/upstream"
}

# ---- the gate on columns ---------------------------------------------------------------------------
d18 0 "green: every difference triaged and matched"         "GREEN modulo triage — 7 differences" ferro-green.xml
d18 0 "a shared assertion that differs only by a path"       "shared 3 (0 with" ferro-green.xml
d18 1 "A: Ferro fails a test the control skipped"            "UNTRIAGED       fail (control: skip): Fx\SuiteTest::testBothSkip" ferro-fail-vs-skip.xml
d18 1 "E: a test the control ran is not collected"           "UNTRIAGED       not collected (control: pass): Fx\SuiteTest::testCollected" ferro-not-collected.xml
d18 1 "F: a shared failure with another exception type"      "fail (not the control's cause) (control: fail): Fx\SuiteTest::testSharedError" ferro-different-type.xml
d18 1 "F3: a shared assertion failure with another diff"     "fail (not the control's cause) (control: fail): Fx\SuiteTest::testSharedAssert" ferro-assert-message.xml
d18 1 "F2: a triaged test fails with another TYPE"           "MISMATCH        fail (control: pass): Fx\SuiteTest::testRefused [TypeError" ferro-wrong-type.xml
d18 1 "F2: a triaged test fails with another MESSAGE"        "MISMATCH        fail (control: pass): Fx\SuiteTest::testRefused [Fx\Refusal" ferro-wrong-message.xml
d18 1 "F2: a name-gated SKIP entry does not excuse a fail"   "MISMATCH        fail (control: pass): Fx\SuiteTest::testNameGated" ferro-name-gate-fails.xml
d18 1 "F1: an untriaged sibling of a triaged test fails"     "UNTRIAGED       fail (control: pass): Fx\SuiteTest::testPlain" ferro-sibling-injected.xml
d18 1 "F6: Ferro runs and passes a test the control skips"   "UNTRIAGED       pass (control: skip): Fx\SuiteTest::testBothSkip" ferro-reverse-skip.xml
d18 1 "J': a test triaged \`fail\` that Ferro SKIPS"           "MISMATCH        skip (control: pass): Fx\SuiteTest::testRefused" ferro-fail-entry-skipped.xml
d18 1 "H: a column key that names no column (a typo)"        "D18 VERDICT fx-colx: NOT GREEN — 7 differences" ferro-green.xml good fx-colx
d18 1 "a known defect keeps the column RED"                  "RED — 1 known Ferro defect(s)" ferro-green.xml defect
case_ 2 "G: an empty JUnit column"                           "no test cases" php "$gate" "$fx/empty.xml" "$fx/empty.xml" --triage "$fx/triage-good.txt" --column fx-col
case_ 2 "G: an empty Ferro column against a real control"    "no test cases" php "$gate" "$fx/empty.xml" "$fx/control.xml" --triage "$fx/triage-good.txt" --column fx-col

# ---- the gate refuses a triage file it cannot hold a column to (exit 2), each for its own reason ----
d18 2 "refuses triage-method-glob"             "only as a trailing"                     ferro-green.xml method-glob
d18 2 "refuses triage-class-glob-plain-cite"   "needs an upstream-class: citation"      ferro-green.xml class-glob-plain-cite
d18 2 "refuses triage-class-cite-not-class"    "not a class-level gate (next code line: public function testNameGated"              ferro-green.xml class-cite-not-class
d18 2 "refuses triage-upstream-moved"          "SuiteTest.php:8 does not contain"       ferro-green.xml upstream-moved
d18 2 "refuses triage-skip-incompat"           "admits only a driver-name gate"         ferro-green.xml skip-incompat
d18 2 "refuses triage-bad-expect"              "expect must be"                         ferro-green.xml bad-expect
d18 2 "refuses triage-defect-resolved"         "follow-up that is not OPEN"             ferro-green.xml defect-resolved
d18 2 "refuses triage-dead-spec"               "(zz) is not in the spec"                ferro-green.xml dead-spec
d18 2 "refuses triage-dead-known"              "does not contain: acceptance gate, where nobody" ferro-green.xml dead-known
d18 2 "refuses triage-dead-path"               "cited path does not exist"              ferro-green.xml dead-path
d18 2 "refuses triage-driver-name-no-upstream" "must cite the upstream line"            ferro-green.xml driver-name-no-upstream
# E9 review R2-1: a `fail` regex is required, anchored, and too tight to match a neutral probe. Run
# against ferro-wrong-message.xml, the column each loose form once let through GREEN.
d18 2 "R2-1: refuses triage-regex-missing"      "needs a message regex"                 ferro-wrong-message.xml regex-missing
d18 2 "R2-1: refuses triage-regex-unanchored"   "must be anchored"                      ferro-wrong-message.xml regex-unanchored
d18 2 "R2-1: refuses triage-regex-dotstar"      "matches the probe \"\""                ferro-wrong-message.xml regex-dotstar
d18 2 "R2-1: refuses triage-regex-probe"        "matches the probe \"x\""               ferro-wrong-message.xml regex-probe
d18 2 "R2-1: refuses triage-regex-multiline"    "only the s, i and u flags"             ferro-wrong-message.xml regex-multiline
d18 2 "R2-1: refuses triage-regex-headline"     "bare assertion headline"               ferro-wrong-message.xml regex-headline
d18 2 "T5: refuses triage-bad-column-glob"      "bad column glob 'FX_COL'"              ferro-green.xml bad-column-glob
# E9 review R2-2: an upstream citation must be on the entry's OWN class, namespace and method.
d18 2 "R2-2: refuses triage-class-cite-wrong-class"     "the gate is on Fx\GatedTest, not on the entry's class Fx\SuiteTest" ferro-green.xml class-cite-wrong-class
d18 2 "R2-2: refuses triage-class-cite-wrong-namespace" "not on the entry's class Other\GatedTest" ferro-green.xml class-cite-wrong-namespace
d18 2 "R2-2: refuses triage-class-cite-comment"         "is a comment, which gates nothing"      ferro-green.xml class-cite-comment
d18 2 "R2-2: refuses triage-method-cite-wrong-method"   "on testNameGated(), not on the entry's method testCollected()" ferro-green.xml method-cite-wrong-method
d18 2 "R2-2: refuses triage-method-cite-wrong-class"    "in Fx\SuiteTest, not in the entry's class Fx\GatedTest" ferro-green.xml method-cite-wrong-class
d18 2 "R2-2: refuses triage-method-body-wrong-method"   "in testControlSkips(), not in the entry's method testNameGated()" ferro-green.xml method-body-wrong-method
d18 2 "R2-2: refuses triage-method-cite-outside-body"   "the line is in no method body"          ferro-green.xml method-cite-outside-body
d18 2 "R2-2: refuses triage-via-wrong-helper"          "in stockEnvironment(), which none of the entry's own upstream: lines names" ferro-green.xml via-wrong-helper
d18 2 "R2-2: refuses triage-via-without-upstream"       "needs the upstream: line on the entry's own method" ferro-green.xml via-without-upstream

# ---- --repro compares SETS, not sizes (mutation I) -------------------------------------------------
case_ 0 "repro: a column against itself"        "the two runs agree"       php "$gate" "$fx/ferro-green.xml" "$fx/ferro-green.xml" --repro
case_ 1 "I: repro, same counts, different sets" "pass in one run only: 2"  php "$gate" "$fx/ferro-swapped.xml" "$fx/ferro-green.xml" --repro
case_ 1 "repro: a test missing from one run"    "tests in only one run: 1" php "$gate" "$fx/ferro-not-collected.xml" "$fx/ferro-green.xml" --repro

# ---- the static check (no clone): what it can see, and what only the gate can ----------------------
st="$root/ci/check-suite-triage.sh"
case_ 0 "static: triage-good"                            "every citation resolves" "$st" "$fx/triage-good.txt"
case_ 0 "static: triage-defect (an OPEN follow-up)"      "every citation resolves" "$st" "$fx/triage-defect.txt"
case_ 1 "static: refuses triage-method-glob"             "only as a trailing"            "$st" "$fx/triage-method-glob.txt"
case_ 1 "static: refuses triage-class-glob-plain-cite"   "needs an upstream-class:"      "$st" "$fx/triage-class-glob-plain-cite.txt"
case_ 1 "static: refuses triage-skip-incompat"           "admits only a driver-name"     "$st" "$fx/triage-skip-incompat.txt"
case_ 1 "static: refuses triage-bad-expect"              "expect must be"                "$st" "$fx/triage-bad-expect.txt"
case_ 1 "static: refuses triage-defect-resolved"         "follow-up that is not OPEN"    "$st" "$fx/triage-defect-resolved.txt"
case_ 1 "static: refuses triage-dead-spec"               "(zz) is cited but not present" "$st" "$fx/triage-dead-spec.txt"
case_ 1 "static: refuses triage-dead-known"              "does not contain: acceptance gate, where nobody" "$st" "$fx/triage-dead-known.txt"
case_ 1 "static: refuses triage-dead-path"               "cited path does not exist"     "$st" "$fx/triage-dead-path.txt"
case_ 1 "static: refuses triage-driver-name-no-upstream" "must cite the upstream line"   "$st" "$fx/triage-driver-name-no-upstream.txt"
case_ 1 "static R2-1: refuses triage-regex-missing"      "needs a message regex"         "$st" "$fx/triage-regex-missing.txt"
case_ 1 "static R2-1: refuses triage-regex-unanchored"   "must be anchored"              "$st" "$fx/triage-regex-unanchored.txt"
case_ 1 "static R2-1: refuses triage-regex-dotstar"      "matches the probe \"\""        "$st" "$fx/triage-regex-dotstar.txt"
case_ 1 "static R2-1: refuses triage-regex-probe"        "matches the probe \"x\""       "$st" "$fx/triage-regex-probe.txt"
case_ 1 "static R2-1: refuses triage-regex-multiline"    "only the s, i and u flags"     "$st" "$fx/triage-regex-multiline.txt"
case_ 1 "static R2-1: refuses triage-regex-headline"     "bare assertion headline"       "$st" "$fx/triage-regex-headline.txt"
case_ 1 "static T5: refuses triage-bad-column-glob"      "bad column glob 'FX_COL'"      "$st" "$fx/triage-bad-column-glob.txt"
case_ 1 "static R2-2: refuses triage-via-without-upstream" "needs the upstream: line on the entry's own method" "$st" "$fx/triage-via-without-upstream.txt"
# Only the clone can show these are wrong; the static check is not expected to.
case_ 0 "static: cannot see triage-class-cite-not-class" "every citation resolves" "$st" "$fx/triage-class-cite-not-class.txt"
case_ 0 "static: cannot see triage-upstream-moved"       "every citation resolves" "$st" "$fx/triage-upstream-moved.txt"
for t in class-cite-wrong-class class-cite-wrong-namespace class-cite-comment method-cite-wrong-method method-cite-wrong-class \
         method-body-wrong-method method-cite-outside-body via-wrong-helper; do
  case_ 0 "static: cannot see triage-$t" "every citation resolves" "$st" "$fx/triage-$t.txt"
done
# And the real triage files.
case_ 0 "static: the checked-in triage files"            "2 files" "$st"

if [ "$fail" -ne 0 ]; then
  echo "" >&2
  echo "the D18 gate's own tests failed (see above)." >&2
  exit 1
fi
echo "suite gate tests: $n cases, all as expected"
