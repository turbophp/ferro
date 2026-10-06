<?php // testkit/dbal/compare-columns.php
declare(strict_types=1);

/**
 * Compare two suite columns — a Ferro one and its control — test by test, from their JUnit logs
 * (`testkit/{dbal,laravel,orm}-suite.sh … --log-junit <file>`). M2-C5b, SPEC §22.2 (bz); the D18
 * gate is E9's, SPEC §22.2 (da).
 *
 * THREE MODES.
 *
 *   php compare-columns.php <ferro.xml> <control.xml>
 *     The REPORT (C5b). Prints, for each outcome, the tests in one column and not the other, plus a
 *     digest of each sorted skip and failure list. Exits 1 when a test the CONTROL ran is SKIPPED
 *     under Ferro, or when a failure shared with the control has a different exception TYPE; 0
 *     otherwise. Failures alone do not set the status in this mode. Unchanged since C1f — the ORM
 *     workflow reads it.
 *
 *   php compare-columns.php <ferro.xml> <control.xml> --triage <file> --column <key> [--upstream <dir>]
 *     THE D18 GATE (E9). SPEC D18 calls a suite column green when it has PARITY with a stock-driver
 *     control (read with §21's open item on D18's clauses, which this gate's interim reading
 *     follows): every DIFFERENCE between the columns must be explained by a checked-in triage entry
 *     whose expectation matches what Ferro actually did. A difference is any of:
 *       - Ferro FAILS a test the control passes, skips or did not collect;
 *       - both FAIL, with a different exception type, or — for PHPUnit's own assertion types, which
 *         every assertion failure shares — a different normalised first message line (E9 review F3);
 *       - Ferro SKIPS, or never collects, a test the control RAN (C5b's rule);
 *       - Ferro RUNS AND PASSES a test the control skipped or did not collect (E9 review F6 — D18's
 *         skip clause is about the two skip SETS, so a difference in either direction counts).
 *     Exit 1 when a difference has no matching entry, or matches only an entry of kind `defect`
 *     (a known Ferro defect keeps the column RED). `--upstream` is the pinned clone: every
 *     `upstream:`/`upstream-class:` citation of an entry that applies to this column is checked
 *     against it.
 *
 *   php compare-columns.php <run2.xml> <run1.xml> --repro
 *     REPRODUCIBILITY: exits 1 unless the two runs have the same tests with the same outcomes —
 *     the pass, fail and skip SETS equal, not merely their sizes.
 *
 * Exit 2 on unusable input, in every mode: an unreadable or empty JUnit file, a malformed triage
 * file, or a triage citation that does not resolve.
 *
 * THE TRIAGE FILE FORMAT (one entry per line; `#` comments and blank lines ignored):
 *
 *   <column globs> | <test> | <kind> | <expect> | <citations> | <reason>
 *
 *   column globs  comma-separated column keys, `*` the only wildcard (`dbal*-pg,dbal*-mysql`)
 *   test          `Fully\Qualified\TestClass::testMethod`, exact. Exactly two wildcards exist (E9
 *                 review F1 — a method glob once excused four ungated SQL-injection tests):
 *                   `…::testMethod with data set *`  every data set of ONE method;
 *                   `…::*`                           every method of one class, and ONLY with an
 *                                                    `upstream-class:` citation — a gate that
 *                                                    applies to the whole class.
 *   kind          `driver-name`      upstream code branching on the driver NAME (needs an
 *                                    `upstream:` or `upstream-class:` citation);
 *                 `incompatibility`  documented in docs/known-incompatibilities.md (needs `known:`);
 *                 `defect`           a known Ferro defect (needs an OPEN docs/followups/ file);
 *                                    it is reported by name and keeps the column RED.
 *   expect        what Ferro did, which the entry excuses and nothing else (E9 review F2):
 *                   `skip`                      skipped, or did not collect, a test the control ran
 *                   `pass`                      ran and passed a test the control skipped
 *                   `fail <Type>`               failed with exactly this JUnit exception type
 *                   `fail <Type> /<regex>/`     … and a first message line matching the regex
 *                 `skip` and `pass` are skip-SET differences, and D18 admits only a driver-name
 *                 gate for those, so they require kind `driver-name`.
 *   citations     `;`-separated, each one of
 *                   §22.2 (xx)                              a SPEC changelog entry that exists
 *                   docs/<path>                             a file that exists
 *                   known:"<text>"                          text in docs/known-incompatibilities.md
 *                   upstream:<path>:<line>:"<text>"         that clone line contains <text>
 *                   upstream-class:<path>:<line>:"<text>"   … and it is a CLASS-level gate: the next
 *                                                           code line declares the class
 *   reason        free text, required
 *
 * ci/check-suite-triage.sh checks the same format and every citation it can without the clone, per
 * push; ci/test-suite-gate.sh runs this file against testkit/dbal/gate-fixtures/, per push.
 */

$repo = dirname(__DIR__, 2);

/** @var array<string,array<string,array{0:string,1:string}>> column => test id => [type, first message line] */
$causes = [];

/** @return array<string,string> test id => pass|fail|skip */
function outcomes(string $file, string $column, array &$causes): array
{
    $xml = @simplexml_load_file($file);
    if ($xml === false) {
        fwrite(STDERR, "cannot read JUnit XML: $file\n");
        exit(2);
    }
    $out = [];
    foreach ($xml->xpath('//testcase') ?: [] as $tc) {
        // The FULL class name: both columns run the same tree, and two upstream classes share a
        // short name (`PrimaryReadReplicaConnectionTest` exists in two namespaces).
        $id = (string) $tc['class'] . '::' . (string) $tc['name'];
        $state = 'pass';
        if (isset($tc->failure) || isset($tc->error)) {
            $state = 'fail';
            $el = isset($tc->error) ? $tc->error : $tc->failure;
            // JUnit's text is "<test id>\n<message>\n\n<trace>": the message is the SECOND line.
            $lines = explode("\n", (string) $el);
            // [2] is the WHOLE message — every line up to the blank line before the trace — because
            // for an assertion the first line is only "Failed asserting that two strings are
            // identical." and the expected/actual diff beneath it is the cause.
            $msg = [];
            for ($k = 1; $k < count($lines) && trim($lines[$k]) !== ''; $k++) {
                $msg[] = rtrim($lines[$k]);
            }
            $causes[$column][$id] = [(string) $el['type'], trim($lines[1] ?? ''), implode("\n", $msg)];
        } elseif (isset($tc->skipped)) {
            $state = 'skip';
        }
        if (isset($out[$id])) {
            fwrite(STDERR, "duplicate test case $id in $file\n");
            exit(2);
        }
        $out[$id] = $state;
    }
    if ($out === []) {
        fwrite(STDERR, "no test cases in $file — the run did not measure anything\n");
        exit(2);
    }
    ksort($out);
    return $out;
}

/** @param array<string,string> $o @return list<string> */
function having(array $o, string $state): array
{
    return array_keys(array_filter($o, static fn (string $s): bool => $s === $state));
}

/** A glob with `*` as the ONLY wildcard — test names carry `[`, `#` and `\`, which fnmatch would read. */
function glob_re(string $glob): string
{
    return '/^' . str_replace('\*', '.*', preg_quote($glob, '/')) . '$/s';
}

/**
 * A message line with what legitimately differs between two runs of the SAME cause removed: file
 * paths (each column runs from its own checkout and temp dirs), object hashes and whitespace.
 */
function normalise_message(string $m): string
{
    $m = preg_replace('~(?:/[\w.@+-]+){2,}~', '<path>', $m) ?? $m;
    $m = preg_replace('/0x[0-9a-f]+/i', '<hex>', $m) ?? $m;
    return trim(preg_replace('/\s+/', ' ', $m) ?? $m);
}

/** Do two failures of the same test share a cause? Type first; PHPUnit's own types also by message. */
function same_cause(array $f, array $c): bool
{
    if ($f[0] !== $c[0]) {
        return false;
    }
    // Every assertion failure is `PHPUnit\Framework\ExpectationFailedException` (or a sibling), so
    // the type alone says nothing about WHICH assertion failed or why.
    if (str_starts_with($f[0], 'PHPUnit\\')) {
        return normalise_message($f[2]) === normalise_message($c[2]);
    }
    return true;
}

/** A parse or citation failure in the triage file. Never a verdict: exit 2, not 1. */
function triage_die(string $file, int $line, string $why): never
{
    fwrite(STDERR, "triage file $file:$line: $why\n");
    exit(2);
}

/** @return array{outcome:string,type:?string,re:?string} */
function parse_expect(string $file, int $n, string $x): array
{
    if ($x === 'skip' || $x === 'pass') {
        return ['outcome' => $x, 'type' => null, 're' => null];
    }
    if (preg_match('~^fail ([A-Za-z_\\\\][A-Za-z0-9_\\\\]*)(?: (/.+/[a-z]*))?$~', $x, $m) !== 1) {
        triage_die($file, $n, "expect must be `skip`, `pass`, `fail <Type>` or `fail <Type> /<regex>/`, got '$x'");
    }
    $re = $m[2] ?? null;
    if ($re !== null && @preg_match($re, '') === false) {
        triage_die($file, $n, "expect regex does not compile: $re");
    }
    return ['outcome' => 'fail', 'type' => $m[1], 're' => $re];
}

/**
 * @return list<array{line:int,columns:list<string>,test:string,kind:string,expect:array,cites:list<string>,reason:string}>
 */
function load_triage(string $file): array
{
    $text = @file_get_contents($file);
    if ($text === false) {
        fwrite(STDERR, "cannot read triage file: $file\n");
        exit(2);
    }
    $entries = [];
    foreach (explode("\n", $text) as $i => $raw) {
        $n = $i + 1;
        $l = trim($raw);
        if ($l === '' || $l[0] === '#') {
            continue;
        }
        $f = array_map('trim', explode(' | ', $l));
        if (count($f) !== 6) {
            triage_die($file, $n, sprintf('expected 6 fields separated by " | ", got %d', count($f)));
        }
        [$cols, $test, $kind, $expectField, $citeField, $reason] = $f;
        $columns = array_map('trim', explode(',', $cols));
        foreach ($columns as $c) {
            if (preg_match('/^[a-z0-9*.-]+$/', $c) !== 1) {
                triage_die($file, $n, "bad column glob '$c'");
            }
        }
        if (preg_match('/^([^:*\s][^:*]*)::([^*]+|\*)$/', preg_replace('/ with data set \*$/', ' with data set X', $test) ?? '') !== 1) {
            triage_die($file, $n, "the test must be Class::method — `*` only as a trailing ` with data set *` or as the whole method (`Class::*`), never inside a class or method name: '$test'");
        }
        if (! in_array($kind, ['driver-name', 'incompatibility', 'defect'], true)) {
            triage_die($file, $n, "kind must be driver-name, incompatibility or defect, got '$kind'");
        }
        $expect = parse_expect($file, $n, $expectField);
        if ($expect['outcome'] !== 'fail' && $kind !== 'driver-name') {
            triage_die($file, $n, "`{$expect['outcome']}` is a skip-set difference, and D18 admits only a driver-name gate for one");
        }
        if ($reason === '') {
            triage_die($file, $n, 'a reason is required');
        }
        // Each citation is one recognised token; anything left over is a malformed citation, never
        // silently dropped — an unparsed citation is one nobody checks.
        $cites = [];
        foreach (array_map('trim', explode(';', $citeField)) as $c) {
            if (preg_match('/^(§22\.2 \([a-z]+\)|docs\/\S+|known:"[^"]+"|upstream(-class)?:[^:\s]+:[0-9]+:"[^"]+")$/u', $c) !== 1) {
                triage_die($file, $n, "unrecognised citation '$c'");
            }
            $cites[] = $c;
        }
        $has = static fn (string $p): bool => array_filter($cites, static fn ($c) => str_starts_with($c, $p)) !== [];
        if ($kind === 'driver-name' && ! $has('upstream:') && ! $has('upstream-class:')) {
            triage_die($file, $n, 'a driver-name entry must cite the upstream line that branches on the name (upstream:…)');
        }
        if ($kind === 'incompatibility' && ! $has('known:')) {
            triage_die($file, $n, 'an incompatibility entry must cite docs/known-incompatibilities.md (known:"…") — D18');
        }
        if ($kind === 'defect' && ! $has('docs/followups/')) {
            triage_die($file, $n, 'a defect entry must cite its OPEN follow-up under docs/followups/');
        }
        if (str_ends_with($test, '::*') && ! $has('upstream-class:')) {
            triage_die($file, $n, 'a whole-class entry (`Class::*`) needs an upstream-class: citation — a gate on the CLASS, not on one method');
        }
        $entries[] = ['line' => $n, 'columns' => $columns, 'test' => $test, 'kind' => $kind, 'expect' => $expect, 'cites' => $cites, 'reason' => $reason];
    }
    if ($entries === []) {
        fwrite(STDERR, "triage file $file has no entries\n");
        exit(2);
    }
    return $entries;
}

/** Resolve one entry's citations. `upstream:`/`upstream-class:` only when a clone root is given. */
function check_citations(string $file, array $e, string $repo, ?string $upstream): void
{
    static $spec = null, $known = null;
    $spec ??= (string) @file_get_contents("$repo/ferro-spec-v0.2.md");
    $known ??= (string) @file_get_contents("$repo/docs/known-incompatibilities.md");
    foreach ($e['cites'] as $c) {
        if (preg_match('/^§22\.2 (\([a-z]+\))$/', $c, $m) === 1) {
            // The changelog spells an entry `**(ae) <title>**` (ci/check-incompatibilities-doc.sh).
            str_contains($spec, "**{$m[1]} ") || triage_die($file, $e['line'], "§22.2 {$m[1]} is not in the spec");
        } elseif (str_starts_with($c, 'docs/')) {
            file_exists("$repo/$c") || triage_die($file, $e['line'], "cited path does not exist: $c");
            if ($e['kind'] === 'defect' && str_starts_with($c, 'docs/followups/')) {
                preg_match('/\*\*STATUS: OPEN\b/', (string) file_get_contents("$repo/$c")) === 1
                    || triage_die($file, $e['line'], "a defect entry cites a follow-up that is not OPEN: $c");
            }
        } elseif (preg_match('/^known:"(.+)"$/su', $c, $m) === 1) {
            str_contains($known, $m[1]) || triage_die($file, $e['line'], "docs/known-incompatibilities.md does not contain: {$m[1]}");
        } elseif ($upstream !== null && preg_match('/^upstream(-class)?:([^:]+):([0-9]+):"(.+)"$/su', $c, $m) === 1) {
            $lines = @file("$upstream/{$m[2]}", FILE_IGNORE_NEW_LINES);
            $lines !== false || triage_die($file, $e['line'], "upstream file does not exist in $upstream: {$m[2]}");
            $idx = (int) $m[3] - 1;
            $at = $lines[$idx] ?? null;
            ($at !== null && str_contains($at, $m[4]))
                || triage_die($file, $e['line'], "upstream {$m[2]}:{$m[3]} does not contain: {$m[4]}");
            if ($m[1] === '-class') {
                // The next line that is code — not another attribute, a comment or blank — must
                // declare the class: then the cited gate is on the class, not on one method.
                for ($i = $idx + 1; $i < count($lines); $i++) {
                    $t = trim($lines[$i]);
                    if ($t === '' || str_starts_with($t, '#[') || str_starts_with($t, '//') || str_starts_with($t, '*') || str_starts_with($t, '/*')) {
                        continue;
                    }
                    preg_match('/^(final |abstract |readonly )*class \w+/', $t) === 1
                        || triage_die($file, $e['line'], "upstream-class {$m[2]}:{$m[3]} is not a class-level gate (next code line: $t)");
                    break;
                }
            }
        }
    }
}

/** Does entry $e excuse what Ferro did? */
function expect_matches(array $e, string $what, ?array $cause): bool
{
    $x = $e['expect'];
    if ($what === 'skip' || $what === 'not collected') {
        return $x['outcome'] === 'skip';
    }
    if ($what === 'pass') {
        return $x['outcome'] === 'pass';
    }
    // a failure
    if ($x['outcome'] !== 'fail' || $cause === null || $cause[0] !== $x['type']) {
        return false;
    }
    return $x['re'] === null || preg_match($x['re'], strip_type_echo($cause[0], $cause[2])) === 1;
}

/**
 * JUnit's message line for an error repeats the exception class ("Foo\Bar: Foo\Bar: message"); the
 * regex is matched against the message itself, with that echo removed.
 */
function strip_type_echo(string $type, string $m): string
{
    $p = $type . ': ';
    while ($p !== ': ' && str_starts_with($m, $p)) {
        $m = substr($m, strlen($p));
    }
    return $m;
}

// ---- arguments ---------------------------------------------------------------------------------
$pos = [];
$opt = ['triage' => null, 'column' => null, 'upstream' => null, 'repro' => false];
for ($i = 1; $i < $argc; $i++) {
    $a = $argv[$i];
    if ($a === '--repro') {
        $opt['repro'] = true;
    } elseif (in_array($a, ['--triage', '--column', '--upstream'], true)) {
        $v = $argv[++$i] ?? null;
        if ($v === null || $v === '') {
            fwrite(STDERR, "$a needs a value\n");
            exit(2);
        }
        $opt[substr($a, 2)] = $v;
    } else {
        $pos[] = $a;
    }
}
if (count($pos) !== 2 || (($opt['triage'] === null) !== ($opt['column'] === null))
    || ($opt['repro'] && $opt['triage'] !== null) || ($opt['upstream'] !== null && $opt['triage'] === null)) {
    fwrite(STDERR, "usage: php compare-columns.php <ferro.xml> <control.xml> [--triage <file> --column <key> [--upstream <dir>]]\n"
        . "       php compare-columns.php <run2.xml> <run1.xml> --repro\n");
    exit(2);
}
if ($opt['upstream'] !== null && ! is_dir($opt['upstream'])) {
    fwrite(STDERR, "--upstream is not a directory: {$opt['upstream']}\n");
    exit(2);
}
// The triage file is validated BEFORE the JUnit files are read, so a malformed file fails the same
// way whatever the run measured.
$triage = $opt['triage'] !== null ? load_triage($opt['triage']) : null;

$ferro = outcomes($pos[0], 'ferro', $causes);
$control = outcomes($pos[1], 'control', $causes);

$section = static function (string $title, array $names): void {
    printf("%s: %d\n", $title, count($names));
    foreach ($names as $n) {
        echo "  $n\n";
    }
};
$digest = static fn (array $l): string => substr(hash('sha256', implode("\n", $l)), 0, 16);

// ---- --repro -----------------------------------------------------------------------------------
if ($opt['repro']) {
    $rc = 0;
    if (array_keys($ferro) !== array_keys($control)) {
        $section('tests in only one run', array_merge(array_keys(array_diff_key($ferro, $control)), array_keys(array_diff_key($control, $ferro))));
        $rc = 1;
    }
    foreach (['pass', 'fail', 'skip'] as $s) {
        $a = having($ferro, $s);
        $b = having($control, $s);
        printf("%s: %d (sha256 %s) vs %d (sha256 %s)%s\n", $s, count($a), $digest($a), count($b), $digest($b), $a === $b ? '' : '  DIFFERENT');
        if ($a !== $b) {
            $section("  $s in one run only", array_values(array_merge(array_diff($a, $b), array_diff($b, $a))));
            $rc = 1;
        }
    }
    echo $rc === 0 ? "RESULT: the two runs agree, test by test.\n" : "RESULT: the two runs DIFFER — this column is not reproducible.\n";
    exit($rc);
}

// ---- the report (every non-repro mode) ---------------------------------------------------------
printf("tests: ferro %d, control %d\n", count($ferro), count($control));
$section('in only one column', array_values(array_merge(
    array_map(static fn ($n) => "ferro only: $n", array_keys(array_diff_key($ferro, $control))),
    array_map(static fn ($n) => "control only: $n", array_keys(array_diff_key($control, $ferro))),
)));
foreach (['skip', 'fail'] as $state) {
    $f = having($ferro, $state);
    $c = having($control, $state);
    printf("%s: ferro %d (sha256 %s), control %d (sha256 %s)\n", $state, count($f), $digest($f), count($c), $digest($c));
    $section("  $state under ferro, not under control", array_values(array_diff($f, $c)));
    $section("  $state under control, not under ferro", array_values(array_diff($c, $f)));
}

$bothFail = array_values(array_intersect(having($ferro, 'fail'), having($control, 'fail')));
$differentType = [];
foreach ($bothFail as $n) {
    [$ft, $fm] = $causes['ferro'][$n];
    [$ct, $cm] = $causes['control'][$n];
    if ($ft !== $ct) {
        $differentType[] = $n;
        echo "  DIFFERENT CAUSE: $n\n    ferro:   $ft: $fm\n    control: $ct: $cm\n";
    } elseif ($fm !== $cm) {
        echo "  same type, different message: $n\n    ferro:   $fm\n    control: $cm\n";
    }
}
printf("fail in both columns: %d, of which a DIFFERENT exception type: %d\n", count($bothFail), count($differentType));

$skippedOnlyByFerro = array_values(array_filter(
    array_diff(having($ferro, 'skip'), having($control, 'skip')),
    static fn (string $n): bool => isset($control[$n]),
));

if ($triage === null) {
    $rc = 0;
    if ($skippedOnlyByFerro !== []) {
        echo "RESULT: a test the control runs is SKIPPED under Ferro — a defect until shown otherwise.\n";
        $rc = 1;
    } else {
        echo "RESULT: no test the control runs is skipped under Ferro.\n";
    }
    if ($differentType !== []) {
        echo "RESULT: a failure shared with the control has a DIFFERENT cause — the control does not attribute it.\n";
        $rc = 1;
    }
    exit($rc);
}

// ---- the D18 gate ------------------------------------------------------------------------------
$column = $opt['column'];
$applies = array_values(array_filter($triage, static function (array $e) use ($column): bool {
    foreach ($e['columns'] as $g) {
        if (preg_match(glob_re($g), $column) === 1) {
            return true;
        }
    }
    return false;
}));
foreach ($applies as $e) {
    check_citations($opt['triage'], $e, $repo, $opt['upstream']);
}

/** Every difference between the columns: test id => what Ferro did that the control did not. */
$diffs = [];
foreach (array_unique(array_merge(array_keys($ferro), array_keys($control))) as $id) {
    $f = $ferro[$id] ?? 'absent';
    $c = $control[$id] ?? 'absent';
    if ($f === 'fail' && $c !== 'fail') {
        $diffs[$id] = 'fail';
    } elseif ($f === 'fail' && ! same_cause($causes['ferro'][$id], $causes['control'][$id])) {
        $diffs[$id] = 'fail (not the control\'s cause)';
    } elseif (($f === 'skip' || $f === 'absent') && ($c === 'pass' || $c === 'fail')) {
        $diffs[$id] = $f === 'skip' ? 'skip' : 'not collected';
    } elseif ($f === 'pass' && ($c === 'skip' || $c === 'absent')) {
        $diffs[$id] = 'pass';
    }
}
ksort($diffs);

$used = [];
$counts = ['driver-name' => 0, 'incompatibility' => 0, 'defect' => 0];
$untriaged = [];
$defects = [];
echo "\n== D18 ($column): every difference from the control, against " . basename($opt['triage']) . "\n";
foreach ($diffs as $id => $what) {
    $cause = $causes['ferro'][$id] ?? null;
    $named = [];
    $hit = null;
    foreach ($applies as $e) {
        if (preg_match(glob_re($e['test']), $id) !== 1) {
            continue;
        }
        $named[] = $e;
        if ($hit === null && expect_matches($e, $what, $cause)) {
            $hit = $e;
        }
    }
    $detail = $cause !== null ? ' [' . substr("{$cause[0]}: {$cause[1]}", 0, 240) . ']' : '';
    $ctl = $control[$id] ?? 'absent';
    if ($hit === null) {
        $untriaged[] = $id;
        if ($named !== []) {
            // Named, but not for THIS outcome: the entry excuses a different failure, not this one.
            $lines = implode(', ', array_map(static fn ($e) => $e['line'], $named));
            echo "  MISMATCH        $what (control: $ctl): $id$detail — line(s) $lines name this test but expect something else\n";
        } else {
            echo "  UNTRIAGED       $what (control: $ctl): $id$detail\n";
        }
        continue;
    }
    $used[$hit['line']] = true;
    $counts[$hit['kind']]++;
    if ($hit['kind'] === 'defect') {
        $defects[] = $id;
    }
    printf("  %-15s %s (control: %s): %s  <- line %d\n", $hit['kind'], $what, $ctl, $id, $hit['line']);
}

$stale = array_values(array_filter($applies, static fn (array $e): bool => ! isset($used[$e['line']])));
if ($stale !== []) {
    echo "\n  triage entries for this column that matched nothing here (STALE here — informational: an entry\n"
        . "  can apply to another server version or run; delete it once no column needs it):\n";
    foreach ($stale as $e) {
        echo "    line {$e['line']}: {$e['test']}\n";
    }
}

// The literal first clause of D18, reported beside the gate rather than folded into it (§21's open
// item on D18): a documented incompatibility is, by definition, a control pass that does not pass.
$ctlPass = having($control, 'pass');
$notPassing = array_values(array_filter($ctlPass, static fn (string $n): bool => ($ferro[$n] ?? 'absent') !== 'pass'));
$skipF = having($ferro, 'skip');
$skipC = having($control, 'skip');
printf("\n  pass parity: the control passes %d; through Ferro %d of them do not pass\n", count($ctlPass), count($notPassing));
printf("  skip parity: %s (ferro %d, control %d)\n", $skipF === $skipC ? 'IDENTICAL sets' : 'sets differ', count($skipF), count($skipC));
printf("  failure set: ferro %d, control %d, shared %d (%d with a different exception type)\n",
    count(having($ferro, 'fail')), count(having($control, 'fail')), count($bothFail), count($differentType));

if ($untriaged === [] && $defects === []) {
    printf("D18 VERDICT %s: GREEN modulo triage — %d differences, %d driver-name, %d documented incompatibility%s\n",
        $column, count($diffs), $counts['driver-name'], $counts['incompatibility'],
        $notPassing === [] ? ', strict pass parity' : sprintf(' (strict pass parity NOT met: %d control passes do not pass through Ferro)', count($notPassing)));
    exit(0);
}
if ($untriaged === []) {
    printf("D18 VERDICT %s: RED — %d known Ferro defect(s), triaged as `defect` with an open follow-up: %s\n",
        $column, count($defects), implode(', ', $defects));
    exit(1);
}
printf("D18 VERDICT %s: NOT GREEN — %d differences have no matching triage entry%s. Each is a defect until\n"
    . "  shown otherwise: fix it, or add an entry to %s whose expectation matches what Ferro did.\n",
    $column, count($untriaged), $defects === [] ? '' : sprintf(' (and %d known defects)', count($defects)), $opt['triage']);
exit(1);
