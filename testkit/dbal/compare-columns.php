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
 *     under Ferro, or when a failure shared with the control has a different exception TYPE (below);
 *     0 otherwise. Failures alone do not set the status in this mode: they are a triage question.
 *
 *   php compare-columns.php <ferro.xml> <control.xml> --triage <file> --column <key> [--upstream <dir>]
 *     THE D18 GATE (E9). SPEC D18 calls a suite column green when it has PARITY with a stock-driver
 *     control: every test the control passes passes through Ferro, the skip sets match or each
 *     difference is shown to be driver-name gated, and every Ferro-only non-pass is triaged as a
 *     driver-name artifact or a documented incompatibility. This mode prints the report above and
 *     then a per-column verdict, and exits 1 when any FERRO-ONLY NON-PASS is not covered by an entry
 *     of the checked-in triage file for this column. A Ferro-only non-pass is:
 *       - a test that fails under Ferro and passes, skips, or is absent under the control;
 *       - a test that fails in both, with a DIFFERENT exception type (C1f — see below);
 *       - a test the control RAN (pass or fail) that Ferro skipped or never collected.
 *     A Ferro-only SKIP must be triaged `driver-name`: D18's skip clause admits exactly that
 *     explanation, so an `incompatibility` entry does not excuse a skip and the gate says so.
 *     `--upstream` is the pinned upstream clone; every `upstream:` citation of an entry that applies
 *     to this column is checked against it (the file exists, the line exists and contains the
 *     quoted text), so a triage entry cannot keep pointing at a line upstream has moved.
 *
 *   php compare-columns.php <run2.xml> <run1.xml> --repro
 *     REPRODUCIBILITY (E9): exits 1 unless the two runs have the same tests with the same outcomes —
 *     pass, fail and skip sets all equal. It replaces the workflows' old digest-grepping.
 *
 * Exit 2 on unusable input, in every mode: an unreadable or empty JUnit file, a malformed triage
 * file, or a triage citation that does not resolve.
 *
 * A FAILURE SHARED WITH THE CONTROL IS ONLY ATTRIBUTED IF IT FAILED FOR THE SAME REASON (M2-C1f).
 * Two `testBasicUpdateForJson` cases failed in BOTH MySQL-family columns and so read as "upstream's
 * own" — the control on MariaDB's syntax error for `cast(? as json)`, the Ferro column on the shim's
 * `quote()` refusal, which had nothing to do with it. So every test that fails in both columns is
 * compared by the exception TYPE JUnit records. The first message line is printed beside it when it
 * differs, for triage, but does not decide anything: messages carry paths and values that
 * legitimately differ between two runs of the same cause.
 *
 * THE TRIAGE FILE FORMAT (one entry per line; `#` comments and blank lines ignored):
 *
 *   <column globs> | <test glob> | <kind> | <citations> | <reason>
 *
 *   column globs  comma-separated column keys, `*` the only wildcard (`dbal*-pg,dbal*-mysql`)
 *   test glob     `Fully\Qualified\TestClass::testMethod`, `*` the only wildcard (data sets are part
 *                 of the method name: `testFoo with data set #0`)
 *   kind          `driver-name` (upstream code branching on the driver NAME; needs an `upstream:`
 *                 citation) or `incompatibility` (documented in docs/known-incompatibilities.md;
 *                 needs a `known:` citation) — the two explanations D18 admits
 *   citations     `;`-separated, each one of
 *                   §22.2 (xx)                        a SPEC changelog entry that exists
 *                   docs/<path>                       a file that exists
 *                   known:"<text>"                    text that appears in docs/known-incompatibilities.md
 *                   upstream:<path>:<line>:"<text>"   that line of the pinned upstream clone contains <text>
 *   reason        free text, required
 *
 * ci/check-suite-triage.sh checks the same format and every citation except `upstream:` (which needs
 * the clone) in the per-push `rust` lane, so a dead citation fails CI before any suite runs.
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
            $causes[$column][$id] = [(string) $el['type'], trim($lines[1] ?? '')];
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

/** A parse or citation failure in the triage file. Never a verdict: exit 2, not 1. */
function triage_die(string $file, int $line, string $why): never
{
    fwrite(STDERR, "triage file $file:$line: $why\n");
    exit(2);
}

/**
 * @return list<array{line:int,columns:list<string>,test:string,kind:string,cites:list<string>,reason:string}>
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
        if (count($f) !== 5) {
            triage_die($file, $n, sprintf('expected 5 fields separated by " | ", got %d', count($f)));
        }
        [$cols, $test, $kind, $citeField, $reason] = $f;
        $columns = array_map('trim', explode(',', $cols));
        foreach ($columns as $c) {
            if (preg_match('/^[a-z0-9*.-]+$/', $c) !== 1) {
                triage_die($file, $n, "bad column glob '$c'");
            }
        }
        if ($test === '' || ! str_contains($test, '::')) {
            triage_die($file, $n, 'the test glob must be Class::method');
        }
        if (! in_array($kind, ['driver-name', 'incompatibility'], true)) {
            triage_die($file, $n, "kind must be driver-name or incompatibility, got '$kind'");
        }
        if ($reason === '') {
            triage_die($file, $n, 'a reason is required');
        }
        // Each citation is one recognised token; anything left over is a malformed citation, never
        // silently dropped — an unparsed citation is one nobody checks.
        $cites = [];
        foreach (array_map('trim', explode(';', $citeField)) as $c) {
            if (preg_match('/^(§22\.2 \([a-z]+\)|docs\/\S+|known:"[^"]+"|upstream:[^:\s]+:[0-9]+:"[^"]+")$/u', $c) !== 1) {
                triage_die($file, $n, "unrecognised citation '$c'");
            }
            $cites[] = $c;
        }
        $has = static fn (string $p): bool => array_filter($cites, static fn ($c) => str_starts_with($c, $p)) !== [];
        if ($kind === 'driver-name' && ! $has('upstream:')) {
            triage_die($file, $n, 'a driver-name entry must cite the upstream line that branches on the name (upstream:…)');
        }
        if ($kind === 'incompatibility' && ! $has('known:')) {
            triage_die($file, $n, 'an incompatibility entry must cite docs/known-incompatibilities.md (known:"…") — D18');
        }
        $entries[] = ['line' => $n, 'columns' => $columns, 'test' => $test, 'kind' => $kind, 'cites' => $cites, 'reason' => $reason];
    }
    if ($entries === []) {
        fwrite(STDERR, "triage file $file has no entries\n");
        exit(2);
    }
    return $entries;
}

/** Resolve one entry's citations. `upstream:` only when a clone root is given. */
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
        } elseif (preg_match('/^known:"(.+)"$/su', $c, $m) === 1) {
            str_contains($known, $m[1]) || triage_die($file, $e['line'], "docs/known-incompatibilities.md does not contain: {$m[1]}");
        } elseif ($upstream !== null && preg_match('/^upstream:([^:]+):([0-9]+):"(.+)"$/su', $c, $m) === 1) {
            $lines = @file("$upstream/{$m[1]}", FILE_IGNORE_NEW_LINES);
            $lines !== false || triage_die($file, $e['line'], "upstream file does not exist in $upstream: {$m[1]}");
            $at = $lines[(int) $m[2] - 1] ?? null;
            ($at !== null && str_contains($at, $m[3]))
                || triage_die($file, $e['line'], "upstream {$m[1]}:{$m[2]} does not contain: {$m[3]}");
        }
    }
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

/** Every Ferro-only non-pass: test id => [what kind of difference, ferro state, control state]. */
$ferroOnly = [];
foreach (array_unique(array_merge(array_keys($ferro), array_keys($control))) as $id) {
    $f = $ferro[$id] ?? 'absent';
    $c = $control[$id] ?? 'absent';
    if ($f === 'fail' && $c !== 'fail') {
        $ferroOnly[$id] = 'fail';
    } elseif ($f === 'fail' && in_array($id, $differentType, true)) {
        $ferroOnly[$id] = 'fail (different exception type from the control)';
    } elseif (($f === 'skip' || $f === 'absent') && ($c === 'pass' || $c === 'fail')) {
        $ferroOnly[$id] = $f === 'skip' ? 'skip' : 'not collected';
    }
}
ksort($ferroOnly);

$used = [];
$counts = ['driver-name' => 0, 'incompatibility' => 0];
$untriaged = [];
$wrongKind = [];
echo "\n== D18 ($column): every Ferro-only non-pass, against " . basename($opt['triage']) . "\n";
foreach ($ferroOnly as $id => $what) {
    // The first matching entry, except that a SKIP prefers a driver-name entry when one matches —
    // two overlapping entries must not turn an explained skip into a WRONG KIND.
    $hit = null;
    foreach ($applies as $e) {
        if (preg_match(glob_re($e['test']), $id) !== 1) {
            continue;
        }
        if ($hit === null || (! str_starts_with($what, 'fail') && $hit['kind'] !== 'driver-name' && $e['kind'] === 'driver-name')) {
            $hit = $e;
        }
    }
    $detail = isset($causes['ferro'][$id])
        ? ' [' . mb_strimwidth("{$causes['ferro'][$id][0]}: {$causes['ferro'][$id][1]}", 0, 240, '…') . ']'
        : '';
    $ctl = $control[$id] ?? 'absent';
    if ($hit === null) {
        $untriaged[] = $id;
        echo "  UNTRIAGED       $what (control: $ctl): $id$detail\n";
        continue;
    }
    $used[$hit['line']] = true;
    // D18's skip clause: "the skip sets match (or every difference is shown to be driver-name
    // gated)". A documented incompatibility does not explain a SKIP.
    if (($what === 'skip' || $what === 'not collected') && $hit['kind'] !== 'driver-name') {
        $wrongKind[] = $id;
        echo "  WRONG KIND      $what (control: $ctl): $id — line {$hit['line']} is '{$hit['kind']}', and D18 admits only a driver-name gate for a skip\n";
        continue;
    }
    $counts[$hit['kind']]++;
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

// The literal first clause of D18, reported beside the gate rather than folded into it: a
// documented incompatibility is, by definition, a control pass that does not pass through Ferro.
$ctlPass = having($control, 'pass');
$notPassing = array_values(array_filter($ctlPass, static fn (string $n): bool => ($ferro[$n] ?? 'absent') !== 'pass'));
$skipF = having($ferro, 'skip');
$skipC = having($control, 'skip');
printf("\n  pass parity: the control passes %d; through Ferro %d of them do not pass\n", count($ctlPass), count($notPassing));
printf("  skip parity: %s (ferro %d, control %d)\n", $skipF === $skipC ? 'IDENTICAL sets' : 'sets differ', count($skipF), count($skipC));
printf("  failure set: ferro %d, control %d, shared %d (%d with a different exception type)\n",
    count(having($ferro, 'fail')), count(having($control, 'fail')), count($bothFail), count($differentType));

if ($untriaged === [] && $wrongKind === []) {
    printf("D18 VERDICT %s: GREEN modulo triage — %d Ferro-only non-passes, %d driver-name, %d documented incompatibility%s\n",
        $column, count($ferroOnly), $counts['driver-name'], $counts['incompatibility'],
        $notPassing === [] ? ', strict pass parity' : sprintf(' (strict pass parity NOT met: %d control passes do not pass through Ferro)', count($notPassing)));
    exit(0);
}
printf("D18 VERDICT %s: NOT GREEN — %d Ferro-only non-passes have no triage entry%s. Each is a defect until\n"
    . "  shown otherwise: fix it, or add an entry to %s with its reason and a citation.\n",
    $column, count($untriaged), $wrongKind === [] ? '' : sprintf(', %d skips are triaged with a kind D18 does not admit for a skip', count($wrongKind)),
    $opt['triage']);
exit(1);
