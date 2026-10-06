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
 *         every assertion failure shares — a different normalised WHOLE message, the expected/actual
 *         diff included (E9 review F3);
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
 *                   `fail <Type> /^<regex>/`    failed with exactly this JUnit exception type, and a
 *                                               message matching the regex — matched against the
 *                                               WHOLE message (every line before the trace, the
 *                                               assertion diff included), with JUnit's echo of the
 *                                               type stripped. The regex is REQUIRED, anchored with
 *                                               `^`, flags s/i/u only, and must match none of the
 *                                               neutral probes in EXPECT_PROBES (E9 review R2-1).
 *                 `skip` and `pass` are skip-SET differences, and D18 admits only a driver-name
 *                 gate for those, so they require kind `driver-name`.
 *   citations     `;`-separated, each one of
 *                   §22.2 (xx)                              a SPEC changelog entry that exists
 *                   docs/<path>                             a file that exists
 *                   known:"<text>"                          text in docs/known-incompatibilities.md
 *                   upstream:<path>:<line>:"<text>"         that clone line contains <text>, is
 *                                                           code (not a comment), and sits in the
 *                                                           entry's own class — immediately above
 *                                                           or inside the entry's own method
 *                   upstream-class:<path>:<line>:"<text>"   … and it is a CLASS-level gate: the next
 *                                                           code line declares the entry's own
 *                                                           class, in the entry's namespace
 *                   upstream-via:<path>:<line>:"<text>"     a line in a HELPER method of the entry's
 *                                                           class that one of the entry's own
 *                                                           upstream: lines names (e.g. a
 *                                                           DefineEnvironment attribute's target)
 *                 (the binding to the entry's class/method is E9 review R2-2; citation_binding())
 *   reason        free text, required
 *
 * ci/check-suite-triage.sh checks the same format and every citation it can without the clone, per
 * push; ci/test-suite-gate.sh runs this file against testkit/dbal/gate-fixtures/, per push.
 */

$repo = dirname(__DIR__, 2);

/** @var array<string,array<string,array{0:string,1:string,2:string}>> column => test id => [type, first message line, whole message] */
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

/**
 * Strings no expectation regex may match (E9 review R2-1). A regex that matches one of them excuses
 * failures that share nothing with the one the entry was written for: the empty string and a
 * one-letter message match a regex that constrains nothing (`^` plus a dot-star, `/^\w/`), and the rest are the
 * framing EVERY failure of a common type starts with — the DBAL wrapper's prefix, PHPUnit's
 * assertion headlines without their diff — so a regex that stops there names no cause at all.
 */
const EXPECT_PROBES = [
    '',
    'x',
    "x\nx",
    'An exception occurred while executing a query: x',
    'SQLSTATE[HY000]: General error: x',
    'Ferro: x',
];

/**
 * PHPUnit's assertion headlines. A regex may match one of these only by pinning the WHOLE message
 * to it (`/^Failed asserting that false is true\.$/`): an unlabelled `assertTrue()` reports nothing
 * else, so that exact message is the most an entry can say, while a regex that also matches the
 * headline followed by anything (a diff, another line) says nothing about which assertion failed.
 */
const EXPECT_HEADLINES = [
    'Failed asserting that two strings are identical.',
    'Failed asserting that two arrays are identical.',
    'Failed asserting that two values are equal.',
    'Failed asserting that false is true.',
    'Failed asserting that true is false.',
];

/**
 * Why an `expect` field is unusable, or null. Shared by the gate and ci/check-suite-triage.sh (which
 * calls `--check-expects`), so the two cannot disagree about what a usable regex is.
 *
 * A `fail` entry needs a regex (R2-1: `fail <Type>` alone excused every failure of that type — for
 * `PHPUnit\Framework\ExpectationFailedException`, every assertion failure there is), anchored with
 * `^` at the start of the message (an unanchored `/e/` matches almost anything), without the `m`
 * flag (which lets `^` anchor at any line of the diff), and matching none of EXPECT_PROBES.
 */
function expect_problem(string $x): ?string
{
    if ($x === 'skip' || $x === 'pass') {
        return null;
    }
    if (preg_match('~^fail ([A-Za-z_\\\\][A-Za-z0-9_\\\\]*)(?: (/.+/[a-z]*))?$~', $x, $m) !== 1) {
        return "expect must be `skip`, `pass` or `fail <Type> /^<regex>/`, got '$x'";
    }
    $re = $m[2] ?? null;
    if ($re === null) {
        return "a `fail` expectation needs a message regex — `fail <Type>` alone excuses every failure of that type: '$x'";
    }
    if (! str_starts_with($re, '/^')) {
        return "the expect regex must be anchored at the start of the message (`/^…/`): $re";
    }
    $flags = substr($re, strrpos($re, '/') + 1);
    if (preg_match('/^[siu]*$/', $flags) !== 1) {
        return "the expect regex may carry only the s, i and u flags (`m` would let `^` anchor at any line): $re";
    }
    if (@preg_match($re, '') === false) {
        return "expect regex does not compile: $re";
    }
    foreach (EXPECT_PROBES as $p) {
        if (preg_match($re, $p) === 1) {
            return sprintf('the expect regex %s is too loose: it matches the probe %s, which names no cause', $re, json_encode($p));
        }
    }
    foreach (EXPECT_HEADLINES as $h) {
        if (preg_match($re, "$h\nx") === 1) {
            return sprintf('the expect regex %s is too loose: it matches the bare assertion headline %s followed by anything — '
                . 'match the diff beneath it, or pin the whole message with `$` when there is none', $re, json_encode($h));
        }
    }
    return null;
}

/** @return array{outcome:string,type:?string,re:?string} */
function parse_expect(string $file, int $n, string $x): array
{
    $why = expect_problem($x);
    if ($why !== null) {
        triage_die($file, $n, $why);
    }
    if ($x === 'skip' || $x === 'pass') {
        return ['outcome' => $x, 'type' => null, 're' => null];
    }
    preg_match('~^fail (\S+) (/.+/[a-z]*)$~', $x, $m);
    return ['outcome' => 'fail', 'type' => $m[1], 're' => $m[2]];
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
            if (preg_match('/^(§22\.2 \([a-z]+\)|docs\/\S+|known:"[^"]+"|upstream(-class|-via)?:[^:\s]+:[0-9]+:"[^"]+")$/u', $c) !== 1) {
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
        if ($has('upstream-via:') && ! $has('upstream:')) {
            triage_die($file, $n, 'an upstream-via: citation needs the upstream: line on the entry\'s own method that names its helper');
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

/** Resolve one entry's citations. `upstream…:` ones only when a clone root is given. */
function check_citations(string $file, array $e, string $repo, ?string $upstream): void
{
    static $spec = null, $known = null;
    $spec ??= (string) @file_get_contents("$repo/ferro-spec-v0.2.md");
    $known ??= (string) @file_get_contents("$repo/docs/known-incompatibilities.md");
    // The texts of the entry's own `upstream:` citations: an `upstream-via:` line must sit in a
    // helper one of them names.
    $named = [];
    foreach ($e['cites'] as $c) {
        if (preg_match('/^upstream:[^:]+:[0-9]+:"(.+)"$/su', $c, $m) === 1) {
            $named[] = $m[1];
        }
    }
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
        } elseif ($upstream !== null && preg_match('/^upstream(-class|-via)?:([^:]+):([0-9]+):"(.+)"$/su', $c, $m) === 1) {
            $lines = @file("$upstream/{$m[2]}", FILE_IGNORE_NEW_LINES);
            $lines !== false || triage_die($file, $e['line'], "upstream file does not exist in $upstream: {$m[2]}");
            $idx = (int) $m[3] - 1;
            $at = $lines[$idx] ?? null;
            ($at !== null && str_contains($at, $m[4]))
                || triage_die($file, $e['line'], "upstream {$m[2]}:{$m[3]} does not contain: {$m[4]}");
            $why = citation_binding($lines, $idx, $m[1], $e['test'], $named);
            $why === null || triage_die($file, $e['line'], "upstream{$m[1]} {$m[2]}:{$m[3]}: $why");
        }
    }
}

/** Is this (trimmed) line a comment — a docblock line, `//` or a `#` that is not an attribute? */
function is_comment_line(string $t): bool
{
    return str_starts_with($t, '*') || str_starts_with($t, '/*') || str_starts_with($t, '//')
        || (str_starts_with($t, '#') && ! str_starts_with($t, '#['));
}

/** The namespace in force at line $idx (0-based): the last `namespace X;` at or before it. */
function namespace_at(array $lines, int $idx): string
{
    for ($i = min($idx, count($lines) - 1); $i >= 0; $i--) {
        if (preg_match('/^\s*namespace\s+([\w\\\\]+)\s*;/', $lines[$i], $m) === 1) {
            return $m[1];
        }
    }
    return '';
}

/** The class declared on this line, or null. */
function declared_class(string $line): ?string
{
    return preg_match('/^\s*(?:(?:final|abstract|readonly)\s+)*class\s+(\w+)/', $line, $m) === 1 ? $m[1] : null;
}

/**
 * Does the cited upstream line belong to the entry's OWN test (E9 review R2-2)? Without this, a
 * citation proved only that SOME gate exists in the clone: `SchemaBuilderTest::*` citing
 * `SchemaBuilderSchemaNameTest`'s class gate excused an injected skip of `testDropAllTables`, and a
 * method entry citing another method's gate passed the same way. Null when it does; else why not.
 *
 *   - the cited line is code, never a comment: a docblock that quotes a gate gates nothing;
 *   - `upstream-class:` — the next code line after it (attributes, comments and blanks skipped)
 *     declares the class, that class is the entry's own short name, and the namespace in force
 *     there is the entry's namespace;
 *   - `upstream:` — the line sits in the entry's class and namespace and, for a method entry,
 *     either immediately above the entry's method (an attribute run ending at its declaration) or
 *     inside its body (the nearest named method before it is the entry's, and its braces are open);
 *   - `upstream-via:` — the line sits in the entry's class and namespace, inside a HELPER method
 *     whose name appears in the quoted text of one of the entry's own `upstream:` citations (which
 *     are themselves bound to the entry's method). Upstream gates some tests through a helper the
 *     test names — `#[DefineEnvironment('defineEnvironmentWouldThrowsPDOException')]` on the test,
 *     the `$this->driver` branch in the helper — and this is that chain, checked link by link.
 *
 * @param list<string> $named the quoted texts of the entry's `upstream:` citations
 */
function citation_binding(array $lines, int $idx, string $mode, string $test, array $named = []): ?string
{
    $classLevel = $mode === '-class';
    [$fqcn, $method] = explode('::', $test, 2);
    $method = preg_replace('/ with data set \*$/', '', $method) ?? $method;
    $pos = strrpos($fqcn, '\\');
    $short = $pos === false ? $fqcn : substr($fqcn, $pos + 1);
    $ns = $pos === false ? '' : substr($fqcn, 0, $pos);
    $t = trim($lines[$idx]);
    if (is_comment_line($t)) {
        return "the cited line is a comment, which gates nothing: $t";
    }
    if ($classLevel) {
        for ($i = $idx + 1; $i < count($lines); $i++) {
            $u = trim($lines[$i]);
            if ($u === '' || str_starts_with($u, '#[') || is_comment_line($u)) {
                continue;
            }
            $cls = declared_class($u);
            if ($cls === null) {
                return "not a class-level gate (next code line: $u)";
            }
            if ($cls !== $short || namespace_at($lines, $i) !== $ns) {
                return sprintf('the gate is on %s\\%s, not on the entry\'s class %s', namespace_at($lines, $i), $cls, $fqcn);
            }
            return null;
        }
        return 'not a class-level gate (no class follows it)';
    }
    // The class the line sits in.
    $cls = null;
    for ($i = $idx; $i >= 0; $i--) {
        if (($cls = declared_class($lines[$i])) !== null) {
            break;
        }
    }
    if ($cls !== $short || namespace_at($lines, $idx) !== $ns) {
        return sprintf('the line is in %s\\%s, not in the entry\'s class %s', namespace_at($lines, $idx), $cls ?? '(no class)', $fqcn);
    }
    $fn = '/\bfunction\s+(\w+)\s*\(/';
    if ($mode === '-via') {
        $helper = enclosing_method($lines, $idx);
        if ($helper === null) {
            return 'the line is in no method body';
        }
        foreach ($named as $text) {
            if (preg_match('/\b' . preg_quote($helper, '/') . '\b/', $text) === 1) {
                return null;
            }
        }
        return "the line is in $helper(), which none of the entry's own upstream: lines names";
    }
    if ($method === '*') {
        return null;
    }
    // Immediately above: the cited line opens an attribute run that ends at a method declaration.
    if (str_starts_with($t, '#[') || preg_match($fn, $t) === 1) {
        for ($i = $idx; $i < count($lines); $i++) {
            if (preg_match($fn, $lines[$i], $m) === 1) {
                return $m[1] === $method ? null : "the gate is on {$m[1]}(), not on the entry's method $method()";
            }
            if ($i > $idx && preg_match('/[{};]/', $lines[$i]) === 1) {
                break;
            }
        }
        return 'an attribute that is not on a method declaration';
    }
    // Inside: the nearest named method before the line, with its body still open at the line.
    $in = enclosing_method($lines, $idx);
    if ($in === null) {
        return 'the line is in no method body';
    }
    return $in === $method ? null : "the line is in $in(), not in the entry's method $method()";
}

/** The named method whose body contains line $idx: the nearest declaration before it, braces open. */
function enclosing_method(array $lines, int $idx): ?string
{
    for ($i = $idx - 1; $i >= 0; $i--) {
        if (preg_match('/\bfunction\s+(\w+)\s*\(/', $lines[$i], $m) === 1) {
            $depth = 0;
            for ($k = $i; $k < $idx; $k++) {
                $depth += substr_count($lines[$k], '{') - substr_count($lines[$k], '}');
            }
            return $depth > 0 ? $m[1] : null;
        }
    }
    return null;
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
    // Every `fail` expectation carries a regex (expect_problem); there is no type-only arm.
    return $x['re'] !== null && preg_match($x['re'], strip_type_echo($cause[0], $cause[2])) === 1;
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
if (($argv[1] ?? null) === '--check-expects') {
    // For ci/check-suite-triage.sh: stdin is `<where>\t<expect>` lines; one line out per unusable one.
    $bad = 0;
    while (($l = fgets(STDIN)) !== false) {
        [$where, $x] = array_pad(explode("\t", rtrim($l, "\n"), 2), 2, '');
        if (($why = expect_problem($x)) !== null) {
            echo "$where: $why\n";
            $bad++;
        }
    }
    exit($bad === 0 ? 0 : 1);
}
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
