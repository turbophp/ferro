<?php // testkit/dbal/compare-columns.php
declare(strict_types=1);

/**
 * Compare two suite columns — a Ferro one and its control — test by test, from their JUnit logs
 * (`testkit/dbal-suite.sh … --log-junit <file>`). M2-C5b, SPEC §22.2 (bz).
 *
 * It exists because the thing worth knowing is a SET difference, and equal COUNTS do not prove
 * equal sets: C5b's first PostgreSQL triage classified every non-pass and was still incomplete, since
 * one upstream test SKIPPED under Ferro and RAN under `pdo_pgsql`. A skip is not a non-pass, so only
 * a comparison of what each column skipped can see that class.
 *
 *   php testkit/dbal/compare-columns.php <ferro.xml> <control.xml>
 *
 * Prints, for each outcome, the tests in one column and not the other, plus a digest of each sorted
 * skip list so two reports can be compared without the lists. Exits 1 when a test the CONTROL ran is
 * SKIPPED under Ferro — the case the rule exists for — and 0 otherwise (failures are a triage
 * question, not a harness one). Exits 2 on unusable input.
 *
 * A FAILURE SHARED WITH THE CONTROL IS ONLY ATTRIBUTED IF IT FAILED FOR THE SAME REASON (M2-C1f).
 * The comparison above is by test NAME, and the C1f review showed what that misses: two
 * `testBasicUpdateForJson` cases failed in BOTH MySQL-family columns and so read as "upstream's own"
 * — the control on MariaDB's syntax error for `cast(? as json)`, the Ferro column on the shim's
 * `quote()` refusal, which has nothing to do with it. So every test that fails in both columns is
 * compared by the exception TYPE JUnit records, and a different type also exits 1. The first message
 * line is printed beside it when it differs, for triage, but does not decide the exit status:
 * messages carry paths and values that legitimately differ between two runs of the same cause.
 */

/** @var array<string,array{0:string,1:string}> test id => [exception type, first message line] */
$GLOBALS['causes'] = [];

/** @return array<string,string> test id => pass|fail|skip */
function outcomes(string $file, string $column): array
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
            $GLOBALS['causes'][$column][$id] = [(string) $el['type'], trim($lines[1] ?? '')];
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

if ($argc !== 3) {
    fwrite(STDERR, "usage: php compare-columns.php <ferro.xml> <control.xml>\n");
    exit(2);
}
$ferro = outcomes($argv[1], 'ferro');
$control = outcomes($argv[2], 'control');

$section = static function (string $title, array $names): void {
    printf("%s: %d\n", $title, count($names));
    foreach ($names as $n) {
        echo "  $n\n";
    }
};

printf("tests: ferro %d, control %d\n", count($ferro), count($control));
$section('in only one column', array_values(array_merge(
    array_map(static fn ($n) => "ferro only: $n", array_keys(array_diff_key($ferro, $control))),
    array_map(static fn ($n) => "control only: $n", array_keys(array_diff_key($control, $ferro))),
)));
foreach (['skip', 'fail'] as $state) {
    $f = having($ferro, $state);
    $c = having($control, $state);
    printf("%s: ferro %d (sha256 %s), control %d (sha256 %s)\n", $state, count($f),
        substr(hash('sha256', implode("\n", $f)), 0, 16), count($c), substr(hash('sha256', implode("\n", $c)), 0, 16));
    $section("  $state under ferro, not under control", array_values(array_diff($f, $c)));
    $section("  $state under control, not under ferro", array_values(array_diff($c, $f)));
}

$bothFail = array_values(array_intersect(having($ferro, 'fail'), having($control, 'fail')));
$differentType = [];
foreach ($bothFail as $n) {
    [$ft, $fm] = $GLOBALS['causes']['ferro'][$n];
    [$ct, $cm] = $GLOBALS['causes']['control'][$n];
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
