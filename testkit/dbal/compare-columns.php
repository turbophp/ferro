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
 */

/** @return array<string,string> test id => pass|fail|skip */
function outcomes(string $file): array
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
$ferro = outcomes($argv[1]);
$control = outcomes($argv[2]);

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

$skippedOnlyByFerro = array_values(array_filter(
    array_diff(having($ferro, 'skip'), having($control, 'skip')),
    static fn (string $n): bool => isset($control[$n]),
));
if ($skippedOnlyByFerro !== []) {
    echo "RESULT: a test the control runs is SKIPPED under Ferro — a defect until shown otherwise.\n";
    exit(1);
}
echo "RESULT: no test the control runs is skipped under Ferro.\n";
exit(0);
