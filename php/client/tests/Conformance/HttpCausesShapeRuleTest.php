<?php // /php/client/tests/Conformance/HttpCausesShapeRuleTest.php
declare(strict_types=1);
namespace Ferro\Tests\Conformance;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The PHP half of the `[http.causes]` shape-rule AGREEMENT test (M6-F2, SPEC §22.2 (cy)). The rule
 * has two implementations — ferro-proto's `src/http_causes_rule.rs` (shared by the registry parser
 * and `build.rs`) and `proto/tools/gen-php.php` — and the shared fixture
 * `proto/tools/http-causes-shape-cases.json` states one verdict per causes table. Rust's
 * `registry_sync.rs` holds its function to those verdicts; this test holds the GENERATOR to them, by
 * running it on a copy of the committed lock whose `http.causes` is replaced by each case: a valid
 * table must generate (exit 0, every token emitted), an invalid one must be refused (non-zero exit,
 * nothing written).
 */
final class HttpCausesShapeRuleTest extends TestCase
{
    private const ROOT = __DIR__ . '/../../../..';

    /** @return iterable<string, array{0:array<string,string>,1:bool}> */
    public static function cases(): iterable
    {
        /** @var array{cases:list<array{why:string,causes:array<string,string>,valid:bool}>} $fixture */
        $fixture = json_decode(
            (string) file_get_contents(self::ROOT . '/proto/tools/http-causes-shape-cases.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        foreach ($fixture['cases'] as $case) {
            yield $case['why'] => [$case['causes'], $case['valid']];
        }
    }

    /** @param array<array-key,string> $causes */
    #[DataProvider('cases')]
    public function testGenPhpGivesTheFixturesVerdict(array $causes, bool $valid): void
    {
        $lock = json_decode((string) file_get_contents(self::ROOT . '/proto/registry.lock.json'), true, 512, JSON_THROW_ON_ERROR);
        $this->assertIsArray($lock);
        // An empty PHP array would encode as `[]`, which is not a JSON object; force `{}`.
        $lock['http']['causes'] = $causes === [] ? new \stdClass() : $causes;

        $dir = sys_get_temp_dir() . '/ferro_shape_' . getmypid() . '_' . bin2hex(random_bytes(4));
        mkdir($dir);
        try {
            $lockPath = "$dir/registry.lock.json";
            file_put_contents($lockPath, json_encode($lock, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
            $out = [];
            exec(sprintf(
                'php %s %s %s 2>&1',
                escapeshellarg(self::ROOT . '/proto/tools/gen-php.php'),
                escapeshellarg($lockPath),
                escapeshellarg($dir),
            ), $out, $rc);
            $generated = "$dir/Constants.php";
            if ($valid) {
                $this->assertSame(0, $rc, 'gen-php refused a table the rule accepts: ' . implode("\n", $out));
                $php = (string) file_get_contents($generated);
                foreach ($causes as $name => $token) {
                    $this->assertStringContainsString("public const HTTP_CAUSE_{$name} = '{$token}';", $php);
                }
            } else {
                $this->assertNotSame(0, $rc, 'gen-php accepted a table the rule refuses');
                $this->assertFileDoesNotExist($generated, 'a refused table must generate nothing');
            }
        } finally {
            foreach (glob("$dir/*") ?: [] as $f) { unlink($f); }
            rmdir($dir);
        }
    }
}
