<?php // /php/client/tests/Conformance/RegistrySyncTest.php
declare(strict_types=1);
namespace Ferro\Tests\Conformance;
use PHPUnit\Framework\TestCase;

final class RegistrySyncTest extends TestCase
{
    public function testGeneratedConstantsMatchLock(): void
    {
        $root = dirname(__DIR__, 4);
        $constants = "$root/php/client/src/Protocol/Generated/Constants.php";
        $before = (string) file_get_contents($constants);
        // Regenerate into a temp location by running the generator, then diff.
        $tmpHome = sys_get_temp_dir() . '/ferro_gen_' . getmypid();
        exec(sprintf('php %s 2>&1', escapeshellarg("$root/proto/tools/gen-php.php")), $out, $rc);
        $this->assertSame(0, $rc, 'gen-php.php failed: ' . implode("\n", $out));
        $after = (string) file_get_contents($constants);
        $this->assertSame($before, $after,
            'Constants.php is stale — run `php proto/tools/gen-php.php` and commit');
    }
}
