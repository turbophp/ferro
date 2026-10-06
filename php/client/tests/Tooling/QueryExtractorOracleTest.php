<?php // /php/client/tests/Tooling/QueryExtractorOracleTest.php
declare(strict_types=1);
namespace Ferro\Tests\Tooling;

use Ferro\Attribute\FerroQuery;
use Ferro\Tooling\QueryExtractor;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * M3-D2a review round: the extractor against PHP ITSELF. Each case is compiled with `eval` and its
 * class's attributes are instantiated through Reflection, so "what the manifest records" is compared
 * with "what PHP would run" rather than with what the test author believed PHP does. The review found
 * three silent divergences this way (a `b''` prefix, name resolution, line terminators).
 */
final class QueryExtractorOracleTest extends TestCase
{
    private static int $seq = 0;

    /**
     * @return list<array<string, mixed>> what PHP instantiates for class `C` in the code's namespace
     */
    private static function oracle(string $code, string $namespace): array
    {
        eval($code);
        $class = ($namespace === '' ? '' : $namespace . '\\') . 'C';
        $out = [];
        foreach ((new \ReflectionClass($class))->getAttributes(FerroQuery::class) as $a) {
            $q = $a->newInstance();
            $out[] = [
                'id' => $q->id, 'sql' => $q->sql, 'pool' => $q->pool,
                'readonly' => $q->readonly, 'idempotent' => $q->idempotent, 'dto' => $q->dto,
            ];
        }
        return $out;
    }

    /**
     * @param list<array<string, mixed>> $extracted
     * @return list<array<string, mixed>>
     */
    private static function normalise(array $extracted): array
    {
        return array_map(static fn (array $q): array => [
            'id' => $q['id'], 'sql' => $q['sql'], 'pool' => $q['pool'] ?? 'default',
            'readonly' => $q['readonly'] ?? false, 'idempotent' => $q['idempotent'] ?? false,
            'dto' => $q['dto'] ?? null,
        ], $extracted);
    }

    /** @return array<string, array{0: string, 1: int}> code with `NS` for the namespace, and how many queries PHP sees */
    public static function cases(): array
    {
        return [
            'F2 b-prefixed single quotes' => ["namespace NS; use Ferro\\Attribute\\FerroQuery;\n#[FerroQuery(id: 'a', sql: b'SELECT 1')] class C {}", 1],
            'F2 B-prefixed double quotes' => ["namespace NS; use Ferro\\Attribute\\FerroQuery;\n#[FerroQuery(id: 'a', sql: B\"SELECT 2\")] class C {}", 1],
            'F3 lower-case short name' => ["namespace NS; use Ferro\\Attribute\\FerroQuery;\n#[ferroquery(id: 'a', sql: 'SELECT 1')] class C {}", 1],
            'F3 upper-case FQN' => ["namespace NS;\n#[\\FERRO\\Attribute\\FerroQuery(id: 'a', sql: 'SELECT 1')] class C {}", 1],
            'F3 qualified via an imported namespace' => ["namespace NS; use Ferro\\Attribute;\n#[Attribute\\FerroQuery(id: 'a', sql: 'SELECT 1')] class C {}", 1],
            'F3 alias' => ["namespace NS; use Ferro\\Attribute\\FerroQuery as Q;\n#[Q(id: 'a', sql: 'SELECT 1')] class C {}", 1],
            'F3 group import with alias' => ["namespace NS; use Ferro\\Attribute\\{FerroQuery as FQ};\n#[FQ(id: 'a', sql: 'SELECT 1')] class C {}", 1],
            'F3 mixed group import' => ["namespace NS; use Ferro\\{function strlen, Attribute\\FerroQuery};\n#[FerroQuery(id: 'a', sql: 'SELECT 1')] class C {}", 1],
            'F3 comma-separated imports' => ["namespace NS; use Ferro\\Client, Ferro\\Attribute\\FerroQuery;\n#[FerroQuery(id: 'a', sql: 'SELECT 1')] class C {}", 1],
            'F4 short name without an import is another class' => ["namespace NS;\n#[FerroQuery(id: 'a', sql: 'SELECT 1')] class C {}", 0],
            'F4 unrooted qualified name is relative' => ["namespace NS;\n#[Ferro\\Attribute\\FerroQuery(id: 'a', sql: 'SELECT 1')] class C {}", 0],
            'F4 nested in another attribute' => ["namespace NS; use Ferro\\Attribute\\FerroQuery;\n#[\\Attribute(\\Attribute::TARGET_ALL), FerroQuery(id: 'b', sql: 'SELECT 2')] class C {}", 1],
            'function-import does not import a class' => ["namespace NS; use function Ferro\\Attribute\\FerroQuery;\n#[FerroQuery(id: 'a', sql: 'SELECT 1')] class C {}", 0],
            'trait and closure use do not import' => ["namespace NS; use Ferro\\Attribute\\FerroQuery; trait T {}\nclass D { use T; public function f(): \\Closure { \$x = 1; return function () use (\$x) { return \$x; }; } }\n#[FerroQuery(id: 'a', sql: 'SELECT 1')] class C {}", 1],
            'F1 CRLF in an indented nowdoc' => ["namespace NS; use Ferro\\Attribute\\FerroQuery;\r\n#[FerroQuery(id: 'a', sql: <<<'SQL'\r\n    SELECT 'x\r\n    y'\r\n    SQL)] class C {}", 1],
            'F1 form feed and non-ASCII in an indented nowdoc' => ["namespace NS; use Ferro\\Attribute\\FerroQuery;\n#[FerroQuery(id: 'a', sql: <<<'SQL'\n    SELECT 'Å' AS \"х\"\f\n    , 1\n    SQL)] class C {}", 1],
            'case-insensitive true' => ["namespace NS; use Ferro\\Attribute\\FerroQuery;\n#[FerroQuery(id: 'a', sql: 'SELECT 1', readonly: TRUE, idempotent: False)] class C {}", 1],
            'comments between arguments' => ["namespace NS; use Ferro\\Attribute\\FerroQuery;\n#[FerroQuery(/* c */ id: 'a' /* d */, // e\n sql: 'SELECT 1', # f\n pool: 'p')] class C {}", 1],
        ];
    }

    #[DataProvider('cases')]
    public function testTheExtractorAgreesWithPhp(string $template, int $count): void
    {
        $ns = 'Ferro\\Tests\\Tooling\\Oracle' . ++self::$seq;
        $code = str_replace('namespace NS;', "namespace {$ns};", $template);
        [$queries, $problems] = QueryExtractor::extractSource('x.php', "<?php\n" . $code);
        $this->assertSame([], $problems);
        $php = self::oracle($code, $ns);
        $this->assertCount($count, $php, 'the case does not test what it says (PHP saw a different count)');
        $this->assertSame($php, self::normalise($queries));
    }

    public function testBracedNamespacesKeepTheirOwnImports(): void
    {
        $a = 'Ferro\\Tests\\Tooling\\Oracle' . ++self::$seq;
        $b = 'Ferro\\Tests\\Tooling\\Oracle' . ++self::$seq;
        $code = "namespace {$a} { use Ferro\\Attribute\\FerroQuery; #[FerroQuery(id: 'a', sql: 'SELECT 1')] class C {} }\n"
            . "namespace {$b} { #[FerroQuery(id: 'b', sql: 'SELECT 2')] class C {} }";
        [$queries, $problems] = QueryExtractor::extractSource('x.php', "<?php\n" . $code);
        $this->assertSame([], $problems);
        eval($code);
        $this->assertSame(self::oracle('', $a), self::normalise($queries));
        $this->assertSame([], self::oracle('', $b), 'the second namespace has no import');
    }

    public function testAClassReferenceInsideAnAttributeIsNotADeclaration(): void
    {
        // F17: `#[CoversClass(FerroQuery::class)]` was refused as "FerroQuery needs arguments", so
        // scanning a tests directory broke the build.
        [$q, $p] = QueryExtractor::extractSource('x.php', "<?php\nnamespace T; use Ferro\\Attribute\\FerroQuery;\n#[CoversClass(FerroQuery::class)] #[Other(new FerroQuery(id: 'x', sql: 'y'))] class C {}");
        $this->assertSame([[], []], [$q, $p]);
    }

    /** @return array<string, array{0: string, 1: string}> */
    public static function refusals(): array
    {
        return [
            // F14: each of these survived a mutation of the extractor.
            'heredoc with a backslash escape' => ["sql: <<<SQL\n    SELECT '\\n'\n    SQL", 'heredoc with a backslash'],
            'heredoc with interpolation' => ["sql: <<<SQL\n    SELECT {\$x}\n    SQL", 'interpolation'],
            'empty id' => ["sql: 'SELECT 1', id: ''", '`id:` is required'],
            'empty sql' => ["sql: ''", '`sql:` is required'],
            'null pool' => ["sql: 'SELECT 1', pool: null", '`pool:` must be a string'],
            'invalid UTF-8' => ["sql: '\xff'", 'not valid UTF-8'],
        ];
    }

    #[DataProvider('refusals')]
    public function testRefusals(string $args, string $message): void
    {
        $code = "<?php\nnamespace T; use Ferro\\Attribute\\FerroQuery;\n#[FerroQuery(" . (str_contains($args, 'id:') ? '' : "id: 'a', ") . $args . ")] class C {}";
        [$q, $p] = QueryExtractor::extractSource('x.php', $code);
        $this->assertSame([], $q);
        $this->assertCount(1, $p, implode("\n", $p));
        $this->assertStringContainsString($message, $p[0]);
    }

    public function testASymlinkCycleIsWalkedOnceAndUpperCaseExtensionsAreRead(): void
    {
        // F5: symlinked directories were not entered and `.PHP` files were skipped, silently,
        // while the Rust `.sql` walk did both.
        $dir = sys_get_temp_dir() . '/ferro-queries-walk-' . getmypid();
        @mkdir($dir . '/real', 0777, true);
        file_put_contents($dir . '/real/A.PHP', "<?php\nnamespace W; use Ferro\\Attribute\\FerroQuery;\n#[FerroQuery(id: 'w', sql: 'SELECT 1')] class C {}");
        @symlink($dir . '/real', $dir . '/link');
        @symlink('.', $dir . '/real/self');
        try {
            [$q, $p] = QueryExtractor::extractPaths([$dir]);
            $this->assertSame([], $p);
            $this->assertCount(1, $q, 'found once, through one path');
            $this->assertSame('w', $q[0]['id']);
        } finally {
            @unlink($dir . '/real/self');
            @unlink($dir . '/link');
            @unlink($dir . '/real/A.PHP');
            @rmdir($dir . '/real');
            @rmdir($dir);
        }
    }
}
