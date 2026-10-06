<?php // /php/client/tests/Tooling/QueryExtractorTest.php
declare(strict_types=1);
namespace Ferro\Tests\Tooling;

use Ferro\Tooling\QueryExtractor;
use PHPUnit\Framework\TestCase;

/**
 * M3-D2a: `#[FerroQuery]` declarations are read with PHP's tokenizer, and only what can be read
 * EXACTLY is accepted — a query the manifest gets wrong runs the wrong SQL under a trusted id.
 */
final class QueryExtractorTest extends TestCase
{
    /** @return array{0: list<array<string, mixed>>, 1: list<string>} */
    private static function extract(string $body): array
    {
        return QueryExtractor::extractSource('x.php', "<?php\nnamespace App;\nuse Ferro\\Attribute\\FerroQuery;\n" . $body);
    }

    public function testNamedLiteralArgumentsAreRead(): void
    {
        [$q, $p] = self::extract(<<<'PHP'
            #[FerroQuery(id: 'a.find', sql: 'SELECT 1 FROM t WHERE id = ?', pool: 'reports', readonly: true, dto: 'App\Dto\A')]
            final class A {}
            PHP);
        $this->assertSame([], $p);
        $this->assertSame([[
            'id' => 'a.find', 'sql' => 'SELECT 1 FROM t WHERE id = ?', 'pool' => 'reports',
            'readonly' => true, 'dto' => 'App\\Dto\\A', 'source' => 'x.php:4',
        ]], $q);
    }

    public function testANowdocIsDedentedLikePhpDoesIt(): void
    {
        [$q, $p] = self::extract(<<<'PHP'
            final class B {
                #[\Ferro\Attribute\FerroQuery(id: 'b.upd', sql: <<<'SQL'
                    UPDATE t
                      SET x = 1
                    SQL, idempotent: true)]
                public function m(): void {}
            }
            PHP);
        $this->assertSame([], $p);
        $this->assertSame("UPDATE t\n  SET x = 1", $q[0]['sql']);
        $this->assertTrue($q[0]['idempotent']);
    }

    public function testSingleQuotedEscapesAreUnescapedAsPhpDoes(): void
    {
        [$q] = self::extract("#[FerroQuery(id: 'c', sql: 'SELECT \\'it\\'\\'s\\', \\\\x')]\nfinal class C {}");
        $this->assertSame("SELECT 'it''s', \\x", $q[0]['sql']);
    }

    /** @return iterable<string, array{0: string, 1: string}> */
    public static function refused(): iterable
    {
        yield 'concatenation' => ["#[FerroQuery(id: 'x', sql: 'SELECT ' . 'x')]", 'single literal'];
        yield 'constant' => ["#[FerroQuery(id: 'x', sql: self::SQL)]", 'literal'];
        yield 'positional' => ["#[FerroQuery('x', 'SELECT 1')]", 'NAMED'];
        yield 'unknown argument' => ["#[FerroQuery(id: 'x', sql: 'SELECT 1', idempotnet: true)]", 'unknown FerroQuery argument `idempotnet`'];
        yield 'string boolean' => ["#[FerroQuery(id: 'x', sql: 'SELECT 1', idempotent: 'true')]", 'literal true or false'];
        yield 'backslash in double quotes' => ["#[FerroQuery(id: 'x', sql: \"SELECT '\\n'\")]", 'backslash'];
        yield 'interpolating heredoc' => ["#[FerroQuery(id: 'x', sql: <<<SQL\n    SELECT {\$t}\n    SQL)]", 'interpolation'];
        yield 'missing sql' => ["#[FerroQuery(id: 'x')]", '`sql:` is required'];
        yield 'repeated argument' => ["#[FerroQuery(id: 'x', id: 'y', sql: 'SELECT 1')]", 'repeated'];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('refused')]
    public function testAnythingThatCannotBeReadExactlyIsRefused(string $attribute, string $message): void
    {
        [$q, $p] = self::extract($attribute . "\nfinal class X {}");
        $this->assertSame([], $q);
        $this->assertCount(1, $p);
        $this->assertStringContainsString($message, $p[0]);
        $this->assertStringStartsWith('x.php:4', $p[0], 'the problem names its location');
    }

    public function testOtherAttributesAndRepeatedFerroQueriesInOneGroup(): void
    {
        [$q, $p] = self::extract(<<<'PHP'
            #[\Attribute, FerroQuery(id: 'd1', sql: 'SELECT 1'), FerroQuery(id: 'd2', sql: 'SELECT 2')]
            final class D {}
            #[Deprecated]
            final class E {}
            PHP);
        $this->assertSame([], $p);
        $this->assertSame(['d1', 'd2'], array_column($q, 'id'));
    }

    /** The extractor reads code; it never runs it. */
    public function testTheSourceIsNotExecuted(): void
    {
        [$q] = self::extract("throw new \\RuntimeException('executed');\n#[FerroQuery(id: 'f', sql: 'SELECT 1')]\nfinal class F {}");
        $this->assertSame('f', $q[0]['id']);
    }
}
