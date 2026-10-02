<?php // /php/doctrine-dbal/tests/Dbal3/Dbal3ConnectionTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Dbal3;

use Doctrine\DBAL\Driver\Exception as DriverExceptionInterface;
use Doctrine\DBAL\Driver\ServerInfoAwareConnection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\MySQL80Platform;
use Doctrine\DBAL\Platforms\PostgreSQL100Platform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\DBAL\Dbal3\Connection;
use Ferro\DBAL\Dbal3\FerroConnection;
use Ferro\DBAL\Exception\UnsupportedStatement;
use Ferro\DBAL\PlatformVersion;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M2-C5 — the DBAL 3 connection's own methods: the ones whose SIGNATURE (and so whose return
 * convention) is DBAL 3's. Everything behavioural is the shared core's and is pinned by the DBAL 4
 * lane's unit tests plus the live suite, which runs in both lanes.
 */
final class Dbal3ConnectionTest extends TestCase
{
    private static function conn(FakeSession $session, string $kind = PlatformVersion::KIND_POSTGRES): Connection
    {
        return new Connection(new FerroClientConnection($session, 'default'), 'p', $kind, false);
    }

    /**
     * Without this interface DBAL 3 never asks for a version at all and falls back to a
     * version-less platform — so it is asserted, not assumed from the class declaration.
     */
    public function testItIsServerInfoAware(): void
    {
        self::assertInstanceOf(ServerInfoAwareConnection::class, self::conn(new FakeSession()));
    }

    /** Same per-family rule as DBAL 4, locked against DBAL 3's OWN stock platforms. */
    public function testQuoteMatchesEachFamilysStockDbal3Platform(): void
    {
        foreach (["o'brien", 'C:\\path\\to', "a'b\\c"] as $in) {
            self::assertSame((new PostgreSQL100Platform())->quoteStringLiteral($in), self::conn(new FakeSession())->quote($in));
            self::assertSame((new MySQL80Platform())->quoteStringLiteral($in), self::conn(new FakeSession(), PlatformVersion::KIND_MYSQL)->quote($in));
            self::assertSame((new SqlitePlatform())->quoteStringLiteral($in), self::conn(new FakeSession(), PlatformVersion::KIND_SQLITE)->quote($in));
        }
    }

    /**
     * DBAL 3's wrapper converts the value through its Type BEFORE calling the driver, so a scalar
     * that is not a string arrives here; `$type` does not change a string literal.
     */
    public function testQuoteTakesTheScalarsDbal3HandsIt(): void
    {
        $c = self::conn(new FakeSession());
        self::assertSame("'42'", $c->quote(42, ParameterType::INTEGER));
        self::assertSame("'1'", $c->quote(true, ParameterType::BOOLEAN));
        self::assertSame("'0'", $c->quote(false, ParameterType::BOOLEAN));
        self::assertSame("''", $c->quote(null));
        $this->expectException(DriverExceptionInterface::class);
        $c->quote([1]);
    }

    public function testLastInsertIdReturnsTheKeyTheEngineReported(): void
    {
        $c = self::conn((new FakeSession())->thenExecOk(41), PlatformVersion::KIND_MYSQL);
        $c->exec('INSERT INTO t () VALUES ()');
        self::assertSame(41, $c->lastInsertId());
    }

    /**
     * DBAL 3's docblock allows `false` for "no key". `false` is a silently WRONG key in Doctrine
     * ORM 2's `IdentityGenerator` (`(int) false === 0`), so the driver throws — as a DRIVER exception,
     * which DBAL 3's wrapper converts — and a sequence name changes nothing.
     */
    public function testNoKeyThrowsADriverExceptionNeverFalse(): void
    {
        foreach ([null, 'items_id_seq'] as $name) {
            $c = self::conn((new FakeSession())->thenExecOk());
            $c->exec('INSERT INTO items DEFAULT VALUES');
            try {
                $c->lastInsertId($name);
                self::fail('no key must throw, never return false');
            } catch (DriverExceptionInterface $e) {
                self::assertStringContainsString('PostgreSQL reports no generated key', $e->getMessage());
            }
        }
    }

    /** DBAL 3's transaction methods return `bool` (DBAL 4's return `void`). */
    public function testTransactionMethodsReturnTrue(): void
    {
        $c = self::conn(FakeSession::withTxBegin(txId: 7)->thenControlOk());
        self::assertTrue($c->beginTransaction());
        self::assertTrue($c->commit());

        $c = self::conn(FakeSession::withTxBegin(txId: 8)->thenControlOk());
        self::assertTrue($c->beginTransaction());
        self::assertTrue($c->rollBack());
    }

    /**
     * The isolation refusal must name THIS major's wrapper: DBAL 4's wrapper cannot even be declared
     * under DBAL 3, so naming it would turn the one-line fix into a fatal error.
     */
    public function testTheIsolationRefusalNamesTheDbal3Wrapper(): void
    {
        $session = new FakeSession();
        try {
            self::conn($session)->exec('SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE');
            self::fail('the isolation statement must be refused');
        } catch (UnsupportedStatement $e) {
            self::assertStringContainsString(FerroConnection::class, $e->getMessage());
            self::assertStringNotContainsString('Ferro\\DBAL\\Wrapper\\FerroConnection', $e->getMessage());
        }
        self::assertSame(0, $session->sendCount(), 'refused before anything reached the wire');
    }

    /**
     * A BINARY/LARGE_OBJECT value is refused rather than quoted as text (M2-C5 review F7): both stock
     * PostgreSQL drivers escape it as `bytea` there, so `"\\x41"` quoted as TEXT would be read by
     * PostgreSQL's `bytea` input as hex and store one byte instead of four.
     */
    public function testQuoteRefusesBinaryTypes(): void
    {
        $c = self::conn(new FakeSession());
        foreach ([ParameterType::BINARY, ParameterType::LARGE_OBJECT] as $type) {
            try {
                $c->quote('\\x41', $type);
                self::fail("quote() must refuse ParameterType $type");
            } catch (DriverExceptionInterface $e) {
                self::assertStringContainsString('Bind it as a parameter', $e->getMessage());
            }
        }
        self::assertSame("'x'", $c->quote('x', ParameterType::ASCII), 'the textual types still quote');
    }

    /** The isolation refusal on the `query()` and prepared paths too, not only `exec()`. */
    public function testTheIsolationRefusalCoversTheQueryAndPreparedPaths(): void
    {
        $sql = 'SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE';
        $session = new FakeSession();
        $c = self::conn($session);
        foreach ([
            'query' => static fn () => $c->query($sql),
            'prepared' => static fn () => $c->prepare($sql)->execute(),
        ] as $path => $run) {
            try {
                $run();
                self::fail("the $path path must refuse the isolation statement");
            } catch (UnsupportedStatement $e) {
                self::assertStringContainsString(FerroConnection::class, $e->getMessage());
            }
        }
        self::assertSame(0, $session->sendCount());
    }

    /**
     * DBAL 3's SPI has no `getColumnName()`, but the shared `Result` keeps it for DBAL 4 — and a
     * direct call past the last column must still fail loudly, with no DBAL 4 `InvalidColumnIndex`
     * class to throw.
     */
    public function testAnOutOfRangeColumnNameIsALoudDriverException(): void
    {
        $r = \Ferro\DBAL\Result::buffered(['a'], [[1]], 0);
        self::assertSame('a', $r->getColumnName(0));
        $this->expectException(DriverExceptionInterface::class);
        $this->expectExceptionMessage('column index 1 does not exist');
        $r->getColumnName(1);
    }
}
