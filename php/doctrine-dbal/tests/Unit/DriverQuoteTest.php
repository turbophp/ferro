<?php // /php/doctrine-dbal/tests/Unit/DriverQuoteTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Platforms\MySQL84Platform;
use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\DBAL\Connection;
use Ferro\DBAL\PlatformVersion;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 5 — `quote()` is PER-FAMILY, and the two branches are locked against the stock
 * platform accessors rather than restated.
 *
 * `AbstractPlatform::quoteStringLiteral()` doubles the single quote; `AbstractMySQLPlatform`
 * OVERRIDES it to escape backslashes first, because MySQL treats `\` as an escape character inside
 * a string literal. A driver that emitted the PostgreSQL form on a MySQL connection would mangle
 * every value containing a backslash — silently, since the result is still valid SQL.
 *
 * `quote()` must not need a platform (and therefore must not need a server version), because it has
 * to keep working on a pool whose version is unknown. So the two branches live in the driver, and
 * this test is what stops them drifting from Doctrine's.
 */
final class DriverQuoteTest extends TestCase
{
    /** @return array<string, array{0: string, 1: string}> the values that discriminate the two forms */
    public static function values(): array
    {
        return [
            'plain' => ["o'brien", "o'brien"],
            'backslash' => ['C:\\path\\to', 'C:\\path\\to'],
            'both' => ["a'b\\c", "a'b\\c"],
        ];
    }

    private function driverConn(string $kind): Connection
    {
        // `Ferro\Client\Connection` is FINAL (php/client/src/Client/Connection.php:48), so it is
        // constructed directly over a scripted-nothing FakeSession rather than subclassed. `quote()`
        // never sends a frame, so nothing needs queueing.
        //
        // Four arguments: ($ferro, $poolName, $poolKind, $readonly). The pool NAME ('p') is unused
        // by `quote()` and is there because it is part of the constructor from this task onward —
        // see the `__construct` docblock in src/Connection.php for why it is not added later.
        return new Connection(new FerroClientConnection(new FakeSession(), 'default'), 'p', $kind, false);
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('values')]
    public function testEachFamilyMatchesItsStockPlatform(string $in, string $same): void
    {
        self::assertSame($in, $same); // the provider carries the value once; this pins the shape

        self::assertSame(
            (new PostgreSQL120Platform())->quoteStringLiteral($in),
            $this->driverConn(PlatformVersion::KIND_POSTGRES)->quote($in),
        );
        self::assertSame(
            (new MySQL84Platform())->quoteStringLiteral($in),
            $this->driverConn(PlatformVersion::KIND_MYSQL)->quote($in),
        );
    }

    /** …and the two families genuinely differ, so neither branch is dead code. */
    public function testTheTwoFamiliesDifferOnABackslash(): void
    {
        self::assertNotSame(
            $this->driverConn(PlatformVersion::KIND_POSTGRES)->quote('a\\b'),
            $this->driverConn(PlatformVersion::KIND_MYSQL)->quote('a\\b'),
        );
    }

    /**
     * ADDED (beyond the plan) — the accessors the LATER tasks consume, asserted here because this is
     * the first task that can. Task 6's loud `ServerVersionUnavailable` must NAME the pool, and it
     * gets the name from `poolName()`; Tasks 7-13 all read `poolKind()`. A constructor that bound
     * the two strings in the wrong ORDER would leave `quote()` correct in this file (the kind is
     * passed positionally by the helper above) while making Task 6's message name the family instead
     * of the pool — so the pair has to be read back, not merely passed in.
     */
    public function testThePoolNameAndKindAreReadBackDistinctly(): void
    {
        $c = new Connection(
            new FerroClientConnection(new FakeSession(), 'default'),
            'reporting',
            PlatformVersion::KIND_MYSQL,
            false,
        );
        self::assertSame('reporting', $c->poolName());
        self::assertSame(PlatformVersion::KIND_MYSQL, $c->poolKind());
    }
}
